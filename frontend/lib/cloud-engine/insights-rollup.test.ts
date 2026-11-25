// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

const settings = vi.hoisted(() => ({ off: new Set<string>() }));
// What the adapter resolves on the main thread and sends with a summaries read.
const withSettings = <T extends object>(request: T) => ({
  ...request,
  enabled_log_fields: [...new Set(INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields))]
    .filter((field) => !settings.off.has(field)),
});
vi.mock("./active-log-fields", async () => {
  const { INSIGHT_DEFINITIONS } = await import("./insights");
  const every = INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields);
  return { cloudActiveLogFieldIds: async () => new Set(every.filter((field) => !settings.off.has(field))) };
});
import { DuckDBAnalyticalEngine as ParquetEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import { BOUNDS, Histogram } from "./dashboard/histogram";
import { decodeRollup, emptyAgg, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
import {
  FIXTURE_START,
  SUMMARY_FIXTURE_BUFFERS as BUFFERS,
  fixtureFiles,
  fixturePage,
} from "./dashboard/summary-fixtures";
import { fixtureSource, projectingSource, rangedReadsOnly } from "./dashboard/test-fixtures";
import { queryCloudInsights } from "./insight-scans";
import {
  _coalesced_asn_aggregates,
  _coalesced_city_aggregates,
  _coalesced_dim_aggregates,
  _coalesced_scalar_aggregates,
  handleInsights,
} from "./insights";
import { INSIGHTS_RESIDUAL_FIELDS } from "./insights-residual";
import { INSIGHTS_SUMMARY_SPEC, coalesceUncoveredIntervals, insightsBaselineFromRollup, insightsBaselineRollup, insightsDeferredDetectors, insightsFromRollup, ipDetectorsRuledOut, rollupInsightsBaseline } from "./insights-rollup";
import { parquetWriteBuffer } from "hyparquet-writer";
import { _coalesced_ip_security_aggregates, INSIGHT_DEFINITIONS } from "./insights";
import type { Rollup } from "./dashboard/rollup-query";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const WINDOW_START = T0 + HOUR;
const T1 = T0 + 2 * HOUR;
const RAW = fixtureFiles("raw");
const BASELINE_WINDOWS = fixtureFiles("window", (e) => e.end <= FIXTURE_START + 3600);
const body = { service_id: "example", window_size_hrs: 1, baseline_hours: 1, clamp_end: new Date(T1).toISOString() };
const signal = new AbortController().signal;

async function baselineRollup(): Promise<RollupResult> {
  const parts = await Promise.all(BASELINE_WINDOWS.map((f) => decodeRollup(BUFFERS[f.key], 300_000)));
  return { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: WINDOW_START };
}

type Row = Record<string, unknown>;
const byKey = (rows: readonly Row[], key: (row: Row) => string, value: string) =>
  Object.fromEntries(rows.map((row) => [key(row), Number(row[value] ?? 0)]));
const cityKey = (row: Row) => `${row.city}|${row.region}|${row.country}`;

describe("insights baseline from summaries vs raw DuckDB over the same rows", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  let read: DuckDBReadContext;

  beforeAll(async () => {
    for (const file of RAW) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, BUFFERS[file.key], undefined, undefined, "request", {
        deferView: true,
      });
    }
    await lease.publishView();
    read = lease.readContext();
  }, 60_000);

  afterAll(async () => {
    lease.release();
    await engine.reset();
  });

  const scan = (fromMs: number, builder: (table: string, start: string) => string) =>
    read.executeSql(`WITH s AS (SELECT * FROM requests WHERE ts_ms >= ${fromMs} AND ts_ms < ${T1})
      SELECT * FROM (${builder("s", new Date(WINDOW_START).toISOString())})`, [], signal);

  it("fills each window row's baseline from summaries as the raw scan counts it", async () => {
    const [raw, windowOnly] = await Promise.all([T0, WINDOW_START].map(async (from) => ({
      city: await scan(from, _coalesced_city_aggregates),
      dimension: await scan(from, _coalesced_dim_aggregates),
      asn: await scan(from, _coalesced_asn_aggregates),
      scalar: await scan(from, _coalesced_scalar_aggregates),
    })));
    const [rawTotal] = await read.executeSql(
      `SELECT COUNT(*) AS n FROM requests WHERE ts_ms >= ${T0} AND ts_ms < ${WINDOW_START}`, [], signal);
    const baseline = rollupInsightsBaseline(await baselineRollup(), windowOnly);

    expect(baseline.baselineRequests).toBe(Number(rawTotal.n));

    // One region per city in the fixture, so merging regions changes nothing here.
    expect(byKey(baseline.rows.city, cityKey, "b_cnt")).toEqual(byKey(raw.city, cityKey, "b_cnt"));
    const rawErrors = byKey(raw.city, cityKey, "b_errors_4xx");
    const errors = byKey(baseline.rows.city, cityKey, "b_errors_4xx");
    // Summaries count status 0 as an error too, which the fixture has.
    for (const [key, n] of Object.entries(rawErrors)) expect(errors[key], key).toBeGreaterThanOrEqual(n);
    expect(Object.values(errors).reduce((a, b) => a + b, 0))
      .toBeGreaterThan(Object.values(rawErrors).reduce((a, b) => a + b, 0));

    const country = (rows: readonly Row[]) => rows.filter((r) => r.dimension === "country");
    const countryKey = (row: Row) => String(row.country);
    expect(byKey(country(baseline.rows.dimension), countryKey, "b_cnt"))
      .toEqual(byKey(country(raw.dimension), countryKey, "b_cnt"));
    expect(byKey(country(baseline.rows.dimension), countryKey, "w_cnt"))
      .toEqual(byKey(country(raw.dimension), countryKey, "w_cnt"));

    const asnKey = (row: Row) => String(row.asn);
    const asns = (rows: readonly Row[]) => rows.filter((r) => r.asn != null);
    expect(byKey(asns(baseline.rows.asn), asnKey, "b_cnt")).toEqual(byKey(asns(raw.asn), asnKey, "b_cnt"));

    for (const column of ["b_hits", "b_cacheable", "b_quic", "b_tr_total", "w_hits", "w_cacheable"]) {
      expect(Number(baseline.rows.scalar[0][column] ?? 0), column).toBe(Number(raw.scalar[0][column] ?? 0));
    }
    expect(Number(raw.scalar[0].b_hits)).toBeGreaterThan(0);
  });

  it("evaluates the summarized detectors as the raw path does, with summary labels and data-through", async () => {
    const options = { signal, maskIps: false, filters: [], engine: read };
    // The fixtures lie in 2027; the window end is clamped to now.
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    const baseline = await baselineRollup();
    const [raw, rolled] = await (async () => {
      try {
        return [
          await queryCloudInsights(body, options),
          await queryCloudInsights(body, { ...options, baseline }),
        ];
      } finally {
        vi.useRealTimers();
      }
    })();

    expect(raw).not.toHaveProperty("rollup");
    expect(raw).not.toHaveProperty("deferred_detectors");
    expect(rolled.rollup).toMatchObject({
      data_from: new Date(T0).toISOString(),
      data_through: new Date(WINDOW_START).toISOString(),
      lower_bound: [],
    });
    expect(rolled.rollup!.approximate).toEqual(expect.arrayContaining(["city_surges", "city_error_spikes", "new_city_traffic"]));
    expect(rolled.window_total_requests).toBe(raw.window_total_requests);
    expect(rolled.insights.map((c) => c.id)).toEqual(raw.insights.map((c) => c.id));
    expect(raw.insights.filter((c) => c.summary.includes("Not evaluated") || c.placeholder)).toEqual([]);
    const ids = raw.insights.map((c) => c.id);
    const deferredSet = insightsDeferredDetectors(baseline.rollup);
    const deferred = ids.filter((id) => deferredSet.has(id));
    expect(deferred).toEqual([]);
    expect(rolled.deferred_detectors).toEqual([]);
    expect(ids).toEqual(expect.arrayContaining(["city_surges", "new_country_traffic", "asn_concentration", "cache_hit_cliff"]));
    const strip = (items: { baseline_val: number; baseline_label?: string }[], dropBaseline: boolean) =>
      items.map(({ baseline_label: _, baseline_val, ...rest }) => (dropBaseline ? rest : { ...rest, baseline_val }));
    for (const card of rolled.insights) {
      const rawCard = raw.insights.find((c) => c.id === card.id)!;
      if (deferred.includes(card.id)) {
        expect(card, card.id).toMatchObject({ severity: "info", summary: "Computing baseline…", items: [], placeholder: true });
        continue;
      }
      const approximate = rolled.rollup!.approximate.includes(card.id);
      expect(card.severity, card.id).toBe(rawCard.severity);
      expect(strip(card.items, approximate), card.id).toEqual(strip(rawCard.items, approximate));
      for (const [i, item] of card.items.entries()) {
        const wantLabel = card.id === "tail_latency"
          ? (approximate ? "≈ P50" : "P50")
          : (approximate ? "≈ baseline" : undefined);
        expect(item.baseline_label, card.id).toBe(wantLabel);
        // Summaries also count status 0 as an error.
        if (card.id === "city_error_spikes") expect(item.baseline_val).toBeGreaterThanOrEqual(rawCard.items[i].baseline_val);
      }
    }
    expect(rolled.insights.find((c) => c.id === "city_error_spikes")!.items.length).toBeGreaterThan(0);
  });

  it("labels detectors over cut top-K counts as lower bounds, or estimates for ratios", async () => {
    const result = await baselineRollup();
    const truncated: RollupResult = {
      ...result,
      rollup: { ...result.rollup, truncated: new Set(["all_fields/ua", "all_fields/cache", "dims/city"]) },
    };
    const { meta, source } = rollupInsightsBaseline(truncated, { city: [], dimension: [], asn: [], scalar: [] });
    expect(meta.lower_bound).toEqual(["ua_monoculture"]);
    expect(meta.approximate.toSorted()).toEqual([
      "cache_hit_cliff",
      "city_error_spikes",
      "city_latency_regressions",
      "city_surges",
      "latency_regression",
      "new_city_traffic",
      "pop_latency_regression",
    ]);

    const ua = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["ua"]),
      availableHistoryHours: 1,
      dimensionAggregateRows: [
        { dimension: "ua", ua: "bot", w_cnt: 90, b_cnt: 1 },
        { dimension: "ua", ua: null, w_cnt: 10, b_cnt: 99 },
      ],
      windowTotalRequests: 100,
      baselineTotalRequests: 100,
      baselineSource: source,
    });
    expect(ua.insights.find((c) => c.id === "ua_monoculture")!.items)
      .toMatchObject([{ label: "bot", baseline_val: 1, baseline_label: "≥ baseline" }]);
  });

  it("reads the unfiltered baseline range from summaries, never the window's", async () => {
    const log = { json: [] as string[], files: [] as string[] };
    const source = fixtureSource([fixturePage(RAW)], BUFFERS, log, [fixturePage(fixtureFiles("window"))]);
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    const result = await insightsBaselineFromRollup(body, [], source, signal).finally(() => vi.useRealTimers());
    expect(result).toMatchObject({ dataFromMs: T0, dataThroughMs: WINDOW_START });
    const none = { city: [], dimension: [], asn: [], scalar: [] };
    expect(rollupInsightsBaseline(result!, none).baselineRequests)
      .toBe(rollupInsightsBaseline(await baselineRollup(), none).baselineRequests);
    expect(log.files.toSorted()).toEqual(BASELINE_WINDOWS.map((f) => f.key).toSorted());

    const filtered = { json: [] as string[], files: [] as string[] };
    const country = [{ field: "country", operator: "eq", value: "US" }] as never;
    expect(await insightsBaselineFromRollup(body, country,
      fixtureSource([fixturePage(RAW)], BUFFERS, filtered, [fixturePage(fixtureFiles("window"))]), signal)).toBeNull();
    expect(filtered).toEqual({ json: [], files: [] });
  });

  it("url detectors read ins_url baselines and match raw, skipping absent URLs when truncated", async () => {
    const base = await baselineRollup();
    const h = new Histogram();
    h.min = 50_000;
    h.max = 50_000;
    h.buckets[BOUNDS.findIndex((b) => b > 50_000) - 1] = 25; // 50 ms

    const insUrlMap = new Map([
      ["/err", { ...emptyAgg(), requests: 100, status5xx: 1, hitRequests: 0, missRequests: 0, passRequests: 0 }],
      ["/collapse", { ...emptyAgg(), requests: 100, status5xx: 0, hitRequests: 90, missRequests: 10, passRequests: 0 }],
      ["/pass", { ...emptyAgg(), requests: 100, status5xx: 0, hitRequests: 0, missRequests: 0, passRequests: 5 }],
      ["/slow", { ...emptyAgg(), requests: 50, status5xx: 0, hitRequests: 0, missRequests: 0, passRequests: 0 }],
    ]);
    const urlsMap = new Map([
      ["/slow", { ...emptyAgg(), requests: 50, elapsedCount: 25, hists: { ...emptyAgg().hists, elapsed: h } }],
    ]);
    const covered: RollupResult = {
      ...base,
      rollup: {
        ...base.rollup,
        groups: new Map([
          ["ins_url", new Map([["url", insUrlMap]])],
          ["urls", new Map([["url", urlsMap]])],
        ]),
        present: new Set([...(base.rollup.present ?? []), "ins_url/url", "urls/url"]),
      },
    };
    const urlRows = [
      { url: "/err", w_total: 20, w_5xx: 10, w_hits: 0, w_miss: 0, w_pass: 0, w_lat_total: 0, w_p95: 0, w_p99: 0, w_p50: 0 },
      { url: "/collapse", w_total: 20, w_5xx: 0, w_hits: 2, w_miss: 18, w_pass: 0, w_lat_total: 0, w_p95: 0, w_p99: 0, w_p50: 0 },
      { url: "/pass", w_total: 20, w_5xx: 0, w_hits: 0, w_miss: 0, w_pass: 18, w_lat_total: 0, w_p95: 0, w_p99: 0, w_p50: 0 },
      { url: "/slow", w_total: 20, w_5xx: 0, w_hits: 0, w_miss: 0, w_pass: 0, w_lat_total: 20, w_p95: 500, w_p99: 600, w_p50: 100 },
    ];

    const { rows, source, meta } = rollupInsightsBaseline(covered, {
      url: urlRows,
      city: [],
      dimension: [],
      asn: [],
      scalar: [],
    });

    for (const id of ["error_spikes", "cache_collapse", "cacheability_regression", "latency_regression"]) {
      expect(source.deferred.has(id), id).toBe(false);
    }
    expect(meta.approximate).toContain("latency_regression");
    expect(meta.approximate).not.toContain("error_spikes");

    const env = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["url", "status", "cache", "elapsed"]),
      availableHistoryHours: 1,
      urlAggregateRows: rows.url,
      scalarAggregateRows: rows.scalar,
      windowTotalRequests: 80,
      baselineTotalRequests: 350,
      baselineSource: source,
    });
    expect(env.insights.find((c) => c.id === "error_spikes")!.items).toMatchObject([{ label: "/err", baseline_val: 1 }]);
    expect(env.insights.find((c) => c.id === "cache_collapse")!.items).toMatchObject([{ label: "/collapse", baseline_val: 90 }]);
    expect(env.insights.find((c) => c.id === "cacheability_regression")!.items).toMatchObject([{ label: "/pass", baseline_val: 5 }]);
    expect(env.insights.find((c) => c.id === "latency_regression")!.items).toMatchObject([
      { label: "/slow", baseline_label: "≈ baseline" },
    ]);

    // Truncated baseline: absent URL must be skipped by error_spikes, while tail_latency still runs.
    const truncated: RollupResult = {
      ...covered,
      rollup: { ...covered.rollup, truncated: new Set([...covered.rollup.truncated, "ins_url/url"]) },
    };
    const truncRes = rollupInsightsBaseline(truncated, {
      url: [{ url: "/absent-5xx", w_total: 25, w_5xx: 20, w_hits: 0, w_miss: 0, w_pass: 0, w_lat_total: 25, w_p95: 100, w_p99: 600, w_p50: 50 }],
      city: [],
      dimension: [],
      asn: [],
      scalar: [],
    });
    const truncEnv = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["url", "status", "cache", "elapsed"]),
      availableHistoryHours: 1,
      urlAggregateRows: truncRes.rows.url,
      scalarAggregateRows: truncRes.rows.scalar,
      windowTotalRequests: 25,
      baselineTotalRequests: 350,
      baselineSource: truncRes.source,
    });
    expect(truncEnv.insights.find((c) => c.id === "error_spikes")!.items).toEqual([]);
    expect(truncEnv.insights.find((c) => c.id === "tail_latency")!.items).toMatchObject([{ label: "/absent-5xx" }]);
  });

  it("ip detectors read ins_ip baselines and label lower bounds when truncated", async () => {
    const base = await baselineRollup();
    const insIpMap = new Map([
      ["1.2.3.4", { ...emptyAgg(), requests: 5, errors: 2, count: 3 }],
    ]);
    const covered: RollupResult = {
      ...base,
      rollup: {
        ...base.rollup,
        groups: new Map([["ins_ip", new Map([["ip", insIpMap]])]]),
        present: new Set([...(base.rollup.present ?? []), "ins_ip/ip"]),
      },
    };
    const ipRows = [
      { ip: "1.2.3.4", ce_w_denied: 30, ce_w_attempts: 40, ce_w_paths: 5, w_total: 40, cd_w_404: 35, cd_distinct_404: 20 },
    ];

    const { rows, source } = rollupInsightsBaseline(covered, {
      ip: ipRows,
      city: [],
      dimension: [],
      asn: [],
      scalar: [],
    });
    expect(source.deferred.has("credential_enumeration")).toBe(false);
    expect(source.deferred.has("content_discovery")).toBe(false);

    const env = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["ip", "url", "status"]),
      availableHistoryHours: 1,
      urlAggregateRows: [],
      ipSecurityRows: rows.ip,
      windowTotalRequests: 40,
      baselineTotalRequests: 100,
      baselineSource: source,
    });
    expect(env.insights.find((c) => c.id === "credential_enumeration")!.items).toMatchObject([
      { label: "1.2.3.4", baseline_val: 2 },
    ]);
    expect(env.insights.find((c) => c.id === "content_discovery")!.items).toMatchObject([
      { label: "1.2.3.4", baseline_val: 3 },
    ]);

    const truncated: RollupResult = {
      ...covered,
      rollup: { ...covered.rollup, truncated: new Set([...covered.rollup.truncated, "ins_ip/ip"]) },
    };
    const truncRes = rollupInsightsBaseline(truncated, {
      ip: [{ ip: "9.9.9.9", ce_w_denied: 30, ce_w_attempts: 40, ce_w_paths: 5, w_total: 40, cd_w_404: 35, cd_distinct_404: 20 }],
      city: [],
      dimension: [],
      asn: [],
      scalar: [],
    });
    expect(truncRes.meta.lower_bound).toEqual(expect.arrayContaining(["credential_enumeration", "content_discovery"]));
    const truncEnv = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["ip", "url", "status"]),
      availableHistoryHours: 1,
      urlAggregateRows: [],
      ipSecurityRows: truncRes.rows.ip,
      windowTotalRequests: 40,
      baselineTotalRequests: 100,
      baselineSource: truncRes.source,
    });
    expect(truncEnv.insights.find((c) => c.id === "credential_enumeration")!.items).toMatchObject([
      { label: "9.9.9.9", baseline_val: 0, baseline_label: "≥ baseline" },
    ]);
  });

  it("city and pop latency regressions from ins_lat", async () => {
    const base = await baselineRollup();
    const h = new Histogram();
    h.min = 50_000;
    h.max = 50_000;
    h.buckets[BOUNDS.findIndex((b) => b > 50_000) - 1] = 60; // 50 ms

    const insLatCity = new Map([
      [JSON.stringify(["Springfield", "IL", "US"]), { ...emptyAgg(), requests: 60, elapsedCount: 60, hists: { ...emptyAgg().hists, elapsed: h } }],
    ]);
    const insLatPop = new Map([
      ["ORD", { ...emptyAgg(), requests: 60, elapsedCount: 60, hists: { ...emptyAgg().hists, elapsed: h } }],
    ]);
    const covered: RollupResult = {
      ...base,
      rollup: {
        ...base.rollup,
        groups: new Map([
          ["ins_lat", new Map([["city", insLatCity], ["pop", insLatPop]])],
        ]),
        present: new Set([...(base.rollup.present ?? []), "ins_lat/city", "ins_lat/pop"]),
      },
    };
    const cityRows = [
      { city: "Springfield", region: "IL", country: "US", w_cnt: 20, w_errors_4xx: 0, w_lat_total: 20, w_p95: 700 },
      { city: "Springfield", region: "MO", country: "US", w_cnt: 20, w_errors_4xx: 0, w_lat_total: 20, w_p95: 700 },
    ];
    const geoRows = [
      { pop: "ORD", server_region: null, metro: null, w_lat_total: 30, w_p95: 250 },
      { pop: null, server_region: "US-Central", metro: null, w_lat_total: 30, w_p95: 250 },
    ];

    const { rows, source, meta } = rollupInsightsBaseline(covered, {
      city: cityRows,
      dimension: [],
      asn: [],
      geo: geoRows,
      scalar: [],
    });

    expect(source.deferred.has("city_latency_regressions")).toBe(false);
    expect(source.deferred.has("pop_latency_regression")).toBe(false);
    expect(meta.approximate).toEqual(expect.arrayContaining(["city_latency_regressions", "pop_latency_regression"]));

    const env = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["city", "region", "country", "pop", "elapsed"]),
      availableHistoryHours: 1,
      cityAggregateRows: rows.city,
      dimensionAggregateRows: rows.dimension,
      geoLatencyRows: rows.geo,
      windowTotalRequests: 40,
      baselineTotalRequests: 120,
      baselineSource: source,
    });
    expect(env.insights.find((c) => c.id === "city_latency_regressions")!.items).toMatchObject([
      { label: "Springfield, IL, US", baseline_label: "≈ baseline" },
    ]);
    expect(env.insights.find((c) => c.id === "pop_latency_regression")!.items).toMatchObject([
      { label: "ORD", baseline_label: "≈ baseline" },
    ]);
  });

  it("asn_hosting_shift from ins_asn", async () => {
    const base = await baselineRollup();
    const insAsnMap = new Map([
      ["15169", { ...emptyAgg(), requests: 100, count: 5 }],
    ]);
    const covered: RollupResult = {
      ...base,
      rollup: {
        ...base.rollup,
        groups: new Map([["ins_asn", new Map([["asn", insAsnMap]])]]),
        present: new Set([...(base.rollup.present ?? []), "ins_asn/asn"]),
      },
    };
    const asnRows = [
      { asn: 15169, w_cnt: 80, w_proxy_total: 60, w_hosting: 48 },
    ];

    const { rows, source, meta } = rollupInsightsBaseline(covered, {
      city: [],
      dimension: [],
      asn: asnRows,
      scalar: [],
    });

    expect(source.deferred.has("asn_hosting_shift")).toBe(false);
    expect(meta.approximate).not.toContain("asn_hosting_shift");

    const env = await handleInsights(body, {
      now: new Date(T1),
      availableColumns: new Set(["asn", "p_type"]),
      availableHistoryHours: 1,
      asnAggregateRows: rows.asn,
      dimensionAggregateRows: rows.dimension,
      windowTotalRequests: 80,
      baselineTotalRequests: 100,
      baselineSource: source,
    });
    expect(env.insights.find((c) => c.id === "asn_hosting_shift")!.items).toMatchObject([
      { label: "AS15169", current_val: 80, baseline_val: 5 },
    ]);
  });

  it("keeps urls and ins_lat elapsed histograms clone-safe across structuredClone", async () => {
    const base = await baselineRollup();
    const h = new Histogram();
    h.min = 50_000;
    h.max = 50_000;
    h.buckets[BOUNDS.findIndex((b) => b > 50_000) - 1] = 25;

    const covered: RollupResult = {
      ...base,
      rollup: {
        ...base.rollup,
        groups: new Map([
          ["ins_url", new Map([["url", new Map([["/slow", { ...emptyAgg(), requests: 25 }]])]])],
          ["urls", new Map([["url", new Map([["/slow", { ...emptyAgg(), elapsedCount: 25, hists: { ...emptyAgg().hists, elapsed: h } }]])]])],
          ["ins_lat", new Map([
            ["city", new Map([[JSON.stringify(["Springfield", "IL", "US"]), { ...emptyAgg(), elapsedCount: 25, hists: { ...emptyAgg().hists, elapsed: h } }]])],
            ["pop", new Map([["ORD", { ...emptyAgg(), elapsedCount: 25, hists: { ...emptyAgg().hists, elapsed: h } }]])],
          ])],
        ]),
        present: new Set([...(base.rollup.present ?? []), "ins_url/url", "urls/url", "ins_lat/city", "ins_lat/pop"]),
      },
    };
    const cut = insightsBaselineRollup(covered);
    expect([...cut.rollup.fields.keys()].every((k) => (INSIGHTS_RESIDUAL_FIELDS as readonly string[]).includes(k))).toBe(true);
    expect(cut.rollup.fields.has("status")).toBe(false);
    const cloned = structuredClone(cut);
    expect(cloned).toStrictEqual(cut);

    const rows = {
      url: [{ url: "/slow", w_total: 10, w_5xx: 0, w_hits: 0, w_miss: 0, w_pass: 0, w_lat_total: 10 }],
      city: [{ city: "Springfield", region: "IL", country: "US", w_cnt: 10, w_errors_4xx: 0 }],
      dimension: [],
      asn: [],
      geo: [{ pop: "ORD", w_lat_total: 10 }],
      scalar: [],
    };
    const fromCloned = rollupInsightsBaseline(cloned, rows);
    const fromFull = rollupInsightsBaseline(covered, rows);
    expect(fromCloned).toStrictEqual(fromFull);
    expect(fromCloned.rows.url?.[0].b_p95).toBeGreaterThan(0);
    expect(fromCloned.rows.city[0].b_p95).toBeGreaterThan(0);
    expect(fromCloned.rows.geo?.[0].b_p95).toBeGreaterThan(0);
  });

  it("matches auth paths ASCII case-insensitively, as the summaries' ins_ip does", async () => {
    const rows = await read.executeSql(`WITH t AS (SELECT * FROM (VALUES
      ('9.9.9.9', '/TOKEN', 401, TIMESTAMPTZ '2030-01-01 00:00:00+00'),
      ('9.9.9.9', '/to\u{212A}en', 401, TIMESTAMPTZ '2030-01-01 00:00:00+00'),
      ('9.9.9.9', '/\u{17F}ession', 403, TIMESTAMPTZ '2030-01-01 00:00:00+00'),
      ('9.9.9.9', '/Sign-In', 403, TIMESTAMPTZ '2030-01-01 00:00:00+00'),
      ('9.9.9.9', '/x', 200, TIMESTAMPTZ '2030-01-01 00:00:00+00')
    ) v(ip, url, status, timestamp))
    SELECT * FROM (${_coalesced_ip_security_aggregates("t", "2029-12-31T00:00:00Z")})`, [], signal);
    expect(rows.map((r) => Number(r.ce_w_denied))).toEqual([2]);
  });

  it("serves from summaries only the detectors the log field settings enable, as the raw path does", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    settings.off = new Set(["url"]);
    try {
      const src = fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(fixtureFiles("window"))]);
      const [raw, rolled] = await Promise.all([
        queryCloudInsights(body, { signal, maskIps: false, filters: [], engine: read }),
        insightsFromRollup(withSettings(body), [], src, signal),
      ]);
      expect(raw.insights.map((c) => c.id)).not.toContain("error_spikes");
      expect(rolled!.insights.map((c) => c.id)).toEqual(raw.insights.map((c) => c.id));
    } finally {
      settings.off = new Set();
      vi.useRealTimers();
    }
  });

  it("answers window and baseline from v3 summaries without raw Parquet, deferring only IP detectors", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    try {
      const src = fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(fixtureFiles("window"))]);
      const [raw, rolled] = await Promise.all([
        queryCloudInsights(body, { signal, maskIps: false, filters: [], engine: read }),
        insightsFromRollup(withSettings(body), [], src, signal),
      ]);
      expect(rolled).not.toBeNull();
      expect(rolled!.rollup).toMatchObject({
        data_from: new Date(T0).toISOString(),
        data_through: new Date(T1).toISOString(),
      });
      expect(rolled!.window_total_requests).toBe(raw.window_total_requests);
      expect(rolled!.deferred_detectors).toEqual(["credential_enumeration", "content_discovery"]);
      expect(rolled!.insights.map((c) => c.id)).toEqual(raw.insights.map((c) => c.id));
      expect(rolled!.unsupported_detectors).toEqual(raw.unsupported_detectors);

      const strip = (items: { baseline_val: number; baseline_label?: string }[], dropBaseline: boolean) =>
        items.map(({ baseline_label: _, baseline_val, ...rest }) => (dropBaseline ? rest : { ...rest, baseline_val }));
      for (const card of rolled!.insights) {
        const rawCard = raw.insights.find((c) => c.id === card.id)!;
        if (rolled!.deferred_detectors.includes(card.id)) {
          expect(card, card.id).toMatchObject({ severity: "info", summary: "Computing baseline…", items: [], placeholder: true });
          continue;
        }
        const approximate = rolled!.rollup.approximate.includes(card.id);
        if (card.id === "city_error_spikes") {
          // dims/city merges regions and counts status 0 as an error.
          expect(["warning", "critical"]).toContain(card.severity);
          expect(card.items.length).toBeGreaterThan(0);
          continue;
        }
        expect(card.severity, card.id).toBe(rawCard.severity);
        expect(strip(card.items, approximate), card.id).toEqual(strip(rawCard.items, approximate));
      }

      // Filters or explicit detectors must return null so raw DuckDB handles them.
      expect(await insightsFromRollup(body, [{ field: "country", mode: "include", value: "US" }], src, signal)).toBeNull();
      expect(await insightsFromRollup({ ...body, detectors: ["error_spikes"] }, [], src, signal)).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("answers an end inside a summarized window by rolling up the edge raw files, matching raw", async () => {
    rangedReadsOnly();
    const end = T1 - 7 * 60_000;
    const cut = { ...body, clamp_end: new Date(end).toISOString() };
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    try {
      const plain = fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(fixtureFiles("window"))]);
      const src = projectingSource(
        fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(fixtureFiles("window"))]),
        BUFFERS,
      );
      const [raw, rolled] = await Promise.all([
        queryCloudInsights(cut, { signal, maskIps: false, filters: [], engine: read }),
        insightsFromRollup(withSettings(cut), [], src, signal),
      ]);
      expect(plain.fetchRange).toBeUndefined();
      expect(rolled).not.toBeNull();
      expect(rolled!.rollup).toMatchObject({
        // The baseline starts before the fixtures' first row.
        data_from: new Date(T0).toISOString(),
        data_through: new Date(end).toISOString(),
      });
      expect(rolled!.window_total_requests).toBe(raw.window_total_requests);
      expect(src.ranges.length).toBeGreaterThan(0);
      expect(src.ranges.length).toBeLessThanOrEqual(3);
      for (const card of rolled!.insights) {
        if (rolled!.deferred_detectors.includes(card.id) || card.id === "city_error_spikes") continue;
        const rawCard = raw.insights.find((c) => c.id === card.id)!;
        const approximate = rolled!.rollup.approximate.includes(card.id);
        expect(card.severity, card.id).toBe(rawCard.severity);
        expect(
          card.items.map(({ baseline_label: _, baseline_val, ...rest }) => (approximate ? rest : { ...rest, baseline_val })),
          card.id,
        ).toEqual(rawCard.items.map(({ baseline_label: _, baseline_val, ...rest }) => (approximate ? rest : { ...rest, baseline_val })));
      }
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("IP detectors from the window's ins_ip", () => {
  const ipRollup = (ips: [string, number, number][], extra: Partial<Rollup> = {}): Rollup => ({
    dims: new Map(),
    series: new Map(),
    seriesStepMs: 300_000,
    fields: new Map(),
    truncated: new Set(),
    groups: new Map([
      ["ins_ip", new Map([["ip", new Map(ips.map(([ip, errors, count]) => [ip, { ...emptyAgg(), requests: errors + count, errors, count }]))]])],
      ["ins_url", new Map([["url", new Map([["/x", { ...emptyAgg(), requests: 1 }]])]])],
    ]),
    present: new Set(["ins_ip/ip", "ins_url/url"]),
    ...extra,
  });

  const IP_IDS = ["content_discovery", "credential_enumeration"];
  const ruledOut = (window: Rollup, baseline: Rollup = ipRollup([])) => [...ipDetectorsRuledOut(window, baseline, 1, 1)].sort();
  const withIpCounts = (rollup: Rollup, counts: [string, number][]): Rollup =>
    ({ ...rollup, fields: new Map([["ip", new Map(counts)]]) });

  it("rules both out only when every IP's denials and 404s are under the 20 either needs", () => {
    expect(ruledOut(ipRollup([["1.1.1.1", 19, 19], ["2.2.2.2", 0, 3]]))).toEqual(IP_IDS);
    expect(ruledOut(ipRollup([["1.1.1.1", 20, 0]]))).toEqual(["content_discovery"]);
    expect(ruledOut(ipRollup([["1.1.1.1", 0, 20]]))).toEqual(["credential_enumeration"]);
  });

  it("rules content discovery out when an IP's window requests keep its 404s under 70%", () => {
    // 20 of at least 29 requests is 69%; of 28, 71%.
    expect(ruledOut(withIpCounts(ipRollup([["1.1.1.1", 0, 20]]), [["1.1.1.1", 29]]))).toEqual(IP_IDS);
    expect(ruledOut(withIpCounts(ipRollup([["1.1.1.1", 0, 20]]), [["1.1.1.1", 28]]))).toEqual(["credential_enumeration"]);
    expect(ruledOut(withIpCounts(ipRollup([["1.1.1.1", 0, 20]]), [["9.9.9.9", 1000]]))).toEqual(["credential_enumeration"]);
  });

  it("rules credential enumeration out when an IP's baseline denials keep its spike under 3x + 5", () => {
    // 20 denials against a baseline of 5 is not above 5 * 3 + 5; against 4 it is.
    expect(ruledOut(ipRollup([["1.1.1.1", 20, 0]]), ipRollup([["1.1.1.1", 5, 0]]))).toEqual(IP_IDS);
    expect(ruledOut(ipRollup([["1.1.1.1", 20, 0]]), ipRollup([["1.1.1.1", 4, 0]]))).toEqual(["content_discovery"]);
    expect([...ipDetectorsRuledOut(ipRollup([["1.1.1.1", 20, 0]]), ipRollup([["1.1.1.1", 5, 0]]), 1, 2)])
      .toEqual(["content_discovery"]);
  });

  it("rules both out over an uncut empty ins_ip once all_fields shows ip values", () => {
    // Most windows have no 404 or auth-path denial at all: no IP can reach 20.
    expect(ruledOut(withIpCounts(ipRollup([]), [["1.1.1.1", 500]]))).toEqual(IP_IDS);
    expect(ruledOut(withIpCounts(ipRollup([], { truncated: new Set(["ins_ip/ip"]) }), [["1.1.1.1", 500]]))).toEqual([]);
    expect(ruledOut(withIpCounts(ipRollup([], { present: new Set(["ins_url/url"]) }), [["1.1.1.1", 500]]))).toEqual([]);
    expect(ruledOut(withIpCounts(ipRollup([]), []))).toEqual([]);
  });

  it("never rules out over cut, missing or empty ins_ip, which can't show the ip column exists", () => {
    expect(ruledOut(ipRollup([["1.1.1.1", 1, 1]], { truncated: new Set(["ins_ip/ip"]) }))).toEqual([]);
    expect(ruledOut(ipRollup([["1.1.1.1", 1, 1]], { present: new Set(["ins_url/url"]) }))).toEqual([]);
    expect(ruledOut(ipRollup([]))).toEqual([]);
    expect(ruledOut(ipRollup([["1.1.1.1", 1, 1]], { groups: new Map([["ins_ip", new Map([["ip", new Map([["1.1.1.1", { ...emptyAgg(), requests: 2, errors: 1, count: 1 }]])]])]]) }))).toEqual([]);
  });

  // One raw file per hour and no summaries, so both ranges come from the raw residual.
  const END = Date.UTC(2030, 0, 1);
  const rawFile = (fromMs: number, n404: number, n200 = 0) => {
    const rows = [
      ...Array.from({ length: n404 }, (_, i) => ["7.7.7.7", 404, `/probe/${i}`] as const),
      ...Array.from({ length: n200 }, () => ["7.7.7.7", 200, "/"] as const),
      ...Array.from({ length: 30 }, () => ["8.8.8.8", 200, "/"] as const),
    ];
    return parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: rows.map((_, i) => new Date(fromMs + i * 1000)) },
        { name: "ip", type: "STRING", data: rows.map((r) => r[0]) },
        { name: "status", type: "INT64", data: rows.map((r) => BigInt(r[1])) },
        { name: "url", type: "STRING", data: rows.map((r) => r[2]) },
      ],
    });
  };
  const answer = async (n404: number, n200 = 0) => {
    const files = [END - 2 * HOUR, END - HOUR].map((fromMs, i) => ({
      key: `raw-${i}`,
      start: new Date(fromMs).toISOString(),
      end: new Date(fromMs + HOUR).toISOString(),
      size: 1,
      rows: 1,
      bytes: rawFile(fromMs, i ? n404 : 0, i ? n200 : 0),
    }));
    const page = { files: files.map(({ bytes: _, ...f }) => f), next_cursor: null, complete: true };
    const src = fixtureSource([page], Object.fromEntries(files.map((f) => [f.key, f.bytes])), { json: [], files: [] }, []);
    vi.useFakeTimers({ toFake: ["Date"], now: END });
    try {
      return (await insightsFromRollup(withSettings({ ...body, clamp_end: new Date(END).toISOString() }), [], src, signal))!;
    } finally {
      vi.useRealTimers();
    }
  };

  it.each([19, 0])("evaluates them clean without the raw follow-up when no IP reaches 20 (%i 404s)", async (n404) => {
    const result = await answer(n404);
    expect(result.deferred_detectors).toEqual([]);
    for (const id of ["credential_enumeration", "content_discovery"]) {
      const card = result.insights.find((c) => c.id === id)!;
      expect(card.placeholder, id).toBeFalsy();
      expect(card.items, id).toEqual([]);
    }
  });

  it("defers only content discovery once an IP reaches 20 404s that are 70% of its requests", async () => {
    expect((await answer(20)).deferred_detectors).toEqual(["content_discovery"]);
    expect((await answer(20, 8)).deferred_detectors).toEqual(["content_discovery"]);
  });

  it("skips the follow-up when the IP's other window requests keep its 404s under 70%", async () => {
    const result = await answer(20, 9);
    expect(result.deferred_detectors).toEqual([]);
    expect(result.insights.find((c) => c.id === "content_discovery")!.placeholder).toBeFalsy();
  });
});

describe("INSIGHTS_SUMMARY_SPEC column scoping and multi-hour window alignment", () => {
  it("omits series5 and scopes expensive columns in INSIGHTS_SUMMARY_SPEC to their owning groups", () => {
    expect(INSIGHTS_SUMMARY_SPEC.groups.has("series5")).toBe(false);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("requests") ?? [])].sort()).toEqual(["dims", "ins_asn", "ins_url"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("errors") ?? [])].sort()).toEqual(["dims", "ins_ip"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("elapsed_hist") ?? [])].sort()).toEqual(["ins_lat", "urls"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("elapsed_count") ?? [])].sort()).toEqual(["ins_lat", "urls"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("status_5xx") ?? [])]).toEqual(["ins_url"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("hit_requests") ?? [])]).toEqual(["ins_url"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("miss_requests") ?? [])]).toEqual(["ins_url"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("pass_requests") ?? [])]).toEqual(["ins_url"]);
    expect([...(INSIGHTS_SUMMARY_SPEC.only.get("count") ?? [])].sort()).toEqual(["all_fields", "ins_asn", "ins_ip"]);
  });

  const makeHourlyFixture = (alignedEnd: number, extraFiles: { fromMs: number; toMs: number; key: string }[] = []) => {
    const summaryFiles: { key: string; start: string; end: string; size: number; rows: number }[] = [];
    const bufs: Record<string, ArrayBuffer> = {};
    const addFile = (key: string, fromMs: number, toMs: number) => {
      const raw = parquetWriteBuffer({
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: [new Date(fromMs + 60_000), new Date(fromMs + 120_000)] },
          { name: "status", type: "INT64", data: [BigInt(200), BigInt(200)] },
          { name: "url", type: "STRING", data: ["/ok", "/ok"] },
          { name: "ip", type: "STRING", data: ["1.1.1.1", "1.1.1.1"] },
        ],
      });
      bufs[key] = raw;
      summaryFiles.push({
        key,
        start: new Date(fromMs).toISOString(),
        end: new Date(toMs).toISOString(),
        size: raw.byteLength,
        rows: 2,
      });
    };
    for (let h = 12; h >= 1; h--) {
      const fromMs = alignedEnd - h * HOUR;
      addFile(`raw-h${h}`, fromMs, fromMs + HOUR);
    }
    for (const f of extraFiles) addFile(f.key, f.fromMs, f.toMs);
    const calls = { json: [] as string[], files: [] as string[] };
    const src = fixtureSource([fixturePage(summaryFiles)], bufs, calls, []);
    return { src, calls };
  };

  it("aligns multi-hour (>=6h) windows to completed UTC hours even when clamp_end is mid-hour", async () => {
    // 12 hours of 1-hour files ending at 2030-01-02T17:00:00Z; adapter passes mid-hour clamp_end (17:32:00Z).
    const ALIGNED_END = Date.UTC(2030, 0, 2, 17, 0, 0);
    const MID_HOUR_NOW = ALIGNED_END + 32 * 60_000;
    const { src, calls } = makeHourlyFixture(ALIGNED_END);
    vi.useFakeTimers({ toFake: ["Date"], now: MID_HOUR_NOW });
    try {
      const result = await insightsFromRollup(
        withSettings({
          ...body,
          clamp_end: new Date(MID_HOUR_NOW).toISOString(),
          window_size_hrs: 6,
          baseline_hours: 6,
          earliest_log_at: new Date(ALIGNED_END - 24 * HOUR).toISOString(),
        }),
        [],
        src,
        signal,
      );
      expect(result).not.toBeNull();
      // Window end must be aligned to 17:00:00Z, NOT 17:32:00Z.
      expect(result!.window_end).toBe(new Date(ALIGNED_END).toISOString());
      expect(result!.window_start).toBe(new Date(ALIGNED_END - 6 * HOUR).toISOString());
      expect(result!.window_total_requests).toBe(12);
      // No manifest query should have asked for the open hour (17:32:00Z).
      expect(calls.json.some((u) => u.includes("17%3A32") || u.includes("17:32"))).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves exact mid-hour window when end_time is explicitly requested", async () => {
    const ALIGNED_END = Date.UTC(2030, 0, 2, 17, 0, 0);
    const MID_HOUR_END = ALIGNED_END + 32 * 60_000;
    const { src, calls } = makeHourlyFixture(ALIGNED_END, [
      { key: "raw-open", fromMs: ALIGNED_END, toMs: MID_HOUR_END },
    ]);
    vi.useFakeTimers({ toFake: ["Date"], now: MID_HOUR_END });
    try {
      const result = await insightsFromRollup(
        withSettings({
          ...body,
          clamp_end: undefined,
          end_time: new Date(MID_HOUR_END).toISOString(),
          window_size_hrs: 6,
          baseline_hours: 6,
          earliest_log_at: new Date(ALIGNED_END - 24 * HOUR).toISOString(),
        }),
        [],
        src,
        signal,
      );
      expect(result).not.toBeNull();
      expect(result!.window_end).toBe(new Date(MID_HOUR_END).toISOString());
      expect(result!.window_start).toBe(new Date(MID_HOUR_END - 6 * HOUR).toISOString());
      expect(calls.json.some((u) => u.includes("17%3A32") || u.includes("17:32"))).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("lists baseline and current window in a single superset /api/files request", async () => {
    const ALIGNED_END = Date.UTC(2030, 0, 2, 17, 0, 0);
    const MID_HOUR_NOW = ALIGNED_END + 32 * 60_000;
    const { src, calls } = makeHourlyFixture(ALIGNED_END);
    vi.useFakeTimers({ toFake: ["Date"], now: MID_HOUR_NOW });
    try {
      const result = await insightsFromRollup(
        withSettings({
          ...body,
          clamp_end: new Date(MID_HOUR_NOW).toISOString(),
          window_size_hrs: 6,
          baseline_hours: 6,
          earliest_log_at: new Date(ALIGNED_END - 24 * HOUR).toISOString(),
        }),
        [],
        src,
        signal,
      );
      expect(result).not.toBeNull();
      const fileCalls = calls.json.filter((u) => u.startsWith("/api/files"));
      expect(fileCalls).toHaveLength(1);
      const params = new URL(fileCalls[0], "http://localhost").searchParams;
      expect(params.get("from")).toBe(new Date(ALIGNED_END - 12 * HOUR).toISOString());
      expect(params.get("to")).toBe(new Date(ALIGNED_END).toISOString());
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("insightsFromRollup summary-first multi-day manifest handling", () => {
  const DAY = 24 * HOUR;
  const sampleDayBuf = BUFFERS[fixtureFiles("day")[0].key];
  const sampleHourBuf = BUFFERS[fixtureFiles("hour", (e) => e.sources === 4)[0].key];

  function makeMultiDayServer(options: {
    endMs: number;
    dayCount?: number;
    missingLastHourBundle?: boolean;
    missingHourStartsMs?: number[];
    omitInlineSummaries?: boolean;
    rawTruncatedOverMs?: number;
    summaryIncomplete?: boolean;
    tailRawIncomplete?: boolean;
  }) {
    const dayCount = options.dayCount ?? 8;
    const startMs = options.endMs - dayCount * DAY;
    const dayBundles: { key: string; start: string; end: string; size: number; rows: number; kind: "day"; sources: number }[] = [];
    const hourBundles: { key: string; start: string; end: string; size: number; rows: number; kind: "hour"; sources: number }[] = [];
    const bufs: Record<string, ArrayBuffer> = {};
    const tailStartMs = options.endMs - HOUR;
    const missingHours = new Set(options.missingHourStartsMs ?? []);
    if (options.missingLastHourBundle) missingHours.add(tailStartMs);

    const dayHasMissingHour = (dayStart: number) => {
      for (const h of missingHours) {
        if (h >= dayStart && h < dayStart + DAY) return true;
      }
      return false;
    };

    const firstDay = Math.floor(startMs / DAY) * DAY;
    const lastDay = Math.floor((options.endMs - 1) / DAY) * DAY;
    const rawFiles: { key: string; start: string; end: string; size: number; rows: number }[] = [];
    for (let d = firstDay; d <= lastDay; d += DAY) {
      const dt = new Date(d);
      const y = dt.getUTCFullYear();
      const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
      const day = String(dt.getUTCDate()).padStart(2, "0");
      const dayKey = `processed/summary_bundle/request/year=${y}/month=${m}/day=${day}/day_bundle=c24-n1600-h0000000000000001.parquet`;
      dayBundles.push({
        key: dayKey,
        start: new Date(d).toISOString(),
        end: new Date(d + DAY).toISOString(),
        size: sampleDayBuf.byteLength,
        rows: 1600,
        kind: "day",
        sources: 24,
      });
      bufs[dayKey] = sampleDayBuf;
      for (let h = 0; h < 24; h++) {
        const hStart = d + h * HOUR;
        if (missingHours.has(hStart)) {
          const rawKey = `processed/request/raw-${hStart}.parquet`;
          const rawBuf = parquetWriteBuffer({
            columnData: [
              { name: "timestamp", type: "TIMESTAMP", data: [new Date(hStart + 60_000), new Date(hStart + 120_000)] },
              { name: "status", type: "INT64", data: [BigInt(200), BigInt(200)] },
              { name: "url", type: "STRING", data: ["/ok", "/ok"] },
              { name: "ip", type: "STRING", data: ["1.1.1.1", "1.1.1.1"] },
            ],
          });
          bufs[rawKey] = rawBuf;
          rawFiles.push({
            key: rawKey,
            start: new Date(hStart).toISOString(),
            end: new Date(hStart + HOUR).toISOString(),
            size: rawBuf.byteLength,
            rows: 2,
          });
          continue;
        }
        const hh = String(h).padStart(2, "0");
        const hKey = `processed/summary_bundle/request/year=${y}/month=${m}/day=${day}/hour_bundle=${hh}-c4-n800-h0000000000000002.parquet`;
        hourBundles.push({
          key: hKey,
          start: new Date(hStart).toISOString(),
          end: new Date(hStart + HOUR).toISOString(),
          size: sampleHourBuf.byteLength,
          rows: 800,
          kind: "hour",
          sources: 4,
        });
        bufs[hKey] = sampleHourBuf;
      }
    }

    const rawCalls: string[] = [];
    const summaryCalls: string[] = [];

    // Simulates compute/cloud discovery.rs `enter_day`: emits a day_bundle whenever
    // the full UTC day fits inside [from, to], otherwise emits hour_bundles for that day.
    const summariesForRange = (fromMs: number, toMs: number) => {
      const coveredDays = new Set<number>();
      const out = [];
      for (const db of dayBundles) {
        const s = Date.parse(db.start);
        const e = Date.parse(db.end);
        if (s >= fromMs && e <= toMs && !dayHasMissingHour(s)) {
          out.push(db);
          coveredDays.add(s);
        }
      }
      for (const hb of hourBundles) {
        const s = Date.parse(hb.start);
        const e = Date.parse(hb.end);
        const dayStart = Math.floor(s / DAY) * DAY;
        if (coveredDays.has(dayStart)) continue;
        if (e > fromMs && s < toMs) out.push(hb);
      }
      return out;
    };

    const src = {
      cacheScope: `multi-day-${Math.random().toString(36).slice(2)}`,
      async fetchJson(url: string) {
        const params = new URL(url, "http://x").searchParams;
        const fromMs = Date.parse(params.get("from")!);
        const toMs = Date.parse(params.get("to")!);
        if (params.get("view") === "summary") {
          summaryCalls.push(url);
          if (options.summaryIncomplete) {
            return { files: [], next_cursor: null, complete: false };
          }
          return {
            files: summariesForRange(fromMs, toMs),
            next_cursor: null,
            complete: true,
          };
        }
        rawCalls.push(url);
        if (options.rawTruncatedOverMs !== undefined && toMs - fromMs > options.rawTruncatedOverMs) {
          return {
            files: [],
            ...(!options.omitInlineSummaries && params.get("summaries") === "1"
              ? { summary_files: summariesForRange(fromMs, toMs) }
              : {}),
            next_cursor: null,
            complete: false,
          };
        }
        if (options.tailRawIncomplete && toMs > tailStartMs) {
          return { files: [], next_cursor: null, complete: false };
        }
        const files = rawFiles.filter(
          (f) => Date.parse(f.end) > fromMs && Date.parse(f.start) < toMs,
        );
        return {
          files,
          ...(!options.omitInlineSummaries && params.get("summaries") === "1"
            ? { summary_files: summariesForRange(fromMs, toMs) }
            : {}),
          next_cursor: null,
          complete: true,
        };
      },
      async fetchFile(key: string) {
        return { bytes: bufs[key], cached: false };
      },
    };
    return { src, rawCalls, summaryCalls };
  }

  it("answers 8-day insights from view=summary without listing 8 days of raw files when raw would truncate", async () => {
    const END = Date.UTC(2025, 0, 9, 0, 0, 0);
    const { src, rawCalls, summaryCalls } = makeMultiDayServer({
      endMs: END,
      rawTruncatedOverMs: 2 * HOUR,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).not.toBeNull();
    expect(result!.window_total_requests).toBeGreaterThan(0);
    // Single view=summary call for [fromStart, toEnd] when fromWindow is on UTC midnight.
    expect(summaryCalls).toHaveLength(1);
    // Zero raw /api/files calls when all hours are covered by complete bundles.
    expect(rawCalls).toHaveLength(0);
  });

  it("handles mid-day window/baseline split and fetches raw only for the uncovered residual tail", async () => {
    const END = Date.UTC(2025, 0, 9, 3, 0, 0);
    const { src, rawCalls, summaryCalls } = makeMultiDayServer({
      endMs: END,
      missingLastHourBundle: true,
      rawTruncatedOverMs: 2 * HOUR,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).not.toBeNull();
    expect(result!.window_total_requests).toBeGreaterThan(0);
    // Splits [fromStart, fromWindow] and [fromWindow, toEnd] so the split day gets hour_bundles.
    expect(summaryCalls).toHaveLength(2);
    // Only fetches raw for the single uncovered tail hour [02:00Z, 03:00Z].
    expect(rawCalls).toHaveLength(1);
    const rawParams = new URL(rawCalls[0], "http://x").searchParams;
    expect(rawParams.get("from")).toBe(new Date(END - HOUR).toISOString());
    expect(rawParams.get("to")).toBe(new Date(END).toISOString());
  });

  it("falls back cleanly to null when the uncovered residual tail raw listing is truncated", async () => {
    const END = Date.UTC(2025, 0, 9, 3, 0, 0);
    const { src } = makeMultiDayServer({
      endMs: END,
      missingLastHourBundle: true,
      tailRawIncomplete: true,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).toBeNull();
  });

  it("falls back cleanly to null when view=summary is incomplete", async () => {
    const END = Date.UTC(2025, 0, 9, 0, 0, 0);
    const { src, rawCalls } = makeMultiDayServer({
      endMs: END,
      summaryIncomplete: true,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).toBeNull();
    expect(rawCalls).toHaveLength(0);
  });

  it("coalesces scattered uncovered intervals within 24h spans without splitting contiguous ranges", () => {
    const base = Date.UTC(2025, 0, 2, 0, 0, 0);
    const groups = coalesceUncoveredIntervals([
      { startMs: base + 1 * HOUR, endMs: base + 2 * HOUR },
      { startMs: base + 5 * HOUR, endMs: base + 6 * HOUR },
      { startMs: base + 22 * HOUR, endMs: base + 23 * HOUR },
      // 3 days later: two gaps within 12h
      { startMs: base + 3 * DAY + 2 * HOUR, endMs: base + 3 * DAY + 3 * HOUR },
      { startMs: base + 3 * DAY + 13 * HOUR, endMs: base + 3 * DAY + 14 * HOUR },
    ]);
    expect(groups).toHaveLength(2);
    expect(groups[0]).toEqual({
      startMs: base + 1 * HOUR,
      endMs: base + 23 * HOUR,
      intervals: [
        { startMs: base + 1 * HOUR, endMs: base + 2 * HOUR },
        { startMs: base + 5 * HOUR, endMs: base + 6 * HOUR },
        { startMs: base + 22 * HOUR, endMs: base + 23 * HOUR },
      ],
    });
    expect(groups[1]).toEqual({
      startMs: base + 3 * DAY + 2 * HOUR,
      endMs: base + 3 * DAY + 14 * HOUR,
      intervals: [
        { startMs: base + 3 * DAY + 2 * HOUR, endMs: base + 3 * DAY + 3 * HOUR },
        { startMs: base + 3 * DAY + 13 * HOUR, endMs: base + 3 * DAY + 14 * HOUR },
      ],
    });
    // A single contiguous 8-day uncovered range stays intact as 1 group.
    const single = coalesceUncoveredIntervals([{ startMs: base, endMs: base + 8 * DAY }]);
    expect(single).toHaveLength(1);
    expect(single[0].endMs - single[0].startMs).toBe(8 * DAY);
  });

  it("coalesces scattered uncovered gaps across an 8-day window into bounded raw-only listings without re-fetching summaries", async () => {
    const END = Date.UTC(2025, 0, 9, 0, 0, 0);
    const day2 = Date.UTC(2025, 0, 2, 0, 0, 0);
    const day8 = Date.UTC(2025, 0, 8, 0, 0, 0);
    // 9 scattered uncovered hours across 2 days (5 in baseline day2, 4 in window day8).
    const missingHourStartsMs = [
      day2 + 1 * HOUR,
      day2 + 5 * HOUR,
      day2 + 11 * HOUR,
      day2 + 18 * HOUR,
      day2 + 22 * HOUR,
      day8 + 2 * HOUR,
      day8 + 8 * HOUR,
      day8 + 15 * HOUR,
      day8 + 21 * HOUR,
    ];
    const { src, rawCalls, summaryCalls } = makeMultiDayServer({
      endMs: END,
      missingHourStartsMs,
      omitInlineSummaries: true,
      rawTruncatedOverMs: DAY,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).not.toBeNull();
    // 20 covered hour_bundles (each from sampleHourBuf) + 4 uncovered raw hours (2 requests each).
    const hourReq = (await decodeRollup(sampleHourBuf, 300_000))!.dims.get("proto")!;
    let perHourReqs = 0;
    for (const agg of hourReq.values()) perHourReqs += agg.requests;
    expect(result!.window_total_requests).toBe(20 * perHourReqs + 4 * 2);

    // Only 1 view=summary call for the whole 8-day window; never re-fetched for uncovered gaps.
    expect(summaryCalls).toHaveLength(1);
    // 9 scattered gaps coalesced into 2 bounded (<=24h) raw listings (3 /api/files calls total).
    expect(rawCalls).toHaveLength(2);
    for (const url of rawCalls) {
      const p = new URL(url, "http://x").searchParams;
      expect(p.get("view")).toBeNull();
      expect(p.get("summaries")).toBeNull();
      expect(Date.parse(p.get("to")!) - Date.parse(p.get("from")!)).toBeLessThanOrEqual(DAY);
    }
    const ranges = rawCalls.map((url) => {
      const p = new URL(url, "http://x").searchParams;
      return [p.get("from"), p.get("to")];
    }).sort();
    expect(ranges).toEqual([
      [new Date(day2 + 1 * HOUR).toISOString(), new Date(day2 + 23 * HOUR).toISOString()],
      [new Date(day8 + 2 * HOUR).toISOString(), new Date(day8 + 22 * HOUR).toISOString()],
    ]);
  });

  it("falls back to individual gap listings when a coalesced span truncates, and to null if a single gap truncates", async () => {
    const END = Date.UTC(2025, 0, 9, 0, 0, 0);
    const day8 = Date.UTC(2025, 0, 8, 0, 0, 0);
    // Two gaps 14h apart: coalesced 14h span exceeds rawTruncatedOverMs (2h), individual 1h spans succeed.
    const { src, rawCalls, summaryCalls } = makeMultiDayServer({
      endMs: END,
      missingHourStartsMs: [day8 + 2 * HOUR, day8 + 15 * HOUR],
      omitInlineSummaries: true,
      rawTruncatedOverMs: 2 * HOUR,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).not.toBeNull();
    expect(summaryCalls).toHaveLength(1);
    // 1 coalesced attempt (truncated) + 2 individual 1h fallbacks = 3 raw calls.
    expect(rawCalls).toHaveLength(3);

    // If even a 1h gap truncates, falls back cleanly to null.
    const { src: truncSrc } = makeMultiDayServer({
      endMs: END,
      missingHourStartsMs: [day8 + 2 * HOUR, day8 + 15 * HOUR],
      omitInlineSummaries: true,
      rawTruncatedOverMs: 30 * 60_000,
    });
    const truncResult = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 168,
        earliest_log_at: new Date(END - 14 * DAY).toISOString(),
      }),
      [],
      truncSrc,
      signal,
    );
    expect(truncResult).toBeNull();
  });

  it("clips uncovered pre-history before earliest_log_at so raw listings do not bridge bundled hours", async () => {
    const END = Date.UTC(2025, 0, 4, 0, 0, 0);
    // Data only exists from Jan 2 18:00Z onward (30h of history), while baseline looks back 48h to Jan 1 00:00Z.
    const earliestMs = Date.UTC(2025, 0, 2, 18, 0, 0);
    // Hours [Jan 1 00:00Z .. Jan 2 18:00Z) have no bundles (pre-history), and Jan 3 04:00Z is an uncovered gap.
    const preHistoryHours: number[] = [];
    for (let h = Date.UTC(2025, 0, 1, 0, 0, 0); h < earliestMs; h += HOUR) {
      preHistoryHours.push(h);
    }
    const gapHour = Date.UTC(2025, 0, 3, 4, 0, 0);
    const { src, rawCalls } = makeMultiDayServer({
      endMs: END,
      dayCount: 3,
      missingHourStartsMs: [...preHistoryHours, gapHour],
      omitInlineSummaries: true,
      rawTruncatedOverMs: 2 * HOUR,
    });
    const result = await insightsFromRollup(
      withSettings({
        ...body,
        clamp_end: new Date(END).toISOString(),
        window_size_hrs: 24,
        baseline_hours: 48,
        earliest_log_at: new Date(earliestMs + 12_000).toISOString(),
      }),
      [],
      src,
      signal,
    );
    expect(result).not.toBeNull();
    // Only the single 1h uncovered gap after earliest_log_at is listed; pre-history is clipped and never bridged.
    expect(rawCalls).toHaveLength(1);
    const p = new URL(rawCalls[0], "http://x").searchParams;
    expect(p.get("from")).toBe(new Date(gapHour).toISOString());
    expect(p.get("to")).toBe(new Date(gapHour + HOUR).toISOString());
  });

  it("decodeRollup with INSIGHTS_SUMMARY_SPEC skips non-insights dims and all_fields", async () => {
    const full = await decodeRollup(sampleHourBuf, 300_000);
    const filtered = await decodeRollup(sampleHourBuf, 300_000, INSIGHTS_SUMMARY_SPEC);
    expect(full).not.toBeNull();
    expect(filtered).not.toBeNull();
    // Full bundle has more dims than just city/proto, while INSIGHTS_SUMMARY_SPEC keeps only city and proto.
    expect(full!.dims.size).toBeGreaterThan(2);
    expect([...filtered!.dims.keys()].sort()).toEqual(["city", "proto"]);
    for (const key of filtered!.fields.keys()) {
      expect(["country", "p_type", "method", "referer", "ua", "asn", "cache", "transport", "ip"]).toContain(key);
    }
  });
});
