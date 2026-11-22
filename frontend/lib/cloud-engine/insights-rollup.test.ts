// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import { BOUNDS, Histogram } from "./dashboard/histogram";
import { decodeRollup, emptyAgg, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
import {
  FIXTURE_START,
  SUMMARY_FIXTURE_BUFFERS as BUFFERS,
  fixtureFiles,
  fixturePage,
} from "./dashboard/summary-fixtures";
import { fixtureSource, projectingSource } from "./dashboard/test-fixtures";
import { queryCloudInsights } from "./insight-scans";
import {
  _coalesced_asn_aggregates,
  _coalesced_city_aggregates,
  _coalesced_dim_aggregates,
  _coalesced_scalar_aggregates,
  handleInsights,
} from "./insights";
import { insightsBaselineFromRollup, insightsBaselineRollup, insightsDeferredDetectors, insightsFromRollup, ipDetectorsRuledOut, rollupInsightsBaseline } from "./insights-rollup";
import { parquetWriteBuffer } from "hyparquet-writer";
import { _coalesced_ip_security_aggregates } from "./insights";
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
        expect(item.baseline_label, card.id).toBe(approximate ? "≈ baseline" : undefined);
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

  it("answers window and baseline from v3 summaries without raw Parquet, deferring only IP detectors", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    try {
      const src = fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(fixtureFiles("window"))]);
      const [raw, rolled] = await Promise.all([
        queryCloudInsights(body, { signal, maskIps: false, filters: [], engine: read }),
        insightsFromRollup(body, [], src, signal),
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
        expect(card.severity, card.id).toBe(rawCard.severity);
        if (card.id === "city_error_spikes") {
          // dims/city merges regions and counts status 0 as an error.
          expect(card.items.length).toBeGreaterThan(0);
          continue;
        }
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
        insightsFromRollup(cut, [], src, signal),
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
      return (await insightsFromRollup({ ...body, clamp_end: new Date(END).toISOString() }, [], src, signal))!;
    } finally {
      vi.useRealTimers();
    }
  };

  it("evaluates them clean without the raw follow-up when no IP reaches 20", async () => {
    const result = await answer(19);
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
