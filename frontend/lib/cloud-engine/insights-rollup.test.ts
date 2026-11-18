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
import { fixtureSource } from "./dashboard/test-fixtures";
import { queryCloudInsights } from "./insight-scans";
import {
  _coalesced_asn_aggregates,
  _coalesced_city_aggregates,
  _coalesced_dim_aggregates,
  _coalesced_scalar_aggregates,
  handleInsights,
} from "./insights";
import { INSIGHTS_DEFERRED, insightsBaselineFromRollup, insightsBaselineRollup, insightsDeferredDetectors, rollupInsightsBaseline } from "./insights-rollup";

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
});
