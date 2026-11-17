// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import { decodeRollup, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
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
import { INSIGHTS_DEFERRED, insightsBaselineFromRollup, rollupInsightsBaseline } from "./insights-rollup";

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
    const [raw, rolled] = await (async () => {
      try {
        return [
          await queryCloudInsights(body, options),
          await queryCloudInsights(body, { ...options, baseline: await baselineRollup() }),
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
    const deferred = ids.filter((id) => INSIGHTS_DEFERRED.has(id));
    expect(deferred).toEqual(expect.arrayContaining(["content_discovery", "error_spikes"]));
    expect(rolled.deferred_detectors).toEqual(deferred);
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
    expect(meta.approximate.toSorted()).toEqual(["cache_hit_cliff", "city_error_spikes", "city_surges", "new_city_traffic"]);

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
});
