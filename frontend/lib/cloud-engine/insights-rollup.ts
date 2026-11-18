import type { DashboardFilter } from "./dashboard/filters";
import { emptyAgg, queryRollup, type RollupAgg, type RollupResult } from "./dashboard/rollup-query";
import { resolveInsightWindow, type InsightsRequest } from "./insights";
import type { NetworkRollupMeta } from "./network-rollup";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";

/**
 * How an /insights response whose baseline came from v3 summaries differs from
 * the raw scan: the baseline covers only whole raw windows from `data_from`
 * through `data_through` (the window itself is read raw), detectors in
 * `approximate` compare against a baseline the summaries can only estimate,
 * and those in `lower_bound` against counts cut to a summary's top K.
 */
export type InsightsRollupMeta = NetworkRollupMeta;

/** How each detector's baseline stands when it comes from summaries. */
export type InsightsBaselineSource = {
  approximate: ReadonlySet<string>;
  lowerBound: ReadonlySet<string>;
  /** Detectors whose baseline the summaries do not keep; a raw follow-up computes them. */
  deferred: ReadonlySet<string>;
};

/**
 * Detectors that need a breakdown the summaries do not keep (per-URL status,
 * cache state and latency; per-IP auth paths and 404s; per-city and per-PoP
 * latency; per-ASN proxy type). Content discovery is window-only but its
 * baseline 404 count is per IP too.
 */
export const INSIGHTS_DEFERRED: ReadonlySet<string> = new Set([
  "error_spikes", "cache_collapse", "cacheability_regression", "latency_regression",
  "credential_enumeration", "content_discovery", "city_latency_regressions",
  "pop_latency_regression", "asn_hosting_shift",
]);

/** Summaries merge a city's regions and count status 0 as an error. */
const ALWAYS_APPROXIMATE = ["city_surges", "city_error_spikes", "new_city_traffic"];

/** The `all_fields` (or `dims`) counts each detector's baseline reads. */
const BASELINE_FIELDS: Record<string, string> = {
  city_surges: "dims/city",
  new_city_traffic: "dims/city",
  city_error_spikes: "dims/city",
  new_country_traffic: "all_fields/country",
  ua_monoculture: "all_fields/ua",
  referer_monoculture: "all_fields/referer",
  method_drift: "all_fields/method",
  proxy_surge: "all_fields/p_type",
  asn_concentration: "all_fields/asn",
  new_asn_traffic: "all_fields/asn",
  cache_hit_cliff: "all_fields/cache",
  http3_fallback: "all_fields/transport",
};

/** Ratios of two cut counts are estimates rather than bounds. */
const RATIO_DETECTORS = new Set(["cache_hit_cliff", "http3_fallback"]);

const DIMENSIONS = ["country", "p_type", "method", "referer", "ua"] as const;

/** The summary groups and columns the /insights baseline reads. */
export const INSIGHTS_SUMMARY_SPEC = defineSummarySpec({
  groups: ["dims", "series5", "all_fields", "field_totals", "dim_totals"],
  columns: ["count", "requests", "errors"],
});

/**
 * Merges the v3 summaries for an unfiltered /insights baseline, the range
 * before the window, or returns null when the raw scan must cover it (filters,
 * which summaries cannot apply, or summaries that can't answer the range).
 */
export function insightsBaselineFromRollup(
  body: InsightsRequest,
  filters: readonly DashboardFilter[],
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files. */
  spec: SummarySpec | null = INSIGHTS_SUMMARY_SPEC,
): Promise<RollupResult | null> {
  if (filters.length) return Promise.resolve(null);
  const { start, windowStart } = resolveInsightWindow(body);
  return queryRollup(source, { fromMs: start, toMs: windowStart, signal, spec: spec ?? undefined });
}

type Row = Record<string, unknown>;

/** Window-only rows of the coalesced scans whose baseline the summaries can fill. */
export type InsightScanRows = {
  city: Row[];
  dimension: Row[];
  asn: Row[];
  scalar: Row[];
};

/**
 * A baseline rollup cut to what `rollupInsightsBaseline` reads (field counts,
 * city request and error counts, the overall request series and truncation),
 * so the worker that merged it posts a small, plain-data copy to the page.
 */
export function insightsBaselineRollup(result: RollupResult): RollupResult {
  const { rollup } = result;
  const bare = <K>(values: Map<K, RollupAgg>) =>
    new Map([...values].map(([key, agg]) => [key, { ...emptyAgg(), requests: agg.requests, errors: agg.errors }]));
  const cities = rollup.dims.get("city");
  const overall = rollup.series.get("overall")?.get("");
  return {
    ...result,
    rollup: {
      dims: new Map(cities ? [["city", bare(cities)]] : []),
      series: new Map(overall ? [["overall", new Map([["", bare(overall)]])]] : []),
      seriesStepMs: rollup.seriesStepMs,
      fields: rollup.fields,
      truncated: new Set(Object.values(BASELINE_FIELDS).filter((key) => rollup.truncated.has(key))),
    },
  };
}

/**
 * Fills the `b_*` columns of window-only scan rows from summaries, the way the
 * raw scan over window and baseline would have counted them: per city, per
 * dimension value (adding values seen only in the baseline and an unnamed row
 * for the rest, so the rows still sum to every baseline request), per ASN and
 * service-wide cache and transport shares.
 */
export function rollupInsightsBaseline(result: RollupResult, rows: InsightScanRows): {
  rows: InsightScanRows;
  baselineRequests: number;
  source: InsightsBaselineSource;
  meta: InsightsRollupMeta;
} {
  const { rollup } = result;
  const fields = (name: string) => rollup.fields.get(name) ?? new Map<string, number>();
  let baselineRequests = 0;
  for (const agg of rollup.series.get("overall")?.get("")?.values() ?? []) baselineRequests += agg.requests;

  const cities = rollup.dims.get("city");
  const city = rows.city.map((row) => {
    const agg = cities?.get(JSON.stringify([row.country == null ? "Unknown" : String(row.country), String(row.city)]));
    return { ...row, b_cnt: agg?.requests ?? 0, b_errors_4xx: agg?.errors ?? 0 };
  });

  const dimension: Row[] = [];
  for (const key of DIMENSIONS) {
    const counts = fields(key);
    const own = rows.dimension.filter((row) => row.dimension === key);
    const seen = new Set<string>();
    let named = 0;
    for (const row of own) {
      if (row[key] == null) continue;
      const value = String(row[key]);
      seen.add(value);
      dimension.push({ ...row, b_cnt: counts.get(value) ?? 0 });
    }
    for (const [value, n] of counts) {
      named += n;
      if (!seen.has(value)) dimension.push({ dimension: key, [key]: value, w_cnt: 0, b_cnt: n });
    }
    const unnamed = own.find((row) => row[key] == null);
    dimension.push({ ...(unnamed ?? { dimension: key, [key]: null, w_cnt: 0 }), b_cnt: Math.max(0, baselineRequests - named) });
  }

  const asnCounts = fields("asn");
  const asn = rows.asn.map((row) => ({ ...row, b_cnt: row.asn == null ? 0 : asnCounts.get(String(row.asn)) ?? 0 }));

  const sum = (counts: Map<string, number>, pred: (value: string) => boolean) =>
    [...counts].reduce((total, [value, n]) => total + (pred(value) ? n : 0), 0);
  const cache = fields("cache");
  const transport = fields("transport");
  const scalar = rows.scalar.map((row) => ({
    ...row,
    b_quic: sum(transport, (v) => v.toLowerCase() === "quic"),
    b_tr_total: sum(transport, (v) => v !== ""),
    b_hits: sum(cache, (v) => v.startsWith("HIT")),
    b_cacheable: sum(cache, (v) => v.startsWith("HIT") || v.startsWith("MISS")),
  }));

  const approximate = new Set(ALWAYS_APPROXIMATE);
  const lowerBound = new Set<string>();
  for (const [id, group] of Object.entries(BASELINE_FIELDS)) {
    if (!rollup.truncated.has(group) || approximate.has(id)) continue;
    (RATIO_DETECTORS.has(id) ? approximate : lowerBound).add(id);
  }
  return {
    rows: { city, dimension, asn, scalar },
    baselineRequests,
    source: { approximate, lowerBound, deferred: INSIGHTS_DEFERRED },
    meta: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate: [...approximate],
      lower_bound: [...lowerBound],
    },
  };
}
