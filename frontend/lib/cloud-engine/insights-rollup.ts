import type { DashboardFilter } from "./dashboard/filters";
import { Histogram } from "./dashboard/histogram";
import { emptyAgg, queryRollup, rollupGroup, type Rollup, type RollupAgg, type RollupResult } from "./dashboard/rollup-query";
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
 * Detectors that need a breakdown pre-batch-2 summaries do not keep; kept as an
 * empty set now that `ins_url`, `ins_ip`, `ins_lat`, and `ins_asn` cover all
 * baseline detectors (pre-batch-2 ranges still defer via `insightsDeferredDetectors`).
 */
export const INSIGHTS_DEFERRED: ReadonlySet<string> = new Set();

const INS_URL_DETECTORS = [
  "error_spikes", "cache_collapse", "cacheability_regression", "latency_regression",
] as const;
const INS_IP_DETECTORS = ["credential_enumeration", "content_discovery"] as const;

/** Detectors deferred for this rollup: always-deferred ones plus any whose batch-2 group is absent. */
export function insightsDeferredDetectors(rollup: Rollup): ReadonlySet<string> {
  const out = new Set(INSIGHTS_DEFERRED);
  if (!rollupGroup(rollup, "ins_url", "url") || !rollupGroup(rollup, "urls", "url")) {
    for (const id of INS_URL_DETECTORS) out.add(id);
  }
  if (!rollupGroup(rollup, "ins_ip", "ip")) {
    for (const id of INS_IP_DETECTORS) out.add(id);
  }
  if (!rollupGroup(rollup, "ins_lat", "city")) out.add("city_latency_regressions");
  if (!rollupGroup(rollup, "ins_lat", "pop")) out.add("pop_latency_regression");
  if (!rollupGroup(rollup, "ins_asn", "asn")) out.add("asn_hosting_shift");
  return out;
}

/** Summaries merge a city's regions and count status 0 as an error. */
const ALWAYS_APPROXIMATE = ["city_surges", "city_error_spikes", "new_city_traffic"];

/** The `all_fields` (or `dims` / `ins_*`) counts each detector's baseline reads. */
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
  error_spikes: "ins_url/url",
  cache_collapse: "ins_url/url",
  cacheability_regression: "ins_url/url",
  credential_enumeration: "ins_ip/ip",
  content_discovery: "ins_ip/ip",
  city_latency_regressions: "ins_lat/city",
  pop_latency_regression: "ins_lat/pop",
  asn_hosting_shift: "ins_asn/asn",
};

/** Ratios of two cut counts are estimates rather than bounds. */
const RATIO_DETECTORS = new Set([
  "cache_hit_cliff", "http3_fallback", "error_spikes", "cache_collapse", "cacheability_regression",
  "asn_hosting_shift",
]);

const DIMENSIONS = ["country", "p_type", "method", "referer", "ua"] as const;

/** The summary groups and columns the /insights baseline reads. */
export const INSIGHTS_SUMMARY_SPEC = defineSummarySpec({
  groups: ["dims", "series5", "all_fields", "field_totals", "dim_totals", "urls", "ins_url", "ins_ip", "ins_lat", "ins_asn"],
  columns: [
    "count", "requests", "errors",
    "status_5xx", "hit_requests", "miss_requests", "pass_requests",
    "elapsed_count", "elapsed_hist",
  ],
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
  url?: Row[];
  ip?: Row[];
  city: Row[];
  dimension: Row[];
  asn: Row[];
  geo?: Row[];
  scalar: Row[];
};

/**
 * A baseline rollup cut to what `rollupInsightsBaseline` reads (field counts,
 * city request and error counts, the overall request series, `ins_url`/`urls`,
 * `ins_ip` and truncation), so the worker that merged it posts a small,
 * plain-data copy to the page.
 */
export function insightsBaselineRollup(result: RollupResult): RollupResult {
  const { rollup } = result;
  const bare = <K>(values: Map<K, RollupAgg>) =>
    new Map([...values].map(([key, agg]) => [key, { ...emptyAgg(), requests: agg.requests, errors: agg.errors }]));
  const cities = rollup.dims.get("city");
  const overall = rollup.series.get("overall")?.get("");
  const insUrl = rollup.groups?.get("ins_url")?.get("url");
  const urls = rollup.groups?.get("urls")?.get("url");
  const insIp = rollup.groups?.get("ins_ip")?.get("ip");
  const insLatCity = rollup.groups?.get("ins_lat")?.get("city");
  const insLatPop = rollup.groups?.get("ins_lat")?.get("pop");
  const insAsn = rollup.groups?.get("ins_asn")?.get("asn");
  const groups = new Map<string, Map<string, Map<string, RollupAgg>>>();
  if (insUrl) {
    groups.set("ins_url", new Map([["url", new Map([...insUrl].map(([k, a]) => [k, {
      ...emptyAgg(),
      requests: a.requests,
      status5xx: a.status5xx,
      hitRequests: a.hitRequests,
      missRequests: a.missRequests,
      passRequests: a.passRequests,
    }]))]]));
  }
  const copyElapsed = (values: Map<string, RollupAgg>) => new Map([...values].map(([k, a]) => {
    const bytes = a.hists.elapsed?.toBytes();
    return [k, {
      ...emptyAgg(),
      elapsedCount: a.elapsedCount,
      ...(bytes ? { elapsedHist: bytes } : {}),
    }];
  }));
  if (urls) {
    groups.set("urls", new Map([["url", copyElapsed(urls)]]));
  }
  if (insIp) {
    groups.set("ins_ip", new Map([["ip", new Map([...insIp].map(([k, a]) => [k, {
      ...emptyAgg(),
      requests: a.requests,
      errors: a.errors,
      count: a.count,
    }]))]]));
  }
  if (insLatCity || insLatPop) {
    const latFields = new Map<string, Map<string, RollupAgg>>();
    if (insLatCity) latFields.set("city", copyElapsed(insLatCity));
    if (insLatPop) latFields.set("pop", copyElapsed(insLatPop));
    groups.set("ins_lat", latFields);
  }
  if (insAsn) {
    groups.set("ins_asn", new Map([["asn", new Map([...insAsn].map(([k, a]) => [k, {
      ...emptyAgg(),
      requests: a.requests,
      count: a.count,
    }]))]]));
  }
  const present = new Set(
    ["ins_url/url", "urls/url", "ins_ip/ip", "ins_lat/city", "ins_lat/pop", "ins_asn/asn"]
      .filter((key) => rollup.present?.has(key)),
  );
  return {
    ...result,
    rollup: {
      dims: new Map(cities ? [["city", bare(cities)]] : []),
      series: new Map(overall ? [["overall", new Map([["", bare(overall)]])]] : []),
      seriesStepMs: rollup.seriesStepMs,
      fields: rollup.fields,
      groups,
      present,
      truncated: new Set(
        [...Object.values(BASELINE_FIELDS), "urls/url"].filter((key) => rollup.truncated.has(key)),
      ),
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
  const insLatCity = rollupGroup(rollup, "ins_lat", "city");
  const p95Ms = (agg: RollupAgg | undefined) => {
    const hist = agg && agg.elapsedCount > 0
      ? (agg.hists.elapsed ?? (agg.elapsedHist ? Histogram.fromBytes(agg.elapsedHist) : null))
      : null;
    const q95 = hist?.quantile(0.95) ?? null;
    return q95 == null ? null : q95 / 1000;
  };
  const city = rows.city.map((row) => {
    const agg = cities?.get(JSON.stringify([row.country == null ? "Unknown" : String(row.country), String(row.city)]));
    const latAgg = insLatCity?.get(JSON.stringify([String(row.city), row.region ?? null, row.country ?? null]));
    return {
      ...row,
      b_cnt: agg?.requests ?? 0,
      b_errors_4xx: agg?.errors ?? 0,
      ...(insLatCity ? { b_lat_total: latAgg?.elapsedCount ?? 0, b_p95: p95Ms(latAgg) } : {}),
    };
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
  const insAsn = rollupGroup(rollup, "ins_asn", "asn");
  const asn = rows.asn.map((row) => {
    const key = row.asn == null ? null : String(row.asn);
    const hosting = key == null ? undefined : insAsn?.get(key);
    return {
      ...row,
      b_cnt: key == null ? 0 : asnCounts.get(key) ?? 0,
      ...(insAsn ? { b_proxy_total: hosting?.requests ?? 0, b_hosting: hosting?.count ?? 0 } : {}),
    };
  });

  const insLatPop = rollupGroup(rollup, "ins_lat", "pop");
  const geo = rows.geo?.map((row) => {
    if (!insLatPop || !row.pop) return row;
    const agg = insLatPop.get(String(row.pop));
    return { ...row, b_lat_total: agg?.elapsedCount ?? 0, b_p95: p95Ms(agg) };
  });

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

  const insUrl = rollupGroup(rollup, "ins_url", "url");
  const urls = rollupGroup(rollup, "urls", "url");
  const insUrlTruncated = rollup.truncated.has("ins_url/url");
  const url = (rows.url ?? []).map((row) => {
    if (!insUrl || !urls) return row;
    const key = row.url == null ? null : String(row.url);
    const ins = key == null ? undefined : insUrl.get(key);
    const u = key == null ? undefined : urls.get(key);
    return {
      ...row,
      b_total: ins?.requests ?? 0,
      b_5xx: ins?.status5xx ?? 0,
      b_hits: ins?.hitRequests ?? 0,
      b_miss: ins?.missRequests ?? 0,
      b_pass: ins?.passRequests ?? 0,
      b_lat_total: u?.elapsedCount ?? 0,
      b_p95: p95Ms(u),
      ...(insUrlTruncated && !ins ? { b_ins_url_absent: true } : {}),
    };
  });

  const insIp = rollupGroup(rollup, "ins_ip", "ip");
  const ip = (rows.ip ?? []).map((row) => {
    if (!insIp) return row;
    const key = row.ip == null ? null : String(row.ip);
    const agg = key == null ? undefined : insIp.get(key);
    return { ...row, ce_b_denied: agg?.errors ?? 0, cd_b_404: agg?.count ?? 0 };
  });

  const deferred = insightsDeferredDetectors(rollup);
  const approximate = new Set(ALWAYS_APPROXIMATE);
  for (const id of ["latency_regression", "city_latency_regressions", "pop_latency_regression"]) {
    if (!deferred.has(id)) approximate.add(id);
  }
  const lowerBound = new Set<string>();
  for (const [id, group] of Object.entries(BASELINE_FIELDS)) {
    if (deferred.has(id) || !rollup.truncated.has(group) || approximate.has(id)) continue;
    (RATIO_DETECTORS.has(id) ? approximate : lowerBound).add(id);
  }
  return {
    rows: { url, ip, city, dimension, asn, ...(geo ? { geo } : {}), scalar },
    baselineRequests,
    source: { approximate, lowerBound, deferred },
    meta: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate: [...approximate],
      lower_bound: [...lowerBound],
    },
  };
}
