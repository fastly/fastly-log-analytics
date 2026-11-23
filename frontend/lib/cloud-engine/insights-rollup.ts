import type { DashboardFilter } from "./dashboard/filters";
import { Histogram } from "./dashboard/histogram";
import { emptyAgg, queryRollup, rollupGroup, type Rollup, type RollupAgg, type RollupResult } from "./dashboard/rollup-query";
import {
  CLOUD_INSIGHT_DETECTORS,
  handleInsightAvailability,
  handleInsights,
  INSIGHT_DEFINITIONS,
  IP_DETECTOR_MIN_HITS,
  resolveInsightWindow,
  type InsightsRequest,
} from "./insights";
import type { NetworkRollupMeta } from "./network-rollup";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";
import { INSIGHTS_RESIDUAL, INSIGHTS_RESIDUAL_FIELDS } from "./insights-residual";

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

/**
 * The IP detectors the summaries prove can't fire, so the raw follow-up can skip
 * them. The window's `ins_ip` must be uncut, so it lists every IP with a 404 or
 * an auth-path 401/403 with exact counts; a non-empty `ins_url` and either a
 * non-empty `ins_ip` or `all_fields/ip` values show the columns exist. An empty
 * uncut `ins_ip` (the usual window: no 404 or denial at all) rules both out.
 * Otherwise, for every IP:
 * - credential_enumeration needs `IP_DETECTOR_MIN_HITS` denials above 3x its
 *   scaled baseline denials + 5. The baseline `ins_ip` count is the one the
 *   follow-up uses and never above the true count.
 * - content_discovery needs `IP_DETECTOR_MIN_HITS` 404s that are 70% of the
 *   IP's window requests. Its `all_fields` count sums exact per-file counts of
 *   only the files that kept it, so it never exceeds the true request count.
 */
export function ipDetectorsRuledOut(
  window: Rollup,
  baseline: Rollup,
  windowHours: number,
  baselineHours: number,
): ReadonlySet<string> {
  const ips = rollupGroup(window, "ins_ip", "ip");
  const requests = window.fields.get("ip");
  if (!ips || (!ips.size && !requests?.size) || window.truncated.has("ins_ip/ip")
    || !rollupGroup(window, "ins_url", "url")?.size) return new Set();
  const baselineIps = rollupGroup(baseline, "ins_ip", "ip");
  // insights.ts `scaled` and `pct`, in the same operation order.
  const scaled = (count: number) => count * windowHours / baselineHours;
  let credential = true;
  let content = true;
  for (const [ip, agg] of ips) {
    const denied = agg.errors;
    if (denied >= IP_DETECTOR_MIN_HITS && denied > scaled(baselineIps?.get(ip)?.errors ?? 0) * 3 + 5) credential = false;
    const seen = requests?.get(ip) ?? 0;
    if (agg.count >= IP_DETECTOR_MIN_HITS && !(seen > 0 && agg.count * 100 / seen < 70)) content = false;
  }
  return new Set([...credential ? ["credential_enumeration"] : [], ...content ? ["content_discovery"] : []]);
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
  return queryRollup(source, {
    fromMs: start, toMs: windowStart, signal, spec: spec ?? undefined, residual: INSIGHTS_RESIDUAL,
  });
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
      fields: new Map(
        [...rollup.fields].filter(([k]) =>
          (INSIGHTS_RESIDUAL_FIELDS as readonly string[]).includes(k),
        ),
      ),
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

export type InsightsRollupRequest = InsightsRequest & {
  mask_ips?: boolean;
  earliest_log_at?: string | null;
  /** The service's enabled log field ids, resolved on the main thread (this may run in a worker). */
  enabled_log_fields?: readonly string[];
};

const ROLLUP_INSIGHT_COLUMNS = new Set(
  INSIGHT_DEFINITIONS.filter((d) => CLOUD_INSIGHT_DETECTORS.has(d.id)).flatMap((d) => d.required_fields),
);

/**
 * Answers an unfiltered /insights request (without explicit `detectors`) from
 * v3 summaries for both window and baseline, deferring only the IP detectors
 * (`credential_enumeration`, `content_discovery`) and any detector whose group
 * is absent from either rollup. Returns null when summaries cannot cover both
 * ranges so the caller falls back to the raw scan.
 */
export async function insightsFromRollup(
  body: InsightsRollupRequest,
  filters: readonly DashboardFilter[],
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  spec: SummarySpec | null = INSIGHTS_SUMMARY_SPEC,
) {
  if (filters.length || Array.isArray(body.detectors)) return null;
  const { end, windowStart, start, windowHours, baselineHours } = resolveInsightWindow(body);
  const [windowResult, baselineResult] = await Promise.all([
    queryRollup(source, {
      fromMs: windowStart, toMs: end, signal, spec: spec ?? undefined, residual: INSIGHTS_RESIDUAL,
    }),
    queryRollup(source, {
      fromMs: start, toMs: windowStart, signal, spec: spec ?? undefined, residual: INSIGHTS_RESIDUAL,
    }),
  ]);
  if (!windowResult || !baselineResult) return null;

  const wRollup = windowResult.rollup;
  const bRollup = baselineResult.rollup;
  const sumOverall = (rollup: Rollup) => {
    let total = 0;
    for (const agg of rollup.series.get("overall")?.get("")?.values() ?? []) total += agg.requests;
    return total;
  };
  const windowRequests = sumOverall(wRollup);
  const baselineRequests = sumOverall(bRollup);

  const histOf = (agg: RollupAgg | undefined) =>
    agg && agg.elapsedCount > 0
      ? (agg.hists.elapsed ?? (agg.elapsedHist ? Histogram.fromBytes(agg.elapsedHist) : null))
      : null;
  const p95Ms = (agg: RollupAgg | undefined) => {
    const q95 = histOf(agg)?.quantile(0.95) ?? null;
    return q95 == null ? null : q95 / 1000;
  };

  const wCities = wRollup.dims.get("city");
  const bCities = bRollup.dims.get("city");
  const city: Row[] = [];
  for (const [key, wAgg] of wCities ?? []) {
    if (wAgg.requests < 5) continue;
    const [country, name] = JSON.parse(key) as [string, string];
    if (name === "Unknown") continue;
    const bAgg = bCities?.get(key);
    city.push({
      city: name,
      region: null,
      country: country === "Unknown" ? null : country,
      w_cnt: wAgg.requests,
      b_cnt: bAgg?.requests ?? 0,
      w_errors_4xx: wAgg.errors,
      b_errors_4xx: bAgg?.errors ?? 0,
    });
  }
  const wInsLatCity = rollupGroup(wRollup, "ins_lat", "city");
  const bInsLatCity = rollupGroup(bRollup, "ins_lat", "city");
  if (wInsLatCity && bInsLatCity) {
    for (const [key, wAgg] of wInsLatCity) {
      if (wAgg.elapsedCount < 10) continue;
      const [name, region, country] = JSON.parse(key) as [string, string | null, string | null];
      const bAgg = bInsLatCity.get(key);
      city.push({
        city: name,
        region,
        country,
        w_cnt: 0,
        b_cnt: 0,
        w_errors_4xx: 0,
        b_errors_4xx: 0,
        w_lat_total: wAgg.elapsedCount,
        b_lat_total: bAgg?.elapsedCount ?? 0,
        w_p95: p95Ms(wAgg),
        b_p95: p95Ms(bAgg),
      });
    }
  }

  const dimension: Row[] = [];
  for (const key of DIMENSIONS) {
    const wCounts = wRollup.fields.get(key) ?? new Map<string, number>();
    const bCounts = bRollup.fields.get(key) ?? new Map<string, number>();
    let wNamed = 0;
    let bNamed = 0;
    for (const [value, n] of wCounts) {
      wNamed += n;
      dimension.push({ dimension: key, [key]: value, w_cnt: n, b_cnt: bCounts.get(value) ?? 0 });
    }
    for (const [value, n] of bCounts) {
      bNamed += n;
      if (!wCounts.has(value)) dimension.push({ dimension: key, [key]: value, w_cnt: 0, b_cnt: n });
    }
    dimension.push({
      dimension: key,
      [key]: null,
      w_cnt: Math.max(0, windowRequests - wNamed),
      b_cnt: Math.max(0, baselineRequests - bNamed),
    });
  }

  const wAsn = wRollup.fields.get("asn") ?? new Map<string, number>();
  const bAsn = bRollup.fields.get("asn") ?? new Map<string, number>();
  const wInsAsn = rollupGroup(wRollup, "ins_asn", "asn");
  const bInsAsn = rollupGroup(bRollup, "ins_asn", "asn");
  const asn: Row[] = [];
  for (const [key, wCnt] of wAsn) {
    const wH = wInsAsn?.get(key);
    const bH = bInsAsn?.get(key);
    asn.push({
      asn: /^\d+$/.test(key) ? Number(key) : key,
      w_cnt: wCnt,
      b_cnt: bAsn.get(key) ?? 0,
      ...(wInsAsn && bInsAsn ? {
        w_proxy_total: wH?.requests ?? 0,
        w_hosting: wH?.count ?? 0,
        b_proxy_total: bH?.requests ?? 0,
        b_hosting: bH?.count ?? 0,
      } : {}),
    });
  }

  const wInsLatPop = rollupGroup(wRollup, "ins_lat", "pop");
  const bInsLatPop = rollupGroup(bRollup, "ins_lat", "pop");
  const geo: Row[] = [];
  if (wInsLatPop && bInsLatPop) {
    for (const [pop, wAgg] of wInsLatPop) {
      const bAgg = bInsLatPop.get(pop);
      geo.push({
        pop,
        w_lat_total: wAgg.elapsedCount,
        b_lat_total: bAgg?.elapsedCount ?? 0,
        w_p95: p95Ms(wAgg),
        b_p95: p95Ms(bAgg),
      });
    }
  }

  const sum = (counts: Map<string, number>, pred: (value: string) => boolean) =>
    [...counts].reduce((total, [value, n]) => total + (pred(value) ? n : 0), 0);
  const wCache = wRollup.fields.get("cache") ?? new Map<string, number>();
  const bCache = bRollup.fields.get("cache") ?? new Map<string, number>();
  const wTransport = wRollup.fields.get("transport") ?? new Map<string, number>();
  const bTransport = bRollup.fields.get("transport") ?? new Map<string, number>();
  const scalar: Row[] = [{
    w_quic: sum(wTransport, (v) => v.toLowerCase() === "quic"),
    w_tr_total: sum(wTransport, (v) => v !== ""),
    b_quic: sum(bTransport, (v) => v.toLowerCase() === "quic"),
    b_tr_total: sum(bTransport, (v) => v !== ""),
    w_hits: sum(wCache, (v) => v.startsWith("HIT")),
    w_cacheable: sum(wCache, (v) => v.startsWith("HIT") || v.startsWith("MISS")),
    b_hits: sum(bCache, (v) => v.startsWith("HIT")),
    b_cacheable: sum(bCache, (v) => v.startsWith("HIT") || v.startsWith("MISS")),
  }];

  const wInsUrl = rollupGroup(wRollup, "ins_url", "url");
  const bInsUrl = rollupGroup(bRollup, "ins_url", "url");
  const wUrls = rollupGroup(wRollup, "urls", "url");
  const bUrls = rollupGroup(bRollup, "urls", "url");
  const insUrlTruncated = bRollup.truncated.has("ins_url/url");
  const url: Row[] = [];
  if (wInsUrl && bInsUrl && wUrls && bUrls) {
    for (const key of new Set([...wInsUrl.keys(), ...wUrls.keys()])) {
      const wIns = wInsUrl.get(key);
      const bIns = bInsUrl.get(key);
      const wU = wUrls.get(key);
      const bU = bUrls.get(key);
      if ((wIns?.requests ?? 0) < 3 && (wU?.elapsedCount ?? 0) < 5) continue;
      const wHist = histOf(wU);
      const wQ99 = wHist && wU && wU.elapsedCount > 0 ? wHist.quantile(0.99) : null;
      const wQ50 = wHist && wU && wU.elapsedCount > 0 ? wHist.quantile(0.5) : null;
      url.push({
        url: key,
        w_total: wIns?.requests ?? 0,
        b_total: bIns?.requests ?? 0,
        w_5xx: wIns?.status5xx ?? 0,
        b_5xx: bIns?.status5xx ?? 0,
        w_hits: wIns?.hitRequests ?? 0,
        b_hits: bIns?.hitRequests ?? 0,
        w_miss: wIns?.missRequests ?? 0,
        b_miss: bIns?.missRequests ?? 0,
        w_pass: wIns?.passRequests ?? 0,
        b_pass: bIns?.passRequests ?? 0,
        w_lat_total: wU?.elapsedCount ?? 0,
        b_lat_total: bU?.elapsedCount ?? 0,
        w_p95: p95Ms(wU),
        b_p95: p95Ms(bU),
        w_p99: wQ99 == null ? null : Math.round(wQ99 / 1000),
        w_p50: wQ50 == null ? null : Math.round(wQ50 / 1000),
        ...(insUrlTruncated && !bIns ? { b_ins_url_absent: true } : {}),
      });
    }
  }

  const ipRuledOut = ipDetectorsRuledOut(wRollup, bRollup, windowHours, baselineHours);
  const deferred = new Set<string>([
    ...INS_IP_DETECTORS,
    ...insightsDeferredDetectors(wRollup),
    ...insightsDeferredDetectors(bRollup),
  ].filter((id) => !ipRuledOut.has(id)));
  const approximate = new Set(ALWAYS_APPROXIMATE);
  for (const id of ["latency_regression", "tail_latency", "city_latency_regressions", "pop_latency_regression"]) {
    if (!deferred.has(id)) approximate.add(id);
  }
  const lowerBound = new Set<string>();
  for (const [id, group] of Object.entries(BASELINE_FIELDS)) {
    if (deferred.has(id) || (!wRollup.truncated.has(group) && !bRollup.truncated.has(group)) || approximate.has(id)) continue;
    (RATIO_DETECTORS.has(id) ? approximate : lowerBound).add(id);
  }

  // Summaries hold every supported detector's fields; the log field settings decide which run.
  const enabled = new Set(body.enabled_log_fields ?? []);
  const availableColumns = new Set([...ROLLUP_INSIGHT_COLUMNS].filter((field) => enabled.has(field)));
  const availability = handleInsightAvailability(body.service_id, availableColumns);
  const earliestLogMs = body.earliest_log_at ? Date.parse(body.earliest_log_at) : Number.NaN;
  const effectiveFirstMs = baselineRequests > 0 && Number.isFinite(earliestLogMs)
    ? Math.min(baselineResult.dataFromMs, earliestLogMs)
    : baselineRequests > 0 ? baselineResult.dataFromMs : null;
  const historyHours = effectiveFirstMs == null ? 0 : Math.max(0, (end - effectiveFirstMs) / 3_600_000);
  const envelope = await handleInsights(body, {
    now: new Date(end),
    maskIps: Boolean(body.mask_ips),
    availableColumns,
    availableHistoryHours: historyHours,
    urlAggregateRows: url,
    ipSecurityRows: [],
    cityAggregateRows: city,
    dimensionAggregateRows: dimension,
    asnAggregateRows: asn,
    geoLatencyRows: geo,
    scalarAggregateRows: scalar,
    windowTotalRequests: windowRequests,
    baselineTotalRequests: baselineRequests,
    baselineSource: { approximate, lowerBound, deferred },
  });
  return {
    ...envelope,
    rollup: {
      data_from: new Date(baselineResult.dataFromMs).toISOString(),
      data_through: new Date(windowResult.dataThroughMs).toISOString(),
      approximate: [...approximate],
      lower_bound: [...lowerBound],
    },
    deferred_detectors: envelope.insights.filter((card) => card.placeholder).map((card) => card.id),
    unsupported_detectors: availability.unavailable
      .filter((d) => d.reason === "cloud_detector_unsupported")
      .map(({ id, reason }) => ({ id, reason })),
    unavailable_detectors: availability.unavailable,
  };
}
