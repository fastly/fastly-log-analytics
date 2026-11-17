import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries, sumAggs } from "./dashboard/rollup-query";
import {
  computeNetworkHealthScore,
  deduplicateMetroLeaderboard,
  emptyNetworkHealth,
  emptyPopHealth,
  networkBundle,
  parseEpochMs,
  roundTo,
  TCP_FIELDS,
  type MetroRow,
  type NetworkQueryParams,
} from "./network";

/**
 * How a response answered from v3 summaries differs from the raw scan: it
 * covers only whole raw windows from `data_from` through `data_through` (at most
 * one newer window is omitted), the `approximate` fields come from latency
 * histograms, and the `lower_bound` fields or tables count only the values a
 * summary kept in its top K (or, for `cache_hit_rate`, only HIT and HIT-STALE).
 */
export type NetworkRollupMeta = {
  data_from: string;
  data_through: string;
  approximate: string[];
  lower_bound: string[];
};

const HEALTH_SECTIONS = new Set(["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets"]);

const HEALTH_APPROXIMATE = [
  "median_rtt_ms", "p95_rtt_ms", "rtt_med_us", "rtt_med_ms", "health_score",
  "global_health_score", "health_score_now", "worst_asn", "worst_country",
];

const POP_APPROXIMATE = ["p50_rtt_us", "p95_ttfb_ms", "p50_ms", "p95_ms", "baseline_p50_ms", "is_elevated"];

/** Tables whose counts become lower bounds when the named summary group was cut to its top K. */
const TRUNCATION_BOUNDS: Record<string, string[]> = {
  "dims/asn": ["by_asn", "leaderboard"],
  "dims/city": ["metro_leaderboard"],
  "dims/country": ["by_country"],
  "series5/asn": ["heatmap"],
};

/** True when every requested /network-health section can come from summaries. */
export function canServeHealthFromRollup(sections: readonly string[] | undefined): boolean {
  return Boolean(sections?.length) && sections!.every((section) => HEALTH_SECTIONS.has(section));
}

function meta(result: RollupResult, approximate: string[], lowerBound: string[]): NetworkRollupMeta {
  return {
    data_from: new Date(result.dataFromMs).toISOString(),
    data_through: new Date(result.dataThroughMs).toISOString(),
    approximate,
    lower_bound: lowerBound,
  };
}

type Stats = {
  reqs: number;
  avgRttUs: number | null;
  medRttUs: number | null;
  p95RttUs: number | null;
  p10RttUs: number | null;
  avgPloss: number | null;
  errorPct: number;
};

function stats(agg: RollupAgg): Stats {
  const rtt = agg.hists.rtt;
  return {
    reqs: agg.requests,
    avgRttUs: agg.rttCount ? agg.rttSum / agg.rttCount : null,
    medRttUs: rtt?.quantile(0.5) ?? null,
    p95RttUs: rtt?.quantile(0.95) ?? null,
    p10RttUs: rtt?.quantile(0.1) ?? null,
    avgPloss: agg.plossCount ? agg.plossSum / agg.plossCount : null,
    errorPct: agg.requests ? (agg.status5xx * 100) / agg.requests : 0,
  };
}

/** The raw path's per-group fields (see `queryNetworkHealth`), from one aggregate. */
function scored(s: Stats) {
  const rttMedUs = s.medRttUs ?? 0;
  const ploss = s.avgPloss ?? 0;
  return {
    reqs: s.reqs,
    rtt_med_us: roundTo(rttMedUs, 1),
    rtt_med_ms: roundTo(rttMedUs / 1000, 2),
    avg_ploss: roundTo(ploss, 4),
    avg_ploss_pct: roundTo(ploss <= 1 ? ploss * 100 : ploss, 2),
    error_pct: roundTo(s.errorPct, 2),
    health_score: s.medRttUs == null ? null : computeNetworkHealthScore(
      rttMedUs, Math.max(1, s.p10RttUs ?? rttMedUs), ploss, s.errorPct,
    ),
  };
}

function ranked(values: Map<string, RollupAgg> | undefined, limit: number): [string, RollupAgg][] {
  return [...values ?? []].sort((a, b) => b[1].requests - a[1].requests).slice(0, limit);
}

/** TCP fields the summaries saw no value for; fields they don't count are not reported. */
function missingTcpFields(all: RollupAgg): string[] {
  const seen: Record<string, boolean> = {
    client_rtt_us: all.rttCount > 0,
    client_tcpi_rtt: all.rttCount > 0,
    client_ploss: all.plossCount > 0,
    client_cwnd: all.cwndCount > 0,
    client_tcpi_rttvar: all.rttvarCount > 0,
  };
  return TCP_FIELDS.filter((field) => seen[field] === false);
}

/**
 * The /network-health core sections (summary, country, ASN and metro tables,
 * leaderboard and ASN heatmap) from merged summaries, shaped like
 * `queryNetworkHealth`. Counts, bytes, average RTT, loss and 5xx shares are
 * exact; RTT percentiles and health scores come from histograms. Returns null
 * when the heatmap bucket is not a whole number of the summaries' steps.
 */
export function networkHealthFromRollup(
  result: RollupResult,
  params: Pick<NetworkQueryParams, "bucket_seconds" | "top_n">,
) {
  const { rollup } = result;
  const bucketMs = Math.max(1, Math.min(14400, Math.floor(params.bucket_seconds ?? 300))) * 1000;
  const topN = Math.max(1, Math.min(100, Math.floor(params.top_n ?? 30)));
  const overall = rebucketSeries(rollup, "overall", bucketMs);
  const asnSeries = rebucketSeries(rollup, "asn", bucketMs);
  if (!overall || !asnSeries) return null;
  const lowerBound = [...new Set([...rollup.truncated].flatMap((group) => TRUNCATION_BOUNDS[group] ?? []))];
  const rollupMeta = meta(result, HEALTH_APPROXIMATE, lowerBound);
  const missing = missingTcpFields(sumAggs(rollup.dims.get("country")?.values() ?? []));
  const total = stats(sumAggs(overall.get("")?.values() ?? []));
  if (total.reqs === 0) return { ...emptyNetworkHealth(missing), rollup: rollupMeta };

  const by_country = ranked(rollup.dims.get("country"), 100).map(([country, agg]) => {
    const row = scored(stats(agg));
    return { country, ...row, total_reqs: row.reqs };
  });
  const by_asn = ranked(rollup.dims.get("asn"), 100).map(([asn, agg]) => {
    const { avg_ploss_pct: _pct, ...row } = scored(stats(agg));
    return { asn: Number(asn), as_name: agg.label ?? `AS${asn}`, ...row };
  });
  const metros = ranked(rollup.dims.get("city"), 400).map(([key, agg]): MetroRow => {
    const [country, city] = JSON.parse(key) as [string, string];
    const row = scored(stats(agg));
    return { city, country, raw_city: city, region: "", ...row, total_reqs: row.reqs };
  });
  const metro_leaderboard = deduplicateMetroLeaderboard(metros);

  const buckets = [...overall.get("")?.keys() ?? []].sort((a, b) => a - b);
  const bucketIndexes = new Map(buckets.map((t, index) => [t, index]));
  const heatmap = [...asnSeries].map(([asn, cells]) => ({
    asn: Number(asn),
    label: `AS${asn}`,
    total_reqs: sumAggs(cells.values()).requests,
    buckets: [...cells].sort((a, b) => a[0] - b[0]).map(([t, agg]) => {
      const s = stats(agg);
      return {
        reqs: s.reqs,
        rtt_med_us: s.medRttUs,
        avg_ploss: s.avgPloss,
        error_pct: s.errorPct,
        health_score: s.medRttUs == null ? null : computeNetworkHealthScore(
          s.medRttUs, Math.max(1, s.p10RttUs ?? 0), s.avgPloss ?? 0, s.errorPct,
        ),
        bucket: new Date(t).toISOString(),
        bucket_idx: bucketIndexes.get(t),
      };
    }),
  })).sort((a, b) => b.total_reqs - a.total_reqs).slice(0, topN);

  const medRttUs = total.medRttUs ?? 0;
  const avgPloss = total.avgPloss ?? 0;
  const healthScore = total.medRttUs == null ? null : computeNetworkHealthScore(
    medRttUs, Math.max(1, total.p10RttUs ?? medRttUs), avgPloss, total.errorPct,
  );
  const worst = <T extends { health_score: number | null }>(rows: T[]) =>
    rows.filter((row) => row.health_score != null).sort((a, b) => a.health_score! - b.health_score!)[0];
  const worstAsn = worst(by_asn);
  const worstCountry = worst(by_country);

  return {
    available: true,
    has_data: true,
    buckets: buckets.map((t) => new Date(t).toISOString()),
    cities: [] as { name: string; lat: number | null; lon: number | null }[],
    map_buckets: [] as Record<string, unknown>[],
    heatmap,
    shielding_analysis: undefined,
    has_metro: metro_leaderboard.length > 0,
    leaderboard: by_asn.slice(0, topN).map((row) => ({
      ...row, label: row.as_name, total_reqs: row.reqs,
      health_score_now: row.health_score,
      health_score_1h_ago: null,
      health_score_1w_ago: null,
    })),
    summary: {
      total_requests: total.reqs,
      total_reqs: total.reqs,
      global_health_score: healthScore,
      worst_asn: worstAsn ? { label: worstAsn.as_name, score: worstAsn.health_score } : null,
      worst_country: worstCountry ? { label: worstCountry.country, score: worstCountry.health_score } : null,
      avg_rtt_ms: roundTo((total.avgRttUs ?? 0) / 1000, 2),
      median_rtt_ms: roundTo(medRttUs / 1000, 2),
      p95_rtt_ms: roundTo((total.p95RttUs ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo(avgPloss <= 1 ? avgPloss * 100 : avgPloss, 2),
      health_score: healthScore,
    },
    timeseries: [] as Record<string, unknown>[],
    by_country,
    by_asn,
    metro_leaderboard,
    missing_tcp_fields: missing,
    rollup: rollupMeta,
  };
}

/**
 * The PoP health table from merged summaries, shaped like `queryPopHealth`.
 * Requests, bytes and 5xx counts are exact; latency comes from histograms; the
 * hit rate counts HIT and HIT-STALE only (raw counts every `HIT*` state), so it
 * is a lower bound. Rows without a PoP, which raw groups as `Unknown`, are absent.
 */
export function popHealthFromRollup(result: RollupResult) {
  const rollupMeta = meta(result, POP_APPROXIMATE, ["cache_hit_rate"]);
  const rows = ranked(result.rollup.dims.get("pop"), 200);
  if (!rows.length) return { ...emptyPopHealth(), rollup: rollupMeta };
  let elevatedCount = 0;
  const pops = rows.map(([pop, agg]) => {
    const s = stats(agg);
    const p95TtfbUs = agg.hists.ttfb?.quantile(0.95) ?? null;
    const p50Ms = (s.medRttUs ?? 0) / 1000;
    const baselineMs = Math.max(0.1, (s.p10RttUs ?? 0) / 1000);
    const isElevated = p50Ms > baselineMs * 1.5;
    if (isElevated) elevatedCount++;
    return {
      pop,
      requests: agg.requests,
      errors: agg.status5xx,
      error_rate: s.errorPct,
      bandwidth_bytes: agg.respBytes,
      cache_hit_rate: agg.requests ? (agg.cacheHits * 100) / agg.requests : 0,
      p50_rtt_us: s.medRttUs,
      p95_ttfb_ms: p95TtfbUs == null ? null : p95TtfbUs / 1000,
      p50_ms: roundTo(p50Ms, 2),
      p95_ms: roundTo((s.p95RttUs ?? 0) / 1000, 2),
      baseline_p50_ms: roundTo(baselineMs, 2),
      avg_ploss_pct: roundTo((s.avgPloss ?? 0) * 100, 2),
      error_pct: roundTo(s.errorPct, 2),
      is_elevated: isElevated,
    };
  });
  return {
    data: pops,
    pops,
    by_pop: pops,
    elevated_count: elevatedCount,
    total_pops: pops.length,
    rollup: rollupMeta,
  };
}

/**
 * True when every requested /network/bundle section can come from summaries:
 * the health core sections and the PoP table. Transport needs delivery rate,
 * which summaries don't keep, and quality, the map and shielding need rows.
 */
function canServeBundleFromRollup(sections: readonly string[] | undefined): boolean {
  return Boolean(sections?.length) && sections!.every((section) => HEALTH_SECTIONS.has(section) || section === "pop_health");
}

/** Whether summaries may answer this read at all, before any range or listing is resolved. */
export function networkRollupEligible(path: string, params: NetworkQueryParams): boolean {
  if (params.filter_country || params.filter_pop ||
      params.filter_asn !== undefined && params.filter_asn !== "" ||
      Object.keys(params.filters ?? {}).length) return false;
  if (path === "/api/network/bundle") return canServeBundleFromRollup(params.sections);
  return path === "/api/network-health" ? canServeHealthFromRollup(params.sections) : path === "/api/network/pop-health";
}

/**
 * Answers a Cloud /network read from v3 summaries, or returns null when the raw
 * scan must: any filter (summaries hold no dimension crossed with another, so
 * even one PoP, country or ASN filter can't narrow the ASN, metro or PoP
 * tables), a section that needs rows, a bucket finer than the summaries, or a
 * range the summaries don't cover (see `queryRollup`).
 */
export async function networkFromRollup(
  path: string,
  params: NetworkQueryParams,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
) {
  if (!networkRollupEligible(path, params)) return null;
  const health = path === "/api/network-health";
  const fromMs = parseEpochMs(params.start_time);
  const toMs = parseEpochMs(params.end_time);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal });
  if (!result) return null;
  if (path === "/api/network/bundle") {
    const sections = params.sections!;
    const wantsHealth = sections.some((section) => HEALTH_SECTIONS.has(section));
    const core = wantsHealth ? networkHealthFromRollup(result, params) : null;
    if (wantsHealth && core === null) return null;
    return networkBundle(core, null, sections.includes("pop_health") ? popHealthFromRollup(result) : null, null);
  }
  return health ? networkHealthFromRollup(result, params) : popHealthFromRollup(result);
}
