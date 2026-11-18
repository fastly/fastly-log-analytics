import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries, rollupGroup, sumAggs } from "./dashboard/rollup-query";
import {
  computeNetworkHealthScore,
  deduplicateMetroLeaderboard,
  emptyNetworkHealth,
  emptyNetworkQuality,
  emptyPopHealth,
  networkBundle,
  normalizeProtocolBucket,
  parseEpochMs,
  PROTOCOL_BUCKETS,
  roundTo,
  TCP_FIELDS,
  type MetroRow,
  type NetworkQualityParts,
  type NetworkQueryParams,
  type QualityPart,
} from "./network";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";
import { shieldingFromRollup } from "./origin-rollup";

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
/** The city map's sections, from the `series5` `city` rows (summaries before them leave it to raw). */
const MAP_SECTIONS = new Set(["cities", "map_buckets"]);

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
  "series5/city": ["cities", "map_buckets"],
  "quality/asn": ["by_asn"],
  "quality/region": ["by_region"],
};

const lowerBounds = (rollup: RollupResult["rollup"]) =>
  [...new Set([...rollup.truncated].flatMap((group) => TRUNCATION_BOUNDS[group] ?? []))];

/**
 * True when every requested /network-health section can come from summaries:
 * the core sections and the city map (for every ASN; one map ASN needs rows).
 */
export function canServeHealthFromRollup(sections: readonly string[] | undefined, mapAsn?: string): boolean {
  return Boolean(sections?.length) && sections!.every((section) =>
    HEALTH_SECTIONS.has(section) ||
    section === "shielding_analysis" ||
    MAP_SECTIONS.has(section) && (!mapAsn || mapAsn === "all"));
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
  const rollupMeta = meta(result, HEALTH_APPROXIMATE, lowerBounds(rollup));
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
 * The /network-health city map (`buckets`, `cities`, `map_buckets`) from the
 * `series5` `city` rows, shaped like `queryNetworkBuckets`: counts, loss and 5xx
 * shares are exact, coordinates are each cell's average, and RTT medians and
 * health scores come from histograms. Cities beyond the summaries' top K are
 * absent. Null when a summary predates the rows or the bucket is not a whole
 * number of the summaries' steps.
 */
export function mapFromRollup(result: RollupResult, params: Pick<NetworkQueryParams, "bucket_seconds">) {
  const { rollup } = result;
  if (!rollup.present?.has("series5/city")) return null;
  const bucketMs = Math.max(1, Math.min(14400, Math.floor(params.bucket_seconds ?? 300))) * 1000;
  const overall = rebucketSeries(rollup, "overall", bucketMs);
  const series = rebucketSeries(rollup, "city", bucketMs);
  if (!overall || !series) return null;
  const buckets = [...overall.get("")?.keys() ?? []].sort((a, b) => a - b);
  const bucketIndexes = new Map(buckets.map((t, index) => [t, index]));
  const cells = [...series].flatMap(([key, values]) => [...values].map(([t, agg]) => ({ key, t, agg })))
    .filter((cell) => cell.agg.requests > 0)
    .sort((a, b) => a.t - b.t || b.agg.requests - a.agg.requests || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const cities: { name: string; lat: number | null; lon: number | null }[] = [];
  const cityIndexes = new Map<string, number>();
  const mapBuckets = new Map<number, { bucket: string; bucket_idx: number; cities: Record<string, unknown>[] }>();
  for (const { key, t, agg } of cells) {
    const [country, city] = JSON.parse(key) as [string | null, string];
    if (!cityIndexes.has(key)) {
      cityIndexes.set(key, cities.length);
      const geo = (sum: number) => (agg.geoCount ? sum / agg.geoCount : null);
      cities.push({ name: city, lat: geo(agg.latSum), lon: geo(agg.lonSum) });
    }
    const s = stats(agg);
    const entry = mapBuckets.get(t) ??
      { bucket: new Date(t).toISOString(), bucket_idx: bucketIndexes.get(t) ?? buckets.length, cities: [] };
    entry.cities.push({
      reqs: s.reqs,
      rtt_med_us: s.medRttUs,
      avg_ploss: s.avgPloss,
      error_pct: s.errorPct,
      health_score: s.medRttUs == null ? null : computeNetworkHealthScore(
        s.medRttUs, Math.max(1, s.p10RttUs ?? 0), s.avgPloss ?? 0, s.errorPct,
      ),
      city_idx: cityIndexes.get(key),
      country,
    });
    mapBuckets.set(t, entry);
  }
  return {
    buckets: buckets.map((t) => new Date(t).toISOString()),
    cities,
    map_buckets: [...mapBuckets.values()],
  };
}

/** /network-health from summaries: the core sections, the city map, shielding, or any combination. */
function healthFromRollup(result: RollupResult, params: NetworkQueryParams) {
  const sections = params.sections ?? [];
  const wantsShielding = sections.includes("shielding_analysis");
  const shielding = wantsShielding ? shieldingFromRollup(result, params.top_n ?? 30) : undefined;
  if (wantsShielding && !shielding) return null;
  const wantsMap = sections.some((section) => MAP_SECTIONS.has(section));
  const wantsCore = sections.some((section) => HEALTH_SECTIONS.has(section) && section !== "buckets");
  const map = wantsMap ? mapFromRollup(result, params) : null;
  if (wantsMap && !map) return null;
  const shieldingLb = wantsShielding && result.rollup.truncated.has("shield_routes/route")
    ? ["shielding_analysis.total_routes"] : [];
  if (!wantsCore) {
    const missing = missingTcpFields(sumAggs(result.rollup.dims.get("country")?.values() ?? []));
    const approx = [
      ...(map ? HEALTH_APPROXIMATE : []),
      ...(wantsShielding ? ["shielding_analysis"] : []),
    ];
    return {
      ...emptyNetworkHealth(missing),
      has_data: Boolean(map?.buckets.length) || Boolean(shielding?.has_data),
      ...(map ?? {}),
      ...(shielding ? { shielding_analysis: shielding } : {}),
      rollup: meta(result, approx, [...(map ? lowerBounds(result.rollup) : []), ...shieldingLb]),
    };
  }
  const core = networkHealthFromRollup(result, params);
  if (!core) return null;
  return {
    ...core,
    ...(map ? { cities: map.cities, map_buckets: map.map_buckets } : {}),
    ...(shielding ? { shielding_analysis: shielding } : {}),
    rollup: wantsShielding
      ? {
          ...core.rollup,
          approximate: [...core.rollup.approximate, "shielding_analysis"],
          lower_bound: [...core.rollup.lower_bound, ...shieldingLb],
        }
      : core.rollup,
  };
}

const QUALITY_APPROXIMATE = ["scatter"];
/** The raw scatter's row cap. */
const SCATTER_LIMIT = 2000;

/** /network-quality parts a summary answer lacked, which a raw read of only those must fill in. */
export type NetworkPartial = {
  network_partial: true;
  answer: QualityAnswer;
  residual: {
    quality_parts: QualityPart[];
    quality_asns?: number[];
    /** The summaries' own range, so raw and summary figures cover the same rows. */
    start_time: string;
    end_time: string;
  };
};

export function isNetworkPartial(value: unknown): value is NetworkPartial {
  return typeof value === "object" && value !== null && (value as { network_partial?: unknown }).network_partial === true;
}

const avg = (sum: number, count: number) => (count ? sum / count : null);

type QualityAnswer = NonNullable<ReturnType<typeof qualityAnswer>>["answer"];

/** Null when the summaries hold no rows. */
function qualityAnswer(result: RollupResult, regionCountry: string, rollupMeta: NetworkRollupMeta) {
  const { rollup } = result;
  const total = sumAggs(rollup.series.get("overall")?.get("")?.values() ?? []);
  const all = sumAggs(rollup.dims.get("country")?.values() ?? []);
  const missing = missingTcpFields(all);
  const overall = rollupGroup(rollup, "quality", "overall");
  const regions = rollupGroup(rollup, "quality", "region");
  const asnClient = rollupGroup(rollup, "quality", "asn");
  const missingParts: QualityPart[] = [
    ...overall ? [] : ["throughput" as const],
    ...regions ? [] : ["region" as const],
    ...asnClient ? [] : ["asn_client" as const],
  ];
  const popsCut = rollup.truncated.has("dims/pop");
  if (total.requests === 0) return null;

  const rttMs = (agg: RollupAgg) => roundTo((avg(agg.rttSum, agg.rttCount) ?? 0) / 1000, 2);
  const by_country = ranked(rollup.dims.get("country"), 100).map(([country, agg]) => ({
    value: country, label: country, rtt_ms: rttMs(agg), reqs: agg.requests,
  }));
  const by_asn = ranked(rollup.dims.get("asn"), 100).map(([asn, agg]) => {
    const client = asnClient?.get(asn);
    const row = {
      asn: Number(asn),
      as_name: agg.label ?? `AS${asn}`,
      requests: agg.requests,
      avg_rtt_ms: rttMs(agg),
      avg_rttvar_ms: roundTo((avg(client?.rttvarSum ?? 0, client?.rttvarCount ?? 0) ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo((avg(client?.plossSum ?? 0, client?.plossCount ?? 0) ?? 0) * 100, 2),
      avg_cwnd: roundTo(avg(agg.cwndSum, agg.cwndCount) ?? 0, 1),
    };
    return { ...row, value: asn, label: row.as_name, rtt_ms: row.avg_rtt_ms, reqs: row.requests };
  });

  const protoTotals = new Map<string, { requests: number; rttSum: number; rttCount: number }>(
    PROTOCOL_BUCKETS.map((bucket) => [bucket, { requests: 0, rttSum: 0, rttCount: 0 }]),
  );
  for (const [proto, agg] of rollup.dims.get("proto") ?? []) {
    const into = protoTotals.get(normalizeProtocolBucket(proto))!;
    into.requests += agg.requests;
    into.rttSum += agg.rttSum;
    into.rttCount += agg.rttCount;
  }
  const protocol_distribution = PROTOCOL_BUCKETS.map((protocol) => {
    const entry = protoTotals.get(protocol)!;
    return {
      protocol,
      requests: entry.requests,
      pct: roundTo((entry.requests * 100) / total.requests, 2),
      avg_rtt_ms: entry.requests > 0 ? roundTo((avg(entry.rttSum, entry.rttCount) ?? 0) / 1000, 2) : 0,
    };
  });

  const popRow = (pop: string, agg: Pick<RollupAgg, "requests" | "rttSum" | "rttCount">) => {
    const us = avg(agg.rttSum, agg.rttCount);
    return { pop, rtt_ms: us == null ? null : us / 1000, reqs: agg.requests, value: pop, label: pop };
  };
  const pops = [...rollup.dims.get("pop") ?? []].map(([pop, agg]) => popRow(pop, agg));
  // Rows without a PoP: what the PoP rows leave of the total, unless some PoPs were cut.
  const popped = sumAggs(rollup.dims.get("pop")?.values() ?? []);
  if (!popsCut && total.requests > popped.requests) {
    pops.push(popRow("Unknown", {
      requests: total.requests - popped.requests,
      rttSum: total.rttSum - popped.rttSum,
      rttCount: total.rttCount - popped.rttCount,
    }));
  }
  const by_pop = pops.sort((a, b) => b.reqs - a.reqs);

  const by_region = regions ? [...regions].flatMap(([key, agg]) => {
    const [country, region] = JSON.parse(key) as [string, string];
    if (country !== regionCountry) return [];
    const us = avg(agg.rttSum, agg.rttCount);
    return [{ value: region, rtt_ms: us == null ? null : us / 1000, reqs: agg.requests, label: region }];
  }).sort((a, b) => b.reqs - a.reqs) : [];

  const scatter = (rollup.samples ?? [])
    .filter((sample) => sample.rttUs != null)
    .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.tsMs - b.tsMs))
    .slice(0, SCATTER_LIMIT)
    .map((sample) => ({ rtt_ms: sample.rttUs! / 1000, ttfb_ms: sample.ttfbS * 1000, cache: sample.cache }));

  const tp = overall?.get("");
  return {
    answer: {
      available: total.rttCount > 0,
      by_country,
      by_pop,
      countries: by_country.map((row) => row.value),
      region_country: regionCountry,
      summary: {
        total_requests: total.requests,
        avg_throughput_mbps: roundTo(avg(tp?.throughputSum ?? 0, tp?.throughputCount ?? 0) ?? 0, 2),
        avg_cwnd: roundTo(avg(all.cwndSum, all.cwndCount) ?? 0, 1),
        avg_rtt_ms: rttMs(total),
        avg_rttvar_ms: roundTo((avg(all.rttvarSum, all.rttvarCount) ?? 0) / 1000, 2),
        retrans_rate_pct: roundTo((all.retrans * 100) / total.requests, 2),
      },
      scatter,
      by_region,
      by_asn,
      protocol_distribution,
      missing_tcp_fields: missing,
      rollup: rollupMeta,
    },
    missingParts,
  };
}

/**
 * /network-quality from summaries, shaped like `queryNetworkQuality`. Counts,
 * averages and shares are exact; the scatter is a hashed sample of the rows
 * with a TTFB (≈), and tables cut to a top K are lower bounds (≥). Summaries
 * written before the `quality` rows lack the throughput, region table and
 * client-only ASN loss and RTT variance: then this returns a `NetworkPartial`
 * naming only those parts, for a raw read of just their columns.
 */
export function qualityFromRollup(result: RollupResult, params: NetworkQueryParams) {
  const { rollup } = result;
  const lowerBound = lowerBounds(rollup).filter((field) => ["by_asn", "by_country", "by_region"].includes(field));
  if (rollup.truncated.has("dims/pop")) lowerBound.push("by_pop");
  const rollupMeta = meta(result, QUALITY_APPROXIMATE, lowerBound);
  const quality = qualityAnswer(result, String(params.region_country ?? "US"), rollupMeta);
  if (!quality) {
    const missing = missingTcpFields(sumAggs(rollup.dims.get("country")?.values() ?? []));
    return { ...emptyNetworkQuality(missing), rollup: rollupMeta };
  }
  const { answer, missingParts } = quality;
  if (!missingParts.length) return answer;
  const partial: NetworkPartial = {
    network_partial: true,
    answer,
    residual: {
      quality_parts: missingParts,
      ...missingParts.includes("asn_client") ? { quality_asns: answer.by_asn.map((row) => row.asn) } : {},
      start_time: new Date(result.dataFromMs).toISOString(),
      end_time: new Date(result.dataThroughMs).toISOString(),
    },
  };
  return partial;
}

/** A partial summary answer with the raw read of its missing parts (`{ quality_parts }`) filled in. */
export function mergeNetworkPartial(partial: NetworkPartial, raw: { quality_parts?: NetworkQualityParts }) {
  const parts = raw.quality_parts ?? {};
  const answer = { ...partial.answer, summary: { ...partial.answer.summary } };
  if (parts.throughput && "avg_throughput_mbps" in answer.summary) {
    answer.summary.avg_throughput_mbps = roundTo(parts.throughput.avg_throughput_mbps ?? 0, 2);
  }
  if (parts.region) answer.by_region = parts.region;
  if (parts.asn_client && answer.by_asn) {
    const byAsn = new Map(parts.asn_client.map((row) => [row.asn, row]));
    answer.by_asn = answer.by_asn.map((row) => {
      const client = byAsn.get(row.asn);
      return {
        ...row,
        avg_rttvar_ms: roundTo((client?.avg_rttvar_us ?? 0) / 1000, 2),
        avg_ploss_pct: roundTo((client?.avg_ploss ?? 0) * 100, 2),
      };
    });
  }
  return answer;
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

const TRANSPORT_APPROXIMATE = ["tcp_rtt_p50_ms", "tcp_rtt_p90_ms", "tcp_rtt_p95_ms", "tcp_rtt_p99_ms"];

/**
 * `queryNetworkTransport` from the `transport` rows: requests, bytes, packet
 * loss, delivery rate and the protocol split are exact; RTT percentiles come
 * from the coalesced RTT histogram. Null when a summary predates the rows.
 */
export function transportFromRollup(result: RollupResult) {
  const overall = rollupGroup(result.rollup, "transport", "overall");
  const protos = rollupGroup(result.rollup, "transport", "proto");
  if (!overall || !protos) return null;
  const agg = overall.get("");
  const total = agg?.requests ?? 0;
  const rtt = (q: number) => {
    const us = total ? agg!.hists.rtt?.quantile(q) : null;
    return us == null ? null : us / 1000;
  };
  const p50 = rtt(0.5);
  const lowerBound = result.rollup.truncated.has("transport/proto") ? ["protocol_distribution"] : [];
  return {
    total_requests: total,
    bandwidth_bytes: total ? agg!.respBytes : null,
    tcp_rtt_p50_ms: p50,
    tcp_rtt_p90_ms: rtt(0.9),
    tcp_rtt_p95_ms: rtt(0.95),
    tcp_rtt_p99_ms: rtt(0.99),
    packet_loss_pct: agg?.plossCount ? (agg.plossSum / agg.plossCount) * 100 : null,
    delivery_rate_mbps: agg?.deliveryCount ? ((agg.deliverySum / agg.deliveryCount) * 8) / 1_000_000 : null,
    has_transport_data: total > 0,
    missing_fields: p50 == null ? ["tcp_rtt"] : [],
    protocol_distribution: [...protos]
      .filter(([, p]) => p.requests > 0)
      .sort((a, b) => b[1].requests - a[1].requests || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([protocol, p]) => ({ protocol, requests: p.requests, share_pct: (p.requests * 100) / total })),
    _approx: true,
    _approx_note: lowerBound.length
      ? "TCP RTT percentiles are estimated from summary histograms (≈); protocol counts are lower bounds (≥)."
      : "TCP RTT percentiles are estimated from summary histograms (≈); the other figures are exact.",
    rollup: meta(result, TRANSPORT_APPROXIMATE, lowerBound),
  };
}

/**
 * True when every requested /network/bundle section can come from summaries:
 * the health core sections, the PoP table and transport. Quality, the map and
 * shielding are read alone (/network-health, /network-quality).
 */
function canServeBundleFromRollup(sections: readonly string[] | undefined): boolean {
  return Boolean(sections?.length) &&
    sections!.every((section) => HEALTH_SECTIONS.has(section) || section === "pop_health" || section === "transport");
}

/** Whether summaries may answer this read at all, before any range or listing is resolved. */
export function networkRollupEligible(path: string, params: NetworkQueryParams): boolean {
  if (params.network_residual || params.quality_parts?.length) return false;
  if (params.filter_country || params.filter_pop ||
      params.filter_asn !== undefined && params.filter_asn !== "" ||
      Object.keys(params.filters ?? {}).length) return false;
  if (path === "/api/network/bundle") return canServeBundleFromRollup(params.sections);
  if (path === "/api/network-health") return canServeHealthFromRollup(params.sections, params.map_asn);
  return path === "/api/network/pop-health" || path === "/api/network-quality";
}

/**
 * The summary groups and columns /network reads: one spec for all its paths,
 * so they share one projection per file.
 */
export const NETWORK_SUMMARY_SPEC = defineSummarySpec({
  groups: ["dims", "series5", "quality", "transport", "sample", "shield_routes", "dim_totals"],
  columns: [
    "label", "requests", "status_5xx", "cache_hits", "resp_bytes_sum", "rtt_sum", "rtt_count", "ploss_sum",
    "ploss_count", "cwnd_sum", "cwnd_count", "rttvar_sum", "rttvar_count", "retrans_count", "lat_sum", "lon_sum",
    "geo_count", "delivery_rate_sum", "delivery_rate_count", "throughput_sum", "throughput_count", "ttfb_sum",
    "sample_key", "rtt_hist", "ttfb_hist", "overhead_hist",
  ],
});

/**
 * Answers a Cloud /network read from v3 summaries, or returns null when the raw
 * scan must: any filter (summaries hold no dimension crossed with another, so
 * even one PoP, country or ASN filter can't narrow the ASN, metro or PoP
 * tables), a section that needs rows (shielding, or the map for one ASN), a
 * bucket finer than the summaries, summaries older than the map rows, or a
 * range the summaries don't cover (see `queryRollup`). /network-quality over
 * summaries older than its `quality` rows returns a `NetworkPartial`.
 */
export async function networkFromRollup(
  path: string,
  params: NetworkQueryParams,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files. */
  spec: SummarySpec | null = NETWORK_SUMMARY_SPEC,
) {
  if (!networkRollupEligible(path, params)) return null;
  const fromMs = parseEpochMs(params.start_time);
  const toMs = parseEpochMs(params.end_time);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal, spec: spec ?? undefined });
  if (!result) return null;
  if (path === "/api/network/bundle") {
    const sections = params.sections!;
    const wantsHealth = sections.some((section) => HEALTH_SECTIONS.has(section));
    const core = wantsHealth ? networkHealthFromRollup(result, params) : null;
    if (wantsHealth && core === null) return null;
    const transport = sections.includes("transport") ? transportFromRollup(result) : null;
    if (sections.includes("transport") && transport === null) return null;
    return networkBundle(core, null, sections.includes("pop_health") ? popHealthFromRollup(result) : null, transport);
  }
  if (path === "/api/network-quality") return qualityFromRollup(result, params);
  return path === "/api/network-health" ? healthFromRollup(result, params) : popHealthFromRollup(result);
}
