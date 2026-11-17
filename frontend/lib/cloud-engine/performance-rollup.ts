import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rollupGroup } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import {
  MIN_ASN_REQUESTS,
  MIN_URL_REQUESTS,
  PERF_TOP_N,
  resolveSortColumn,
  SCATTER_SAMPLE_SIZE,
  shapePerformanceAsnRows,
  shapePerfScatterRows,
  shapePerfTopUrlRows,
  type PerformanceAggregatesResponse,
  type PerfTtlDistItem,
} from "./performance";
import { hasFilterValues, PERFORMANCE_ROLLUP_SECTIONS } from "./rollup-sections";

export type PerformanceRollupBody = {
  start_time?: string | number | null;
  end_time?: string | number | null;
  filters?: unknown;
  sections?: readonly string[] | null;
  sort_by?: string | null;
};

/** `buildTtlBucketCaseSql`'s buckets, in its `MIN(ttl)` order. */
const TTL_BUCKETS: [string, number][] = [["< 60s", 60], ["1m - 1h", 3600], ["1h - 1d", 86400], ["> 1d", Infinity]];

/** `buildPerfTtlDistSql` from the `all_fields` TTL counts; values `TRY_CAST(... AS DOUBLE)` drops are skipped. */
function ttlDist(counts: Map<string, number> | undefined): PerfTtlDistItem[] {
  const totals = new Map<string, number>();
  for (const [value, count] of counts ?? []) {
    const ttl = value.trim() === "" ? NaN : Number(value);
    if (Number.isNaN(ttl)) continue;
    const bucket = TTL_BUCKETS.find(([, below]) => ttl < below)?.[0] ?? "> 1d";
    totals.set(bucket, (totals.get(bucket) ?? 0) + count);
  }
  return TTL_BUCKETS.filter(([bucket]) => totals.has(bucket))
    .map(([bucket]) => ({ bucket, count: totals.get(bucket)! }));
}

/**
 * `buildPerfLatencyRankingSql` from the `urls` rows: requests and the average
 * count only rows with an elapsed, so both are exact; percentiles come from
 * the elapsed histogram and so does the ranking unless sorted by average.
 * Null when a summary predates the rows.
 */
function latencyRanking(result: RollupResult, key: "url" | "asn", minRequests: number, sortBy: string | null | undefined) {
  const rows = rollupGroup(result.rollup, "urls", key);
  if (!rows) return null;
  const sortColumn = resolveSortColumn(sortBy);
  const quantile = (agg: RollupAgg, q: number) => {
    const us = agg.hists.elapsed?.quantile(q);
    return us == null ? null : us / 1000;
  };
  return [...rows]
    .filter(([value, agg]) => value !== "" && agg.elapsedCount > minRequests)
    .map(([value, agg]) => ({
      [key]: value,
      req_count: agg.elapsedCount,
      avg_ms: agg.elapsedSum / agg.elapsedCount / 1000,
      p50_ms: quantile(agg, 0.5),
      p95_ms: quantile(agg, 0.95),
      p99_ms: quantile(agg, 0.99),
    } as Record<string, unknown>))
    .sort((a, b) => {
      const x = a[sortColumn] as number | null;
      const y = b[sortColumn] as number | null;
      return x == null ? (y == null ? 0 : 1) : y == null ? -1 : y - x;
    })
    .slice(0, PERF_TOP_N);
}

/**
 * `buildPerfScatterSql` from the `sample` rows: each summary keeps the rows
 * with a TTFB with the smallest sample keys, so the smallest of their union
 * are a uniform sample of the range. Raw orders by another hash, so the points
 * differ, and dropping rows without an elapsed or with a negative edge time
 * after sampling can leave fewer than raw's 300.
 */
function scatter(result: RollupResult) {
  return shapePerfScatterRows((result.rollup.samples ?? [])
    .toSorted((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : a.tsMs - b.tsMs)
    .slice(0, SCATTER_SAMPLE_SIZE)
    .flatMap((sample) => {
      if (sample.elapsedUs === null) return [];
      const origin = sample.ttfbS * 1000;
      const edge = sample.elapsedUs / 1000 - origin;
      return edge >= 0 ? [{ origin_ms: origin, edge_ms: edge, cache: sample.cache ?? "" }] : [];
    }));
}

/** Whether summaries may answer this /performance read at all, before any range or listing is resolved. */
export function performanceRollupEligible(path: string, body: PerformanceRollupBody): boolean {
  if (!/^\/api\/performance(?:\/aggregates)?\/*$/.test(path) || hasFilterValues(body.filters)) return false;
  const servable = new Set<string>(PERFORMANCE_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * The requested /performance waterfall averages, TTL buckets, URL and network
 * rankings and scatter sample from merged summaries, shaped like
 * `executePerformanceAggregates`. The waterfall and TTL buckets are exact: the
 * waterfall sums the same per-row components, and TTL counts every value
 * unless a summary cut TTLs to its top K, when the buckets are lower bounds.
 * Ranking percentiles are approximate, and their counts lower bounds when a
 * summary cut URLs or networks to its top K. The scatter is labelled
 * approximate as a different sample from raw's. Null when a summary predates the
 * ranking rows.
 */
export function performanceAggregatesFromRollup(
  result: RollupResult,
  body: PerformanceRollupBody,
): (PerformanceAggregatesResponse & { rollup: NetworkRollupMeta }) | null {
  const sections = new Set(body.sections ?? []);
  const out: PerformanceAggregatesResponse = { _approx: false };
  const { waterfall, fields, truncated } = result.rollup;
  const approximate: string[] = [];
  const lowerBound: string[] = [];
  for (const [section, key, minRequests] of [
    ["top_urls", "url", MIN_URL_REQUESTS], ["top_asns", "asn", MIN_ASN_REQUESTS],
  ] as const) {
    if (!sections.has(section)) continue;
    const rows = latencyRanking(result, key, minRequests, body.sort_by);
    if (!rows) return null;
    if (section === "top_urls") out.top_urls = shapePerfTopUrlRows(rows);
    else out.top_asns = shapePerformanceAsnRows(rows);
    out._approx = true;
    approximate.push(`${section}.p50`, `${section}.p95`, `${section}.p99`);
    if (truncated.has(`urls/${key}`)) lowerBound.push(`${section}.requests`);
  }
  if (sections.has("waterfall")) {
    const n = waterfall?.count ?? 0;
    const avg = (sum: number) => Math.max(0, roundTo(sum / n, 2));
    out.waterfall = {
      avg: waterfall && n > 0 ? {
        edge_processing: avg(waterfall.edgeProcessing),
        origin_wait: avg(waterfall.originWait),
        origin_download: avg(waterfall.originDownload),
        client_download: avg(waterfall.clientDownload),
      } : null,
    };
  }
  if (sections.has("ttl_dist")) out.ttl_dist = ttlDist(fields.get("ttl"));
  if (sections.has("scatter")) {
    out.scatter = scatter(result);
    approximate.push("scatter");
  }
  return {
    ...out,
    rollup: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate,
      lower_bound: sections.has("ttl_dist") && truncated.has("all_fields/ttl") ? [...lowerBound, "ttl_dist"] : lowerBound,
    },
  };
}

/**
 * Answers an unfiltered Cloud /performance read of the waterfall, TTL,
 * ranking and scatter sections from v3 summaries, or returns null when the raw
 * scan must: a filter, summaries without the ranking rows, or a range the
 * summaries don't cover (see `queryRollup`).
 */
export async function performanceFromRollup(
  path: string,
  body: PerformanceRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
) {
  if (!performanceRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal });
  return result ? performanceAggregatesFromRollup(result, body) : null;
}
