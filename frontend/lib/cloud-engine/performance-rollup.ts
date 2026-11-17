import type { RollupResult } from "./dashboard/rollup-query";
import { queryRollup } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import type { PerformanceAggregatesResponse, PerfTtlDistItem } from "./performance";
import { hasFilterValues, PERFORMANCE_ROLLUP_SECTIONS } from "./rollup-sections";

export type PerformanceRollupBody = {
  start_time?: string | number | null;
  end_time?: string | number | null;
  filters?: unknown;
  sections?: readonly string[] | null;
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

/** Whether summaries may answer this /performance read at all, before any range or listing is resolved. */
export function performanceRollupEligible(path: string, body: PerformanceRollupBody): boolean {
  if (!/^\/api\/performance(?:\/aggregates)?\/*$/.test(path) || hasFilterValues(body.filters)) return false;
  const servable = new Set<string>(PERFORMANCE_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * The requested /performance waterfall averages and TTL buckets from merged
 * summaries, shaped like `executePerformanceAggregates`. Both are exact: the
 * waterfall sums the same per-row components, and TTL counts every value
 * unless a summary cut TTLs to its top K, when the buckets are lower bounds.
 */
export function performanceAggregatesFromRollup(
  result: RollupResult,
  body: PerformanceRollupBody,
): PerformanceAggregatesResponse & { rollup: NetworkRollupMeta } {
  const sections = new Set(body.sections ?? []);
  const out: PerformanceAggregatesResponse = { _approx: false };
  const { waterfall, fields, truncated } = result.rollup;
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
  if (sections.has("ttl_dist")) out.ttl_dist = ttlDist(fields?.get("ttl"));
  return {
    ...out,
    rollup: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate: [],
      lower_bound: sections.has("ttl_dist") && truncated.has("fields/ttl") ? ["ttl_dist"] : [],
    },
  };
}

/**
 * Answers an unfiltered Cloud /performance read of the waterfall and TTL
 * sections from v3 summaries, or returns null when the raw scan must: a
 * filter, a row-level section (URL and ASN rankings, the scatter sample), or a
 * range the summaries don't cover (see `queryRollup`).
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
