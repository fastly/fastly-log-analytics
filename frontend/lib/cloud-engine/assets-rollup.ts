import { computeSummaryFromDims, emptyAssetsAggregates, selectAssetsSections, type DimRowInput } from "./assets";
import type { RollupResult } from "./dashboard/rollup-query";
import { queryRollup } from "./dashboard/rollup-query";
import { parseEpochMs } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import { ASSETS_ROLLUP_SECTIONS, assetsRollupCacheFilter } from "./rollup-sections";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";

export type AssetsRollupBody = {
  start_time?: string | number | null;
  end_time?: string | number | null;
  filters?: unknown;
  sections?: readonly string[] | null;
};

const ASCII = /^[\x00-\x7f]*$/;

/** Whether summaries may answer this /assets read at all, before any range or listing is resolved. */
export function assetsRollupEligible(path: string, body: AssetsRollupBody): boolean {
  if (!/^\/api\/assets\/aggregates\/*$/.test(path) || assetsRollupCacheFilter(body.filters) === false) return false;
  const servable = new Set<string>(ASSETS_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * The /assets totals and breakdowns from the `assets` rows' `[asset_type,
 * cache, encoding]` sums, shaped like `queryAssetsAggregates` with sections
 * `["summary"]`, or null when they can't match it exactly: the summaries cut
 * the keys to their top K, or a key holds non-ASCII text (summaries case-fold
 * ASCII only; DuckDB folds all of Unicode). A cache filter is applied to the
 * keys only when the logged `cache` values are all upper case, none is
 * `OTHER` (the key's stand-in for no cache value) and no `cache_status`
 * column outranks `cache`, so the key's cache is exactly the filtered column.
 */
export function assetsAggregatesFromRollup(result: RollupResult, body: AssetsRollupBody) {
  const { assets, overview, fields, truncated } = result.rollup;
  if (truncated.has("assets/asset")) return null;
  const cacheFilter = assetsRollupCacheFilter(body.filters);
  if (cacheFilter === false) return null;
  if (cacheFilter) {
    const logged = fields.get("cache");
    if (truncated.has("all_fields/cache") || fields.has("cache_status")) return null;
    for (const value of logged?.keys() ?? []) {
      if (value === "OTHER" || value !== value.toUpperCase()) return null;
    }
  }
  const rows: DimRowInput[] = [];
  let requests = 0;
  for (const [key, asset] of assets ?? []) {
    const [assetType, cache, encoding] = JSON.parse(key) as [string, string, string];
    if (!ASCII.test(cache) || !ASCII.test(encoding)) return null;
    requests += asset.requests;
    if (cacheFilter && (cacheFilter.mode === "include"
      ? cache === "OTHER" || !cacheFilter.values.includes(cache)
      : cache !== "OTHER" && cacheFilter.values.includes(cache))) continue;
    rows.push({
      asset_type: assetType,
      cache_status: cache,
      content_encoding: encoding,
      requests: asset.requests,
      total_bytes: asset.respBytes,
      sum_ttfb: asset.ttfbSum,
      sum_origin_ms: asset.originWaitSum,
      origin_count: asset.originRequests,
    });
  }
  // Every request lands in one key, so a shortfall means a summary without `assets` rows.
  if (requests !== (overview?.requests ?? 0)) return null;
  const meta: NetworkRollupMeta = {
    data_from: new Date(result.dataFromMs).toISOString(),
    data_through: new Date(result.dataThroughMs).toISOString(),
    approximate: [],
    lower_bound: [],
  };
  if (rows.length === 0) return { ...selectAssetsSections(emptyAssetsAggregates(), ["summary"]), rollup: meta };
  rows.sort((a, b) => [a.asset_type!, a.cache_status!, a.content_encoding!].join("\0")
    .localeCompare([b.asset_type!, b.cache_status!, b.content_encoding!].join("\0")));
  const computed = computeSummaryFromDims(rows);
  // Cloud reads the fixed log schema, which always has `ttl` and `resp_header_content_encoding`.
  return {
    missing_fields: [] as string[],
    summary: {
      total_requests: computed.total_requests,
      total_bytes: computed.total_bytes,
      overall_hit_ratio: computed.overall_hit_ratio,
      overall_hit_ratio_pct: computed.overall_hit_ratio_pct,
      avg_ttfb_ms: computed.avg_ttfb_ms,
      avg_origin_ms: computed.avg_origin_ms,
    },
    asset_type_breakdown: computed.asset_type_breakdown,
    cache_performance: computed.cache_performance,
    compression_performance: computed.compression_performance,
    rollup: meta,
  };
}

/** The summary groups and columns /assets reads. */
export const ASSETS_SUMMARY_SPEC = defineSummarySpec({
  groups: ["overview", "assets", "all_fields", "field_totals", "dim_totals"],
  columns: ["requests", "resp_bytes_sum", "ttfb_sum", "origin_wait_sum", "origin_requests"],
});

/**
 * Answers a Cloud /assets read of the summary section, unfiltered or filtered
 * on cache status alone, from v3 summaries, or returns null when the raw scan
 * must: another filter, a URL-level section (large uncompressed and low-TTL
 * assets), or a range the summaries don't cover (see `queryRollup`).
 */
export async function assetsFromRollup(
  path: string,
  body: AssetsRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files. */
  spec: SummarySpec | null = ASSETS_SUMMARY_SPEC,
) {
  if (!assetsRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal, spec: spec ?? undefined });
  return result ? assetsAggregatesFromRollup(result, body) : null;
}
