import type { Histogram } from "./dashboard/histogram";
import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries, rollupGroup, sumAggs } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import {
  emptyOriginAggregates,
  emptyOriginSummary,
  shapeOriginPathBreakdownRows,
  shapeOriginPopLatencyRows,
  shapeOriginSlowUrlsRows,
  shapeOriginSummaryRow,
  type OriginAggregatesResponse,
  type OriginIpHealthRow,
  type OriginIpHealthSection,
  type OriginPathBreakdownSection,
  type OriginPopLatencySection,
  type OriginSlowUrlsSection,
  type OriginStatusCodesSection,
  type OriginSummarySection,
  type OriginTimeseriesPoint,
  type OriginTimeseriesSection,
} from "./origin";
import { hasFilterValues, originRollupSections } from "./rollup-sections";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";

export type OriginRollupBody = {
  start_time?: string | number;
  end_time?: string | number;
  filters?: unknown;
  custom_filters?: unknown;
  bucket_minutes?: number;
  split_by_leg?: boolean;
  timeseries_metric?: string;
  timeseries_percentile?: string;
  ip_health_limit?: number;
  slow_urls_limit?: number;
  slow_urls_min_requests?: number;
  pop_latency_limit?: number;
  sections?: readonly string[] | null;
};

const PERCENTILES: Record<string, number> = { p50: 0.5, p95: 0.95, p99: 0.99 };

/**
 * The histogram that holds an origin IP's raw latency, `COALESCE(ottfb, ttfb * 1e6)`:
 * OTTFB when every row with a latency has one, client TTFB when none does. A mix
 * is in neither histogram, so it returns undefined and raw must answer.
 */
function latencyHistogram(agg: RollupAgg): Histogram | null | undefined {
  if (agg.latUsCount === 0) return null;
  const { ottfb, ttfb } = agg.hists;
  const ottfbCount = ottfb?.count() ?? 0;
  if (ottfbCount === agg.latUsCount) return ottfb;
  if (ottfbCount === 0 && (ttfb?.count() ?? 0) === agg.latUsCount) return ttfb;
  return undefined;
}

const ms = (h: Histogram | null, q: number) => {
  const us = h?.quantile(q);
  return us == null ? null : roundTo(us / 1000, 2);
};

/**
 * `queryOriginIpHealthWithFallback` from the `dims` origin IP rows. Requests
 * count rows with an origin status, as raw does, so a row is only served when
 * every row for that IP has one; error shares are exact; latency comes from the
 * IP's OTTFB or TTFB histogram. Undefined when a shown IP fails either check.
 */
function ipHealth(result: RollupResult, limit: number): OriginIpHealthSection | undefined {
  const all = [...result.rollup.dims.get("oip") ?? []]
    .filter(([oip]) => oip !== "" && oip !== "-")
    .map(([oip, agg]) => ({
      oip, agg,
      errorPct: agg.ostTotal ? roundTo((agg.ost5xx * 100) / agg.ostTotal, 1) : null,
    }));
  const pick = (min: number) => all.filter(({ agg }) => agg.ostTotal >= min);
  const kept = pick(10).length ? pick(10) : pick(1);
  const shown = kept
    .sort((a, b) => (b.errorPct ?? -1) - (a.errorPct ?? -1) || b.agg.ostTotal - a.agg.ostTotal ||
      (a.oip < b.oip ? -1 : a.oip > b.oip ? 1 : 0))
    .slice(0, Math.max(1, Math.min(100, Math.trunc(limit))));
  const rows: OriginIpHealthRow[] = [];
  for (const { oip, agg, errorPct } of shown) {
    const hist = latencyHistogram(agg);
    if (hist === undefined || agg.ostTotal !== agg.requests) return undefined;
    rows.push({ oip, requests: agg.ostTotal, p50_ms: ms(hist, 0.5), p95_ms: ms(hist, 0.95), error_pct: errorPct });
  }
  return { has_data: rows.length > 0, rows };
}

/**
 * `buildOriginStatusCodesSql` from the `all_fields` origin status counts, which
 * skip null statuses as raw does; null when they were cut to a top K.
 */
function statusCodes(result: RollupResult): OriginStatusCodesSection | null {
  if (result.rollup.truncated.has("all_fields/ost")) return null;
  const counts = new Map<number, number>();
  for (const [value, count] of result.rollup.fields.get("ost") ?? []) {
    const ost = Number(value);
    if (value === "" || !Number.isFinite(ost) || count <= 0) continue;
    const status = ost >= 100 && ost <= 599 ? ost : -1;
    counts.set(status, (counts.get(status) ?? 0) + count);
  }
  let total = 0;
  for (const count of counts.values()) total += count;
  const rows = [...counts]
    .sort((a, b) => b[1] - a[1] || a[0] - b[0])
    .map(([status, count]) => ({ status, count, pct: roundTo((count * 100) / total, 2) }));
  return { has_data: rows.length > 0, rows };
}

const clampLimit = (limit: number | undefined, fallback: number) =>
  Math.max(1, Math.min(100, Math.trunc(limit ?? fallback)));

const legValue = (leg: string) => leg === "true" ? true : leg === "false" ? false : null;

/**
 * `buildOriginSummarySql` from the overall `origin` row, which sums rows with an
 * origin latency as raw does: MISS and PASS counts and the 5xx share are exact,
 * percentiles come from histograms. Undefined when a summary predates the row,
 * or when the median CDN overhead falls among negative values, which a histogram
 * reads as its minimum.
 */
function originSummary(result: RollupResult): OriginSummarySection | undefined {
  const agg = rollupGroup(result.rollup, "origin", "overall")?.get("");
  if (agg === undefined) return result.rollup.present?.has("origin/overall") ? emptyOriginSummary() : undefined;
  const { ttfb, ottlb, overhead, obytes } = agg.hists;
  if (!ttfb?.count()) return emptyOriginSummary();
  const n = overhead?.count() ?? 0;
  if (overhead && overhead.min < 0 && Math.ceil(n / 2) <= overhead.under) return undefined;
  return shapeOriginSummaryRow({
    total_misses: agg.missRequests,
    total_passes: agg.passRequests,
    ottfb_p50_ms: ms(ttfb, 0.5),
    ottfb_p75_ms: ms(ttfb, 0.75),
    ottfb_p95_ms: ms(ttfb, 0.95),
    ottfb_p99_ms: ms(ttfb, 0.99),
    ottlb_p50_ms: ms(ottlb, 0.5),
    ottlb_p95_ms: ms(ottlb, 0.95),
    cdn_overhead_p50_ms: ms(overhead, 0.5),
    origin_error_rate: agg.ostTotal ? agg.ost5xx / agg.ostTotal : null,
    obytes_p50: obytes?.quantile(0.5) ?? null,
  });
}

/**
 * `queryOriginSlowUrlsWithFallback` from the `urls` rows: per URL, the origin
 * latency histogram counts the rows raw ranks, so requests are exact unless the
 * URLs were cut to a top K. Ranked by the histograms' p95.
 */
function slowUrls(result: RollupResult, body: OriginRollupBody): OriginSlowUrlsSection | null {
  const urls = rollupGroup(result.rollup, "urls", "url");
  if (!urls) return null;
  const all = [...urls].flatMap(([url, agg]) => {
    const requests = agg.hists.ttfb?.count() ?? 0;
    return requests ? [{ url, requests, hist: agg.hists.ttfb }] : [];
  });
  const min = Math.max(1, Math.trunc(body.slow_urls_min_requests ?? 10));
  let kept = all.filter((row) => row.requests >= min);
  if (!kept.length) kept = all;
  const rows = kept
    .map((row) => ({ ...row, p95: row.hist!.quantile(0.95)! }))
    .sort((a, b) => b.p95 - a.p95 || b.requests - a.requests || (a.url < b.url ? -1 : a.url > b.url ? 1 : 0))
    .slice(0, clampLimit(body.slow_urls_limit, 20))
    .map(({ url, requests, hist }) => ({
      url, requests, p50_ms: ms(hist, 0.5), p95_ms: ms(hist, 0.95), p99_ms: ms(hist, 0.99),
    }));
  return shapeOriginSlowUrlsRows(rows);
}

/** `buildOriginPathBreakdownSql` from the `origin` rows per edge flag (true, false, then null). */
function pathBreakdown(result: RollupResult): OriginPathBreakdownSection | null {
  const legs = rollupGroup(result.rollup, "origin", "edge");
  if (!legs) return null;
  const rows = ["true", "false", ""].flatMap((leg) => {
    const agg = legs.get(leg);
    const hist = agg?.hists.ttfb ?? null;
    return agg?.requests ? [{ edge: legValue(leg), requests: agg.requests, p50_ms: ms(hist, 0.5), p95_ms: ms(hist, 0.95) }] : [];
  });
  return shapeOriginPathBreakdownRows(rows);
}

/** `buildOriginPopLatencySql` from the `origin` rows per POP, ranked by the histograms' p95. */
function popLatency(result: RollupResult, body: OriginRollupBody): OriginPopLatencySection | null {
  const pops = rollupGroup(result.rollup, "origin", "pop");
  if (!pops) return null;
  const rows = [...pops]
    .filter(([pop, agg]) => pop !== "" && agg.requests > 0)
    .map(([pop, agg]) => ({ pop, requests: agg.requests, p50_ms: ms(agg.hists.ttfb, 0.5), p95_ms: ms(agg.hists.ttfb, 0.95) }))
    .sort((a, b) => (b.p95_ms ?? -1) - (a.p95_ms ?? -1) || b.requests - a.requests || (a.pop < b.pop ? -1 : 1))
    .slice(0, clampLimit(body.pop_latency_limit, 30));
  return shapeOriginPopLatencyRows(rows, { requiresGroupC: false });
}

/**
 * The origin latency series of `buildOriginTimeseriesSql` per bucket, and per
 * path leg when split, from the `series5` `origin` histograms: TTFB counts rows
 * with an origin latency and TTLB rows with an OTTLB, as raw does. The unsplit
 * OTTLB series reads the overall histograms, which older summaries carry too.
 */
function latencySeries(result: RollupResult, body: OriginRollupBody): OriginTimeseriesSection | null {
  const metric = body.timeseries_metric === "ttlb" ? "ttlb" : "ttfb";
  const percentile = body.timeseries_percentile ?? "p95";
  const bucketMinutes = body.bucket_minutes ?? 5;
  if (metric === "ttlb" && !body.split_by_leg) return ottlbSeries(result, bucketMinutes, percentile);
  if (!result.rollup.present?.has("series5/origin")) return null;
  const rebucketed = rebucketSeries(result.rollup, "origin", bucketMinutes * 60_000);
  if (!rebucketed) return null;
  const key = metric === "ttlb" ? "ottlb" : "ttfb";
  const q = PERCENTILES[percentile] ?? 0.95;
  const point = (t: number, agg: RollupAgg, leg?: string): OriginTimeseriesPoint[] => {
    const count = agg.hists[key]?.count() ?? 0;
    if (!count) return [];
    const p: OriginTimeseriesPoint = { time: new Date(t).toISOString(), miss_count: count, value: ms(agg.hists[key], q) };
    if (leg !== undefined) p.edge = legValue(leg);
    return [p];
  };
  const times = [...new Set([...rebucketed.values()].flatMap((buckets) => [...buckets.keys()]))].sort((a, b) => a - b);
  const series = times.flatMap((t) => body.split_by_leg
    ? ["true", "false", ""].flatMap((leg) => {
      const agg = rebucketed.get(leg)?.get(t);
      return agg ? point(t, agg, leg) : [];
    })
    : point(t, sumAggs([...rebucketed.values()].flatMap((buckets) => buckets.get(t) ?? []))));
  return { has_data: series.length > 0, series };
}

/** The OTTLB series of `buildOriginTimeseriesSql` from the overall `series5` histograms. */
function ottlbSeries(result: RollupResult, bucketMinutes: number, percentile: string): OriginTimeseriesSection | null {
  const rebucketed = rebucketSeries(result.rollup, "overall", bucketMinutes * 60_000);
  if (!rebucketed) return null;
  const q = PERCENTILES[percentile] ?? 0.95;
  const series: OriginTimeseriesPoint[] = [...rebucketed.get("") ?? []]
    .sort((a, b) => a[0] - b[0])
    .flatMap(([t, agg]) => {
      const count = agg.hists.ottlb?.count() ?? 0;
      return count ? [{ time: new Date(t).toISOString(), miss_count: count, value: ms(agg.hists.ottlb, q) }] : [];
    });
  return { has_data: series.length > 0, series };
}

/** Whether summaries may answer this /origin read at all, before any range or listing is resolved. */
export function originRollupEligible(path: string, body: OriginRollupBody): boolean {
  if (path !== "/api/origin/aggregates" || hasFilterValues(body.filters, body.custom_filters)) return false;
  const servable = new Set<string>(originRollupSections({
    metric: body.timeseries_metric ?? "ttfb",
    bucketMinutes: body.bucket_minutes ?? 5,
    splitByLeg: body.split_by_leg,
  }));
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * The requested /origin aggregates sections from merged summaries, shaped like
 * `queryOriginAggregates`, or null when a section can't match raw (a summary
 * without its rows, a cut status list, a series bucket that doesn't tile; see
 * each section's reader). Counts and shares are exact unless a top K cut them;
 * percentiles come from histograms.
 */
export function originAggregatesFromRollup(
  result: RollupResult,
  body: OriginRollupBody,
): (OriginAggregatesResponse & { rollup: NetworkRollupMeta }) | null {
  const sections = new Set(body.sections ?? []);
  const out = emptyOriginAggregates();
  const approximate: string[] = [];
  const lowerBound: string[] = [];
  if (sections.has("ip_health")) {
    const section = ipHealth(result, body.ip_health_limit ?? 30);
    if (!section) return null;
    out.ip_health = section;
    approximate.push("ip_health.p50_ms", "ip_health.p95_ms");
    if (result.rollup.truncated.has("dims/oip")) {
      approximate.push("ip_health.error_pct");
      lowerBound.push("ip_health.requests");
    }
  }
  if (sections.has("status_codes")) {
    const section = statusCodes(result);
    if (!section) return null;
    out.status_codes = section;
  }
  if (sections.has("timeseries")) {
    const section = latencySeries(result, body);
    if (!section) return null;
    out.timeseries = section;
    approximate.push("timeseries.value");
  }
  if (sections.has("summary")) {
    const section = originSummary(result);
    if (!section) return null;
    out.summary = section;
    approximate.push(
      "summary.ottfb_p50_ms", "summary.ottfb_p75_ms", "summary.ottfb_p95_ms", "summary.ottfb_p99_ms",
      "summary.ottlb_p50_ms", "summary.ottlb_p95_ms", "summary.cdn_overhead_p50_ms", "summary.obytes_p50",
    );
  }
  if (sections.has("slow_urls")) {
    const section = slowUrls(result, body);
    if (!section) return null;
    out.slow_urls = section;
    approximate.push("slow_urls.p50_ms", "slow_urls.p95_ms", "slow_urls.p99_ms");
    if (result.rollup.truncated.has("urls/url")) lowerBound.push("slow_urls.requests");
  }
  if (sections.has("path_breakdown")) {
    const section = pathBreakdown(result);
    if (!section) return null;
    out.path_breakdown = section;
    approximate.push("path_breakdown.p50_ms", "path_breakdown.p95_ms");
  }
  if (sections.has("pop_latency")) {
    const section = popLatency(result, body);
    if (!section) return null;
    out.pop_latency = section;
    approximate.push("pop_latency.p50_ms", "pop_latency.p95_ms");
    if (result.rollup.truncated.has("origin/pop")) lowerBound.push("pop_latency.requests");
  }
  out.has_data = out.summary.has_data || out.timeseries.has_data || out.slow_urls.has_data ||
    out.status_codes.has_data || out.path_breakdown.has_data || out.pop_latency.has_data || out.ip_health.has_data;
  return {
    ...out,
    rollup: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate,
      lower_bound: lowerBound,
    },
  };
}

/** The summary groups and columns /origin reads. */
export const ORIGIN_SUMMARY_SPEC = defineSummarySpec({
  groups: ["dims", "series5", "all_fields", "field_totals", "origin", "urls", "dim_totals"],
  columns: [
    "count", "requests", "ost_5xx_count", "ost_total_count", "lat_us_count", "miss_requests", "pass_requests",
    "ttfb_hist", "ottfb_hist", "ottlb_hist", "overhead_hist", "obytes_hist",
  ],
});

/**
 * Answers an unfiltered Cloud /origin aggregates read from v3 summaries, or
 * returns null when the raw scan must: a filter, a section or series option
 * summaries don't hold, or a range they don't cover (see `queryRollup`).
 */
export async function originFromRollup(
  path: string,
  body: OriginRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files. */
  spec: SummarySpec | null = ORIGIN_SUMMARY_SPEC,
) {
  if (!originRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time);
  const toMs = parseEpochMs(body.end_time);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal, spec: spec ?? undefined });
  return result ? originAggregatesFromRollup(result, body) : null;
}
