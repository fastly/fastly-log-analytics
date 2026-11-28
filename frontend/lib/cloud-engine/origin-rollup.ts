import type { Histogram } from "./dashboard/histogram";
import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { emptyAgg, queryRollup, rebucketSeries, rollupGroup, sumAggs } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import {
  emptyOriginAggregates,
  emptyOriginSummary,
  enrichShieldingRouteWithPopDistance,
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
import { hasFilterValues, originRollupSections, singleLowCardFilter, type FilteredRollupFilter } from "./rollup-sections";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";
import { ORIGIN_RESIDUAL } from "./origin-residual";

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
  active_log_field_ids?: readonly string[];
  edge_only?: boolean;
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
      hist: latencyHistogram(agg),
    }));
  const kept = all.filter(({ agg }) => agg.ostTotal >= 10);
  const rows: OriginIpHealthRow[] = [];
  for (const { oip, agg, hist } of kept) {
    if (hist === undefined || agg.ostTotal !== agg.requests) return undefined;
    rows.push({
      oip,
      requests: agg.ostTotal,
      p50_ms: ms(hist, 0.5),
      p95_ms: ms(hist, 0.95),
      error_pct: agg.ostTotal ? roundTo((agg.ost5xx * 100) / agg.ostTotal, 1) : null,
    });
  }
  const sorted = rows
    .sort((a, b) => (b.p95_ms ?? -Infinity) - (a.p95_ms ?? -Infinity) || b.requests - a.requests ||
      (a.oip < b.oip ? -1 : a.oip > b.oip ? 1 : 0))
    .slice(0, Math.max(1, Math.min(100, Math.trunc(limit))));
  return { has_data: sorted.length > 0, rows: sorted };
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
    if (!url) return [];
    const requests = agg.hists.ttfb?.count() ?? 0;
    return requests ? [{ url, requests, hist: agg.hists.ttfb }] : [];
  });
  const min = Math.max(1, Math.trunc(body.slow_urls_min_requests ?? 50));
  const kept = all.filter((row) => row.requests >= min);
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
  const requiresGroupC = Array.isArray(body.active_log_field_ids)
    ? !body.active_log_field_ids.includes("edge")
    : false;
  return shapeOriginPopLatencyRows(rows, { requiresGroupC });
}

/** Whether the series bucket needs the per-minute `series1` rows: it doesn't tile 5 minutes. */
const minuteSeries = (body: OriginRollupBody) => (body.bucket_minutes ?? 5) % 5 !== 0;

/**
 * The origin latency series of `buildOriginTimeseriesSql` per bucket, and per
 * path leg when split, from the `series5` `origin` histograms (`series1` for
 * buckets that don't tile 5 minutes): TTFB counts rows with an origin latency
 * and TTLB rows with an OTTLB, as raw does. The unsplit 5-minute OTTLB series
 * reads the overall histograms, which older summaries carry too; every row has
 * a leg row, so the legs sum to it.
 */
function latencySeries(result: RollupResult, body: OriginRollupBody): OriginTimeseriesSection | null {
  const metric = body.timeseries_metric === "ttlb" ? "ttlb" : "ttfb";
  const percentile = body.timeseries_percentile ?? "p95";
  const bucketMinutes = body.bucket_minutes ?? 5;
  const group = minuteSeries(body) ? "series1" : "series5";
  if (metric === "ttlb" && !body.split_by_leg && group === "series5") {
    return ottlbSeries(result, bucketMinutes, percentile);
  }
  if (!result.rollup.present?.has(`${group}/origin`)) return null;
  const rebucketed = rebucketSeries(result.rollup, "origin", bucketMinutes * 60_000, group);
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
  if (hasFilterValues(body.filters, body.custom_filters)) {
    if (path !== "/api/origin/aggregates") return false;
    if (!Array.isArray(body.sections) || body.sections.length === 0 || !body.sections.every((s) => s === "summary")) {
      return false;
    }
    return singleLowCardFilter(body.filters, body.custom_filters, body) !== null;
  }
  if (path === "/api/origin/shielding-analysis") return true;
  if (path !== "/api/origin/aggregates") return false;
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
const APPROX_EXACT_NOTE = "Percentiles are estimated from summary histograms (≈). Counts are exact.";
const APPROX_LOWER_BOUND_NOTE =
  "Percentiles are estimated from summary histograms (≈). Request counts are lower bounds (≥): summaries keep only their busiest values.";
const APPROX_TIMESERIES_NOTE = "Percentiles are estimated from summary histograms (≈). Miss counts are exact.";

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
    const truncatedIp = result.rollup.truncated.has("dims/oip");
    if (section.has_data) {
      out.ip_health = {
        ...section,
        _approx: true,
        _approx_note: truncatedIp ? APPROX_LOWER_BOUND_NOTE : APPROX_EXACT_NOTE,
      };
      approximate.push("ip_health.p50_ms", "ip_health.p95_ms");
      if (truncatedIp) {
        approximate.push("ip_health.error_pct");
        lowerBound.push("ip_health.requests");
      }
    } else {
      out.ip_health = section;
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
    if (section.has_data) {
      out.timeseries = { ...section, _approx: true, _approx_note: APPROX_TIMESERIES_NOTE };
      approximate.push("timeseries.value");
    } else {
      out.timeseries = section;
    }
  }
  if (sections.has("summary")) {
    const section = originSummary(result);
    if (!section) return null;
    if (section.has_data) {
      out.summary = { ...section, _approx: true, _approx_note: APPROX_EXACT_NOTE };
      approximate.push(
        "summary.ottfb_p50_ms", "summary.ottfb_p75_ms", "summary.ottfb_p95_ms", "summary.ottfb_p99_ms",
        "summary.ottlb_p50_ms", "summary.ottlb_p95_ms", "summary.cdn_overhead_p50_ms", "summary.obytes_p50",
      );
    } else {
      out.summary = section;
    }
  }
  if (sections.has("slow_urls")) {
    const section = slowUrls(result, body);
    if (!section) return null;
    const truncatedUrls = result.rollup.truncated.has("urls/url");
    if (section.has_data) {
      out.slow_urls = {
        ...section,
        _approx: true,
        _approx_note: truncatedUrls ? APPROX_LOWER_BOUND_NOTE : APPROX_EXACT_NOTE,
      };
      approximate.push("slow_urls.p50_ms", "slow_urls.p95_ms", "slow_urls.p99_ms");
      if (truncatedUrls) lowerBound.push("slow_urls.requests");
    } else {
      out.slow_urls = section;
    }
  }
  if (sections.has("path_breakdown")) {
    const section = pathBreakdown(result);
    if (!section) return null;
    if (section.has_data) {
      out.path_breakdown = { ...section, _approx: true, _approx_note: APPROX_EXACT_NOTE };
      approximate.push("path_breakdown.p50_ms", "path_breakdown.p95_ms");
    } else {
      out.path_breakdown = section;
    }
  }
  if (sections.has("pop_latency")) {
    const section = popLatency(result, body);
    if (!section) return null;
    const truncatedPops = result.rollup.truncated.has("origin/pop");
    if (section.has_data) {
      out.pop_latency = {
        ...section,
        _approx: true,
        _approx_note: truncatedPops ? APPROX_LOWER_BOUND_NOTE : APPROX_EXACT_NOTE,
      };
      approximate.push("pop_latency.p50_ms", "pop_latency.p95_ms");
      if (truncatedPops) lowerBound.push("pop_latency.requests");
    } else {
      out.pop_latency = section;
    }
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

/**
 * /network and /origin shielding analysis from `shield_routes` (window-local join
 * of edge `rid` to shield `prid`), ranked by requests desc and p50 desc.
 * Returns null when summaries predate `shield_routes`.
 */
export function shieldingFromRollup(
  result: RollupResult,
  limit = 50,
  options?: { active_log_field_ids?: readonly string[]; edge_only?: boolean },
) {
  const routes = rollupGroup(result.rollup, "shield_routes", "route");
  if (!routes) return null;
  const safeLimit = Math.max(1, Math.min(200, Math.trunc(limit)));
  const shaped = [...routes].map(([key, agg]) => {
    const [edge_pop, shield_pop] = JSON.parse(key) as [string, string];
    const h = agg.hists.overhead;
    return enrichShieldingRouteWithPopDistance({
      edge_pop,
      shield_pop,
      requests: agg.requests,
      p50_ms: ms(h, 0.5),
      p95_ms: ms(h, 0.95),
      p99_ms: ms(h, 0.99),
    });
  });
  const byReqs = [...shaped].sort((a, b) => b.requests - a.requests);
  const byP50 = [...shaped].sort(
    (a, b) => (b.p50_ms ?? -Infinity) - (a.p50_ms ?? -Infinity) || b.requests - a.requests,
  );
  const keep = new Set([...byReqs.slice(0, safeLimit), ...byP50.slice(0, safeLimit)]);
  const rows = byReqs.filter((r) => keep.has(r));
  const total_routes = routes.size;
  const truncated = total_routes > rows.length || result.rollup.truncated.has("shield_routes/route");
  const activeSet = Array.isArray(options?.active_log_field_ids)
    ? new Set(options.active_log_field_ids)
    : null;
  const requiresFields = activeSet
    ? ["edge", "rid", "prid", "pop"].filter((f) => !activeSet.has(f))
    : ([] as string[]);
  return {
    has_data: rows.length > 0 && requiresFields.length === 0,
    requires_fields: requiresFields,
    edge_only: typeof options?.edge_only === "boolean" ? options.edge_only : false,
    rows: requiresFields.length > 0 ? [] : rows,
    total_routes: requiresFields.length > 0 ? 0 : total_routes,
    truncated: requiresFields.length > 0 ? false : truncated,
    error: false,
    _approx: true as const,
    _approx_note: "≈ (window-local join)",
  };
}

function filteredOriginSummary(
  result: RollupResult,
  filter: FilteredRollupFilter,
): (OriginAggregatesResponse & { rollup: NetworkRollupMeta }) | null {
  const group = rollupGroup(result.rollup, "filtered_rollups", filter.field);
  if (!group) return null;
  const isTruncated = result.rollup.truncated.has(`filtered_rollups/${filter.field}`);
  const found = group.get(filter.value);
  if (!found && isTruncated) return null;
  const agg = found ?? emptyAgg();
  const out = emptyOriginAggregates();
  const { ottfb, ottlb } = agg.hists;
  out.summary = {
    ...(ottfb?.count()
      ? shapeOriginSummaryRow({
        total_misses: agg.missRequests,
        total_passes: agg.passRequests,
        ottfb_p50_ms: ms(ottfb, 0.5),
        ottfb_p75_ms: ms(ottfb, 0.75),
        ottfb_p95_ms: ms(ottfb, 0.95),
        ottfb_p99_ms: ms(ottfb, 0.99),
        ottlb_p50_ms: ms(ottlb, 0.5),
        ottlb_p95_ms: ms(ottlb, 0.95),
        cdn_overhead_p50_ms: null,
        origin_error_rate: agg.ostTotal ? agg.ost5xx / agg.ostTotal : null,
        obytes_p50: null,
      })
      : emptyOriginSummary()),
    _approx: true,
    _approx_note: isTruncated ? APPROX_LOWER_BOUND_NOTE : APPROX_EXACT_NOTE,
  };
  out.has_data = out.summary.has_data;
  return {
    ...out,
    rollup: {
      data_from: new Date(result.dataFromMs).toISOString(),
      data_through: new Date(result.dataThroughMs).toISOString(),
      approximate: [
        "summary.ottfb_p50_ms", "summary.ottfb_p75_ms", "summary.ottfb_p95_ms", "summary.ottfb_p99_ms",
        "summary.ottlb_p50_ms", "summary.ottlb_p95_ms",
      ],
      lower_bound: isTruncated ? ["summary"] : [],
    },
  };
}

const ORIGIN_COLUMNS = [
  "count", "requests", "ost_5xx_count", "ost_total_count", "lat_us_count", "miss_requests", "pass_requests",
  "ttfb_hist", "ottfb_hist", "ottlb_hist", "overhead_hist", "obytes_hist",
];
const ORIGIN_GROUPS = ["dims", "all_fields", "field_totals", "origin", "urls", "shield_routes", "dim_totals"];
const ORIGIN_ONLY_BASE = {
  count: ["all_fields"],
  requests: ["dims", "origin", "shield_routes"],
  ost_5xx_count: ["dims", "origin"],
  ost_total_count: ["dims", "origin"],
  lat_us_count: ["dims"],
  miss_requests: ["origin"],
  pass_requests: ["origin"],
  ottfb_hist: ["dims"],
  overhead_hist: ["origin", "shield_routes"],
  obytes_hist: ["origin"],
};
const ORIGIN_FIELDS_BASE = {
  all_fields: ["ost"],
  dims: ["oip"],
  urls: ["url"],
};

/** The summary groups and columns unfiltered /origin reads. */
export const ORIGIN_SUMMARY_SPEC = defineSummarySpec({
  groups: [...ORIGIN_GROUPS, "series5"],
  columns: ORIGIN_COLUMNS,
  only: {
    ...ORIGIN_ONLY_BASE,
    ttfb_hist: ["dims", "origin", "urls", "series5"],
    ottlb_hist: ["origin", "series5"],
  },
  fields: {
    ...ORIGIN_FIELDS_BASE,
    series5: ["origin", "overall"],
  },
});

/** `ORIGIN_SUMMARY_SPEC` for a series whose bucket doesn't tile 5 minutes: per-minute rows instead. */
export const ORIGIN_MINUTE_SUMMARY_SPEC = defineSummarySpec({
  groups: [...ORIGIN_GROUPS, "series1"],
  columns: ORIGIN_COLUMNS,
  only: {
    ...ORIGIN_ONLY_BASE,
    ttfb_hist: ["dims", "origin", "urls", "series1"],
    ottlb_hist: ["origin", "series1"],
  },
  fields: {
    ...ORIGIN_FIELDS_BASE,
    series1: ["origin"],
  },
});

/** Summary spec for single-filter /origin summary reads (`filtered_rollups` only). */
export const ORIGIN_FILTERED_SUMMARY_SPEC = defineSummarySpec({
  groups: ["filtered_rollups", "dim_totals"],
  columns: ["miss_requests", "pass_requests", "ost_5xx_count", "ost_total_count", "ottfb_hist", "ottlb_hist"],
  only: {
    miss_requests: ["filtered_rollups"],
    pass_requests: ["filtered_rollups"],
    ost_5xx_count: ["filtered_rollups"],
    ost_total_count: ["filtered_rollups"],
    ottfb_hist: ["filtered_rollups"],
    ottlb_hist: ["filtered_rollups"],
  },
});

/**
 * Answers a Cloud /origin aggregates or shielding-analysis read from
 * v3 summaries, or returns null when the raw scan must: unsupported filters, a
 * section or series option summaries don't hold, or a range they don't cover
 * (see `queryRollup`).
 */
export async function originFromRollup(
  path: string,
  body: OriginRollupBody & { limit?: number },
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files; undefined picks the unfiltered, minute, or filtered spec. */
  spec?: SummarySpec | null,
) {
  if (!originRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time);
  const toMs = parseEpochMs(body.end_time);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const filtered = hasFilterValues(body.filters, body.custom_filters);
  const effectiveSpec = spec === undefined
    ? (filtered
      ? ORIGIN_FILTERED_SUMMARY_SPEC
      : body.sections?.includes("timeseries") && minuteSeries(body)
        ? ORIGIN_MINUTE_SUMMARY_SPEC
        : ORIGIN_SUMMARY_SPEC)
    : spec;
  const result = await queryRollup(source, {
    fromMs,
    toMs,
    signal,
    spec: effectiveSpec ?? undefined,
    residual: filtered ? undefined : ORIGIN_RESIDUAL,
  });
  if (!result) return null;
  if (hasFilterValues(body.filters, body.custom_filters)) {
    const filter = singleLowCardFilter(body.filters, body.custom_filters, body);
    return filter ? filteredOriginSummary(result, filter) : null;
  }
  if (path === "/api/origin/shielding-analysis") {
    const shielding = shieldingFromRollup(result, body.limit ?? 50, {
      active_log_field_ids: body.active_log_field_ids,
      edge_only: body.edge_only,
    });
    if (!shielding) return null;
    return {
      ...shielding,
      rollup: {
        data_from: new Date(result.dataFromMs).toISOString(),
        data_through: new Date(result.dataThroughMs).toISOString(),
        approximate: ["p50_ms", "p95_ms", "p99_ms"],
        lower_bound: result.rollup.truncated.has("shield_routes/route") ? ["total_routes"] : [],
      },
    };
  }
  return originAggregatesFromRollup(result, body);
}
