import type { Histogram } from "./dashboard/histogram";
import type { RollupAgg, RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import {
  emptyOriginAggregates,
  type OriginAggregatesResponse,
  type OriginIpHealthRow,
  type OriginIpHealthSection,
  type OriginTimeseriesPoint,
  type OriginTimeseriesSection,
} from "./origin";
import { hasFilterValues, originRollupSections } from "./rollup-sections";

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
 * The requested /origin aggregates sections (origin IP health and the OTTLB
 * series) from merged summaries, shaped like `queryOriginAggregates`, or null
 * when a section can't match raw (see `ipHealth` and `rebucketSeries`). Counts
 * and error shares are exact; percentiles come from histograms.
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
  if (sections.has("timeseries")) {
    const section = ottlbSeries(result, body.bucket_minutes ?? 5, body.timeseries_percentile ?? "p95");
    if (!section) return null;
    out.timeseries = section;
    approximate.push("timeseries.value");
  }
  out.has_data = out.ip_health.has_data || out.timeseries.has_data;
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
 * Answers an unfiltered Cloud /origin aggregates read from v3 summaries, or
 * returns null when the raw scan must: a filter, a section or series option
 * summaries don't hold, or a range they don't cover (see `queryRollup`).
 */
export async function originFromRollup(
  path: string,
  body: OriginRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
) {
  if (!originRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time);
  const toMs = parseEpochMs(body.end_time);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal });
  return result ? originAggregatesFromRollup(result, body) : null;
}
