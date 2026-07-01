import { useMemo } from 'react'
import { formatDate } from '@/lib/date'
import { CHART_LAYOUT_DEFAULTS } from '@/lib/constants'

/**
 * Returns the standard Plotly xaxis config for a time-series chart bounded
 * by the current filter range.
 *
 * Replaces the repeated inline xaxis block in page components.
 */
export function makeTimeXAxis(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string
) {
  return {
    range: [
      startTime ? formatDate(startTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
      endTime ? formatDate(endTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
    ],
    nticks: 8,
    tickangle: -45,
    automargin: true,
    type: 'date' as const,
    tickformatstops: CHART_LAYOUT_DEFAULTS.tickformatstops,
  }
}

/** Standard hover + legend layout for time-series charts. */
export const TIME_HOVER_LAYOUT = {
  hovermode: 'x unified' as const,
  legend: {
    orientation: 'h' as const,
    y: 1.15,
    x: 1,
    xanchor: 'right' as const,
    yanchor: 'bottom' as const,
  },
}

/** Memoised combination of TIME_HOVER_LAYOUT + makeTimeXAxis. */
export function useTimeLayout(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string
) {
  return useMemo(
    () => ({ ...TIME_HOVER_LAYOUT, xaxis: makeTimeXAxis(startTime, endTime, timezone) }),
    [startTime, endTime, timezone]
  )
}

/** Safety valve: never synthesize more than this many buckets. Guards against
 * an interval/window mismatch (e.g. 1-min grid over a 30d span) blowing up the
 * point count. Above it, gap-fill no-ops and the chart keeps its sparse bars. */
const MAX_DENSE_BUCKETS = 5000

/**
 * Build the full contiguous list of bucket timestamps (UTC ISO) from the first
 * to the last *present* bucket, spaced by `intervalSeconds`.
 *
 * Plotly sizes every bar to the *smallest* gap between adjacent x-values, so a
 * sparse COUNT series — e.g. a single low-traffic route, or a quiet hour with no
 * scored requests, where the backend's `GROUP BY` emits only buckets that have
 * rows — collapses bars to hairlines and turns empty buckets into ambiguous
 * gaps. Re-indexing the series onto this dense grid (filling the holes with 0)
 * makes the minimum gap equal the interval, so bars regain an even width and an
 * empty bucket honestly reads as "0", not "no data".
 *
 * Returns the ISO grid, or `null` when the caller should keep its original axis
 * because a grid can't be built safely: unknown interval, fewer than 2 distinct
 * buckets, buckets not on a single interval grid (mixed grain — filling would
 * silently drop a value), no gaps to fill, or a grid larger than
 * MAX_DENSE_BUCKETS.
 *
 * COUNT/rate bar metrics only — never use for latency/throughput scatter, where
 * a missing bucket is undefined, not 0. Anchored on the real DB bucket
 * boundaries (DuckDB `time_bucket`/`date_trunc` space buckets exactly by the
 * interval), so synthesized buckets land on the same instants. Lookups against
 * the grid must key by `Date.parse` (epoch ms), not the raw string, so a `Z` vs
 * `+00:00` suffix difference between the grid and the source rows can't miss.
 */
export function denseTimeGrid(
  times: Array<string | number | null | undefined>,
  intervalSeconds: number | undefined,
): string[] | null {
  if (!intervalSeconds || intervalSeconds <= 0) return null
  const stepMs = intervalSeconds * 1000

  const timesMs = [
    ...new Set(
      times
        .map((t) => (t == null ? NaN : typeof t === 'number' ? t : Date.parse(t)))
        .filter((t) => !Number.isNaN(t)),
    ),
  ].sort((a, b) => a - b)
  if (timesMs.length < 2) return null

  const first = timesMs[0]
  const last = timesMs[timesMs.length - 1]
  // Bail if any present bucket is off the interval grid — filling would silently
  // drop its value. DuckDB guarantees alignment, so this only trips on a mixed
  // or unexpected grain.
  if (!timesMs.every((t) => (t - first) % stepMs === 0)) return null

  const nBuckets = Math.round((last - first) / stepMs) + 1
  // Already contiguous (no gaps) or grid too large to be worth/safe filling.
  if (nBuckets <= timesMs.length || nBuckets > MAX_DENSE_BUCKETS) return null

  const grid: string[] = []
  for (let k = 0; k < nBuckets; k++) grid.push(new Date(first + k * stepMs).toISOString())
  return grid
}

/** One bucket of a bar time-series: a parseable timestamp, a numeric value,
 * and an optional stacked-bar category. Extra fields on the source rows are
 * ignored by the densifier. */
export interface BarSeriesPoint {
  time: string
  value: number
  category?: string | number | null
}

/**
 * Zero-fill empty buckets in a COUNT-style bar series so the x-axis is
 * contiguous at the bucket interval. Thin wrapper over {@link denseTimeGrid}
 * that re-pivots a `{ time, value, category? }[]` series onto the dense grid.
 *
 * No-ops (returns the input array unchanged) whenever `denseTimeGrid` declines
 * to build a grid — see its doc for the conditions. Callers that hold a flat
 * `{ time, value }` series (the dashboard traffic chart) use this; callers that
 * map their own row shape onto an axis call `denseTimeGrid` directly.
 */
export function densifyBarSeries(
  time_series: BarSeriesPoint[],
  intervalSeconds: number | undefined,
  hasCategories: boolean,
): BarSeriesPoint[] {
  const grid = denseTimeGrid(
    time_series.map((d) => d.time),
    intervalSeconds,
  )
  if (!grid) return time_series

  if (hasCategories) {
    // Keyed by (bucketMs, category) with the category order as first seen, so
    // every category shares the dense grid and stacks cleanly.
    const categories: string[] = []
    const seen = new Set<string>()
    const byKey = new Map<string, number>()
    for (const d of time_series) {
      const ms = Date.parse(d.time)
      if (Number.isNaN(ms)) continue
      const cat = d.category != null ? String(d.category) : 'Other'
      if (!seen.has(cat)) { seen.add(cat); categories.push(cat) }
      byKey.set(`${ms}|${cat}`, d.value)
    }
    const out: BarSeriesPoint[] = []
    for (const iso of grid) {
      const ms = Date.parse(iso)
      for (const cat of categories) {
        out.push({ time: iso, value: byKey.get(`${ms}|${cat}`) ?? 0, category: cat })
      }
    }
    return out
  }

  const byMs = new Map<number, number>()
  for (const d of time_series) {
    const ms = Date.parse(d.time)
    if (!Number.isNaN(ms)) byMs.set(ms, d.value)
  }
  return grid.map((iso) => ({ time: iso, value: byMs.get(Date.parse(iso)) ?? 0 }))
}
