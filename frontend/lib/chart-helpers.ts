import { useMemo } from 'react'
import { formatDate, toUTCDate } from '@/lib/date'
import { CHART_LAYOUT_DEFAULTS } from '@/lib/utils'
import type { PlotlyTrace } from '@/types/plotly'

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
    y: 1.05,
    x: 0.5,
    xanchor: 'center' as const,
    yanchor: 'bottom' as const,
    font: { size: 9.5 },
    bgcolor: 'transparent',
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
function parseUtcMs(t: string | number | null | undefined): number {
  if (t == null) return NaN
  if (typeof t === 'number') return Math.round(t / 1000) * 1000
  const parsed = toUTCDate(t).getTime()
  const raw = Number.isNaN(parsed) ? Date.parse(t) : parsed
  return Number.isNaN(raw) ? NaN : Math.round(raw / 1000) * 1000
}

export function denseTimeGrid(
  times: Array<string | number | null | undefined>,
  intervalSeconds: number | undefined,
  startTime?: string | null,
  endTime?: string | null,
): string[] | null {
  if (!intervalSeconds || intervalSeconds <= 0) return null
  const stepMs = intervalSeconds * 1000

  const timesMs = [
    ...new Set(
      times
        .map((t) => parseUtcMs(t))
        .filter((t) => !Number.isNaN(t)),
    ),
  ].sort((a, b) => a - b)

  let first = timesMs.length > 0 ? timesMs[0] : undefined
  let last = timesMs.length > 0 ? timesMs[timesMs.length - 1] : undefined

  if (startTime) {
    const sMs = parseUtcMs(startTime)
    if (!Number.isNaN(sMs)) {
      const startBucket = sMs - (sMs % stepMs)
      if (first === undefined || startBucket < first) first = startBucket
    }
  }
  if (endTime) {
    const eMs = parseUtcMs(endTime)
    if (!Number.isNaN(eMs)) {
      const endBucket = eMs - (eMs % stepMs)
      if (last === undefined || endBucket > last) last = endBucket
    }
  }

  if (first === undefined || last === undefined || first > last) return null

  // Bail if any present bucket is off the interval grid — filling would silently
  // drop its value. DuckDB guarantees alignment, so this only trips on a mixed
  // or unexpected grain. Tolerates sub-second rounding or boundary jitter.
  if (
    !timesMs.every((t) => {
      const rem = Math.abs((t - first) % stepMs)
      return rem === 0 || rem < 1000 || Math.abs(rem - stepMs) < 1000
    })
  ) {
    return null
  }

  const nBuckets = Math.round((last - first) / stepMs) + 1
  // Already contiguous (no gaps) or grid too large to be worth/safe filling.
  if (nBuckets <= timesMs.length && !startTime && !endTime) return null
  if (nBuckets > MAX_DENSE_BUCKETS) return null

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
  startTime?: string | null,
  endTime?: string | null,
): BarSeriesPoint[] {
  const grid = denseTimeGrid(
    time_series.map((d) => d.time),
    intervalSeconds,
    startTime,
    endTime,
  )
  if (!grid) return time_series

  if (hasCategories) {
    // Keyed by (bucketMs, category) with the category order as first seen, so
    // every category shares the dense grid and stacks cleanly.
    const categories: string[] = []
    const seen = new Set<string>()
    const byKey = new Map<string, number>()
    for (const d of time_series) {
      const ms = parseUtcMs(d.time)
      if (Number.isNaN(ms)) continue
      const cat = d.category != null ? String(d.category) : 'Other'
      if (!seen.has(cat)) { seen.add(cat); categories.push(cat) }
      byKey.set(`${ms}|${cat}`, d.value)
    }
    const out: BarSeriesPoint[] = []
    for (const iso of grid) {
      const ms = parseUtcMs(iso)
      for (const cat of categories) {
        out.push({ time: iso, value: byKey.get(`${ms}|${cat}`) ?? 0, category: cat })
      }
    }
    return out
  }

  const byMs = new Map<number, number>()
  for (const d of time_series) {
    const ms = parseUtcMs(d.time)
    if (!Number.isNaN(ms)) byMs.set(ms, d.value)
  }
  return grid.map((iso) => ({ time: iso, value: byMs.get(parseUtcMs(iso)) ?? 0 }))
}

// ── Chart Colors & Trace Builders ─────────────────────────────────────────────

/**
 * Semantic hex colours for Plotly traces, maps and other canvas/SVG surfaces
 * that cannot read CSS variables. Tailwind 500-series values; keep in sync with
 * the status/severity intent, not with individual charts.
 */
export const CHART_COLORS = {
  success: '#10b981',
  danger: '#ef4444',
  warning: '#f59e0b',
  info: '#3b82f6',
  neutral: '#94a3b8',
} as const

/** HTTP status code to chart colour: 2xx success, 3xx info, 4xx warning, 5xx danger. */
export function statusHex(code: number): string {
  if (code >= 500) return CHART_COLORS.danger
  if (code >= 400) return CHART_COLORS.warning
  if (code >= 300) return CHART_COLORS.info
  return CHART_COLORS.success
}

/**
 * Bar trace on a contiguous time grid: empty buckets become 0 so a sparse
 * window renders even-width bars and a quiet bucket reads as "0" rather than a
 * gap. Values are looked up by epoch ms (the grid's `Z` suffix and a row's
 * `+00:00` suffix are the same instant). Falls back to the buckets that are
 * present when `denseTimeGrid` declines to build a grid.
 */
export function denseBarTrace<R>(opts: {
  name: string
  rows: R[]
  getTime: (row: R) => string | null | undefined
  getValue: (row: R) => number | null | undefined
  intervalSeconds: number
  color: string
  yaxis?: 'y2'
  showlegend?: boolean
  /** Reuse a grid shared by several traces (stacked bars). */
  grid?: string[]
}) {
  const { name, rows, getTime, getValue, intervalSeconds, color, yaxis, showlegend, grid } = opts
  const present: string[] = []
  const byMs = new Map<number, number>()
  for (const r of rows) {
    const t = getTime(r)
    if (t == null) continue
    present.push(t)
    byMs.set(Date.parse(t), getValue(r) ?? 0)
  }
  const x = grid ?? denseTimeGrid(present, intervalSeconds) ?? present
  return {
    type: 'bar' as const,
    name,
    x,
    y: x.map((h) => byMs.get(Date.parse(h)) ?? 0),
    ...(yaxis ? { yaxis } : {}),
    marker: { color },
    ...(showlegend !== undefined ? { showlegend } : {}),
  }
}

/** Sparse line trace; gaps are connected because a missing bucket has no defined value. */
export function lineTrace(opts: {
  name: string
  x: Array<string | null | undefined>
  y: Array<number | null>
  color: string
  width?: number
  dash?: 'dot' | 'dash'
  yaxis?: 'y2'
  connectgaps?: boolean
}) {
  const { name, x, y, color, width = 2, dash, yaxis, connectgaps } = opts
  return {
    type: 'scatter' as const,
    mode: 'lines' as const,
    name,
    x,
    y,
    ...(yaxis ? { yaxis } : {}),
    line: { color, width, dash },
    ...(connectgaps !== undefined ? { connectgaps } : {}),
  }
}

/** A right-hand y axis overlaying the primary one (grid hidden by default). */
export function secondaryYAxis(
  title: string,
  extra: { ticksuffix?: string; rangemode?: 'tozero' | 'normal' | 'nonnegative'; showgrid?: boolean } = {},
) {
  return {
    title,
    overlaying: 'y' as const,
    side: 'right' as const,
    showgrid: false,
    rangemode: 'tozero' as const,
    ...extra,
  }
}

/** Adds yaxis2 and widens the right margin so its labels are not clipped. */
export function withSecondaryAxis<L extends { margin?: { r?: number } }>(
  layout: L,
  title: string,
  extra?: Parameters<typeof secondaryYAxis>[1],
) {
  return {
    ...layout,
    yaxis2: secondaryYAxis(title, extra),
    margin: { ...layout.margin, r: 50 },
  }
}

/** A row of a bucketed series: a `time` (or legacy `ts`) plus numeric metric keys. */
export interface TimeseriesDataPoint {
  time?: string | null
  ts?: string | null
}

interface TraceConfig {
  key: string
  name: string
  color: string
  stackgroup?: string
  fill?: 'none' | 'tozeroy' | 'tonexty' | 'toself'
  type?: 'scatter' | 'bar'
}

/**
 * Transforms flat backend timeseries data into Plotly trace arrays.
 */
export function useTimeseriesToTraces(
  data: readonly TimeseriesDataPoint[] | undefined,
  configs: TraceConfig[],
  timezone?: string
): PlotlyTrace[] {
  return useMemo((): PlotlyTrace[] => {
    if (!data || data.length === 0) return []

    const sortedData = [...data]
      .filter(d => d && (d.time || d.ts))
      .sort((a, b) => {
        const ta = a.time || a.ts || ''
        const tb = b.time || b.ts || ''
        return ta.localeCompare(tb)
      })

    const xValues = sortedData.map(d => {
      const t = d.time || d.ts || ''
      return timezone ? formatDate(t, timezone, "yyyy-MM-dd HH:mm:ss") : t
    })

    return configs.map(config => {
      const yValues = sortedData.map(d => Number((d as Record<string, unknown>)[config.key]) || 0)
      const isBar = config.type === 'bar'

      const trace: PlotlyTrace = {
        x: xValues,
        y: yValues,
        name: config.name,
        type: config.type || 'scatter',
        fill: config.fill || 'tozeroy',
      }
      if (isBar) {
        trace.marker = { color: config.color }
      } else {
        trace.mode = 'lines'
        trace.line = { width: 1.5, color: config.color }
      }
      if (config.stackgroup) {
        trace.stackgroup = config.stackgroup
      }
      return trace
    })
  }, [data, configs, timezone])
}
