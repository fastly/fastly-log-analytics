import { useMemo } from 'react'
import { denseTimeGrid } from '@/lib/chart-helpers'
import { formatDate } from '@/lib/date'
import type { PlotlyTrace } from '@/types/plotly'

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
 * Shared Plotly trace/layout builders. They replace the hand-built bar and
 * line traces and the right-hand secondary-axis layout that were copied across
 * the scorer, compliance and streaming charts.
 */

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
 *
 * Given an array of objects like:
 * [{ time: '2023-01-01', http2: 100, http3: 50 }, ...]
 *
 * Returns Plotly traces based on the provided configuration.
 */
export function useTimeseriesToTraces(
  data: readonly TimeseriesDataPoint[] | undefined,
  configs: TraceConfig[],
  timezone?: string
): PlotlyTrace[] {
  return useMemo((): PlotlyTrace[] => {
    if (!data || data.length === 0) return []

    // Ensure times are sorted for Plotly (though the backend should do this)
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

      // Build trace WITHOUT explicit `undefined` properties — Plotly's
      // schema validator silently rejects the whole trace if it sees
      // `stackgroup: undefined` or `marker: undefined` for a non-bar
      // chart, leaving the canvas empty with no console error. This is
      // why the Origin Latency chart "stopped working" even though the
      // data was fully present.
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
