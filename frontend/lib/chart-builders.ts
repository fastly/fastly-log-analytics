import { denseTimeGrid } from '@/lib/chart-helpers'

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
