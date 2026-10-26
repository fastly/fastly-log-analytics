import { useMemo } from 'react'
import { formatDate, toUTCDate } from '@/lib/format'
import type { PlotlyTrace } from '@/types/api'

export const CHART_LAYOUT_DEFAULTS = {
  tickformatstops: [
    { dtickrange: [0, 1000], value: '%H:%M:%S.%f' },
    { dtickrange: [1000, 60000], value: '%H:%M:%S' },
    { dtickrange: [60000, 3600000], value: '%H:%M' },
    { dtickrange: [3600000, 86400000], value: '%H:%M<br>%b %d' },
    { dtickrange: [86400000, 604800000], value: '%b %d' },
    { dtickrange: [604800000, 'M1'], value: '%b %d' },
    { dtickrange: ['M1', 'M12'], value: '%b %Y' },
    { dtickrange: ['M12', null], value: '%Y' },
  ],
}

export function makeTimeXAxis(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string,
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

export function useTimeLayout(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string,
) {
  return useMemo(
    () => ({ ...TIME_HOVER_LAYOUT, xaxis: makeTimeXAxis(startTime, endTime, timezone) }),
    [startTime, endTime, timezone],
  )
}

const MAX_DENSE_BUCKETS = 5000

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
    ...new Set(times.map((t) => parseUtcMs(t)).filter((t) => !Number.isNaN(t))),
  ].sort((a, b) => a - b)

  let first = timesMs[0]
  let last = timesMs[timesMs.length - 1]

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

  if (
    !timesMs.every((t) => {
      const rem = Math.abs((t - first) % stepMs)
      return rem === 0 || rem < 1000 || Math.abs(rem - stepMs) < 1000
    })
  ) {
    return null
  }

  const nBuckets = Math.round((last - first) / stepMs) + 1
  if (nBuckets <= timesMs.length && !startTime && !endTime) return null
  if (nBuckets > MAX_DENSE_BUCKETS) return null

  const grid: string[] = []
  for (let k = 0; k < nBuckets; k++) grid.push(new Date(first + k * stepMs).toISOString())
  return grid
}

export interface BarSeriesPoint {
  time: string
  value: number
  category?: string | number | null
}

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
    const categories: string[] = []
    const seen = new Set<string>()
    const byKey = new Map<string, number>()
    for (const d of time_series) {
      const ms = parseUtcMs(d.time)
      if (Number.isNaN(ms)) continue
      const cat = d.category != null ? String(d.category) : 'Other'
      if (!seen.has(cat)) {
        seen.add(cat)
        categories.push(cat)
      }
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

export const CHART_COLORS = {
  success: '#10b981',
  danger: '#ef4444',
  warning: '#f59e0b',
  info: '#3b82f6',
  neutral: '#94a3b8',
} as const

export function statusHex(code: number): string {
  if (code >= 500) return CHART_COLORS.danger
  if (code >= 400) return CHART_COLORS.warning
  if (code >= 300) return CHART_COLORS.info
  return CHART_COLORS.success
}

export function denseBarTrace<R>(opts: {
  name: string
  rows: R[]
  getTime: (row: R) => string | null | undefined
  getValue: (row: R) => number | null | undefined
  intervalSeconds: number
  color: string
  yaxis?: 'y2'
  showlegend?: boolean
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

export function useTimeseriesToTraces(
  data: readonly TimeseriesDataPoint[] | undefined,
  configs: TraceConfig[],
  timezone?: string,
): PlotlyTrace[] {
  return useMemo((): PlotlyTrace[] => {
    if (!data?.length) return []

    const sortedData = [...data]
      .filter((d) => d && (d.time || d.ts))
      .sort((a, b) => (a.time || a.ts || '').localeCompare(b.time || b.ts || ''))

    const xValues = sortedData.map((d) => {
      const t = d.time || d.ts || ''
      return timezone ? formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss') : t
    })

    return configs.map((config) => {
      const yValues = sortedData.map((d) => Number((d as Record<string, unknown>)[config.key]) || 0)
      const isBar = config.type === 'bar'
      return {
        x: xValues,
        y: yValues,
        name: config.name,
        type: config.type || 'scatter',
        fill: config.fill || 'tozeroy',
        ...(isBar
          ? { marker: { color: config.color } }
          : { mode: 'lines' as const, line: { width: 1.5, color: config.color } }),
        ...(config.stackgroup ? { stackgroup: config.stackgroup } : {}),
      }
    })
  }, [data, configs, timezone])
}
