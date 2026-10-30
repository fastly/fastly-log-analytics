import { useMemo } from 'react'
import { CHART_COLORS } from './colors'
import { formatDate, toUTCDate } from './format'
import type { components } from '@/types/api.generated'
import type { PlotlyTrace } from '@/types/api'

export function parseExtentMs(value: unknown, edge: 'earliest' | 'latest'): number | null {
  if (typeof value !== 'string' || value.length === 0) return null
  const t = Date.parse(value.length === 10 ? `${value}${edge === 'earliest' ? 'T00:00:00.000Z' : 'T23:59:59.999Z'}` : value)
  return Number.isNaN(t) ? null : t
}

const CHART_LAYOUT_DEFAULTS = {
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
  const timesMs = [...new Set(times.map((t) => parseUtcMs(t)).filter((t) => !Number.isNaN(t)))].sort((a, b) => a - b)
  let first = timesMs[0]
  let last = timesMs[timesMs.length - 1]
  const sMs = startTime ? parseUtcMs(startTime) : NaN
  const eMs = endTime ? parseUtcMs(endTime) : NaN
  if (!Number.isNaN(sMs)) {
    const sb = sMs - (sMs % stepMs)
    if (first === undefined || sb < first) first = sb
  }
  if (!Number.isNaN(eMs)) {
    const eb = eMs - (eMs % stepMs)
    if (last === undefined || eb > last) last = eb
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
  if ((nBuckets <= timesMs.length && !startTime && !endTime) || nBuckets > MAX_DENSE_BUCKETS) return null
  return Array.from({ length: nBuckets }, (_, k) => new Date(first + k * stepMs).toISOString())
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
  const grid = denseTimeGrid(time_series.map((d) => d.time), intervalSeconds, startTime, endTime)
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
    return grid.flatMap((iso) => {
      const ms = parseUtcMs(iso)
      return categories.map((cat) => ({ time: iso, value: byKey.get(`${ms}|${cat}`) ?? 0, category: cat }))
    })
  }
  const byMs = new Map<number, number>()
  for (const d of time_series) {
    const ms = parseUtcMs(d.time)
    if (!Number.isNaN(ms)) byMs.set(ms, d.value)
  }
  return grid.map((iso) => ({ time: iso, value: byMs.get(parseUtcMs(iso)) ?? 0 }))
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
    if (t != null) {
      present.push(t)
      byMs.set(Date.parse(t), getValue(r) ?? 0)
    }
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
  return { ...layout, yaxis2: secondaryYAxis(title, extra), margin: { ...layout.margin, r: 50 } }
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

export const INTERVALS = [
  { label: '1s', value: '1 second' },
  { label: '1m', value: '1 minute' },
  { label: '1h', value: '1 hour' },
  { label: '1d', value: '1 day' },
] as const

export type ChartInterval = (typeof INTERVALS)[number]['value']
export const INTERVAL_SECONDS: Record<ChartInterval, number> = {
  '1 second': 1,
  '1 minute': 60,
  '1 hour': 3600,
  '1 day': 86400,
}

export const TRENDS = [
  { label: 'Off', value: 'off' },
  { label: 'Auto', value: 'auto' },
  { label: '1m', value: '1m' },
  { label: '5m', value: '5m' },
  { label: '1h', value: '1h' },
  { label: '1d', value: '1d' },
]

type Aggregates = Partial<Pick<components['schemas']['AggregatesResponse'], 'metric' | 'interval' | 'time_series'>>
type Catalog = { fields?: Pick<components['schemas']['LogFieldCatalogEntry'], 'id' | 'label' | 'unit' | 'precision'>[] | null }

export interface BuildTrafficDataParams {
  aggregates: Aggregates | null | undefined
  compareAggregates: Aggregates | null | undefined
  compareMode: boolean
  compareStartTime: string | null | undefined
  startTime: string | null
  endTime: string | null
  trend: string
  timezone: string
  metric: string
  effectiveInterval: string
  hiddenCategories: ReadonlySet<string>
  catalog: Catalog | null | undefined
}

export function getIntervalSeconds(intervalStr?: string | null): number | undefined {
  if (!intervalStr) return undefined
  const str = intervalStr.trim().toLowerCase()
  if (INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS] !== undefined) {
    return INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS]
  }
  const match = str.match(/^(?:(\d+)\s*)?(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/)
  if (!match) return undefined
  const num = match[1] ? parseInt(match[1], 10) : 1
  const u = match[2][0]
  return u === 's' ? num : u === 'm' ? num * 60 : u === 'h' ? num * 3600 : num * 86400
}

export function buildTrafficData({
  aggregates,
  compareAggregates,
  compareMode,
  compareStartTime,
  startTime,
  endTime,
  trend,
  timezone,
  metric,
  effectiveInterval,
  hiddenCategories,
  catalog,
}: BuildTrafficDataParams): PlotlyTrace[] {
  const time_series = aggregates?.time_series
  if (!time_series?.length) return []
  const actualMetric = aggregates?.metric || metric
  const isBar = actualMetric === 'requests' || actualMetric === '5xx' || actualMetric === '4xx'
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)
  const unit = metricField?.unit || ''
  const precision = metricField?.precision ?? (actualMetric === 'requests' ? 0 : 1)
  const getHoverTemplate = (_m: string, label?: string) =>
    `${label ? `${label}: ` : ''}%{y:${precision > 0 ? `.${precision}f` : ','}}${unit}<extra></extra>`
  const hasCategories = time_series.some((d) => d.category != null)
  const actualInterval = aggregates?.interval || effectiveInterval
  const intervalSeconds = getIntervalSeconds(actualInterval)
  const barSeries: BarSeriesPoint[] = isBar
    ? densifyBarSeries(time_series, intervalSeconds, hasCategories, startTime, endTime)
    : time_series
  let tsXValues: string[] | null = null
  const getTsXValues = (): string[] =>
    (tsXValues ??= time_series.map((d) => formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss')))
  let traces: PlotlyTrace[] = []
  if (hasCategories) {
    const catMap: Record<string, { x: string[]; y: number[] }> = {}
    barSeries.forEach((d) => {
      const cat = d.category || 'Other'
      ;(catMap[cat] ??= { x: [], y: [] }).x.push(formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss'))
      catMap[cat].y.push(d.value)
    })
    const colorMap: Record<string, string> = {
      '400': '#fbbf24',
      '401': CHART_COLORS.warning,
      '403': '#d97706',
      '404': '#b45309',
      '500': CHART_COLORS.danger,
      '502': '#dc2626',
      '503': '#b91c1c',
      '504': '#991b1b',
    }
    traces = Object.entries(catMap).map(([cat, data], i) => ({
      x: data.x,
      y: data.y,
      type: 'bar',
      width: intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
      name: cat,
      showlegend: false,
      visible: hiddenCategories.has(cat) ? 'legendonly' : true,
      hovertemplate: `Status ${cat}: %{y:,}<extra></extra>`,
      marker: { color: colorMap[cat] || `hsl(${(i * 50) % 360}, 70%, 50%)` },
    }))
  } else {
    const xValues =
      barSeries === time_series ? getTsXValues() : barSeries.map((d) => formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss'))
    traces = [
      {
        x: xValues,
        y: barSeries.map((d) => d.value),
        type: isBar ? 'bar' : 'scatter',
        width: isBar && intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
        mode: isBar ? undefined : 'lines+markers',
        name: compareMode ? 'Primary Range' : metricField?.label || actualMetric,
        showlegend: compareMode,
        hovertemplate: getHoverTemplate(actualMetric, compareMode ? 'Primary' : undefined),
        marker: { color: CHART_COLORS.info },
      },
    ]
  }
  if (compareMode && compareAggregates?.time_series?.length && !hasCategories && startTime && compareStartTime) {
    const shift = new Date(startTime).getTime() - new Date(compareStartTime).getTime()
    traces.push({
      x: compareAggregates.time_series.map((d) =>
        formatDate(new Date(toUTCDate(d.time).getTime() + shift).toISOString(), timezone, 'yyyy-MM-dd HH:mm:ss'),
      ),
      y: compareAggregates.time_series.map((d) => d.value),
      type: 'scatter',
      mode: 'lines',
      name: 'Comparison Range',
      line: { color: '#f97316', dash: 'dash', width: 2 },
      hovertemplate: getHoverTemplate(actualMetric, 'Comparison'),
    })
  }
  if (!hasCategories && time_series.some((d) => d.baseline != null)) {
    traces.push({
      x: getTsXValues(),
      y: time_series.map((d) => d.baseline),
      type: 'scatter',
      mode: 'lines',
      name: 'Baseline (7d prior)',
      hovertemplate: getHoverTemplate(actualMetric, 'Baseline'),
      line: { color: '#a1a1aa', dash: 'dot', width: 2 },
    })
  }
  if (!hasCategories && trend !== 'off') {
    const xValues = getTsXValues()
    const yValues = time_series.map((d) => d.value)
    const n = yValues.length
    const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
    const windowSize =
      trend === 'auto'
        ? Math.floor(n / (n > 1000 ? 20 : n > 100 ? 10 : 5))
        : Math.floor((trendMap[trend] ?? 0) / (getIntervalSeconds(actualInterval) ?? 60))
    if (windowSize > 1) {
      const trendY: (number | null)[] = new Array(n).fill(null)
      if (n >= windowSize) {
        let runningSum = 0
        let runningCount = 0
        for (let i = 0; i < n; i++) {
          const enterVal = yValues[i]
          if (enterVal != null) {
            runningSum += enterVal
            runningCount++
          }
          if (i >= windowSize) {
            const exitVal = yValues[i - windowSize]
            if (exitVal != null) {
              runningSum -= exitVal
              runningCount--
            }
          }
          if (i >= windowSize - 1) trendY[i] = runningCount > 0 ? runningSum / runningCount : null
        }
      }
      traces.push({
        x: xValues,
        y: trendY,
        type: 'scatter',
        mode: 'lines',
        name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
        hovertemplate: getHoverTemplate(actualMetric),
        line: { color: '#f97316', width: 3 },
      })
    }
  }
  return traces
}

export const WINDOW_OPTIONS = [
  { label: 'Last 15 Minutes', value: '0.25' },
  { label: 'Last 1 Hour', value: '1' },
  { label: 'Last 4 Hours', value: '4' },
  { label: 'Last 24 Hours', value: '24' },
]

export const BASELINE_OPTIONS = [
  { label: 'Previous 1 Hour', value: '1' },
  { label: 'Last 24 Hours', value: '24' },
  { label: 'Last 7 Days', value: '168' },
  { label: 'Last 30 Days', value: '720' },
]

const STATIC_DEFAULT = { window: '24', baseline: '168' } as const

export function historyHoursFromExtents(earliest: string | null | undefined, now: number = Date.now()): number | null {
  if (!earliest) return null
  const ms = Date.parse(earliest.length === 10 ? earliest + 'T00:00:00.000Z' : earliest)
  return Number.isFinite(ms) ? Math.max(0, (now - ms) / 3_600_000) : null
}

export function pickInsightsDefault(historyHours: number | null): { window: string; baseline: string } {
  if (historyHours == null || !Number.isFinite(historyHours)) {
    return { window: STATIC_DEFAULT.window, baseline: STATIC_DEFAULT.baseline }
  }
  const h = historyHours
  if (h < 1) return { window: '0.25', baseline: '1' }
  if (h < 4) return { window: '1', baseline: '1' }
  if (h < 24) return { window: '4', baseline: '1' }
  if (h < 48) return { window: '4', baseline: '24' }
  if (h < 168) return { window: '24', baseline: '24' }
  return { window: '24', baseline: '168' }
}
