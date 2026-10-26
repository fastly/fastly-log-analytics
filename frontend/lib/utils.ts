import { useMemo, useRef } from 'react'
import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { usePathname, useRouter, useSearchParams } from 'next/navigation'
import 'nuqs'
import { differenceInSeconds, formatDistanceToNow, parseISO, subHours } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'
import { ShieldAlert, ServerCrash, Zap, Network, Globe, LayoutGrid, type LucideIcon } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'
import type { FiltersPayload, PlotlyTrace } from '@/types/api'

export function ensureNuqsAdapter(): void {
  if (typeof window === 'undefined') return
  const ctx = (window as unknown as { __NuqsAdapterContext?: { _currentValue?: unknown; _currentValue2?: unknown } }).__NuqsAdapterContext
  if (!ctx) return
  const fallback = {
    useAdapter() {
      const firstRef = useRef(true)
      if (firstRef.current) {
        firstRef.current = false
        ;(globalThis as Record<symbol, { abort?: () => void }>)[Symbol.for('nuqs.2.10.2.throttle-queue')]?.abort?.()
      }
      const router = useRouter()
      const pathname = usePathname() || '/'
      const sp = useSearchParams() ?? new URLSearchParams()
      return {
        searchParams: sp,
        getSearchParamsSnapshot: () => new URLSearchParams(sp.toString()),
        pathname,
        rateLimitFactor: 0,
        autoResetQueueOnUpdate: true,
        updateUrl: (search: URLSearchParams, options: { history?: 'push' | 'replace'; scroll?: boolean; shallow?: boolean }) => {
          const qs = search.toString()
          const url = qs ? `${pathname}?${qs}` : pathname
          if (!options.shallow) (options.history === 'push' ? router?.push : router?.replace)?.(url, { scroll: options.scroll ?? false })
        },
      }
    },
  }
  ctx._currentValue = fallback
  ctx._currentValue2 = fallback
}
ensureNuqsAdapter()

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

const _numFmtCache = new Map<string, Intl.NumberFormat>()
function getNumberFormat(locale?: string, options?: Intl.NumberFormatOptions): Intl.NumberFormat {
  const key = `${locale ?? ''}|${options ? JSON.stringify(options) : ''}`
  let fmt = _numFmtCache.get(key)
  if (!fmt) _numFmtCache.set(key, (fmt = new Intl.NumberFormat(locale, options)))
  return fmt
}

const _dtFmtCache = new Map<string, Intl.DateTimeFormat>()
function getDateTimeFormat(locale: string | undefined, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `${locale ?? ''}|${JSON.stringify(options)}`
  let fmt = _dtFmtCache.get(key)
  if (!fmt) _dtFmtCache.set(key, (fmt = new Intl.DateTimeFormat(locale, options)))
  return fmt
}

const _relFmt = typeof Intl !== 'undefined' && typeof Intl.RelativeTimeFormat === 'function'
  ? new Intl.RelativeTimeFormat('en', { numeric: 'always', style: 'narrow' })
  : null

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  let value = bytes, i = 0
  while (value >= 1000 && i < BYTE_UNITS.length - 1) { value /= 1024; i++ }
  return `${parseFloat(value.toFixed(2))} ${BYTE_UNITS[i]}`
}

export function formatBytesTiered(
  bytes: number | null | undefined,
  options: { maxUnit?: 'MB' | 'GB'; nullText?: string } = {},
): string {
  const { maxUnit = 'MB', nullText = '–' } = options
  if (bytes == null) return nullText
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (maxUnit === 'MB' || bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

interface ElapsedOptions {
  minutesFrom?: number
  hoursFrom?: number
  daysFrom?: number
  rounding?: 'floor' | 'round'
  fractionDigits?: { seconds?: number; minutes?: number; hours?: number; days?: number }
  justNowBelow?: number
  suffix?: string
  fallback?: string
}

export function formatElapsed(seconds: number | null | undefined, options: ElapsedOptions = {}): string {
  const {
    minutesFrom = 60, hoursFrom = 3600, daysFrom = 86400, rounding = 'floor',
    fractionDigits = {}, justNowBelow, suffix = '', fallback = '—',
  } = options
  if (seconds == null || Number.isNaN(seconds)) return fallback
  const r = rounding === 'round' ? Math.round : Math.floor
  const unit = (v: number, digits: number | undefined, label: string) => `${digits ? v.toFixed(digits) : r(v)}${label}${suffix}`
  if (justNowBelow !== undefined && seconds < justNowBelow) return 'just now'
  if (seconds < minutesFrom) return unit(seconds, fractionDigits.seconds, 's')
  if (seconds < hoursFrom) return unit(seconds / 60, fractionDigits.minutes, 'm')
  if (seconds < daysFrom) return unit(seconds / 3600, fractionDigits.hours, 'h')
  return unit(seconds / 86400, fractionDigits.days, 'd')
}

export const ELAPSED_RELATIVE_AGO_JUST_NOW: ElapsedOptions = { suffix: ' ago', justNowBelow: 120 }
export const ELAPSED_RELATIVE_AGO: ElapsedOptions = { suffix: ' ago' }
export const ELAPSED_AGE_COMPACT: ElapsedOptions = {
  rounding: 'round', minutesFrom: 90, hoursFrom: 5400, daysFrom: 172800, fractionDigits: { hours: 1, days: 1 },
}
export const ELAPSED_FRESHNESS: ElapsedOptions = {
  rounding: 'round', daysFrom: Infinity, fractionDigits: { hours: 1 },
}

export function resolveCountryName(code: string): string {
  if (!code || code.length !== 2 || typeof Intl === 'undefined') return code
  try {
    return new Intl.DisplayNames(['en'], { type: 'region' }).of(code.toUpperCase()) || code
  } catch { return code }
}

export function formatCompactCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1).replace(/\.0$/, '')}k`
  return String(n)
}

export function formatAbbreviatedCount(n: number, millionDecimals = 1): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(millionDecimals)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(millionDecimals)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return formatCount(n)
}

export function formatCount(n: number, options?: Intl.NumberFormatOptions): string {
  return getNumberFormat(undefined, options).format(n)
}

export function formatCountMaybe(n: number | null | undefined, options?: Intl.NumberFormatOptions): string | undefined {
  return n == null ? undefined : formatCount(n, options)
}

export function formatValue(field: string | undefined, value: string | number | null | undefined): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'number') return field?.includes('bytes') ? formatBytes(value) : formatCount(value)
  const str = String(value)
  if (field === 'country' && str.length === 2) return resolveCountryName(str)
  if (field === 'pop' || field === 'region') return str.toUpperCase()
  if (field === 'city') return str.toLowerCase().replace(/(^|[^\w])(\w)/g, (m) => m.toUpperCase())
  if (field === 'proto' || field === 'tls') {
    const num = Number(value)
    if (!isNaN(num)) return parseFloat(num.toFixed(1)).toString()
  }
  return str
}

export function formatCurrency(amount: number): string {
  if (amount >= 1000) return `$${(amount / 1000).toFixed(1).replace(/\.0$/, '')}k`
  return `$${amount.toFixed(amount >= 1 ? 2 : 4)}`
}

export function formatUSD(n: number, options?: { subDollarDigits?: number }): string {
  if (n >= 1000) return `$${getNumberFormat('en-US', { maximumFractionDigits: 0 }).format(Number(n.toFixed(0)))}`
  return `$${n.toFixed(n >= 1 ? 2 : (options?.subDollarDigits ?? 4))}`
}

export function calculateDelta(current: number, previous: number | undefined): number | null {
  return previous ? ((current - previous) / previous) * 100 : null
}

export function formatPercent(ratio: number, options?: { decimals?: number }): string {
  return `${(ratio * 100).toFixed(options?.decimals ?? 1)}%`
}

export function formatPctValue(v: number | null | undefined, decimals?: number): string {
  return v == null ? '—' : `${decimals != null ? v.toFixed(decimals) : v}%`
}

export function formatMs(v: number | null | undefined, decimals = 1, unit = ' ms'): string {
  return v == null ? '—' : `${v.toFixed(decimals)}${unit}`
}

export function statusBadgeClass(
  status: number | string | null | undefined,
  variant: 'class' | 'traffic-light' = 'class',
): string {
  if (variant === 'traffic-light') {
    const n = typeof status === 'number' ? status : status == null ? 0 : Number(status)
    if (!n) return 'bg-muted text-muted-foreground'
    if (n >= 200 && n < 300) return 'bg-green-500/10 text-green-500 border-green-500/20'
    if (n >= 300 && n < 400) return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'
    return 'bg-red-500/10 text-red-500 border-red-500/20'
  }
  const code = typeof status === 'number' ? status : parseInt(String(status), 10)
  if (isNaN(code)) return 'bg-muted text-muted-foreground'
  if (code >= 200 && code < 300) return 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'
  if (code >= 300 && code < 400) return 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'
  if (code >= 400 && code < 500) return 'bg-amber-500/15 text-amber-800 dark:text-amber-400 border-amber-500/20'
  if (code >= 500) return 'bg-destructive/15 text-destructive border-destructive/20 font-bold'
  return 'bg-muted text-muted-foreground'
}

export function toUTCDate(date: string | Date): Date {
  if (date instanceof Date) return date
  if (!date) return new Date(NaN)
  const hasTz = date.includes('Z') || /[+-]\d{2}:?\d{2}$/.test(date)
  if (date.includes('T') && hasTz) return parseISO(date)
  return parseISO(hasTz ? date.replace(' ', 'T') : `${date.replace(' ', 'T')}Z`)
}

export function formatForInput(date: string | Date, tz: string) {
  try {
    const d = toUTCDate(date)
    return isNaN(d.getTime()) ? '' : formatInTimeZone(d, tz, "yyyy-MM-dd'T'HH:mm:ss")
  } catch { return '' }
}

export function parseFromInput(dateStr: string, tz: string) {
  try {
    const d = toDate(dateStr, { timeZone: tz })
    return isNaN(d.getTime()) ? null : d.toISOString()
  } catch { return null }
}

export function formatDate(date: string | Date, tz: string, pattern: string = 'yyyy-MM-dd h:mm:ss a') {
  try {
    const d = toUTCDate(date)
    return isNaN(d.getTime()) ? 'Invalid Date' : formatInTimeZone(d, tz, pattern)
  } catch (e) {
    console.error('Error formatting date:', date, e)
    return 'Invalid Date'
  }
}

export function formatRelative(date: string | Date) {
  try {
    const d = toUTCDate(date)
    return isNaN(d.getTime()) ? 'Invalid Date' : formatDistanceToNow(d, { addSuffix: true })
  } catch { return 'Invalid Date' }
}

export function formatTimeAgo(date: string | Date) {
  const d = toUTCDate(date)
  if (isNaN(d.getTime())) return ''
  const diffSec = differenceInSeconds(new Date(), d)
  if (diffSec < 0) return 'just now'
  if (diffSec < 60) return _relFmt ? _relFmt.format(-diffSec, 'second') : `${diffSec}s ago`
  const s = diffSec % 60, m = Math.floor((diffSec % 3600) / 60)
  if (diffSec < 3600) return `${m}m ${s}s ago`
  const h = Math.floor((diffSec % 86400) / 3600)
  if (diffSec < 86400) return `${h}h ${m}m ${s}s ago`
  return `${Math.floor(diffSec / 86400)}d ${h}h ${m}m ago`
}

export function getTimezoneAbbr(date: Date, tz: string) {
  return formatInTimeZone(date, tz, 'zzz')
}

export function formatLocalDateTime(value: string | number | Date, options?: Intl.DateTimeFormatOptions): string {
  const d = value instanceof Date ? value : new Date(value)
  if (isNaN(d.getTime())) return d.toLocaleString(undefined, options)
  return options ? getDateTimeFormat(undefined, options).format(d) : d.toLocaleString()
}

export function formatCompactDuration(seconds: number): string {
  return seconds <= 0 ? 'any second' : formatElapsed(seconds)
}

export function formatDuration(seconds: number | null | undefined, options?: { round?: boolean }): string {
  if (seconds == null || seconds < 0) return '—'
  const rounded = (options?.round ?? true) ? Math.round(seconds) : seconds
  if (rounded < 60) return `${rounded}s`
  const m = Math.floor(rounded / 60), s = rounded % 60
  if (m < 60) return s > 0 ? `${m}m ${s}s` : `${m}m`
  const h = Math.floor(m / 60), mRem = m % 60
  if (h < 24) return mRem > 0 ? `${h}h ${mRem}m` : `${h}h`
  const d = Math.floor(h / 24), hRem = h % 24
  return hRem > 0 ? `${d}d ${hRem}h` : `${d}d`
}

export function formatDeterministicUTC(iso: string | null | undefined): string {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    if (isNaN(d.getTime())) return iso
    const datePart = getDateTimeFormat('en-US', { timeZone: 'UTC', year: 'numeric', month: 'short', day: 'numeric' }).format(d)
    const timePart = getDateTimeFormat('en-US', { timeZone: 'UTC', hour: '2-digit', minute: '2-digit' }).format(d)
    return `${datePart}, ${timePart} UTC`
  } catch { return iso }
}

export function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const utc = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(iso) ? iso : `${iso}Z`
  const d = new Date(utc)
  if (isNaN(d.getTime())) return iso
  return getDateTimeFormat('en-US', {
    timeZone: tz, month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true,
  }).format(d)
}

export function parseExtentMs(value: unknown, edge: 'earliest' | 'latest'): number | null {
  if (typeof value !== 'string' || value.length === 0) return null
  const iso = value.length === 10 ? `${value}${edge === 'earliest' ? 'T00:00:00.000Z' : 'T23:59:59.999Z'}` : value
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : t
}

export function toQueryDate(d: Date): string {
  return `${d.toISOString().slice(0, 19)}Z`
}

export function formatDateTime(isoString: string | null | undefined, timeZone?: string): string {
  return isoString ? formatDate(isoString, timeZone || 'UTC', 'MMM d, yyyy h:mm:ss a') : 'Unknown'
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

export function makeTimeXAxis(startTime: string | null | undefined, endTime: string | null | undefined, timezone: string) {
  return {
    range: [
      startTime ? formatDate(startTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
      endTime ? formatDate(endTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
    ],
    nticks: 8, tickangle: -45, automargin: true, type: 'date' as const,
    tickformatstops: CHART_LAYOUT_DEFAULTS.tickformatstops,
  }
}

export const TIME_HOVER_LAYOUT = {
  hovermode: 'x unified' as const,
  legend: {
    orientation: 'h' as const, y: 1.05, x: 0.5, xanchor: 'center' as const, yanchor: 'bottom' as const,
    font: { size: 9.5 }, bgcolor: 'transparent',
  },
}

export function useTimeLayout(startTime: string | null | undefined, endTime: string | null | undefined, timezone: string) {
  return useMemo(() => ({ ...TIME_HOVER_LAYOUT, xaxis: makeTimeXAxis(startTime, endTime, timezone) }), [startTime, endTime, timezone])
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
  let first = timesMs[0], last = timesMs[timesMs.length - 1]
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
  if (!timesMs.every((t) => { const rem = Math.abs((t - first) % stepMs); return rem === 0 || rem < 1000 || Math.abs(rem - stepMs) < 1000 })) return null
  const nBuckets = Math.round((last - first) / stepMs) + 1
  if ((nBuckets <= timesMs.length && !startTime && !endTime) || nBuckets > MAX_DENSE_BUCKETS) return null
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
  const grid = denseTimeGrid(time_series.map((d) => d.time), intervalSeconds, startTime, endTime)
  if (!grid) return time_series
  if (hasCategories) {
    const categories: string[] = [], seen = new Set<string>(), byKey = new Map<string, number>()
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
      for (const cat of categories) out.push({ time: iso, value: byKey.get(`${ms}|${cat}`) ?? 0, category: cat })
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
  success: '#10b981', danger: '#ef4444', warning: '#f59e0b', info: '#3b82f6', neutral: '#94a3b8',
} as const

export function statusHex(code: number): string {
  return code >= 500 ? CHART_COLORS.danger : code >= 400 ? CHART_COLORS.warning : code >= 300 ? CHART_COLORS.info : CHART_COLORS.success
}

export function denseBarTrace<R>(opts: {
  name: string; rows: R[]; getTime: (row: R) => string | null | undefined; getValue: (row: R) => number | null | undefined
  intervalSeconds: number; color: string; yaxis?: 'y2'; showlegend?: boolean; grid?: string[]
}) {
  const { name, rows, getTime, getValue, intervalSeconds, color, yaxis, showlegend, grid } = opts
  const present: string[] = [], byMs = new Map<number, number>()
  for (const r of rows) {
    const t = getTime(r)
    if (t == null) continue
    present.push(t)
    byMs.set(Date.parse(t), getValue(r) ?? 0)
  }
  const x = grid ?? denseTimeGrid(present, intervalSeconds) ?? present
  return {
    type: 'bar' as const, name, x, y: x.map((h) => byMs.get(Date.parse(h)) ?? 0),
    ...(yaxis ? { yaxis } : {}), marker: { color }, ...(showlegend !== undefined ? { showlegend } : {}),
  }
}

export function lineTrace(opts: {
  name: string; x: Array<string | null | undefined>; y: Array<number | null>; color: string
  width?: number; dash?: 'dot' | 'dash'; yaxis?: 'y2'; connectgaps?: boolean
}) {
  const { name, x, y, color, width = 2, dash, yaxis, connectgaps } = opts
  return {
    type: 'scatter' as const, mode: 'lines' as const, name, x, y,
    ...(yaxis ? { yaxis } : {}), line: { color, width, dash }, ...(connectgaps !== undefined ? { connectgaps } : {}),
  }
}

export function secondaryYAxis(
  title: string,
  extra: { ticksuffix?: string; rangemode?: 'tozero' | 'normal' | 'nonnegative'; showgrid?: boolean } = {},
) {
  return { title, overlaying: 'y' as const, side: 'right' as const, showgrid: false, rangemode: 'tozero' as const, ...extra }
}

export function withSecondaryAxis<L extends { margin?: { r?: number } }>(
  layout: L, title: string, extra?: Parameters<typeof secondaryYAxis>[1],
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
    const sortedData = [...data].filter((d) => d && (d.time || d.ts)).sort((a, b) => (a.time || a.ts || '').localeCompare(b.time || b.ts || ''))
    const xValues = sortedData.map((d) => {
      const t = d.time || d.ts || ''
      return timezone ? formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss') : t
    })
    return configs.map((config) => {
      const yValues = sortedData.map((d) => Number((d as Record<string, unknown>)[config.key]) || 0)
      const isBar = config.type === 'bar'
      return {
        x: xValues, y: yValues, name: config.name, type: config.type || 'scatter', fill: config.fill || 'tozeroy',
        ...(isBar ? { marker: { color: config.color } } : { mode: 'lines' as const, line: { width: 1.5, color: config.color } }),
        ...(config.stackgroup ? { stackgroup: config.stackgroup } : {}),
      }
    })
  }, [data, configs, timezone])
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = window.URL.createObjectURL(blob)
  const a = Object.assign(document.createElement('a'), { href: url, download: filename })
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

export function downloadAsCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
  const escape = (v: unknown): string => {
    if (v == null) return ''
    const s = String(v)
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const csv = [columns.map(escape).join(','), ...rows.map((r) => columns.map((c) => escape(r[c])).join(','))].join('\n')
  downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8;' }), filename)
}

export async function copyToClipboard(text: string): Promise<boolean> {
  try { await navigator.clipboard.writeText(text); return true } catch { return false }
}

export function keyByOccurrence<T>(items: readonly T[], label: (item: T) => string): { key: string; item: T }[] {
  const seen = new Map<string, number>()
  return items.map((item) => {
    const base = label(item), n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    return { key: n === 0 ? base : `${base}#${n}`, item }
  })
}

export function recordOrNull<T extends object>(v: T | null | undefined): T | null {
  return typeof v === 'object' && v !== null && !Array.isArray(v) ? (v as T) : null
}

export function arrayOrEmpty<T>(v: readonly T[] | null | undefined): T[] {
  return Array.isArray(v) ? [...v] : []
}

export function oneOf<T extends string>(allowed: readonly T[], value: string | null | undefined): T | undefined {
  return allowed.find((a) => a === value)
}

export const IP_FAMILY_FIELDS = ['ip', 'client_ip', 'ip_address', 'remote_addr'] as const

export function isIpFamilyField(column: string): boolean {
  return (IP_FAMILY_FIELDS as readonly string[]).includes(column)
}

export const ACTIVE_SERVICE_COOKIE = 'fla.activeServiceId'
export const DEBUG_RESPONSES_COOKIE = 'fla.debugResponses'
export const SIDEBAR_COLLAPSED_COOKIE = 'fla.sidebarCollapsed'

export function setActiveServiceCookie(serviceId: string | null) {
  if (typeof document === 'undefined') return
  const valid = serviceId && /^[A-Za-z0-9]{10,30}$/.test(serviceId)
  document.cookie = `${ACTIVE_SERVICE_COOKIE}=${valid ? serviceId : ''}; path=/; max-age=${valid ? 31536000 : 0}; samesite=lax`
}

export function buildServiceHref(path: string, serviceId: string | null | undefined): string {
  if (!serviceId) return path
  return `${path}${path.includes('?') ? '&' : '?'}service=${encodeURIComponent(serviceId)}`
}

export function buildStreamDetailUrl(token: string, serviceId?: string | null): string {
  const base = `/sessions/stream?token=${encodeURIComponent(token)}`
  return serviceId ? `${base}&service=${encodeURIComponent(serviceId)}` : base
}

export function buildSessionDashboardUrl(
  serviceId: string | null | undefined, filterKey: string, filterValue: string | null | undefined,
  sessionStart: string | null | undefined, sessionEnd: string | null | undefined,
): string {
  const params = new URLSearchParams()
  if (serviceId != null) params.set('service', serviceId)
  if (filterValue != null) params.set(`filter_${filterKey}`, filterValue)
  if (sessionStart != null) params.set('start_time', sessionStart)
  if (sessionEnd != null) params.set('end_time', sessionEnd)
  return `/dashboard?${params.toString()}`
}

export const panelDialogContent = 'max-h-[90vh] flex flex-col p-0 overflow-hidden'
export const panelDialogHeaderMuted = 'px-6 pt-6 pb-4 border-b bg-muted/10 shrink-0'
export const panelDialogHeaderSolid = 'px-6 pt-6 pb-4 border-b bg-background shrink-0'
export const panelDialogFooter = 'px-6 py-4 border-t bg-muted/10 shrink-0'

export const ACTIVE_WINDOW_MS = 120_000
const MOUSE_THROTTLE_MS = 1_000

let lastInteractionAt = typeof window !== 'undefined' ? Date.now() : 0
let lastThrottledBump = 0
let listenersAttached = false

function bump() { lastInteractionAt = Date.now() }
function bumpThrottled() {
  const now = Date.now()
  if (now - lastThrottledBump < MOUSE_THROTTLE_MS) return
  lastThrottledBump = now
  lastInteractionAt = now
}

function ensureListeners() {
  if (listenersAttached || typeof window === 'undefined' || typeof window.addEventListener !== 'function') return
  listenersAttached = true
  window.addEventListener('mousemove', bumpThrottled, { passive: true })
  window.addEventListener('wheel', bumpThrottled, { passive: true })
  window.addEventListener('keydown', bump)
  window.addEventListener('pointerdown', bump, { passive: true })
  window.addEventListener('touchstart', bump, { passive: true })
}
ensureListeners()

export function msSinceLastInteraction(): number {
  if (typeof window === 'undefined') return 0
  ensureListeners()
  return Date.now() - lastInteractionAt
}

export function isUserActive(windowMs: number = ACTIVE_WINDOW_MS): boolean {
  if (typeof window === 'undefined') return true
  ensureListeners()
  return Date.now() - lastInteractionAt < windowMs
}

export function __resetForTests(at: number = 0): void {
  lastInteractionAt = at
  lastThrottledBump = 0
}

export const INTERVALS = [
  { label: '1s', value: '1 second' }, { label: '1m', value: '1 minute' },
  { label: '1h', value: '1 hour' }, { label: '1d', value: '1 day' },
] as const

export type ChartInterval = typeof INTERVALS[number]['value']

export const INTERVAL_SECONDS: Record<ChartInterval, number> = {
  '1 second': 1, '1 minute': 60, '1 hour': 3600, '1 day': 86400,
}

export const TRENDS = [
  { label: 'Off', value: 'off' }, { label: 'Auto', value: 'auto' },
  { label: '1m', value: '1m' }, { label: '5m', value: '5m' },
  { label: '1h', value: '1h' }, { label: '1d', value: '1d' },
]

export const SHIELD_MAP: Record<string, string> = {
  "us-east-1": "iad-va-us", "us-west": "bfi-wa-us", "us-central-1": "chi-il-us",
  "eu-central-1": "frankfurt-de", "eu-south-1": "mxp-milan-it", "eu-west-1": "mxp-paris-fr",
  "uk-east-1": "london-uk", "jp-central-1": "nrt-tokyo-jp", "au-east-1": "sydney-au",
}

export const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US East (Ashburn)", "us-west": "US West (Seattle)", "us-central-1": "US Central (Chicago)",
  "eu-central-1": "EU Central (Frankfurt)", "eu-south-1": "EU South (Milan)", "eu-west-1": "EU West (Paris)",
  "uk-east-1": "UK East (London)", "jp-central-1": "JP Central (Tokyo)", "au-east-1": "AU East (Sydney)",
}

export const SHIELD_LABELS: Record<string, string> = {
  none: "None", "iad-va-us": "IAD (Ashburn)", "bfi-wa-us": "BFI (Seattle)", "chi-il-us": "CHI (Chicago)",
  "frankfurt-de": "FRA (Frankfurt)", "mxp-milan-it": "MXP (Milan)", "mxp-paris-fr": "MXP (Paris)",
  "london-uk": "LHR (London)", "nrt-tokyo-jp": "NRT (Tokyo)", "sydney-au": "SYD (Sydney)",
}

export const PERIOD_LABELS: Record<string, string> = {
  "1": "1 second", "5": "5 seconds", "10": "10 seconds", "20": "20 seconds", "30": "30 seconds",
  "60": "1 minute", "120": "2 minutes", "300": "5 minutes", "600": "10 minutes", "900": "15 minutes",
}

export const RETENTION_OPTIONS = [
  { value: "1", label: "1 day" }, { value: "3", label: "3 days" }, { value: "7", label: "7 days" },
  { value: "14", label: "14 days" }, { value: "30", label: "30 days" }, { value: "90", label: "90 days" },
  { value: "180", label: "180 days" }, { value: "365", label: "365 days" }, { value: "0", label: "Forever" },
] as const

type ToastKind = 'info' | 'success' | 'error' | 'warn'
interface ToastOptions { durationMs?: number }
interface ToastActionOptions extends ToastOptions { actionLabel: string; onAction: () => void; kind?: ToastKind }

const PALETTE: Record<ToastKind, { bg: string; border: string; fg: string }> = {
  info: { bg: '#0f172a', border: '#1e293b', fg: '#e2e8f0' },
  success: { bg: '#064e3b', border: '#065f46', fg: '#ecfdf5' },
  error: { bg: '#7f1d1d', border: '#991b1b', fg: '#fef2f2' },
  warn: { bg: '#78350f', border: '#92400e', fg: '#fffbeb' },
}

let stackContainer: HTMLDivElement | null = null
const recentMessages = new Map<string, number>()
const RECENT_DEDUP_MS = 1500

function ensureContainer(): HTMLDivElement | null {
  if (typeof document === 'undefined') return null
  if (stackContainer && document.body.contains(stackContainer)) return stackContainer
  const el = document.createElement('div')
  el.setAttribute('role', 'region')
  el.setAttribute('aria-label', 'Notifications')
  el.style.cssText = 'position:fixed;top:16px;right:16px;z-index:2147483647;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:380px'
  document.body.appendChild(el)
  return (stackContainer = el)
}

function mountToastNode(kind: ToastKind, durationMs: number, extraCss: string[] = [], setup?: (node: HTMLDivElement, dismiss: () => boolean) => void): void {
  const root = ensureContainer()
  if (!root) return
  const palette = PALETTE[kind], isAlert = kind === 'error' || kind === 'warn', node = document.createElement('div')
  node.setAttribute('role', isAlert ? 'alert' : 'status')
  node.setAttribute('aria-live', isAlert ? 'assertive' : 'polite')
  node.style.cssText = [
    `background:${palette.bg}`, `color:${palette.fg}`, `border:1px solid ${palette.border}`,
    'padding:10px 14px', 'border-radius:8px', 'font:13px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
    'box-shadow:0 8px 24px rgba(0,0,0,.18)', 'pointer-events:auto', 'transition:opacity .18s ease,transform .18s ease',
    'opacity:0', 'transform:translateY(-4px)', ...extraCss,
  ].join(';')
  let dismissed = false
  const dismiss = (): boolean => {
    if (dismissed) return false
    dismissed = true
    node.style.opacity = '0'
    node.style.transform = 'translateY(-4px)'
    setTimeout(() => {
      node.remove()
      if (root.childElementCount === 0 && stackContainer) { stackContainer.remove(); stackContainer = null }
    }, 200)
    return true
  }
  setup?.(node, dismiss)
  root.appendChild(node)
  requestAnimationFrame(() => { node.style.opacity = '1'; node.style.transform = 'translateY(0)' })
  setTimeout(dismiss, durationMs)
}

export function showToast(message: string, kind: ToastKind = 'info', opts: ToastOptions = {}): void {
  if (typeof document === 'undefined') return
  const now = Date.now()
  if (now - (recentMessages.get(message) || 0) < RECENT_DEDUP_MS) return
  recentMessages.set(message, now)
  if (recentMessages.size > 32) {
    for (const [k, t] of recentMessages) if (now - t > RECENT_DEDUP_MS * 4) recentMessages.delete(k)
  }
  mountToastNode(kind, opts.durationMs ?? (kind === 'error' ? 5500 : 3500), [], (node, dismiss) => {
    node.textContent = message
    node.addEventListener('click', dismiss)
  })
}

export function showReadOnlyToast(): void { showToast('Read-only access — that action is unavailable for shared sessions.', 'warn') }
export function showBusyToast(): void { showToast('Server busy, retrying…', 'info', { durationMs: 2500 }) }

export function showToastWithAction(message: string, opts: ToastActionOptions): void {
  if (typeof document === 'undefined') return
  const kind: ToastKind = opts.kind ?? 'info', palette = PALETTE[kind]
  mountToastNode(kind, opts.durationMs ?? 5500, ['display:flex', 'align-items:center', 'gap:12px'], (node, dismiss) => {
    const label = document.createElement('span')
    label.textContent = message
    label.style.cssText = 'flex:1;min-width:0'
    node.appendChild(label)
    const button = document.createElement('button')
    button.type = 'button'
    button.textContent = opts.actionLabel
    button.style.cssText = `background:transparent;color:${palette.fg};border:1px solid ${palette.fg};border-radius:6px;padding:4px 10px;font:600 12px/1 inherit;cursor:pointer;flex-shrink:0;opacity:.9`
    button.addEventListener('click', (e) => { e.stopPropagation(); if (dismiss()) opts.onAction() })
    node.appendChild(button)
  })
}

export const DEFAULT_RANGE_TOKEN = '24h'

export interface RangeWireArgs {
  relativeRange: string | null
  isAutoRange: boolean
  startTime: string | null
  endTime: string | null
  anchor: string
}

export interface RangeWire {
  rangeToken: string | null
  rangeKey: string
  rangeBody:
    | { range_token: string; anchor: string; start_time: string | null; end_time: string | null }
    | { start_time: string | null; end_time: string | null }
}

export function resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor }: RangeWireArgs): RangeWire {
  const rangeToken = relativeRange ?? (isAutoRange ? DEFAULT_RANGE_TOKEN : null)
  if (rangeToken !== null) {
    return { rangeToken, rangeKey: rangeToken, rangeBody: { range_token: rangeToken, anchor, start_time: startTime, end_time: endTime } }
  }
  return { rangeToken: null, rangeKey: `abs:${startTime ?? ''}|${endTime ?? ''}`, rangeBody: { start_time: startTime, end_time: endTime } }
}

export const ANCHOR_QUANTUM_SECONDS = 60

export function quantizeAnchor(iso?: string | null, now: Date = new Date()): string {
  const parsed = iso ? Date.parse(iso) : NaN
  const epoch = Math.floor((Number.isFinite(parsed) ? parsed : now.getTime()) / 1000)
  return new Date((epoch - (epoch % ANCHOR_QUANTUM_SECONDS)) * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

let hydrated = false

export function rangeLabelToHours(label: string): number | null {
  const m = /^(\d+)([mhd])$/.exec(label)
  if (!m) return null
  const n = parseInt(m[1], 10)
  if (!Number.isFinite(n) || n <= 0) return null
  return m[2] === 'm' ? n / 60 : m[2] === 'd' ? n * 24 : n
}

export function hydrateFilterStoreFromUrl(): void {
  if (hydrated || typeof window === 'undefined') return
  hydrated = true
  const params = new URLSearchParams(window.location.search)
  const qsRange = params.get('range'), qsStart = params.get('start_time'), qsEnd = params.get('end_time'), qsFilters = params.get('filters')
  const store = useFilterStore.getState()
  const rangeHours = qsRange ? rangeLabelToHours(qsRange) : null
  let touchedRange = false
  if (qsRange && rangeHours !== null) {
    const now = new Date()
    store.setRelativeRange(qsRange, subHours(now, rangeHours).toISOString(), now.toISOString())
    touchedRange = true
  } else if (qsStart && qsEnd) {
    store.setRange(qsStart, qsEnd)
    touchedRange = true
  }
  let touchedFilters = false
  if (qsFilters) {
    try {
      const parsed = JSON.parse(qsFilters) as FiltersPayload
      if (parsed && typeof parsed === 'object') {
        store.clearFilters()
        touchedFilters = true
        for (const [rawCol, spec] of Object.entries(parsed)) {
          if (!spec || !Array.isArray(spec.values)) continue
          const col = rawCol.replace(/_\d+$/, '')
          for (const v of spec.values) store.addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
        }
      }
    } catch { /* ignore */ }
  }
  if (!touchedFilters) {
    let cleared = false
    params.forEach((value, key) => {
      if (!key.startsWith('filter_')) return
      if (!cleared) { store.clearFilters(); cleared = true; touchedFilters = true }
      store.addFilter(key.substring('filter_'.length).replace(/_\d+$/, ''), value, 'include')
    })
  }
  if (touchedRange || touchedFilters) {
    const url = new URL(window.location.href)
    for (const k of ['range', 'start_time', 'end_time', 'filters']) url.searchParams.delete(k)
    Array.from(url.searchParams.keys()).forEach((key) => { if (key.startsWith('filter_')) url.searchParams.delete(key) })
    window.history.replaceState({}, '', url.toString())
  }
}

export function _resetUrlHydrationFlag(): void { hydrated = false }

interface InsightSection { key: string; label: string; description: string; icon: LucideIcon }

const ALL_SECTION: InsightSection = {
  key: 'all', label: 'All Categories', description: 'All automated insights and detected anomalies across every category.', icon: LayoutGrid,
}

export const INSIGHT_SECTIONS: readonly InsightSection[] = [
  { key: 'security', label: 'Security & Threat Detection', description: 'Suspicious signatures, scanning, spoofing, proxy/abuse, and WAF activity.', icon: ShieldAlert },
  { key: 'origin', label: 'Origin Health & Stability', description: 'Origin 5xx, latency/timeouts, retries, per-IP failures, and the shield path.', icon: ServerCrash },
  { key: 'edge', label: 'Edge & Delivery Performance', description: 'Cache efficiency, edge/regional latency and tail, and delivery quality.', icon: Zap },
  { key: 'network', label: 'Network & Routing Path', description: 'ISP / ASN reachability, RTT and packet loss, and metro/region delivery quality.', icon: Network },
  { key: 'traffic', label: 'Traffic & Volumetrics', description: 'Volume swings, geography, composition, and automation.', icon: Globe },
] as const

export const SECTION_LOADING_MESSAGES: Record<string, string> = {
  all: 'Evaluating detector baseline comparisons…',
  security: 'Analyzing security signatures & JA3 patterns…',
  origin: 'Evaluating origin 5xx errors & latency shifts…',
  edge: 'Measuring cache collapse & delivery performance…',
  network: 'Checking ISP latency & routing packet loss…',
  traffic: 'Scanning traffic surges & botnet clustering…',
  other: 'Evaluating anomaly detector results…',
}

export function normalizeTabKey(tab: string | null | undefined): string {
  if (!tab) return 'all'
  const lower = tab.toLowerCase().trim()
  return lower === 'performance' ? 'edge' : lower === 'routing' ? 'network' : lower
}

export const OTHER_SECTION: InsightSection = {
  key: 'other', label: 'Other', description: 'Insights not yet assigned to a section.', icon: LayoutGrid,
}

const SECTION_BY_KEY = new Map<string, InsightSection>(INSIGHT_SECTIONS.map((s) => [s.key, s]))

export function categoryToSection(category: string | null | undefined): InsightSection {
  return (category && SECTION_BY_KEY.get(category)) || OTHER_SECTION
}

const SEVERITY_ORDER = ['critical', 'error', 'warning', 'info', 'clean'] as const
const SEVERITY_RANK: Record<string, number> = Object.fromEntries(SEVERITY_ORDER.map((sev, i) => [sev, i]))

export interface SectionableInsight { category?: string | null; severity?: string | null; title?: string | null }

export function sortInsights<T extends SectionableInsight>(cards: readonly T[]): T[] {
  return [...cards].sort((a, b) => {
    const ra = SEVERITY_RANK[a.severity ?? ''] ?? 99, rb = SEVERITY_RANK[b.severity ?? ''] ?? 99
    return ra !== rb ? ra - rb : (a.title ?? '').localeCompare(b.title ?? '')
  })
}

export interface SeveritySummary { severity: string; count: number }

export function summarizeSeverities(cards: readonly SectionableInsight[]): SeveritySummary[] {
  const counts = new Map<string, number>()
  for (const card of cards) {
    if (card.severity && card.severity in SEVERITY_RANK) counts.set(card.severity, (counts.get(card.severity) ?? 0) + 1)
  }
  return SEVERITY_ORDER.flatMap((severity) => {
    const count = counts.get(severity)
    return count ? [{ severity, count }] : []
  })
}

export interface InsightSectionGroup<T> { section: InsightSection; cards: T[] }

export function groupInsightsBySection<T extends SectionableInsight>(
  cards: readonly T[],
  options?: { includeAll?: boolean; includeEmptySections?: boolean },
): InsightSectionGroup<T>[] {
  const buckets = new Map<string, T[]>()
  for (const card of cards) {
    const key = categoryToSection(card.category).key
    const bucket = buckets.get(key)
    if (bucket) bucket.push(card)
    else buckets.set(key, [card])
  }
  const groups: InsightSectionGroup<T>[] = options?.includeAll ? [{ section: ALL_SECTION, cards: sortInsights(cards) }] : []
  for (const section of [...INSIGHT_SECTIONS, OTHER_SECTION]) {
    const bucket = buckets.get(section.key)
    if (bucket?.length) groups.push({ section, cards: sortInsights(bucket) })
    else if (options?.includeEmptySections && section.key !== OTHER_SECTION.key) groups.push({ section, cards: [] })
  }
  return groups
}

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
  if (INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS] !== undefined) return INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS]
  const match = str.match(/^(?:(\d+)\s*)?(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/)
  if (match) {
    const num = match[1] ? parseInt(match[1], 10) : 1, u = match[2][0]
    return u === 's' ? num : u === 'm' ? num * 60 : u === 'h' ? num * 3600 : num * 86400
  }
  return undefined
}

export function buildTrafficData({
  aggregates, compareAggregates, compareMode, compareStartTime, startTime, endTime,
  trend, timezone, metric, effectiveInterval, hiddenCategories, catalog,
}: BuildTrafficDataParams): PlotlyTrace[] {
  const time_series = aggregates?.time_series
  if (!time_series?.length) return []
  const actualMetric = aggregates?.metric || metric
  const isBar = actualMetric === 'requests' || actualMetric === '5xx' || actualMetric === '4xx'
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)
  const unit = metricField?.unit || '', precision = metricField?.precision ?? (actualMetric === 'requests' ? 0 : 1)
  const getHoverTemplate = (_m: string, label?: string) => `${label ? `${label}: ` : ''}%{y:${precision > 0 ? `.${precision}f` : ','}}${unit}<extra></extra>`
  const hasCategories = time_series.some((d) => d.category != null)
  const actualInterval = aggregates?.interval || effectiveInterval
  const intervalSeconds = getIntervalSeconds(actualInterval)
  const barSeries: BarSeriesPoint[] = isBar ? densifyBarSeries(time_series, intervalSeconds, hasCategories, startTime, endTime) : time_series
  let tsXValues: string[] | null = null
  const getTsXValues = (): string[] => (tsXValues ??= time_series.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")))
  let traces: PlotlyTrace[] = []
  if (hasCategories) {
    const catMap: Record<string, { x: string[]; y: number[] }> = {}
    barSeries.forEach((d) => {
      const cat = d.category || 'Other'
      ;(catMap[cat] ??= { x: [], y: [] }).x.push(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
      catMap[cat].y.push(d.value)
    })
    const colorMap: Record<string, string> = {
      '400': '#fbbf24', '401': CHART_COLORS.warning, '403': '#d97706', '404': '#b45309',
      '500': CHART_COLORS.danger, '502': '#dc2626', '503': '#b91c1c', '504': '#991b1b',
    }
    traces = Object.entries(catMap).map(([cat, data], i) => ({
      x: data.x, y: data.y, type: 'bar', width: intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
      name: cat, showlegend: false, visible: hiddenCategories.has(cat) ? 'legendonly' : true,
      hovertemplate: `Status ${cat}: %{y:,}<extra></extra>`,
      marker: { color: colorMap[cat] || `hsl(${(i * 50) % 360}, 70%, 50%)` },
    }))
  } else {
    const xValues = barSeries === time_series ? getTsXValues() : barSeries.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    traces = [{
      x: xValues, y: barSeries.map((d) => d.value),
      type: isBar ? 'bar' : 'scatter', width: isBar && intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
      mode: isBar ? undefined : 'lines+markers',
      name: compareMode ? 'Primary Range' : (metricField?.label || actualMetric),
      showlegend: compareMode,
      hovertemplate: getHoverTemplate(actualMetric, compareMode ? 'Primary' : undefined),
      marker: { color: CHART_COLORS.info },
    }]
  }
  if (compareMode && compareAggregates?.time_series?.length && !hasCategories && startTime && compareStartTime) {
    const shift = new Date(startTime).getTime() - new Date(compareStartTime).getTime()
    traces.push({
      x: compareAggregates.time_series.map((d) => formatDate(new Date(toUTCDate(d.time).getTime() + shift).toISOString(), timezone, "yyyy-MM-dd HH:mm:ss")),
      y: compareAggregates.time_series.map((d) => d.value),
      type: 'scatter', mode: 'lines', name: 'Comparison Range',
      line: { color: '#f97316', dash: 'dash', width: 2 },
      hovertemplate: getHoverTemplate(actualMetric, 'Comparison'),
    })
  }
  if (!hasCategories && time_series.some((d) => d.baseline != null)) {
    traces.push({
      x: getTsXValues(), y: time_series.map((d) => d.baseline),
      type: 'scatter', mode: 'lines', name: 'Baseline (7d prior)',
      hovertemplate: getHoverTemplate(actualMetric, 'Baseline'),
      line: { color: '#a1a1aa', dash: 'dot', width: 2 },
    })
  }
  if (!hasCategories && trend !== 'off') {
    const xValues = getTsXValues(), yValues = time_series.map((d) => d.value), n = yValues.length
    const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
    const windowSize = trend === 'auto'
      ? Math.floor(n / (n > 1000 ? 20 : n > 100 ? 10 : 5))
      : Math.floor((trendMap[trend] ?? 0) / (getIntervalSeconds(actualInterval) ?? 60))
    if (windowSize > 1) {
      const trendY: (number | null)[] = new Array(n).fill(null)
      if (n >= windowSize) {
        let runningSum = 0, runningCount = 0
        for (let i = 0; i < n; i++) {
          const enterVal = yValues[i]
          if (enterVal != null) { runningSum += enterVal; runningCount++ }
          if (i >= windowSize) {
            const exitVal = yValues[i - windowSize]
            if (exitVal != null) { runningSum -= exitVal; runningCount-- }
          }
          if (i >= windowSize - 1) trendY[i] = runningCount > 0 ? runningSum / runningCount : null
        }
      }
      traces.push({
        x: xValues, y: trendY, type: 'scatter', mode: 'lines',
        name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
        hovertemplate: getHoverTemplate(actualMetric),
        line: { color: '#f97316', width: 3 },
      })
    }
  }
  return traces
}

export const WINDOW_OPTIONS = [
  { label: 'Last 15 Minutes', value: '0.25' }, { label: 'Last 1 Hour', value: '1' },
  { label: 'Last 4 Hours', value: '4' }, { label: 'Last 24 Hours', value: '24' },
]

export const BASELINE_OPTIONS = [
  { label: 'Previous 1 Hour', value: '1' }, { label: 'Last 24 Hours', value: '24' },
  { label: 'Last 7 Days', value: '168' }, { label: 'Last 30 Days', value: '720' },
]

const STATIC_DEFAULT = { window: '24', baseline: '168' } as const

export function historyHoursFromExtents(earliest: string | null | undefined, now: number = Date.now()): number | null {
  if (!earliest) return null
  const ms = Date.parse(earliest.length === 10 ? earliest + 'T00:00:00.000Z' : earliest)
  return Number.isFinite(ms) ? Math.max(0, (now - ms) / 3_600_000) : null
}

export function pickInsightsDefault(historyHours: number | null): { window: string; baseline: string } {
  if (historyHours == null || !Number.isFinite(historyHours)) return { window: STATIC_DEFAULT.window, baseline: STATIC_DEFAULT.baseline }
  const h = historyHours
  if (h < 1) return { window: '0.25', baseline: '1' }
  if (h < 4) return { window: '1', baseline: '1' }
  if (h < 24) return { window: '4', baseline: '1' }
  if (h < 48) return { window: '4', baseline: '24' }
  if (h < 168) return { window: '24', baseline: '24' }
  return { window: '24', baseline: '168' }
}
