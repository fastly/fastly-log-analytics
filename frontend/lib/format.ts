import { differenceInSeconds, formatDistanceToNow, parseISO } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'

const _numFmtCache = new Map<string, Intl.NumberFormat>()
function getNumberFormat(locale?: string, options?: Intl.NumberFormatOptions): Intl.NumberFormat {
  const key = `${locale ?? ''}|${options ? JSON.stringify(options) : ''}`
  let fmt = _numFmtCache.get(key)
  if (!fmt) {
    fmt = new Intl.NumberFormat(locale, options)
    _numFmtCache.set(key, fmt)
  }
  return fmt
}

const _dtFmtCache = new Map<string, Intl.DateTimeFormat>()
function getDateTimeFormat(locale: string | undefined, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `${locale ?? ''}|${JSON.stringify(options)}`
  let fmt = _dtFmtCache.get(key)
  if (!fmt) {
    fmt = new Intl.DateTimeFormat(locale, options)
    _dtFmtCache.set(key, fmt)
  }
  return fmt
}

const _relFmt = typeof Intl !== 'undefined' && typeof Intl.RelativeTimeFormat === 'function'
  ? new Intl.RelativeTimeFormat('en', { numeric: 'always', style: 'narrow' })
  : null

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  let value = bytes
  let i = 0
  while (value >= 1000 && i < BYTE_UNITS.length - 1) {
    value /= 1024
    i++
  }
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
    minutesFrom = 60,
    hoursFrom = 3600,
    daysFrom = 86400,
    rounding = 'floor',
    fractionDigits = {},
    justNowBelow,
    suffix = '',
    fallback = '—',
  } = options
  if (seconds == null || Number.isNaN(seconds)) return fallback
  const r = rounding === 'round' ? Math.round : Math.floor
  const unit = (value: number, digits: number | undefined, label: string) =>
    `${digits ? value.toFixed(digits) : r(value)}${label}${suffix}`
  if (justNowBelow !== undefined && seconds < justNowBelow) return 'just now'
  if (seconds < minutesFrom) return unit(seconds, fractionDigits.seconds, 's')
  if (seconds < hoursFrom) return unit(seconds / 60, fractionDigits.minutes, 'm')
  if (seconds < daysFrom) return unit(seconds / 3600, fractionDigits.hours, 'h')
  return unit(seconds / 86400, fractionDigits.days, 'd')
}

export const ELAPSED_RELATIVE_AGO_JUST_NOW: ElapsedOptions = { suffix: ' ago', justNowBelow: 120 }
export const ELAPSED_RELATIVE_AGO: ElapsedOptions = { suffix: ' ago' }
export const ELAPSED_AGE_COMPACT: ElapsedOptions = {
  rounding: 'round',
  minutesFrom: 90,
  hoursFrom: 5400,
  daysFrom: 172800,
  fractionDigits: { hours: 1, days: 1 },
}
export const ELAPSED_FRESHNESS: ElapsedOptions = {
  rounding: 'round',
  daysFrom: Infinity,
  fractionDigits: { hours: 1 },
}

export function resolveCountryName(code: string): string {
  if (!code || code.length !== 2 || typeof Intl === 'undefined') return code
  try {
    return new Intl.DisplayNames(['en'], { type: 'region' }).of(code.toUpperCase()) || code
  } catch {
    return code
  }
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
  if (typeof value === 'number') {
    return field?.includes('bytes') ? formatBytes(value) : formatCount(value)
  }
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
  if (v == null) return '—'
  return `${decimals != null ? v.toFixed(decimals) : v}%`
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
  } catch {
    return ''
  }
}

export function parseFromInput(dateStr: string, tz: string) {
  try {
    const d = toDate(dateStr, { timeZone: tz })
    return isNaN(d.getTime()) ? null : d.toISOString()
  } catch {
    return null
  }
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
  } catch {
    return 'Invalid Date'
  }
}

export function formatTimeAgo(date: string | Date) {
  const d = toUTCDate(date)
  if (isNaN(d.getTime())) return ''
  const diffSec = differenceInSeconds(new Date(), d)
  if (diffSec < 0) return 'just now'
  if (diffSec < 60) return _relFmt ? _relFmt.format(-diffSec, 'second') : `${diffSec}s ago`
  const s = diffSec % 60
  const m = Math.floor((diffSec % 3600) / 60)
  if (diffSec < 3600) return `${m}m ${s}s ago`
  const h = Math.floor((diffSec % 86400) / 3600)
  if (diffSec < 86400) return `${h}h ${m}m ${s}s ago`
  return `${Math.floor(diffSec / 86400)}d ${h}h ${m}m ago`
}

export function getTimezoneAbbr(date: Date, tz: string) {
  return formatInTimeZone(date, tz, 'zzz')
}

export function formatLocalDateTime(
  value: string | number | Date,
  options?: Intl.DateTimeFormatOptions,
): string {
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
  const m = Math.floor(rounded / 60)
  const s = rounded % 60
  if (m < 60) return s > 0 ? `${m}m ${s}s` : `${m}m`
  const h = Math.floor(m / 60)
  const mRem = m % 60
  if (h < 24) return mRem > 0 ? `${h}h ${mRem}m` : `${h}h`
  const d = Math.floor(h / 24)
  const hRem = h % 24
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
  } catch {
    return iso
  }
}

export function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const utc = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(iso) ? iso : `${iso}Z`
  const d = new Date(utc)
  if (isNaN(d.getTime())) return iso
  return getDateTimeFormat('en-US', {
    timeZone: tz,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(d)
}

export function parseExtentMs(value: unknown, edge: 'earliest' | 'latest'): number | null {
  if (typeof value !== 'string' || value.length === 0) return null
  const iso = value.length === 10
    ? `${value}${edge === 'earliest' ? 'T00:00:00.000Z' : 'T23:59:59.999Z'}`
    : value
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : t
}

export function toQueryDate(d: Date): string {
  return `${d.toISOString().slice(0, 19)}Z`
}
