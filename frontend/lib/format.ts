import { formatDistanceToNow, parseISO } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'

/**
 * Centralized formatting and date utilities for the Fastly Log Analysis frontend.
 */

// Binary units (1 KB = 1024 B); matches GCP/AWS/Fastly console conventions.
// Bumps to the next unit once the value would show 4+ digits (e.g. 1023.24 MB
// reads as "1 GB" instead), rather than only at the strict 1024 boundary.
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = bytes
  let i = 0
  while (value >= 1000 && i < sizes.length - 1) {
    value /= k
    i++
  }
  return parseFloat(value.toFixed(2)) + ' ' + sizes[i]
}

/**
 * Binary byte formatter with fixed per-unit decimals (B as-is, KB/MB 1 decimal,
 * GB 2 decimals) and strict 1024 thresholds. ``maxUnit: 'MB'`` keeps counting
 * in MB past 1 GiB. Distinct from ``formatBytes`` above, which bumps units at
 * 1000 and trims trailing zeros.
 */
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
  /** Seconds at which the minute / hour / day units take over. */
  minutesFrom?: number
  hoursFrom?: number
  /** ``Infinity`` caps the ladder at hours. */
  daysFrom?: number
  rounding?: 'floor' | 'round'
  /** Fixed decimals for the second / minute / hour / day units (0 = integer via ``rounding``). */
  fractionDigits?: { seconds?: number; minutes?: number; hours?: number; days?: number }
  /** Below this many seconds, render ``just now`` (no suffix). */
  justNowBelow?: number
  suffix?: string
  fallback?: string
}

/** Compact elapsed-time ladder: 45s / 12m / 3h / 6d, parameterised so every
 *  caller keeps its own thresholds, rounding and decimals. */
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

/** BotSourcesPanel: "just now" under 2 minutes, then m / h / d ago. */
export const ELAPSED_RELATIVE_AGO_JUST_NOW: ElapsedOptions = { suffix: ' ago', justNowBelow: 120 }
/** ResumeBanner: s / m / h / d ago. */
export const ELAPSED_RELATIVE_AGO: ElapsedOptions = { suffix: ' ago' }
/** SystemHealthCard: 45s / 12m / 3.4h / 6.2d, rounded. */
export const ELAPSED_AGE_COMPACT: ElapsedOptions = {
  rounding: 'round',
  minutesFrom: 90,
  hoursFrom: 5400,
  daysFrom: 172800,
  fractionDigits: { hours: 1, days: 1 },
}
/** HighScaleRequestFactsPanel: 45s / 12m / 3.4h (never days), rounded. */
export const ELAPSED_FRESHNESS: ElapsedOptions = {
  rounding: 'round',
  daysFrom: Infinity,
  fractionDigits: { hours: 1 },
}

export function resolveCountryName(code: string): string {
  if (!code || code.length !== 2 || typeof Intl === 'undefined') return code
  try {
    const regionNames = new Intl.DisplayNames(['en'], { type: 'region' })
    return regionNames.of(code.toUpperCase()) || code
  } catch {
    return code
  }
}

export function formatCompactCount(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1).replace(/\.0$/, '') + 'k'
  return String(n)
}

export function formatAbbreviatedCount(n: number, millionDecimals = 1): string {
  if (n >= 1e9) return (n / 1e9).toFixed(millionDecimals) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(millionDecimals) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return formatCount(n)
}

const _defaultNumberFormat = new Intl.NumberFormat(undefined)
const _optionsNumberFormatCache = new Map<string, Intl.NumberFormat>()

export function formatCount(n: number, options?: Intl.NumberFormatOptions): string {
  if (!options) {
    return _defaultNumberFormat.format(n)
  }
  const key = JSON.stringify(options)
  let fmt = _optionsNumberFormatCache.get(key)
  if (!fmt) {
    fmt = new Intl.NumberFormat(undefined, options)
    _optionsNumberFormatCache.set(key, fmt)
  }
  return fmt.format(n)
}

export function formatCountMaybe(n: number | null | undefined, options?: Intl.NumberFormatOptions): string | undefined {
  return n == null ? undefined : formatCount(n, options)
}

/**
 * Formats a raw value (like bytes or country codes) based on its field name.
 */
export function formatValue(field: string | undefined, value: string | number | null | undefined): string {
  if (value === null || value === undefined) return 'null'

  if (typeof value === 'number') {
    if (field?.includes('bytes')) return formatBytes(value)
    return value.toLocaleString()
  }

  const str = String(value)

  // Country Code resolution
  if (field === 'country' && str.length === 2) {
    return resolveCountryName(str)
  }

  if (field === 'pop' || field === 'region') {
    return str.toUpperCase()
  }

  if (field === 'city') {
    return str.toLowerCase().replace(/(^|[^\w])(\w)/g, m => m.toUpperCase())
  }

  if (field === 'proto' || field === 'tls') {
    const num = Number(value)
    if (!isNaN(num)) return parseFloat(num.toFixed(1)).toString()
  }

  return str
}

export function formatCurrency(amount: number): string {
  if (amount >= 1000) return '$' + (amount / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (amount >= 1) return '$' + amount.toFixed(2)
  return '$' + amount.toFixed(4)
}

export function formatUSD(n: number, options?: { subDollarDigits?: number }): string {
  if (n >= 1000) return '$' + n.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  if (n >= 1) return '$' + n.toFixed(2)
  return '$' + n.toFixed(options?.subDollarDigits ?? 4)
}

/**
 * Calculates the percentage delta between two numeric values.
 */
export function calculateDelta(current: number, previous: number | undefined): number | null {
  if (previous === undefined || previous === 0) return null;
  return ((current - previous) / previous) * 100;
}

/**
 * Formats a percentage ratio (e.g. 0.1234 -> "12.3%").
 * Expects a ratio between 0 and 1.
 */
export function formatPercent(ratio: number, options?: { decimals?: number }): string {
  const decimals = options?.decimals ?? 1
  return `${(ratio * 100).toFixed(decimals)}%`
}

/**
 * Formats an already-scaled percentage value (e.g. 12.3 -> "12.3%") with nullish fallback.
 */
export function formatPctValue(v: number | null | undefined, decimals?: number): string {
  if (v == null) return '—'
  return `${decimals != null ? v.toFixed(decimals) : v}%`
}

/**
 * Formats a millisecond value with nullish fallback.
 */
export function formatMs(v: number | null | undefined, decimals = 1, unit = ' ms'): string {
  if (v == null) return '—'
  return `${v.toFixed(decimals)}${unit}`
}

/**
 * Tailwind badge classes keyed by HTTP status class.
 * - `'class'` (default): 2xx emerald, 3xx sky, 4xx amber, 5xx destructive.
 * - `'traffic-light'`: 2xx green, 3xx yellow, everything else red; a falsy
 *   status renders muted (the Insights cache-collapse modal's palette).
 */
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

// ---------------------------------------------------------------------------
// Date & Timezone Helpers (folded from date.ts)
// ---------------------------------------------------------------------------

export function toUTCDate(date: string | Date): Date {
  if (date instanceof Date) return date
  if (!date) return new Date(NaN)

  // If it's already a valid ISO string with timezone, parse it
  if (date.includes('T') && (date.includes('Z') || /[+-]\d{2}:?\d{2}$/.test(date))) {
    return parseISO(date)
  }

  // Handle common "YYYY-MM-DD HH:mm:ss" format from backends
  const utcStr = date.includes('Z') || /[+-]\d{2}:?\d{2}$/.test(date)
    ? date.replace(' ', 'T')
    : date.replace(' ', 'T') + 'Z'

  return parseISO(utcStr)
}

export function formatForInput(date: string | Date, tz: string) {
  try {
    const d = toUTCDate(date)
    if (isNaN(d.getTime())) return ''
    return formatInTimeZone(d, tz, "yyyy-MM-dd'T'HH:mm:ss")
  } catch {
    return ''
  }
}

export function parseFromInput(dateStr: string, tz: string) {
  try {
    const d = toDate(dateStr, { timeZone: tz })
    if (isNaN(d.getTime())) return null
    return d.toISOString()
  } catch {
    return null
  }
}

export function formatDate(date: string | Date, tz: string, pattern: string = 'yyyy-MM-dd h:mm:ss a') {
  try {
    const d = toUTCDate(date)
    if (isNaN(d.getTime())) return 'Invalid Date'
    return formatInTimeZone(d, tz, pattern)
  } catch (e) {
    console.error('Error formatting date:', date, e)
    return 'Invalid Date'
  }
}

export function formatRelative(date: string | Date) {
  try {
    const d = toUTCDate(date)
    if (isNaN(d.getTime())) return 'Invalid Date'
    return formatDistanceToNow(d, { addSuffix: true })
  } catch {
    return 'Invalid Date'
  }
}

function getTimeDiff(date: string | Date) {
  const d = toUTCDate(date)
  if (isNaN(d.getTime())) return null

  const now = new Date()
  const diffSec = Math.floor((now.getTime() - d.getTime()) / 1000)
  return { diffSec, absSec: Math.abs(diffSec) }
}

export function formatTimeAgo(date: string | Date) {
  const diff = getTimeDiff(date)
  if (!diff) return ''
  const { diffSec } = diff

  if (diffSec < 0) return "just now"
  if (diffSec < 60) return `${diffSec}s ago`

  if (diffSec < 3600) {
    const m = Math.floor(diffSec / 60)
    const s = diffSec % 60
    return `${m}m ${s}s ago`
  }
  if (diffSec < 86400) {
    const h = Math.floor(diffSec / 3600)
    const m = Math.floor((diffSec % 3600) / 60)
    const s = diffSec % 60
    return `${h}h ${m}m ${s}s ago`
  }
  const d = Math.floor(diffSec / 86400)
  const h = Math.floor((diffSec % 86400) / 3600)
  const m = Math.floor((diffSec % 3600) / 60)
  return `${d}d ${h}h ${m}m ago`
}

export function getTimezoneAbbr(date: Date, tz: string) {
  return formatInTimeZone(date, tz, 'zzz')
}

export function formatLocalDateTime(
  value: string | number | Date,
  options?: Intl.DateTimeFormatOptions,
): string {
  const d = value instanceof Date ? value : new Date(value)
  return d.toLocaleString(undefined, options)
}

export function formatCompactDuration(seconds: number): string {
  if (seconds <= 0) return 'any second'
  return formatElapsed(seconds)
}

export function formatDuration(seconds: number | null | undefined, options?: { round?: boolean }): string {
  if (seconds == null || seconds < 0) return '—'
  const round = options?.round ?? true
  const rounded = round ? Math.round(seconds) : seconds
  if (rounded < 60) return `${rounded}s`

  const m = Math.floor(rounded / 60)
  const s = rounded % 60
  if (m < 60) {
    return s > 0 ? `${m}m ${s}s` : `${m}m`
  }

  const h = Math.floor(m / 60)
  const mRemaining = m % 60
  if (h < 24) {
    return mRemaining > 0 ? `${h}h ${mRemaining}m` : `${h}h`
  }

  const d = Math.floor(h / 24)
  const hRemaining = h % 24
  return hRemaining > 0 ? `${d}d ${hRemaining}h` : `${d}d`
}

/**
 * Safari-safe, deterministic UTC timestamp formatter.
 * Connects the date and time parts manually to bypass browser connection-string locale drift
 * (which throws React hydration errors on dehydrated SSR pages).
 */
export function formatDeterministicUTC(iso: string | null | undefined): string {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    if (isNaN(d.getTime())) return iso

    const datePart = d.toLocaleDateString('en-US', {
      timeZone: 'UTC',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    })
    const timePart = d.toLocaleTimeString('en-US', {
      timeZone: 'UTC',
      hour: '2-digit',
      minute: '2-digit',
    })
    return `${datePart}, ${timePart} UTC`
  } catch {
    return iso
  }
}

const _bucketFormatters = new Map<string, Intl.DateTimeFormat>()

function _getBucketFormatter(tz: string): Intl.DateTimeFormat {
  let fmt = _bucketFormatters.get(tz)
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    })
    _bucketFormatters.set(tz, fmt)
  }
  return fmt
}

/** Map-bucket label ("Jun 15, 3:30:45 PM") in ``tz``; naive ISO strings are read as UTC, unparseable input is returned as-is. */
export function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const utc = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(iso) ? iso : iso + 'Z'
  const d = new Date(utc)
  if (isNaN(d.getTime())) return iso
  return _getBucketFormatter(tz).format(d)
}

/**
 * Parse a status-cache extent string to epoch millis. Handles full ISO
 * timestamps and the date-only ("YYYY-MM-DD") shorthand the status cache
 * can emit: for date-only values the `earliest` edge is treated as
 * start-of-day and the `latest` edge as end-of-day (UTC), mirroring the
 * snap parsing in FilterBar. Returns null for missing/invalid input.
 */
export function parseExtentMs(value: unknown, edge: 'earliest' | 'latest'): number | null {
  if (typeof value !== 'string' || value.length === 0) return null
  const iso =
    value.length === 10
      ? `${value}${edge === 'earliest' ? 'T00:00:00.000Z' : 'T23:59:59.999Z'}`
      : value
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : t
}

export function toQueryDate(d: Date): string {
  return d.toISOString().slice(0, 19) + 'Z'
}
