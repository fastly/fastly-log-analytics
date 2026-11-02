import { differenceInSeconds, formatDistanceToNow, parseISO } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'

const _numFmtCache = new Map<string, Intl.NumberFormat>()
function getNumberFormat(locale?: string, options?: Intl.NumberFormatOptions): Intl.NumberFormat {
  const key = `${locale ?? ''}|${options ? JSON.stringify(options) : ''}`
  let fmt = _numFmtCache.get(key)
  if (!fmt) _numFmtCache.set(key, (fmt = new Intl.NumberFormat(locale, options)))
  return fmt
}

function formatDecimal(n: number, minDigits: number, maxDigits = minDigits): string {
  return getNumberFormat('en-US', {
    minimumFractionDigits: minDigits,
    maximumFractionDigits: maxDigits,
    useGrouping: false,
  }).format(n)
}

const _dtFmtCache = new Map<string, Intl.DateTimeFormat>()
function getDateTimeFormat(locale: string | undefined, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `${locale ?? ''}|${JSON.stringify(options)}`
  let fmt = _dtFmtCache.get(key)
  if (!fmt) _dtFmtCache.set(key, (fmt = new Intl.DateTimeFormat(locale, options)))
  return fmt
}

const _relFmt =
  typeof Intl !== 'undefined' && typeof Intl.RelativeTimeFormat === 'function'
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
  return `${formatDecimal(value, 0, 2)} ${BYTE_UNITS[i]}`
}

export function formatBytesTiered(
  bytes: number | null | undefined,
  options: { maxUnit?: 'MB' | 'GB'; nullText?: string } = {},
): string {
  const { maxUnit = 'MB', nullText = '–' } = options
  if (bytes == null) return nullText
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${formatDecimal(bytes / 1024, 1)} KB`
  if (maxUnit === 'MB' || bytes < 1024 * 1024 * 1024) return `${formatDecimal(bytes / 1024 / 1024, 1)} MB`
  return `${formatDecimal(bytes / 1024 / 1024 / 1024, 2)} GB`
}

export interface ElapsedOptions {
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
  const unit = (v: number, digits: number | undefined, label: string) =>
    `${digits ? formatDecimal(v, digits) : r(v)}${label}${suffix}`
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
  if (n >= 1_000_000) return `${formatDecimal(n / 1_000_000, 0, 1)}M`
  return n >= 1_000 ? `${formatDecimal(n / 1_000, 0, 1)}k` : String(n)
}

export function formatAbbreviatedCount(n: number, millionDecimals = 1): string {
  if (n >= 1e9) return `${formatDecimal(n / 1e9, millionDecimals)}B`
  if (n >= 1e6) return `${formatDecimal(n / 1e6, millionDecimals)}M`
  return n >= 1e3 ? `${formatDecimal(n / 1e3, 1)}K` : formatCount(n)
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
    if (!isNaN(num)) return formatDecimal(num, 0, 1)
  }
  return str
}

export function formatUSD(n: number, options?: { subDollarDigits?: number }): string {
  if (n >= 1000) return getNumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(Math.round(n))
  const digits = n >= 1 ? 2 : (options?.subDollarDigits ?? 4)
  return getNumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(n)
}

export function formatCurrency(amount: number): string {
  return amount >= 1000 ? `$${formatDecimal(amount / 1000, 0, 1)}k` : formatUSD(amount)
}

export function calculateDelta(current: number, previous: number | undefined): number | null {
  return previous ? ((current - previous) / previous) * 100 : null
}

export function formatPercent(ratio: number, options?: { decimals?: number }): string {
  const digits = options?.decimals ?? 1
  return getNumberFormat('en-US', {
    style: 'percent',
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
    useGrouping: false,
  }).format(ratio)
}

export function formatPctValue(v: number | null | undefined, decimals?: number): string {
  return v == null ? '—' : `${decimals != null ? formatDecimal(v, decimals) : v}%`
}

export function formatMs(v: number | null | undefined, decimals = 1, unit = ' ms'): string {
  return v == null ? '—' : `${formatDecimal(v, decimals)}${unit}`
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
  return diffSec < 86400 ? `${h}h ${m}m ${s}s ago` : `${Math.floor(diffSec / 86400)}d ${h}h ${m}m ago`
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
    const formatted = getDateTimeFormat('en-US', {
      timeZone: 'UTC',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }).format(d)
    return `${formatted} UTC`
  } catch {
    return iso
  }
}

export function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const d = toUTCDate(iso)
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

export function toQueryDate(d: Date): string {
  return formatInTimeZone(d, 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'")
}

export function formatDateTime(isoString: string | null | undefined, timeZone?: string): string {
  return isoString ? formatDate(isoString, timeZone || 'UTC', 'MMM d, yyyy h:mm:ss a') : 'Unknown'
}

export const SHIELD_MAP: Record<string, string> = {
  'us-east-1': 'iad-va-us',
  'us-west': 'bfi-wa-us',
  'us-central-1': 'chi-il-us',
  'eu-central-1': 'frankfurt-de',
  'eu-south-1': 'mxp-milan-it',
  'eu-west-1': 'mxp-paris-fr',
  'uk-east-1': 'london-uk',
  'jp-central-1': 'nrt-tokyo-jp',
  'au-east-1': 'sydney-au',
}

export const REGION_LABELS: Record<string, string> = {
  'us-east-1': 'US East (Ashburn)',
  'us-west': 'US West (Seattle)',
  'us-central-1': 'US Central (Chicago)',
  'eu-central-1': 'EU Central (Frankfurt)',
  'eu-south-1': 'EU South (Milan)',
  'eu-west-1': 'EU West (Paris)',
  'uk-east-1': 'UK East (London)',
  'jp-central-1': 'JP Central (Tokyo)',
  'au-east-1': 'AU East (Sydney)',
}

export const SHIELD_LABELS: Record<string, string> = {
  none: 'None',
  'iad-va-us': 'IAD (Ashburn)',
  'bfi-wa-us': 'BFI (Seattle)',
  'chi-il-us': 'CHI (Chicago)',
  'frankfurt-de': 'FRA (Frankfurt)',
  'mxp-milan-it': 'MXP (Milan)',
  'mxp-paris-fr': 'MXP (Paris)',
  'london-uk': 'LHR (London)',
  'nrt-tokyo-jp': 'NRT (Tokyo)',
  'sydney-au': 'SYD (Sydney)',
}

export const PERIOD_LABELS: Record<string, string> = {
  '1': '1 second',
  '5': '5 seconds',
  '10': '10 seconds',
  '20': '20 seconds',
  '30': '30 seconds',
  '60': '1 minute',
  '120': '2 minutes',
  '300': '5 minutes',
  '600': '10 minutes',
  '900': '15 minutes',
}

export const RETENTION_OPTIONS = [
  { value: '1', label: '1 day' },
  { value: '3', label: '3 days' },
  { value: '7', label: '7 days' },
  { value: '14', label: '14 days' },
  { value: '30', label: '30 days' },
  { value: '90', label: '90 days' },
  { value: '180', label: '180 days' },
  { value: '365', label: '365 days' },
  { value: '0', label: 'Forever' },
] as const
