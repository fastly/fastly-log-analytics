import { formatDistanceToNow, parseISO } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'
import { formatElapsed } from '@/lib/format'

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
  } catch (e) {
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
