import { formatDistanceToNow, parseISO } from 'date-fns'
import { formatInTimeZone, toDate } from 'date-fns-tz'

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

/**
 * Shared >=60s unit ladder for the compact formatters: <60→s, <3600→m,
 * <86400→h, else→d, each with Math.floor. Callers handle their own
 * zero/parens branches around this.
 */
function compactUnit(s: number): string {
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m`
  if (s < 86400) return `${Math.floor(s / 3600)}h`
  return `${Math.floor(s / 86400)}d`
}

export function formatCompactRelative(date: string | Date) {
  const diff = getTimeDiff(date)
  if (!diff) return ''
  const { absSec } = diff

  if (absSec <= 1) return '(any second)'
  return `(${compactUnit(absSec)})`
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

/**
 * Format a duration in seconds as the largest single unit (e.g. "5s",
 * "10m", "5h", "3d"). Negative or zero values return
 * "any second" — used by countdown timers (next-run badges) that hit
 * zero before the next interval fires.
 */
export function formatCompactDuration(seconds: number): string {
  if (seconds <= 0) return 'any second'
  return compactUnit(seconds)
}

/**
 * Format a duration in seconds into a human-readable string with dynamic detail
 * (e.g. "45s", "12m 30s", "3h 15m", "5d 6h").
 */
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
