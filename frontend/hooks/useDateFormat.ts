import { useCallback, useMemo } from 'react'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { formatDate, formatRelative, formatTimeAgo, getTimezoneAbbr } from '@/lib/date'

/**
 * Active timezone (IANA tz string). Subscribe here when a component formats
 * dates or labels axes; orthogonal to time range and service selection.
 */
export function useTimezone(): string {
  return useTimezoneStore(s => s.timezone)
}

export function useDateFormat() {
  const timezone = useTimezoneStore(state => state.timezone)

  const short = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm a'), [timezone])
  const full = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm:ss a'), [timezone])
  const time = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm a'), [timezone])
  const timeMs = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm:ss.SSS a'), [timezone])
  const abbr = useCallback(() => getTimezoneAbbr(new Date(), timezone), [timezone])
  const relative = useCallback((date: string | Date) => formatRelative(date), [])
  const timeAgo = useCallback((date: string | Date) => formatTimeAgo(typeof date === 'string' ? date : date.toISOString()), [])
  const format = useCallback((date: string | Date, pattern: string) => formatDate(date, timezone, pattern), [timezone])

  return useMemo(() => ({
    short,
    full,
    time,
    timeMs,
    abbr,
    relative,
    timeAgo,
    format
  }), [short, full, time, timeMs, abbr, relative, timeAgo, format])
}
