import { useCallback, useMemo } from 'react'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { formatDate, formatRelative, formatCompactRelative, formatTimeAgo, getTimezoneAbbr } from '@/lib/date'

export function useDateFormat() {
  const timezone = useTimezoneStore(state => state.timezone)

  const short = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm a'), [timezone])
  const full = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm:ss a'), [timezone])
  const time = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm a'), [timezone])
  const timeMs = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm:ss.SSS a'), [timezone])
  const abbr = useCallback(() => getTimezoneAbbr(new Date(), timezone), [timezone])
  const relative = useCallback((date: string | Date) => formatRelative(date), [])
  const timeAgo = useCallback((date: string | Date) => formatTimeAgo(typeof date === 'string' ? date : date.toISOString()), [])
  const compactRelative = useCallback((date: string | Date) => formatCompactRelative(date), [])
  const format = useCallback((date: string | Date, pattern: string) => formatDate(date, timezone, pattern), [timezone])

  return useMemo(() => ({
    short,
    full,
    time,
    timeMs,
    abbr,
    relative,
    timeAgo,
    compactRelative,
    format
  }), [short, full, time, timeMs, abbr, relative, timeAgo, compactRelative, format])
}
