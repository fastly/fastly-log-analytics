import { subDays } from 'date-fns'
import { toUTCDate } from '@/lib/date'

// Shared by FilterBar.tsx (client-side extents sync) and lib/ssr/*.ts (SSR
// seed key resolution) so both sides make the identical "snap to real data"
// decision from the same log_extents payload — see FilterBar.tsx's
// autoSetRange effect, which this was extracted from verbatim.
export const EXTENTS_STALE_THRESHOLD_MINUTES = 15

export interface LogExtents {
  earliest_log_at?: string | null
  latest_log_at?: string | null
}

export interface SnappedWindow {
  start: string
  end: string
}

/**
 * Safely narrow the untyped `bootstrap.log_extents` blob SSR fetchers get
 * from fetchBootstrapServerSide() (typed `unknown` at that boundary — the
 * bootstrap response shape isn't shared with the frontend). Mirrors the
 * narrowing lib/ssr/insights.ts's earliestFromLogExtents already does for
 * the same field.
 */
export function narrowLogExtents(value: unknown): LogExtents | undefined {
  if (!value || typeof value !== 'object') return undefined
  const v = value as Record<string, unknown>
  return {
    earliest_log_at: typeof v.earliest_log_at === 'string' ? v.earliest_log_at : null,
    latest_log_at: typeof v.latest_log_at === 'string' ? v.latest_log_at : null,
  }
}

/**
 * Decide whether the visible window should snap to the service's actual
 * log extents instead of the naive "last 24h ending now" default.
 *
 * Returns null when no snap is warranted: extents are missing, or the
 * latest log is within EXTENTS_STALE_THRESHOLD_MINUTES of `now` (data is
 * actively flowing, so the default window already captures it).
 */
export function resolveSnappedWindow(
  logExtents: LogExtents | null | undefined,
  now: Date = new Date(),
): SnappedWindow | null {
  if (!logExtents?.earliest_log_at || !logExtents?.latest_log_at) return null

  const earliestLog = toUTCDate(
    logExtents.earliest_log_at.length === 10
      ? logExtents.earliest_log_at + 'T00:00:00.000Z'
      : logExtents.earliest_log_at,
  )
  const latestLog = toUTCDate(
    logExtents.latest_log_at.length === 10
      ? logExtents.latest_log_at + 'T23:59:59.999Z'
      : logExtents.latest_log_at,
  )

  const ageMinutes = (now.getTime() - latestLog.getTime()) / (1000 * 60)
  if (ageMinutes <= EXTENTS_STALE_THRESHOLD_MINUTES) return null

  const spanDays = (latestLog.getTime() - earliestLog.getTime()) / (1000 * 3600 * 24)

  let finalStart: string
  let finalEnd: string

  if (spanDays <= 1 && spanDays >= 0) {
    // 1 day of data or less — show the entire available range.
    finalStart = earliestLog.toISOString()
    finalEnd = latestLog.toISOString()
  } else {
    // More than 1 day — show the most recent 24 hours of data.
    finalEnd = latestLog.toISOString()
    finalStart = subDays(latestLog, 1).toISOString()
  }

  // Degenerate-extent guard: a service with a single log — or all logs
  // sharing one timestamp — has earliest_log_at === latest_log_at, so the
  // snapped window collapses to finalStart === finalEnd. Widen to a 1-hour
  // window centred on the log so the dashboard renders it (with context)
  // instead of the backend's half-open-range clamp rejecting a zero-width
  // window outright.
  if (new Date(finalEnd).getTime() <= new Date(finalStart).getTime()) {
    const anchorMs = new Date(finalEnd).getTime()
    finalStart = new Date(anchorMs - 30 * 60 * 1000).toISOString()
    finalEnd = new Date(anchorMs + 30 * 60 * 1000).toISOString()
  }

  return { start: finalStart, end: finalEnd }
}
