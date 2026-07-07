import { describe, expect, it } from 'vitest'
import { resolveSnappedWindow, narrowLogExtents, EXTENTS_STALE_THRESHOLD_MINUTES } from '@/lib/log-extents-snap'

// Ported from FilterBar.tsx's former inline snap-decision effect — these cases
// pin the SAME behavior now shared between FilterBar (client) and the 4 SSR
// fetchers (lib/ssr/dashboard.ts, security.ts, origin.ts, performance.ts).

describe('resolveSnappedWindow', () => {
  it('returns null when extents are missing', () => {
    expect(resolveSnappedWindow(null)).toBeNull()
    expect(resolveSnappedWindow(undefined)).toBeNull()
    expect(resolveSnappedWindow({})).toBeNull()
    expect(resolveSnappedWindow({ earliest_log_at: '2026-06-29T00:00:00Z' })).toBeNull()
  })

  it('returns null when the latest log is fresher than the staleness threshold', () => {
    const now = new Date('2026-06-29T12:00:00Z')
    const fresh = new Date(now.getTime() - (EXTENTS_STALE_THRESHOLD_MINUTES - 1) * 60 * 1000)
    const got = resolveSnappedWindow(
      { earliest_log_at: '2026-06-01T00:00:00Z', latest_log_at: fresh.toISOString() },
      now,
    )
    expect(got).toBeNull()
  })

  it('snaps to [latest-24h, latest] when stale and span > 1 day', () => {
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveSnappedWindow(
      { earliest_log_at: '2026-06-01T00:00:00Z', latest_log_at: '2026-06-29T11:00:00Z' },
      now,
    )
    expect(got).toEqual({
      start: new Date('2026-06-28T11:00:00Z').toISOString(),
      end: new Date('2026-06-29T11:00:00Z').toISOString(),
    })
  })

  it('snaps to the full available range when stale and span <= 1 day', () => {
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveSnappedWindow(
      { earliest_log_at: '2026-06-29T02:00:00Z', latest_log_at: '2026-06-29T11:00:00Z' },
      now,
    )
    expect(got).toEqual({
      start: new Date('2026-06-29T02:00:00Z').toISOString(),
      end: new Date('2026-06-29T11:00:00Z').toISOString(),
    })
  })

  it('widens a degenerate (single-log) window to ±30 minutes', () => {
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveSnappedWindow(
      { earliest_log_at: '2026-06-29T11:00:00Z', latest_log_at: '2026-06-29T11:00:00Z' },
      now,
    )
    expect(got).toEqual({
      start: new Date('2026-06-29T10:30:00Z').toISOString(),
      end: new Date('2026-06-29T11:30:00Z').toISOString(),
    })
  })

  it('accepts date-only extent strings (widened to full-day bounds)', () => {
    const now = new Date('2026-07-01T12:00:00Z')
    const got = resolveSnappedWindow({ earliest_log_at: '2026-06-01', latest_log_at: '2026-06-29' }, now)
    expect(got).not.toBeNull()
    expect(got!.end).toBe(new Date('2026-06-29T23:59:59.999Z').toISOString())
  })

  it('exactly at the staleness threshold is still "fresh" (<=, not <)', () => {
    const now = new Date('2026-06-29T12:00:00Z')
    const boundary = new Date(now.getTime() - EXTENTS_STALE_THRESHOLD_MINUTES * 60 * 1000)
    const got = resolveSnappedWindow(
      { earliest_log_at: '2026-06-01T00:00:00Z', latest_log_at: boundary.toISOString() },
      now,
    )
    expect(got).toBeNull()
  })
})

describe('narrowLogExtents', () => {
  it('narrows a well-formed extents object', () => {
    expect(narrowLogExtents({ earliest_log_at: 'a', latest_log_at: 'b', extra: 'ignored' })).toEqual({
      earliest_log_at: 'a',
      latest_log_at: 'b',
    })
  })

  it('returns undefined for non-object / missing values', () => {
    expect(narrowLogExtents(null)).toBeUndefined()
    expect(narrowLogExtents(undefined)).toBeUndefined()
    expect(narrowLogExtents('not-an-object')).toBeUndefined()
  })

  it('nulls out non-string fields instead of passing them through', () => {
    expect(narrowLogExtents({ earliest_log_at: 123, latest_log_at: null })).toEqual({
      earliest_log_at: null,
      latest_log_at: null,
    })
  })
})
