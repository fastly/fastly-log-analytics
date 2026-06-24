/**
 * @vitest-environment jsdom
 *
 * Audit finding: `useDataWindowOverlap` produces the data-window
 * banner status (FilterBar + dashboard). It has 4 real states, a 60s
 * grace window to suppress ingest-tick flicker at boundaries, and a
 * `toIsoFromLooseExtent` helper that widens a date-only extent. None
 * of those branches had direct coverage — regressions would land as a
 * perma-banner / never-fired banner that look like UI bugs unrelated
 * to the time-math. Strategy: seed the `['log-extents', sid]` cache
 * entry directly so `isSuccess` is synchronously true (the same cache
 * path the hook takes in production alongside FilterBar).
 */
import { renderHook } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, vi } from 'vitest'

// Per-test mutable filter window — the mock below reads through these.
let storedStart = ''
let storedEnd = ''
const ACTIVE_SID = 'svc-test'

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) => {
    const state = { activeServiceId: ACTIVE_SID }
    return selector ? selector(state) : state
  })
  useServiceStore.getState = () => ({ activeServiceId: ACTIVE_SID })
  return { useServiceStore }
})

vi.mock('@/stores/filterStore', () => {
  const useFilterStore: any = vi.fn((selector?: (s: any) => any) => {
    const state = { startTime: storedStart, endTime: storedEnd }
    return selector ? selector(state) : state
  })
  useFilterStore.getState = () => ({ startTime: storedStart, endTime: storedEnd })
  return { useFilterStore }
})

/**
 * Build a wrapper whose QueryClient is pre-seeded with a log-extents
 * cache entry keyed by activeServiceId. Passing `undefined` leaves the
 * cache empty (drives the "unknown" status — query never resolved).
 */
function wrapperWithExtents(extents: { earliest_log_at: string | null; latest_log_at: string | null } | undefined) {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  if (extents !== undefined) {
    qc.setQueryData(['log-extents', ACTIVE_SID], extents)
  }
  return makeQueryWrapper(qc)
}

describe('useDataWindowOverlap', () => {
  beforeEach(() => {
    storedStart = ''
    storedEnd = ''
    vi.clearAllMocks()
  })

  const EXTENTS = {
    earliest_log_at: '2026-06-01T00:00:00.000Z',
    latest_log_at: '2026-06-15T00:00:00.000Z',
  }

  it('returns status="ok" when the selected window is fully inside log extents', async () => {
    storedStart = '2026-06-10T12:00:00.000Z'
    storedEnd = '2026-06-10T18:00:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(EXTENTS),
    })
    expect(result.current.status).toBe('ok')
    expect(result.current.earliestLogAt).toBe(EXTENTS.earliest_log_at)
    expect(result.current.latestLogAt).toBe(EXTENTS.latest_log_at)
    expect(result.current.pickedStart).toBe(storedStart)
    expect(result.current.pickedEnd).toBe(storedEnd)
  })

  it('returns status="before-earliest" when selected_end is before earliest minus 60s grace', async () => {
    // 2 minutes before earliest — well outside grace.
    storedStart = '2026-05-31T22:00:00.000Z'
    storedEnd = '2026-05-31T23:58:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(EXTENTS),
    })
    expect(result.current.status).toBe('before-earliest')
  })

  it('returns status="after-latest" when selected_start is after latest plus 60s grace', async () => {
    storedStart = '2026-06-15T00:02:00.000Z'
    storedEnd = '2026-06-15T01:00:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(EXTENTS),
    })
    expect(result.current.status).toBe('after-latest')
  })

  it('returns status="no-data" when the extents query resolves with null extents', async () => {
    storedStart = '2026-06-10T00:00:00.000Z'
    storedEnd = '2026-06-10T01:00:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents({ earliest_log_at: null, latest_log_at: null }),
    })
    expect(result.current.status).toBe('no-data')
    expect(result.current.earliestLogAt).toBeNull()
    expect(result.current.latestLogAt).toBeNull()
  })

  it('returns status="unknown" before the extents query has resolved (initial render)', async () => {
    storedStart = '2026-06-10T00:00:00.000Z'
    storedEnd = '2026-06-10T01:00:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(undefined),
    })
    expect(result.current.status).toBe('unknown')
  })

  it('grace window (60s) suppresses false positives at the earliest boundary', async () => {
    // selected_end is 30s before earliest — still "ok" under 60s grace
    // (banner-flicker suppression around the /api/log-extents snapshot
    // lag).
    storedStart = '2026-05-31T23:30:00.000Z'
    storedEnd = '2026-05-31T23:59:30.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(EXTENTS),
    })
    expect(result.current.status).toBe('ok')
  })

  it('toIsoFromLooseExtent widens a date-only earliest ("2026-06-15") to 00:00:00.000Z (isEnd=false)', async () => {
    // Morning pick on 2026-06-15. earliest is date-only — without the
    // isEnd=false widener, Date.parse on the bare date would land at
    // local midnight and this could falsely flag "before-earliest" off
    // the UTC test machine.
    storedStart = '2026-06-15T08:00:00.000Z'
    storedEnd = '2026-06-15T09:00:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents({
        earliest_log_at: '2026-06-15',
        latest_log_at: '2026-06-20T00:00:00.000Z',
      }),
    })
    expect(result.current.status).toBe('ok')
  })

  it('toIsoFromLooseExtent widens a date-only latest ("2026-06-15") to 23:59:59.999Z (isEnd=true)', async () => {
    // Evening pick on 2026-06-15 with date-only latest — without the
    // isEnd=true widener, would falsely flag "after-latest".
    storedStart = '2026-06-15T22:00:00.000Z'
    storedEnd = '2026-06-15T23:30:00.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents({
        earliest_log_at: '2026-06-01T00:00:00.000Z',
        latest_log_at: '2026-06-15',
      }),
    })
    expect(result.current.status).toBe('ok')
  })

  it('toIsoFromLooseExtent passes already-ISO strings through unchanged', async () => {
    // Full-ISO earliest + selected_end 30s before it must stay "ok" under
    // grace — i.e. the widener didn't mangle the string into a different
    // instant. earliestLogAt is returned verbatim.
    storedStart = '2026-06-01T11:55:00.000Z'
    storedEnd = '2026-06-01T11:59:30.000Z'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents({
        earliest_log_at: '2026-06-01T12:00:00.000Z',
        latest_log_at: '2026-06-15T12:00:00.000Z',
      }),
    })
    expect(result.current.status).toBe('ok')
    expect(result.current.earliestLogAt).toBe('2026-06-01T12:00:00.000Z')
    expect(result.current.latestLogAt).toBe('2026-06-15T12:00:00.000Z')
  })

  it('returns status="unknown" when selected window is unparseable (Number.isFinite guard)', async () => {
    // Empty / garbage strings make Date.parse return NaN. Without the
    // Number.isFinite guard at lines ~72-73, NaN comparisons would
    // silently land in "ok" (NaN < anything is always false).
    storedStart = ''
    storedEnd = 'not-a-date'
    const { useDataWindowOverlap } = await import('@/hooks/useDataWindowOverlap')
    const { result } = renderHook(() => useDataWindowOverlap(), {
      wrapper: wrapperWithExtents(EXTENTS),
    })
    expect(result.current.status).toBe('unknown')
  })
})
