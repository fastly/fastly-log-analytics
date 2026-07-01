/**
 * @vitest-environment jsdom
 *
 * useInsightsDefaults picks the Insights window/baseline default from how much
 * history a service has (via the shared ['log-extents', sid] cache) while
 * letting an explicit user pick win and dropping it on service switch. Seed
 * the cache directly (same path the hook takes alongside useBootstrap) so the
 * adaptive value is available synchronously — no network.
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { useInsightsDefaults } from '@/hooks/useInsightsDefaults'

// The hook only calls client.GET on a cache miss; we pre-seed, so this guards
// against an accidental real fetch rather than being exercised.
vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(async () => ({ data: undefined })) },
}))

function hoursAgoIso(h: number): string {
  return new Date(Date.now() - h * 3_600_000).toISOString()
}

function seedExtents(qc: ReturnType<typeof createTestQueryClient>, sid: string, earliestHoursAgo: number) {
  qc.setQueryData(['log-extents', sid], {
    earliest_log_at: hoursAgoIso(earliestHoursAgo),
    latest_log_at: hoursAgoIso(0),
  })
}

describe('useInsightsDefaults', () => {
  beforeEach(() => vi.clearAllMocks())

  it('adapts to ~2h of history → this hour vs the previous hour', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    seedExtents(qc, 'svc-a', 2)
    const { result } = renderHook(() => useInsightsDefaults('svc-a'), {
      wrapper: makeQueryWrapper(qc),
    })
    expect(result.current.windowHours).toBe('1')
    expect(result.current.baselineHours).toBe('1')
  })

  it('falls back to the static default when no service is active', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    const { result } = renderHook(() => useInsightsDefaults(null), {
      wrapper: makeQueryWrapper(qc),
    })
    expect(result.current.windowHours).toBe('1')
    expect(result.current.baselineHours).toBe('168')
  })

  it('keeps an explicit user pick even when extents would suggest otherwise', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    seedExtents(qc, 'svc-a', 2)
    const { result } = renderHook(() => useInsightsDefaults('svc-a'), {
      wrapper: makeQueryWrapper(qc),
    })
    act(() => result.current.setBaselineHours('720'))
    expect(result.current.baselineHours).toBe('720') // sticky
    expect(result.current.windowHours).toBe('1') // untouched window stays adaptive
  })

  it('drops the override when the active service changes', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    seedExtents(qc, 'svc-a', 2) // ~2h
    seedExtents(qc, 'svc-b', 240) // ~10d
    const { result, rerender } = renderHook(({ sid }) => useInsightsDefaults(sid), {
      wrapper: makeQueryWrapper(qc),
      initialProps: { sid: 'svc-a' },
    })
    act(() => result.current.setBaselineHours('720'))
    expect(result.current.baselineHours).toBe('720')

    rerender({ sid: 'svc-b' })
    // svc-b has ~10d history → adaptive 1/168, and svc-a's override is gone.
    expect(result.current.windowHours).toBe('1')
    expect(result.current.baselineHours).toBe('168')
  })
})
