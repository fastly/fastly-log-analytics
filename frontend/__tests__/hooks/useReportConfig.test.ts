/**
 * @vitest-environment jsdom
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { hashKey, QueryClient } from '@tanstack/react-query'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { useReportConfig, useInsightsDefaults } from '@/hooks/useDashboardCards'
import { useAnalyticsReportQuery } from '@/hooks/useDashboardCards'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'

let mockState = {
  startTime: '',
  endTime: '',
}

vi.mock('@/stores/filterStore', () => {
  return {
    useFilterStore: vi.fn((selector) => selector(mockState))
  }
})

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: { GET: vi.fn(async () => ({ data: undefined })) },
}))

// useServiceQuery gates `enabled` on useIsDataReady — force it ready so the
// queryFn actually runs in useAnalyticsReportQuery tests.
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsDataReady: () => true,
}))

describe('useReportConfig', () => {
  beforeEach(() => {
    mockState = { startTime: '', endTime: '' }
  })

  it('provides default configuration', () => {
    const { result } = renderHook(() => useReportConfig())
    expect(result.current.metric).toBe('requests')
    expect(result.current.chartInterval).toBe('1 second')
    expect(result.current.trend).toBe('off')
  })

  it('restricts "1 day" interval when range is exactly 12 hours', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 12 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 day' }))

    expect(result.current.config.validIntervals.has('1 day')).toBe(false)
    expect(result.current.chartInterval).toBe('1 minute')
  })

  it('restricts "1 hour" interval when range is exactly 30 minutes', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 30 * 60 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 hour' }))

    expect(result.current.config.validIntervals.has('1 hour')).toBe(false)
    expect(result.current.chartInterval).toBe('1 minute')
  })

  it('allows "1 day" when range is greater than 24 hours', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 48 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 day' }))

    expect(result.current.config.validIntervals.has('1 day')).toBe(true)
    act(() => {
      result.current.setChartInterval('1 day')
    })
    expect(result.current.chartInterval).toBe('1 day')
  })

  it('defaults to "1 hour" granularity for a 7-day window', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 7 * 24 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig())

    expect(result.current.config.validIntervals.has('1 hour')).toBe(true)
    expect(result.current.chartInterval).toBe('1 hour')
  })

  it('defaults to "1 day" granularity for a 30-day window', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 30 * 24 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig())

    expect(result.current.chartInterval).toBe('1 day')
  })

  it('defaults to "1 hour" granularity for an exactly-24h window', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 24 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 hour' }))

    expect(result.current.config.validIntervals.has('1 hour')).toBe(true)
    expect(result.current.chartInterval).toBe('1 hour')
  })

  it('gracefully falls back when user shrinks timeline while an invalid interval is selected', () => {
    const now = new Date()
    const start48 = new Date(now.getTime() - 48 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start48, endTime: end }

    const { result, rerender } = renderHook(() => useReportConfig({ defaultInterval: '1 day' }))

    act(() => {
      result.current.setChartInterval('1 day')
    })

    expect(result.current.chartInterval).toBe('1 day')

    const start6 = new Date(now.getTime() - 6 * 3600 * 1000).toISOString()
    act(() => {
      mockState = { startTime: start6, endTime: end }
    })
    rerender()

    expect(result.current.config.validIntervals.has('1 day')).toBe(false)
    expect(result.current.chartInterval).toBe('1 minute')
  })
})

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
    expect(result.current.windowHours).toBe('24')
    expect(result.current.baselineHours).toBe('168')
  })

  it('keeps an explicit user pick even when extents would suggest otherwise', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    seedExtents(qc, 'svc-a', 2)
    const { result } = renderHook(() => useInsightsDefaults('svc-a'), {
      wrapper: makeQueryWrapper(qc),
    })
    act(() => result.current.setBaselineHours('720'))
    expect(result.current.baselineHours).toBe('720')
    expect(result.current.windowHours).toBe('1')
  })

  it('drops the override when the active service changes', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    seedExtents(qc, 'svc-a', 2)
    seedExtents(qc, 'svc-b', 240)
    const { result, rerender } = renderHook(({ sid }) => useInsightsDefaults(sid), {
      wrapper: makeQueryWrapper(qc),
      initialProps: { sid: 'svc-a' },
    })
    act(() => result.current.setBaselineHours('720'))
    expect(result.current.baselineHours).toBe('720')

    rerender({ sid: 'svc-b' })
    expect(result.current.windowHours).toBe('24')
    expect(result.current.baselineHours).toBe('168')
  })
})

function makeClient() {
  return createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
}

function soleKey(qc: QueryClient): unknown[] {
  const entries = qc.getQueryCache().getAll()
  expect(entries).toHaveLength(1)
  return entries[0].queryKey as unknown[]
}

describe('useAnalyticsReportQuery', () => {
  it('builds [...namespace, serviceId, rangeKey, anchor, filterPayload, ...keyParts] and byte-matches the SSR seed key in token mode', async () => {
    const captured: Array<Record<string, unknown>> = []
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['origin', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: true,
            startTime: '2026-06-29T11:00:00Z',
            endTime: '2026-06-29T12:00:00Z',
            anchor: '2026-06-29T12:00:00Z',
          },
          keyParts: [60, 'ttfb', 'p95'],
          queryFn: async ({ rangeBody, rangeToken }) => {
            captured.push({ rangeBody, rangeToken })
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))

    // Cold-load / auto default resolves the server-reproducible '24h' token.
    expect(captured[0].rangeToken).toBe('24h')
    expect(captured[0].rangeBody).toMatchObject({
      range_token: '24h',
      anchor: '2026-06-29T12:00:00Z',
    })

    // The actual key written to the cache.
    const actualKey = soleKey(qc)
    expect(actualKey).toEqual([
      'origin',
      'aggregates',
      'svc-1',
      '24h', // rangeKey === rangeToken in token mode
      '2026-06-29T12:00:00Z',
      {},
      60,
      'ttfb',
      'p95',
    ])

    // hashKey-equal to the SSR seed key (mirrors lib/ssr/origin.ts +
    // app/origin/page.tsx), so cold-load hydration still hits.
    const ssrSeedKey = [
      'origin',
      'aggregates',
      'svc-1',
      '24h',
      '2026-06-29T12:00:00Z',
      {},
      60,
      'ttfb',
      'p95',
    ]
    expect(hashKey(actualKey)).toBe(hashKey(ssrSeedKey))
  })

  it('passes absolute-mode rangeBody (start_time/end_time, no range_token) and keys on abs:… when a custom range is set', async () => {
    const captured: Array<Record<string, unknown>> = []
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['performance', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: false,
            startTime: '2026-06-01T00:00:00Z',
            endTime: '2026-06-02T00:00:00Z',
            anchor: '2026-06-02T00:00:00Z',
          },
          keyParts: ['p99'],
          queryFn: async ({ rangeBody, rangeToken }) => {
            captured.push({ rangeBody, rangeToken })
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))
    expect(captured[0].rangeToken).toBeNull()
    expect(captured[0].rangeBody).toEqual({
      start_time: '2026-06-01T00:00:00Z',
      end_time: '2026-06-02T00:00:00Z',
    })

    const actualKey = soleKey(qc)
    expect(actualKey).toEqual([
      'performance',
      'aggregates',
      'svc-1',
      'abs:2026-06-01T00:00:00Z|2026-06-02T00:00:00Z',
      '2026-06-02T00:00:00Z',
      {},
      'p99',
    ])
  })

  it('threads the AbortSignal from React Query into the fetcher', async () => {
    let seenSignal: AbortSignal | undefined
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['security', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: true,
            startTime: '2026-06-29T11:00:00Z',
            endTime: '2026-06-29T12:00:00Z',
            anchor: '2026-06-29T12:00:00Z',
          },
          keyParts: [3600],
          queryFn: async ({ signal }) => {
            seenSignal = signal
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))
    expect(seenSignal).toBeInstanceOf(AbortSignal)
  })
})
