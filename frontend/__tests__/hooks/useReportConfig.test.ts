/**
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { useReportConfig } from '@/hooks/useReportConfig'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { useInsightsDefaults } from '@/hooks/useInsightsDefaults'

let mockState = {
  startTime: '',
  endTime: '',
}

vi.mock('@/stores/filterStore', () => {
  return {
    useFilterStore: vi.fn((selector) => selector(mockState))
  }
})

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(async () => ({ data: undefined })) },
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
