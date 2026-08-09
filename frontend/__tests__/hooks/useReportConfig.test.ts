/**
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { useReportConfig } from '@/hooks/useReportConfig'

// Mock state we can manipulate
let mockState = {
  startTime: '',
  endTime: '',
}

vi.mock('@/stores/filterStore', () => {
  return {
    useFilterStore: vi.fn((selector) => selector(mockState))
  }
})

describe('useReportConfig', () => {
  beforeEach(() => {
    mockState = { startTime: '', endTime: '' }
  })

  it('provides default configuration', () => {
    const { result } = renderHook(() => useReportConfig())
    expect(result.current.metric).toBe('requests')
    // When dates are empty, span is 0, so 1 minute is invalid and it falls back to 1 second
    expect(result.current.chartInterval).toBe('1 second')
    expect(result.current.trend).toBe('off')
  })

  it('restricts "1 day" interval when range is exactly 12 hours', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 12 * 3600 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 day' }))

    // '1 day' is too large for a 12 hour window, fallback finds '1 minute'
    expect(result.current.config.validIntervals.has('1 day')).toBe(false)
    expect(result.current.chartInterval).toBe('1 minute')
  })

  it('restricts "1 hour" interval when range is exactly 30 minutes', () => {
    const now = new Date()
    const start = new Date(now.getTime() - 30 * 60 * 1000).toISOString()
    const end = now.toISOString()

    mockState = { startTime: start, endTime: end }

    const { result } = renderHook(() => useReportConfig({ defaultInterval: '1 hour' }))

    // 30 min window falls back to '1 minute' because span <= 6 hours
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
    // We should actually manually set it to see if it allows it, because auto-fallback might
    // select something else since manualInterval is null initially.
    act(() => {
      result.current.setChartInterval('1 day')
    })
    expect(result.current.chartInterval).toBe('1 day')
  })

  it('defaults to "1 hour" granularity for a 7-day window', () => {
    // 7d view at 1d granularity = 7 bars, which is unreadable. The
    // perf limit removes 1h only past 30d (spanHours > 720), so 7d
    // through 30d all default to 1h.
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
    // The usage/cost page sets defaultInterval="1 hour" — a 24h window
    // with 1-minute bars is 1440 buckets, which is both visually noisy
    // and expensive to render. The auto-pick must honor the page's
    // intent at the 24h boundary, not fall through to '1 minute'.
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

    // Shrink window to 6 hours
    const start6 = new Date(now.getTime() - 6 * 3600 * 1000).toISOString()
    act(() => {
      mockState = { startTime: start6, endTime: end }
    })
    rerender()

    expect(result.current.config.validIntervals.has('1 day')).toBe(false)
    // 6 hours exactly triggers the fallback to 1 minute
    expect(result.current.chartInterval).toBe('1 minute')
  })
})
