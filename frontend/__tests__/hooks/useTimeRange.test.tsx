/**
 * @vitest-environment jsdom
 *
 * useTimeRange — shallow projection of the time-window fields from filterStore.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it } from 'vitest'

import { useTimeRange } from '@/hooks/useTimeRange'
import { useFilterStore } from '@/stores/filterStore'

beforeEach(() => {
  useFilterStore.setState({
    startTime: '2026-07-01T00:00:00Z',
    endTime: '2026-07-02T00:00:00Z',
    compareMode: false,
    compareStartTime: null,
    compareEndTime: null,
  })
})

describe('useTimeRange', () => {
  it('returns the current time window from filterStore', () => {
    const { result } = renderHook(() => useTimeRange())
    expect(result.current.startTime).toBe('2026-07-01T00:00:00Z')
    expect(result.current.endTime).toBe('2026-07-02T00:00:00Z')
    expect(result.current.compareMode).toBe(false)
    expect(result.current.compareStartTime).toBeNull()
    expect(result.current.compareEndTime).toBeNull()
  })

  it('reflects filterStore changes', () => {
    const { result } = renderHook(() => useTimeRange())

    act(() => {
      useFilterStore.setState({
        startTime: '2026-06-01T00:00:00Z',
        endTime: '2026-06-15T00:00:00Z',
        compareMode: true,
        compareStartTime: '2026-05-01T00:00:00Z',
        compareEndTime: '2026-05-15T00:00:00Z',
      })
    })

    expect(result.current.startTime).toBe('2026-06-01T00:00:00Z')
    expect(result.current.compareMode).toBe(true)
    expect(result.current.compareStartTime).toBe('2026-05-01T00:00:00Z')
  })
})
