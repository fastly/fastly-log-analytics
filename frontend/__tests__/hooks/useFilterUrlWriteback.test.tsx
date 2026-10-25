/**
 * @vitest-environment jsdom
 *
 * Tests for useFilterUrlWriteback, useFilterPayload, and useTimeRange.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useFilterUrlWriteback } from '@/hooks/useDashboardCards'
import { useFilterPayload } from '@/hooks/useDashboardCards'
import { useTimeRange } from '@/hooks/useDashboardCards'
import { useFilterStore } from '@/stores/filterStore'

let currentPathname = '/dashboard'

vi.mock('next/navigation', () => ({
  usePathname: () => currentPathname,
}))

function resetStore() {
  act(() => {
    useFilterStore.setState({
      filters: [],
      edgeOnly: false,
      startTime: '2026-06-09T20:00:00.000Z',
      endTime: '2026-06-10T20:00:00.000Z',
      isAutoRange: true,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
    })
  })
}

beforeEach(() => {
  currentPathname = '/dashboard'
  window.history.replaceState({}, '', '/dashboard')
  resetStore()
})

describe('useFilterUrlWriteback', () => {
  it('re-hydrates filterStore on client-side pathname change (ingress)', () => {
    const { rerender } = renderHook(() => useFilterUrlWriteback())

    const filters = {
      country: { values: ['CA'], mode: 'include' },
    }
    window.history.replaceState({}, '', `/dashboard?filters=${encodeURIComponent(JSON.stringify(filters))}`)

    currentPathname = '/security'
    act(() => {
      rerender()
    })

    const store = useFilterStore.getState()
    expect(store.filters).toHaveLength(1)
    expect(store.filters[0]).toMatchObject({
      column: 'country',
      value: 'CA',
      mode: 'include',
    })
  })

  it('does not write start_time/end_time on fresh load (isAutoRange=true)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().toggleEdgeOnly()
    })

    const params = new URLSearchParams(window.location.search)
    expect(params.has('start_time')).toBe(false)
    expect(params.has('end_time')).toBe(false)
  })

  it('writes start_time/end_time after user picks a range (isAutoRange=false)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().setRange(
        '2026-06-09T17:36:00.000Z',
        '2026-06-10T17:36:00.000Z',
      )
    })

    const params = new URLSearchParams(window.location.search)
    expect(params.get('start_time')).toBe('2026-06-09T17:36:00.000Z')
    expect(params.get('end_time')).toBe('2026-06-10T17:36:00.000Z')
  })

  it('removes start_time/end_time on Reset (user-picked range → defaults)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().setRange(
        '2026-06-09T17:36:00.000Z',
        '2026-06-10T17:36:00.000Z',
      )
    })
    expect(new URLSearchParams(window.location.search).has('start_time')).toBe(true)

    act(() => {
      useFilterStore.getState().resetAll()
    })
    const params = new URLSearchParams(window.location.search)
    expect(params.has('start_time')).toBe(false)
    expect(params.has('end_time')).toBe(false)
    expect(params.has('filters')).toBe(false)
  })

  it('removes filters from URL when filter list is cleared', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    expect(new URLSearchParams(window.location.search).has('filters')).toBe(true)

    act(() => {
      useFilterStore.getState().resetAll()
    })
    expect(new URLSearchParams(window.location.search).has('filters')).toBe(false)
  })
})

describe('useFilterPayload', () => {
  it('returns an empty object when no filters are set', () => {
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current).toEqual({})
  })

  it('reflects current filter pills as a backend-shaped payload', () => {
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
      useFilterStore.getState().addFilter('status', '500', 'exclude')
    })
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current.country).toEqual({ mode: 'include', values: ['US'] })
    const excludeKey = Object.keys(result.current).find(k => result.current[k].mode === 'exclude')
    expect(excludeKey).toBeTruthy()
    expect(result.current[excludeKey!].values).toContain('500')
  })

  it('does NOT include edge filter by default, even when edgeOnly is on', () => {
    act(() => {
      useFilterStore.setState({ edgeOnly: true })
    })
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current).not.toHaveProperty('edge')
  })

  it('includes edge filter when called with includeEdgeOnly=true AND edgeOnly is on', () => {
    act(() => {
      useFilterStore.setState({ edgeOnly: true })
    })
    const { result } = renderHook(() => useFilterPayload(true))
    expect(result.current).toHaveProperty('edge')
    expect(result.current.edge).toEqual({ mode: 'include', values: ['true'] })
  })

  it('does not include edge filter when includeEdgeOnly=true but edgeOnly is off', () => {
    const { result } = renderHook(() => useFilterPayload(true))
    expect(result.current).not.toHaveProperty('edge')
  })

  it('memoises across renders when filters and edgeOnly are unchanged', () => {
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    const { result, rerender } = renderHook(() => useFilterPayload())
    const first = result.current
    rerender()
    expect(result.current).toBe(first)
  })

  it('returns a new object when filters change', () => {
    const { result } = renderHook(() => useFilterPayload())
    const first = result.current
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    expect(result.current).not.toBe(first)
  })
})

describe('useTimeRange', () => {
  beforeEach(() => {
    act(() => {
      useFilterStore.setState({
        startTime: '2026-07-01T00:00:00Z',
        endTime: '2026-07-02T00:00:00Z',
        compareMode: false,
        compareStartTime: null,
        compareEndTime: null,
      })
    })
  })

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
