/**
 * @vitest-environment jsdom
 *
 * useFilterPayload — translates the global filter pill list into the
 * { col: { mode, values: [...] } } shape the backend expects. Used by every
 * page that fetches analytical data.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it } from 'vitest'
import { useFilterPayload } from '@/hooks/useFilterPayload'
import { useFilterStore } from '@/stores/filterStore'

beforeEach(() => {
  // Wrap in act() — a prior renderHook's TestComponent may still hold a
  // store subscription; React 19 warns when a subscriber is notified
  // outside act().
  act(() => {
    useFilterStore.setState({
      filters: [],
      edgeOnly: false,
      startTime: '',
      endTime: '',
      isAutoRange: true,
      hasSyncedExtents: false,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
    })
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
    // Exclude filters land on a separate key (or under a suffix); just check it exists
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
