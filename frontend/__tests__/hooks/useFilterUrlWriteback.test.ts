/**
 * @vitest-environment jsdom
 *
 * useFilterUrlWriteback — bidirectional sync between the global filterStore and
 * the page URL. Pins the gating on isAutoRange: only persist
 * start_time/end_time in the URL when the user has explicitly chosen a
 * range. On fresh load and after Reset the store sits at its auto-range
 * default; writing those computed defaults to the URL would pollute it
 * with values the user never picked.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useFilterUrlWriteback } from '@/hooks/useFilterUrlWriteback'
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
  // Start every test with a clean URL — no leftover query params from
  // a sibling test's window.history.replaceState writes.
  window.history.replaceState({}, '', '/dashboard')
  resetStore()
})

describe('useFilterUrlWriteback', () => {
  it('re-hydrates filterStore on client-side pathname change (ingress)', () => {
    // 1. Initial page with no filters
    const { rerender } = renderHook(() => useFilterUrlWriteback())

    // 2. Set filters in URL manually (simulate client-side link navigation with query params)
    const filters = {
      country: { values: ['CA'], mode: 'include' },
    }
    window.history.replaceState({}, '', `/dashboard?filters=${encodeURIComponent(JSON.stringify(filters))}`)

    // 3. Change pathname (trigger client-side transition)
    currentPathname = '/security'
    act(() => {
      rerender()
    })

    // 4. Assert that filterStore has been hydrated with the new filters!
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

    // Force a store mutation so the write effect fires post-hydration.
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

    // First: user picks a range — URL gets params.
    act(() => {
      useFilterStore.getState().setRange(
        '2026-06-09T17:36:00.000Z',
        '2026-06-10T17:36:00.000Z',
      )
    })
    expect(new URLSearchParams(window.location.search).has('start_time')).toBe(true)

    // Then: Reset → clearFilters → isAutoRange flips back to true → URL clears.
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
