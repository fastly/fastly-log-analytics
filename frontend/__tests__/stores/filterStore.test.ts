/**
 * @vitest-environment jsdom
 *
 * Reducer-style tests for filterStore — the state engine that drives every
 * page's date range, filter pills, and compare-mode toggle.
 *
 * The store is global (zustand `create` returns a singleton hook). Each test
 * snapshots the initial state on import, mutates the store, then resets via
 * setState() so subsequent tests aren't affected.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useFilterStore } from '@/stores/filterStore'

const _initial = useFilterStore.getState()

beforeEach(() => {
  // Reset to a known baseline. Use a deterministic range so duration math is testable.
  useFilterStore.setState({
    ..._initial,
    startTime: '2026-05-01T00:00:00.000Z',
    endTime: '2026-05-08T00:00:00.000Z',
    filters: [],
    edgeOnly: false,
    hasSyncedExtents: false,
    isAutoRange: true,
    compareMode: false,
    compareStartTime: null,
    compareEndTime: null,
  })
})

describe('setRange', () => {
  it('sets start/end and disables auto-range', () => {
    useFilterStore.getState().setRange('2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z')
    const s = useFilterStore.getState()
    expect(s.startTime).toBe('2026-06-01T00:00:00Z')
    expect(s.endTime).toBe('2026-06-02T00:00:00Z')
    expect(s.isAutoRange).toBe(false)
  })
})

describe('autoSetRange', () => {
  it('updates range only when isAutoRange is true', () => {
    // Default (isAutoRange = true)
    useFilterStore.getState().autoSetRange('2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z')
    expect(useFilterStore.getState().startTime).toBe('2026-06-01T00:00:00Z')
    // After autoSetRange, isAutoRange flips to false (so it doesn't reapply on every datum)
    expect(useFilterStore.getState().isAutoRange).toBe(false)

    // Second autoSetRange should be a no-op
    useFilterStore.getState().autoSetRange('2099-01-01T00:00:00Z', '2099-01-02T00:00:00Z')
    expect(useFilterStore.getState().startTime).toBe('2026-06-01T00:00:00Z')
  })
})

describe('resetRange', () => {
  it('re-enables auto-range and clears the synced flag', () => {
    useFilterStore.setState({ isAutoRange: false, hasSyncedExtents: true })
    useFilterStore.getState().resetRange()
    const s = useFilterStore.getState()
    expect(s.isAutoRange).toBe(true)
    expect(s.hasSyncedExtents).toBe(false)
  })
})

describe('addFilter / removeFilter', () => {
  it('adds an include filter with a generated id', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    const filters = useFilterStore.getState().filters
    expect(filters).toHaveLength(1)
    expect(filters[0]).toMatchObject({ column: 'country', value: 'US', mode: 'include' })
    expect(filters[0].id).toBeTruthy()
  })

  it('does not duplicate an exact (column, value) match', () => {
    const { addFilter } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    addFilter('country', 'US', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(1)
  })

  it('allows the same column with a different value', () => {
    const { addFilter } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    addFilter('country', 'CA', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(2)
  })

  it('removeFilter removes by id', () => {
    useFilterStore.getState().addFilter('status', '500', 'include')
    const id = useFilterStore.getState().filters[0].id
    useFilterStore.getState().removeFilter(id)
    expect(useFilterStore.getState().filters).toHaveLength(0)
  })

  it('removeFilter is a no-op for unknown id', () => {
    useFilterStore.getState().addFilter('status', '500', 'include')
    useFilterStore.getState().removeFilter('nonexistent-id')
    expect(useFilterStore.getState().filters).toHaveLength(1)
  })
})

describe('toggleFilterMode', () => {
  it('flips include → exclude and back', () => {
    useFilterStore.getState().addFilter('status', '500', 'include')
    const id = useFilterStore.getState().filters[0].id

    useFilterStore.getState().toggleFilterMode(id)
    expect(useFilterStore.getState().filters[0].mode).toBe('exclude')

    useFilterStore.getState().toggleFilterMode(id)
    expect(useFilterStore.getState().filters[0].mode).toBe('include')
  })

  it('only touches the matching id', () => {
    const { addFilter } = useFilterStore.getState()
    addFilter('a', '1', 'include')
    addFilter('b', '2', 'include')
    const [first, second] = useFilterStore.getState().filters
    useFilterStore.getState().toggleFilterMode(first.id)
    const after = useFilterStore.getState().filters
    expect(after.find(f => f.id === first.id)?.mode).toBe('exclude')
    expect(after.find(f => f.id === second.id)?.mode).toBe('include')
  })
})

describe('toggleEdgeOnly', () => {
  it('flips the boolean', () => {
    expect(useFilterStore.getState().edgeOnly).toBe(false)
    useFilterStore.getState().toggleEdgeOnly()
    expect(useFilterStore.getState().edgeOnly).toBe(true)
    useFilterStore.getState().toggleEdgeOnly()
    expect(useFilterStore.getState().edgeOnly).toBe(false)
  })
})

describe('toggleCompareMode', () => {
  it('on enable, derives a compare window equal to the current window placed immediately prior', () => {
    // Current window: 2026-05-01 → 2026-05-08 (7 days)
    useFilterStore.getState().toggleCompareMode()
    const s = useFilterStore.getState()
    expect(s.compareMode).toBe(true)
    expect(s.compareStartTime).toBe('2026-04-24T00:00:00.000Z')
    expect(s.compareEndTime).toBe('2026-05-01T00:00:00.000Z')
  })

  it('snaps the current window to whole minutes when enabling compare mode', () => {
    useFilterStore.setState({
      startTime: '2026-05-01T00:00:30.500Z',
      endTime: '2026-05-08T12:34:56.789Z',
    })
    useFilterStore.getState().toggleCompareMode()
    const s = useFilterStore.getState()
    expect(s.startTime).toBe('2026-05-01T00:00:00.000Z')
    expect(s.endTime).toBe('2026-05-08T12:34:00.000Z')
  })

  it('on disable, only flips compareMode (does not clear comparison range)', () => {
    useFilterStore.getState().toggleCompareMode()
    const before = useFilterStore.getState().compareStartTime
    useFilterStore.getState().toggleCompareMode()
    const s = useFilterStore.getState()
    expect(s.compareMode).toBe(false)
    expect(s.compareStartTime).toBe(before)
  })
})

describe('clearFilters', () => {
  it('only clears filters', () => {
    const { addFilter } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    useFilterStore.getState().clearFilters()
    expect(useFilterStore.getState().filters).toEqual([])
  })
})

describe('resetAll', () => {
  it('wipes filters, re-enables auto-range, clears compare state', () => {
    const { addFilter, toggleCompareMode } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    toggleCompareMode()
    useFilterStore.setState({ isAutoRange: false, hasSyncedExtents: true })

    useFilterStore.getState().resetAll()
    const s = useFilterStore.getState()
    expect(s.filters).toEqual([])
    expect(s.isAutoRange).toBe(true)
    expect(s.hasSyncedExtents).toBe(false)
    expect(s.compareMode).toBe(false)
    expect(s.compareStartTime).toBeNull()
    expect(s.compareEndTime).toBeNull()
  })

  it('restores startTime/endTime to last-24h-from-now defaults (Reset regression)', () => {
    // Regression for: prod Reset was a no-op for the time range whenever
    // data was fresh, because resetAll only flipped flags and the
    // FilterBar snap effect took its "keep current range" branch
    // (ageMinutes < 15). resetAll now restores the same defaults the
    // store initializes with, so Reset always returns to "last 24h from
    // now" regardless of data freshness.
    useFilterStore.getState().setRange('2026-05-01T18:00:00.000Z', '2026-05-02T00:00:00.000Z')
    expect(useFilterStore.getState().isAutoRange).toBe(false)
    const before = useFilterStore.getState()
    const spanBefore = new Date(before.endTime).getTime() - new Date(before.startTime).getTime()
    expect(spanBefore).toBeCloseTo(6 * 3600 * 1000, -2) // 6 hours +/- small

    const nowMs = Date.now()
    useFilterStore.getState().resetAll()
    const after = useFilterStore.getState()
    const startMs = new Date(after.startTime).getTime()
    const endMs = new Date(after.endTime).getTime()

    // endTime ~= now (within 1s)
    expect(Math.abs(endMs - nowMs)).toBeLessThan(1000)
    // span ~= 24h (within 1s)
    expect(Math.abs((endMs - startMs) - 24 * 3600 * 1000)).toBeLessThan(1000)
    // auto-range flipped back on so the snap effect can apply the stale-
    // data branch when extents are old.
    expect(after.isAutoRange).toBe(true)
    expect(after.hasSyncedExtents).toBe(false)
  })
})
