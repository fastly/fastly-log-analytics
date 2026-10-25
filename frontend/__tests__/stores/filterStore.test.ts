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
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useFilterStore } from '@/stores/filterStore'
import { useDebugStore } from '@/stores/filterStore'
import { usePopGeoStore } from '@/stores/filterStore'
import { useServiceStore, type Service } from '@/stores/filterStore'
import { useSessionRoleStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/filterStore'

const _initial = useFilterStore.getState()

beforeEach(() => {
  window.localStorage.clear()
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
  usePopGeoStore.setState({ map: {} })
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
  // Reset to a known baseline. Use a deterministic range so duration math is testable.
  useFilterStore.setState({
    ..._initial,
    startTime: '2026-05-01T00:00:00.000Z',
    endTime: '2026-05-08T00:00:00.000Z',
    filters: [],
    edgeOnly: false,
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

describe('resetRange', () => {
  it('re-enables auto-range', () => {
    useFilterStore.setState({ isAutoRange: false })
    useFilterStore.getState().resetRange()
    const s = useFilterStore.getState()
    expect(s.isAutoRange).toBe(true)
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

describe('addFilter — dedup-suffix guard', () => {
  it('drops column names matching /_\\d+$/ (would corrupt on URL round-trip)', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    useFilterStore.getState().addFilter('response_1', 'foo', 'include')
    useFilterStore.getState().addFilter('field_42', 'bar', 'exclude')
    expect(useFilterStore.getState().filters).toHaveLength(0)
    expect(warn).toHaveBeenCalledTimes(2)
    warn.mockRestore()
  })

  it('allows column names with embedded digits but no trailing _<digit>', () => {
    const { addFilter } = useFilterStore.getState()
    addFilter('status_code', '500', 'include')
    addFilter('http2_pushes', '3', 'include')
    addFilter('response', '200', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(3)
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
    useFilterStore.setState({ isAutoRange: false })

    useFilterStore.getState().resetAll()
    const s = useFilterStore.getState()
    expect(s.filters).toEqual([])
    expect(s.isAutoRange).toBe(true)
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
    // auto-range flipped back on so the snap effect can apply when extents are old.
    expect(after.isAutoRange).toBe(true)
  })
})

describe('debugStore', () => {
  it('setEnabled flips independently of apiCallsEnabled', () => {
    useDebugStore.getState().setEnabled(true)
    expect(useDebugStore.getState().enabled).toBe(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(false)
  })

  it('setApiCallsEnabled flips independently of enabled', () => {
    useDebugStore.getState().setApiCallsEnabled(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    expect(useDebugStore.getState().enabled).toBe(false)
  })

  it('persists to fastly-debug-settings', () => {
    useDebugStore.getState().setEnabled(true)
    useDebugStore.getState().setApiCallsEnabled(true)
    const raw = window.localStorage.getItem('fastly-debug-settings')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.enabled).toBe(true)
    expect(parsed.state.apiCallsEnabled).toBe(true)
  })
})

describe('popGeoStore', () => {
  it('defaults to empty map', () => {
    expect(usePopGeoStore.getState().map).toEqual({})
  })

  it('setMap populates the PoP lookup', () => {
    const geo = {
      SJC: { city: 'San Jose', region: 'California', country: 'US' },
      AMS: { city: 'Amsterdam', region: 'North Holland', country: 'NL' },
    }
    usePopGeoStore.getState().setMap(geo)
    expect(usePopGeoStore.getState().map).toEqual(geo)
    expect(usePopGeoStore.getState().map.SJC.city).toBe('San Jose')
  })

  it('setMap replaces the entire map', () => {
    usePopGeoStore.getState().setMap({ SJC: { city: 'San Jose', region: 'CA', country: 'US' } })
    usePopGeoStore.getState().setMap({ AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' } })
    expect(usePopGeoStore.getState().map).toEqual({
      AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' },
    })
    expect(usePopGeoStore.getState().map.SJC).toBeUndefined()
  })
})

describe('serviceStore', () => {
  it('sets and clears the active id', () => {
    useServiceStore.getState().setActiveServiceId('svc-1')
    expect(useServiceStore.getState().activeServiceId).toBe('svc-1')
    useServiceStore.getState().setActiveServiceId(null)
    expect(useServiceStore.getState().activeServiceId).toBeNull()
  })

  it('replaces the entire services list', () => {
    const a: Service = { id: 'a', name: 'A', accessLevel: 'read_write' }
    const b: Service = { id: 'b', name: 'B', accessLevel: 'read_only' }
    useServiceStore.getState().setServices([a, b])
    expect(useServiceStore.getState().services).toEqual([a, b])

    useServiceStore.getState().setServices([])
    expect(useServiceStore.getState().services).toEqual([])
  })

  it('flips the bootstrap flag', () => {
    expect(useServiceStore.getState().isInitialized).toBe(false)
    useServiceStore.getState().setInitialized(true)
    expect(useServiceStore.getState().isInitialized).toBe(true)
  })

  it('only persists activeServiceId and services (not isInitialized)', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'S' }],
      isInitialized: true,
    })
    const raw = window.localStorage.getItem('service-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.activeServiceId).toBe('svc-1')
    expect(parsed.state.services).toEqual([{ id: 'svc-1', name: 'S' }])
    expect(parsed.state.isInitialized).toBeUndefined()
  })
})

describe('sessionRoleStore', () => {
  it('defaults to admin (isRemoteAnalyst=false)', () => {
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('setIsRemoteAnalyst toggles analyst role', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(true)
    useSessionRoleStore.getState().setIsRemoteAnalyst(false)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('is not persisted to localStorage', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    const keys = Object.keys(window.localStorage)
    const roleKey = keys.find((k) => k.toLowerCase().includes('role') || k.toLowerCase().includes('analyst'))
    expect(roleKey).toBeUndefined()
  })
})

describe('timezoneStore', () => {
  it('setTimezone updates the value and switches mode to manual', () => {
    useTimezoneStore.getState().setTimezone('America/New_York')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
    expect(useTimezoneStore.getState().mode).toBe('manual')
  })

  it('accepts arbitrary strings (the consumer validates)', () => {
    useTimezoneStore.getState().setTimezone('not-a-real-tz')
    expect(useTimezoneStore.getState().timezone).toBe('not-a-real-tz')
  })

  it('setSystemTimezone updates the value and keeps/restores mode as system', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    expect(useTimezoneStore.getState().mode).toBe('manual')

    useTimezoneStore.getState().setSystemTimezone('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
  })

  it('writes through to localStorage with the timezone-storage key', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    const raw = window.localStorage.getItem('timezone-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.timezone).toBe('Europe/London')
    expect(parsed.state.mode).toBe('manual')
  })

  it('stamps mode: manual onto a pre-v1 persisted blob that only had `timezone`', () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/Denver' }, version: 0 }),
    )
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState().timezone).toBe('America/Denver')
    expect(useTimezoneStore.getState().mode).toBe('manual')
  })

  it('leaves an already-migrated v1 blob untouched', () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'Asia/Tokyo', mode: 'system' }, version: 1 }),
    )
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
  })
})
