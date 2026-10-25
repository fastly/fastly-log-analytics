/**
 * @vitest-environment jsdom
 *
 * Tests for useIsAnalyst, useMaskIps, and useEnforceMaskedFilters.
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { useIsAnalyst, useMaskIps, useEnforceMaskedFilters } from '@/hooks/useIsAnalyst'
import { useCardVisibility } from '@/hooks/useDashboardCards'
import { useHighScaleRequestFacts } from '@/components/high-scale/HighScaleRequestFactsPanel'
import { client } from '@/lib/api'
import { postOk } from '@/__tests__/helpers/partial'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'

function makeWrapper(opts: { isRemoteAnalyst?: boolean; maskIps?: boolean } = {}) {
  const qc = createTestQueryClient()
  const settings: Record<string, unknown> = {}
  if (opts.isRemoteAnalyst !== undefined) {
    settings.is_remote_analyst = opts.isRemoteAnalyst
  }
  if (opts.maskIps !== undefined) {
    settings.mask_ips = opts.maskIps
  }
  if (Object.keys(settings).length > 0 || opts.isRemoteAnalyst !== undefined) {
    qc.setQueryData(queryKeys.bootstrap(), { settings })
  }
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
})

describe('useIsAnalyst', () => {
  it('returns false for admin (no analyst signals)', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(false)
  })

  it('returns true when bootstrap.settings.is_remote_analyst is true', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: true }),
    })
    expect(result.current).toBe(true)
  })

  it('returns true when activeService.accessLevel is read_only', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(true)
  })

  it('returns true when both signals are present', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: true }),
    })
    expect(result.current).toBe(true)
  })

  it('returns false when activeServiceId has no matching service', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-missing',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(false)
  })
})

describe('useMaskIps', () => {
  it('is true when bootstrap.settings.mask_ips is true', () => {
    const { result } = renderHook(() => useMaskIps(), { wrapper: makeWrapper({ maskIps: true }) })
    expect(result.current).toBe(true)
  })

  it('is false when mask_ips is false or absent', () => {
    const { result: off } = renderHook(() => useMaskIps(), { wrapper: makeWrapper({ maskIps: false }) })
    expect(off.current).toBe(false)
    const { result: missing } = renderHook(() => useMaskIps(), { wrapper: makeWrapper() })
    expect(missing.current).toBe(false)
  })
})

describe('useEnforceMaskedFilters', () => {
  beforeEach(() => {
    useFilterStore.getState().clearFilters()
  })

  it('strips IP-family filters (incl. dedup-suffixed) when masking', async () => {
    const store = useFilterStore.getState()
    store.addFilter('ip', '73.217.41.5', 'include')
    store.addFilter('country', 'US', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper({ maskIps: true }) })
    await waitFor(() => {
      const cols = useFilterStore.getState().filters.map(f => f.column)
      expect(cols).not.toContain('ip')
      expect(cols).toContain('country')
    })
  })

  it('leaves IP filters in place when NOT masking', async () => {
    useFilterStore.getState().addFilter('ip', '73.217.41.5', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper({ maskIps: false }) })
    await new Promise(r => setTimeout(r, 0))
    expect(useFilterStore.getState().filters.map(f => f.column)).toContain('ip')
  })
})

const STORAGE_KEY = 'test-card-visibility'
const MIGRATION_KEY = `${STORAGE_KEY}_mv`
const ALL_IDS = ['a', 'b', 'c', 'd']

/**
 * Persistence + migration semantics for the localStorage-backed
 * card-visibility hook. Several admin/analyst surfaces (raw-logs columns,
 * dashboard cards) depend on the synchronous initializer correctly
 * hydrating from localStorage so the first paint doesn't flash a wrong
 * set of cards.
 */
describe('useCardVisibility', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('cold mount with no stored value returns the default set (all ids when no default passed)', () => {
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    expect([...result.current.visibleCards].sort()).toEqual([...ALL_IDS].sort())
  })

  it('cold mount with no stored value honours the explicit defaultIds', () => {
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a', 'b']))
    expect([...result.current.visibleCards].sort()).toEqual(['a', 'b'])
  })

  it('cold mount reads the persisted set from localStorage', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['b', 'c']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    expect([...result.current.visibleCards].sort()).toEqual(['b', 'c'])
  })

  it('toggleCard adds a missing id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.toggleCard('b'))
    expect(result.current.visibleCards.has('b')).toBe(true)
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted.sort()).toEqual(['a', 'b'])
  })

  it('toggleCard removes an existing id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.toggleCard('a'))
    expect(result.current.visibleCards.has('a')).toBe(false)
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted).toEqual(['b'])
  })

  it('showAll resets visibility to every id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.showAll())
    expect([...result.current.visibleCards].sort()).toEqual([...ALL_IDS].sort())
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted.sort()).toEqual([...ALL_IDS].sort())
  })

  it('reset reverts to the default set and persists it', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b', 'c', 'd']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a']))
    act(() => result.current.reset())
    expect([...result.current.visibleCards]).toEqual(['a'])
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted).toEqual(['a'])
  })

  it('migration strips removeIds when storedVersion < migration.version', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b', 'c']))
    localStorage.setItem(MIGRATION_KEY, '0')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 2,
        removeIds: ['b'],
      }),
    )
    // The initial render returns the raw persisted set; the post-mount
    // useEffect re-runs load() and applies the migration. Reading the
    // hook value here reflects the post-effect state.
    expect(result.current.visibleCards.has('b')).toBe(false)
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('2')
  })

  it('migration adds addIds when storedVersion < migration.version', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    localStorage.setItem(MIGRATION_KEY, '0')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 3,
        addIds: ['d'],
      }),
    )
    expect(result.current.visibleCards.has('d')).toBe(true)
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('3')
  })

  it('migration is a no-op when storedVersion already at target', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b']))
    localStorage.setItem(MIGRATION_KEY, '5')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 5,
        removeIds: ['a'],
      }),
    )
    // 'a' should still be present — migration version matches, no prune.
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(result.current.visibleCards.has('b')).toBe(true)
  })

  it('migration applies both removeIds and addIds in one pass and re-runs on version bump', () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id']),
    )
    renderHook(() =>
      useCardVisibility(
        STORAGE_KEY,
        ['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'],
        undefined,
        {
          version: 2,
          removeIds: ['bw', 'rid', 'prid', 'waf_sig', 'waf_req_id'],
          addIds: ['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'],
        },
      ),
    )
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY)!).sort()
    expect(saved).toEqual(['_bot_name', '_ngwaf_bot_name', 'method', 'status', 'waf_sig_ind'])
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('2')
  })

  it('does not run migration when localStorage has no saved set yet (fresh browser)', () => {
    renderHook(() =>
      useCardVisibility(STORAGE_KEY, ['a', 'b'], ['a'], {
        version: 1,
        addIds: ['b'],
      }),
    )
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull()
    expect(localStorage.getItem(MIGRATION_KEY)).toBeNull()
  })
})

const hsMetadata = {
  status: 'complete' as const,
  exact: true,
  coverage: 1,
  freshness_lag_seconds: 0,
  watermark: {
    service_id: 'service-1',
    domain: 'request',
    owner_epoch: 1,
    coverage_start: null,
    coverage_end: null,
    last_accepted_cursor: null,
    last_archived_event_id: null,
    last_visible_event_id: null,
    exact: true,
  },
  approximation_error: null,
  error: null,
}

const hsPageOne = {
  rows: [{ event_id: 'event-1', timestamp: '2026-09-11T00:00:01Z' }],
  metadata: hsMetadata,
  next_cursor: 'cursor-1',
  _is_cached: false,
}

const hsPageTwo = {
  rows: [{ event_id: 'event-2', timestamp: '2026-09-11T00:00:02Z' }],
  metadata: hsMetadata,
  next_cursor: null,
  _is_cached: false,
}

describe('useHighScaleRequestFacts', () => {
  beforeEach(() => {
    vi.spyOn(client, 'POST').mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('advances and rewinds with server-issued cursors', async () => {
    vi.mocked(client.POST)
      .mockResolvedValueOnce(postOk(hsPageOne))
      .mockResolvedValueOnce(postOk(hsPageTwo))

    const { result } = renderHook(
      () => useHighScaleRequestFacts({
        serviceId: 'service-1',
        startTime: '2026-09-11T00:00:00Z',
        endTime: '2026-09-11T01:00:00Z',
        pageSize: 1,
      }),
      { wrapper: makeQueryWrapper(createTestQueryClient()) },
    )

    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-1'))
    expect(result.current.pageIndex).toBe(0)
    expect(result.current.canNextPage).toBe(true)

    await act(async () => {
      result.current.nextPage()
    })
    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-2'))
    expect(result.current.pageIndex).toBe(1)
    expect(result.current.canPreviousPage).toBe(true)

    const calls = vi.mocked(client.POST).mock.calls
    expect((calls[0]?.[1] as { body: { cursor: string | null } }).body.cursor).toBeNull()
    expect((calls[1]?.[1] as { body: { cursor: string | null } }).body.cursor).toBe('cursor-1')

    await act(async () => {
      result.current.previousPage()
    })
    expect(result.current.pageIndex).toBe(0)
  })
})
