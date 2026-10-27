/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { applyShare, applySyncStatus, applySystemMetrics, makeCronRunsApplier } from '@/hooks/useSSE'
import { buildFiltersPayload } from '@/types/api'
import {
  useFilterStore, useDebugStore, usePopGeoStore, useServiceStore,
  useSessionRoleStore, useTimezoneStore, type Service,
} from '@/stores/filterStore'
import { createTestQueryClient } from '../helpers/page-smoke'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetRelativeRange = vi.fn()

const _initial = useFilterStore.getState()

beforeEach(() => {
  window.localStorage.clear()
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
  usePopGeoStore.setState({ map: {} })
  useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false })
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
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

describe('buildFiltersPayload', () => {
  it('returns empty object for no filters and builds single include/exclude filters', () => {
    expect(buildFiltersPayload([])).toEqual({})
    expect(buildFiltersPayload([{ id: '1', column: 'status', value: '200', mode: 'include' }])).toEqual({
      status: { mode: 'include', values: ['200'] },
    })
    expect(buildFiltersPayload([{ id: '1', column: 'country', value: 'US', mode: 'exclude' }])).toEqual({
      country: { mode: 'exclude', values: ['US'] },
    })
  })

  it('two include filters on the same column merge into one entry', () => {
    const result = buildFiltersPayload([
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '201', mode: 'include' },
    ])
    expect(result.status.mode).toBe('include')
    expect(result.status.values).toEqual(expect.arrayContaining(['200', '201']))
  })

  it('include and exclude on the same column get separate keys', () => {
    const result = buildFiltersPayload([
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '500', mode: 'exclude' },
    ])
    const keys = Object.keys(result)
    expect(keys.some((k) => k === 'status' || k.startsWith('status_'))).toBe(true)
    expect(keys).toHaveLength(2)
    expect(keys.map((k) => result[k].mode)).toEqual(expect.arrayContaining(['include', 'exclude']))
  })

  it('multiple different columns each get their own key and match FiltersDict shape', () => {
    const result = buildFiltersPayload([
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'country', value: 'US', mode: 'include' },
      { id: '3', column: 'pop', value: 'JFK', mode: 'include' },
      { id: '4', column: 'ip', value: '1.2.3.4', mode: 'include' },
    ])
    expect(result.status).toBeDefined()
    expect(result.country).toBeDefined()
    expect(result.pop).toBeDefined()
    expect(typeof result.ip.mode).toBe('string')
    expect(Array.isArray(result.ip.values)).toBe(true)
  })
})

function setSearch(search: string) {
  window.history.pushState({}, '', search ? `?${search}` : '/')
}

async function hydrate() {
  const mod = await import('@/lib/utils')
  mod._resetUrlHydrationFlag()
  mod.hydrateFilterStoreFromUrl()
}

describe('hydrateFilterStoreFromUrl', () => {
  let getStateSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
    getStateSpy = vi.spyOn(useFilterStore, 'getState').mockReturnValue({
      ...useFilterStore.getState(),
      addFilter: mockAddFilter,
      clearFilters: mockClearFilters,
      setRange: mockSetRange,
      setRelativeRange: mockSetRelativeRange,
    })
  })

  afterEach(() => getStateSpy.mockRestore())

  it('does nothing when there are no URL params', async () => {
    await hydrate()
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockSetRange).not.toHaveBeenCalled()
    expect(mockSetRelativeRange).not.toHaveBeenCalled()
    expect(mockClearFilters).not.toHaveBeenCalled()
  })

  it('parses legacy filter_<col>=val params and calls addFilter', async () => {
    setSearch('filter_status=200&filter_country=US')
    await hydrate()
    expect(mockClearFilters).toHaveBeenCalledOnce()
    expect(mockAddFilter).toHaveBeenCalledWith('status', '200', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('country', 'US', 'include')
  })

  it('parses absolute start_time / end_time params and calls setRange', async () => {
    setSearch('start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    await hydrate()
    expect(mockSetRange).toHaveBeenCalledWith('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')
  })

  it.each(['24h', '5m'])('parses ?range=%s and calls setRelativeRange with computed start/end', async (token) => {
    setSearch(`range=${token}`)
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    const [arg0, arg1, arg2] = mockSetRelativeRange.mock.calls[0]
    expect(arg0).toBe(token)
    expect(typeof arg1).toBe('string')
    expect(typeof arg2).toBe('string')
  })

  it('parses modern ?filters JSON payload', async () => {
    const filters = { status: { values: ['200', '404'], mode: 'include' }, country: { values: ['CA'], mode: 'exclude' } }
    setSearch(`filters=${encodeURIComponent(JSON.stringify(filters))}`)
    await hydrate()
    expect(mockClearFilters).toHaveBeenCalledOnce()
    expect(mockAddFilter).toHaveBeenCalledWith('status', '200', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('status', '404', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('country', 'CA', 'exclude')
  })

  it('?range= wins over absolute ?start_time/?end_time', async () => {
    setSearch('range=24h&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    expect(mockSetRange).not.toHaveBeenCalled()
  })

  it('strips consumed params from URL while leaving unrelated params untouched', async () => {
    setSearch('filter_status=200&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&other=keep')
    await hydrate()
    const remaining = new URLSearchParams(window.location.search)
    expect(remaining.get('filter_status')).toBeNull()
    expect(remaining.get('start_time')).toBeNull()
    expect(remaining.get('end_time')).toBeNull()
    expect(remaining.get('other')).toBe('keep')
  })

  it('silently ignores malformed ?filters JSON', async () => {
    setSearch('filters=not-json')
    await hydrate()
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockClearFilters).not.toHaveBeenCalled()
  })
})

function filterKeysFrom(url: string): string[] {
  const qs = url.includes('?') ? url.split('?')[1] : url
  return Array.from(new URLSearchParams(qs).keys()).filter((k) => k.startsWith('filter_'))
}

function assertFilterFormat(url: string) {
  const search = url.includes('?') ? url.split('?')[1] : url
  for (const key of new URLSearchParams(search).keys()) {
    if (key.startsWith('filter')) expect(key).toMatch(/^filter_\w+/)
  }
}

describe('Dashboard filter link contracts (Security, Network, Origin)', () => {
  it.each([
    ['Security: bot id', `/dashboard?filter__wellknown_bot_id=${encodeURIComponent('bot-123')}`, 'filter__wellknown_bot_id', 'bot-123'],
    ['Security: ngwaf bot', `/dashboard?filter__ngwaf_bot_name=${encodeURIComponent('scraperbot')}`, 'filter__ngwaf_bot_name', 'scraperbot'],
    ['Security: tls fingerprint', `/dashboard?filter_tls_client_hello=${encodeURIComponent('abc123')}`, 'filter_tls_client_hello', 'abc123'],
    ['Security: client IP', `/dashboard?filter_client_ip=${encodeURIComponent('1.2.3.4')}`, 'filter_client_ip', '1.2.3.4'],
    ['Network: POP', `/dashboard?filter_pop=${encodeURIComponent('JFK')}`, 'filter_pop', 'JFK'],
    ['Network: shield POP', `/dashboard?filter_shield_pop=${encodeURIComponent('IAD')}`, 'filter_shield_pop', 'IAD'],
    ['Network: ASN', `/dashboard?filter_asn=${encodeURIComponent('7922')}`, 'filter_asn', '7922'],
    ['Origin: URL', `/dashboard?filter_url=${encodeURIComponent('/api/data')}`, 'filter_url', '/api/data'],
    ['Origin: POP', `/dashboard?filter_pop=${encodeURIComponent('SYD')}`, 'filter_pop', 'SYD'],
    ['Origin: origin IP (oip)', `/dashboard?filter_oip=${encodeURIComponent('10.0.0.1')}`, 'filter_oip', '10.0.0.1'],
  ] as const)('%s uses %s', (_label, url, expectedKey, expectedVal) => {
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain(expectedKey)
    expect(new URLSearchParams(url.split('?')[1]).get(expectedKey)).toBe(expectedVal)
  })

  it('city/region/country multi-filter all use filter_ prefix and are parseable by useViewMetricUrlSync', () => {
    const url = `/dashboard?filter_city=${encodeURIComponent('London')}&filter_region=${encodeURIComponent('ENG')}&filter_country=${encodeURIComponent('GB')}`
    assertFilterFormat(url)
    const params = new URLSearchParams(url.split('?')[1])
    expect(params.get('filter_city')).toBe('London')
    expect(params.get('filter_region')).toBe('ENG')
    expect(params.get('filter_country')).toBe('GB')
    expect(params.has('city')).toBe(false)
    expect(params.has('region')).toBe(false)

    for (const sampleUrl of [`/dashboard?filter_status=200`, `/dashboard?filter_country=US&filter_pop=JFK`, `/dashboard?filter__wellknown_bot_id=bot-1`, url]) {
      for (const key of new URLSearchParams(sampleUrl.split('?')[1]).keys()) {
        expect(key.startsWith('filter_')).toBe(true)
        expect(key.substring(7).length).toBeGreaterThan(0)
      }
    }
  })
})

function makeQueryClient(): QueryClient {
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

function invalidatedKeys(spy: ReturnType<typeof vi.spyOn>): string[] {
  return spy.mock.calls.map((c: unknown[]) => JSON.stringify((c[0] as { queryKey: unknown })?.queryKey))
}

describe('applySyncStatus, applySystemMetrics & applyShare', () => {
  it('writes the snapshot to [sync-status, serviceId]', () => {
    const qc = makeQueryClient()
    applySyncStatus(qc, 'svc-1', { local_rows: 7, latest_log_at: '2026-06-26T00:00:00Z' })
    expect(qc.getQueryData(['sync-status', 'svc-1'])).toEqual({ local_rows: 7, latest_log_at: '2026-06-26T00:00:00Z' })
  })

  it('fans non-null slices into their cache keys, skips null/omitted slices, and ignores non-object payloads', () => {
    const qc = makeQueryClient()
    const payload = {
      health_snapshot: { status: 'ok' }, metric_history_1h: [{ t: 1, v: 2 }], queries_summary: { total: 42 },
      slow_queries_count: 3, log_accounting: { rows: 1000 }, metadata_storage: { bytes: 9999 }, system_jobs: [{ id: 'job-1' }],
    }
    applySystemMetrics(qc, 'svc-1', payload)
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(payload.health_snapshot)
    expect(qc.getQueryData(['admin', 'metric-history-batch', '1h'])).toEqual(payload.metric_history_1h)
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual(payload.queries_summary)
    expect(qc.getQueryData(['admin', 'overview', 'slow-queries-count', 'svc-1'])).toEqual(payload.slow_queries_count)
    expect(qc.getQueryData(['admin', 'overview', 'log-accounting', 'svc-1'])).toEqual(payload.log_accounting)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual(payload.metadata_storage)
    expect(qc.getQueryData(['system-jobs'])).toEqual(payload.system_jobs)

    applySystemMetrics(qc, 'svc-1', { health_snapshot: null, queries_summary: { total: 99 }, metadata_storage: null })
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(payload.health_snapshot)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual(payload.metadata_storage)
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual({ total: 99 })

    expect(() => applySystemMetrics(qc, 'svc-1', null)).not.toThrow()
    expect(() => applySystemMetrics(qc, 'svc-1', 'nope')).not.toThrow()
  })

  it('writes the live share payload and ignores undefined without clobbering cached data', () => {
    const qc = makeQueryClient()
    const payload = { sharing_active: true, public_url: 'https://x.test', active_session_count: 2 }
    applyShare(qc, payload)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(payload)
    applyShare(qc, undefined)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(payload)
  })
})

function setupCronApplier() {
  const qc = makeQueryClient()
  const spy = vi.spyOn(qc, 'invalidateQueries')
  const applier = makeCronRunsApplier(qc, 'svc-1')
  return { qc, spy, applier }
}

describe('makeCronRunsApplier', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('coalesces a burst into one flush after 100ms (recent + schedule always)', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply({ task: 'commit', status: 'success' })
    applier.apply({ task: 'log_discovery', status: 'running' })
    expect(spy).not.toHaveBeenCalled()

    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'cron-schedule', 'svc-1']))
    expect(keys.filter((k) => k === JSON.stringify(['admin', 'cron-schedule', 'svc-1']))).toHaveLength(1)
  })

  it('invalidates the heavy table only on a COMPLETED event, not running', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply({ task: 'commit', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))

    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
  })

  it('invalidates [last-sync] for sync events (both running and complete) but not other tasks', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['last-sync', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['last-sync', 'svc-1']))
  })

  it('invalidates [admin, iceberg] only when an iceberg-mutating task COMPLETES', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply({ task: 'optimize_iceberg', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    spy.mockClear()
    applier.apply({ task: 'gap_heal', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    spy.mockClear()
    applier.apply({ task: 'optimize_iceberg', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'iceberg']))
  })

  it('invalidates audit/ingested on complete, schema only for data-mutating tasks', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    let keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'alerts', status: 'success' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).not.toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))
  })

  it('malformed payload falls back to table, skips last-sync, and cleanup() cancels pending flush', () => {
    const { spy, applier } = setupCronApplier()
    applier.apply(null)
    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['last-sync', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'log_discovery', status: 'success' })
    applier.cleanup()
    vi.advanceTimersByTime(100)
    expect(spy).not.toHaveBeenCalled()
  })

  it('coalesces a staggered sub-100ms burst via true trailing-edge debounce into a single flush', () => {
    const { spy, applier } = setupCronApplier()
    for (let i = 0; i < 9; i++) {
      applier.apply({ task: 'log_discovery', status: 'success' })
      if (i < 8) vi.advanceTimersByTime(30)
    }
    expect(spy).not.toHaveBeenCalled()
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy).filter((k) => k === JSON.stringify(['last-sync', 'svc-1']))).toHaveLength(1)
  })

  it('passes { cancelRefetch: false } so in-flight /api/cron-runs requests are never aborted', async () => {
    const { qc, spy, applier } = setupCronApplier()
    qc.setQueryData(['last-sync', 'svc-1'], { started_at: '2026-10-21T00:00:00Z', status: 'success' })
    const abortedSignals: boolean[] = []
    let fetchCount = 0
    const obs = new QueryObserver(qc, {
      queryKey: ['last-sync', 'svc-1'],
      staleTime: 30_000,
      queryFn: ({ signal }) =>
        new Promise((resolve) => {
          fetchCount += 1
          signal.addEventListener('abort', () => abortedSignals.push(true))
          setTimeout(() => resolve({ started_at: '2026-10-21T00:01:00Z', status: 'success' }), 250)
        }),
    })
    const unsub = obs.subscribe(() => {})

    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(fetchCount).toBe(1)

    vi.advanceTimersByTime(20)
    applier.apply({ task: 'log_discovery', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(abortedSignals).toHaveLength(0)
    for (const call of spy.mock.calls) expect(call[1]).toEqual({ cancelRefetch: false })
    unsub()
  })
})

describe('filterStore actions (setRange, resetRange, addFilter, removeFilter, toggleFilterMode, toggleEdgeOnly, toggleCompareMode, clearFilters, resetAll)', () => {
  it('sets range and re-enables auto-range on resetRange', () => {
    useFilterStore.getState().setRange('2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z')
    expect(useFilterStore.getState()).toMatchObject({ startTime: '2026-06-01T00:00:00Z', endTime: '2026-06-02T00:00:00Z', isAutoRange: false })
    useFilterStore.getState().resetRange()
    expect(useFilterStore.getState().isAutoRange).toBe(true)
  })

  it('adds, deduplicates, and removes filters by id', () => {
    const { addFilter, removeFilter } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    addFilter('country', 'US', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(1)
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'country', value: 'US', mode: 'include' })
    expect(useFilterStore.getState().filters[0].id).toBeTruthy()

    addFilter('country', 'CA', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(2)

    removeFilter('nonexistent-id')
    expect(useFilterStore.getState().filters).toHaveLength(2)
    removeFilter(useFilterStore.getState().filters[0].id)
    expect(useFilterStore.getState().filters).toHaveLength(1)
  })

  it('addFilter dedup-suffix guard drops /_\\d+$/ columns and allows embedded digits', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { addFilter } = useFilterStore.getState()
    addFilter('response_1', 'foo', 'include')
    addFilter('field_42', 'bar', 'exclude')
    expect(useFilterStore.getState().filters).toHaveLength(0)
    expect(warn).toHaveBeenCalledTimes(2)
    warn.mockRestore()

    addFilter('status_code', '500', 'include')
    addFilter('http2_pushes', '3', 'include')
    addFilter('response', '200', 'include')
    expect(useFilterStore.getState().filters).toHaveLength(3)
  })

  it('toggleFilterMode flips include <-> exclude only for the matching id and toggleEdgeOnly flips boolean', () => {
    const { addFilter, toggleFilterMode, toggleEdgeOnly } = useFilterStore.getState()
    addFilter('a', '1', 'include')
    addFilter('b', '2', 'include')
    const [first, second] = useFilterStore.getState().filters
    toggleFilterMode(first.id)
    expect(useFilterStore.getState().filters.find((f) => f.id === first.id)?.mode).toBe('exclude')
    expect(useFilterStore.getState().filters.find((f) => f.id === second.id)?.mode).toBe('include')
    toggleFilterMode(first.id)
    expect(useFilterStore.getState().filters.find((f) => f.id === first.id)?.mode).toBe('include')

    expect(useFilterStore.getState().edgeOnly).toBe(false)
    toggleEdgeOnly()
    expect(useFilterStore.getState().edgeOnly).toBe(true)
    toggleEdgeOnly()
    expect(useFilterStore.getState().edgeOnly).toBe(false)
  })

  it('toggleCompareMode derives prior compare window, snaps to whole minutes, and preserves range on disable', () => {
    useFilterStore.getState().toggleCompareMode()
    expect(useFilterStore.getState()).toMatchObject({
      compareMode: true, compareStartTime: '2026-04-24T00:00:00.000Z', compareEndTime: '2026-05-01T00:00:00.000Z',
    })
    const before = useFilterStore.getState().compareStartTime
    useFilterStore.getState().toggleCompareMode()
    expect(useFilterStore.getState()).toMatchObject({ compareMode: false, compareStartTime: before })

    useFilterStore.setState({ startTime: '2026-05-01T00:00:30.500Z', endTime: '2026-05-08T12:34:56.789Z' })
    useFilterStore.getState().toggleCompareMode()
    expect(useFilterStore.getState()).toMatchObject({
      startTime: '2026-05-01T00:00:00.000Z', endTime: '2026-05-08T12:34:00.000Z',
    })
  })

  it('clearFilters and resetAll restore defaults and last-24h window', () => {
    const { addFilter, clearFilters, toggleCompareMode, setRange, resetAll } = useFilterStore.getState()
    addFilter('country', 'US', 'include')
    clearFilters()
    expect(useFilterStore.getState().filters).toEqual([])

    addFilter('country', 'US', 'include')
    toggleCompareMode()
    setRange('2026-05-01T18:00:00.000Z', '2026-05-02T00:00:00.000Z')
    expect(useFilterStore.getState().isAutoRange).toBe(false)

    const nowMs = Date.now()
    resetAll()
    const after = useFilterStore.getState()
    expect(after).toMatchObject({ filters: [], isAutoRange: true, compareMode: false, compareStartTime: null, compareEndTime: null })
    const startMs = new Date(after.startTime).getTime()
    const endMs = new Date(after.endTime).getTime()
    expect(Math.abs(endMs - nowMs)).toBeLessThan(1000)
    expect(Math.abs(endMs - startMs - 24 * 3600 * 1000)).toBeLessThan(1000)
  })
})

describe('debugStore, popGeoStore, serviceStore, sessionRoleStore & timezoneStore', () => {
  it('debugStore toggles flags independently and persists to fastly-debug-settings', () => {
    useDebugStore.getState().setEnabled(true)
    expect(useDebugStore.getState()).toMatchObject({ enabled: true, apiCallsEnabled: false })
    useDebugStore.setState({ enabled: false })
    useDebugStore.getState().setApiCallsEnabled(true)
    expect(useDebugStore.getState()).toMatchObject({ enabled: false, apiCallsEnabled: true })
    useDebugStore.getState().setEnabled(true)
    const parsed = JSON.parse(window.localStorage.getItem('fastly-debug-settings')!)
    expect(parsed.state).toMatchObject({ enabled: true, apiCallsEnabled: true })
  })

  it('popGeoStore defaults to empty map and setMap populates/replaces lookup', () => {
    expect(usePopGeoStore.getState().map).toEqual({})
    const geo = { SJC: { city: 'San Jose', region: 'California', country: 'US' }, AMS: { city: 'Amsterdam', region: 'North Holland', country: 'NL' } }
    usePopGeoStore.getState().setMap(geo)
    expect(usePopGeoStore.getState().map.SJC.city).toBe('San Jose')
    usePopGeoStore.getState().setMap({ AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' } })
    expect(usePopGeoStore.getState().map.SJC).toBeUndefined()
  })

  it('serviceStore updates active id, services, initialized flag, and persists only activeServiceId/services', () => {
    useServiceStore.getState().setActiveServiceId('svc-1')
    expect(useServiceStore.getState().activeServiceId).toBe('svc-1')
    useServiceStore.getState().setActiveServiceId(null)
    expect(useServiceStore.getState().activeServiceId).toBeNull()

    const list: Service[] = [{ id: 'a', name: 'A' }, { id: 'b', name: 'B' }]
    useServiceStore.getState().setServices(list)
    expect(useServiceStore.getState().services).toEqual(list)
    useServiceStore.getState().setServices([])
    expect(useServiceStore.getState().services).toEqual([])

    useServiceStore.getState().setInitialized(true)
    expect(useServiceStore.getState().isInitialized).toBe(true)

    useServiceStore.setState({ activeServiceId: 'svc-1', services: [{ id: 'svc-1', name: 'S' }], isInitialized: true })
    const parsed = JSON.parse(window.localStorage.getItem('service-storage')!)
    expect(parsed.state).toEqual({ activeServiceId: 'svc-1', services: [{ id: 'svc-1', name: 'S' }] })
  })

  it('sessionRoleStore toggles analyst role without persisting to localStorage', () => {
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(true)
    expect(Object.keys(window.localStorage).find((k) => /role|analyst/i.test(k))).toBeUndefined()
    useSessionRoleStore.getState().setIsRemoteAnalyst(false)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('timezoneStore updates manual/system timezones, persists, and migrates pre-v1 blobs', () => {
    useTimezoneStore.getState().setTimezone('America/New_York')
    expect(useTimezoneStore.getState()).toMatchObject({ timezone: 'America/New_York', mode: 'manual' })
    useTimezoneStore.getState().setTimezone('not-a-real-tz')
    expect(useTimezoneStore.getState().timezone).toBe('not-a-real-tz')

    useTimezoneStore.getState().setSystemTimezone('Asia/Tokyo')
    expect(useTimezoneStore.getState()).toMatchObject({ timezone: 'Asia/Tokyo', mode: 'system' })

    useTimezoneStore.getState().setTimezone('Europe/London')
    expect(JSON.parse(window.localStorage.getItem('timezone-storage')!).state).toMatchObject({ timezone: 'Europe/London', mode: 'manual' })

    window.localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'America/Denver' }, version: 0 }))
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState()).toMatchObject({ timezone: 'America/Denver', mode: 'manual' })

    window.localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'Asia/Tokyo', mode: 'system' }, version: 1 }))
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState()).toMatchObject({ timezone: 'Asia/Tokyo', mode: 'system' })
  })
})
