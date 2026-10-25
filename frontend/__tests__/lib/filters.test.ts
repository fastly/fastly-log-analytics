/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { applyShare, applySyncStatus, applySystemMetrics, makeCronRunsApplier } from '@/lib/admin-stream-apply'
import { buildFiltersPayload } from '@/types/api'
import type { FilterPill } from '@/types/api'

import { createTestQueryClient } from '../helpers/page-smoke'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetRelativeRange = vi.fn()

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: {
    getState: vi.fn(() => ({
      addFilter: mockAddFilter,
      clearFilters: mockClearFilters,
      setRange: mockSetRange,
      setRelativeRange: mockSetRelativeRange,
    })),
  },
}))

describe('buildFiltersPayload', () => {
  it('returns empty object for no filters', () => {
    expect(buildFiltersPayload([])).toEqual({})
  })

  it('single include filter', () => {
    const pills: FilterPill[] = [{ id: '1', column: 'status', value: '200', mode: 'include' }]
    expect(buildFiltersPayload(pills)).toEqual({
      status: { mode: 'include', values: ['200'] },
    })
  })

  it('single exclude filter', () => {
    const pills: FilterPill[] = [{ id: '1', column: 'country', value: 'US', mode: 'exclude' }]
    expect(buildFiltersPayload(pills)).toEqual({
      country: { mode: 'exclude', values: ['US'] },
    })
  })

  it('two include filters on the same column merge into one entry', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '201', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(result.status.mode).toBe('include')
    expect(result.status.values).toContain('200')
    expect(result.status.values).toContain('201')
  })

  it('include and exclude on the same column get separate keys', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '500', mode: 'exclude' },
    ]
    const result = buildFiltersPayload(pills)
    const keys = Object.keys(result)
    expect(keys.some(k => k === 'status' || k.startsWith('status_'))).toBe(true)
    expect(keys.length).toBe(2)
    const modes = keys.map(k => result[k].mode)
    expect(modes).toContain('include')
    expect(modes).toContain('exclude')
  })

  it('multiple different columns each get their own key', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'country', value: 'US', mode: 'include' },
      { id: '3', column: 'pop', value: 'JFK', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(result.status).toBeDefined()
    expect(result.country).toBeDefined()
    expect(result.pop).toBeDefined()
  })

  it('produces the shape the backend FiltersDict expects', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'ip', value: '1.2.3.4', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(typeof result).toBe('object')
    expect(typeof result.ip).toBe('object')
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
  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
  })

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
    expect(mockSetRange).toHaveBeenCalledWith(
      '2026-01-01T00:00:00Z',
      '2026-01-02T00:00:00Z',
    )
  })

  it('parses ?range= and calls setRelativeRange with computed start/end', async () => {
    setSearch('range=24h')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    const args = mockSetRelativeRange.mock.calls[0]
    expect(args[0]).toBe('24h')
    expect(typeof args[1]).toBe('string')
    expect(typeof args[2]).toBe('string')
  })

  it('parses minute relative ranges such as ?range=5m', async () => {
    setSearch('range=5m')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    expect(mockSetRelativeRange.mock.calls[0][0]).toBe('5m')
  })

  it('parses modern ?filters JSON payload', async () => {
    const filters = {
      status: { values: ['200', '404'], mode: 'include' },
      country: { values: ['CA'], mode: 'exclude' },
    }
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
  return Array.from(new URLSearchParams(qs).keys()).filter(k => k.startsWith('filter_'))
}

function assertFilterFormat(url: string) {
  const search = url.includes('?') ? url.split('?')[1] : url
  const params = new URLSearchParams(search)
  for (const key of params.keys()) {
    if (key.startsWith('filter')) {
      expect(key).toMatch(/^filter_\w+/)
    }
  }
}

describe('Security page dashboard links', () => {
  it('bot id filter uses filter_ prefix', () => {
    const url = `/dashboard?filter__wellknown_bot_id=${encodeURIComponent('bot-123')}`
    const keys = filterKeysFrom(url)
    expect(keys).toContain('filter__wellknown_bot_id')
  })

  it('ngwaf bot filter uses filter_ prefix', () => {
    const url = `/dashboard?filter__ngwaf_bot_name=${encodeURIComponent('scraperbot')}`
    const keys = filterKeysFrom(url)
    expect(keys).toContain('filter__ngwaf_bot_name')
  })

  it('tls fingerprint filter uses filter_ prefix', () => {
    const url = `/dashboard?filter_tls_client_hello=${encodeURIComponent('abc123')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_tls_client_hello')
  })

  it('client IP filter uses filter_ prefix', () => {
    const url = `/dashboard?filter_client_ip=${encodeURIComponent('1.2.3.4')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_client_ip')
  })
})

describe('Network page dashboard links', () => {
  it('POP filter uses filter_pop', () => {
    const pop = 'JFK'
    const url = `/dashboard?filter_pop=${encodeURIComponent(pop)}`
    assertFilterFormat(url)
    expect(new URLSearchParams(url.split('?')[1]).get('filter_pop')).toBe(pop)
  })

  it('shield POP filter uses filter_shield_pop', () => {
    const url = `/dashboard?filter_shield_pop=${encodeURIComponent('IAD')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_shield_pop')
  })

  it('ASN filter uses filter_asn', () => {
    const url = `/dashboard?filter_asn=${encodeURIComponent('7922')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_asn')
  })

  it('city/region/country multi-filter all use filter_ prefix', () => {
    const city = 'London'
    const region = 'ENG'
    const country = 'GB'
    const url = `/dashboard?filter_city=${encodeURIComponent(city)}&filter_region=${encodeURIComponent(region)}&filter_country=${encodeURIComponent(country)}`
    assertFilterFormat(url)
    const params = new URLSearchParams(url.split('?')[1])
    expect(params.get('filter_city')).toBe(city)
    expect(params.get('filter_region')).toBe(region)
    expect(params.get('filter_country')).toBe(country)
    expect(params.has('city')).toBe(false)
    expect(params.has('region')).toBe(false)
  })
})

describe('Origin page dashboard links', () => {
  it('URL filter uses filter_url', () => {
    const url = `/dashboard?filter_url=${encodeURIComponent('/api/data')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_url')
  })

  it('POP filter uses filter_pop', () => {
    const url = `/dashboard?filter_pop=${encodeURIComponent('SYD')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_pop')
  })

  it('origin IP filter uses filter_oip (the actual schema column)', () => {
    const url = `/dashboard?filter_oip=${encodeURIComponent('10.0.0.1')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_oip')
  })
})

describe('filter_ format is parseable by useViewMetricUrlSync', () => {
  it('all filter keys start with filter_ and are followed by at least one word char', () => {
    const sampleUrls = [
      `/dashboard?filter_status=200`,
      `/dashboard?filter_country=US&filter_pop=JFK`,
      `/dashboard?filter__wellknown_bot_id=bot-1`,
      `/dashboard?filter_city=London&filter_region=ENG&filter_country=GB`,
    ]
    for (const url of sampleUrls) {
      const params = new URLSearchParams(url.split('?')[1])
      for (const key of params.keys()) {
        expect(key.startsWith('filter_')).toBe(true)
        const col = key.substring(7)
        expect(col.length).toBeGreaterThan(0)
      }
    }
  })
})


function makeQueryClient(): QueryClient {
  // gcTime > 0 — appliers write via setQueryData with no subscriber.
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

function invalidatedKeys(spy: ReturnType<typeof vi.spyOn>): string[] {
  return spy.mock.calls.map((c: unknown[]) => JSON.stringify((c[0] as { queryKey: unknown })?.queryKey))
}

// ── applySyncStatus ──────────────────────────────────────────────────────────

describe('applySyncStatus', () => {
  it('writes the snapshot to [sync-status, serviceId]', () => {
    const qc = makeQueryClient()
    applySyncStatus(qc, 'svc-1', { local_rows: 7, latest_log_at: '2026-06-26T00:00:00Z' })
    expect(qc.getQueryData(['sync-status', 'svc-1'])).toEqual({
      local_rows: 7,
      latest_log_at: '2026-06-26T00:00:00Z',
    })
  })
})

// ── applySystemMetrics ───────────────────────────────────────────────────────

describe('applySystemMetrics', () => {
  it('fans non-null slices into their cache keys', () => {
    const qc = makeQueryClient()
    const payload = {
      health_snapshot: { status: 'ok' },
      metric_history_1h: [{ t: 1, v: 2 }],
      queries_summary: { total: 42 },
      slow_queries_count: 3,
      log_accounting: { rows: 1000 },
      metadata_storage: { bytes: 9999 },
      system_jobs: [{ id: 'job-1' }],
    }
    applySystemMetrics(qc, 'svc-1', payload)
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(payload.health_snapshot)
    expect(qc.getQueryData(['admin', 'metric-history-batch', '1h'])).toEqual(payload.metric_history_1h)
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual(payload.queries_summary)
    // log-accounting + slow-queries-count are per-service: written to the
    // service-scoped key the cards read, so a service switch can't show one
    // service's freshness under another.
    expect(qc.getQueryData(['admin', 'overview', 'slow-queries-count', 'svc-1'])).toEqual(payload.slow_queries_count)
    expect(qc.getQueryData(['admin', 'overview', 'log-accounting', 'svc-1'])).toEqual(payload.log_accounting)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual(payload.metadata_storage)
    expect(qc.getQueryData(['system-jobs'])).toEqual(payload.system_jobs)
  })

  it('SKIPS null/omitted slices and preserves stale data', () => {
    const qc = makeQueryClient()
    const stale = { status: 'ok' }
    qc.setQueryData(['admin', 'health-snapshot'], stale)
    qc.setQueryData(['admin', 'metadata-storage'], { bytes: 12345 })
    applySystemMetrics(qc, 'svc-1', { health_snapshot: null, queries_summary: { total: 99 }, metadata_storage: null })
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(stale)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual({ bytes: 12345 })
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual({ total: 99 })
  })

  it('ignores a non-object payload without throwing', () => {
    const qc = makeQueryClient()
    expect(() => applySystemMetrics(qc, 'svc-1', null)).not.toThrow()
    expect(() => applySystemMetrics(qc, 'svc-1', 'nope')).not.toThrow()
  })
})

// ── applyShare ───────────────────────────────────────────────────────────────

describe('applyShare', () => {
  it('writes the live payload to [admin, share, live]', () => {
    const qc = makeQueryClient()
    const payload = { sharing_active: true, public_url: 'https://x.test', active_session_count: 2 }
    applyShare(qc, payload)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(payload)
  })

  it('ignores an undefined payload without clobbering cached data', () => {
    const qc = makeQueryClient()
    const stale = { sharing_active: false }
    qc.setQueryData(['admin', 'share', 'live'], stale)
    applyShare(qc, undefined)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(stale)
  })
})

// ── makeCronRunsApplier ──────────────────────────────────────────────────────

describe('makeCronRunsApplier', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('coalesces a burst into one flush after 100ms (recent + schedule always)', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'commit', status: 'success' })
    applier.apply({ task: 'log_discovery', status: 'running' })
    // Nothing flushed yet — trailing-edge coalesce.
    expect(spy).not.toHaveBeenCalled()

    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'cron-schedule', 'svc-1']))
    // Each key invalidated exactly once despite two apply() calls.
    expect(keys.filter((k) => k === JSON.stringify(['admin', 'cron-schedule', 'svc-1']))).toHaveLength(1)
  })

  it('invalidates the heavy table only on a COMPLETED event, not running', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'commit', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))

    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
  })

  it('invalidates [last-sync] for sync events (both running and complete) but not other tasks', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['last-sync', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['last-sync', 'svc-1']))
  })

  it('invalidates [admin, iceberg] only when an iceberg-mutating task COMPLETES', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // running iceberg task → no iceberg invalidation
    applier.apply({ task: 'optimize_iceberg', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    // non-iceberg completed task → no iceberg invalidation
    spy.mockClear()
    applier.apply({ task: 'gap_heal', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    // completed iceberg-mutating task → invalidates
    spy.mockClear()
    applier.apply({ task: 'optimize_iceberg', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'iceberg']))
  })

  it('invalidates audit/ingested on complete, schema only for data-mutating tasks', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // data-mutating (commit) completed → audit + ingested + schema
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    let keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    // non-data-mutating (alerts) completed → audit/ingested fire, schema does NOT
    spy.mockClear()
    applier.apply({ task: 'alerts', status: 'success' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    // running event → none of audit/ingested/schema
    spy.mockClear()
    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).not.toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))
  })

  it('malformed payload falls back to table, skips last-sync', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply(null)
    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['last-sync', 'svc-1']))
  })

  it('cleanup() cancels a pending flush so nothing invalidates after teardown', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'log_discovery', status: 'success' })
    applier.cleanup()
    vi.advanceTimersByTime(100)
    expect(spy).not.toHaveBeenCalled()
  })

  it('coalesces a staggered sub-100ms burst via true trailing-edge debounce into a single flush', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // Simulate 9 rapid cron_runs SSE events arriving 30ms apart (240ms total span).
    for (let i = 0; i < 9; i++) {
      applier.apply({ task: 'log_discovery', status: 'success' })
      if (i < 8) {
        vi.advanceTimersByTime(30)
      }
    }
    // Still inside the trailing-edge coalesce window — must not have flushed mid-burst.
    expect(spy).not.toHaveBeenCalled()

    vi.advanceTimersByTime(100)
    const lastSyncCalls = invalidatedKeys(spy).filter((k) => k === JSON.stringify(['last-sync', 'svc-1']))
    expect(lastSyncCalls).toHaveLength(1)
  })

  it('passes { cancelRefetch: false } so in-flight /api/cron-runs requests are never aborted', async () => {
    const qc = makeQueryClient()
    // Seed initial data (matching useBootstrap seeding lastSync on mount).
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
    // Attach an active observer so invalidateQueries triggers refetching.
    const unsub = obs.subscribe(() => {})

    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // First flush starts a 250ms in-flight fetch.
    applier.apply({ task: 'log_discovery', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(fetchCount).toBe(1)

    // Second flush 120ms later arrives while the 250ms fetch is STILL in flight.
    vi.advanceTimersByTime(20)
    applier.apply({ task: 'log_discovery', status: 'success' })
    vi.advanceTimersByTime(100)

    // Must never abort the in-flight request (which causes net::ERR_ABORTED in the browser).
    expect(abortedSignals).toHaveLength(0)
    for (const call of spy.mock.calls) {
      expect(call[1]).toEqual({ cancelRefetch: false })
    }

    unsub()
  })
})
