/**
 * @vitest-environment jsdom
 *
 * Consolidated hook tests:
 * - useBootstrap, useLogFieldsCatalog, useActiveLogFields
 * - useIsAnalyst, useMaskIps, useEnforceMaskedFilters, useIsDataReady, useAnalystHeartbeat
 * - useReportConfig, useInsightsDefaults, useAnalyticsReportQuery, useCardVisibility
 * - useSyncStatus, useLastSync, useHeaderBadgeStream
 * - useShareStatusBanner
 * - useHighScaleRequestFacts
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { hashKey } from '@tanstack/react-query'
import {
  createTestQueryClient,
  makeNeverClosingStreamResponse,
  makeQueryWrapper,
  makeStreamResponse,
  postOk,
} from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { queryKeys, client } from "@/lib/api";
import { __resetForTests } from '@/lib/utils'
import {
  useActiveLogFields,
  useAnalystHeartbeat,
  useBootstrap,
  useEnforceMaskedFilters,
  useIsAnalyst,
  useIsDataReady,
  useLogFieldsCatalog,
  useMaskIps,
} from '@/hooks/useBootstrap'
import {
  useAnalyticsReportQuery,
  useCardVisibility,
  useInsightsDefaults,
  useReportConfig,
} from '@/hooks/useDashboardCards'
import { useHeaderBadgeStream, useLastSync, useSyncStatus } from '@/hooks/useSSE'
import { useShareStatusBanner } from '@/components/AppLayout'
import { useHighScaleRequestFacts } from '@/components/high-scale/HighScaleRequestFactsPanel'
import { useFilterStore } from '@/stores/filterStore'

const push = vi.fn()
const replace = vi.fn()
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push, replace }),
  useSearchParams: () => new URLSearchParams(),
}))

const mockSetServices = vi.fn()
const mockSetInitialized = vi.fn()
const mockSetActiveServiceId = vi.fn()

const mockServiceState = {
  activeServiceId: null as string | null,
  services: [] as Array<{ id: string; name: string; accessLevel?: 'read_write' | 'read_only' }>,
  isInitialized: false,
  setActiveServiceId: mockSetActiveServiceId,
  setServices: mockSetServices,
  setInitialized: mockSetInitialized,
}

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockServiceState) => unknown) =>
      selector ? selector(mockServiceState) : mockServiceState,
    ),
    {
      getState: () => mockServiceState,
      setState: (patch: Partial<typeof mockServiceState>) => Object.assign(mockServiceState, patch),
    },
  )
  return { ...actual, useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

function wrapper() {
  return makeQueryWrapper(createTestQueryClient({ queries: { gcTime: 0 } }))
}

function catalogWrapper(catalogSeed?: unknown, bootstrapSeed?: unknown) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (catalogSeed !== undefined) qc.setQueryData(queryKeys.logFieldsCatalog('svc-1'), catalogSeed)
  if (bootstrapSeed !== undefined) qc.setQueryData(queryKeys.bootstrap(), bootstrapSeed)
  return makeQueryWrapper(qc)
}

// ── useBootstrap, useLogFieldsCatalog & useActiveLogFields ──────────────────

describe('useBootstrap (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockServiceState.activeServiceId = null
    mockServiceState.services = []
    mockServiceState.isInitialized = false
  })

  it('calls setServices with mapped shape, sets active_service_id, and marks initialized', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [
            { service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' },
            {
              service_id: 'svc-2',
              name: 'Staging',
              access_level: 'read_only',
              analyst_path_a_supported: false,
              analyst_path_a_reason: 'Use Path B.',
            },
          ],
          active_service_id: 'svc-1',
        }),
      ),
    )
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
    const [mapped] = mockSetServices.mock.calls[0]
    expect(mapped).toEqual([
      {
        id: 'svc-1', name: 'My CDN', accessLevel: 'read_write',
        cmcdEnabled: undefined, rum_enabled: undefined,
        analystPathASupported: true, analystPathAReason: null,
      },
      {
        id: 'svc-2', name: 'Staging', accessLevel: 'read_only',
        cmcdEnabled: undefined, rum_enabled: undefined,
        analystPathASupported: false, analystPathAReason: 'Use Path B.',
      },
    ])
    expect(mapped[1].service_id).toBeUndefined()
    expect(mapped[1].access_level).toBeUndefined()
    expect(mapped[1].analyst_path_a_supported).toBeUndefined()
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-1')
    expect(mockSetInitialized).toHaveBeenCalledWith(true)
  })

  it('handles empty services array gracefully and surfaces 500 fetch errors', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ services: [], active_service_id: null }),
      ),
    )
    const { unmount } = renderHook(() => useBootstrap(), { wrapper: wrapper() })
    await waitFor(() => expect(mockSetServices).toHaveBeenCalledWith([]))
    await waitFor(() => expect(mockSetInitialized).toHaveBeenCalledWith(true))
    unmount()

    vi.clearAllMocks()
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ detail: 'boom' }, { status: 500 }),
      ),
    )
    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 3000 })
    expect(mockSetServices).not.toHaveBeenCalled()
    expect(mockSetInitialized).not.toHaveBeenCalled()
  })

  it('reconcile: does NOT revert a selected service from a STALE snapshot, but reverts on a FRESH snapshot', async () => {
    mockServiceState.activeServiceId = 'svc-new'
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    qc.setQueryData(
      ['bootstrap'],
      { services: [{ service_id: 'svc-old', name: 'Old', access_level: 'read_write' }], active_service_id: 'svc-old' },
      { updatedAt: Date.now() - 10 * 60 * 1000 },
    )
    server.use(http.get(`${API_BASE}/api/bootstrap`, () => new Promise(() => {})))
    const { unmount } = renderHook(() => useBootstrap(), { wrapper: makeQueryWrapper(qc) })
    await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
    await new Promise((r) => setTimeout(r, 30))
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
    unmount()

    mockServiceState.activeServiceId = 'svc-dead'
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ service_id: 'svc-real', name: 'Real', access_level: 'read_write' }],
          active_service_id: 'svc-real',
        }),
      ),
    )
    renderHook(() => useBootstrap(), { wrapper: wrapper() })
    await waitFor(() => expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-real'))
  })

  it('recovers automatically after a 503 deploy and stops polling once succeeded', async () => {
    let failing = true
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () => {
        calls += 1
        if (failing) return new HttpResponse(null, { status: 503 })
        return HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
        })
      }),
    )

    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 10000 })
    failing = false
    await waitFor(() => expect(result.current.isSuccess).toBe(true), { timeout: 15000 })
    expect(mockSetServices).toHaveBeenCalled()

    const afterSuccess = calls
    await new Promise((r) => setTimeout(r, 100))
    expect(calls).toBe(afterSuccess)
  }, 30000)
})

const CATALOG = {
  groups: [
    { id: 'L', label: 'Origin Metrics', fields: ['ottfb', 'ottlb', 'oip', 'prid'] },
    { id: 'D', label: 'Geolocation', fields: ['country', 'city', 'region'] },
  ],
}

describe('useLogFieldsCatalog & useActiveLogFields', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockServiceState.activeServiceId = 'svc-1'
  })

  it('useLogFieldsCatalog threads service_id, returns catalog payload, and surfaces 500 errors', async () => {
    let observedSid: string | null = null
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, ({ request }) => {
        observedSid = new URL(request.url).searchParams.get('service_id')
        return HttpResponse.json({
          fields: [{ id: 'status', label: 'HTTP Status' }],
          custom_fields: [{ name: 'x-edge-ts', duckdb_type: 'VARCHAR' }],
          groups: [],
          presets: { default: { columns: ['status'] } },
        })
      }),
    )
    const { result, unmount } = renderHook(() => useLogFieldsCatalog(), { wrapper: catalogWrapper() })
    await waitFor(() => expect(result.current.data).toBeDefined())
    expect(observedSid).toBe('svc-1')
    expect(result.current.data?.fields).toEqual([{ id: 'status', label: 'HTTP Status' }])
    expect((result.current.data as { custom_fields?: unknown } | undefined)?.custom_fields).toEqual([
      { name: 'x-edge-ts', duckdb_type: 'VARCHAR' },
    ])
    unmount()

    server.use(http.get(`${API_BASE}/api/log-fields/catalog`, () => HttpResponse.json({ error: 'oops' }, { status: 500 })))
    const { result: errRes } = renderHook(() => useLogFieldsCatalog(), { wrapper: catalogWrapper() })
    await waitFor(() => expect(errRes.current.isError).toBe(true))
  })

  it('useActiveLogFields handles pending/empty/populated active_log_field_ids, groups, and missing catalog', () => {
    server.use(http.get(`${API_BASE}/api/bootstrap`, () => new Promise(() => {})))
    const { result: pending } = renderHook(() => useActiveLogFields(), { wrapper: catalogWrapper(CATALOG) })
    expect(pending.current.ready).toBe(false)
    expect(pending.current.isFieldActive('ottfb')).toBe(true)
    expect(pending.current.isGroupActive('L')).toBe(true)

    const { result: empty } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: [] }),
    })
    expect(empty.current.ready).toBe(false)
    expect(empty.current.isFieldActive('ottfb')).toBe(true)

    const { result: populated } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['ottfb', 'country'] }),
    })
    expect(populated.current.ready).toBe(true)
    expect(populated.current.isFieldActive('ottfb')).toBe(true)
    expect(populated.current.isFieldActive('ja3')).toBe(false)
    expect(populated.current.isGroupActive('D')).toBe(true)
    expect(populated.current.isGroupActive('ZZ')).toBe(false)

    server.use(http.get(`${API_BASE}/api/log-fields/catalog`, () => HttpResponse.json({ error: 'missing' }, { status: 500 })))
    const { result: noCat } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(undefined, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['ottfb'] }),
    })
    expect(noCat.current.isFieldActive('ottfb')).toBe(true)
    expect(noCat.current.isGroupActive('L')).toBe(false)
  })
})

// ── useIsAnalyst, useMaskIps, useEnforceMaskedFilters & useIsDataReady ──────

function settingsWrapper(opts: { isRemoteAnalyst?: boolean; maskIps?: boolean; bootstrapExtra?: Record<string, unknown> } = {}) {
  const qc = createTestQueryClient()
  const settings: Record<string, unknown> = {}
  if (opts.isRemoteAnalyst !== undefined) settings.is_remote_analyst = opts.isRemoteAnalyst
  if (opts.maskIps !== undefined) settings.mask_ips = opts.maskIps
  if (Object.keys(settings).length > 0 || opts.isRemoteAnalyst !== undefined || opts.bootstrapExtra) {
    qc.setQueryData(queryKeys.bootstrap(), { settings, ...opts.bootstrapExtra })
  }
  return makeQueryWrapper(qc)
}

describe('useIsAnalyst, useMaskIps, useEnforceMaskedFilters & useIsDataReady', () => {
  beforeEach(() => {
    mockServiceState.activeServiceId = null
    mockServiceState.services = []
    mockServiceState.isInitialized = false
    useFilterStore.getState().clearFilters()
  })

  it.each([
    ['admin (no signals)', 'svc-1', [{ id: 'svc-1', name: 'Test' }], false, false],
    ['remote analyst flag', 'svc-1', [{ id: 'svc-1', name: 'Test' }], true, true],
    ['read_only accessLevel', 'svc-1', [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' as const }], false, true],
    ['both signals', 'svc-1', [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' as const }], true, true],
    ['missing service', 'svc-missing', [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' as const }], false, false],
  ])('useIsAnalyst (%s) → %s', (_label, activeId, services, isRemoteAnalyst, expected) => {
    mockServiceState.activeServiceId = activeId
    mockServiceState.services = services
    const { result } = renderHook(() => useIsAnalyst(), { wrapper: settingsWrapper({ isRemoteAnalyst }) })
    expect(result.current).toBe(expected)
  })

  it('useMaskIps and useEnforceMaskedFilters strip IP filters only when masking', async () => {
    expect(renderHook(() => useMaskIps(), { wrapper: settingsWrapper({ maskIps: true }) }).result.current).toBe(true)
    expect(renderHook(() => useMaskIps(), { wrapper: settingsWrapper({ maskIps: false }) }).result.current).toBe(false)
    expect(renderHook(() => useMaskIps(), { wrapper: settingsWrapper() }).result.current).toBe(false)

    useFilterStore.getState().addFilter('ip', '73.217.41.5', 'include')
    useFilterStore.getState().addFilter('country', 'US', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: settingsWrapper({ maskIps: false }) })
    await new Promise((r) => setTimeout(r, 0))
    expect(useFilterStore.getState().filters.map((f) => f.column)).toContain('ip')

    renderHook(() => useEnforceMaskedFilters(), { wrapper: settingsWrapper({ maskIps: true }) })
    await waitFor(() => {
      const cols = useFilterStore.getState().filters.map((f) => f.column)
      expect(cols).not.toContain('ip')
      expect(cols).toContain('country')
    })
  })

  it('useIsDataReady checks store activeServiceId and falls back to bootstrap.active_service_id', () => {
    expect(renderHook(() => useIsDataReady(), { wrapper: settingsWrapper() }).result.current).toBe(false)
    expect(
      renderHook(() => useIsDataReady(), {
        wrapper: settingsWrapper({ bootstrapExtra: { active_service_id: 'svc-from-bootstrap' } }),
      }).result.current,
    ).toBe(true)
    mockServiceState.activeServiceId = 'svc-from-store'
    expect(renderHook(() => useIsDataReady(), { wrapper: settingsWrapper() }).result.current).toBe(true)
  })
})

// ── useReportConfig, useInsightsDefaults, useAnalyticsReportQuery & useCardVisibility ─

describe('useReportConfig', () => {
  beforeEach(() => {
    useFilterStore.setState({ startTime: '', endTime: '' })
  })

  it('provides default configuration', () => {
    const { result } = renderHook(() => useReportConfig())
    expect(result.current).toMatchObject({ metric: 'requests', chartInterval: '1 second', trend: 'off' })
  })

  it.each([
    ['12h window restricts 1 day', 12 * 3600_000, '1 day' as const, '1 day' as const, false, '1 minute'],
    ['30m window restricts 1 hour', 30 * 60_000, '1 hour' as const, '1 hour' as const, false, '1 minute'],
    ['48h window allows 1 day', 48 * 3600_000, '1 day' as const, '1 day' as const, true, '1 day'],
    ['7d window defaults to 1 hour', 7 * 24 * 3600_000, undefined, '1 hour' as const, true, '1 hour'],
    ['30d window defaults to 1 day', 30 * 24 * 3600_000, undefined, '1 day' as const, true, '1 day'],
    ['24h window allows 1 hour', 24 * 3600_000, '1 hour' as const, '1 hour' as const, true, '1 hour'],
  ])('%s', (_label, spanMs, defaultInterval, checkInterval, isValid, expectedInterval) => {
    const now = new Date()
    useFilterStore.setState({
      startTime: new Date(now.getTime() - spanMs).toISOString(),
      endTime: now.toISOString(),
    })
    const { result } = renderHook(() => useReportConfig())
    if (isValid && defaultInterval) {
      act(() => result.current.setChartInterval(defaultInterval))
    }
    expect(result.current.config.validIntervals.has(checkInterval)).toBe(isValid)
    expect(result.current.chartInterval).toBe(expectedInterval)
  })

  it('gracefully falls back when user shrinks timeline while an invalid interval is selected', () => {
    const now = new Date()
    useFilterStore.setState({
      startTime: new Date(now.getTime() - 48 * 3600_000).toISOString(),
      endTime: now.toISOString(),
    })
    const { result, rerender } = renderHook(() => useReportConfig())
    act(() => result.current.setChartInterval('1 day'))
    expect(result.current.chartInterval).toBe('1 day')

    act(() => {
      useFilterStore.setState({
        startTime: new Date(now.getTime() - 6 * 3600_000).toISOString(),
        endTime: now.toISOString(),
      })
    })
    rerender()
    expect(result.current.config.validIntervals.has('1 day')).toBe(false)
    expect(result.current.chartInterval).toBe('1 minute')
  })
})

describe('useInsightsDefaults & useAnalyticsReportQuery', () => {
  beforeEach(() => {
    mockServiceState.activeServiceId = 'svc-1'
  })

  it('useInsightsDefaults adapts to extents, falls back when null, preserves user override, and resets on service switch', () => {
    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    const hoursAgo = (h: number) => new Date(Date.now() - h * 3_600_000).toISOString()
    qc.setQueryData(['log-extents', 'svc-a'], { earliest_log_at: hoursAgo(2), latest_log_at: hoursAgo(0) })
    qc.setQueryData(['log-extents', 'svc-b'], { earliest_log_at: hoursAgo(240), latest_log_at: hoursAgo(0) })

    const { result: nullRes } = renderHook(() => useInsightsDefaults(null), { wrapper: makeQueryWrapper(qc) })
    expect(nullRes.current).toMatchObject({ windowHours: '24', baselineHours: '168' })

    const { result, rerender } = renderHook(({ sid }) => useInsightsDefaults(sid), {
      wrapper: makeQueryWrapper(qc),
      initialProps: { sid: 'svc-a' },
    })
    expect(result.current).toMatchObject({ windowHours: '1', baselineHours: '1' })

    act(() => result.current.setBaselineHours('720'))
    expect(result.current).toMatchObject({ windowHours: '1', baselineHours: '720' })

    rerender({ sid: 'svc-b' })
    expect(result.current).toMatchObject({ windowHours: '24', baselineHours: '168' })
  })

  it('useAnalyticsReportQuery matches SSR seed key in token mode, keys on abs: in custom mode, and forwards AbortSignal', async () => {
    const captured: Array<Record<string, unknown>> = []
    let seenSignal: AbortSignal | undefined
    const qc1 = createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
    const { result: r1 } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['origin', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null, isAutoRange: true,
            startTime: '2026-06-29T11:00:00Z', endTime: '2026-06-29T12:00:00Z', anchor: '2026-06-29T12:00:00Z',
          },
          keyParts: [60, 'ttfb', 'p95'],
          queryFn: async ({ rangeBody, rangeToken, signal }) => {
            captured.push({ rangeBody, rangeToken })
            seenSignal = signal
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc1) },
    )
    await waitFor(() => expect(r1.current.data).toEqual({ ok: true }))
    expect(captured[0]).toMatchObject({ rangeToken: '24h', rangeBody: { range_token: '24h', anchor: '2026-06-29T12:00:00Z' } })
    expect(seenSignal).toBeInstanceOf(AbortSignal)
    const key1 = qc1.getQueryCache().getAll()[0].queryKey
    const ssrSeedKey = ['origin', 'aggregates', 'svc-1', '24h', '2026-06-29T12:00:00Z', {}, 60, 'ttfb', 'p95']
    expect(key1).toEqual(ssrSeedKey)
    expect(hashKey(key1)).toBe(hashKey(ssrSeedKey))

    const qc2 = createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
    const { result: r2 } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['performance', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null, isAutoRange: false,
            startTime: '2026-06-01T00:00:00Z', endTime: '2026-06-02T00:00:00Z', anchor: '2026-06-02T00:00:00Z',
          },
          keyParts: ['p99'],
          queryFn: async ({ rangeBody, rangeToken }) => {
            captured.push({ rangeBody, rangeToken })
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc2) },
    )
    await waitFor(() => expect(r2.current.data).toEqual({ ok: true }))
    expect(captured[1]).toEqual({
      rangeToken: null,
      rangeBody: { start_time: '2026-06-01T00:00:00Z', end_time: '2026-06-02T00:00:00Z' },
    })
    expect(qc2.getQueryCache().getAll()[0].queryKey).toEqual([
      'performance', 'aggregates', 'svc-1', 'abs:2026-06-01T00:00:00Z|2026-06-02T00:00:00Z',
      '2026-06-02T00:00:00Z', {}, 'p99',
    ])
  })
})

describe('useCardVisibility', () => {
  const STORAGE_KEY = 'test-card-visibility'
  const MIGRATION_KEY = `${STORAGE_KEY}_mv`
  const ALL_IDS = ['a', 'b', 'c', 'd']

  beforeEach(() => localStorage.clear())

  it('hydrates defaults or persisted set, and supports toggleCard, showAll, and reset', () => {
    expect([...renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS)).result.current.visibleCards].sort()).toEqual(ALL_IDS)
    expect([...renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a', 'b'])).result.current.visibleCards].sort()).toEqual(['a', 'b'])

    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a']))
    act(() => result.current.toggleCard('b'))
    expect([...result.current.visibleCards].sort()).toEqual(['a', 'b'])
    act(() => result.current.toggleCard('a'))
    expect([...result.current.visibleCards]).toEqual(['b'])
    act(() => result.current.showAll())
    expect([...result.current.visibleCards].sort()).toEqual(ALL_IDS)
    act(() => result.current.reset())
    expect([...result.current.visibleCards]).toEqual(['a'])
  })

  it('applies removeIds/addIds migrations when version advances, skips when up-to-date or fresh browser', () => {
    renderHook(() => useCardVisibility(STORAGE_KEY, ['a', 'b'], ['a'], { version: 1, addIds: ['b'] }))
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull()
    expect(localStorage.getItem(MIGRATION_KEY)).toBeNull()

    localStorage.setItem(STORAGE_KEY, JSON.stringify(['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id']))
    renderHook(() =>
      useCardVisibility(
        STORAGE_KEY,
        ['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'],
        undefined,
        { version: 2, removeIds: ['bw', 'rid', 'prid', 'waf_sig', 'waf_req_id'], addIds: ['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'] },
      ),
    )
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!).sort()).toEqual(['_bot_name', '_ngwaf_bot_name', 'method', 'status', 'waf_sig_ind'])
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('2')

    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b']))
    localStorage.setItem(MIGRATION_KEY, '5')
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, { version: 5, removeIds: ['a'] }))
    expect(result.current.visibleCards.has('a')).toBe(true)
  })
})

// ── useSyncStatus, useLastSync & useHeaderBadgeStream ───────────────────────

function syncWrapper(opts: { staleSeed?: boolean; staleLastSyncSeed?: boolean } = {}) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (opts.staleSeed) qc.setQueryData(queryKeys.syncStatus('svc-1'), null, { updatedAt: 0 })
  if (opts.staleLastSyncSeed) qc.setQueryData(queryKeys.lastSync('svc-1'), null, { updatedAt: 0 })
  return makeQueryWrapper(qc)
}

describe('useSyncStatus, useLastSync & useHeaderBadgeStream', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockServiceState.activeServiceId = 'svc-1'
    mockServiceState.services = [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }]
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('useSyncStatus fetches with skip_fos=true for admins and is disabled for analysts', async () => {
    let observedSkipFos: string | null = null
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/sync-status`, ({ request }) => {
        calls += 1
        observedSkipFos = new URL(request.url).searchParams.get('skip_fos')
        return HttpResponse.json({
          latest_log_at: '2026-06-14T00:00:00Z', local_rows: 12345, fos_rows: null, last_sync_at: '2026-06-14T00:01:00Z',
        })
      }),
    )
    const { result, unmount } = renderHook(() => useSyncStatus(), { wrapper: syncWrapper({ staleSeed: true }) })
    await waitFor(() => expect(result.current.data?.local_rows).toBe(12345))
    expect(result.current.data?.latest_log_at).toBe('2026-06-14T00:00:00Z')
    expect(observedSkipFos).toBe('true')
    unmount()

    calls = 0
    mockServiceState.services = [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }]
    const { result: analystRes } = renderHook(() => useSyncStatus(), { wrapper: syncWrapper({ staleSeed: true }) })
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(0)
    expect(analystRes.current.data).toBeNull()
  })

  it('useLastSync returns latest log_discovery run, handles empty entries, and skips for analysts', async () => {
    let params: URLSearchParams | null = null
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, ({ request }) => {
        calls += 1
        params = new URL(request.url).searchParams
        return HttpResponse.json({
          total: 1, page: 1, per_page: 1,
          entries: [{ id: 195883, task: 'sync', started_at: '2026-06-15T23:37:41Z', duration_s: 7.2, status: 'success', error_message: null, files_downloaded: 12, rows_ingested: 15 }],
        })
      }),
    )
    const { result, unmount } = renderHook(() => useLastSync(), { wrapper: syncWrapper({ staleLastSyncSeed: true }) })
    await waitFor(() => expect(result.current.data?.started_at).toBe('2026-06-15T23:37:41Z'))
    expect(result.current.data).toMatchObject({ started_at: '2026-06-15T23:37:41Z', status: 'success', duration_s: 7.2 })
    expect(params!.get('task')).toBe('log_discovery')
    expect(params!.get('per_page')).toBe('1')
    expect(params!.get('skip_total')).toBe('true')
    unmount()

    server.use(http.get(`${API_BASE}/api/cron-runs`, () => HttpResponse.json({ total: 0, page: 1, per_page: 1, entries: [] })))
    const { result: emptyRes, unmount: u2 } = renderHook(() => useLastSync(), { wrapper: syncWrapper({ staleLastSyncSeed: true }) })
    await waitFor(() => expect(emptyRes.current.data).toEqual({ started_at: null, status: null, duration_s: null }))
    u2()

    calls = 0
    mockServiceState.services = [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }]
    const { result: analystRes } = renderHook(() => useLastSync(), { wrapper: syncWrapper({ staleLastSyncSeed: true }) })
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(0)
    expect(analystRes.current.data).toBeNull()
  })

  it.each([
    { latest_log_at: null, local_rows: null, request: null, rum: null },
    { latest_log_at: null, local_rows: 0, request: { latest_log_at: null, total_rows: 0 }, rum: { latest_log_at: null, total_rows: 0 } },
  ])('useHeaderBadgeStream preserves explicit clears: %j', async (cleared) => {
    vi.stubGlobal('fetch', vi.fn(async () => makeStreamResponse([`data: ${JSON.stringify(cleared)}\r\n\r\n`])))
    const seed = {
      settings: { is_remote_analyst: true },
      header_badge: {
        latest_log_at: '2026-06-15T22:00:00Z', local_rows: 1000,
        request: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 900 },
        rum: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 100 },
      },
    }
    const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
    qc.setQueryData(['bootstrap'], seed)
    renderHook(() => useHeaderBadgeStream(true), { wrapper: makeQueryWrapper(qc) })
    await waitFor(() => expect(qc.getQueryData(['bootstrap'])).toEqual({ ...seed, header_badge: cleared }))
  })

  it('useHeaderBadgeStream merges sparse updates, respects enabled=false, uses /api/log-extents/stream, and aborts on unmount', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const qcOff = createTestQueryClient()
    qcOff.setQueryData(['bootstrap'], {})
    renderHook(() => useHeaderBadgeStream(false), { wrapper: makeQueryWrapper(qcOff) })
    await new Promise((r) => setTimeout(r, 20))
    expect(fetchMock).not.toHaveBeenCalled()

    const seed = {
      header_badge: {
        latest_log_at: '2026-06-15T22:00:00Z', local_rows: 1000,
        request: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 900 },
        rum: { latest_log_at: '2026-06-15T21:00:00Z', total_rows: 100 },
      },
    }
    const earlier = '2026-06-14T22:00:00Z'
    fetchMock.mockImplementationOnce(async () =>
      makeStreamResponse([`data: ${JSON.stringify({ latest_log_at: earlier, request: { latest_log_at: earlier } })}\r\n\r\n`]),
    )
    const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
    qc.setQueryData(['bootstrap'], seed)
    const { unmount } = renderHook(() => useHeaderBadgeStream(true), { wrapper: makeQueryWrapper(qc) })
    await waitFor(() => {
      expect(qc.getQueryData(['bootstrap'])).toEqual({
        header_badge: {
          ...seed.header_badge,
          latest_log_at: earlier,
          request: { ...seed.header_badge.request, latest_log_at: earlier },
        },
      })
    })
    expect(String(fetchMock.mock.calls[0][0])).toContain('/api/log-extents/stream')
    unmount()

    let abortSignal: AbortSignal | undefined
    fetchMock.mockImplementationOnce(async (_url: RequestInfo | URL, init?: RequestInit) => {
      abortSignal = init?.signal ?? undefined
      return makeNeverClosingStreamResponse()
    })
    const { unmount: u2 } = renderHook(() => useHeaderBadgeStream(true), { wrapper: makeQueryWrapper(qc) })
    await waitFor(() => expect(abortSignal).toBeDefined())
    expect(abortSignal?.aborted).toBe(false)
    act(() => u2())
    expect(abortSignal?.aborted).toBe(true)
  })
})

// ── useShareStatusBanner, useAnalystHeartbeat & useHighScaleRequestFacts ────

function bannerWrapper(seedBootstrap?: unknown) {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  if (seedBootstrap) qc.setQueryData(['bootstrap'], seedBootstrap)
  return makeQueryWrapper(qc)
}

describe('useShareStatusBanner', () => {
  beforeEach(() => {
    push.mockReset()
    mockServiceState.activeServiceId = 'svc-default'
    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  })

  it('seeds from bootstrap.share_banner without fetching, returns null when disabled, and polls on 15s cadence', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      let bannerHits = 0
      server.use(
        http.get(`${API_BASE}/api/admin/share/banner`, () => {
          bannerHits++
          return HttpResponse.json({ sharing_active: false, public_url: null })
        }),
      )
      const { result: offRes } = renderHook(() => useShareStatusBanner({ enabled: false }), { wrapper: bannerWrapper() })
      expect(offRes.current).toMatchObject({ sharingActive: false, node: null })

      const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), {
        wrapper: bannerWrapper({ share_banner: { sharing_active: true, public_url: 'https://example.test/x' } }),
      })
      expect(result.current.sharingActive).toBe(true)
      expect(result.current.node).not.toBeNull()
      expect(bannerHits).toBe(0)

      await act(async () => { await vi.advanceTimersByTimeAsync(15_100) })
      await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
      await waitFor(() => expect(result.current.sharingActive).toBe(false))
    } finally {
      vi.useRealTimers()
    }
  })

  it('defers fetching while tab is hidden, starts when visible, and aborts/stops on unmount', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: true, public_url: 'https://v.test' })
      }),
    )
    Object.defineProperty(document, 'hidden', { configurable: true, value: true })
    const { result, unmount } = renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper: bannerWrapper() })
    await act(async () => { await new Promise((r) => setTimeout(r, 30)) })
    expect(bannerHits).toBe(0)

    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
    await act(async () => { document.dispatchEvent(new Event('visibilitychange')) })
    await waitFor(() => expect(result.current.sharingActive).toBe(true))
    unmount()

    let aborted = false
    let started = false
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, async ({ request }) => {
        started = true
        request.signal.addEventListener('abort', () => { aborted = true })
        await new Promise((r) => setTimeout(r, 300))
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    const { unmount: u2 } = renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper: bannerWrapper() })
    await waitFor(() => expect(started).toBe(true))
    u2()
    await waitFor(() => expect(aborted).toBe(true))
  })
})

describe('useAnalystHeartbeat & useHighScaleRequestFacts', () => {
  const IDLE = 1_000
  const INTERVAL = 5_000

  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    __resetForTests(Date.now())
    replace.mockReset()
    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it.each(['mousemove', 'touchstart'] as const)('does not poll while user triggers %s inside idle window, or when enabled=false', async (evt) => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }))
    const { unmount } = renderHook(() => useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }))
    for (let i = 0; i < 2; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(INTERVAL - 1)
        window.dispatchEvent(new Event(evt))
        await vi.advanceTimersByTimeAsync(1)
      })
    }
    expect(fetchSpy).not.toHaveBeenCalled()
    unmount()

    renderHook(() => useAnalystHeartbeat({ enabled: false, idleAfterMs: IDLE, intervalMs: INTERVAL }))
    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 3)
      await vi.advanceTimersByTimeAsync(INTERVAL * 3)
    })
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('shows disconnect overlay after consecutive failures, clears on recovery, redirects on 401, and stops on unmount', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))
    fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }))

    const { result, unmount } = renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL, failuresBeforeOverlay: 2 }),
    )
    expect(result.current.disconnected).toBe(false)

    vi.setSystemTime(Date.now() + IDLE + 1)
    await act(async () => { await vi.advanceTimersByTimeAsync(INTERVAL) })
    expect(result.current.disconnected).toBe(false)

    vi.setSystemTime(Date.now() + IDLE + 1)
    await act(async () => { await vi.advanceTimersByTimeAsync(INTERVAL) })
    expect(result.current.disconnected).toBe(true)

    vi.setSystemTime(Date.now() + IDLE + 1)
    await act(async () => { await vi.advanceTimersByTimeAsync(INTERVAL) })
    expect(result.current.disconnected).toBe(false)

    vi.setSystemTime(Date.now() + IDLE + 1)
    await act(async () => { await vi.advanceTimersByTimeAsync(INTERVAL) })
    expect(replace).toHaveBeenCalledWith('/share-login')

    const callsBeforeUnmount = fetchSpy.mock.calls.length
    unmount()
    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 3)
      await vi.advanceTimersByTimeAsync(INTERVAL * 3)
    })
    expect(fetchSpy.mock.calls.length).toBe(callsBeforeUnmount)
  })

  it('useHighScaleRequestFacts advances and rewinds with server-issued cursors', async () => {
    vi.useRealTimers()
    const hsMetadata = {
      status: 'complete' as const, exact: true, coverage: 1, freshness_lag_seconds: 0,
      watermark: {
        service_id: 'service-1', domain: 'request', owner_epoch: 1,
        coverage_start: null, coverage_end: null, last_accepted_cursor: null,
        last_archived_event_id: null, last_visible_event_id: null, exact: true,
      },
      approximation_error: null, error: null,
    }
    const postSpy = vi.spyOn(client, 'POST')
      .mockResolvedValueOnce(postOk({ rows: [{ event_id: 'event-1', timestamp: '2026-09-11T00:00:01Z' }], metadata: hsMetadata, next_cursor: 'cursor-1', _is_cached: false }))
      .mockResolvedValueOnce(postOk({ rows: [{ event_id: 'event-2', timestamp: '2026-09-11T00:00:02Z' }], metadata: hsMetadata, next_cursor: null, _is_cached: false }))

    const { result } = renderHook(
      () => useHighScaleRequestFacts({ serviceId: 'service-1', startTime: '2026-09-11T00:00:00Z', endTime: '2026-09-11T01:00:00Z', pageSize: 1 }),
      { wrapper: makeQueryWrapper(createTestQueryClient()) },
    )
    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-1'))
    expect(result.current).toMatchObject({ pageIndex: 0, canNextPage: true })

    await act(async () => { result.current.nextPage() })
    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-2'))
    expect(result.current).toMatchObject({ pageIndex: 1, canPreviousPage: true })
    expect((postSpy.mock.calls[0]?.[1] as { body: { cursor: string | null } }).body.cursor).toBeNull()
    expect((postSpy.mock.calls[1]?.[1] as { body: { cursor: string | null } }).body.cursor).toBe('cursor-1')

    await act(async () => { result.current.previousPage() })
    expect(result.current.pageIndex).toBe(0)
  })
})
