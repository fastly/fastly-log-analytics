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
import { hashKey, QueryClient } from '@tanstack/react-query'
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
  useUrlServiceSync,
} from '@/hooks/useBootstrap'
import {
  useAnalyticsReportQuery,
  useCardVisibility,
  useDashboardCards,
  useFilterPayload,
  useFilterUrlWriteback,
  useInsightsDefaults,
  useReportConfig,
  useTimeRange,
} from '@/hooks/useDashboardCards'
import {
  adminStreamHooks,
  EventStreamHttpError,
  runEventStream,
  useAdminEventStream,
  useEventStream,
  useHeaderBadgeStream,
  useLastSync,
  useServiceStream,
  useSSE,
  useSyncStatus,
  type EventStreamRequest,
} from '@/hooks/useSSE'
import { useShareStatusBanner } from '@/components/AppLayout'
import { useHighScaleRequestFacts } from '@/components/high-scale/HighScaleRequestFactsPanel'
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload } from '@/types/api'

const push = vi.fn()
const replace = vi.fn()
let currentPathname = '/dashboard'
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push, replace }),
  usePathname: () => currentPathname,
  useSearchParams: () => new URLSearchParams(),
}))

let mockUrlService: string | null = null
const mockSetUrlService = vi.fn((v: string | null) => {
  mockUrlService = v
})
vi.mock('nuqs', async (importOriginal) => ({
  ...(await importOriginal<typeof import('nuqs')>()),
  useQueryState: () => [mockUrlService, mockSetUrlService],
}))

let mockHydrated = true
let finishHydrationCb: (() => void) | null = null

const { mockSetServices, mockSetInitialized, mockSetActiveServiceId, mockServiceState } = vi.hoisted(() => {
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
  return { mockSetServices, mockSetInitialized, mockSetActiveServiceId, mockServiceState }
})

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockServiceState) => unknown) =>
      selector ? selector(mockServiceState) : mockServiceState,
    ),
    {
      getState: () => mockServiceState,
      setState: (patch: Partial<typeof mockServiceState>) => Object.assign(mockServiceState, patch),
      persist: {
        hasHydrated: () => mockHydrated,
        onFinishHydration: (cb: () => void) => {
          finishHydrationCb = cb
          return () => { finishHydrationCb = null }
        },
        rehydrate: () => {},
      },
    },
  )
  return { ...actual, useServiceStore, __state: mockServiceState }
})

let mockUseBootstrap = false
let mockUseBootstrapSettled = false
let bootstrapSettled = true
const useBootstrapMock = vi.fn()
const useLogFieldsCatalogMock = vi.fn()

vi.mock('@/hooks/useBootstrap', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useBootstrap')>()
  return {
    ...actual,
    useBootstrap: (...args: Parameters<typeof actual.useBootstrap>) =>
      // eslint-disable-next-line react-hooks/rules-of-hooks
      mockUseBootstrap ? useBootstrapMock(...args) : actual.useBootstrap(...args),
    useLogFieldsCatalog: (...args: Parameters<typeof actual.useLogFieldsCatalog>) =>
      // eslint-disable-next-line react-hooks/rules-of-hooks
      mockUseBootstrap ? useLogFieldsCatalogMock(...args) : actual.useLogFieldsCatalog(...args),
    useBootstrapSettled: (...args: Parameters<typeof actual.useBootstrapSettled>) =>
      mockUseBootstrapSettled ? bootstrapSettled : actual.useBootstrapSettled(...args),
  }
})

vi.mock('@/app/dashboard/_sections/DashboardClient', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/app/dashboard/_sections/DashboardClient')>()),
  CATEGORIZED_CARD_IDS: new Set([
    'status', 'method', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    'rid', 'prid', 'waf_sig', 'waf_req_id', 'ottfb', 'tcp_rtt',
  ]),
  CARD_CATEGORIES: [],
}))

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


// ============================================================================
// Merged from useDashboardBundle.test.ts
// ============================================================================
describe('useDashboardBundle, useSSE, useUrlServiceSync, useDashboardCards, useFilterUrlWriteback, useFilterPayload & useTimeRange', () => {
  const mockState = mockServiceState

  function makeClient() {
    return createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
  }

  function wrapperFor(qc: QueryClient) {
    return makeQueryWrapper(qc)
  }

  function findBundleQuery(qc: QueryClient) {
    return qc
      .getQueryCache()
      .getAll()
      .find(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard' && q.queryKey[1] === 'bundle')
  }

  const baseArgs = {
    startTime: '2026-06-15T00:00:00Z',
    endTime: '2026-06-15T01:00:00Z',
    relativeRange: null as string | null,
    isAutoRange: true,
    anchor: '2026-06-15T00:00:00Z',
    filterPayload: {} as FiltersPayload,
    metric: 'requests',
    interval: '1m',
    fields: ['status'],
  }

  describe('useDashboardBundle', () => {
    beforeEach(() => {
      vi.clearAllMocks()
      Object.assign(mockState, {
        activeServiceId: 'svc-test',
        services: [],
        isInitialized: false,
        setActiveServiceId: mockSetActiveServiceId,
        setServices: mockSetServices,
        setInitialized: mockSetInitialized,
      })
    })

    it('does not fire the POST while enabled=false', async () => {
      let hits = 0
      server.use(http.post(`${API_BASE}/api/dashboard/bundle`, () => { hits++; return HttpResponse.json({}) }))
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      renderHook(() => useDashboardBundle({ ...baseArgs, enabled: false }), { wrapper: wrapperFor(makeClient()) })
      await new Promise(r => setTimeout(r, 30))
      expect(hits).toBe(0)
    })

    it('fires the POST, resolves data, and does NOT fan out into separate cache entries', async () => {
      let hits = 0
      server.use(
        http.post(`${API_BASE}/api/dashboard/bundle`, () => {
          hits++
          return HttpResponse.json({
            aggregates: { data: { status: { total: 2, top: [{ key: '200', count: 1 }] } } },
            top_bots: [{ name: 'curl', count: 7 }],
          })
        }),
      )
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), { wrapper: wrapperFor(qc) })
      await waitFor(() => expect(result.current.data).toBeDefined())
      expect(hits).toBe(1)
      expect(result.current.data?.aggregates).toEqual({ data: { status: { total: 2, top: [{ key: '200', count: 1 }] } } })
      expect(result.current.data?.top_bots).toEqual([{ name: 'curl', count: 7 }])

      const dashboardEntries = qc.getQueryCache().getAll()
        .filter(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard')
        .map(q => (q.queryKey as unknown[])[1])
      expect(dashboardEntries).toEqual(['bundle'])
    })

    it('does not automatically refetch the live relative range', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        let hits = 0
        server.use(http.post(`${API_BASE}/api/dashboard/bundle`, () => { hits++; return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] }) }))
        const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
        renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), { wrapper: wrapperFor(makeClient()) })
        await waitFor(() => expect(hits).toBe(1))
        await vi.advanceTimersByTimeAsync(5_100)
        expect(hits).toBe(1)
      } finally {
        vi.useRealTimers()
      }
    })

    it('queryKey for the bundle is keyed on rangeKey + anchor (+ metric/interval/fields/sections)', async () => {
      server.use(http.post(`${API_BASE}/api/dashboard/bundle`, () => HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })))
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), { wrapper: wrapperFor(qc) })
      await waitFor(() => expect(result.current.data).toBeDefined())
      expect(findBundleQuery(qc)?.queryKey).toEqual([
        'dashboard', 'bundle', 'svc-test', '24h', baseArgs.anchor,
        baseArgs.filterPayload, baseArgs.metric, baseArgs.interval, baseArgs.fields, undefined,
      ])
    })

    it('custom absolute range → body carries start/end (no range_token), keyed on abs:<start>|<end>', async () => {
      let capturedBody: Record<string, unknown> = {}
      server.use(
        http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
          capturedBody = (await request.json()) as Record<string, unknown>
          return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
        }),
      )
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, isAutoRange: false, enabled: true }), { wrapper: wrapperFor(qc) })
      await waitFor(() => expect(result.current.data).toBeDefined())
      expect(capturedBody.start_time).toBe(baseArgs.startTime)
      expect(capturedBody.end_time).toBe(baseArgs.endTime)
      expect(capturedBody.range_token).toBeUndefined()
      expect(findBundleQuery(qc)?.queryKey[3]).toBe(`abs:${baseArgs.startTime}|${baseArgs.endTime}`)
    })

    it('forwards the sections selector through to the POST body and queryKey', async () => {
      let capturedBody: Record<string, unknown> = {}
      server.use(
        http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
          capturedBody = (await request.json()) as Record<string, unknown>
          return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
        }),
      )
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      const { result } = renderHook(
        () => useDashboardBundle({ ...baseArgs, enabled: true, sections: ['core', 'topten', 'bots'] }),
        { wrapper: wrapperFor(qc) },
      )
      await waitFor(() => expect(result.current.data).toBeDefined())
      expect(capturedBody.sections).toEqual(['core', 'topten', 'bots'])
      expect(findBundleQuery(qc)?.queryKey[9]).toEqual(['core', 'topten', 'bots'])
    })

    it('retries when the bundle returns a stale-view aggregates response', async () => {
      let attempts = 0
      server.use(
        http.post(`${API_BASE}/api/dashboard/bundle`, () => {
          attempts++
          return attempts === 1
            ? HttpResponse.json({ aggregates: { latest_log_at: '2026-06-15T00:00:00Z', data: {} } })
            : HttpResponse.json({ aggregates: { data: { status: { total: 1, top: [] } } }, top_bots: [] })
        }),
      )
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), { wrapper: wrapperFor(qc) })
      await waitFor(() => expect(result.current.data).toBeDefined(), { timeout: 5_000 })
      expect(attempts).toBeGreaterThanOrEqual(2)
      expect(result.current.data?.aggregates?.data.status.total).toBe(1)
    })
  })

  describe('useSSE', () => {
    beforeEach(() => { vi.stubGlobal('fetch', vi.fn()) })
    afterEach(() => { vi.unstubAllGlobals() })

    async function runStream(messages: string[], opts?: { maxLines?: number; headers?: Record<string, string> }) {
      vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))
      const { result } = renderHook(() => useSSE(opts?.maxLines ? { maxLines: opts.maxLines } : undefined))
      await act(async () => {
        await result.current.start('/api/some-stream', undefined, opts?.headers)
      })
      return result
    }

    it('starts in idle state with empty lines and resets cleanly after completion', async () => {
      const { result } = renderHook(() => useSSE())
      expect(result.current).toMatchObject({ status: 'idle', lines: [], error: null })

      vi.mocked(fetch).mockResolvedValue(makeStreamResponse(['data: {"type":"done"}\n\n']))
      await act(async () => { await result.current.start('/api/x') })
      expect(result.current.status).toBe('done')

      act(() => { result.current.reset() })
      expect(result.current).toMatchObject({ status: 'idle', lines: [], error: null })
    })

    it.each([
      ['LF boundaries', [
        'data: {"type":"status","message":"starting"}\n\n',
        'data: {"type":"file_done","file_name":"a.gz"}\n\n',
        'data: {"type":"done","message":"all good"}\n\n',
      ]],
      ['CRLF boundaries (sse-starlette wire format)', [
        'data: {"type":"status","message":"starting"}\r\n\r\n',
        'data: {"type":"file_done","file_name":"a.gz"}\r\n\r\n',
        'data: {"type":"done","message":"all good"}\r\n\r\n',
      ]],
    ])('captures data: events through to a terminal done event (%s)', async (_label, messages) => {
      const result = await runStream(messages)
      expect(result.current.status).toBe('done')
      expect(result.current.lines).toHaveLength(3)
      expect(result.current.lines[0]).toMatchObject({ type: 'status', message: 'starting' })
      expect(result.current.lines[1]).toMatchObject({ type: 'file_done', file_name: 'a.gz' })
      expect(result.current.lines[2]).toMatchObject({ type: 'done', message: 'all good' })
      expect(result.current.isDone).toBe(true)
    })

    it('keeps only the newest maxLines frames when a cap is given', async () => {
      const messages = [...[1, 2, 3, 4, 5].map(n => `data: {"type":"tick","n":${n}}\n\n`), 'data: {"type":"done"}\n\n']
      const result = await runStream(messages, { maxLines: 3 })
      expect(result.current.lines.map(l => l.n ?? l.type)).toEqual([4, 5, 'done'])
      expect(result.current.status).toBe('done')
    })

    it('merges caller-provided headers into the SSE request', async () => {
      await runStream(['data: {"type":"done"}\n\n'], { headers: { 'X-Page-Load-ID': 'page-123' } })
      expect(fetch).toHaveBeenCalledWith(
        expect.stringContaining('/api/some-stream'),
        expect.objectContaining({ headers: expect.objectContaining({ 'X-Page-Load-ID': 'page-123' }) }),
      )
    })

    it.each([
      ['ignore keepalive comments', [
        ':                              (keepalive)\n\n',
        'data: {"type":"status","message":"hello"}\n\n',
        ':                              (keepalive)\n\n',
        'data: {"type":"done"}\n\n',
      ], ['hello', undefined]],
      ['chunk split across read boundaries', [
        'data: {"type":"status","mes',
        'sage":"split"}\n\ndata: {"type":"done"}\n\n',
      ], ['split', undefined]],
      ['mixed CRLF and LF boundaries', [
        'data: {"type":"status","message":"crlf"}\r\n\r\ndata: {"type":"done"}\n\n',
      ], ['crlf', undefined]],
    ])('parses %s', async (_label, messages, expectedMessages) => {
      const result = await runStream(messages)
      expect(result.current.lines.map(l => l.message)).toEqual(expectedMessages)
      expect(result.current.status).toBe('done')
    })

    it.each([
      ['error event in stream', () => makeStreamResponse([
        'data: {"type":"status","message":"starting"}\n\n',
        'data: {"type":"error","message":"503 Service Unavailable"}\n\n',
      ]), '503 Service Unavailable'],
      ['unexpected EOF without done', () => makeStreamResponse([
        'data: {"type":"status","message":"starting"}\n\n',
      ]), 'Connection closed unexpectedly before completion.'],
      ['non-2xx HTTP status', () => new Response('nope', { status: 500 }), /500/],
      ['TypeError fetch failure', () => Promise.reject(new TypeError('Failed to fetch')), /Backend API unreachable/],
    ])('surfaces error on %s', async (_label, mockResp, expectedErr) => {
      vi.mocked(fetch).mockImplementation(() => Promise.resolve(mockResp() as Response))
      const { result } = renderHook(() => useSSE())
      await act(async () => { await result.current.start('/api/x') })
      expect(result.current.status).toBe('error')
      if (typeof expectedErr === 'string') {
        expect(result.current.error).toBe(expectedErr)
      } else {
        expect(result.current.error).toMatch(expectedErr)
      }
    })
  })

  describe('useUrlServiceSync', () => {
    beforeEach(() => {
      mockUrlService = null
      mockSetUrlService.mockReset()
      mockSetActiveServiceId.mockReset()
      Object.assign(mockState, {
        activeServiceId: null,
        services: [],
        isInitialized: false,
        setActiveServiceId: mockSetActiveServiceId,
        setServices: mockSetServices,
        setInitialized: mockSetInitialized,
      })
    })

    describe('URL → store', () => {
      it.each([
        ['writes ?service= URL param into the store on mount', null, 'svc-from-url', 'svc-from-url'],
        ['does nothing when there is no ?service= param', null, null, null],
        ['does not re-write when URL param matches store already', 'svc-1', 'svc-1', null],
      ])('%s', async (_label, storeSvc, urlSvc, expectedCall) => {
        mockState.activeServiceId = storeSvc
        mockUrlService = urlSvc
        renderHook(() => useUrlServiceSync())
        if (expectedCall) {
          expect(mockSetActiveServiceId).toHaveBeenCalledWith(expectedCall)
        } else {
          expect(mockSetActiveServiceId).not.toHaveBeenCalled()
        }
      })
    })

    describe('store → URL', () => {
      it.each([
        [
          'pushes activeServiceId into URL when it differs from current ?service',
          { activeServiceId: 'svc-new', services: [{ id: 'svc-new', name: 'New' }], isInitialized: true },
          null,
          'svc-new',
          null,
        ],
        [
          'strips ?service= from URL when services list is empty',
          { activeServiceId: 'svc-orphan', services: [], isInitialized: true },
          'svc-orphan',
          null,
          null,
        ],
        [
          'skips the store→URL sync until isInitialized is true',
          { activeServiceId: 'svc-1', services: [{ id: 'svc-1', name: 'S' }], isInitialized: false },
          null,
          undefined,
          null,
        ],
        [
          'does not re-write the URL when it already matches the store',
          { activeServiceId: 'svc-1', services: [{ id: 'svc-1', name: 'One' }], isInitialized: true },
          'svc-1',
          undefined,
          null,
        ],
        [
          'does NOT overwrite the URL when it names a different VALID service (prevents #185 swap loop)',
          { activeServiceId: 'svc-old', services: [{ id: 'svc-old', name: 'Old' }, { id: 'svc-new', name: 'New' }], isInitialized: true },
          'svc-new',
          undefined,
          'svc-new',
        ],
        [
          'DEFERS the stamp during a mid-switch (active id not yet in the list)',
          { activeServiceId: 'svc-new', services: [{ id: 'svc-old', name: 'Old' }], isInitialized: true },
          null,
          undefined,
          null,
        ],
        [
          'converges in a single stamp once the list lands and the URL is blank',
          { activeServiceId: 'svc-new', services: [{ id: 'svc-old', name: 'Old' }, { id: 'svc-new', name: 'New' }], isInitialized: true },
          null,
          'svc-new',
          null,
        ],
        [
          'still clears a STALE ?service that names no known service',
          { activeServiceId: 'svc-real', services: [{ id: 'svc-real', name: 'Real' }], isInitialized: true },
          'svc-dead',
          'svc-real',
          null,
        ],
      ])('%s', async (_label, statePatch, initialUrl, expectedUrlCall, expectedStoreCall) => {
        Object.assign(mockState, statePatch)
        mockUrlService = initialUrl
        const { rerender } = renderHook(() => useUrlServiceSync())
        act(() => { rerender() })
        if (expectedStoreCall) {
          expect(mockSetActiveServiceId).toHaveBeenCalledWith(expectedStoreCall)
        }
        if (expectedUrlCall === undefined) {
          expect(mockSetUrlService).not.toHaveBeenCalled()
        } else {
          expect(mockSetUrlService).toHaveBeenCalledWith(expectedUrlCall)
        }
      })
    })
  })

  function resetFilterStore() {
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

  describe('useDashboardCards', () => {
    beforeEach(() => {
      mockUseBootstrap = true
      useBootstrapMock.mockReset()
      useLogFieldsCatalogMock.mockReset()
    })

    afterEach(() => {
      mockUseBootstrap = false
    })

    it('returns [] until both catalog and bootstrap have data', () => {
      useBootstrapMock.mockReturnValue({ data: undefined })
      useLogFieldsCatalogMock.mockReturnValue({ data: undefined })
      const { result } = renderHook(() => useDashboardCards())
      expect(result.current).toEqual([])
    })

    it.each([
      [
        'excludes METRICS and INTERNAL groups',
        { active_log_field_ids: [] },
        [
          { id: 'status', label: 'Status', group: 'EDGE' },
          { id: 'm1', label: 'Metric', group: 'METRICS' },
          { id: 'i1', label: 'Internal', group: 'INTERNAL' },
        ],
        ['status'],
      ],
      [
        'excludes the hard-coded id list (timestamp, elapsed, req_bytes…)',
        { active_log_field_ids: [] },
        [
          { id: 'timestamp', label: 'T', group: 'EDGE' },
          { id: 'elapsed', label: 'E', group: 'EDGE' },
          { id: 'req_bytes', label: 'RB', group: 'EDGE' },
          { id: 'status', label: 'S', group: 'EDGE' },
        ],
        ['status'],
      ],
      [
        'force-hides noisy IDs (rid, prid, waf_sig, waf_req_id) even when active',
        { active_log_field_ids: ['rid', 'prid', 'waf_sig', 'waf_req_id', 'status'] },
        [
          { id: 'rid', label: 'RID', group: 'EDGE' },
          { id: 'prid', label: 'PRID', group: 'EDGE' },
          { id: 'waf_sig', label: 'Sig', group: 'EDGE' },
          { id: 'waf_req_id', label: 'WafR', group: 'EDGE' },
          { id: 'status', label: 'S', group: 'EDGE' },
        ],
        ['status'],
      ],
      [
        'appends custom_dashboard_cards and dedupes by id',
        {
          active_log_field_ids: [],
          custom_dashboard_cards: [
            { id: 'cf-1', label: 'Custom 1' },
            { id: 'status', label: 'dup-of-standard' },
          ],
        },
        [{ id: 'status', label: 'S', group: 'EDGE' }],
        ['status', 'cf-1'],
      ],
      [
        'filters out system custom fields, uncategorized built-in fields, non-dashboard user custom fields, and non-top-N metric fields',
        { active_log_field_ids: [], custom_dashboard_cards: [] },
        [
          { id: 'status', label: 'S', group: 'EDGE' },
          { id: 'ottfb', label: 'Origin TTFB', group: 'L' },
          { id: 'tcp_rtt', label: 'TCP RTT', group: 'F' },
          { id: 'io_input_bytes', label: 'IO Input', group: 'M' },
          { id: 'edge_score', label: 'Edge Score', group: 'custom', is_custom: true },
          { id: 'cmcd_version', label: 'CMCD Version', group: 'custom', is_custom: true },
          { id: 'rum_cid', label: 'RUM Session ID', group: 'custom', is_custom: true },
          { id: 'fastly_req_id', label: 'Fastly Req ID', group: 'custom', is_custom: true },
          { id: 'user_cf_1', label: 'My Custom Field', group: 'custom', is_custom: true, show_in_dashboard: true },
          { id: 'user_cf_2', label: 'Other Custom Field', group: 'custom', is_custom: true, show_in_dashboard: false },
        ],
        ['status', 'user_cf_1'],
      ],
    ])('%s', (_label, bootstrapData, fields, expectedIds) => {
      useBootstrapMock.mockReturnValue({ data: bootstrapData })
      useLogFieldsCatalogMock.mockReturnValue({ data: { fields } })
      const { result } = renderHook(() => useDashboardCards())
      expect(result.current.map(c => c.id)).toEqual(expectedIds)
    })

    it('marks all standard cards inActiveFormat=true when active_log_field_ids is empty and honours non-empty list + virtuals', () => {
      const fields = [
        { id: 'status', label: 'S', group: 'EDGE' },
        { id: 'method', label: 'M', group: 'EDGE' },
        { id: '_bot_name', label: 'Bot', group: 'EDGE' },
        { id: '_ngwaf_bot_name', label: 'NgBot', group: 'EDGE' },
        { id: 'waf_sig_ind', label: 'Sig', group: 'EDGE' },
      ]
      useLogFieldsCatalogMock.mockReturnValue({ data: { fields } })
      useBootstrapMock.mockReturnValue({ data: { active_log_field_ids: [], ngwaf_configured: true } })
      const { result, rerender } = renderHook(() => useDashboardCards())
      expect(result.current.every(c => c.inActiveFormat)).toBe(true)

      useBootstrapMock.mockReturnValue({ data: { active_log_field_ids: ['status'], ngwaf_configured: true } })
      rerender()
      const byId = Object.fromEntries(result.current.map(c => [c.id, c.inActiveFormat]))
      expect(byId).toEqual({
        status: true,
        method: false,
        _bot_name: true,
        _ngwaf_bot_name: true,
        waf_sig_ind: true,
      })
    })

    it.each([false, undefined])('excludes NGWAF cards when attachment is %s, even with WAF fields', (configured) => {
      useBootstrapMock.mockReturnValue({
        data: {
          ngwaf_configured: configured,
          active_log_field_ids: ['status', 'waf_req_id', 'waf_sig'],
          custom_dashboard_cards: [{ id: '_ngwaf_bot_name', label: 'Stale card' }],
        },
      })
      useLogFieldsCatalogMock.mockReturnValue({
        data: {
          fields: [
            { id: '_bot_name', label: 'Fastly Bots', group: 'VIRTUAL' },
            { id: '_ngwaf_bot_name', label: 'NGWAF Verified Bots', group: 'VIRTUAL' },
            { id: 'waf_sig_ind', label: 'NGWAF Signals', group: 'VIRTUAL' },
          ],
        },
      })
      const { result, rerender } = renderHook(() => useDashboardCards())
      expect(result.current.map(c => c.id)).toEqual(['_bot_name'])

      useBootstrapMock.mockReturnValue({ data: { ngwaf_configured: true, active_log_field_ids: [] } })
      rerender()
      expect(result.current.map(c => c.id)).toEqual(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'])

      useBootstrapMock.mockReturnValue({ data: { ngwaf_configured: false, active_log_field_ids: [] } })
      rerender()
      expect(result.current.map(c => c.id)).toEqual(['_bot_name'])
    })
  })

  describe('useFilterUrlWriteback, useFilterPayload & useTimeRange', () => {
    beforeEach(() => {
      currentPathname = '/dashboard'
      window.history.replaceState({}, '', '/dashboard')
      resetFilterStore()
    })

    it('re-hydrates filterStore on client-side pathname change (ingress)', () => {
      const { rerender } = renderHook(() => useFilterUrlWriteback())
      const filters = { country: { values: ['CA'], mode: 'include' } }
      window.history.replaceState({}, '', `/dashboard?filters=${encodeURIComponent(JSON.stringify(filters))}`)
      currentPathname = '/security'
      act(() => { rerender() })
      expect(useFilterStore.getState().filters).toEqual([
        expect.objectContaining({ column: 'country', value: 'CA', mode: 'include' }),
      ])
    })

    it('manages start_time, end_time, and filters URL params across range selection and resetAll', () => {
      renderHook(() => useFilterUrlWriteback())
      act(() => { useFilterStore.getState().toggleEdgeOnly() })
      let params = new URLSearchParams(window.location.search)
      expect(params.has('start_time')).toBe(false)
      expect(params.has('end_time')).toBe(false)

      act(() => {
        useFilterStore.getState().setRange('2026-06-09T17:36:00.000Z', '2026-06-10T17:36:00.000Z')
        useFilterStore.getState().addFilter('country', 'US', 'include')
      })
      params = new URLSearchParams(window.location.search)
      expect(params.get('start_time')).toBe('2026-06-09T17:36:00.000Z')
      expect(params.get('end_time')).toBe('2026-06-10T17:36:00.000Z')
      expect(params.has('filters')).toBe(true)

      act(() => { useFilterStore.getState().resetAll() })
      params = new URLSearchParams(window.location.search)
      expect(params.has('start_time')).toBe(false)
      expect(params.has('end_time')).toBe(false)
      expect(params.has('filters')).toBe(false)
    })

    it('useFilterPayload formats filter pills, handles includeEdgeOnly, and memoizes across renders', () => {
      const { result, rerender } = renderHook(({ includeEdge }) => useFilterPayload(includeEdge), {
        initialProps: { includeEdge: false },
      })
      expect(result.current).toEqual({})

      act(() => {
        useFilterStore.getState().addFilter('country', 'US', 'include')
        useFilterStore.getState().addFilter('status', '500', 'exclude')
        useFilterStore.setState({ edgeOnly: true })
      })
      expect(result.current.country).toEqual({ mode: 'include', values: ['US'] })
      const excludeKey = Object.keys(result.current).find(k => result.current[k].mode === 'exclude')
      expect(result.current[excludeKey!].values).toContain('500')
      expect(result.current).not.toHaveProperty('edge')

      const prevRef = result.current
      rerender({ includeEdge: false })
      expect(result.current).toBe(prevRef)

      rerender({ includeEdge: true })
      expect(result.current.edge).toEqual({ mode: 'include', values: ['true'] })

      act(() => { useFilterStore.setState({ edgeOnly: false }) })
      expect(result.current).not.toHaveProperty('edge')
    })

    it('useTimeRange returns and tracks the current time window from filterStore', () => {
      act(() => {
        useFilterStore.setState({
          startTime: '2026-07-01T00:00:00Z',
          endTime: '2026-07-02T00:00:00Z',
          compareMode: false,
          compareStartTime: null,
          compareEndTime: null,
        })
      })
      const { result } = renderHook(() => useTimeRange())
      expect(result.current).toMatchObject({
        startTime: '2026-07-01T00:00:00Z',
        endTime: '2026-07-02T00:00:00Z',
        compareMode: false,
        compareStartTime: null,
        compareEndTime: null,
      })

      act(() => {
        useFilterStore.setState({
          startTime: '2026-06-01T00:00:00Z',
          endTime: '2026-06-15T00:00:00Z',
          compareMode: true,
          compareStartTime: '2026-05-01T00:00:00Z',
          compareEndTime: '2026-05-15T00:00:00Z',
        })
      })
      expect(result.current).toMatchObject({
        startTime: '2026-06-01T00:00:00Z',
        compareMode: true,
        compareStartTime: '2026-05-01T00:00:00Z',
      })
    })
  })
})

// ============================================================================
// Merged from useSSE.test.ts
// ============================================================================
describe('runEventStream, useEventStream, useServiceStream & useAdminEventStream', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn())
    mockServiceState.activeServiceId = 'svc-1'
    mockHydrated = true
    finishHydrationCb = null
    bootstrapSettled = true
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  describe('runEventStream', () => {
    it('parses CRLF-separated frames (sse-starlette emits \\r\\n\\r\\n)', async () => {
      const frames: string[] = []
      const fetchImpl = vi
        .fn<typeof fetch>()
        .mockResolvedValue(makeStreamResponse(['data: a\r\n\r\n', 'data: b\r\n\r\n']))
      await runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onFrame: (f) => frames.push(f),
        fetchImpl: fetchImpl,
      })
      expect(frames).toEqual(['data: a', 'data: b'])
    })

    it('parses LF and old-Mac boundaries too', async () => {
      const frames: string[] = []
      const fetchImpl = vi
        .fn<typeof fetch>()
        .mockResolvedValue(makeStreamResponse(['data: lf\n\n', 'data: mac\r\r']))
      await runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onFrame: (f) => frames.push(f),
        fetchImpl: fetchImpl,
      })
      expect(frames).toEqual(['data: lf', 'data: mac'])
    })

    it('reassembles a frame split across chunks', async () => {
      const frames: string[] = []
      const fetchImpl = vi
        .fn<typeof fetch>()
        .mockResolvedValue(makeStreamResponse(['data: hel', 'lo\r\n\r\n']))
      await runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onFrame: (f) => frames.push(f),
        fetchImpl: fetchImpl,
      })
      expect(frames).toEqual(['data: hello'])
    })

    it('throws EventStreamHttpError with the status on a non-2xx response', async () => {
      const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 503 }))
      await expect(
        runEventStream({
          url: 'http://test/stream',
          signal: new AbortController().signal,
          onFrame: () => {},
          fetchImpl: fetchImpl,
        }),
      ).rejects.toMatchObject({ status: 503 })
    })

    it('preserves the historical useSSE error message text', () => {
      expect(new EventStreamHttpError(500).message).toBe('HTTP error! status: 500')
    })

    it('stops dispatching frames once isActive() goes false', async () => {
      const frames: string[] = []
      let live = true
      const fetchImpl = vi
        .fn<typeof fetch>()
        .mockResolvedValue(makeStreamResponse(['data: first\r\n\r\n', 'data: second\r\n\r\n']))
      await runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onFrame: (f) => {
          frames.push(f)
          live = false
        },
        isActive: () => live,
        fetchImpl: fetchImpl,
      })
      expect(frames).toEqual(['data: first'])
    })

    it('fires onOpen after the reader is obtained', async () => {
      const onOpen = vi.fn()
      const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(makeStreamResponse(['data: x\r\n\r\n']))
      await runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onOpen,
        onFrame: () => {},
        fetchImpl: fetchImpl,
      })
      expect(onOpen).toHaveBeenCalledTimes(1)
    })
  })

  describe('useEventStream', () => {
    const req: EventStreamRequest = { url: 'http://test/stream' }

    it('does not fetch while disabled', async () => {
      const f = vi.fn()
      vi.stubGlobal('fetch', f)
      renderHook(() => useEventStream(false, req, () => {}))
      await new Promise((r) => setTimeout(r, 20))
      expect(f).not.toHaveBeenCalled()
    })

    it('delivers each data payload (trimmed, data: stripped) to onEvent', async () => {
      vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue(makeStreamResponse(['data: hello\r\n\r\n'])),
      )
      const onEvent = vi.fn()
      renderHook(() => useEventStream(true, req, onEvent))
      await waitFor(() => expect(onEvent).toHaveBeenCalledWith('hello'))
    })

    it('reaches open state on a successful connect', async () => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeNeverClosingStreamResponse(['data: x\r\n\r\n'])))
      const { result } = renderHook(() => useEventStream(true, req, () => {}))
      await waitFor(() => expect(result.current.state).toBe('open'))
    })

    it('does NOT reconnect on 401/403 (terminal auth failure)', async () => {
      const f = vi.fn().mockResolvedValue(new Response(null, { status: 403 }))
      vi.stubGlobal('fetch', f)
      renderHook(() => useEventStream(true, req, () => {}))
      await new Promise((r) => setTimeout(r, 50))
      expect(f).toHaveBeenCalledTimes(1)
    })

    it('reconnects with exponential backoff after a dropped connection', async () => {
      vi.useFakeTimers()
      const f = vi.fn().mockImplementation(() => Promise.resolve(new Response(null, { status: 500 })))
      vi.stubGlobal('fetch', f)
      renderHook(() => useEventStream(true, req, () => {}))

      await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(1))
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1000)
      })
      await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(2))
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1000)
      })
      expect(f).toHaveBeenCalledTimes(2)
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1000)
      })
      await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(3))
    })

    it('aborts the in-flight connection on unmount', async () => {
      let capturedSignal: AbortSignal | undefined
      vi.stubGlobal(
        'fetch',
        vi.fn().mockImplementation((_url: string, init: RequestInit) => {
          capturedSignal = init.signal as AbortSignal
          return Promise.resolve(makeStreamResponse(['data: x\r\n\r\n']))
        }),
      )
      const { unmount } = renderHook(() => useEventStream(true, req, () => {}))
      await waitFor(() => expect(capturedSignal).toBeDefined())
      unmount()
      expect(capturedSignal?.aborted).toBe(true)
    })
  })

  describe('useServiceStream', () => {
    it('short-circuits when enabled=false (no fetch fired)', async () => {
      renderHook(() => useServiceStream(false, '/api/x', () => {}))
      await new Promise(r => setTimeout(r, 20))
      expect(fetch).not.toHaveBeenCalled()
    })

    it('short-circuits when activeServiceId is null (no fetch fired)', async () => {
      mockServiceState.activeServiceId = null
      renderHook(() => useServiceStream(true, '/api/x', () => {}))
      await new Promise(r => setTimeout(r, 20))
      expect(fetch).not.toHaveBeenCalled()
    })

    it('parses LF (\\n\\n) event boundaries', async () => {
      vi.mocked(fetch).mockImplementation(async () =>
        makeStreamResponse(['data: alpha\n\ndata: beta\n\n']),
      )
      const events: string[] = []
      renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
      await waitFor(() => expect(events).toEqual(['alpha', 'beta']))
    })

    it('parses old-Mac (\\r\\r) event boundaries', async () => {
      vi.mocked(fetch).mockImplementation(async () =>
        makeStreamResponse(['data: one\r\rdata: two\r\r']),
      )
      const events: string[] = []
      renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
      await waitFor(() => expect(events).toEqual(['one', 'two']))
    })

    it('aborts the in-flight stream on unmount', async () => {
      let signal: AbortSignal | undefined
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        signal = init?.signal as AbortSignal
        return makeNeverClosingStreamResponse()
      })
      const { unmount } = renderHook(() => useServiceStream(true, '/api/x', () => {}))

      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(signal?.aborted).toBe(false)
      act(() => unmount())
      expect(signal?.aborted).toBe(true)
    })

    it('aborts the prior stream and opens a new one when activeServiceId changes', async () => {
      const signals: AbortSignal[] = []
      const ids: string[] = []
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        const ri = init ?? {}
        signals.push(ri.signal as AbortSignal)
        ids.push((ri.headers as Record<string, string>)['x-service-id'])
        return makeNeverClosingStreamResponse()
      })
      const { rerender } = renderHook(() => useServiceStream(true, '/api/x', () => {}))

      await waitFor(() => expect(signals.length).toBe(1))
      expect(signals[0].aborted).toBe(false)

      mockServiceState.activeServiceId = 'svc-2'
      rerender()

      await waitFor(() => expect(signals.length).toBe(2))
      expect(signals[0].aborted).toBe(true)
      expect(signals[1].aborted).toBe(false)
      expect(ids).toEqual(['svc-1', 'svc-2'])
    })

    it('exponential backoff: roughly doubles per failure, capped at 30s', async () => {
      vi.useFakeTimers()
      let calls = 0
      vi.mocked(fetch).mockImplementation(async () => {
        calls += 1
        return new Response('nope', { status: 500 })
      })
      renderHook(() => useServiceStream(true, '/api/x', () => {}))

      await act(async () => { await vi.advanceTimersByTimeAsync(0) })
      expect(calls).toBe(1)

      await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
      expect(calls).toBeGreaterThanOrEqual(2)
      const afterFirstBackoff = calls

      await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
      expect(calls).toBeGreaterThan(afterFirstBackoff)
      const afterSecondBackoff = calls

      await act(async () => { await vi.advanceTimersByTimeAsync(2_500) })
      expect(calls).toBe(afterSecondBackoff)
      await act(async () => { await vi.advanceTimersByTimeAsync(1_600) })
      expect(calls).toBeGreaterThan(afterSecondBackoff)

      await act(async () => { await vi.advanceTimersByTimeAsync(8_500) })
      await act(async () => { await vi.advanceTimersByTimeAsync(16_500) })
      const callsBeforeCap = calls
      await act(async () => { await vi.advanceTimersByTimeAsync(31_000) })
      expect(calls).toBeGreaterThan(callsBeforeCap)
    })

    describe('optionalService', () => {
      it('connects with NO x-service-id when activeServiceId is null', async () => {
        mockServiceState.activeServiceId = null
        let headers: Record<string, string> | undefined
        vi.mocked(fetch).mockImplementation(async (_url, init) => {
          headers = init?.headers as Record<string, string>
          return makeNeverClosingStreamResponse()
        })
        renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

        await waitFor(() => expect(fetch).toHaveBeenCalled())
        expect(headers && 'x-service-id' in headers).toBe(false)
      })

      it('sends x-service-id when a service IS selected', async () => {
        mockServiceState.activeServiceId = 'svc-1'
        let headers: Record<string, string> | undefined
        vi.mocked(fetch).mockImplementation(async (_url, init) => {
          headers = init?.headers as Record<string, string>
          return makeNeverClosingStreamResponse()
        })
        renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

        await waitFor(() => expect(fetch).toHaveBeenCalled())
        expect(headers?.['x-service-id']).toBe('svc-1')
      })

      it('reconnects WITH the header when a service is added (null → svc-1)', async () => {
        mockServiceState.activeServiceId = null
        const signals: AbortSignal[] = []
        const ids: (string | undefined)[] = []
        vi.mocked(fetch).mockImplementation(async (_url, init) => {
          const ri = init ?? {}
          signals.push(ri.signal as AbortSignal)
          ids.push((ri.headers as Record<string, string>)['x-service-id'])
          return makeNeverClosingStreamResponse()
        })
        const { rerender } = renderHook(() =>
          useServiceStream(true, '/api/x', () => {}, { optionalService: true }),
        )

        await waitFor(() => expect(signals.length).toBe(1))
        expect(signals[0].aborted).toBe(false)

        mockServiceState.activeServiceId = 'svc-1'
        rerender()

        await waitFor(() => expect(signals.length).toBe(2))
        expect(signals[0].aborted).toBe(true)
        expect(signals[1].aborted).toBe(false)
        expect(ids).toEqual([undefined, 'svc-1'])
      })

      it('waits for the persisted store to rehydrate, then connects ONCE already scoped', async () => {
        mockHydrated = false
        mockServiceState.activeServiceId = null
        const ids: (string | undefined)[] = []
        vi.mocked(fetch).mockImplementation(async (_url, init) => {
          const ri = init ?? {}
          ids.push((ri.headers as Record<string, string>)['x-service-id'])
          return makeNeverClosingStreamResponse()
        })
        renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

        await new Promise(r => setTimeout(r, 20))
        expect(fetch).not.toHaveBeenCalled()

        mockHydrated = true
        mockServiceState.activeServiceId = 'svc-1'
        act(() => { finishHydrationCb?.() })

        await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
        expect(ids).toEqual(['svc-1'])
      })
    })

    it('a successful reconnect resets the backoff counter', async () => {
      vi.useFakeTimers()
      let calls = 0
      vi.mocked(fetch).mockImplementation(async () => {
        calls += 1
        if (calls < 3) return new Response('x', { status: 500 })
        if (calls === 3) return makeStreamResponse(['data: ok\n\n'])
        return new Response('x', { status: 500 })
      })

      renderHook(() => useServiceStream(true, '/api/x', () => {}))

      await act(async () => { await vi.advanceTimersByTimeAsync(0) })
      await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
      await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
      expect(calls).toBe(3)

      await act(async () => { await vi.advanceTimersByTimeAsync(1_500) })
      expect(calls).toBe(4)
    })
  })

  function makeQc() {
    return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
  }

  describe('useAdminEventStream', () => {
    let capturedOnEvent: ((raw: string) => void) | null = null
    let lastEnabled: boolean | undefined
    let lastPath: string | undefined
    let lastOpts: Record<string, unknown> | undefined
    let cronApply: ReturnType<typeof vi.fn<(data: unknown) => void>>
    let cronCleanup: ReturnType<typeof vi.fn<() => void>>

    beforeEach(() => {
      mockUseBootstrapSettled = true
      capturedOnEvent = null
      lastEnabled = undefined
      lastPath = undefined
      lastOpts = undefined
      bootstrapSettled = true
      mockServiceState.activeServiceId = 'svc-1'
      cronApply = vi.fn<(data: unknown) => void>()
      cronCleanup = vi.fn<() => void>()
      vi.spyOn(adminStreamHooks, 'applyShare').mockImplementation(() => {})
      vi.spyOn(adminStreamHooks, 'applySyncStatus').mockImplementation(() => {})
      vi.spyOn(adminStreamHooks, 'applySystemMetrics').mockImplementation(() => {})
      vi.spyOn(adminStreamHooks, 'makeCronRunsApplier').mockImplementation(() => ({
        apply: cronApply,
        cleanup: cronCleanup,
      }))
      vi.spyOn(adminStreamHooks, 'useServiceStream').mockImplementation(
        (enabled, path, onEvent, opts) => {
          lastEnabled = enabled
          lastPath = path
          capturedOnEvent = onEvent
          lastOpts = opts as Record<string, unknown> | undefined
          return { state: 'open' as const }
        },
      )
    })

    afterEach(() => {
      mockUseBootstrapSettled = false
    })

    it('connects to /api/admin/events/stream with sorted channels + optionalService opts', () => {
      renderHook(() => useAdminEventStream(true, ['sync-status', 'cron-runs', 'system-metrics']), {
        wrapper: makeQueryWrapper(makeQc()),
      })
      expect(lastEnabled).toBe(true)
      expect(lastPath).toBe('/api/admin/events/stream?channels=cron-runs,sync-status,system-metrics')
      expect(lastOpts).toMatchObject({ optionalService: true, cache: 'no-store' })
    })

    it('holds the stream disabled until bootstrap settles', () => {
      bootstrapSettled = false
      renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: makeQueryWrapper(makeQc()) })
      expect(lastEnabled).toBe(false)
    })

    it('forwards enabled=false', () => {
      renderHook(() => useAdminEventStream(false, ['sync-status']), { wrapper: makeQueryWrapper(makeQc()) })
      expect(lastEnabled).toBe(false)
    })

    it('demuxes sync-status envelope to applySyncStatus(qc, serviceId, data)', () => {
      renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: makeQueryWrapper(makeQc()) })
      const data = { local_rows: 5 }
      act(() => capturedOnEvent!(JSON.stringify({ channel: 'sync-status', data })))
      expect(adminStreamHooks.applySyncStatus).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
    })

    it('demuxes system-metrics envelope to applySystemMetrics(qc, serviceId, data)', () => {
      renderHook(() => useAdminEventStream(true, ['system-metrics']), { wrapper: makeQueryWrapper(makeQc()) })
      const data = { health_snapshot: { status: 'ok' } }
      act(() => capturedOnEvent!(JSON.stringify({ channel: 'system-metrics', data })))
      expect(adminStreamHooks.applySystemMetrics).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
    })

    it('demuxes cron-runs envelope to the cron applier', () => {
      renderHook(() => useAdminEventStream(true, ['cron-runs']), { wrapper: makeQueryWrapper(makeQc()) })
      const data = { task: 'sync', status: 'success' }
      act(() => capturedOnEvent!(JSON.stringify({ channel: 'cron-runs', data })))
      expect(cronApply).toHaveBeenCalledWith(data)
    })

    it('demuxes share envelope to applyShare(qc, data)', () => {
      renderHook(() => useAdminEventStream(true, ['share']), { wrapper: makeQueryWrapper(makeQc()) })
      const data = { sharing_active: true, active_session_count: 1 }
      act(() => capturedOnEvent!(JSON.stringify({ channel: 'share', data })))
      expect(adminStreamHooks.applyShare).toHaveBeenCalledWith(expect.anything(), data)
    })

    it('ignores malformed envelopes and unknown channels without throwing', () => {
      renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: makeQueryWrapper(makeQc()) })
      expect(() => act(() => capturedOnEvent!('not-json'))).not.toThrow()
      expect(() => act(() => capturedOnEvent!(JSON.stringify({ channel: 'mystery', data: {} })))).not.toThrow()
      expect(adminStreamHooks.applySyncStatus).not.toHaveBeenCalled()
      expect(adminStreamHooks.applySystemMetrics).not.toHaveBeenCalled()
      expect(cronApply).not.toHaveBeenCalled()
    })
  })
})
