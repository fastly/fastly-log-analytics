/**
 * Consolidated tests for useDashboardBundle, useSSE, useUrlServiceSync,
 * useDashboardCards, useFilterUrlWriteback, useFilterPayload, and useTimeRange.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper, makeStreamResponse } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { useFilterStore } from '@/stores/filterStore'
import {
  useDashboardCards,
  useFilterUrlWriteback,
  useFilterPayload,
  useTimeRange,
} from '@/hooks/useDashboardCards'
import type { FiltersPayload } from '@/types/api'

let currentPathname = '/dashboard'
vi.mock('next/navigation', () => ({
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

const { mockSetActiveServiceId, mockState } = vi.hoisted(() => {
  const mockSetActiveServiceId = vi.fn()
  const mockState = {
    activeServiceId: 'svc-test' as string | null,
    services: [] as Array<{ id: string; name: string }>,
    isInitialized: false,
    setActiveServiceId: mockSetActiveServiceId,
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  return { mockSetActiveServiceId, mockState }
})

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    { getState: () => mockState },
  )
  return { ...actual, useServiceStore, __state: mockState }
})

const useBootstrapMock = vi.fn()
const useLogFieldsCatalogMock = vi.fn()

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: () => useBootstrapMock(),
  useLogFieldsCatalog: () => useLogFieldsCatalogMock(),
}))

vi.mock('@/app/dashboard/_sections/DashboardClient', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/app/dashboard/_sections/DashboardClient')>()),
  CATEGORIZED_CARD_IDS: new Set([
    'status', 'method', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    'rid', 'prid', 'waf_sig', 'waf_req_id', 'ottfb', 'tcp_rtt',
  ]),
  CARD_CATEGORIES: [],
}))

const API_BASE = 'http://127.0.0.1:8000'

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
      setServices: vi.fn(),
      setInitialized: vi.fn(),
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
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE(opts?.maxLines ? { maxLines: opts.maxLines } : undefined))
    await act(async () => {
      await result.current.start('/api/some-stream', undefined, opts?.headers)
    })
    return result
  }

  it('starts in idle state with empty lines and resets cleanly after completion', async () => {
    const { useSSE } = await import('@/hooks/useSSE')
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
    const { useSSE } = await import('@/hooks/useSSE')
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
      setServices: vi.fn(),
      setInitialized: vi.fn(),
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
      const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
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
      const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
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
    useBootstrapMock.mockReset()
    useLogFieldsCatalogMock.mockReset()
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
