/**
 * useDashboardBundle is the cold-load round-trip saver: one POST to
 * /api/dashboard/bundle that returns aggregates + top_bots together in
 * a single response. The dashboard page reads BOTH directly off
 * bundleQuery.data (no separate cache keys), and the bundle is keyed on
 * the server-reproducible (rangeToken, anchor) pair.
 *
 * Tests assert (1) the bundle's queryKey shape (keyed on rangeToken/
 * anchor), (2) the enabled flag wires through, (3) it does NOT fan out
 * into separate aggregates/top-bots cache entries, (4) the sections
 * selector forwards to body + key, (5) the stale-view retry path.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper, makeStreamResponse } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import type { FiltersPayload } from '@/types/api'

let mockUrlService: string | null = null
const mockSetUrlService = vi.fn((v: string | null) => {
  mockUrlService = v
})
vi.mock('nuqs', async (importOriginal) => ({
  ...(await importOriginal<typeof import('nuqs')>()),
  useQueryState: () => [mockUrlService, mockSetUrlService],
}))

const mockSetActiveServiceId = vi.fn()
let mockState = {
  activeServiceId: 'svc-test' as string | null,
  services: [] as Array<{ id: string; name: string }>,
  isInitialized: false,
  setActiveServiceId: mockSetActiveServiceId,
  setServices: vi.fn(),
  setInitialized: vi.fn(),
}

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    { getState: () => mockState },
  )
  return { ...actual, useServiceStore, __state: mockState }
})

const API_BASE = 'http://127.0.0.1:8000'

function makeClient() {
  // Keep a small non-zero gcTime so cache entries survive between the
  // fetch and the assertion that reads them back (gcTime=0 collects
  // observer-less entries on the next tick). 5s is plenty for tests.
  return createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
}

function wrapperFor(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

const baseArgs = {
  startTime: '2026-06-15T00:00:00Z',
  endTime: '2026-06-15T01:00:00Z',
  // Cold-load / auto default: relativeRange null + isAutoRange true →
  // resolveRangeWire yields the server-reproducible '24h' token (the SSR-seed
  // contract). startTime/endTime stay as the stale-view extents context.
  relativeRange: null as string | null,
  isAutoRange: true,
  anchor: '2026-06-15T00:00:00Z',
  // Realistic empty payload: buildFiltersPayload returns {} (column-keyed)
  // when no filters are active. The stale-view discriminator keys off
  // Object.keys(filterPayload).length, so an empty {} means "no filter".
  filterPayload: {} as FiltersPayload,
  metric: 'requests',
  interval: '1m',
  fields: ['status'],
}

describe('useDashboardBundle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockState = {
      activeServiceId: 'svc-test',
      services: [],
      isInitialized: false,
      setActiveServiceId: mockSetActiveServiceId,
      setServices: vi.fn(),
      setInitialized: vi.fn(),
    }
  })

  it('does not fire the POST while enabled=false', async () => {
    let hits = 0
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () => {
        hits++
        return HttpResponse.json({})
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    renderHook(() => useDashboardBundle({ ...baseArgs, enabled: false }), {
      wrapper: wrapperFor(qc),
    })
    await new Promise(r => setTimeout(r, 30))
    expect(hits).toBe(0)
  })

  it('fires the POST and resolves data when enabled=true', async () => {
    let hits = 0
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () => {
        hits++
        return HttpResponse.json({
          aggregates: { data: { status: { total: 1, top: [{ key: '200', count: 1 }] } } },
          top_bots: [{ name: 'b', count: 1 }],
        })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined())
    expect(hits).toBe(1)
    expect(result.current.data?.aggregates).toBeDefined()
    expect(result.current.data?.top_bots).toEqual([{ name: 'b', count: 1 }])
  })

  it('does not automatically refetch the live relative range', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      let hits = 0
      server.use(
        http.post(`${API_BASE}/api/dashboard/bundle`, () => {
          hits++
          return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
        }),
      )
      const qc = makeClient()
      const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
      renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
        wrapper: wrapperFor(qc),
      })
      await waitFor(() => expect(hits).toBe(1))
      await vi.advanceTimersByTimeAsync(5_100)
      expect(hits).toBe(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('does NOT fan out into separate aggregates/top-bots cache entries', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () =>
        HttpResponse.json({
          aggregates: { data: { status: { total: 2, top: [] } } },
          top_bots: [{ name: 'curl', count: 7 }],
        }),
      ),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined())

    // The page reads aggregates + top_bots straight off bundleQuery.data, so the
    // hook seeds NO dedicated 'aggregates'/'top-bots' keys (that setQueryData
    // fan-out was removed with the SSR re-key). Only the 'bundle' entry exists.
    const dashboardEntries = qc
      .getQueryCache()
      .getAll()
      .filter(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard')
      .map(q => (q.queryKey as unknown[])[1])
    expect(dashboardEntries).toEqual(['bundle'])

    // …and the merged payload is fully readable off the bundle result.
    expect(result.current.data?.aggregates).toEqual({ data: { status: { total: 2, top: [] } } })
    expect(result.current.data?.top_bots).toEqual([{ name: 'curl', count: 7 }])
  })

  it('queryKey for the bundle is keyed on rangeKey + anchor (+ metric/interval/fields/sections)', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () =>
        HttpResponse.json({ aggregates: { data: {} }, top_bots: [] }),
      ),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined())

    const bundleEntry = qc
      .getQueryCache()
      .getAll()
      .find(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard' && q.queryKey[1] === 'bundle')
    expect(bundleEntry).toBeDefined()
    expect(bundleEntry!.queryKey).toEqual([
      'dashboard',
      'bundle',
      'svc-test',
      '24h', // resolveRangeWire token for the cold-load default (relativeRange null + auto)
      baseArgs.anchor,
      baseArgs.filterPayload,
      baseArgs.metric,
      baseArgs.interval,
      baseArgs.fields,
      undefined,
    ])
  })

  it('custom absolute range → body carries start/end (no range_token), keyed on abs:<start>|<end>', async () => {
    let capturedBody = null as Record<string, unknown> | null
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
        capturedBody = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    // relativeRange null + isAutoRange false = the user picked an explicit
    // absolute range (date picker / chart zoom / saved view).
    const { result } = renderHook(
      () => useDashboardBundle({ ...baseArgs, isAutoRange: false, enabled: true }),
      { wrapper: wrapperFor(qc) },
    )
    await waitFor(() => expect(result.current.data).toBeDefined())

    // The scan window comes from the explicit bounds, not a token.
    expect(capturedBody?.start_time).toBe(baseArgs.startTime)
    expect(capturedBody?.end_time).toBe(baseArgs.endTime)
    expect(capturedBody?.range_token).toBeUndefined()

    const bundleEntry = qc
      .getQueryCache()
      .getAll()
      .find(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard' && q.queryKey[1] === 'bundle')
    expect(bundleEntry!.queryKey[3]).toBe(`abs:${baseArgs.startTime}|${baseArgs.endTime}`)
  })

  it('forwards the sections selector through to the POST body and queryKey', async () => {
    let capturedBody = null as Record<string, unknown> | null
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
        capturedBody = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    const sections = ['core', 'topten', 'bots'] as const
    const { result } = renderHook(
      () => useDashboardBundle({ ...baseArgs, enabled: true, sections: [...sections] }),
      { wrapper: wrapperFor(qc) },
    )
    await waitFor(() => expect(result.current.data).toBeDefined())

    expect(capturedBody?.sections).toEqual(['core', 'topten', 'bots'])
    const bundleEntry = qc
      .getQueryCache()
      .getAll()
      .find(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard' && q.queryKey[1] === 'bundle')
    expect(bundleEntry).toBeDefined()
    expect(bundleEntry!.queryKey[9]).toEqual(['core', 'topten', 'bots'])
  })

  it('retries when the bundle returns a stale-view aggregates response', async () => {
    let attempts = 0
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () => {
        attempts++
        if (attempts === 1) {
          // Stale-view shape: latest_log_at set + empty data + no time_series.
          return HttpResponse.json({
            aggregates: { latest_log_at: '2026-06-15T00:00:00Z', data: {} },
          })
        }
        return HttpResponse.json({
          aggregates: { data: { status: { total: 1, top: [] } } },
          top_bots: [],
        })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/app/dashboard/_sections/DashboardClient')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined(), { timeout: 5_000 })
    expect(attempts).toBeGreaterThanOrEqual(2)
    expect(result.current.data?.aggregates?.data.status.total).toBe(1)
  })
})

describe('useSSE', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn())
  })
  afterEach(() => {
    vi.unstubAllGlobals()
  })
  it('starts in idle state with empty lines', async () => {
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())
    expect(result.current.status).toBe('idle')
    expect(result.current.lines).toEqual([])
    expect(result.current.error).toBeNull()
  })

  it('captures data: events through to a terminal done event', async () => {
    const messages = [
      'data: {"type":"status","message":"starting"}\n\n',
      'data: {"type":"file_done","file_name":"a.gz"}\n\n',
      'data: {"type":"done","message":"all good"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/some-stream')
    })

    expect(result.current.status).toBe('done')
    expect(result.current.lines).toHaveLength(3)
    expect(result.current.lines[0]).toMatchObject({ type: 'status', message: 'starting' })
    expect(result.current.lines[2]).toMatchObject({ type: 'done' })
    expect(result.current.isDone).toBe(true)
  })

  it('keeps only the newest maxLines frames when a cap is given', async () => {
    const messages = [1, 2, 3, 4, 5].map((n) => `data: {"type":"tick","n":${n}}\n\n`)
    messages.push('data: {"type":"done"}\n\n')
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE({ maxLines: 3 }))

    await act(async () => {
      await result.current.start('/api/some-stream')
    })

    expect(result.current.lines).toHaveLength(3)
    expect(result.current.lines.map((l) => l.n ?? l.type)).toEqual([4, 5, 'done'])
    expect(result.current.status).toBe('done')
  })

  it('merges caller-provided headers into the SSE request', async () => {
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(['data: {"type":"done"}\n\n']))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/realtime-stream', undefined, { 'X-Page-Load-ID': 'page-123' })
    })

    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/realtime-stream'),
      expect.objectContaining({
        headers: expect.objectContaining({ 'X-Page-Load-ID': 'page-123' }),
      }),
    )
  })

  it('captures an error event and stores the message', async () => {
    const messages = [
      'data: {"type":"status","message":"starting"}\n\n',
      'data: {"type":"error","message":"503 Service Unavailable"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toBe('503 Service Unavailable')
  })

  it('ignores `:` keepalive comment lines', async () => {
    const messages = [
      ':                              (keepalive)\n\n',
      'data: {"type":"status","message":"hello"}\n\n',
      ':                              (keepalive)\n\n',
      'data: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    // Only data: lines become entries — comments are silently dropped
    expect(result.current.lines).toHaveLength(2)
    expect(result.current.lines[0].message).toBe('hello')
  })

  it('handles a chunk that splits across read boundaries', async () => {
    // First chunk only partially contains the first message; second has the rest.
    const messages = [
      'data: {"type":"status","mes',
      'sage":"split"}\n\ndata: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.lines.map(l => l.message)).toEqual(['split', undefined])
    expect(result.current.status).toBe('done')
  })

  it('parses CRLF event boundaries (sse-starlette wire format)', async () => {
    // sse-starlette flushes events as `data: ...\r\n\r\n` (RFC-spec
    // CRLF), not `\n\n`. Pre-fix, useSSE split on `\n\n` only — every
    // chunk arrived fine, but the buffer accumulated forever and no
    // `data:` line was ever extracted. Symptom: every consumer stuck
    // on "Waiting for stream..." despite a healthy 200 response.
    // Pinning the CRLF format here means a regression to LF-only
    // splitting fails this test even before anyone touches the
    // backend.
    const messages = [
      'data: {"type":"status","message":"starting"}\r\n\r\n',
      'data: {"type":"file_done","file_name":"a.gz"}\r\n\r\n',
      'data: {"type":"done","message":"ok"}\r\n\r\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/some-stream')
    })

    expect(result.current.status).toBe('done')
    expect(result.current.lines).toHaveLength(3)
    expect(result.current.lines[0]).toMatchObject({ type: 'status', message: 'starting' })
    expect(result.current.lines[1]).toMatchObject({ type: 'file_done', file_name: 'a.gz' })
    expect(result.current.lines[2]).toMatchObject({ type: 'done', message: 'ok' })
  })

  it('parses a buffer that mixes CRLF and LF event boundaries', async () => {
    // Defensive: if a proxy or intermediary normalises CRLF to LF
    // mid-stream, the parser should still cleanly split both halves.
    const messages = [
      'data: {"type":"status","message":"crlf"}\r\n\r\ndata: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.lines.map(l => l.message)).toEqual(['crlf', undefined])
    expect(result.current.status).toBe('done')
  })

  it('reset() clears state back to idle', async () => {
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(['data: {"type":"done"}\n\n']))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })
    expect(result.current.status).toBe('done')

    act(() => {
      result.current.reset()
    })

    expect(result.current.status).toBe('idle')
    expect(result.current.lines).toEqual([])
    expect(result.current.error).toBeNull()
  })

  it('surfaces a friendlier error when fetch fails with TypeError', async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError('Failed to fetch'))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toMatch(/Backend API unreachable/)
  })

  it('surfaces non-2xx HTTP status as an error', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response('nope', { status: 500 }))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toMatch(/500/)
  })

  it('surfaces an error when the stream ends unexpectedly without a done event', async () => {
    const messages = [
      'data: {"type":"status","message":"starting"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toBe('Connection closed unexpectedly before completion.')
  })
})

describe('useUrlServiceSync', () => {
  beforeEach(() => {
    mockUrlService = null
    mockSetUrlService.mockReset()
    mockSetActiveServiceId.mockReset()
    mockState = {
      activeServiceId: null,
      services: [],
      isInitialized: false,
      setActiveServiceId: mockSetActiveServiceId,
      setServices: vi.fn(),
      setInitialized: vi.fn(),
    }
  })

describe('useUrlServiceSync — URL → store', () => {
  it('writes ?service= URL param into the store on mount', async () => {
    mockUrlService = 'svc-from-url'
    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-from-url')
  })

  it('does nothing when there is no ?service= param', async () => {
    mockUrlService = null
    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
  })

  it('does not re-write when URL param matches store already', async () => {
    mockState.activeServiceId = 'svc-1'
    mockUrlService = 'svc-1'
    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
  })
})

describe('useUrlServiceSync — store → URL', () => {
  it('pushes activeServiceId into URL when it differs from current ?service', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [{ id: 'svc-new', name: 'New' }],
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-new')
  })

  it('strips ?service= from URL when services list is empty', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-orphan',
      services: [],
      isInitialized: true,
    }
    mockUrlService = 'svc-orphan'

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // No services → URL shouldn't carry a service param. Writes null,
    // which nuqs translates to removing the query string entirely.
    expect(mockSetUrlService).toHaveBeenCalledWith(null)
  })

  it('skips the store→URL sync until isInitialized is true', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'S' }],
      isInitialized: false,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    renderHook(() => useUrlServiceSync())
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('does not re-write the URL when it already matches the store', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'One' }],
      isInitialized: true,
    }
    mockUrlService = 'svc-1'

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('does NOT overwrite the URL when it names a different VALID service (prevents the #185 swap loop)', async () => {
    // Regression: switching to a freshly-added 2nd service. The URL names a
    // known service (svc-new) that differs from the store's activeServiceId
    // (svc-old). That is a deep-link / navigation the URL→store effect adopts.
    // The store→URL effect must DEFER — pushing svc-old back into the URL while
    // URL→store pushes svc-new makes the two effects swap values every render
    // and blow React's update-depth limit ("Maximum update depth exceeded",
    // React #185), which crashed the whole shell on switch.
    mockState = {
      ...mockState,
      activeServiceId: 'svc-old',
      services: [
        { id: 'svc-old', name: 'Old' },
        { id: 'svc-new', name: 'New' },
      ],
      isInitialized: true,
    }
    mockUrlService = 'svc-new'

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // URL→store adopts the new service; store→URL must NOT clobber the URL.
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-new')
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('DEFERS the stamp during a mid-switch (active id not yet in the list) to break the #185 swap', async () => {
    // The captured #185 was `Set.forEach → setActiveServiceId` — a store
    // fan-out looping from the render/commit phase. It fires in the window
    // where the switcher has written `activeServiceId = svc-new` but the
    // membership-validated `services` list has NOT yet caught up (still
    // [svc-old]); the URL has not been stamped yet (null). Old behaviour:
    // store→URL eagerly STAMPED svc-new into the URL even though the list
    // can't validate it. The companion useBootstrap reconcile, running
    // against the same stale snapshot, can revert activeServiceId back off
    // svc-new — and with the URL already mutated to svc-new the two writers
    // swap every render. The mid-switch guard must DEFER: stamp nothing
    // until activeServiceId is itself an established member of `services`,
    // mirroring useBootstrap's own "stale snapshot → defer, don't evict".
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [{ id: 'svc-old', name: 'Old' }], // list mid-update; svc-new not in yet
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // store→URL must stay silent: stamping svc-new before the list validates
    // it is the premature write that lets the reconcile-revert close the loop.
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('converges in a single stamp once the list lands and the URL is blank', async () => {
    // Continuation of the mid-switch: the bootstrap reconcile lands the list
    // now carrying svc-new, and the URL has not yet been stamped (null). With
    // the active id finally an established member, the mid-switch guard no
    // longer defers — store→URL stamps svc-new exactly once and converges.
    // (When the URL instead already names a *different* valid service, the
    // L90 "deep-link / back-forward wins" guard takes over; that path is
    // covered by the VALID-service test above.)
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [
        { id: 'svc-old', name: 'Old' },
        { id: 'svc-new', name: 'New' },
      ],
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-new')
  })

  it('still clears a STALE ?service that names no known service', async () => {
    // A `?service=` left by a previous install / dead share link is not in the
    // services list, so URL→store won't adopt it. The store→URL effect should
    // still correct the URL to the real active service (no swap risk, since the
    // stale id can never come back through URL→store).
    mockState = {
      ...mockState,
      activeServiceId: 'svc-real',
      services: [{ id: 'svc-real', name: 'Real' }],
      isInitialized: true,
    }
    mockUrlService = 'svc-dead'

    const { useUrlServiceSync } = await import('@/hooks/useBootstrap')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-real')
  })
})
})
