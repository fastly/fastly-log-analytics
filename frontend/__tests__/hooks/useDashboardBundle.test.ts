/**
 * useDashboardBundle is the cold-load round-trip saver: one POST to
 * /api/dashboard/bundle that returns aggregates + top_bots together in
 * a single response. The dashboard page reads BOTH directly off
 * bundleQuery.data (no separate cache keys), and the bundle is keyed on
 * the server-reproducible (rangeToken, anchor) pair so the SSR seed
 * (lib/ssr/dashboard.ts) byte-matches the first-paint key.
 *
 * Tests assert (1) the bundle's queryKey shape (keyed on rangeToken/
 * anchor), (2) the enabled flag wires through, (3) it does NOT fan out
 * into separate aggregates/top-bots cache entries, (4) the sections
 * selector forwards to body + key, (5) the stale-view retry path.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

vi.mock('@/stores/serviceStore', () => {
  const state: any = {
    activeServiceId: 'svc-test',
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore, __state: state }
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
  filterPayload: {} as any,
  metric: 'requests',
  interval: '1m',
  fields: ['status'],
}

describe('useDashboardBundle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
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
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
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
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined())
    expect(hits).toBe(1)
    expect((result.current.data as any).aggregates).toBeDefined()
    expect((result.current.data as any).top_bots).toEqual([{ name: 'b', count: 1 }])
  })

  it('does not automatically refetch the live relative range', async () => {
    let hits = 0
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () => {
        hits++
        return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
    renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })

    await waitFor(() => expect(hits).toBe(1))
    await new Promise(resolve => setTimeout(resolve, 5_100))
    expect(hits).toBe(1)
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
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
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
    expect((result.current.data as any).aggregates).toEqual({ data: { status: { total: 2, top: [] } } })
    expect((result.current.data as any).top_bots).toEqual([{ name: 'curl', count: 7 }])
  })

  it('queryKey for the bundle is keyed on rangeKey + anchor (+ metric/interval/fields/sections)', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, () =>
        HttpResponse.json({ aggregates: { data: {} }, top_bots: [] }),
      ),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
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
    let capturedBody: any = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
        capturedBody = await request.json()
        return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
    // relativeRange null + isAutoRange false = the user picked an explicit
    // absolute range (date picker / chart zoom / saved view).
    const { result } = renderHook(
      () => useDashboardBundle({ ...baseArgs, isAutoRange: false, enabled: true }),
      { wrapper: wrapperFor(qc) },
    )
    await waitFor(() => expect(result.current.data).toBeDefined())

    // The scan window comes from the explicit bounds, not a token.
    expect(capturedBody.start_time).toBe(baseArgs.startTime)
    expect(capturedBody.end_time).toBe(baseArgs.endTime)
    expect(capturedBody.range_token).toBeUndefined()

    const bundleEntry = qc
      .getQueryCache()
      .getAll()
      .find(q => Array.isArray(q.queryKey) && q.queryKey[0] === 'dashboard' && q.queryKey[1] === 'bundle')
    expect(bundleEntry!.queryKey[3]).toBe(`abs:${baseArgs.startTime}|${baseArgs.endTime}`)
  })

  it('forwards the sections selector through to the POST body and queryKey', async () => {
    let capturedBody: any = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/bundle`, async ({ request }) => {
        capturedBody = await request.json()
        return HttpResponse.json({ aggregates: { data: {} }, top_bots: [] })
      }),
    )
    const qc = makeClient()
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
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
    const { useDashboardBundle } = await import('@/hooks/useDashboardBundle')
    const { result } = renderHook(() => useDashboardBundle({ ...baseArgs, enabled: true }), {
      wrapper: wrapperFor(qc),
    })
    await waitFor(() => expect(result.current.data).toBeDefined(), { timeout: 5_000 })
    expect(attempts).toBeGreaterThanOrEqual(2)
    expect((result.current.data as any).aggregates.data.status.total).toBe(1)
  })
})
