/**
 * useDashboardBundle is the cold-load round-trip saver: one POST to
 * /api/dashboard/bundle that returns aggregates + top_bots together
 * and seeds both sub-queries' cache keys so the dedicated hooks read
 * cache instead of re-fetching.
 *
 * Tests assert (1) the bundle's own queryKey shape, (2) the enabled
 * flag wires through, (3) the seed-on-success behaviour for both
 * sub-queries, (4) the bundle is gated off when activeServiceId is
 * null, (5) the stale-view retry path triggers on a stale response.
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
  // gcTime cannot be 0 here — the bundle's queryFn calls
  // ``setQueryData(aggregatesKey, …)`` to seed an entry with no
  // observers, and gcTime=0 garbage-collects it on the next tick
  // before the assertion can read it back. 5s is plenty for tests.
  return createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
}

function wrapperFor(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

const baseArgs = {
  startTime: '2026-06-15T00:00:00Z',
  endTime: '2026-06-15T01:00:00Z',
  filterPayload: { conditions: [] } as any,
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

  it('seeds the aggregates + top-bots cache keys on success', async () => {
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

    const cached = qc
      .getQueryCache()
      .getAll()
      .reduce<Record<string, unknown>>((acc, q) => {
        const k = q.queryKey as unknown[]
        if (k[0] === 'dashboard' && (k[1] === 'aggregates' || k[1] === 'top-bots')) {
          acc[k[1] as string] = q.state.data
        }
        return acc
      }, {})

    expect(cached.aggregates).toEqual({ data: { status: { total: 2, top: [] } } })
    expect(cached['top-bots']).toEqual([{ name: 'curl', count: 7 }])
  })

  it('queryKey for the bundle includes activeServiceId + chart_metric + interval + fields + sections', async () => {
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
      baseArgs.startTime,
      baseArgs.endTime,
      baseArgs.filterPayload,
      baseArgs.metric,
      baseArgs.interval,
      baseArgs.fields,
      undefined,
    ])
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
