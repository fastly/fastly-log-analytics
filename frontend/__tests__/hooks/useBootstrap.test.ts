/**
 * @vitest-environment jsdom
 *
 * MSW migration (TESTING_PLAN_3 item 9): this test used to
 * ``vi.mock('@/lib/api')`` and ``vi.mock('@tanstack/react-query',
 * { useQuery: mockUseQuery })`` to control what the bootstrap query
 * "returned". That bypassed both the openapi-fetch transport and the
 * react-query state machine — fine for asserting the mapping shape, but
 * blind to any real-world wire-level regression.
 *
 * Now the hook runs end-to-end against an MSW handler. The
 * ``useServiceStore`` is still mocked (it's a Zustand persist store
 * with global state — orthogonal to the network boundary and noisy to
 * leave shared across tests).
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

const mockSetServices = vi.fn()
const mockSetInitialized = vi.fn()
const mockSetActiveServiceId = vi.fn()

vi.mock('@/stores/serviceStore', () => {
  const state = {
    activeServiceId: null,
    setActiveServiceId: mockSetActiveServiceId,
    setServices: mockSetServices,
    setInitialized: mockSetInitialized,
  }
  // The selector form is what React components use.
  // ``useServiceStore.getState()`` is what lib/api.ts's middleware uses;
  // without it the openapi-fetch request middleware throws on every
  // request and MSW never sees the call.
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

/** Build a fresh QueryClient per test so the cache doesn't leak. */
function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  return makeQueryWrapper(qc)
}

describe('useBootstrap (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('calls setServices with the correct shape from API response', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [
            { service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' },
            { service_id: 'svc-2', name: 'Staging', access_level: 'read_only' },
          ],
          active_service_id: 'svc-1',
        }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
    expect(mockSetServices).toHaveBeenCalledWith([
      { id: 'svc-1', name: 'My CDN', accessLevel: 'read_write' },
      { id: 'svc-2', name: 'Staging', accessLevel: 'read_only' },
    ])
  })

  it('maps service_id → id, name → name, access_level → accessLevel', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ service_id: 'abc', name: 'Test', access_level: 'read_only' }],
          active_service_id: null,
        }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
    const [mappedServices] = mockSetServices.mock.calls[0]
    expect(mappedServices[0]).toEqual({ id: 'abc', name: 'Test', accessLevel: 'read_only' })
    expect(mappedServices[0].service_id).toBeUndefined()
    expect(mappedServices[0].access_level).toBeUndefined()
  })

  it('reads active_service_id (not active_id) to set the default service', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
        }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetActiveServiceId).toHaveBeenCalled())
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-1')
  })

  it('calls setInitialized(true) once data arrives', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ services: [], active_service_id: null }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetInitialized).toHaveBeenCalledWith(true))
  })

  it('handles empty services array gracefully', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ services: [], active_service_id: null }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetServices).toHaveBeenCalledWith([]))
    await waitFor(() => expect(mockSetInitialized).toHaveBeenCalledWith(true))
  })

  it('refetches errored queries once the admin token arrives after an SSR bootstrap miss', async () => {
    // Simulate the restart-warmup race: the SSR bootstrap fetch failed, so
    // no admin token was seeded at render time (store empty). When the
    // client bootstrap resolves with the token, the hook must invalidate the
    // queries that 401'd so /admin self-heals instead of waiting for a reload.
    const { useAdminTokenStore } = await import('@/stores/adminTokenStore')
    useAdminTokenStore.getState().setToken(null)

    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
          settings: { admin_token: 'shh-secret' },
        }),
      ),
    )

    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    const wrap = makeQueryWrapper(qc)

    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrap })

    await waitFor(() => expect(useAdminTokenStore.getState().token).toBe('shh-secret'))
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalled())
    // Scoped to errored queries, not a blanket refetch storm.
    expect(invalidateSpy.mock.calls[0][0]).toHaveProperty('predicate')
  })

  it('does NOT refetch when the admin token was already seeded (happy path)', async () => {
    // Happy path: <HydrateAdminToken> seeded the token synchronously during
    // SSR-hydrated render, so the token is already present before this
    // effect runs — the empty→present guard must prevent a needless refetch.
    const { useAdminTokenStore } = await import('@/stores/adminTokenStore')
    useAdminTokenStore.getState().setToken('already-seeded')

    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
          settings: { admin_token: 'already-seeded' },
        }),
      ),
    )

    const qc = createTestQueryClient({ queries: { gcTime: 0 } })
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    const wrap = makeQueryWrapper(qc)

    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrap })

    await waitFor(() => expect(mockSetInitialized).toHaveBeenCalledWith(true))
    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('surfaces a fetch error via the openapi-fetch error middleware', async () => {
    // The error here is *not* asserted via mockSetServices because the
    // hook's useEffect only runs when query.data is truthy. The point of
    // this test is to prove MSW exercises the real error path: the
    // ``onResponse`` middleware in lib/api.ts throws on !response.ok,
    // useQuery catches it, and the hook does not call setServices.
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ detail: 'boom' }, { status: 500 }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })

    // useBootstrap pins its own retry policy (one retry on 5xx/network,
    // never on 4xx) to trim the cold-path multiplier during an incident —
    // this overrides the test client's `retry: false`. A 500 therefore
    // retries once (~1s default backoff) before settling into the error
    // state, so give waitFor room past that single retry. See the
    // 2026-06-23 bootstrap-storm fix.
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 3000 })
    expect(mockSetServices).not.toHaveBeenCalled()
    expect(mockSetInitialized).not.toHaveBeenCalled()
  })
})
