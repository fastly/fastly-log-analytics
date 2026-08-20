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

// Mutable so a test can set the persisted activeServiceId (e.g. a just-selected
// service) before mount. Reset in beforeEach so the default-null tests are
// unaffected. The object identity is stable (only its fields mutate) so the
// selector/getState closures below always read the current values.
const mockServiceState = {
  activeServiceId: null as string | null,
  setActiveServiceId: mockSetActiveServiceId,
  setServices: mockSetServices,
  setInitialized: mockSetInitialized,
}

vi.mock('@/stores/serviceStore', () => {
  // The selector form is what React components use.
  // ``useServiceStore.getState()`` is what lib/api.ts's middleware uses;
  // without it the openapi-fetch request middleware throws on every
  // request and MSW never sees the call.
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockServiceState) : mockServiceState,
  )
  useServiceStore.getState = () => mockServiceState
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
    mockServiceState.activeServiceId = null
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

  describe('reconcile — stale snapshot must not evict a just-selected service', () => {
    // Regression for the "Switch To a freshly-added service bounces back" bug.
    // Clicking "Switch To" sets activeServiceId to the new service and soft-
    // navigates. The bootstrap query is force-dynamic + 5-min staleTime, so a
    // soft nav serves the CACHED bootstrap — which can predate the new service.
    // The reconcile effect must NOT revert the just-selected id off that stale
    // snapshot; it has to wait for the (invalidated/pending) refetch to land a
    // fresh services list. Otherwise the admin is bounced back to the previous
    // service and AppLayout then redirects to /admin.

    /**
     * Seed a STALE bootstrap cache entry (dataUpdatedAt well past the 5-min
     * staleTime) so query.isStale === true on mount, and hang the network so
     * no refetch replaces it during the test — isolating the stale window.
     */
    function staleWrapper(seed: { services: any[]; active_service_id: string | null }) {
      const qc = createTestQueryClient({ queries: { gcTime: 0 } })
      qc.setQueryData(['bootstrap'], seed, { updatedAt: Date.now() - 10 * 60 * 1000 })
      // Hanging handler: the background refetch fires but never resolves, so the
      // stale snapshot (and isStale === true) persists for the assertion window.
      server.use(
        http.get(`${API_BASE}/api/bootstrap`, () => new Promise(() => {})),
      )
      return makeQueryWrapper(qc)
    }

    it('does NOT revert when the selected service is absent from a STALE snapshot', async () => {
      // Store holds the just-selected new service; the stale cached bootstrap
      // only knows the old one. The reconcile must defer, not evict.
      mockServiceState.activeServiceId = 'svc-new'
      const wrap = staleWrapper({
        services: [{ service_id: 'svc-old', name: 'Old', access_level: 'read_write' }],
        active_service_id: 'svc-old',
      })

      const { useBootstrap } = await import('@/hooks/useBootstrap')
      renderHook(() => useBootstrap(), { wrapper: wrap })

      // Give the effect ample time to (wrongly) fire. setServices still runs
      // (it's unconditional on query.data), but the active-service REVERT must
      // not — specifically it must never be called with the old default.
      await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
      await new Promise((r) => setTimeout(r, 50))
      expect(mockSetActiveServiceId).not.toHaveBeenCalledWith('svc-old')
      expect(mockSetActiveServiceId).not.toHaveBeenCalled()
    })

    it('STILL reverts a stale-id selection when the snapshot is FRESH', async () => {
      // Counter-test: a genuinely stale activeServiceId (e.g. a dead id left in
      // localStorage) must still be corrected once a FRESH bootstrap confirms
      // it's gone. Here MSW returns a fresh list without the id, so isStale is
      // false and the revert branch runs.
      mockServiceState.activeServiceId = 'svc-dead'
      server.use(
        http.get(`${API_BASE}/api/bootstrap`, () =>
          HttpResponse.json({
            services: [{ service_id: 'svc-real', name: 'Real', access_level: 'read_write' }],
            active_service_id: 'svc-real',
          }),
        ),
      )

      const { useBootstrap } = await import('@/hooks/useBootstrap')
      renderHook(() => useBootstrap(), { wrapper: wrapper() })

      await waitFor(() => expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-real'))
    })
  })
})

describe('useBootstrap — recovery after a deploy', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockServiceState.activeServiceId = null
  })

  // A deploy takes the backend away for a few seconds. The `retry` policy is
  // deliberately capped at ONE attempt (the 2026-06-23 bootstrap-storm guard),
  // so without a poll an open tab spends that single retry during the outage
  // and parks on AppLayout's dead-end "Can't reach the server" card until the
  // user clicks Retry. This asserts the tab heals itself.
  it('recovers on its own once the server returns, with no manual retry', async () => {
    let failing = true
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () => {
        if (failing) return new HttpResponse(null, { status: 503 })
        return HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
        })
      }),
    )

    const { useBootstrap } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 10000 })

    // Server comes back. Nothing clicks anything.
    failing = false

    await waitFor(() => expect(result.current.isSuccess).toBe(true), { timeout: 15000 })
    expect(mockSetServices).toHaveBeenCalled()
  }, 30000)

  // The poll must stop once healthy, or every client permanently hammers
  // /api/bootstrap — which is the fan-out the storm guard exists to prevent.
  it('does not keep polling once the request succeeds', async () => {
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () => {
        calls += 1
        return HttpResponse.json({
          services: [{ service_id: 'svc-1', name: 'My CDN', access_level: 'read_write' }],
          active_service_id: 'svc-1',
        })
      }),
    )

    const { useBootstrap } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    const afterFirstSuccess = calls
    await new Promise(r => setTimeout(r, 8000))
    expect(calls).toBe(afterFirstSuccess)
  }, 30000)
})
