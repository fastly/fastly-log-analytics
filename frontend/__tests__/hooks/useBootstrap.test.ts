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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import React from 'react'
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
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children)
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

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(mockSetServices).not.toHaveBeenCalled()
    expect(mockSetInitialized).not.toHaveBeenCalled()
  })
})
