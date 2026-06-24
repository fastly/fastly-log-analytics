/**
 * @vitest-environment jsdom
 *
 * MSW-driven contract test for the ``X-User-Active`` interceptor in
 * [lib/api.ts](../../lib/api.ts).
 *
 * Every outbound typed-client request must carry ``X-User-Active: 1|0`` so the
 * backend can reset the analyst idle timeout only on genuine interaction. The
 * "0" case is load-bearing: it's how an automated react-query refetch on a
 * foreground tab (e.g. the ~12s /api/dashboard/bundle the badge stream
 * invalidates) tells the backend NOT to keep an idle session alive. The header
 * must be set BEFORE the activeServiceId early-returns in the onRequest
 * middleware — this test pins that.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { useMutation } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach } from 'vitest'

import { server } from '../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'

// Control the activity signal directly (hoisting-safe: set return value per test).
vi.mock('@/lib/userActivity', () => ({
  isUserActive: vi.fn(() => true),
  ACTIVE_WINDOW_MS: 120_000,
}))

// Minimal store mocks mirroring api-admin-token.test.ts.
vi.mock('@/stores/adminTokenStore', () => {
  const state = { token: null as string | null }
  const useAdminTokenStore: any = vi.fn(() => state)
  useAdminTokenStore.getState = () => state
  return { useAdminTokenStore }
})
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'svc-test' }
  const useServiceStore: any = vi.fn((s?: (x: any) => any) => (s ? s(state) : state))
  useServiceStore.getState = () => state
  return { useServiceStore }
})
vi.mock('@/stores/debugStore', () => {
  const state = { enabled: false, apiCallsEnabled: false }
  const useDebugStore: any = vi.fn(() => state)
  useDebugStore.getState = () => state
  return { useDebugStore }
})

// Import AFTER mocks so the client middleware picks them up.
import { client } from '@/lib/api'
import { isUserActive } from '@/lib/userActivity'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 }, mutations: { retry: false } })
  return makeQueryWrapper(qc)
}

function fireDashboard() {
  return renderHook(
    () =>
      useMutation({
        mutationFn: async () => {
          const { data } = await client.POST('/api/dashboard/aggregates' as any, {
            body: { start_time: 't', end_time: 't', filters: [] } as any,
          } as any)
          return data
        },
      }),
    { wrapper: wrapper() },
  )
}

beforeEach(() => {
  vi.mocked(isUserActive).mockReturnValue(true)
})

describe('X-User-Active request injection', () => {
  it('sends X-User-Active: 1 when the user is active', async () => {
    vi.mocked(isUserActive).mockReturnValue(true)
    let observed: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observed = request.headers.get('X-User-Active')
        return HttpResponse.json({ ok: true })
      }),
    )
    const { result } = fireDashboard()
    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observed).toBe('1')
  })

  it('sends X-User-Active: 0 when the user is idle (suppresses idle-clock reset)', async () => {
    vi.mocked(isUserActive).mockReturnValue(false)
    let observed: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observed = request.headers.get('X-User-Active')
        return HttpResponse.json({ ok: true })
      }),
    )
    const { result } = fireDashboard()
    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observed).toBe('0')
  })
})
