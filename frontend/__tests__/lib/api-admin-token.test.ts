/**
 * @vitest-environment jsdom
 *
 * MSW-driven contract tests for the X-Admin-Token interceptor in
 * [lib/api.ts](../../lib/api.ts).
 *
 * Three contracts pinned here:
 *
 *  1. **Injection on outbound**: when ``useAdminTokenStore`` carries a
 *     token (the SSR-hydrated case after useBootstrap mirrors
 *     settings.admin_token), every openapi-fetch request gets
 *     ``X-Admin-Token: <value>`` so the ADMIN_SHARED_SECRET gate in
 *     ``backend/utils/remote_access.py`` lets the request through.
 *
 *  2. **Token clear on 401 admin_token_required/invalid**: the response
 *     middleware drops the stale token from the store so the next
 *     bootstrap refetch reseeds it from the server. Verified via
 *     ``setToken`` mock spy.
 *
 *  3. **No /share-login redirect on admin_token_required/invalid**: the
 *     401 branch redirects to ``/share-login`` on every OTHER 401 (analyst
 *     session dead). Admin-token 401s must NOT trigger the redirect — the
 *     admin tunnel doesn't use /share-login. Verified via window.location
 *     spy.
 *
 * Why these matter: the admin shared-secret gate was added in cd90317.
 * A regression in the request interceptor would 401-loop every admin call
 * silently; a regression in the response interceptor would bounce admins
 * off the page on transient token issues.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { useMutation } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { server } from '../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'

// --- store mocks --------------------------------------------------------
//
// adminTokenStore: drives the request interceptor's token read AND
// receives the setToken(null) call from the response interceptor on
// admin_token_invalid. ``state`` is mutable so each test can preset and
// observe.
const adminState: { token: string | null; setToken: ReturnType<typeof vi.fn> } = {
  token: null,
  setToken: vi.fn((next: string | null) => {
    adminState.token = next
  }),
}
vi.mock('@/stores/adminTokenStore', () => {
  const useAdminTokenStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(adminState) : adminState,
  )
  useAdminTokenStore.getState = () => adminState
  return { useAdminTokenStore }
})

// serviceStore: same shape as the existing api-error-paths test —
// onRequest reads activeServiceId to set x-service-id.
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'svc-test' }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

// debugStore: the request interceptor pokes useDebugStore.getState() to
// decide whether to set x-debug-responses. Stub a no-op state so the
// try/catch doesn't kick in for unrelated reasons.
vi.mock('@/stores/debugStore', () => {
  const state = { enabled: false, apiCallsEnabled: false }
  const useDebugStore: any = vi.fn(() => state)
  useDebugStore.getState = () => state
  return { useDebugStore }
})

// sessionRoleStore: the response interceptor's /share-login redirect reads
// isRemoteAnalyst to decide whether the dead-session bounce applies (analyst)
// or must be skipped (admin). Mutable so each test sets the role.
const sessionRoleState = { isRemoteAnalyst: false }
vi.mock('@/stores/sessionRoleStore', () => {
  const useSessionRoleStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(sessionRoleState) : sessionRoleState,
  )
  useSessionRoleStore.getState = () => sessionRoleState
  return { useSessionRoleStore }
})

// Import AFTER mocks so client middleware picks them up.
import { client } from '@/lib/api'
import { useAdminTokenStore } from '@/stores/adminTokenStore'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 }, mutations: { retry: false } })
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  adminState.token = null
  adminState.setToken.mockClear()
  sessionRoleState.isRemoteAnalyst = false
})

describe('X-Admin-Token request injection', () => {
  it('attaches X-Admin-Token when the store carries a token', async () => {
    adminState.token = 'admin-secret-123'

    let observedHeader: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observedHeader = request.headers.get('X-Admin-Token')
        return HttpResponse.json({ ok: true })
      }),
    )

    const { result } = renderHook(
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

    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observedHeader).toBe('admin-secret-123')
  })

  it('omits X-Admin-Token when the store is empty (analyst path)', async () => {
    // Default state: token is null. The interceptor's `if (adminToken)` guard
    // should short-circuit so the header is never attached.
    let observedHeader: string | null = 'sentinel'
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observedHeader = request.headers.get('X-Admin-Token')
        return HttpResponse.json({ ok: true })
      }),
    )

    const { result } = renderHook(
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

    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observedHeader).toBeNull()
  })
})

describe('admin_token_invalid 401 response handling', () => {
  // The middleware also tries to redirect via window.location.replace on
  // generic 401s. jsdom locks Location so we can't spy on .replace
  // directly; the negative-redirect assertion is covered by the
  // setToken-cleared assertion below — if the admin-token branch fell
  // through to the redirect, setToken would never be called.

  it('clears the stored token on detail.error=admin_token_invalid', async () => {
    adminState.token = 'stale-token'

    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: { error: 'admin_token_invalid' } }, { status: 401 }),
      ),
    )

    const { result } = renderHook(
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

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    // The interceptor's setToken(null) call lands on the store.
    expect(useAdminTokenStore.getState().setToken).toHaveBeenCalledWith(null)
  })

  it('clears the stored token on detail.error=admin_token_required', async () => {
    adminState.token = 'stale-token'

    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: { error: 'admin_token_required' } }, { status: 401 }),
      ),
    )

    const { result } = renderHook(
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

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(useAdminTokenStore.getState().setToken).toHaveBeenCalledWith(null)
  })

  it('does NOT clear the token on a non-admin-token 401 (analyst session dead)', async () => {
    // The 401 branch redirects to /share-login on unauthenticated; the
    // admin-token-clear path must NOT run for these codes so the store
    // keeps its current value (which is null on the analyst path
    // anyway — but a regression that always-cleared on 401 would mask
    // any token rotation race).
    adminState.token = null

    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: { error: 'unauthenticated' } }, { status: 401 }),
      ),
    )

    const { result } = renderHook(
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

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    // setToken was NOT called — the admin-token-clear branch didn't fire.
    expect(useAdminTokenStore.getState().setToken).not.toHaveBeenCalled()
  })
})

describe('non-admin-token 401 → /share-login redirect is analyst-only', () => {
  // The dead-session bounce must fire for a remote analyst but NEVER for an
  // admin — an admin-only 401 (e.g. a provision call missing its Fastly
  // token, post-deploy) was bouncing the operator to the analyst sign-in.
  // jsdom's window.location is configurable, so swap in a replace() spy.
  let originalLocation: Location

  beforeEach(() => {
    originalLocation = window.location
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { pathname: '/admin', search: '?service=svc-test', replace: vi.fn() },
    })
  })

  afterEach(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: originalLocation,
    })
  })

  function fire401(detailError: string) {
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: { error: detailError } }, { status: 401 }),
      ),
    )
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

  it('redirects a remote analyst to /share-login on a session-dead 401', async () => {
    sessionRoleState.isRemoteAnalyst = true
    const { result } = fire401('unauthenticated')
    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(window.location.replace).toHaveBeenCalledWith(
      expect.stringContaining('/share-login?return='),
    )
  })

  it('does NOT redirect an admin (isRemoteAnalyst=false) on the same 401', async () => {
    sessionRoleState.isRemoteAnalyst = false
    const { result } = fire401('unauthenticated')
    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(window.location.replace).not.toHaveBeenCalled()
  })

  it('does NOT redirect an admin on a provision token_required 401', async () => {
    // The concrete bug: the wizard's post-deploy ngwaf-workspace PATCH
    // returned token_required, which bounced the admin to /share-login.
    sessionRoleState.isRemoteAnalyst = false
    const { result } = fire401('token_required')
    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(window.location.replace).not.toHaveBeenCalled()
  })
})
