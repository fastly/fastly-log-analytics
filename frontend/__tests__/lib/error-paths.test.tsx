/**
 * @vitest-environment jsdom
 *
 * Error-path coverage (TESTING_PLAN_3 item 14).
 *
 * Until now every test mocked 2xx responses. That meant the openapi-fetch
 * ``onResponse`` middleware (extractApiError + throw on !response.ok),
 * extractApiError's FastAPI-422 shape unwrapping, and react-query's
 * ``isError`` propagation into the UI were all dead code under test.
 *
 * This file exercises three concrete scenarios end-to-end against MSW:
 *
 *   - openapi-fetch error middleware extracts a plain ``detail`` string
 *     from a 500.
 *   - openapi-fetch error middleware unwraps the FastAPI 422 array shape
 *     (``[{loc, msg}]``) into a human-readable string.
 *   - A ``useQuery`` consumer's ``isError`` flips to ``true`` on 401 —
 *     no silent failure, no infinite loading state.
 *
 * IMPLEMENTATION NOTE: imports of ``@/lib/api`` are dynamic (inside each
 * test body), not static at module load. openapi-fetch captures
 * ``globalThis.fetch`` at ``createClient`` time, and MSW patches
 * ``globalThis.fetch`` from ``beforeAll`` (in vitest.setup.ts). A static
 * import would race the setup and the client would hold an unpatched
 * fetch reference — silently letting all requests fall through to the
 * real ``http://127.0.0.1:8000`` if a dev server is running. Same pattern
 * as ``__tests__/hooks/useBootstrap.test.ts``.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import React from 'react'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'test-svc' }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children)
}

describe('extractApiError — unit coverage of error shapes', () => {
  it('returns the string for {detail: "..."}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: 'bucket not found' })).toBe('bucket not found')
  })

  it('joins FastAPI 422 array shape {detail: [{loc, msg}]}', async () => {
    const { extractApiError } = await import('@/lib/api')
    const result = extractApiError({
      detail: [
        { loc: ['body', 'name'], msg: 'String must not be empty' },
        { loc: ['body', 'region'], msg: 'String must match pattern' },
      ],
    })
    expect(result).toBe('name: String must not be empty, region: String must match pattern')
  })

  it('joins {detail: {errors: [...]}}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: { errors: ['e1', 'e2'] } })).toBe('e1, e2')
  })

  it('returns {error: "..."} from nested detail', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: { error: 'Token expired' } })).toBe('Token expired')
  })

  it('handles the no-detail fallback {error: "..."}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ error: 'Direct error message' })).toBe('Direct error message')
  })

  it('returns "Unknown error" for falsy input', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError(null)).toBe('Unknown error')
    expect(extractApiError(undefined)).toBe('Unknown error')
  })

  it('passes through a raw string', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError('plain message')).toBe('plain message')
  })
})

describe('openapi-fetch onResponse middleware — error path coverage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('throws extracted message on 500 with {detail: "..."}', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ detail: 'Background scheduler offline' }, { status: 500 }),
      ),
    )

    const { client } = await import('@/lib/api')
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow(
      'Background scheduler offline',
    )
  })

  it('throws joined message on 422 with FastAPI array shape', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json(
          {
            detail: [
              { loc: ['query', 'service_id'], msg: 'field required' },
            ],
          },
          { status: 422 },
        ),
      ),
    )

    const { client } = await import('@/lib/api')
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow(
      /service_id: field required/,
    )
  })

  it('throws a generic message when the error body is not JSON', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        new HttpResponse('not json — gateway timeout', { status: 504 }),
      ),
    )

    const { client } = await import('@/lib/api')
    // .catch(() => ({ message: ... })) → extractApiError sees {message: ...} → JSON.stringify
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow()
  })
})

describe('useQuery isError flag — react-query error propagation', () => {
  it('isError === true when handler returns 401', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ detail: 'Unauthorized' }, { status: 401 }),
      ),
    )

    const { client } = await import('@/lib/api')
    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['system-jobs-401'],
          queryFn: async () => {
            const { data } = await client.GET('/api/admin/system-jobs')
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error).toBeInstanceOf(Error)
    expect((result.current.error as Error).message).toMatch(/Unauthorized/i)
  })

  it('isError === false and data populated on 2xx success', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ jobs: [] }),
      ),
    )

    const { client } = await import('@/lib/api')
    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['system-jobs-ok'],
          queryFn: async () => {
            const { data } = await client.GET('/api/admin/system-jobs')
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.isError).toBe(false)
    expect(result.current.data).toEqual({ jobs: [] })
  })
})
