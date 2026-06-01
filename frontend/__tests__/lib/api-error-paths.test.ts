/**
 * @vitest-environment jsdom
 *
 * MSW-driven contract tests for the API client's error middleware
 * ([lib/api.ts](../../lib/api.ts) ``onResponse`` middleware + ``extractApiError``).
 *
 * Why this file exists
 * --------------------
 * The dashboard, CustomFieldDrawer, and LogSettingsModal all surface
 * ``mutation.error.message`` directly to users. That message comes from
 * a two-stage path: openapi-fetch sees a !ok response, the middleware
 * shapes it via ``extractApiError`` into a JS Error, and the component
 * reads ``.message``. If anything in that path silently changes — a
 * FastAPI version that swaps ``detail.errors`` for ``detail.error_list``,
 * or an Error subclass that drops ``.message`` — the UI quietly
 * degrades to "An unknown error occurred" without any test failing.
 *
 * Pinning here matters because:
 *
 *  - **422** is what the create-custom-field route uses for
 *    ``LOG_FORMAT_TOO_LONG`` and field-name validation. CustomFieldDrawer
 *    reads ``.message`` and surfaces it inline. The 422 body shape is
 *    ``{ detail: { errors: [...] } }`` — pinning that arrives intact.
 *  - **500** is the universal "something blew up" path. extractApiError
 *    handles three different shapes here (string detail, dict detail,
 *    plain error). Customers see these in production.
 *  - **401** is auth — pre-Phase-X this returns a plain string; later
 *    iterations may return ``{ detail: "..." }``. Either shape must
 *    surface a readable message, not "[object Object]".
 *
 * What this file is NOT
 * ---------------------
 * Not a "does the dashboard render an Alert?" test. The dashboard today
 * doesn't have a visible error UI for query failures (returns to empty
 * data + isError on the hook). Adding that UI is its own ticket. Until
 * then, locking in the error CONTRACT means that when the UI is added,
 * it can rely on a stable ``.message``.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider, useMutation, useQuery } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import React from 'react'
import { describe, it, expect, vi } from 'vitest'

import { server } from '../../tests/msw/server'
import { client, extractApiError } from '@/lib/api'

const API_BASE = 'http://127.0.0.1:8000'

// The middleware reads useServiceStore.getState() on every request to
// inject x-service-id. Stub it so the request can be built.
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'svc-test' }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

function wrapper() {
  const qc = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  })
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children)
}

describe('extractApiError shape coverage', () => {
  // These exercise extractApiError without going through MSW because the
  // function is also called directly by some surfaces (e.g., raw fetch
  // helpers). Cheap to pin both layers in one file.

  it('returns string-detail verbatim', () => {
    expect(extractApiError({ detail: 'Service not found' })).toBe('Service not found')
  })

  it('joins detail.errors[] (FastAPI 422 with our custom shape)', () => {
    expect(
      extractApiError({ detail: { errors: ['LOG_FORMAT_TOO_LONG: too big', 'BAD_NAME: nope'] } }),
    ).toBe('LOG_FORMAT_TOO_LONG: too big, BAD_NAME: nope')
  })

  it('falls back to detail.error scalar', () => {
    expect(extractApiError({ detail: { error: 'broken' } })).toBe('broken')
  })

  it('formats Pydantic detail[] validation arrays', () => {
    const msg = extractApiError({
      detail: [{ loc: ['body', 'name'], msg: 'required' }],
    })
    expect(msg).toContain('name: required')
  })

  it('reads top-level error', () => {
    expect(extractApiError({ error: 'kapow' })).toBe('kapow')
  })

  it('returns a sane default for null/undefined', () => {
    expect(extractApiError(null)).toBe('Unknown error')
    expect(extractApiError(undefined)).toBe('Unknown error')
  })
})

describe('API client onResponse middleware (MSW)', () => {
  it('422 detail.errors[] reaches the caller as a joined Error.message', async () => {
    // This is the exact contract CustomFieldDrawer's onError handler
    // relies on when LOG_FORMAT_TOO_LONG fires from the create route.
    server.use(
      http.post(`${API_BASE}/api/services/svc-test/custom-fields`, () =>
        HttpResponse.json(
          {
            detail: {
              errors: ['LOG_FORMAT_TOO_LONG: Log format is 8050 chars; safe max 8000'],
            },
          },
          { status: 422 },
        ),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST(
              '/api/services/{service_id}/custom-fields' as any,
              {
                params: { path: { service_id: 'svc-test' } },
                body: { name: 'whatever' } as any,
              } as any,
            )
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error).toBeInstanceOf(Error)
    expect(result.current.error?.message).toContain('LOG_FORMAT_TOO_LONG')
    expect(result.current.error?.message).toContain('8000')
  })

  it('500 string-detail produces a readable Error.message', async () => {
    // The dashboard's primary aggregates endpoint — when it 500s,
    // operators see this string in their logs. "[object Object]" here
    // would be a regression worth catching.
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: 'Internal server error' }, { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates' as any, {
              body: {
                start_time: '2026-01-01T00:00:00Z',
                end_time: '2026-01-01T01:00:00Z',
                filters: [],
                chart_metric: 'requests',
                chart_interval: 'minute',
              } as any,
            } as any)
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBe('Internal server error')
  })

  it('401 plain message becomes a readable Error.message (not "[object Object]")', async () => {
    // Auth path. We don't currently surface this to users specifically,
    // but the contract that "a 401 produces a useful .message" is what
    // makes future auth-UI work cheap.
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: 'Not authenticated' }, { status: 401 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates' as any, {
              body: {
                start_time: '2026-01-01T00:00:00Z',
                end_time: '2026-01-01T01:00:00Z',
                filters: [],
              } as any,
            } as any)
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBe('Not authenticated')
    // Negative assertion: a regression that drops the extractor would
    // produce the JSON-stringified payload here.
    expect(result.current.error?.message).not.toContain('[object Object]')
    expect(result.current.error?.message).not.toContain('{')
  })

  it('500 with a missing body still produces some message', async () => {
    // Edge: server returns 500 with no JSON. The middleware's catch
    // branch produces a default. Pin that we don't end up surfacing
    // "undefined" or an exception trace.
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.text('', { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates' as any, {
              body: {} as any,
            } as any)
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBeTruthy()
    expect(result.current.error?.message.length).toBeGreaterThan(0)
  })

  it('useQuery error path leaves isError=true and data undefined', async () => {
    // The dashboard uses useQuery, not useMutation. Pin the query-side
    // contract separately: isError flips, data stays undefined so any
    // "if (data)" guards in render code keep functioning.
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ detail: 'catalog fetch broke' }, { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['catalog'],
          queryFn: async () => {
            const { data } = await client.GET('/api/log-fields/catalog' as any, {} as any)
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.data).toBeUndefined()
    expect(result.current.error?.message).toBe('catalog fetch broke')
  })
})
