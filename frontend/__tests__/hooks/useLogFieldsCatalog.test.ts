/**
 * R-10 (testing_suite_audit_2026-06-14.md). Exercises `useLogFieldsCatalog`
 * end-to-end against MSW: the hook reads the active service id from the
 * mocked Zustand store, fires `GET /api/log-fields/catalog?service_id=<sid>`,
 * and returns the parsed payload.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

vi.mock('@/stores/serviceStore', () => {
  const state = {
    activeServiceId: 'svc-1',
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0, staleTime: 0 } })
  return makeQueryWrapper(qc)
}

describe('useLogFieldsCatalog (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('returns the catalog payload from the API', async () => {
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({
          fields: [{ id: 'status', label: 'HTTP Status' }],
          custom_fields: [{ name: 'x-edge-ts', duckdb_type: 'VARCHAR' }],
          groups: [],
          presets: { default: { columns: ['status'] } },
        }),
      ),
    )
    const { useLogFieldsCatalog } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useLogFieldsCatalog(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.data).toBeDefined())
    expect(result.current.data?.fields).toEqual([{ id: 'status', label: 'HTTP Status' }])
    expect((result.current.data as any).custom_fields).toEqual([
      { name: 'x-edge-ts', duckdb_type: 'VARCHAR' },
    ])
  })

  it('threads the active service id into the query-string', async () => {
    let observedSid: string | null = null
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, ({ request }) => {
        const url = new URL(request.url)
        observedSid = url.searchParams.get('service_id')
        return HttpResponse.json({ fields: [], custom_fields: [], groups: [], presets: {} })
      }),
    )
    const { useLogFieldsCatalog } = await import('@/hooks/useLogFieldsCatalog')
    renderHook(() => useLogFieldsCatalog(), { wrapper: wrapper() })

    await waitFor(() => expect(observedSid).not.toBeNull())
    expect(observedSid).toBe('svc-1')
  })

  it('returns isError when the API 500s', async () => {
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ error: 'oops' }, { status: 500 }),
      ),
    )
    const { useLogFieldsCatalog } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useLogFieldsCatalog(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.isError).toBe(true))
  })
})
