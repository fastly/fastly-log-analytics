/**
 * R-10 (testing_suite_audit_2026-06-14.md). Exercises `useLogFieldsCatalog`
 * and `useActiveLogFields` against MSW and React Query cache.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/server'
import { queryKeys } from '@/lib/query-keys'

const useBootstrap = vi.fn()

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => useBootstrap(),
}))

vi.mock('@/stores/serviceStore', () => {
  const state = {
    activeServiceId: 'svc-1',
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state)),
    { getState: () => state },
  )
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper(catalogSeed?: unknown) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (catalogSeed !== undefined) {
    qc.setQueryData(queryKeys.logFieldsCatalog('svc-1'), catalogSeed)
  }
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
    expect((result.current.data as { custom_fields?: unknown } | undefined)?.custom_fields).toEqual([
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

const CATALOG = {
  groups: [
    { id: 'L', label: 'Origin Metrics', fields: ['ottfb', 'ottlb', 'oip', 'prid'] },
    { id: 'D', label: 'Geolocation', fields: ['country', 'city', 'region'] },
  ],
}

describe('useActiveLogFields', () => {
  beforeEach(() => {
    useBootstrap.mockReset()
  })

  it('reports not-ready and treats everything as active until bootstrap lands', async () => {
    useBootstrap.mockReturnValue({ data: undefined })
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper(CATALOG) })
    expect(result.current.ready).toBe(false)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('anything')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(true)
  })

  it('treats an empty active_log_field_ids list as not-ready (everything active)', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: [] } })
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper(CATALOG) })
    expect(result.current.ready).toBe(false)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
  })

  it('honours active_log_field_ids when populated', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb', 'country'] } })
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper(CATALOG) })
    expect(result.current.ready).toBe(true)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('ja3')).toBe(false)
  })

  it('isGroupActive is true when ANY field of the group is active', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['country'] } })
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper(CATALOG) })
    expect(result.current.isGroupActive('D')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(false)
  })

  it('isGroupActive is false for an unknown group id', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb'] } })
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper(CATALOG) })
    expect(result.current.isGroupActive('ZZ')).toBe(false)
  })

  it('does not throw when the catalog is missing (still resolves fields)', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb'] } })
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ error: 'missing' }, { status: 500 }),
      ),
    )
    const { useActiveLogFields } = await import('@/hooks/useLogFieldsCatalog')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: wrapper() })
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(false)
  })
})
