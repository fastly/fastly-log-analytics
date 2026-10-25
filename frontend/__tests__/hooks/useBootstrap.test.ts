/**
 * @vitest-environment jsdom
 *
 * MSW migration (TESTING_PLAN_3 item 9) + R-10 (`useLogFieldsCatalog` and
 * `useActiveLogFields`): exercises `useBootstrap`, `useLogFieldsCatalog`,
 * and `useActiveLogFields` end-to-end against MSW and React Query cache.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { afterEach, describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { queryKeys } from '@/lib/query-keys'

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

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  // The selector form is what React components use.
  // ``useServiceStore.getState()`` is what lib/api.ts's middleware uses;
  // without it the openapi-fetch request middleware throws on every
  // request and MSW never sees the call.
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockServiceState) => unknown) =>
      selector ? selector(mockServiceState) : mockServiceState,
    ),
    { getState: () => mockServiceState },
  )
  return { ...actual, useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

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
      {
        id: 'svc-1',
        name: 'My CDN',
        accessLevel: 'read_write',
        cmcdEnabled: undefined,
        rum_enabled: undefined,
        analystPathASupported: true,
        analystPathAReason: null,
      },
      {
        id: 'svc-2',
        name: 'Staging',
        accessLevel: 'read_only',
        cmcdEnabled: undefined,
        rum_enabled: undefined,
        analystPathASupported: true,
        analystPathAReason: null,
      },
    ])
  })

  it('maps service_id → id, name → name, access_level → accessLevel and topology capability', async () => {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [
            {
              service_id: 'abc',
              name: 'Test',
              access_level: 'read_only',
              analyst_path_a_supported: false,
              analyst_path_a_reason: 'Use Path B.',
            },
          ],
          active_service_id: null,
        }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
    const [mappedServices] = mockSetServices.mock.calls[0]
    expect(mappedServices[0]).toEqual({
      id: 'abc',
      name: 'Test',
      accessLevel: 'read_only',
      cmcdEnabled: undefined,
      rum_enabled: undefined,
      analystPathASupported: false,
      analystPathAReason: 'Use Path B.',
    })
    expect(mappedServices[0].service_id).toBeUndefined()
    expect(mappedServices[0].access_level).toBeUndefined()
    expect(mappedServices[0].analyst_path_a_supported).toBeUndefined()
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
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({ detail: 'boom' }, { status: 500 }),
      ),
    )
    const { useBootstrap } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useBootstrap(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 3000 })
    expect(mockSetServices).not.toHaveBeenCalled()
    expect(mockSetInitialized).not.toHaveBeenCalled()
  })

  describe('reconcile — stale snapshot must not evict a just-selected service', () => {
    function staleWrapper(seed: { services: unknown[]; active_service_id: string | null }) {
      const qc = createTestQueryClient({ queries: { gcTime: 0 } })
      qc.setQueryData(['bootstrap'], seed, { updatedAt: Date.now() - 10 * 60 * 1000 })
      server.use(
        http.get(`${API_BASE}/api/bootstrap`, () => new Promise(() => {})),
      )
      return makeQueryWrapper(qc)
    }

    it('does NOT revert when the selected service is absent from a STALE snapshot', async () => {
      mockServiceState.activeServiceId = 'svc-new'
      const wrap = staleWrapper({
        services: [{ service_id: 'svc-old', name: 'Old', access_level: 'read_write' }],
        active_service_id: 'svc-old',
      })

      const { useBootstrap } = await import('@/hooks/useBootstrap')
      renderHook(() => useBootstrap(), { wrapper: wrap })

      await waitFor(() => expect(mockSetServices).toHaveBeenCalled())
      await new Promise((r) => setTimeout(r, 50))
      expect(mockSetActiveServiceId).not.toHaveBeenCalledWith('svc-old')
      expect(mockSetActiveServiceId).not.toHaveBeenCalled()
    })

    it('STILL reverts a stale-id selection when the snapshot is FRESH', async () => {
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

    failing = false

    await waitFor(() => expect(result.current.isSuccess).toBe(true), { timeout: 15000 })
    expect(mockSetServices).toHaveBeenCalled()
  }, 30000)

  it('does not keep polling once the request succeeds', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
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
      await vi.advanceTimersByTimeAsync(8000)
      expect(calls).toBe(afterFirstSuccess)
    } finally {
      vi.useRealTimers()
    }
  }, 30000)
})

function catalogWrapper(catalogSeed?: unknown, bootstrapSeed?: unknown) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (catalogSeed !== undefined) {
    qc.setQueryData(queryKeys.logFieldsCatalog('svc-1'), catalogSeed)
  }
  if (bootstrapSeed !== undefined) {
    qc.setQueryData(queryKeys.bootstrap(), bootstrapSeed)
  }
  return makeQueryWrapper(qc)
}

describe('useLogFieldsCatalog (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockServiceState.activeServiceId = 'svc-1'
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
    const { useLogFieldsCatalog } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useLogFieldsCatalog(), { wrapper: catalogWrapper() })

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
    const { useLogFieldsCatalog } = await import('@/hooks/useBootstrap')
    renderHook(() => useLogFieldsCatalog(), { wrapper: catalogWrapper() })

    await waitFor(() => expect(observedSid).not.toBeNull())
    expect(observedSid).toBe('svc-1')
  })

  it('returns isError when the API 500s', async () => {
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ error: 'oops' }, { status: 500 }),
      ),
    )
    const { useLogFieldsCatalog } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useLogFieldsCatalog(), { wrapper: catalogWrapper() })

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
    vi.clearAllMocks()
    mockServiceState.activeServiceId = 'svc-1'
  })

  it('reports not-ready and treats everything as active until bootstrap lands', async () => {
    server.use(http.get(`${API_BASE}/api/bootstrap`, () => new Promise(() => {})))
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), { wrapper: catalogWrapper(CATALOG) })
    expect(result.current.ready).toBe(false)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('anything')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(true)
  })

  it('treats an empty active_log_field_ids list as not-ready (everything active)', async () => {
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: [] }),
    })
    expect(result.current.ready).toBe(false)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
  })

  it('honours active_log_field_ids when populated', async () => {
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['ottfb', 'country'] }),
    })
    expect(result.current.ready).toBe(true)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('ja3')).toBe(false)
  })

  it('isGroupActive is true when ANY field of the group is active', async () => {
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['country'] }),
    })
    expect(result.current.isGroupActive('D')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(false)
  })

  it('isGroupActive is false for an unknown group id', async () => {
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(CATALOG, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['ottfb'] }),
    })
    expect(result.current.isGroupActive('ZZ')).toBe(false)
  })

  it('does not throw when the catalog is missing (still resolves fields)', async () => {
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ error: 'missing' }, { status: 500 }),
      ),
    )
    const { useActiveLogFields } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useActiveLogFields(), {
      wrapper: catalogWrapper(undefined, { services: [], active_service_id: 'svc-1', active_log_field_ids: ['ottfb'] }),
    })
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(false)
  })
})
