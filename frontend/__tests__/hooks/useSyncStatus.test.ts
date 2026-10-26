/**
 * R-10: Tests for `useSyncStatus`, `useLastSync`, `useIsDataReady`, and `useHeaderBadgeStream`.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import {
  createTestQueryClient,
  makeQueryWrapper,
  makeStreamResponse,
  makeNeverClosingStreamResponse,
} from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { queryKeys } from '@/lib/query-keys'

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
}

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    { getState: () => mockState },
  )
  return { ...actual, useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper(opts: { staleSeed?: boolean; staleLastSyncSeed?: boolean; seedKey?: string; seedValue?: unknown } = {}) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (opts.staleSeed) qc.setQueryData(queryKeys.syncStatus('svc-1'), null, { updatedAt: 0 })
  if (opts.staleLastSyncSeed) qc.setQueryData(queryKeys.lastSync('svc-1'), null, { updatedAt: 0 })
  if (opts.seedKey) qc.setQueryData([opts.seedKey], opts.seedValue)
  return makeQueryWrapper(qc)
}

function makeQueryClient(seedBootstrap?: Record<string, unknown>) {
  const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
  if (seedBootstrap) {
    qc.setQueryData(['bootstrap'], seedBootstrap)
  }
  return qc
}

function wrapperWith(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

describe('useSyncStatus (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
    }
  })

  it('returns the parsed sync-status payload on the happy path', async () => {
    server.use(
      http.get(`${API_BASE}/api/sync-status`, () =>
        HttpResponse.json({
          latest_log_at: '2026-06-14T00:00:00Z',
          local_rows: 12345,
          fos_rows: null,
          last_sync_at: '2026-06-14T00:01:00Z',
        }),
      ),
    )
    const { useSyncStatus } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSyncStatus(), { wrapper: wrapper({ staleSeed: true }) })

    await waitFor(() => expect(result.current.data?.local_rows).toBe(12345))
    expect(result.current.data?.local_rows).toBe(12345)
    expect(result.current.data?.latest_log_at).toBe('2026-06-14T00:00:00Z')
  })

  it('forwards skip_fos=true (the perf-driven default that keeps the live FOS scan off the page-shell path)', async () => {
    let observedSkipFos: string | null = null
    server.use(
      http.get(`${API_BASE}/api/sync-status`, ({ request }) => {
        observedSkipFos = new URL(request.url).searchParams.get('skip_fos')
        return HttpResponse.json({ latest_log_at: null, local_rows: 0 })
      }),
    )
    const { useSyncStatus } = await import('@/hooks/useSSE')
    renderHook(() => useSyncStatus(), { wrapper: wrapper({ staleSeed: true }) })

    await waitFor(() => expect(observedSkipFos).not.toBeNull())
    expect(observedSkipFos).toBe('true')
  })

  it('is disabled (no fetch) for analyst sessions', async () => {
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    }
    let callCount = 0
    server.use(
      http.get(`${API_BASE}/api/sync-status`, () => {
        callCount++
        return HttpResponse.json({ latest_log_at: null, local_rows: 0 })
      }),
    )
    const { useSyncStatus } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSyncStatus(), { wrapper: wrapper({ staleSeed: true }) })

    await new Promise((r) => setTimeout(r, 50))
    expect(callCount).toBe(0)
    expect(result.current.data).toBeNull()
  })
})

describe('useLastSync (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
    }
  })

  it('returns the latest sync run\'s fields and forwards task=log_discovery,per_page=1,skip_total=true', async () => {
    let observedTask: string | null = null
    let observedPerPage: string | null = null
    let observedSkipTotal: string | null = null
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, ({ request }) => {
        const u = new URL(request.url)
        observedTask = u.searchParams.get('task')
        observedPerPage = u.searchParams.get('per_page')
        observedSkipTotal = u.searchParams.get('skip_total')
        return HttpResponse.json({
          total: 1,
          page: 1,
          per_page: 1,
          entries: [
            {
              id: 195883,
              task: 'sync',
              started_at: '2026-06-15T23:37:41Z',
              duration_s: 7.2,
              status: 'success',
              error_message: null,
              files_downloaded: 12,
              rows_ingested: 15,
            },
          ],
        })
      }),
    )

    const { useLastSync } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper({ staleLastSyncSeed: true }) })

    await waitFor(() => expect(result.current.data?.started_at).toBe('2026-06-15T23:37:41Z'))
    expect(result.current.data?.started_at).toBe('2026-06-15T23:37:41Z')
    expect(result.current.data?.status).toBe('success')
    expect(result.current.data?.duration_s).toBe(7.2)
    expect(observedTask).toBe('log_discovery')
    expect(observedPerPage).toBe('1')
    expect(observedSkipTotal).toBe('true')
  })

  it('returns null fields when there are no sync runs yet', async () => {
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, () =>
        HttpResponse.json({ total: 0, page: 1, per_page: 1, entries: [] }),
      ),
    )

    const { useLastSync } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper({ staleLastSyncSeed: true }) })

    await waitFor(() => expect(result.current.data).toEqual({ started_at: null, status: null, duration_s: null }))
    expect(result.current.data).toEqual({ started_at: null, status: null, duration_s: null })
  })

  it('is disabled (no fetch) for analyst sessions', async () => {
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    }
    let callCount = 0
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, () => {
        callCount++
        return HttpResponse.json({ total: 0, page: 1, per_page: 1, entries: [] })
      }),
    )

    const { useLastSync } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper({ staleLastSyncSeed: true }) })

    await new Promise(r => setTimeout(r, 50))
    expect(callCount).toBe(0)
    expect(result.current.data).toBeNull()
  })
})

describe('useIsDataReady', () => {
  beforeEach(() => {
    mockState = {
      activeServiceId: null,
      services: [],
    }
    vi.clearAllMocks()
  })

  it('returns false when no service id is set anywhere', async () => {
    const { useIsDataReady } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useIsDataReady(), { wrapper: wrapper() })
    expect(result.current).toBe(false)
  })

  it('returns true when the Zustand store has an active service id', async () => {
    mockState = {
      activeServiceId: 'svc-from-store',
      services: [],
    }
    const { useIsDataReady } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useIsDataReady(), { wrapper: wrapper() })
    expect(result.current).toBe(true)
  })

  it('falls back to bootstrap.active_service_id when the store is empty (SSR-hydrated first render)', async () => {
    mockState = {
      activeServiceId: null,
      services: [],
    }
    const { useIsDataReady } = await import('@/hooks/useBootstrap')
    const { result } = renderHook(() => useIsDataReady(), {
      wrapper: wrapper({ seedKey: 'bootstrap', seedValue: { active_service_id: 'svc-from-bootstrap' } }),
    })
    expect(result.current).toBe(true)
  })
})

describe('useHeaderBadgeStream', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn())
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    }
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it.each([
    { latest_log_at: null, local_rows: null, request: null, rum: null },
    {
      latest_log_at: null,
      local_rows: 0,
      request: { latest_log_at: null, total_rows: 0 },
      rum: { latest_log_at: null, total_rows: 0 },
    },
  ])('preserves explicit clears from the stream instead of reviving cached metrics: %j', async (cleared) => {
    const seed = {
      settings: { is_remote_analyst: true },
      header_badge: {
        latest_log_at: '2026-06-15T22:00:00Z',
        local_rows: 1000,
        request: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 900 },
        rum: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 100 },
      },
    }
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: ${JSON.stringify(cleared)}\r\n\r\n`]),
    )
    const qc = makeQueryClient(seed)
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })
    await waitFor(() => {
      expect(qc.getQueryData(['bootstrap'])).toEqual({ ...seed, header_badge: cleared })
    })
  })

  it('accepts backwards timestamps while preserving omitted fields in sparse updates', async () => {
    const seed = {
      header_badge: {
        latest_log_at: '2026-06-15T22:00:00Z',
        local_rows: 1000,
        request: { latest_log_at: '2026-06-15T22:00:00Z', total_rows: 900 },
        rum: { latest_log_at: '2026-06-15T21:00:00Z', total_rows: 100 },
      },
    }
    const earlier = '2026-06-14T22:00:00Z'
    const payload = { latest_log_at: earlier, request: { latest_log_at: earlier } }
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: ${JSON.stringify(payload)}\r\n\r\n`]),
    )
    const qc = makeQueryClient(seed)
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })
    await waitFor(() => {
      expect(qc.getQueryData(['bootstrap'])).toEqual({
        header_badge: {
          ...seed.header_badge,
          latest_log_at: earlier,
          request: { ...seed.header_badge.request, latest_log_at: earlier },
        },
      })
    })
  })

  it('merges incoming payload into bootstrap.header_badge', async () => {
    const seed = { settings: { is_remote_analyst: true }, header_badge: { latest_log_at: '2026-06-15T22:00:00Z', local_rows: 1000 } }
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ latest_log_at: '2026-06-15T22:05:00Z', local_rows: 1234 })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient(seed)
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const bs = qc.getQueryData<{
        settings?: { is_remote_analyst?: boolean }
        header_badge?: { latest_log_at?: string; local_rows?: number }
      }>(['bootstrap'])
      expect(bs?.header_badge?.latest_log_at).toBe('2026-06-15T22:05:00Z')
      expect(bs?.header_badge?.local_rows).toBe(1234)
      expect(bs?.settings?.is_remote_analyst).toBe(true)
    })
  })

  it('does not crash when bootstrap cache is empty (cold load)', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ latest_log_at: '2026-06-15T22:05:00Z', local_rows: 1234 })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await act(async () => {
      await new Promise(r => setTimeout(r, 50))
    })
    expect(qc.getQueryData(['bootstrap'])).toBeUndefined()
  })

  it('does NOT open a connection when disabled is false', async () => {
    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(false), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('uses /api/log-extents/stream (the analyst-safe endpoint, NOT /api/sync-status/stream)', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: ${JSON.stringify({ latest_log_at: 'x', local_rows: 1 })}\r\n\r\n`]),
    )

    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    const [url] = vi.mocked(fetch).mock.calls[0]
    expect(String(url)).toContain('/api/log-extents/stream')
    expect(String(url)).not.toContain('/api/sync-status/stream')
  })

  it('aborts on unmount', async () => {
    let abortSignal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url: RequestInfo | URL, init?: RequestInit) => {
      abortSignal = init?.signal ?? undefined
      return makeNeverClosingStreamResponse()
    })

    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useSSE')
    const { unmount } = renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(abortSignal?.aborted).toBe(false)
    act(() => unmount())
    expect(abortSignal?.aborted).toBe(true)
  })
})
