/**
 * R-10: `useSyncStatus` is the single source of truth for
 * `GET /api/sync-status`. Pinned because the perf audit found
 * 6-8 concurrent calls per cold dashboard load when defaults leaked.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'
import { queryKeys } from '@/lib/query-keys'

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    { getState: () => mockState },
  )
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

// The hook gates its query on useSeededReady, which pre-seeds `null` into the
// ['sync-status', id] cache entry (production seeds it from bootstrap). A fresh
// seed satisfies the hook's 60 s staleTime, so no fetch fires until the entry is
// stale or invalidated. Seed an already-stale entry to exercise the real queryFn;
// gcTime Infinity keeps the observer-less seed alive until the hook mounts.
function wrapper(opts: { staleSeed?: boolean } = {}) {
  const qc = createTestQueryClient({ queries: { gcTime: Infinity, staleTime: 0 } })
  if (opts.staleSeed) qc.setQueryData(queryKeys.syncStatus('svc-1'), null, { updatedAt: 0 })
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
    const { useSyncStatus } = await import('@/hooks/useSyncStatus')
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
    const { useSyncStatus } = await import('@/hooks/useSyncStatus')
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
    const { useSyncStatus } = await import('@/hooks/useSyncStatus')
    const { result } = renderHook(() => useSyncStatus(), { wrapper: wrapper({ staleSeed: true }) })

    // give react-query a tick to NOT fire
    await new Promise((r) => setTimeout(r, 50))
    expect(callCount).toBe(0)
    // No fetch: the hook only exposes the null seed useSeededReady wrote.
    expect(result.current.data).toBeNull()
  })
})
