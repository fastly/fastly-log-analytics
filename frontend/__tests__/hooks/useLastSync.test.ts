/**
 * useLastSync — fetches the latest sync-task row from /api/cron-runs
 * and surfaces it as the "Last Sync: Xs ago" header badge data.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0, staleTime: 0 } })
  return makeQueryWrapper(qc)
}

describe.skip('useLastSync (MSW)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
    }
  })

  it('returns the latest sync run\'s fields and forwards task=sync,per_page=1', async () => {
    let observedTask: string | null = null
    let observedPerPage: string | null = null
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, ({ request }) => {
        const u = new URL(request.url)
        observedTask = u.searchParams.get('task')
        observedPerPage = u.searchParams.get('per_page')
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

    const { useLastSync } = await import('@/hooks/useLastSync')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data?.started_at).toBe('2026-06-15T23:37:41Z')
    expect(result.current.data?.status).toBe('success')
    expect(result.current.data?.duration_s).toBe(7.2)
    expect(observedTask).toBe('sync')
    expect(observedPerPage).toBe('1')
  })

  it('returns null fields when there are no sync runs yet', async () => {
    server.use(
      http.get(`${API_BASE}/api/cron-runs`, () =>
        HttpResponse.json({ total: 0, page: 1, per_page: 1, entries: [] }),
      ),
    )

    const { useLastSync } = await import('@/hooks/useLastSync')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper() })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
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

    const { useLastSync } = await import('@/hooks/useLastSync')
    const { result } = renderHook(() => useLastSync(), { wrapper: wrapper() })

    await new Promise(r => setTimeout(r, 50))
    expect(callCount).toBe(0)
    expect(result.current.data).toBeNull()
  })
})
