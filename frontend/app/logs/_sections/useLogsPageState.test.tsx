import '@testing-library/jest-dom/vitest'
import React from 'react'
import { renderHook, act, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { queryKeys } from '@/lib/api'

const queryStateValues: Record<string, string> = {}

vi.mock('nuqs', () => ({
  parseAsString: {
    withDefault: (def: string) => ({ defaultValue: def }),
  },
  useQueryState: (key: string, opts: { defaultValue: string }) => {
    const [val, setVal] = React.useState<string>(() => queryStateValues[key] ?? opts.defaultValue)
    const set = React.useCallback((next: string) => {
      queryStateValues[key] = next
      setVal(next)
      return Promise.resolve(new URLSearchParams())
    }, [key])
    return [val, set] as const
  },
}))

const mockGet = vi.fn()
const mockDelete = vi.fn()

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    client: {
      GET: (...args: unknown[]) => mockGet(...args),
      DELETE: (...args: unknown[]) => mockDelete(...args),
    },
    unwrapClient: async (p: Promise<{ data?: unknown; error?: unknown }>) => {
      const res = await p
      if (res.error) throw res.error
      return res.data
    },
  }
})

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    status: 'idle',
    error: null,
    start: vi.fn(),
    stop: vi.fn(),
    reset: vi.fn(),
  }),
  useSyncStatus: () => ({ data: undefined }),
}))

vi.mock('@/hooks/useBootstrap', () => ({
  useEffectiveServiceId: () => 'svc-1',
  useLogFieldsCatalog: () => ({ data: undefined }),
}))

vi.mock('@/lib/utils', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/utils')>()
  return {
    ...actual,
    showToastWithAction: vi.fn(),
  }
})

import { useLogsPageState } from './useLogsPageState'

function makeWrapper(qc: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  }
}

describe('useLogsPageState cron deduplication and invalidation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    for (const k of Object.keys(queryStateValues)) delete queryStateValues[k]
    mockGet.mockImplementation(async (path: string, opts?: { params?: { query?: Record<string, unknown> } }) => {
      if (path === '/api/cron-runs') {
        const q = opts?.params?.query ?? {}
        const allEntries = [
          { id: 12, task: 'request_ingest', status: 'running', started_at: '2026-11-24T00:02:00Z' },
          { id: 11, task: 'raw_delete', status: 'error', started_at: '2026-11-24T00:01:00Z' },
          { id: 10, task: 'request_ingest', status: 'success', started_at: '2026-11-24T00:00:00Z' },
        ]
        const filtered = allEntries.filter(
          (e) => (!q.task || e.task === q.task) && (!q.status || e.status === q.status),
        )
        return { data: { entries: filtered, total: filtered.length, page: 1, per_page: Number(q.per_page ?? 500) } }
      }
      if (path === '/api/cron-schedule') {
        return { data: { schedules: [{ task: 'request_ingest', next_run_time: null }], clock: null } }
      }
      return { data: {} }
    })
    mockDelete.mockResolvedValue({ data: { deleted: 3 } })
  })

  it('does not issue a duplicate /api/cron-runs recent query on initial unfiltered cron tab load', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const { result } = renderHook(() => useLogsPageState('cron'), { wrapper: makeWrapper(qc) })

    await waitFor(() => expect(result.current.cronLogs?.entries).toHaveLength(3))

    const cronRunCalls = mockGet.mock.calls.filter(([path]) => path === '/api/cron-runs')
    expect(cronRunCalls).toHaveLength(1)
    expect(cronRunCalls[0][1]?.params?.query).toMatchObject({ page: 1, per_page: 500 })
    expect(result.current.recentCrons?.entries).toHaveLength(3)
    expect(result.current.displayedJobs.some((j) => j.id === 12 && j.status === 'running')).toBe(true)
  })

  it('fetches filtered cronLogs and enables recentCrons when task or status filter is applied', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const { result } = renderHook(() => useLogsPageState('cron'), { wrapper: makeWrapper(qc) })

    await waitFor(() => expect(result.current.cronLogs?.entries).toHaveLength(3))

    act(() => {
      void result.current.setTaskFilter('raw_delete')
    })

    await waitFor(() => {
      expect(result.current.cronLogs?.entries).toHaveLength(1)
      expect(result.current.cronLogs?.entries?.[0]?.task).toBe('raw_delete')
    })

    const cronRunCalls = mockGet.mock.calls.filter(([path]) => path === '/api/cron-runs')
    expect(cronRunCalls.some(([, opts]) => opts?.params?.query?.task === 'raw_delete')).toBe(true)
    expect(cronRunCalls.some(([, opts]) => opts?.params?.query?.per_page === 10)).toBe(true)
  })

  it('invalidates cronLogs, cronLogsRecent, cronSchedule, and lastSync after purgeMutation', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    const { result } = renderHook(() => useLogsPageState('cron'), { wrapper: makeWrapper(qc) })

    await waitFor(() => expect(result.current.cronLogs?.entries).toHaveLength(3))

    await act(async () => {
      result.current.purgeMutation.mutate()
    })

    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith('/api/cron-runs', {}))
    const invalidatedKeys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(invalidatedKeys).toContainEqual(queryKeys.admin.cronLogsFor('svc-1'))
    expect(invalidatedKeys).toContainEqual(queryKeys.admin.cronLogsRecent('svc-1'))
    expect(invalidatedKeys).toContainEqual(queryKeys.admin.cronSchedule('svc-1'))
    expect(invalidatedKeys).toContainEqual(queryKeys.lastSync('svc-1'))
  })

  it('invalidates unfiltered cronLogs on running SSE event when recentCrons has no active observer', async () => {
    vi.useFakeTimers()
    try {
      const { makeCronRunsApplier } = await import('@/hooks/sseStreamAppliers')
      const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
      const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
      const applier = makeCronRunsApplier(qc, 'svc-1')
      applier.apply({ task: 'log_discovery', status: 'running' })
      vi.advanceTimersByTime(150)
      const invalidatedKeys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
      expect(invalidatedKeys).toContainEqual(queryKeys.admin.cronLogs('svc-1', 'all', 'all'))
      applier.cleanup()
    } finally {
      vi.useRealTimers()
    }
  })

  it('skips redundant SSE refetch when cronLogs and cronSchedule were just fetched', async () => {
    vi.useFakeTimers()
    try {
      const { makeCronRunsApplier } = await import('@/hooks/sseStreamAppliers')
      const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
      const cronKey = queryKeys.admin.cronLogs('svc-1', 'all', 'all')
      const schedKey = queryKeys.admin.cronSchedule('svc-1')
      qc.setQueryData(cronKey, { entries: [], total: 0 })
      qc.setQueryData(schedKey, { tasks: [] })

      const applier = makeCronRunsApplier(qc, 'svc-1')
      // Immediate SSE event right after fresh data (< 2000ms) must not mark queries invalidated
      applier.apply({ task: 'log_discovery', status: 'completed' })
      vi.advanceTimersByTime(150)
      expect(qc.getQueryState(cronKey)?.isInvalidated).toBe(false)
      expect(qc.getQueryState(schedKey)?.isInvalidated).toBe(false)

      // Once >= 2000ms have passed, SSE event invalidates normally
      vi.advanceTimersByTime(2_100)
      applier.apply({ task: 'log_discovery', status: 'completed' })
      vi.advanceTimersByTime(150)
      expect(qc.getQueryState(cronKey)?.isInvalidated).toBe(true)
      expect(qc.getQueryState(schedKey)?.isInvalidated).toBe(true)
      applier.cleanup()
    } finally {
      vi.useRealTimers()
    }
  })
})
