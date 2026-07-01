/**
 * useAdminEventStream — single multiplexed connection that demuxes the
 * ``{channel, data}`` envelope to the per-channel appliers. These tests
 * pin the DEMUX WIRING (right applier per channel, gating, path/opts);
 * the per-channel behaviour itself is covered by admin-stream-apply.test.ts.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, vi } from 'vitest'

// Capture what the hook registers with useServiceStream.
let capturedOnEvent: ((raw: string) => void) | null = null
let lastEnabled: boolean | undefined
let lastPath: string | undefined
let lastOpts: Record<string, unknown> | undefined

vi.mock('@/hooks/useServiceStream', () => ({
  useServiceStream: vi.fn(
    (enabled: boolean, path: string, onEvent: (raw: string) => void, opts?: Record<string, unknown>) => {
      lastEnabled = enabled
      lastPath = path
      capturedOnEvent = onEvent
      lastOpts = opts
      return { state: 'open' }
    },
  ),
}))

let bootstrapSettled = true
vi.mock('@/hooks/useIsDataReady', () => ({
  useBootstrapSettled: () => bootstrapSettled,
}))

const mockState = { activeServiceId: 'svc-1' as string | null }
vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

const cronApply = vi.fn()
const cronCleanup = vi.fn()
vi.mock('@/lib/admin-stream-apply', () => ({
  applyShare: vi.fn(),
  applySyncStatus: vi.fn(),
  applySystemMetrics: vi.fn(),
  makeCronRunsApplier: vi.fn(() => ({ apply: cronApply, cleanup: cronCleanup })),
}))

import { applyShare, applySyncStatus, applySystemMetrics } from '@/lib/admin-stream-apply'

function wrapper(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

function makeQc() {
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

beforeEach(() => {
  capturedOnEvent = null
  lastEnabled = undefined
  lastPath = undefined
  lastOpts = undefined
  bootstrapSettled = true
  mockState.activeServiceId = 'svc-1'
  vi.clearAllMocks()
})

describe('useAdminEventStream', () => {
  it('connects to /api/admin/events/stream with sorted channels + optionalService opts', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['sync-status', 'cron-runs', 'system-metrics']), {
      wrapper: wrapper(makeQc()),
    })
    expect(lastEnabled).toBe(true)
    expect(lastPath).toBe('/api/admin/events/stream?channels=cron-runs,sync-status,system-metrics')
    expect(lastOpts).toMatchObject({ optionalService: true, cache: 'no-store' })
  })

  it('holds the stream disabled until bootstrap settles', async () => {
    bootstrapSettled = false
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(lastEnabled).toBe(false)
  })

  it('forwards enabled=false', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(false, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(lastEnabled).toBe(false)
  })

  it('demuxes sync-status envelope to applySyncStatus(qc, serviceId, data)', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    const data = { local_rows: 5 }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'sync-status', data })))
    expect(applySyncStatus).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
  })

  it('demuxes system-metrics envelope to applySystemMetrics(qc, serviceId, data)', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['system-metrics']), { wrapper: wrapper(makeQc()) })
    const data = { health_snapshot: { status: 'ok' } }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'system-metrics', data })))
    expect(applySystemMetrics).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
  })

  it('demuxes cron-runs envelope to the cron applier', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['cron-runs']), { wrapper: wrapper(makeQc()) })
    const data = { task: 'sync', status: 'success' }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'cron-runs', data })))
    expect(cronApply).toHaveBeenCalledWith(data)
  })

  it('demuxes share envelope to applyShare(qc, data)', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['share']), { wrapper: wrapper(makeQc()) })
    const data = { sharing_active: true, active_session_count: 1 }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'share', data })))
    expect(applyShare).toHaveBeenCalledWith(expect.anything(), data)
  })

  it('ignores malformed envelopes and unknown channels without throwing', async () => {
    const { useAdminEventStream } = await import('@/hooks/useAdminEventStream')
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(() => act(() => capturedOnEvent!('not-json'))).not.toThrow()
    expect(() => act(() => capturedOnEvent!(JSON.stringify({ channel: 'mystery', data: {} })))).not.toThrow()
    expect(applySyncStatus).not.toHaveBeenCalled()
    expect(applySystemMetrics).not.toHaveBeenCalled()
    expect(cronApply).not.toHaveBeenCalled()
  })
})
