/**
 * @vitest-environment jsdom
 *
 * useEventStream & useAdminEventStream tests.
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  runEventStream,
  useEventStream,
  EventStreamHttpError,
  type EventStreamRequest,
} from '@/hooks/useEventStream'
import { makeStreamResponse, makeNeverClosingStreamResponse } from '../helpers/sse-mock'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'

import { adminStreamHooks } from '@/hooks/useAdminEventStream'

// Capture what useAdminEventStream registers with useServiceStream.
let capturedOnEvent: ((raw: string) => void) | null = null
let lastEnabled: boolean | undefined
let lastPath: string | undefined
let lastOpts: Record<string, unknown> | undefined

let bootstrapSettled = true
vi.mock('@/hooks/useIsDataReady', () => ({
  useBootstrapSettled: () => bootstrapSettled,
}))

const mockState = { activeServiceId: 'svc-1' as string | null }
vi.mock('@/stores/serviceStore', () => {
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    { getState: () => mockState },
  )
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

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('runEventStream', () => {
  it('parses CRLF-separated frames (sse-starlette emits \\r\\n\\r\\n)', async () => {
    const frames: string[] = []
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValue(makeStreamResponse(['data: a\r\n\r\n', 'data: b\r\n\r\n']))
    await runEventStream({
      url: 'http://test/stream',
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
      fetchImpl: fetchImpl,
    })
    expect(frames).toEqual(['data: a', 'data: b'])
  })

  it('parses LF and old-Mac boundaries too', async () => {
    const frames: string[] = []
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValue(makeStreamResponse(['data: lf\n\n', 'data: mac\r\r']))
    await runEventStream({
      url: 'http://test/stream',
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
      fetchImpl: fetchImpl,
    })
    expect(frames).toEqual(['data: lf', 'data: mac'])
  })

  it('reassembles a frame split across chunks', async () => {
    const frames: string[] = []
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValue(makeStreamResponse(['data: hel', 'lo\r\n\r\n']))
    await runEventStream({
      url: 'http://test/stream',
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
      fetchImpl: fetchImpl,
    })
    expect(frames).toEqual(['data: hello'])
  })

  it('throws EventStreamHttpError with the status on a non-2xx response', async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 503 }))
    await expect(
      runEventStream({
        url: 'http://test/stream',
        signal: new AbortController().signal,
        onFrame: () => {},
        fetchImpl: fetchImpl,
      }),
    ).rejects.toMatchObject({ status: 503 })
  })

  it('preserves the historical useSSE error message text', () => {
    expect(new EventStreamHttpError(500).message).toBe('HTTP error! status: 500')
  })

  it('stops dispatching frames once isActive() goes false', async () => {
    const frames: string[] = []
    let live = true
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValue(makeStreamResponse(['data: first\r\n\r\n', 'data: second\r\n\r\n']))
    await runEventStream({
      url: 'http://test/stream',
      signal: new AbortController().signal,
      onFrame: (f) => {
        frames.push(f)
        live = false
      },
      isActive: () => live,
      fetchImpl: fetchImpl,
    })
    expect(frames).toEqual(['data: first'])
  })

  it('fires onOpen after the reader is obtained', async () => {
    const onOpen = vi.fn()
    const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(makeStreamResponse(['data: x\r\n\r\n']))
    await runEventStream({
      url: 'http://test/stream',
      signal: new AbortController().signal,
      onOpen,
      onFrame: () => {},
      fetchImpl: fetchImpl,
    })
    expect(onOpen).toHaveBeenCalledTimes(1)
  })
})

describe('useEventStream', () => {
  const req: EventStreamRequest = { url: 'http://test/stream' }

  it('does not fetch while disabled', async () => {
    const f = vi.fn()
    vi.stubGlobal('fetch', f)
    renderHook(() => useEventStream(false, req, () => {}))
    await new Promise((r) => setTimeout(r, 20))
    expect(f).not.toHaveBeenCalled()
  })

  it('delivers each data payload (trimmed, data: stripped) to onEvent', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(makeStreamResponse(['data: hello\r\n\r\n'])),
    )
    const onEvent = vi.fn()
    renderHook(() => useEventStream(true, req, onEvent))
    await waitFor(() => expect(onEvent).toHaveBeenCalledWith('hello'))
  })

  it('reaches open state on a successful connect', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeNeverClosingStreamResponse(['data: x\r\n\r\n'])))
    const { result } = renderHook(() => useEventStream(true, req, () => {}))
    await waitFor(() => expect(result.current.state).toBe('open'))
  })

  it('does NOT reconnect on 401/403 (terminal auth failure)', async () => {
    const f = vi.fn().mockResolvedValue(new Response(null, { status: 403 }))
    vi.stubGlobal('fetch', f)
    renderHook(() => useEventStream(true, req, () => {}))
    await new Promise((r) => setTimeout(r, 50))
    expect(f).toHaveBeenCalledTimes(1)
  })

  it('reconnects with exponential backoff after a dropped connection', async () => {
    vi.useFakeTimers()
    const f = vi.fn().mockImplementation(() => Promise.resolve(new Response(null, { status: 500 })))
    vi.stubGlobal('fetch', f)
    renderHook(() => useEventStream(true, req, () => {}))

    await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(1))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(2))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(f).toHaveBeenCalledTimes(2)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(3))
  })

  it('aborts the in-flight connection on unmount', async () => {
    let capturedSignal: AbortSignal | undefined
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((_url: string, init: RequestInit) => {
        capturedSignal = init.signal as AbortSignal
        return Promise.resolve(makeStreamResponse(['data: x\r\n\r\n']))
      }),
    )
    const { unmount } = renderHook(() => useEventStream(true, req, () => {}))
    await waitFor(() => expect(capturedSignal).toBeDefined())
    unmount()
    expect(capturedSignal?.aborted).toBe(true)
  })
})

function wrapper(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

function makeQc() {
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

describe('useAdminEventStream', () => {
  beforeEach(() => {
    capturedOnEvent = null
    lastEnabled = undefined
    lastPath = undefined
    lastOpts = undefined
    bootstrapSettled = true
    mockState.activeServiceId = 'svc-1'
    vi.clearAllMocks()
    vi.spyOn(adminStreamHooks, 'useServiceStream').mockImplementation(
      (enabled, path, onEvent, opts) => {
        lastEnabled = enabled
        lastPath = path
        capturedOnEvent = onEvent
        lastOpts = opts as Record<string, unknown> | undefined
        return { state: 'open' as const }
      },
    )
  })

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
