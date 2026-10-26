/**
 * @vitest-environment jsdom
 *
 * Consolidated SSE test suite: runEventStream, useEventStream, useServiceStream,
 * and useAdminEventStream.
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  runEventStream,
  useEventStream,
  useServiceStream,
  useAdminEventStream,
  adminStreamHooks,
  EventStreamHttpError,
  type EventStreamRequest,
} from '@/hooks/useSSE'
import {
  makeStreamResponse,
  makeNeverClosingStreamResponse,
  createTestQueryClient,
  makeQueryWrapper,
} from '../helpers/page-smoke'

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  getApiBase: () => 'http://test',
}))

let mockState: { activeServiceId: string | null } = { activeServiceId: 'svc-1' }
let mockHydrated = true
let finishHydrationCb: (() => void) | null = null

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockState) => unknown) => (selector ? selector(mockState) : mockState)),
    {
      getState: () => mockState,
      persist: {
        hasHydrated: () => mockHydrated,
        onFinishHydration: (cb: () => void) => {
          finishHydrationCb = cb
          return () => { finishHydrationCb = null }
        },
        rehydrate: () => {},
      },
    },
  )
  return { ...actual, useServiceStore }
})

let bootstrapSettled = true
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrapSettled: () => bootstrapSettled,
}))

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
  mockState = { activeServiceId: 'svc-1' }
  mockHydrated = true
  finishHydrationCb = null
  bootstrapSettled = true
})

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

describe('useServiceStream', () => {
  it('short-circuits when enabled=false (no fetch fired)', async () => {
    renderHook(() => useServiceStream(false, '/api/x', () => {}))
    await new Promise(r => setTimeout(r, 20))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('short-circuits when activeServiceId is null (no fetch fired)', async () => {
    mockState = { activeServiceId: null }
    renderHook(() => useServiceStream(true, '/api/x', () => {}))
    await new Promise(r => setTimeout(r, 20))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('parses LF (\\n\\n) event boundaries', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse(['data: alpha\n\ndata: beta\n\n']),
    )
    const events: string[] = []
    renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
    await waitFor(() => expect(events).toEqual(['alpha', 'beta']))
  })

  it('parses old-Mac (\\r\\r) event boundaries', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse(['data: one\r\rdata: two\r\r']),
    )
    const events: string[] = []
    renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
    await waitFor(() => expect(events).toEqual(['one', 'two']))
  })

  it('aborts the in-flight stream on unmount', async () => {
    let signal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url, init) => {
      signal = init?.signal as AbortSignal
      return makeNeverClosingStreamResponse()
    })
    const { unmount } = renderHook(() => useServiceStream(true, '/api/x', () => {}))

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(signal?.aborted).toBe(false)
    act(() => unmount())
    expect(signal?.aborted).toBe(true)
  })

  it('aborts the prior stream and opens a new one when activeServiceId changes', async () => {
    const signals: AbortSignal[] = []
    const ids: string[] = []
    vi.mocked(fetch).mockImplementation(async (_url, init) => {
      const ri = init ?? {}
      signals.push(ri.signal as AbortSignal)
      ids.push((ri.headers as Record<string, string>)['x-service-id'])
      return makeNeverClosingStreamResponse()
    })
    const { rerender } = renderHook(() => useServiceStream(true, '/api/x', () => {}))

    await waitFor(() => expect(signals.length).toBe(1))
    expect(signals[0].aborted).toBe(false)

    mockState = { activeServiceId: 'svc-2' }
    rerender()

    await waitFor(() => expect(signals.length).toBe(2))
    expect(signals[0].aborted).toBe(true)
    expect(signals[1].aborted).toBe(false)
    expect(ids).toEqual(['svc-1', 'svc-2'])
  })

  it('exponential backoff: roughly doubles per failure, capped at 30s', async () => {
    vi.useFakeTimers()
    let calls = 0
    vi.mocked(fetch).mockImplementation(async () => {
      calls += 1
      return new Response('nope', { status: 500 })
    })
    renderHook(() => useServiceStream(true, '/api/x', () => {}))

    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(calls).toBe(1)

    await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
    expect(calls).toBeGreaterThanOrEqual(2)
    const afterFirstBackoff = calls

    await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
    expect(calls).toBeGreaterThan(afterFirstBackoff)
    const afterSecondBackoff = calls

    await act(async () => { await vi.advanceTimersByTimeAsync(2_500) })
    expect(calls).toBe(afterSecondBackoff)
    await act(async () => { await vi.advanceTimersByTimeAsync(1_600) })
    expect(calls).toBeGreaterThan(afterSecondBackoff)

    await act(async () => { await vi.advanceTimersByTimeAsync(8_500) })
    await act(async () => { await vi.advanceTimersByTimeAsync(16_500) })
    const callsBeforeCap = calls
    await act(async () => { await vi.advanceTimersByTimeAsync(31_000) })
    expect(calls).toBeGreaterThan(callsBeforeCap)
  })

  describe('optionalService', () => {
    it('connects with NO x-service-id when activeServiceId is null', async () => {
      mockState = { activeServiceId: null }
      let headers: Record<string, string> | undefined
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        headers = init?.headers as Record<string, string>
        return makeNeverClosingStreamResponse()
      })
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(headers && 'x-service-id' in headers).toBe(false)
    })

    it('sends x-service-id when a service IS selected', async () => {
      mockState = { activeServiceId: 'svc-1' }
      let headers: Record<string, string> | undefined
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        headers = init?.headers as Record<string, string>
        return makeNeverClosingStreamResponse()
      })
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(headers?.['x-service-id']).toBe('svc-1')
    })

    it('reconnects WITH the header when a service is added (null → svc-1)', async () => {
      mockState = { activeServiceId: null }
      const signals: AbortSignal[] = []
      const ids: (string | undefined)[] = []
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        const ri = init ?? {}
        signals.push(ri.signal as AbortSignal)
        ids.push((ri.headers as Record<string, string>)['x-service-id'])
        return makeNeverClosingStreamResponse()
      })
      const { rerender } = renderHook(() =>
        useServiceStream(true, '/api/x', () => {}, { optionalService: true }),
      )

      await waitFor(() => expect(signals.length).toBe(1))
      expect(signals[0].aborted).toBe(false)

      mockState = { activeServiceId: 'svc-1' }
      rerender()

      await waitFor(() => expect(signals.length).toBe(2))
      expect(signals[0].aborted).toBe(true)
      expect(signals[1].aborted).toBe(false)
      expect(ids).toEqual([undefined, 'svc-1'])
    })

    it('waits for the persisted store to rehydrate, then connects ONCE already scoped', async () => {
      mockHydrated = false
      mockState = { activeServiceId: null }
      const ids: (string | undefined)[] = []
      vi.mocked(fetch).mockImplementation(async (_url, init) => {
        const ri = init ?? {}
        ids.push((ri.headers as Record<string, string>)['x-service-id'])
        return makeNeverClosingStreamResponse()
      })
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      await new Promise(r => setTimeout(r, 20))
      expect(fetch).not.toHaveBeenCalled()

      mockHydrated = true
      mockState = { activeServiceId: 'svc-1' }
      act(() => { finishHydrationCb?.() })

      await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
      expect(ids).toEqual(['svc-1'])
    })
  })

  it('a successful reconnect resets the backoff counter', async () => {
    vi.useFakeTimers()
    let calls = 0
    vi.mocked(fetch).mockImplementation(async () => {
      calls += 1
      if (calls < 3) return new Response('x', { status: 500 })
      if (calls === 3) return makeStreamResponse(['data: ok\n\n'])
      return new Response('x', { status: 500 })
    })

    renderHook(() => useServiceStream(true, '/api/x', () => {}))

    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
    await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
    expect(calls).toBe(3)

    await act(async () => { await vi.advanceTimersByTimeAsync(1_500) })
    expect(calls).toBe(4)
  })
})

function wrapper(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

function makeQc() {
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

describe('useAdminEventStream', () => {
  let capturedOnEvent: ((raw: string) => void) | null = null
  let lastEnabled: boolean | undefined
  let lastPath: string | undefined
  let lastOpts: Record<string, unknown> | undefined
  let cronApply: ReturnType<typeof vi.fn<(data: unknown) => void>>
  let cronCleanup: ReturnType<typeof vi.fn<() => void>>

  beforeEach(() => {
    capturedOnEvent = null
    lastEnabled = undefined
    lastPath = undefined
    lastOpts = undefined
    bootstrapSettled = true
    mockState.activeServiceId = 'svc-1'
    cronApply = vi.fn<(data: unknown) => void>()
    cronCleanup = vi.fn<() => void>()
    vi.spyOn(adminStreamHooks, 'applyShare').mockImplementation(() => {})
    vi.spyOn(adminStreamHooks, 'applySyncStatus').mockImplementation(() => {})
    vi.spyOn(adminStreamHooks, 'applySystemMetrics').mockImplementation(() => {})
    vi.spyOn(adminStreamHooks, 'makeCronRunsApplier').mockImplementation(() => ({
      apply: cronApply,
      cleanup: cronCleanup,
    }))
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

  it('connects to /api/admin/events/stream with sorted channels + optionalService opts', () => {
    renderHook(() => useAdminEventStream(true, ['sync-status', 'cron-runs', 'system-metrics']), {
      wrapper: wrapper(makeQc()),
    })
    expect(lastEnabled).toBe(true)
    expect(lastPath).toBe('/api/admin/events/stream?channels=cron-runs,sync-status,system-metrics')
    expect(lastOpts).toMatchObject({ optionalService: true, cache: 'no-store' })
  })

  it('holds the stream disabled until bootstrap settles', () => {
    bootstrapSettled = false
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(lastEnabled).toBe(false)
  })

  it('forwards enabled=false', () => {
    renderHook(() => useAdminEventStream(false, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(lastEnabled).toBe(false)
  })

  it('demuxes sync-status envelope to applySyncStatus(qc, serviceId, data)', () => {
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    const data = { local_rows: 5 }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'sync-status', data })))
    expect(adminStreamHooks.applySyncStatus).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
  })

  it('demuxes system-metrics envelope to applySystemMetrics(qc, serviceId, data)', () => {
    renderHook(() => useAdminEventStream(true, ['system-metrics']), { wrapper: wrapper(makeQc()) })
    const data = { health_snapshot: { status: 'ok' } }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'system-metrics', data })))
    expect(adminStreamHooks.applySystemMetrics).toHaveBeenCalledWith(expect.anything(), 'svc-1', data)
  })

  it('demuxes cron-runs envelope to the cron applier', () => {
    renderHook(() => useAdminEventStream(true, ['cron-runs']), { wrapper: wrapper(makeQc()) })
    const data = { task: 'sync', status: 'success' }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'cron-runs', data })))
    expect(cronApply).toHaveBeenCalledWith(data)
  })

  it('demuxes share envelope to applyShare(qc, data)', () => {
    renderHook(() => useAdminEventStream(true, ['share']), { wrapper: wrapper(makeQc()) })
    const data = { sharing_active: true, active_session_count: 1 }
    act(() => capturedOnEvent!(JSON.stringify({ channel: 'share', data })))
    expect(adminStreamHooks.applyShare).toHaveBeenCalledWith(expect.anything(), data)
  })

  it('ignores malformed envelopes and unknown channels without throwing', () => {
    renderHook(() => useAdminEventStream(true, ['sync-status']), { wrapper: wrapper(makeQc()) })
    expect(() => act(() => capturedOnEvent!('not-json'))).not.toThrow()
    expect(() => act(() => capturedOnEvent!(JSON.stringify({ channel: 'mystery', data: {} })))).not.toThrow()
    expect(adminStreamHooks.applySyncStatus).not.toHaveBeenCalled()
    expect(adminStreamHooks.applySystemMetrics).not.toHaveBeenCalled()
    expect(cronApply).not.toHaveBeenCalled()
  })
})
