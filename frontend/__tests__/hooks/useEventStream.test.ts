/**
 * @vitest-environment jsdom
 *
 * useEventStream — the single fetch-based SSE transport primitive that
 * useSSE / useServiceStream / useAdminEventStream are thin adapters over.
 *
 * These tests pin the invariants the three consumers depend on:
 *   - CRLF (\r\n\r\n) framing (sse-starlette emits CRLF; a naive \n\n parser
 *     returns zero frames — AGENTS.md trap),
 *   - the HTTP-error class + status surface,
 *   - the isActive liveness gate and AbortSignal cancellation,
 *   - reconnect-with-exponential-backoff and backoff-reset-on-connect,
 *   - terminal 401/403 (no reconnect).
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  runEventStream,
  useEventStream,
  EventStreamHttpError,
  type EventStreamRequest,
} from '@/hooks/useEventStream'
import { makeStreamResponse, makeNeverClosingStreamResponse } from '../helpers/sse-mock'

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
        live = false // deactivate after the first frame batch
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
    // Allow any synchronous retry to have happened.
    await new Promise((r) => setTimeout(r, 50))
    expect(f).toHaveBeenCalledTimes(1)
  })

  it('reconnects with exponential backoff after a dropped connection', async () => {
    vi.useFakeTimers()
    // 500 fails BEFORE onOpen, so the backoff counter actually grows (a
    // successful connect would reset it to 0). 500 is non-terminal (unlike
    // 401/403), so it retries.
    const f = vi.fn().mockImplementation(() => Promise.resolve(new Response(null, { status: 500 })))
    vi.stubGlobal('fetch', f)
    renderHook(() => useEventStream(true, req, () => {}))

    await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(1))
    // First backoff is 1000ms (2**0 * 1000).
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    await vi.waitFor(() => expect(f).toHaveBeenCalledTimes(2))
    // Second backoff is 2000ms (2**1 * 1000) — 1000ms alone must not fire it.
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
