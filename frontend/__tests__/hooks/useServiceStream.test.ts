/**
 * @vitest-environment jsdom
 *
 * useServiceStream — service-scoped SSE abstraction shared by every push-driven
 * panel. Wraps fetch with an AbortController, an event-boundary parser, and an
 * exponential reconnect backoff (1s → 30s cap).
 *
 * Audit finding: a regression in gating, backoff progression, the
 * backoff-reset-on-reconnect path, the multi-separator boundary regex, or
 * the abort lifecycle silently breaks ALL consumers. Sibling tests cover the
 * CRLF (\r\n\r\n) case only — this file pins LF (\n\n), old-Mac (\r\r), and
 * the backoff-counter-reset behaviour that isn't covered anywhere else.
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@/lib/api', () => ({
  getApiBase: () => 'http://test',
}))

let mockState: { activeServiceId: string | null } = { activeServiceId: 'svc-1' }
// Persist-hydration controls (mirrors serviceStore's skipHydration + post-mount
// rehydrate). Default hydrated=true so all existing tests connect immediately;
// the hydration-gating test flips it to false and fires the captured callback.
let mockHydrated = true
let finishHydrationCb: (() => void) | null = null

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  useServiceStore.persist = {
    hasHydrated: () => mockHydrated,
    onFinishHydration: (cb: () => void) => {
      finishHydrationCb = cb
      return () => { finishHydrationCb = null }
    },
    rehydrate: () => {},
  }
  return { useServiceStore }
})

function makeStreamResponse(messages: string[]): Response {
  const enc = new TextEncoder()
  let i = 0
  const stream = new ReadableStream({
    pull(controller) {
      if (i >= messages.length) {
        controller.close()
        return
      }
      controller.enqueue(enc.encode(messages[i]))
      i += 1
    },
  })
  return new Response(stream, { status: 200 })
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
  mockState = { activeServiceId: 'svc-1' }
  mockHydrated = true
  finishHydrationCb = null
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useServiceStream', () => {
  it('short-circuits when enabled=false (no fetch fired)', async () => {
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(false, '/api/x', () => {}))
    await new Promise(r => setTimeout(r, 20))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('short-circuits when activeServiceId is null (no fetch fired)', async () => {
    mockState = { activeServiceId: null }
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(true, '/api/x', () => {}))
    await new Promise(r => setTimeout(r, 20))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('parses LF (\\n\\n) event boundaries', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse(['data: alpha\n\ndata: beta\n\n']),
    )
    const events: string[] = []
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
    await waitFor(() => expect(events).toEqual(['alpha', 'beta']))
  })

  it('parses old-Mac (\\r\\r) event boundaries', async () => {
    // Spec-allowed separator; no modern producer emits it but the regex
    // covers it as defence-in-depth against quirky proxies.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse(['data: one\r\rdata: two\r\r']),
    )
    const events: string[] = []
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(true, '/api/x', raw => events.push(raw)))
    await waitFor(() => expect(events).toEqual(['one', 'two']))
  })

  it('aborts the in-flight stream on unmount', async () => {
    let signal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
      signal = (init as RequestInit | undefined)?.signal as AbortSignal
      const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
      return new Response(stream, { status: 200 })
    })
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    const { unmount } = renderHook(() => useServiceStream(true, '/api/x', () => {}))

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(signal?.aborted).toBe(false)
    act(() => unmount())
    expect(signal?.aborted).toBe(true)
  })

  it('aborts the prior stream and opens a new one when activeServiceId changes', async () => {
    const signals: AbortSignal[] = []
    const ids: string[] = []
    vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
      const ri = init as RequestInit
      signals.push(ri.signal as AbortSignal)
      ids.push((ri.headers as Record<string, string>)['x-service-id'])
      const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
      return new Response(stream, { status: 200 })
    })
    const { useServiceStream } = await import('@/hooks/useServiceStream')
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
      // Always fail — we want to observe the delay BETWEEN retries.
      return new Response('nope', { status: 500 })
    })
    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(true, '/api/x', () => {}))

    // Drain microtasks so the first fetch lands and the catch+setTimeout posts.
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(calls).toBe(1)

    // After ~1s a retry MUST have fired (delay = 1000 * 2^0 = 1000ms).
    // After ~2s after that, the second retry fires (delay = 2000ms).
    // We assert progression and that the gap GROWS between attempts —
    // exact ms boundaries are unstable because setTimeout's schedule
    // baseline shifts depending on whether it lands in the same fake-
    // timer tick as the firing event.
    await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
    expect(calls).toBeGreaterThanOrEqual(2)
    const afterFirstBackoff = calls

    await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
    expect(calls).toBeGreaterThan(afterFirstBackoff)
    const afterSecondBackoff = calls

    // The third backoff is ~4s, demonstrably longer than the second.
    // Advancing by only 2.5s after the second retry's failure should
    // NOT yet fire the third — pins that the gap is wider than 2s.
    await act(async () => { await vi.advanceTimersByTimeAsync(2_500) })
    expect(calls).toBe(afterSecondBackoff)
    // …but crossing 4s does fire it.
    await act(async () => { await vi.advanceTimersByTimeAsync(1_600) })
    expect(calls).toBeGreaterThan(afterSecondBackoff)

    // Burn through the rising delays (8s, 16s) so attempt climbs past 5,
    // then verify the cap: from this point on, every retry waits no
    // more than ~30s. After 31s the next retry MUST have fired.
    await act(async () => { await vi.advanceTimersByTimeAsync(8_500) })
    await act(async () => { await vi.advanceTimersByTimeAsync(16_500) })
    const callsBeforeCap = calls
    await act(async () => { await vi.advanceTimersByTimeAsync(31_000) })
    expect(calls).toBeGreaterThan(callsBeforeCap)
  })

  // optionalService — "soft" service scoping for bundles carrying BOTH global
  // and service-scoped slices (the admin system-metrics stream). Connect even
  // with no service so the global slices stream on a fresh install, but still
  // send x-service-id + reconnect on switch once one is selected.
  describe('optionalService', () => {
    it('connects with NO x-service-id when activeServiceId is null', async () => {
      mockState = { activeServiceId: null }
      let headers: Record<string, string> | undefined
      vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
        headers = (init as RequestInit).headers as Record<string, string>
        const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
        return new Response(stream, { status: 200 })
      })
      const { useServiceStream } = await import('@/hooks/useServiceStream')
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(headers && 'x-service-id' in headers).toBe(false)
    })

    it('sends x-service-id when a service IS selected', async () => {
      mockState = { activeServiceId: 'svc-1' }
      let headers: Record<string, string> | undefined
      vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
        headers = (init as RequestInit).headers as Record<string, string>
        const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
        return new Response(stream, { status: 200 })
      })
      const { useServiceStream } = await import('@/hooks/useServiceStream')
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(headers?.['x-service-id']).toBe('svc-1')
    })

    it('reconnects WITH the header when a service is added (null → svc-1)', async () => {
      mockState = { activeServiceId: null }
      const signals: AbortSignal[] = []
      const ids: (string | undefined)[] = []
      vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
        const ri = init as RequestInit
        signals.push(ri.signal as AbortSignal)
        ids.push((ri.headers as Record<string, string>)['x-service-id'])
        const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
        return new Response(stream, { status: 200 })
      })
      const { useServiceStream } = await import('@/hooks/useServiceStream')
      const { rerender } = renderHook(() =>
        useServiceStream(true, '/api/x', () => {}, { optionalService: true }),
      )

      // First connect: serviceless, no header.
      await waitFor(() => expect(signals.length).toBe(1))
      expect(signals[0].aborted).toBe(false)

      // User adds the first service → reconnect with the header.
      mockState = { activeServiceId: 'svc-1' }
      rerender()

      await waitFor(() => expect(signals.length).toBe(2))
      expect(signals[0].aborted).toBe(true)
      expect(signals[1].aborted).toBe(false)
      expect(ids).toEqual([undefined, 'svc-1'])
    })

    it('waits for the persisted store to rehydrate, then connects ONCE already scoped', async () => {
      // Regression for the per-load Caddy "context canceled" warning: the
      // stream must NOT open on the null serviceId and then abort+reconnect
      // when <StoreHydrator>'s post-mount rehydrate restores activeServiceId.
      mockHydrated = false
      mockState = { activeServiceId: null }
      const ids: (string | undefined)[] = []
      vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
        const ri = init as RequestInit
        ids.push((ri.headers as Record<string, string>)['x-service-id'])
        const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
        return new Response(stream, { status: 200 })
      })
      const { useServiceStream } = await import('@/hooks/useServiceStream')
      renderHook(() => useServiceStream(true, '/api/x', () => {}, { optionalService: true }))

      // Pre-hydration: nothing connects, so there's no throwaway upstream.
      await new Promise(r => setTimeout(r, 20))
      expect(fetch).not.toHaveBeenCalled()

      // StoreHydrator finishes: activeServiceId restored + hydration flag set.
      mockHydrated = true
      mockState = { activeServiceId: 'svc-1' }
      act(() => { finishHydrationCb?.() })

      // Exactly one connect, already carrying the right x-service-id — no abort.
      await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
      expect(ids).toEqual(['svc-1'])
    })
  })

  it('a successful reconnect resets the backoff counter', async () => {
    vi.useFakeTimers()
    let calls = 0
    // 1 & 2 fail (backoff climbs to ~2s); 3 succeeds + closes;
    // post-success retry should wait the BASE delay again, not 4s.
    vi.mocked(fetch).mockImplementation(async () => {
      calls += 1
      if (calls < 3) return new Response('x', { status: 500 })
      if (calls === 3) return makeStreamResponse(['data: ok\n\n'])
      return new Response('x', { status: 500 })
    })

    const { useServiceStream } = await import('@/hooks/useServiceStream')
    renderHook(() => useServiceStream(true, '/api/x', () => {}))

    // Burn the failed-retry climb (1s, then 2s).
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    await act(async () => { await vi.advanceTimersByTimeAsync(1_100) })
    await act(async () => { await vi.advanceTimersByTimeAsync(2_100) })
    expect(calls).toBe(3)

    // After the successful stream closes, the hook loops with attempt=0,
    // so the NEXT retry should fire within ~1s — not 4s (which it would
    // be if the counter hadn't reset). Use 1.5s as a robust upper bound.
    await act(async () => { await vi.advanceTimersByTimeAsync(1_500) })
    expect(calls).toBe(4)
  })
})
