/**
 * @vitest-environment jsdom
 *
 * Exercises the setInterval → consecutive-failure → disconnect-overlay path
 * end-to-end under fake timers. The overlay flips only after
 * `failuresBeforeOverlay` consecutive failures, so we need to drive multiple
 * ticks deterministically — which is what `vi.advanceTimersByTimeAsync` is for.
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { useAnalystHeartbeat } from '@/hooks/useAnalystHeartbeat'

const replace = vi.fn()
vi.mock('next/navigation', () => ({
  useRouter: () => ({ replace }),
}))

const IDLE = 1_000
const INTERVAL = 5_000

function setLastActivityPast(ms: number) {
  // The hook re-uses Date.now() to compute idleness — wind the wall clock
  // forward via fake timers and the hook re-reads on the next tick.
  vi.setSystemTime(Date.now() + ms)
}

describe('useAnalystHeartbeat', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    replace.mockReset()
    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('does not poll while the user is active inside the idle window', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )
    // Drive a mousemove just before each interval tick so lastActivity
    // resets and the hook never crosses the idle threshold.
    for (let i = 0; i < 3; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(INTERVAL - 1)
        window.dispatchEvent(new Event('mousemove'))
        await vi.advanceTimersByTimeAsync(1)
      })
    }
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('shows the disconnect overlay after two consecutive failures', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('network down'))

    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 2,
      }),
    )

    expect(result.current.disconnected).toBe(false)

    // Drift past the idle threshold so the hook actually fetches.
    setLastActivityPast(IDLE + 1)
    // First failing tick: under threshold, no overlay yet.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(false)

    // Second failing tick: overlay should flip on.
    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(true)
  })

  it('clears the overlay after a successful tick following failures', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))

    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 2,
      }),
    )

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(true)

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(false)
  })

  it('redirects to /share-login on a 401 response', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )
    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(replace).toHaveBeenCalledWith('/share-login')
  })
})
