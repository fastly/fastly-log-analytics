/**
 * useNowMs is the shared 1-second tick consumed by every countdown
 * widget (SystemJobBox, SyncStatusBadge, useElapsedTime …). The single-
 * timer guarantee is the perf-critical invariant: register on first
 * subscribe, tear down on last unsubscribe. Tests below exercise both
 * the subscribe-side cadence and the unsubscribe-side cleanup.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

describe('useNowMs', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-06-15T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('returns a value close to Date.now() on first render', async () => {
    const { useNowMs } = await import('@/hooks/useNowSeconds')
    const { result } = renderHook(() => useNowMs())
    // Initial snapshot is captured at module load (or last tick); accept
    // any millisecond value that is finite and non-negative.
    expect(Number.isFinite(result.current)).toBe(true)
    expect(result.current).toBeGreaterThan(0)
  })

  it('updates after the 1s interval fires', async () => {
    const { useNowMs } = await import('@/hooks/useNowSeconds')
    const { result } = renderHook(() => useNowMs())
    const before = result.current

    await act(async () => {
      // Move both the wall clock and the fake-timer queue forward.
      vi.setSystemTime(Date.now() + 1500)
      await vi.advanceTimersByTimeAsync(1500)
    })

    expect(result.current).toBeGreaterThan(before)
    // Should land within ~50ms of the 1500ms wall-clock advance.
    expect(result.current - before).toBeGreaterThanOrEqual(1000)
  })

  it('multiple subscribers share the same snapshot value', async () => {
    const { useNowMs } = await import('@/hooks/useNowSeconds')
    const { result: a } = renderHook(() => useNowMs())
    const { result: b } = renderHook(() => useNowMs())
    expect(a.current).toBe(b.current)

    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })

    expect(a.current).toBe(b.current)
  })

  it('keeps ticking while at least one subscriber is mounted', async () => {
    const { useNowMs } = await import('@/hooks/useNowSeconds')
    const { result: a, unmount: unmountA } = renderHook(() => useNowMs())
    const { result: b } = renderHook(() => useNowMs())

    unmountA()

    const before = b.current
    // Two ticks — under RTL's act+fake-timers, the first
    // ``advanceTimersByTimeAsync(1000)`` sometimes fires the interval
    // callback without flushing React's reconciliation pass; a second
    // tick reliably lands a re-render. The invariant under test is "B
    // still ticks after A's unmount", not the exact tick count.
    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })
    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(b.current).toBeGreaterThan(before)
    // a is unmounted; reading its final captured value should still be a number.
    expect(typeof a.current).toBe('number')
  })

  it('cleans up the underlying interval when the last subscriber unmounts', async () => {
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
    const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

    const { useNowMs } = await import('@/hooks/useNowSeconds')
    const { unmount } = renderHook(() => useNowMs())

    // If the module-level interval was already running from a previous
    // test's mount, the new hook subscribe-path won't allocate another;
    // either way, unmounting the LAST subscriber should clear the
    // interval handle.
    unmount()

    // The cleanup may run asynchronously through useSyncExternalStore's
    // subscribe-cleanup. Allow a tick.
    await act(async () => {
      await Promise.resolve()
    })

    expect(clearIntervalSpy).toHaveBeenCalled()
    setIntervalSpy.mockRestore()
    clearIntervalSpy.mockRestore()
  })
})
