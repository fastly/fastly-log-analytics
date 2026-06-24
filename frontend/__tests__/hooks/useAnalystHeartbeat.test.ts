/**
 * Complement to ``useAnalystHeartbeat.test.tsx`` — covers the
 * gating + cleanup edges the .tsx file doesn't:
 *   - enabled=false short-circuits the setInterval entirely
 *   - unmount stops the polling timer
 *   - thrown errors are swallowed (no unhandled rejection)
 *
 * The .tsx sibling drives the active-poll + overlay + 401 redirect
 * paths under fake timers; this file deliberately keeps a narrow
 * scope so the assertions are easy to attribute when one breaks.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

const replace = vi.fn()
vi.mock('next/navigation', () => ({
  useRouter: () => ({ replace }),
}))

const IDLE = 1_000
const INTERVAL = 5_000

describe('useAnalystHeartbeat (gating + cleanup)', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-06-15T00:00:00Z'))
    replace.mockReset()
    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('does not call fetch when enabled=false (even after multiple intervals)', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    const { useAnalystHeartbeat } = await import('@/hooks/useAnalystHeartbeat')
    renderHook(() =>
      useAnalystHeartbeat({ enabled: false, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )

    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 5)
      await vi.advanceTimersByTimeAsync(INTERVAL * 5)
    })

    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('stops polling after unmount', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    const { useAnalystHeartbeat } = await import('@/hooks/useAnalystHeartbeat')
    const { unmount } = renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )

    // Drift past idle, fire one poll.
    await act(async () => {
      vi.setSystemTime(Date.now() + IDLE + 1)
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    const callsBeforeUnmount = fetchSpy.mock.calls.length

    unmount()

    // Advance several intervals after unmount — should not fire again.
    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 5)
      await vi.advanceTimersByTimeAsync(INTERVAL * 5)
    })

    expect(fetchSpy.mock.calls.length).toBe(callsBeforeUnmount)
  })

  it('swallows fetch errors silently (no unhandled rejection)', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('boom'))
    const unhandled = vi.fn()
    process.on('unhandledRejection', unhandled)

    const { useAnalystHeartbeat } = await import('@/hooks/useAnalystHeartbeat')
    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 999, // keep overlay off so we isolate the swallow path
      }),
    )

    // Drift past idle and fire a few failing ticks.
    for (let i = 0; i < 3; i++) {
      await act(async () => {
        vi.setSystemTime(Date.now() + IDLE + 1)
        await vi.advanceTimersByTimeAsync(INTERVAL)
      })
    }

    // Hook stays mounted, no overlay flipped, no unhandled rejection escaped.
    expect(result.current.disconnected).toBe(false)
    expect(unhandled).not.toHaveBeenCalled()
    process.off('unhandledRejection', unhandled)
  })
})
