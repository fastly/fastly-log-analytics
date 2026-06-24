/**
 * @vitest-environment jsdom
 *
 * Unit tests for the shared user-activity tracker ([lib/userActivity.ts](../../lib/userActivity.ts)).
 * It feeds the ``X-User-Active`` request header that lets the backend reset the
 * analyst idle timeout only on genuine interaction (see backend
 * ``RemoteAccessMiddleware``). Listeners attach on import; ``__resetForTests``
 * winds the module-global activity timestamp back to "long idle".
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  isUserActive,
  msSinceLastInteraction,
  __resetForTests,
  ACTIVE_WINDOW_MS,
} from '@/lib/userActivity'

describe('userActivity', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    __resetForTests(0) // far-past timestamp → starts idle
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('is inactive after reset with no gestures', () => {
    expect(isUserActive()).toBe(false)
    expect(msSinceLastInteraction()).toBeGreaterThan(ACTIVE_WINDOW_MS)
  })

  it('becomes active immediately after a keydown gesture', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    expect(isUserActive()).toBe(true)
    expect(msSinceLastInteraction()).toBeLessThan(1_000)
  })

  it('becomes active after a pointerdown gesture', () => {
    window.dispatchEvent(new Event('pointerdown'))
    expect(isUserActive()).toBe(true)
  })

  it('goes idle again once the active window elapses', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    expect(isUserActive()).toBe(true)
    vi.advanceTimersByTime(ACTIVE_WINDOW_MS + 1)
    expect(isUserActive()).toBe(false)
  })

  it('honors a caller-supplied window (the heartbeat idle-probe use)', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    vi.advanceTimersByTime(20_000)
    expect(isUserActive(15_000)).toBe(false) // 20s elapsed > 15s window
    expect(isUserActive(120_000)).toBe(true) // 20s elapsed < 120s window
  })
})
