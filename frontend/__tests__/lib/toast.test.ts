/**
 * @vitest-environment jsdom
 *
 * Tests for the dependency-free toast helper at [lib/toast.ts](../../lib/toast.ts).
 *
 * The helper is small but carries real invariants that the rest of the app
 * relies on without re-checking:
 *
 *  - The role/aria-live attributes drive screen-reader announcement.
 *    Error/warn toasts use `role=alert` + `aria-live=assertive`; info/success
 *    use `role=status` + `aria-live=polite`. The 2026-06-10 audit added
 *    showToast specifically for the analyst "silent failure" findings (M-1,
 *    N-6) — if the live-region attributes regress, those failures go silent
 *    again.
 *  - The 1.5s dedupe window protects against the openapi-fetch middleware
 *    firing multiple 403s in parallel for the same action.
 *  - The container is created on first use and cleaned up when empty so the
 *    DOM doesn't accumulate orphan toast roots across navigations.
 *  - `showReadOnlyToast` is the canonical call for the read-only analyst case
 *    and must use the `warn` kind (matches the call sites in the app).
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { showReadOnlyToast, showToast } from '@/lib/toast'

beforeEach(() => {
  // Use fake timers so the auto-dismiss + transition timeouts are
  // deterministic — without them the tests would have to wait real wall
  // time for each toast to disappear.
  vi.useFakeTimers()
  // The dedupe map persists across tests because it's module-level state.
  // Walk far enough forward to clear any leftover entries from a prior test.
  vi.setSystemTime(new Date('2026-06-12T00:00:00Z'))
})

afterEach(() => {
  vi.useRealTimers()
  // Tear down any leftover container / toast nodes so the next test starts
  // from a clean DOM (helper only auto-removes when empty).
  document.body.innerHTML = ''
})

describe('showToast', () => {
  it('renders the message into the DOM with the info role by default', () => {
    showToast('hello world')
    const region = document.querySelector('[role="region"][aria-label="Notifications"]')
    expect(region).not.toBeNull()
    const toast = region!.querySelector('[role="status"]')
    expect(toast).not.toBeNull()
    expect(toast!.textContent).toBe('hello world')
    expect(toast!.getAttribute('aria-live')).toBe('polite')
  })

  it('uses role=alert + aria-live=assertive for error toasts', () => {
    showToast('database is on fire', 'error')
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
    expect(toast!.textContent).toBe('database is on fire')
  })

  it('uses role=alert + aria-live=assertive for warn toasts', () => {
    showToast('careful now', 'warn')
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
  })

  it('uses role=status + aria-live=polite for success toasts', () => {
    showToast('saved', 'success')
    const toast = document.querySelector('[role="status"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('polite')
  })

  it('dedupes identical messages within the 1.5s window', () => {
    showToast('parallel 403')
    showToast('parallel 403')
    showToast('parallel 403')
    const toasts = document.querySelectorAll('[role="status"], [role="alert"]')
    expect(toasts.length).toBe(1)
  })

  it('allows the same message after the dedupe window expires', () => {
    showToast('repeat me')
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    // Past the 1.5s dedupe window.
    vi.advanceTimersByTime(2000)
    showToast('repeat me')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
  })

  it('does not dedupe different messages', () => {
    showToast('first')
    showToast('second')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
  })

  it('auto-removes after the default duration', () => {
    showToast('temporary')
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    // Default duration for info is 3500ms; fade-out adds 200ms.
    vi.advanceTimersByTime(3500)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('keeps error toasts on screen for the longer 5500ms duration', () => {
    showToast('serious', 'error')
    // After the info duration the error toast is still up.
    vi.advanceTimersByTime(3500)
    expect(document.querySelectorAll('[role="alert"]').length).toBe(1)
    vi.advanceTimersByTime(2000) // crosses 5500ms
    vi.advanceTimersByTime(200)  // fade-out
    expect(document.querySelectorAll('[role="alert"]').length).toBe(0)
  })

  it('honours an explicit durationMs option', () => {
    showToast('short-lived', 'info', { durationMs: 100 })
    vi.advanceTimersByTime(100)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('removes the toast immediately on click', () => {
    showToast('clickable', 'info')
    const toast = document.querySelector('[role="status"]') as HTMLElement
    expect(toast).not.toBeNull()
    toast.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    // Click triggers the fade-then-remove; advance past the fade.
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('removes the container when the last toast goes away', () => {
    showToast('lonely')
    expect(document.querySelector('[role="region"]')).not.toBeNull()
    vi.advanceTimersByTime(3500)
    vi.advanceTimersByTime(200)
    expect(document.querySelector('[role="region"]')).toBeNull()
  })

  it('keeps the container while multiple toasts are stacked', () => {
    showToast('one')
    vi.advanceTimersByTime(50) // bump the clock so dedupe doesn't suppress the next call
    showToast('two')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
    // First toast expires; container survives because the second is still up.
    vi.advanceTimersByTime(3500 - 50)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    expect(document.querySelector('[role="region"]')).not.toBeNull()
  })

  it('is a no-op when document is undefined (SSR-safe)', () => {
    const originalDocument = globalThis.document
    // @ts-expect-error - simulating SSR where document is undefined.
    delete globalThis.document
    try {
      // Must not throw.
      expect(() => showToast('ssr-safe')).not.toThrow()
    } finally {
      globalThis.document = originalDocument
    }
  })
})

describe('showReadOnlyToast', () => {
  it('renders the canonical read-only warning with the warn kind', () => {
    showReadOnlyToast()
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.textContent).toContain('Read-only access')
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
  })
})
