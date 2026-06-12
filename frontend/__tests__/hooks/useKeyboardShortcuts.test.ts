/**
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import {
  useKeyboardShortcuts,
  type ShortcutBinding,
} from '@/app/admin/queries/_hooks/useKeyboardShortcuts'

/** Dispatch a keydown on window with the given init. Returns the event so
 *  callers can assert on defaultPrevented / target. */
function press(init: KeyboardEventInit): KeyboardEvent {
  const event = new KeyboardEvent('keydown', { bubbles: true, ...init })
  window.dispatchEvent(event)
  return event
}

describe('useKeyboardShortcuts', () => {
  it('fires the matching handler for a simple key', () => {
    const handler = vi.fn()
    const bindings: ShortcutBinding[] = [{ key: '/', description: 'slash', handler }]
    renderHook(() => useKeyboardShortcuts(bindings))
    press({ key: '/' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("fires the '?' binding when the event reports key:'?' (real Chrome on US layout)", () => {
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: '?', description: 'help', handler }]))
    press({ key: '?', shiftKey: true })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("fires the '?' binding when the event reports key:'/' + shiftKey (Playwright, non-US layouts)", () => {
    // This is the regression that the v2 browser smoke-test caught:
    // Playwright sends Shift+/ as KeyboardEvent({ key: '/', shiftKey: true })
    // instead of key: '?'. Real Chrome on US QWERTY sends '?' directly, so
    // earlier manual testing missed it. The logicalKey() normalizer in the
    // hook promotes this case to '?' before binding lookup.
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: '?', description: 'help', handler }]))
    press({ key: '/', shiftKey: true, code: 'Slash' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("does NOT fire '?' when '/' is pressed without Shift", () => {
    const qHandler = vi.fn()
    const slashHandler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([
        { key: '?', description: 'help', handler: qHandler },
        { key: '/', description: 'slash', handler: slashHandler },
      ]),
    )
    press({ key: '/' })
    expect(qHandler).not.toHaveBeenCalled()
    expect(slashHandler).toHaveBeenCalledOnce()
  })

  it('ignores keys while focus is in an INPUT unless allowInForms is set', () => {
    const escHandler = vi.fn()
    const slashHandler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([
        { key: '/', description: 'slash', handler: slashHandler },
        { key: 'Escape', description: 'esc', handler: escHandler, allowInForms: true },
      ]),
    )
    const input = document.createElement('input')
    document.body.appendChild(input)
    input.focus()
    try {
      // '/' is gated — should NOT fire while typing.
      input.dispatchEvent(new KeyboardEvent('keydown', { key: '/', bubbles: true }))
      expect(slashHandler).not.toHaveBeenCalled()
      // 'Escape' has allowInForms: true — SHOULD fire even in the input.
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))
      expect(escHandler).toHaveBeenCalledOnce()
    } finally {
      document.body.removeChild(input)
    }
  })

  it('skips when meta/ctrl/alt modifiers are held', () => {
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: 'k', description: 'up', handler }]))
    press({ key: 'k', metaKey: true })
    press({ key: 'k', ctrlKey: true })
    press({ key: 'k', altKey: true })
    expect(handler).not.toHaveBeenCalled()
    // No modifier — should fire.
    press({ key: 'k' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it('is disabled when enabled=false (no listener attached)', () => {
    const handler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([{ key: '/', description: 'slash', handler }], false),
    )
    press({ key: '/' })
    expect(handler).not.toHaveBeenCalled()
  })

  it('unmounts cleanly — listener removed on cleanup', () => {
    const handler = vi.fn()
    const { unmount } = renderHook(() =>
      useKeyboardShortcuts([{ key: '/', description: 'slash', handler }]),
    )
    press({ key: '/' })
    expect(handler).toHaveBeenCalledOnce()
    unmount()
    press({ key: '/' })
    // Still one call — unmount removed the listener.
    expect(handler).toHaveBeenCalledOnce()
  })
})
