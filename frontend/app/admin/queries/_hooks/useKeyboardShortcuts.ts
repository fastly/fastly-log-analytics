'use client'

import { useEffect } from 'react'

/**
 * Bind a small set of keyboard shortcuts to `window`. Designed for an
 * admin-only page so we don't have to worry about polluting the global
 * shortcut surface — the page mounts conditionally behind the admin gate.
 *
 * Each handler receives the raw `KeyboardEvent` and is responsible for
 * calling `preventDefault()` when it should swallow the key.
 *
 * Keys typed into form fields (`<input>`, `<textarea>`, `contenteditable`)
 * are ignored by default so `/` doesn't hijack searching inside the search
 * box itself. Pass `allowInForms: true` per binding to override (used by
 * `Esc`, which closes the expanded row even when focus is in the search
 * input).
 */
export type ShortcutBinding = {
  key: string
  description: string
  handler: (event: KeyboardEvent) => void
  allowInForms?: boolean
}

function isFormElement(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  const tag = target.tagName
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true
  if (target.isContentEditable) return true
  return false
}

/** Resolve the logical key for a binding match. Most browsers report the
 *  shifted character directly in `event.key` (Shift+/ → "?"), but some
 *  driver paths (Playwright, certain virtual keyboards, non-US layouts on
 *  older Chromium) report the unshifted base key and leave the caller to
 *  apply Shift. Normalise the handful of shifted characters we actually
 *  bind to so shortcuts work consistently. */
function logicalKey(event: KeyboardEvent): string {
  if (event.shiftKey) {
    if (event.key === '/' || event.code === 'Slash') return '?'
  }
  return event.key
}

export function useKeyboardShortcuts(bindings: ShortcutBinding[], enabled: boolean = true): void {
  useEffect(() => {
    if (!enabled) return
    const onKeyDown = (event: KeyboardEvent) => {
      // Don't fight the platform: meta/ctrl combinations belong to the
      // browser (cmd-K command palette, ctrl-R reload, etc.). Skip when
      // any modifier other than Shift is held.
      if (event.metaKey || event.ctrlKey || event.altKey) return
      const key = logicalKey(event)
      const inForm = isFormElement(event.target)
      for (const b of bindings) {
        if (b.key !== key) continue
        if (inForm && !b.allowInForms) continue
        b.handler(event)
        return
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [bindings, enabled])
}
