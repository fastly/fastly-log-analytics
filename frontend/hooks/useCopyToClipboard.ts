'use client'

import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * Copy a string to the clipboard and surface a "just copied" flag that
 * auto-resets after ``resetMs``. This hook is the canonical place for the
 * timeout default, the try/catch around the write, and clearing the reset
 * timer on unmount (no setState-after-unmount).
 *
 * The catch is silent because ``navigator.clipboard.writeText`` rejects
 * with permission errors in non-secure contexts and on some browsers'
 * permission-denied paths — the caller has no useful recovery and the
 * UI just shows the un-copied state.
 */
export function useCopyToClipboard(resetMs: number = 1500) {
  const [copied, setCopied] = useState(false)
  const timer = useRef<number | undefined>(undefined)

  useEffect(() => () => window.clearTimeout(timer.current), [])

  const copy = useCallback(
    async (value: string) => {
      try {
        await navigator.clipboard.writeText(value)
        setCopied(true)
        window.clearTimeout(timer.current)
        timer.current = window.setTimeout(() => setCopied(false), resetMs)
      } catch {
        // ignore — common in non-secure contexts and on permission denial
      }
    },
    [resetMs],
  )
  return { copied, copy }
}
