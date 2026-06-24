'use client'

import { useCallback, useState } from 'react'

/**
 * Copy a string to the clipboard and surface a "just copied" flag that
 * auto-resets after ``resetMs``. Eight in-tree copy buttons follow this
 * exact pattern (navigator.clipboard.writeText + setCopied(true) +
 * setTimeout(setCopied(false), ms)); this hook is the canonical place
 * for the timeout default and the try/catch around the write.
 *
 * The catch is silent because ``navigator.clipboard.writeText`` rejects
 * with permission errors in non-secure contexts and on some browsers'
 * permission-denied paths — the caller has no useful recovery and the
 * UI just shows the un-copied state.
 */
export function useCopyToClipboard(resetMs: number = 1500) {
  const [copied, setCopied] = useState(false)
  const copy = useCallback(
    async (value: string) => {
      try {
        await navigator.clipboard.writeText(value)
        setCopied(true)
        window.setTimeout(() => setCopied(false), resetMs)
      } catch {
        // ignore — common in non-secure contexts and on permission denial
      }
    },
    [resetMs],
  )
  return { copied, copy }
}
