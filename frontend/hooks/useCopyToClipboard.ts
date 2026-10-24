'use client'

import { useCallback } from 'react'
import { copyToClipboard } from '@/lib/clipboard'
import { useAutoFlash } from '@/hooks/useAutoFlash'

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
  const { active: copied, flash } = useAutoFlash(resetMs)

  const copy = useCallback(
    async (value: string) => {
      if (await copyToClipboard(value)) {
        flash()
      }
    },
    [flash],
  )
  return { copied, copy }
}
