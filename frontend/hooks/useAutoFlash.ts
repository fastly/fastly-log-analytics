'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { copyToClipboard } from '@/lib/utils'

/**
 * Auto-resetting flash state with automatic unmount timer cleanup.
 * Supports both boolean flashes (`flash()`) and value-carrying flashes (`flash(val)`).
 */
export function useAutoFlash<T = true>(resetMs: number = 2000) {
  const [value, setValue] = useState<T | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) {
      clearTimeout(timerRef.current)
      timerRef.current = null
    }
  }, [])

  const clear = useCallback(() => {
    clearTimer()
    setValue(null)
  }, [clearTimer])

  const set = useCallback(
    (nextValue: T | null) => {
      clearTimer()
      setValue(nextValue)
    },
    [clearTimer],
  )

  useEffect(() => clearTimer, [clearTimer])

  const flash = useCallback(
    (nextValue: T = true as unknown as T) => {
      clearTimer()
      setValue(nextValue)
      timerRef.current = setTimeout(() => {
        timerRef.current = null
        setValue(null)
      }, resetMs)
    },
    [clearTimer, resetMs],
  )

  return {
    active: value !== null,
    value,
    flash,
    set,
    clear,
  }
}

/**
 * Copy a string to the clipboard and surface a "just copied" flag that
 * auto-resets after ``resetMs``.
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
