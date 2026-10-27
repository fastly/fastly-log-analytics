'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { copyToClipboard, showToast, showToastWithAction } from '@/lib/utils'

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
  return { active: value !== null, value, flash, set, clear }
}

export function useCopyToClipboard(resetMs: number = 1500) {
  const { active: copied, flash } = useAutoFlash(resetMs)
  const copy = useCallback(
    async (value: string) => {
      if (await copyToClipboard(value)) flash()
    },
    [flash],
  )
  return { copied, copy }
}

export interface DeferredDeleteOptions {
  id: string
  toastMessage: string
  performDelete: () => Promise<void>
  errorMessage: string | ((error: unknown) => string)
  durationMs?: number
}

export function useDeferredDeleteWithUndo() {
  const [pendingDeleteIds, setPendingDeleteIds] = useState<Set<string>>(() => new Set())
  const pendingTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

  useEffect(() => {
    const timers = pendingTimersRef.current
    return () => {
      for (const [, t] of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])

  const scheduleDelete = useCallback(
    ({ id, toastMessage, performDelete, errorMessage, durationMs = 5500 }: DeferredDeleteOptions) => {
      setPendingDeleteIds((prev) => new Set(prev).add(id))
      const timer = setTimeout(async () => {
        pendingTimersRef.current.delete(id)
        try {
          await performDelete()
        } catch (err) {
          setPendingDeleteIds((prev) => {
            const next = new Set(prev)
            next.delete(id)
            return next
          })
          showToast(typeof errorMessage === 'function' ? errorMessage(err) : errorMessage, 'error')
        }
      }, durationMs)
      pendingTimersRef.current.set(id, timer)
      showToastWithAction(toastMessage, {
        actionLabel: 'Undo',
        onAction: () => {
          const t = pendingTimersRef.current.get(id)
          if (t) {
            clearTimeout(t)
            pendingTimersRef.current.delete(id)
          }
          setPendingDeleteIds((prev) => {
            const next = new Set(prev)
            next.delete(id)
            return next
          })
        },
        durationMs,
      })
    },
    [],
  )
  return { pendingDeleteIds, scheduleDelete }
}
