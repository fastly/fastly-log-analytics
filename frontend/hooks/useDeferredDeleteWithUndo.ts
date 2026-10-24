'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { showToast, showToastWithAction } from '@/lib/toast'

export interface DeferredDeleteOptions {
  /** Unique item ID to hide while the undo window is open. */
  id: string
  /** Toast message shown when the undo window starts (e.g. `Deleted "${name}"`). */
  toastMessage: string
  /** Perform the actual delete after the undo window expires, and invalidate queries. */
  performDelete: () => Promise<void>
  /** Error message or extractor when `performDelete` rejects. */
  errorMessage: string | ((error: unknown) => string)
  /** Optional window duration in ms (defaults to 5500). */
  durationMs?: number
}

/**
 * Shared 5.5s optimistic-hide + Undo-toast deferred deletion hook.
 * Hides `id` in `pendingDeleteIds` immediately, starts a per-ID timer to run
 * `performDelete`, shows an Undo toast that cancels the timer and restores the
 * item, and clears all pending timers on unmount.
 */
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
      setPendingDeleteIds((prev) => {
        const next = new Set(prev)
        next.add(id)
        return next
      })
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
          const msg = typeof errorMessage === 'function' ? errorMessage(err) : errorMessage
          showToast(msg, 'error')
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
