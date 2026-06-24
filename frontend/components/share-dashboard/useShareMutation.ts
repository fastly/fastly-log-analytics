'use client'

import * as React from 'react'

import { extractApiError } from '@/lib/api'

/**
 * Shared single-busy mutation runner for the share-dashboard panels.
 *
 * Wraps the repeated `setBusy(true) → await fn() → await onRefresh() →
 * catch onError(extractApiError(e)) → finally setBusy(false)` shape that
 * the boot/stop/panic/revoke handlers all share. Confirm() guards stay at
 * the call site (run() is invoked only after the user confirms).
 */
export function useShareMutation(
  onError: (msg: string) => void,
  onRefresh?: () => Promise<void> | void,
) {
  const [busy, setBusy] = React.useState(false)
  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true)
    try {
      await fn()
      await onRefresh?.()
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }
  return { busy, run }
}
