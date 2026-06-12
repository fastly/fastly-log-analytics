/**
 * Display helpers + hooks for the Live Query Monitor.
 *
 * Pure presentational logic only — no API calls, no state machinery beyond
 * one tiny visibility hook. Easier to unit-test in isolation and keeps the
 * section components focused on layout.
 */

import * as React from 'react'

import type { AttributionKind } from './_types'

/** Subscribe to `document.visibilityState` so polling can pause when the
 *  tab is hidden. SSR-safe (initial value defaults to visible). */
export function useDocumentVisible(): boolean {
  const [visible, setVisible] = React.useState(
    typeof document !== 'undefined' ? document.visibilityState !== 'hidden' : true,
  )
  React.useEffect(() => {
    const onVis = () => setVisible(document.visibilityState !== 'hidden')
    document.addEventListener('visibilitychange', onVis)
    return () => document.removeEventListener('visibilitychange', onVis)
  }, [])
  return visible
}

/** Colored text class for a duration. Mirrors the four-band scale called
 *  out in the design doc §7 ("green < 500 ms, yellow < 2 s, orange <
 *  10 s, red ≥ 10 s"). */
export function durationColor(ms: number): string {
  if (ms < 500) return 'text-emerald-600 dark:text-emerald-400'
  if (ms < 2000) return 'text-amber-600 dark:text-amber-400'
  if (ms < 10_000) return 'text-orange-600 dark:text-orange-400'
  return 'text-red-600 dark:text-red-400'
}

/** Human-readable duration. <1s → `123 ms`; <1 min → `1.23 s`; ≥1 min →
 *  `Xm Ys`. */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`
  const mins = Math.floor(ms / 60_000)
  const secs = Math.round((ms % 60_000) / 1000)
  return `${mins}m ${secs}s`
}

export function kindBadgeVariant(
  kind: AttributionKind,
): 'default' | 'secondary' | 'destructive' | 'outline' {
  switch (kind) {
    case 'analyst':
      return 'default'
    case 'admin':
      return 'secondary'
    case 'cron':
      return 'outline'
    case 'system':
      return 'outline'
  }
}
