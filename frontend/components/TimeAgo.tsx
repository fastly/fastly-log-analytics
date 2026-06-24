'use client'

import type { ReactNode } from 'react'
import { useNowMs } from '@/hooks/useNowSeconds'
import { useMounted } from '@/hooks/useMounted'
import { formatTimeAgo } from '@/lib/date'

/**
 * Live "X ago" text node. Subscribes to the SHARED 1 Hz tick (useNowMs)
 * so ``formatTimeAgo`` re-evaluates every second instead of only when the
 * owning query refetches — that stale-until-refetch behaviour is what made
 * the Data Management page's relative times appear to "jump in intervals".
 *
 * Deliberately a text-only leaf: only this node re-renders on the tick, so
 * dropping it inside a Tooltip/table cell (see DateTimeCell) keeps the
 * surrounding chrome stable. A 500-row table costs one cheap text re-render
 * per row per second, not a tooltip-subtree re-render. All consumers share
 * useNowMs's single process-wide setInterval — see useNowSeconds.ts for why
 * that single-timer design replaced per-component tickers.
 *
 * Extracted from the proven inline copy that lived in SyncStatusBadge.
 */
export function TimeAgo({
  timestamp,
  fallback = null,
}: {
  timestamp: string | null | undefined
  fallback?: ReactNode
}) {
  // Subscribe unconditionally (rules of hooks) before any early return.
  // useNowMs drives the 1 Hz re-render once mounted; useMounted gates SSR.
  useNowMs()
  const mounted = useMounted()
  if (!timestamp) return <>{fallback}</>
  // formatTimeAgo reads the wall clock (new Date()) at second precision, so
  // computing it during SSR and again at client hydration (a moment later)
  // yields different strings → React #418. Render the fallback until mounted
  // so server and first client render agree; the live "X ago" appears right
  // after hydration (and ticks every second thereafter via useNowMs).
  if (!mounted) return <>{fallback}</>
  return <>{formatTimeAgo(timestamp)}</>
}
