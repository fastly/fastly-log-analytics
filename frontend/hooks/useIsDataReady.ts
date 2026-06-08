'use client'

import { useServiceStore } from '@/stores/serviceStore'

/**
 * Returns true once a service is selected. Queries fire with whatever
 * range is currently in the filter store (default = last 7 days). If
 * FilterBar's sync-status response later shifts the range via
 * autoSetRange, the queryKey changes and TanStack Query refires
 * automatically — so the "auto-snap to most-recent-24h" behavior still
 * works, the dashboard just doesn't *wait* for it before painting.
 *
 * Previously also required `hasSyncedExtents`. That flag is set in
 * FilterBar's effect after /api/sync-status returns (~1s wall-clock on a
 * cold load). Gating data fetches on it meant every first page load
 * burned ~1s before any of the real queries could even start. The
 * trade-off wasn't worth it: the only thing the wait bought was a
 * marginally better default range, and most users pick their own range
 * anyway. On the rare cases where the default window misses real data,
 * the refire still happens — just from the painted state instead of
 * from a spinner.
 */
export function useIsDataReady(): boolean {
  return !!useServiceStore(s => s.activeServiceId)
}
