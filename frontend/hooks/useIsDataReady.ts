'use client'

import { useQueryClient } from '@tanstack/react-query'

import { useServiceStore } from '@/stores/serviceStore'

/**
 * Returns true once a service is selected. Queries fire with whatever
 * range is currently in the filter store (default = last 7 days). If
 * FilterBar's sync-status response later shifts the range via
 * autoSetRange, the queryKey changes and TanStack Query refires
 * automatically — so the "auto-snap to most-recent-24h" behavior still
 * works, the dashboard just doesn't *wait* for it before painting.
 *
 * Bootstrap fallback (added 2026-06-11 alongside the SSR bootstrap
 * change): with bootstrap pre-seeded in the React Query cache, the
 * active service id is known on first paint. useBootstrap only writes
 * it into the persisted Zustand store from a post-mount useEffect,
 * which leaves a one-render window where "No service selected" flashes
 * before the effect runs. Fall back to bootstrap.active_service_id
 * whenever the store hasn't been populated yet so the gate flips true
 * on first render.
 *
 * Previously also required `hasSyncedExtents`. That flag is set in
 * FilterBar's effect after /api/sync-status returns (~1s wall-clock on a
 * cold load). Gating data fetches on it meant every first page load
 * burned ~1s before any of the real queries could even start.
 */
export function useEffectiveServiceId(): string | null | undefined {
  const stored = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  if (stored) return stored
  const bootstrap = queryClient.getQueryData(['bootstrap']) as
    | { active_service_id?: string | null }
    | undefined
  return bootstrap?.active_service_id ?? stored
}

export function useIsDataReady(): boolean {
  return !!useEffectiveServiceId()
}
