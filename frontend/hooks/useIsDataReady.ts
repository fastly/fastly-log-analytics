'use client'

import { useSyncExternalStore } from 'react'
import { useQueryClient } from '@tanstack/react-query'

import { queryKeys } from '@/lib/query-keys'
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
  const bootstrap = queryClient.getQueryData(queryKeys.bootstrap()) as
    | { active_service_id?: string | null }
    | undefined
  return bootstrap?.active_service_id ?? stored
}

export function useIsDataReady(): boolean {
  return !!useEffectiveServiceId()
}

/**
 * Returns true once the bootstrap query has produced data (success OR
 * error — any state where we know what services exist). Used by
 * <ReportShell /> to gate the <NoServiceSelected /> fallback: on a
 * cold load with empty localStorage, the gap between mount and
 * HydrationBoundary committing the dehydrated bootstrap cache (or, on
 * a true cold path, the client-side useBootstrap fetch returning) is
 * a one-render window where activeServiceId is null but bootstrap is
 * still in-flight. Showing the "Please select a service" message in
 * that window flashes briefly before the real data lands. Reading via
 * getQueryData is intentionally non-subscribing — the bootstrap
 * consumers (useEffectiveServiceId, useBootstrap) already trigger the
 * re-render once bootstrap data arrives; we don't need a second one.
 */
export function useBootstrapResolved(): boolean {
  const queryClient = useQueryClient()
  return queryClient.getQueryData(queryKeys.bootstrap()) !== undefined
}

/**
 * Returns true while the bootstrap query is still in-flight (status
 * 'pending'). Like useBootstrapResolved, this is a non-subscribing
 * getQueryState read — consumers gate their own queries' `enabled` on
 * it and already re-render when bootstrap data arrives.
 */
export function useBootstrapPending(): boolean {
  const queryClient = useQueryClient()
  const s = queryClient.getQueryState(queryKeys.bootstrap())
  return s !== undefined && s.status === 'pending'
}

/**
 * SUBSCRIBING variant of useBootstrapResolved: re-renders the caller once
 * bootstrap lands in the cache. Unlike useBootstrapResolved (a one-shot
 * getQueryData read), this is for gating side-effects that must START the
 * moment bootstrap settles even when nothing else re-renders the caller.
 *
 * Why it exists: bootstrap's queryFn seeds BOTH the active service id
 * (serviceStore) and the admin token (adminTokenStore). An `optionalService`
 * SSE stream that connects before bootstrap resolves opens on the pre-bootstrap
 * serviceId / null admin token, then aborts + reconnects once bootstrap settles
 * those — a throwaway upstream the reverse proxy logs as "aborting with
 * incomplete response / reading: context canceled" on every admin load. Gating
 * such a stream's `enabled` on this flag makes it connect ONCE, already scoped
 * and authed. On a fresh install (no service) bootstrap still resolves, so the
 * global slices stream normally.
 *
 * useSyncExternalStore (not getQueryData) so the caller re-renders even when it
 * subscribes to nothing else that changes on bootstrap (e.g. the admin overview
 * with no service selected). SSR snapshot is false — the stream never runs
 * server-side anyway.
 */
export function useBootstrapSettled(): boolean {
  const queryClient = useQueryClient()
  return useSyncExternalStore(
    (onChange) => queryClient.getQueryCache().subscribe(onChange),
    () => queryClient.getQueryData(queryKeys.bootstrap()) !== undefined,
    () => false,
  )
}
