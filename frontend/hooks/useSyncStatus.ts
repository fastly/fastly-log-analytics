'use client'

import { useQuery } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { useBootstrapPending } from '@/hooks/useIsDataReady'
import type { components } from '@/types/api.generated'
import { useSeededReady } from '@/hooks/useSeededReady'
import { queryKeys } from '@/lib/query-keys'

const SEED_KEYS = [queryKeys.syncStatus] as const

export { useIsAnalyst }
export type SyncStatus = components['schemas']['SyncStatusResponse']

/**
 * Single source of truth for `/api/sync-status`.
 *
 * Why a hook: the perf audit saw 6-8 sync-status calls per dashboard
 * 30d load — far more than the two literal call sites in the codebase
 * (SyncStatusBadge in the header, useLogsPageState on /logs). The
 * inflation came from React Query's default `refetchOnWindowFocus:
 * true` firing every time the tab regained focus during a long load,
 * compounded by a 15 s `refetchInterval`. Centralising the policy
 * here prevents new callers from re-introducing those defaults.
 *
 * Contract:
 * - `staleTime`: 1 minute (allows the event stream in the layout to
 *   push updates, falling back to pull if the WebSocket dies).
 * - `refetchInterval`: 5 minutes (backup pull on a busy page).
 * - `retry: false`: the endpoint is admin-only; analyst sessions
 *   always 403. The badge degrades gracefully when status is null,
 *   so a one-shot failure (analyst permanent, admin transient) is
 *   fine.
 * - `enabled` also skips the fetch for analyst sessions entirely.
 *   The endpoint is admin-only and the analyst dashboard never used
 *   the data — it was just a 403 per page load in DevTools.
 */
export function useSyncStatus() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // Perf audit Phase D-2: useBootstrap now seeds the
  // ['sync-status', service_id] cache from the bootstrap response on
  // admin sessions. Same race fix as useLogFieldsCatalog — gate on
  // bootstrap being in-flight so this hook doesn't fire its own
  // fetch and beat the seed on every cold page load.
  const bootstrapPending = useBootstrapPending()

  // /api/sync-status is in _ANALYST_BLOCKED_SUBPATHS server-side, so
  // any analyst fetch is a guaranteed 403 — skip it.
  const isAnalyst = useIsAnalyst()

  const ready = useSeededReady(activeServiceId, SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.syncStatus(activeServiceId),
    queryFn: async ({ signal }) => {
      return (await unwrapClient(
        client.GET('/api/sync-status', {
          signal,
          params: { query: { skip_fos: true } },
        }),
      )) as SyncStatus
    },
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
