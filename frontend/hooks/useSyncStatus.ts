'use client'

import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

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
 * - `skip_fos: true` because we never need the live FOS bucket scan
 *   on the page-shell path — the data we want (`latest_log_at`,
 *   `local_rows`) is in the local metadata.
 * - `staleTime: 60_000`: status changes every cron tick (~minute);
 *   60 s is fresh enough for a header badge.
 * - `refetchInterval: 30_000`: keeps the badge moving without
 *   spamming the endpoint.
 * - `refetchOnWindowFocus: false`: focus is not a signal that the
 *   sync state changed.
 * - `retry: false`: the endpoint is admin-only; analyst sessions
 *   always 403. The badge degrades gracefully when status is null,
 *   so a one-shot failure (analyst permanent, admin transient) is
 *   fine.
 */
export function useSyncStatus() {
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()

  // Perf audit Phase D-2: useBootstrap now seeds the
  // ['sync-status', service_id] cache from the bootstrap response on
  // admin sessions. Same race fix as useLogFieldsCatalog — gate on
  // bootstrap being in-flight so this hook doesn't fire its own
  // fetch and beat the seed on every cold page load.
  const bootstrapState = queryClient.getQueryState(['bootstrap'])
  const bootstrapPending = bootstrapState !== undefined && bootstrapState.status === 'pending'

  return useQuery({
    queryKey: ['sync-status', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/sync-status', {
        signal,
        params: { query: { skip_fos: true } },
      })
      if (error) throw error
      return data as SyncStatus
    },
    enabled: !!activeServiceId && !bootstrapPending,
    staleTime: 60_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
