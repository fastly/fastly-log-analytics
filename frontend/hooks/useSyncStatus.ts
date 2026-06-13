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
 * - `enabled` also skips the fetch for analyst sessions entirely.
 *   The endpoint is admin-only and the analyst dashboard never used
 *   the data — it was just a 403 per page load in DevTools.
 */
export function useSyncStatus() {
  const { activeServiceId, services } = useServiceStore()
  const queryClient = useQueryClient()

  // Perf audit Phase D-2: useBootstrap now seeds the
  // ['sync-status', service_id] cache from the bootstrap response on
  // admin sessions. Same race fix as useLogFieldsCatalog — gate on
  // bootstrap being in-flight so this hook doesn't fire its own
  // fetch and beat the seed on every cold page load.
  const bootstrapState = queryClient.getQueryState(['bootstrap'])
  const bootstrapPending = bootstrapState !== undefined && bootstrapState.status === 'pending'

  // Mirrors the analyst-detection used in app/alerts/page.tsx: a user is
  // "analyst" if their active service is read_only OR if bootstrap flagged
  // them as a remote share-invited analyst. /api/sync-status is in
  // _ANALYST_BLOCKED_SUBPATHS server-side, so any analyst fetch is a
  // guaranteed 403 — skip it.
  const bootstrapData = queryClient.getQueryData<{ settings?: Record<string, unknown> }>(['bootstrap'])
  const activeService = services.find(s => s.id === activeServiceId)
  const isAnalyst =
    activeService?.accessLevel === 'read_only' ||
    bootstrapData?.settings?.is_remote_analyst === true

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
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst,
    staleTime: 60_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
