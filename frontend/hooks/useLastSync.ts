'use client'

import { useQuery } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { useIsAnalyst } from './useSyncStatus'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

type CronRunsResponse = components['schemas']['CronRunsResponse']

export interface LastSyncInfo {
  started_at: string | null
  status: string | null   // 'running' | 'success' | 'error' | 'partial_success' | null
  duration_s: number | null
}

/**
 * Fetches the most recent ``sync``-task row from ``/api/cron-runs``,
 * surfaced as the "Last Sync: Xs ago" header badge. Refetches whenever
 * ``useAdminEventStream`` sees a ``task === 'sync'`` event arrive —
 * primary update path is SSE invalidation, not the 5-minute fallback
 * poll.
 *
 * Admin-only (``/api/cron-runs`` is in ``_ANALYST_BLOCKED_PREFIXES``).
 * Analysts get nothing here; the badge component should fall back to
 * hiding the slot for them.
 */
export function useLastSync() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const isAnalyst = useIsAnalyst()

  return useQuery({
    queryKey: ['last-sync', activeServiceId],
    queryFn: async ({ signal }) => {
      // Single-row peek — the endpoint already supports task filter
      // + per_page, so this is a tiny query, not a full table dump.
      const { data, error } = await client.GET('/api/cron-runs', {
        signal,
        // skip_total=true skips the COUNT(*) FROM cron_runs WHERE
        // task=? precount inside the repo — useLastSync only reads
        // entries[0] and never touches `total`, so the count was a
        // 200-330 ms wasted scan per call on a busy service.
        params: { query: { task: 'sync', per_page: 1, page: 1, skip_total: true } },
      } as never)
      if (error) throw error
      const e = (data as CronRunsResponse | undefined)?.entries?.[0]
      return {
        started_at: e?.started_at ?? null,
        status: e?.status ?? null,
        duration_s: e?.duration_s ?? null,
      } satisfies LastSyncInfo
    },
    enabled: !!activeServiceId && !isAnalyst,
    // 5 min lines up with the bootstrap seed lifecycle — the
    // ['last-sync', sid] cache entry is pre-seeded BEFORE this hook renders,
    // from bootstrap.last_sync: on the SSR-hydrated path via app/layout.tsx's
    // dehydrated-state seed, and on the pure-CSR path via useBootstrap.queryFn.
    // Pre-seeding the entry is also what keeps useQuery from building
    // ['last-sync', sid] during SyncStatusBadge's render (which would notify
    // the sibling useSyncStatus observer mid-render). The SSE invalidation in
    // useAdminEventStream coalesces real completion events. The 60 s staleTime +
    // cron-runs SSE bursts on cold mount used to fire 1-2 needless refetches
    // in the first second.
    staleTime: 5 * 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
