'use client'

import { useEffect, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { useBootstrapPending } from '@/hooks/useIsDataReady'
import type { components } from '@/types/api.generated'
import { queryKeys } from '@/lib/query-keys'

type KeyFn = (serviceId: string) => readonly unknown[]

/**
 * Pre-seed ``null`` into each per-service cache entry that does not exist yet,
 * then report ``true`` once that has happened. Gate ``useQuery({ enabled })``
 * on the result so the query is never built mid-render: building a cache entry
 * during render notifies the QueryCache and trips React 19's
 * setState-in-render warning on sibling subscribers.
 */
export function useSeededReady(
  serviceId: string | null | undefined,
  keyFns: readonly KeyFn[],
  readyWithoutService = false,
): boolean {
  const queryClient = useQueryClient()
  const [ready, setReady] = useState(false)
  useEffect(() => {
    if (serviceId) {
      for (const keyFn of keyFns) {
        const key = keyFn(serviceId)
        if (queryClient.getQueryData(key) === undefined) {
          queryClient.setQueryData(key, null)
        }
      }
    }
    const next = serviceId ? true : readyWithoutService
    queueMicrotask(() => {
      setReady(next)
    })
  }, [serviceId, queryClient, keyFns, readyWithoutService])
  return ready
}

const SEED_KEYS = [queryKeys.syncStatus] as const
const LAST_SYNC_SEED_KEYS = [queryKeys.lastSync] as const

export type SyncStatus = components['schemas']['SyncStatusResponse']
type CronRunEntry = components['schemas']['CronRunEntry']
type LastSyncInfo = { [K in 'started_at' | 'status' | 'duration_s']: CronRunEntry[K] | null }

/**
 * Single source of truth for `/api/sync-status`.
 */
export function useSyncStatus() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const bootstrapPending = useBootstrapPending()
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

/**
 * Fetches the most recent ``sync``-task row from ``/api/cron-runs``,
 * surfaced as the "Last Sync: Xs ago" header badge.
 */
export function useLastSync() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const isAnalyst = useIsAnalyst()
  const bootstrapPending = useBootstrapPending()
  const ready = useSeededReady(activeServiceId, LAST_SYNC_SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.lastSync(activeServiceId),
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(
        client.GET('/api/cron-runs', {
          signal,
          params: { query: { task: 'log_discovery', per_page: 1, page: 1, skip_total: true } },
        }),
      )
      const e = data?.entries?.[0]
      return {
        started_at: e?.started_at ?? null,
        status: e?.status ?? null,
        duration_s: e?.duration_s ?? null,
      } satisfies LastSyncInfo
    },
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 5 * 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
