'use client'

import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { throwIfStaleAggregates, STALE_VIEW_RETRY_OPTIONS } from '@/lib/staleViewRetry'
import type { FiltersPayload } from '@/types/filters'

/**
 * Composite dashboard fetch — /api/dashboard/bundle returns the two
 * queries the dashboard page mounts on every cold load (aggregates +
 * security/top-bots) in a single round-trip.
 *
 * Pattern: the bundle's queryFn seeds the SAME cache keys the
 * existing dedicated hooks use, then returns the merged result.
 * The dedicated hooks gate on this query being in-flight, so:
 *   - cold load: bundle fires → seeds caches → dedicated hooks read
 *     cache, no fetch.
 *   - warm cache (returning to dashboard): bundle hits its own cache,
 *     same seed re-applies (no-op for unchanged data), dedicated
 *     hooks already had cache.
 *
 * Saves one RTT per cold dashboard load on prod (~150-200 ms via
 * Caddy + Fastly).
 *
 * Compare mode keeps its own dedicated /api/dashboard/aggregates
 * call — it only fires when the user explicitly enables compare, so
 * it's not part of the cold-load path.
 */
export interface DashboardBundleArgs {
  startTime: string | null
  endTime: string | null
  filterPayload: FiltersPayload
  metric: string
  interval: string
  enabled: boolean
  fields?: string[]
}

export function useDashboardBundle({
  startTime,
  endTime,
  filterPayload,
  metric,
  interval,
  enabled,
  fields,
}: DashboardBundleArgs) {
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()

  const aggregatesKey = ['dashboard', 'aggregates', activeServiceId, startTime, endTime, filterPayload, metric, interval, fields]
  const topBotsKey = ['dashboard', 'top-bots', activeServiceId, startTime, endTime, filterPayload]
  const bundleKey = ['dashboard', 'bundle', activeServiceId, startTime, endTime, filterPayload, metric, interval, fields]

  return useQuery({
    queryKey: bundleKey,
    queryFn: async ({ signal }) => {
      const { data } = await client.POST('/api/dashboard/bundle' as any, {
        signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          chart_metric: metric as any,
          chart_interval: interval,
          fields: fields,
        },
      })
      const body = data as { aggregates?: any; top_bots?: any } | undefined
      if (body?.aggregates) {
        // Same stale-view check the dedicated hook applies. Throws if
        // the response is the empty-schema placeholder from a mid-
        // commit window — STALE_VIEW_RETRY_OPTIONS will retry once.
        const aggsChecked = throwIfStaleAggregates(body.aggregates)
        queryClient.setQueryData(aggregatesKey, aggsChecked)
      }
      if (body?.top_bots) {
        queryClient.setQueryData(topBotsKey, body.top_bots)
      }
      return body
    },
    enabled,
    placeholderData: keepPreviousData,
    ...STALE_VIEW_RETRY_OPTIONS,
  })
}
