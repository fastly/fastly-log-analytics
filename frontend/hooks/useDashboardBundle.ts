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
/**
 * P-4 slice 3: section-selector alias mirroring /security and /network.
 * The dashboard bundle's three logical sections — 'core' (time_series +
 * map_data + conn_requests + total_rows), 'topten' (the ~85 top-N cards),
 * and 'bots' (top_bots) — are forwarded to the backend so future server-
 * side gating can suppress unwanted blocks without an API rev. Until the
 * backend honors the field it's a no-op: the bundle still returns the
 * full payload and seeds the same cache keys.
 *
 * The bundle stays a single round-trip rather than fanning out into
 * three parallel POSTs because (1) execute_top_n_rollups does ONE
 * merged scan across the entire field batch — splitting top-N across
 * HTTP requests would re-enumerate the rollup directory and rebuild
 * the active-hour live_temp per request, and (2) the existing seeding
 * contract that other dedicated hooks rely on would need a wider
 * migration.
 */
export type DashboardSection = 'core' | 'topten' | 'bots'

export interface DashboardBundleArgs {
  startTime: string | null
  endTime: string | null
  filterPayload: FiltersPayload
  metric: string
  interval: string
  enabled: boolean
  fields?: string[]
  sections?: DashboardSection[]
}

export function useDashboardBundle({
  startTime,
  endTime,
  filterPayload,
  metric,
  interval,
  enabled,
  fields,
  sections,
}: DashboardBundleArgs) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()

  const aggregatesKey = ['dashboard', 'aggregates', activeServiceId, startTime, endTime, filterPayload, metric, interval, fields]
  const topBotsKey = ['dashboard', 'top-bots', activeServiceId, startTime, endTime, filterPayload]
  const bundleKey = ['dashboard', 'bundle', activeServiceId, startTime, endTime, filterPayload, metric, interval, fields, sections]

  return useQuery({
    queryKey: bundleKey,
    queryFn: async ({ signal }) => {
      const { data } = await client.POST('/api/dashboard/bundle', {
        signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          chart_metric: metric as any,
          chart_interval: interval,
          fields: fields,
          sections: sections,
        },
      })
      const body = data
      if (body?.aggregates) {
        // Same stale-view check the dedicated hook applies. Throws if
        // the response is the empty-schema placeholder from a mid-
        // commit window — STALE_VIEW_RETRY_OPTIONS will retry once.
        // Pass the queried window so an empty result for a range that
        // sits outside the data's extents (e.g. the default 24h window
        // on a fresh install whose logs are older) isn't misread as a
        // stale view and retried-then-hard-failed.
        const aggsChecked = throwIfStaleAggregates(body.aggregates, { startTime, endTime })
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
