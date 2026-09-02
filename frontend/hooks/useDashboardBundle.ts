'use client'

import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { throwIfStaleAggregates, STALE_VIEW_RETRY_OPTIONS } from '@/lib/staleViewRetry'
import { resolveRangeWire } from '@/lib/range-wire'
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
  // startTime/endTime drive the stale-view extents heuristic + the chart's
  // x-axis display range, AND (in custom-range mode) the scan window itself.
  startTime: string | null
  endTime: string | null
  // Time-range wire inputs (lib/range-wire.ts). In token mode the bundle key +
  // body key on the server-reproducible relative token + quantized anchor so the
  // SSR seed key byte-matches the client first-paint key. In custom-absolute
  // mode (relativeRange null + isAutoRange false) the key/body carry the explicit
  // start/end bounds instead, so a custom range scans exactly what it displays.
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
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
  relativeRange,
  isAutoRange,
  anchor,
  filterPayload,
  metric,
  interval,
  enabled,
  fields,
  sections,
}: DashboardBundleArgs) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // Token mode (preset pill / cold-load default) keys on the server-reproducible
  // (rangeKey, anchor) so the SSR seed in app/dashboard/page.tsx byte-matches and
  // paints from the dehydrated cache on first paint. Custom-absolute mode keys on
  // an "abs:<start>|<end>" identity so distinct custom ranges don't collide.
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
  const bundleKey = ['dashboard', 'bundle', activeServiceId, rangeKey, anchor, filterPayload, metric, interval, fields, sections]

  return useQuery({
    queryKey: bundleKey,
    queryFn: async ({ signal }) => {
      const { data } = await client.POST('/api/dashboard/bundle', {
        signal,
        body: {
          // Token mode → {range_token, anchor} + display bounds (server
          // resolves the window from the token and ignores the bounds; they
          // only matter if the token isn't recognized — see lib/range-wire.ts).
          // Custom mode → {start_time, end_time} only.
          filters: filterPayload,
          chart_metric: metric as any,
          chart_interval: interval,
          fields: fields,
          sections: sections,
          ...rangeBody,
        },
      })
      const body = data
      if (body?.aggregates) {
        // Stale-view guard: throws if the response is the empty-schema
        // placeholder from a mid-commit window — STALE_VIEW_RETRY_OPTIONS retries
        // once. The window passed here is the DISPLAY range (startTime/endTime);
        // it only steers the "empty because outside data extents" heuristic, not
        // the scan (which the server resolved from the token). Returned in-place
        // on body.aggregates — the page reads aggregates + top_bots directly off
        // bundleQuery.data (no separate cache keys), so no setQueryData fan-out.
        throwIfStaleAggregates(
          body.aggregates,
          { startTime, endTime },
          Object.keys(filterPayload).length > 0,
        )
      }
      return body
    },
    enabled,
    placeholderData: (previousData, previousQuery) => {
      if (!previousQuery) return undefined
      if (
        Array.isArray(bundleKey) &&
        Array.isArray(previousQuery.queryKey) &&
        bundleKey.length > 2 &&
        previousQuery.queryKey.length > 2 &&
        bundleKey[2] !== previousQuery.queryKey[2]
      ) {
        return undefined
      }
      return previousData
    },
    ...STALE_VIEW_RETRY_OPTIONS,
    refetchInterval: isAutoRange ? 5000 : false,
    refetchIntervalInBackground: false,
  })
}
