'use client'

import { useQueryClient } from '@tanstack/react-query'
import { useServiceStream } from '@/hooks/useServiceStream'
import { useBootstrapSettled } from '@/hooks/useIsDataReady'

/**
 * Subscribe to ``/api/admin/system-metrics/stream`` and fan the
 * bundled payload out into the React Query slice keys the admin
 * overview cards already read from. Replaces seven independent
 * polling queries:
 *
 *  - ``['admin', 'health-snapshot']``                  (10s poll)
 *  - ``['admin', 'metric-history-batch', '1h']``        (60s poll)
 *  - ``['admin', 'overview', 'queries-summary']``       (10s poll)
 *  - ``['admin', 'overview', 'slow-queries-count']``    (10s poll)
 *  - ``['admin', 'overview', 'log-accounting']``        (30s poll)
 *  - ``['admin', 'metadata-storage']``                  (60s poll)
 *  - ``['system-jobs']``                                (30s poll)
 *
 * Backend pushes only when the bundle actually changes, so the wire
 * is quiet when nothing's moving. Components keep their existing
 * ``useQuery`` hooks (with long safety-net intervals) — this hook
 * just writes fresh data into the cache via ``setQueryData`` so no
 * fetch is triggered (and so the FilterBar pill stays calm).
 *
 * Caller is responsible for gating ``enabled`` to admin sessions on
 * pages that actually render these cards — the endpoint is
 * admin-only and would 403-loop on analysts, and an unused stream
 * wastes a server-side sampler loop.
 *
 * Soft service scoping (``optionalService: true``): the stream connects
 * even when no service is selected, so the GLOBAL slices (health,
 * metric-history, queries-summary, system-jobs) stream live on a fresh
 * install before any service exists. The three service-scoped slices
 * arrive as ``null`` until a service is selected — at which point the
 * stream reconnects with ``x-service-id`` and they light up.
 */

interface SystemMetricsPayload {
  health_snapshot?: unknown
  metric_history_1h?: unknown
  queries_summary?: unknown
  slow_queries_count?: unknown
  log_accounting?: unknown
  metadata_storage?: unknown
  system_jobs?: unknown
}

export function useSystemMetricsStream(enabled: boolean) {
  const queryClient = useQueryClient()
  // Hold the (optionalService) stream until bootstrap has resolved. Bootstrap's
  // queryFn seeds both the active service id and the admin token; connecting
  // before that opens on the pre-bootstrap serviceId / null token and then
  // aborts + reconnects once they settle — the per-admin-load "context
  // canceled" the reverse proxy warns about. Gating here makes it connect ONCE,
  // already scoped and authed. (Hydration gating inside useServiceStream covers
  // the persisted-store restore; this covers the bootstrap-fetch settle.)
  const bootstrapSettled = useBootstrapSettled()

  useServiceStream(
    enabled && bootstrapSettled,
    '/api/admin/system-metrics/stream',
    (raw) => {
      let payload: SystemMetricsPayload
      try {
        payload = JSON.parse(raw) as SystemMetricsPayload
      } catch {
        // Malformed event (likely sse_starlette keepalive arriving in
        // an unexpected frame layout) — skip; next push will resync.
        return
      }
      // Only dispatch slices that are present (non-null). A failed
      // component sample comes through as null; we deliberately don't
      // overwrite the last-good cached data in that case so the card
      // keeps showing stale-but-readable values instead of empty state.
      if (payload.health_snapshot != null) {
        queryClient.setQueryData(['admin', 'health-snapshot'], payload.health_snapshot)
      }
      if (payload.metric_history_1h != null) {
        queryClient.setQueryData(['admin', 'metric-history-batch', '1h'], payload.metric_history_1h)
      }
      if (payload.queries_summary != null) {
        queryClient.setQueryData(['admin', 'overview', 'queries-summary'], payload.queries_summary)
      }
      if (payload.slow_queries_count != null) {
        queryClient.setQueryData(['admin', 'overview', 'slow-queries-count'], payload.slow_queries_count)
      }
      if (payload.log_accounting != null) {
        queryClient.setQueryData(['admin', 'overview', 'log-accounting'], payload.log_accounting)
      }
      if (payload.metadata_storage != null) {
        queryClient.setQueryData(['admin', 'metadata-storage'], payload.metadata_storage)
      }
      if (payload.system_jobs != null) {
        queryClient.setQueryData(['system-jobs'], payload.system_jobs)
      }
    },
    { cache: 'no-store', optionalService: true },
  )
}
