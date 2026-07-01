'use client'

import { useEffect, useMemo, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { useServiceStream } from '@/hooks/useServiceStream'
import { useBootstrapSettled } from '@/hooks/useIsDataReady'
import {
  applyShare,
  applySyncStatus,
  applySystemMetrics,
  makeCronRunsApplier,
  type CronRunsApplier,
} from '@/lib/admin-stream-apply'

/**
 * The admin channels a single multiplexed connection can carry. Mirrors
 * ``_ADMIN_EVENT_CHANNELS`` in ``backend/routers/admin/events.py``.
 */
export type AdminEventChannel = 'sync-status' | 'cron-runs' | 'system-metrics' | 'share'

/**
 * Subscribe to the multiplexed admin event stream
 * (``/api/admin/events/stream``) over ONE connection and demux each
 * envelope (``{channel, data}``) to the same React Query cache behavior
 * the old per-channel hooks owned.
 *
 * Replaces three separate SSE connections (``useSyncStatusStream``,
 * ``useCronRunsStream``, ``useSystemMetricsStream``) — over the HTTP/1.1
 * admin tunnel those held three of the browser's ~6 per-origin
 * connections open indefinitely, starving bootstrap + panel fetches.
 *
 * Caller gates ``enabled`` to admin sessions — the endpoint is admin-only
 * (middleware 403s analysts). Analysts keep their own single
 * ``useHeaderBadgeStream`` (log-extents), untouched.
 *
 * Service scoping is ``optionalService`` (the union of the merged
 * channels): connects even before a service is selected so the global
 * ``system-metrics`` slices stream on a fresh install, sends
 * ``x-service-id`` and reconnects on switch when one IS selected so the
 * service-scoped channels (sync-status, cron-runs) stay correctly scoped.
 * ``enabled`` is held until bootstrap settles to avoid the per-load
 * "context canceled" reconnect (see ``useSystemMetricsStream`` history).
 */
export function useAdminEventStream(enabled: boolean, channels: AdminEventChannel[]) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const bootstrapSettled = useBootstrapSettled()

  // Stable channel ordering so the URL (and thus the useServiceStream
  // connection key) doesn't churn when the caller passes a freshly built
  // array each render.
  const channelsParam = useMemo(() => [...channels].sort().join(','), [channels])

  // The cron-runs applier owns a trailing-edge coalesce window; pin it in
  // a ref so the (recreated-each-render) onEvent closure always sees the
  // live instance. Recreated + cleaned up when the connection key changes
  // (service switch / disable / channel-set change) so a pending
  // invalidation doesn't carry into the next mount.
  const cronApplierRef = useRef<CronRunsApplier | null>(null)
  useEffect(() => {
    if (!enabled || !channelsParam.split(',').includes('cron-runs')) {
      return
    }
    const applier = makeCronRunsApplier(queryClient, activeServiceId)
    cronApplierRef.current = applier
    return () => {
      applier.cleanup()
      if (cronApplierRef.current === applier) cronApplierRef.current = null
    }
  }, [enabled, activeServiceId, channelsParam, queryClient])

  const path = `/api/admin/events/stream?channels=${channelsParam}`

  return useServiceStream(
    enabled && bootstrapSettled && channelsParam.length > 0,
    path,
    (raw) => {
      let env: { channel?: string; data?: unknown }
      try {
        env = JSON.parse(raw) as { channel?: string; data?: unknown }
      } catch {
        // Malformed envelope (e.g. an sse-starlette keepalive comment in
        // an unexpected frame layout) — skip; the next push resyncs.
        return
      }
      if (!env || typeof env !== 'object') return
      switch (env.channel) {
        case 'sync-status':
          applySyncStatus(queryClient, activeServiceId, env.data)
          break
        case 'system-metrics':
          applySystemMetrics(queryClient, activeServiceId, env.data)
          break
        case 'share':
          applyShare(queryClient, env.data)
          break
        case 'cron-runs': {
          // Lazily (re)create defensively in case an event lands before
          // the coalescer effect ran for this connection.
          if (!cronApplierRef.current) {
            cronApplierRef.current = makeCronRunsApplier(queryClient, activeServiceId)
          }
          cronApplierRef.current.apply(env.data)
          break
        }
        default:
          // Unknown channel — ignore (forward-compat with a backend that
          // adds channels this client doesn't yet handle).
          break
      }
    },
    { cache: 'no-store', optionalService: true },
  )
}
