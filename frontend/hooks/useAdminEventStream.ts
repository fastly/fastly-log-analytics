'use client'

import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { buildStreamRequest } from '@/lib/streamRequest'
import { useEventStream, type EventStreamRequest, type SSEConnectionState } from '@/hooks/useEventStream'
import { useBootstrapSettled } from '@/hooks/useIsDataReady'
import {
  applyShare,
  applySyncStatus,
  applySystemMetrics,
  makeCronRunsApplier,
  type CronRunsApplier,
} from '@/lib/admin-stream-apply'
import { queryKeys } from '@/lib/query-keys'
import type { Schema } from '@/types/api'

interface UseServiceStreamOpts {
  /** Pass ``cache: 'no-store'`` for endpoints whose response must not
   *  be cached by the browser fetch layer. */
  cache?: RequestCache
  /** "Soft" service scoping for bundles that carry BOTH global and
   *  service-scoped slices (e.g. the admin system-metrics stream): connect
   *  even when no service is selected — so the global slices stream on a
   *  fresh install — but still send ``x-service-id`` and reconnect on switch
   *  when one IS selected, so the service-scoped slices stay correctly
   *  scoped. Default ``false``. */
  optionalService?: boolean
}

/**
 * True once the persisted service store has rehydrated from localStorage.
 *
 * serviceStore uses ``persist({ skipHydration: true })`` and
 * ``<StoreHydrator>`` calls ``persist.rehydrate()`` in a post-paint effect,
 * so ``activeServiceId`` flips ``null`` → ``<persisted id>`` ~50ms after
 * mount. An ``optionalService`` stream connects on the null value and would
 * then abort + reconnect to pick up the ``x-service-id`` — a throwaway
 * upstream the reverse proxy logs as "aborting with incomplete response /
 * reading: context canceled" on every load. Gating the first connect on this
 * flag lets the stream open ONCE, already scoped.
 *
 * Defensive: if the persist API is absent (the unit-test store mock, or any
 * future non-persisted store) report ``true`` so those paths connect
 * immediately, exactly as before.
 */
function useServiceStoreHydrated(): boolean {
  const persist = useServiceStore.persist
  // useSyncExternalStore (not useState+useEffect) so the read is SSR-safe and
  // free of setState-in-effect: server + first-client snapshot agree (both
  // pre-rehydrate → false), then onFinishHydration nudges React to re-read
  // once <StoreHydrator> completes. Mirrors the ReloadLoopGuard pattern.
  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      persist?.onFinishHydration?.(onStoreChange) ?? (() => {}),
    [persist],
  )
  const getSnapshot = () => persist?.hasHydrated?.() ?? true
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

/**
 * Subscribe to a service-scoped SSE channel and dispatch each ``data:``
 * payload to ``onEvent``. Owns the abort/reader/decoder lifecycle, the
 * spec-compliant event-boundary regex, and the exponential reconnect
 * backoff (1s → 30s cap). Consumer is responsible for whatever the
 * payload should DO (setQueryData, schedule invalidations, etc.).
 *
 * The hook gates on ``enabled`` AND on ``activeServiceId`` — every
 * service-scoped SSE endpoint requires both ``x-service-id`` and a
 * valid (admin or analyst) session, so a null serviceId would fail
 * the backend gate immediately and waste reconnect budget.
 *
 * Caller is responsible for gating ``enabled`` to the right
 * audience (e.g. admin-only or analyst-only) — the middleware
 * blocks the wrong audience but the stream would just 403-loop
 * before getting there.
 *
 * Wire format: sse-starlette emits ``data: ...\r\n\r\n``. The split
 * regex covers all spec-allowed event separators (\r\n\r\n, \n\n,
 * \r\r). See useSSE.ts for the longer story.
 *
 * URL construction: string concat (not ``new URL(path, base)``) so an
 * empty base (public Fastly deploy where ``getApiBase()`` returns ""
 * for relative proxying) doesn't TypeError into the silent-retry
 * catch.
 */
export function useServiceStream(
  enabled: boolean,
  path: string,
  onEvent: (raw: string) => void,
  opts: UseServiceStreamOpts = {},
): { state: SSEConnectionState } {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const storeHydrated = useServiceStoreHydrated()
  const { cache, optionalService = false } = opts
  const awaitingHydration = optionalService && !storeHydrated
  const effectiveEnabled = enabled && (optionalService || !!activeServiceId) && !awaitingHydration

  // Build the request lazily — the adapter owns url/header construction, the
  // primitive owns the fetch/read/reconnect loop.
  const request: EventStreamRequest | null = effectiveEnabled
    ? buildStreamRequest({ path, serviceId: activeServiceId, cache })
    : null

  return useEventStream(effectiveEnabled, request, onEvent)
}

export const adminStreamHooks = {
  useServiceStream,
}

/**
 * Analyst-safe sibling of ``useAdminEventStream``.
 *
 * Subscribes to ``/api/log-extents/stream`` — the projected, analyst-
 * safe header-badge channel (request/RUM metrics and log extents,
 * no ``ngwaf_workspace_id`` / ``active_run`` / etc). Writes incoming
 * payloads into the SAME React Query slot the analyst bootstrap
 * already seeds (``['bootstrap']`` → ``header_badge``). Omitted fields
 * preserve cached values; explicit nulls clear them after a reset.
 *
 * Closes Gap 3 from the badge SSE work: admins already get real-time
 * "Latest Log: Xs ago" / "Total Logs" updates via
 * ``useAdminEventStream``; this hook brings analysts to parity.
 */
type BootstrapHeaderBadge = Schema<'ShareHeaderBadgeResponse'>

interface BootstrapShape {
  header_badge?: BootstrapHeaderBadge | null
  [key: string]: unknown
}

type BadgeStreamEvent = Schema<'ShareHeaderBadgeResponse'>

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()

  return useServiceStream(
    enabled,
    '/api/log-extents/stream',
    (raw) => {
      try {
        const payload = JSON.parse(raw) as BadgeStreamEvent
        queryClient.setQueryData<BootstrapShape | undefined>(queryKeys.bootstrap(), (prev) => {
          // If bootstrap hasn't loaded yet (cold load before
          // hydration), skip — the bootstrap fetch will land
          // shortly and supply the initial header_badge slot.
          if (!prev) return prev
          return {
            ...prev,
            header_badge: {
              ...(prev.header_badge ?? {}),
              rum: payload.rum === undefined ? prev.header_badge?.rum
                : payload.rum === null ? null : { ...prev.header_badge?.rum, ...payload.rum },
              request: payload.request === undefined ? prev.header_badge?.request
                : payload.request === null ? null : { ...prev.header_badge?.request, ...payload.request },
              latest_log_at: payload.latest_log_at !== undefined ? payload.latest_log_at : prev.header_badge?.latest_log_at ?? null,
              local_rows: payload.local_rows !== undefined ? payload.local_rows : prev.header_badge?.local_rows ?? null,
            },
          }
        })
      } catch {
        // Malformed payload; skip.
      }
    },
  )
}

/**
 * The admin channels a single multiplexed connection can carry. Mirrors
 * ``_ADMIN_EVENT_CHANNELS`` in ``backend/routers/admin/events.py``.
 */
export type AdminEventChannel = 'sync-status' | 'cron-runs' | 'system-metrics' | 'share' | 'celery-status'

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

  return adminStreamHooks.useServiceStream(
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
        case 'celery-status':
          queryClient.setQueryData(queryKeys.admin.celeryStatus(), env.data)
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
