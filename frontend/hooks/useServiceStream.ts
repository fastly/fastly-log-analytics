'use client'

import { useCallback, useSyncExternalStore } from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { buildStreamRequest } from '@/lib/streamRequest'
import { useEventStream, type EventStreamRequest, type SSEConnectionState } from '@/hooks/useEventStream'

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
