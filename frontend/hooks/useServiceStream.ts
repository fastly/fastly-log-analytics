'use client'

import { useCallback, useEffect, useState, useSyncExternalStore } from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { useAdminTokenStore } from '@/stores/adminTokenStore'
import { getApiBase } from '@/lib/api'
import { parseSSEFrames } from '@/lib/sse-parser'

/** Public connection-state for the SSE stream, surfaced for UI affordances.
 *  - ``idle`` — hook disabled (no service / not enabled).
 *  - ``connecting`` — initial connect in progress, never had a frame yet.
 *  - ``open`` — connected, reading the body.
 *  - ``reconnecting`` — connection dropped, sleeping in the backoff window.
 *
 *  There's no ``error`` terminal — the hook retries forever, so callers
 *  treat ``reconnecting`` lasting "too long" as the error condition.
 */
export type SSEConnectionState = 'idle' | 'connecting' | 'open' | 'reconnecting'

interface UseServiceStreamOpts {
  /** Pass ``cache: 'no-store'`` for endpoints whose response must not
   *  be cached by the browser fetch layer. Note: do NOT add a
   *  ``Cache-Control: no-cache`` request header — FastAPI's
   *  CORSMiddleware uses ``allow_headers=["*"]`` with
   *  ``allow_credentials=true`` which browsers reject per the CORS
   *  spec, silently failing the preflight and the hook will hot-retry
   *  forever without ever delivering an event. ``cache: 'no-store'``
   *  bypasses caching without crossing the CORS line. */
  cache?: RequestCache
  /** When ``false``, the stream is NOT service-scoped: the hook neither
   *  gates on ``activeServiceId`` nor sends the ``x-service-id`` header,
   *  and service switches don't reconnect it. Used by global-admin
   *  channels (e.g. the share dashboard) that own no service. Default
   *  ``true`` — every service-scoped endpoint needs the header + a service. */
  requireService?: boolean
  /** "Soft" service scoping for bundles that carry BOTH global and
   *  service-scoped slices (e.g. the admin system-metrics stream): connect
   *  even when no service is selected — so the global slices stream on a
   *  fresh install — but still send ``x-service-id`` and reconnect on switch
   *  when one IS selected, so the service-scoped slices stay correctly
   *  scoped. Unlike ``requireService: false`` (which NEVER sends the header),
   *  this only drops the header while serviceless. Overrides
   *  ``requireService``. Default ``false``.
   *
   *  Callers should gate ``enabled`` on ``useBootstrapSettled()`` (see
   *  useSystemMetricsStream): an optionalService stream connects before a
   *  service is chosen, so without it the stream opens on the pre-bootstrap
   *  serviceId / null admin token and aborts + reconnects once bootstrap
   *  seeds them — the per-load "context canceled" the reverse proxy warns
   *  about. The hydration gate below covers the persisted-store restore but
   *  not the bootstrap-fetch settle. */
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
  const persist = (useServiceStore as unknown as {
    persist?: {
      hasHydrated?: () => boolean
      onFinishHydration?: (cb: () => void) => () => void
    }
  }).persist
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
  const adminToken = useAdminTokenStore(s => s.token)
  const storeHydrated = useServiceStoreHydrated()
  const { cache, requireService = true, optionalService = false } = opts
  // optionalService: connect even when no service is selected (so the bundle's
  // GLOBAL slices stream on a fresh install), but still send x-service-id +
  // reconnect on switch when one IS selected so any service-scoped slices in
  // the same bundle stay correctly scoped. NOT the same as requireService:false,
  // which never sends the header at all.
  const gateOnService = requireService && !optionalService
  // Non-service-scoped streams (requireService:false) collapse the id to null
  // so switches never reconnect them and no header is sent. requireService and
  // optionalService both keep the id so the header is sent when present and a
  // switch reconnects.
  const serviceId = requireService || optionalService ? activeServiceId : null
  // optionalService opens before a service is chosen. Without waiting for the
  // persisted store to rehydrate, it would connect on the null serviceId and
  // immediately abort + reconnect once <StoreHydrator> restores activeServiceId
  // — a wasted upstream the reverse proxy logs as "context canceled" on every
  // load. Hold the first connect until hydration settles so it opens once,
  // already carrying the right x-service-id (or correctly serviceless on a fresh
  // install). Streams that gate on serviceId don't need this: they don't connect
  // until serviceId is truthy, which is itself post-hydration.
  const awaitingHydration = optionalService && !storeHydrated
  const [state, setState] = useState<SSEConnectionState>('idle')

  useEffect(() => {
    if (!enabled || (gateOnService && !serviceId) || awaitingHydration) {
      setState('idle')
      return
    }

    const abort = new AbortController()
    let cancelled = false
    let attempt = 0

    const safeSetState = (next: SSEConnectionState) => {
      if (!cancelled) setState(next)
    }

    const run = async () => {
      while (!cancelled) {
        safeSetState(attempt === 0 ? 'connecting' : 'reconnecting')
        try {
          const url = `${getApiBase()}${path}`
          const res = await fetch(url, {
            signal: abort.signal,
            ...(cache ? { cache } : {}),
            headers: {
              'Accept': 'text/event-stream',
              ...(serviceId ? { 'x-service-id': serviceId } : {}),
              // Admin SSE endpoints sit behind ADMIN_SHARED_SECRET when
              // configured; lib/api.ts injects X-Admin-Token on the
              // openapi-fetch path but fetch() here bypasses that
              // middleware, so without this the stream 401-loops
              // silently inside the catch.
              ...(adminToken ? { 'X-Admin-Token': adminToken } : {}),
            },
          })
          if (!res.ok || !res.body) {
            if (res.status === 401 || res.status === 403) {
              cancelled = true
            }
            throw new Error(`HTTP ${res.status}`)
          }
          attempt = 0  // successful connect resets the backoff
          safeSetState('open')

          const reader = res.body.getReader()
          const decoder = new TextDecoder()
          let buf = ''
          while (!cancelled) {
            const { done, value } = await reader.read()
            if (done) break
            buf += decoder.decode(value, { stream: true })
            const { frames: events, remainder } = parseSSEFrames(buf)
            buf = remainder
            for (const ev of events) {
              const dataLine = ev.split('\n').find(l => l.startsWith('data:'))
              if (!dataLine) continue
              const raw = dataLine.slice('data:'.length).trim()
              if (!raw) continue
              onEvent(raw)
            }
          }
        } catch {
          if (cancelled) return
          // Fall through to backoff retry — surface as reconnecting.
        }
        if (cancelled) return
        safeSetState('reconnecting')
        const delay = Math.min(30_000, 1_000 * 2 ** attempt++)
        await new Promise(resolve => setTimeout(resolve, delay))
      }
    }

    run()

    return () => {
      cancelled = true
      abort.abort()
    }
    // onEvent is intentionally NOT a dep — callers pass an inline
    // arrow that changes every render; including it would tear down
    // and reopen the stream on every render. Callers should rely on
    // closure-captured values being read at event time (the typical
    // setQueryData / scheduler patterns are stable across renders).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, gateOnService, serviceId, adminToken, path, cache, awaitingHydration])

  return { state }
}
