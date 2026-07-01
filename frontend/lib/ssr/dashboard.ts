// Server-only SSR fetcher for the /dashboard page. Pre-fetches the default
// /api/dashboard/bundle selection so the traffic chart, geo map and aggregation
// cards render from cache on first paint instead of paying the client
// round-trip + skeleton flash on cold load. Replicates the /origin SSR template
// (commit 319c1b0) — a POST caller of the shared transport (lib/ssr/_transport.ts),
// inheriting its Caddy-marker trust gate + analyst-clamp / X-Remote-Analyst
// promotion unchanged.
//
// WHY dashboard is SSR-seedable now: the bundle first-paint key used to carry
// absolute start/end timestamps anchored to the client's `new Date()` —
// server-unreproducible. We ported the range_token/anchor relative-range wire
// contract to /api/dashboard/bundle (+ /aggregates) and re-keyed
// useDashboardBundle on (rangeToken, anchor). Both are server-reproducible, so
// the seed key byte-matches the client key.
//
// The bundle is COMPOSITE: it returns aggregates + top_bots in one round-trip,
// and the page reads BOTH directly off bundleQuery.data (no separate cache
// keys). So seeding this single bundle key paints the whole cold load from
// cache with NO follow-up fetch — top_bots no longer fires its own request.
//
// KEY-MATCH CONTRACT (load-bearing — a mismatch double-fetches, strictly worse
// than no SSR): the (rangeToken, anchor) this helper seeds MUST equal what
// DashboardClient computes on first paint:
//   ['dashboard','bundle', sid, rangeToken, anchor, filterPayload, metric, interval, fields, sections]
// On a cold load every element is server-reproducible:
//   - sid          = bootstrap.active_service_id
//   - rangeToken   = '24h'  (no ?range= → filterStore.relativeRange null; the
//                    cold-load token matches the 24h store-default display window
//                    so the chart x-axis range and the scan window agree — the
//                    old 'auto' resolved to 30d for mature services and squashed
//                    the bars under an off-screen-spike y-axis)
//   - anchor       = quantizeAnchor(now) on the 60s grid (client pins the same;
//                    only divergence is the rare minute-boundary straddle → one
//                    client refetch, never a leak/crash)
//   - filterPayload= {}      (no filters on a cold load)
//   - metric       = 'requests'  (DashboardBody's useState default)
//   - interval     = '1 hour'    (useReportConfig picks "1 hour" at the 24h
//                    default span, overriding defaultInterval="1 minute" — same
//                    determinism /origin relies on)
//   - fields       = undefined   (DashboardBody passes none)
//   - sections     = ['core','topten','bots']  (DASHBOARD_SECTIONS)

import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

// Mirror DASHBOARD_SECTIONS in app/dashboard/_sections/DashboardClient.tsx — the
// page consumes all three. Part of both the request body and the key; kept in
// lockstep (a divergence changes the body shape and misses the seed).
export const DASHBOARD_SSR_SECTIONS = ['core', 'topten', 'bots'] as const

// First-paint key elements that are NOT (rangeToken, anchor). Each must equal the
// client's cold-load value (see the KEY-MATCH CONTRACT above) or the seed misses.
export const DASHBOARD_SSR_DEFAULTS = {
  metric: 'requests' as const,
  interval: '1 hour' as const,
} as const

export interface DashboardSsrSeed {
  /** The /api/dashboard/bundle response body to seed into the cache. */
  data: unknown
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

/**
 * Resolve the default (rangeToken, anchor) pair the client computes on first
 * paint. Exported so the unit test can pin the key-match without a network
 * round-trip. `now` is injectable for deterministic tests.
 */
export function resolveDashboardDefaultKey(now: Date = new Date()): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

/**
 * SSR-prefetch the default dashboard bundle for the active service.
 *
 * @param serviceId  Active service id (from the SSR'd bootstrap). Without it the
 *                   backend's context resolution 400s — bail to client fetch.
 * @param now        Render instant; floored to the anchor quantum. Injectable.
 */
export async function fetchDashboardServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<DashboardSsrSeed | null> {
  const { rangeToken, anchor } = resolveDashboardDefaultKey(now)

  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/dashboard/bundle',
      logPrefix: 'ssr/dashboard',
      method: 'POST',
      // Admin SSH-tunnel branch: when ADMIN_SHARED_SECRET is set the backend's
      // admin-token gate 401s a loopback request without X-Admin-Token. The
      // transport injects it ONLY on the loopback-admin branch — never on the
      // Caddy/analyst branch — so the analyst path is unaffected.
      injectAdminToken: true,
      // Mirror DashboardBody's useDashboardBundle request body EXACTLY.
      // range_token + anchor drive the scan window (the keyed path ignores
      // start_time/end_time, so they are omitted). fields omitted (none passed).
      body: {
        filters: {},
        chart_metric: DASHBOARD_SSR_DEFAULTS.metric,
        chart_interval: DASHBOARD_SSR_DEFAULTS.interval,
        sections: DASHBOARD_SSR_SECTIONS,
        range_token: rangeToken,
        anchor,
      },
      // Mirror the client openapi-fetch middleware, which stamps x-service-id
      // (lib/api.ts). build_request_context reads x-service-id (backend/deps.py).
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/dashboard',
  )

  if (data == null) return null
  return { data, rangeToken, anchor }
}
