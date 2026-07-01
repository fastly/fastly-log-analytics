// Server-only SSR fetcher for the /insights page. Pre-fetches the default
// /api/insights selection so the anomaly cards render from cache on first
// paint instead of paying the client round-trip + skeleton flash on cold
// load. insights is now-INDEPENDENT and filter-independent in its query key
// (['insights', sid, windowHours, baselineHours] with filters:{}), which is
// exactly why it is SSR-seedable while the absolute-timestamped aggregates
// pages are not (their first-paint key depends on a client-side now()-anchored
// extents snap that the server cannot reproduce).
//
// Trust topology + Caddy-marker fail-closed defense live in ./_transport.ts
// (shared with bootstrap/alerts/logs/usage_log/tos). This is the FIRST POST
// caller of the shared transport; the trust gate is method-agnostic, so the
// analyst-clamp / X-Remote-Analyst promotion applies identically to the POST.
//
// KEY-MATCH CONTRACT (load-bearing — a mismatch causes a double-fetch that is
// strictly worse than no SSR): the windowHours/baselineHours this helper seeds
// MUST equal what the client's useInsightsDefaults computes on first paint.
// Both sides call the SAME pure functions (historyHoursFromExtents +
// pickInsightsDefault, lib/insights-defaults.ts) over the SAME earliest_log_at
// (the value bootstrap ships and useBootstrap seeds into ['log-extents', sid]),
// so the only divergence is the now() anchor (server-render vs client-paint,
// seconds apart). On the rare service whose history sits within seconds of a
// pickInsightsDefault band boundary (1h/4h/24h/48h/168h/720h since first log),
// the two anchors can pick different buckets → the client key won't match the
// seed → the client simply refetches (today's behavior). That degrades to the
// status-quo cold fetch for that one service; it never leaks and never crashes.

import {
  historyHoursFromExtents,
  pickInsightsDefault,
} from '@/lib/insights-defaults'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export interface InsightsSsrSeed {
  /** The /api/insights response body to seed into the cache, or null. */
  data: unknown
  /** Window token (string, e.g. '1') — must equal the client first-paint key element. */
  windowHours: string
  /** Baseline token (string, e.g. '168') — must equal the client first-paint key element. */
  baselineHours: string
}

/**
 * Resolve the default insights window/baseline tokens the same way the client
 * does on first paint. Exported so the unit test can pin the key-match without
 * a network round-trip.
 */
export function resolveInsightsDefault(
  earliestLogAt: string | null | undefined,
): { windowHours: string; baselineHours: string } {
  const picked = pickInsightsDefault(historyHoursFromExtents(earliestLogAt))
  return { windowHours: picked.window, baselineHours: picked.baseline }
}

/**
 * Pull `earliest_log_at` out of the bootstrap `log_extents` blob (typed as an
 * open record in the generated schema). Returns undefined when absent →
 * pickInsightsDefault falls back to STATIC_DEFAULT, matching the client.
 */
function earliestFromLogExtents(logExtents: unknown): string | null | undefined {
  if (!logExtents || typeof logExtents !== 'object') return undefined
  const v = (logExtents as Record<string, unknown>).earliest_log_at
  return typeof v === 'string' ? v : undefined
}

/**
 * SSR-prefetch the default insights selection for the active service.
 *
 * @param serviceId    Active service id (from the SSR'd bootstrap). Without it
 *                     the backend's context resolution 400s — bail to client.
 * @param logExtents   The bootstrap `log_extents` blob, used to compute the
 *                     adaptive default window/baseline (server-reproducible).
 */
export async function fetchInsightsServerSide(
  serviceId: string | undefined,
  logExtents: unknown,
): Promise<InsightsSsrSeed | null> {
  const { windowHours, baselineHours } = resolveInsightsDefault(
    earliestFromLogExtents(logExtents),
  )

  // Without a service id the backend 400s — return the tokens so the page can
  // still build the (unseeded) client key path, but no data to seed.
  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/insights',
      logPrefix: 'ssr/insights',
      method: 'POST',
      // Admin SSH-tunnel branch: when ADMIN_SHARED_SECRET is set the backend's
      // admin-token gate 401s a loopback request without X-Admin-Token, so the
      // SSR fetch would fall back to a cold client fetch (the v2.0.0 admin-SSR
      // regression). The transport injects the token ONLY on the loopback-admin
      // branch — never on the Caddy/analyst branch — so the analyst path is
      // unaffected and no admin secret can reach an analyst-classified request.
      injectAdminToken: true,
      // Mirror InsightsBody's client request body EXACTLY (app/insights/page.tsx):
      // parseFloat(windowHours) / parseFloat(baselineHours), empty filters.
      body: {
        window_size_hrs: parseFloat(windowHours),
        baseline_hours: parseFloat(baselineHours),
        filters: {},
      },
      // Mirror the client openapi-fetch middleware, which stamps x-service-id
      // (lib/api.ts). build_request_context reads x-service-id (backend/deps.py).
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/insights',
  )

  if (data == null) return null
  return { data, windowHours, baselineHours }
}
