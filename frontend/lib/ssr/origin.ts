// Server-only SSR fetcher for the /origin page. Pre-fetches the default
// /api/origin/aggregates selection so the origin cards render from cache on
// first paint instead of paying the client round-trip + skeleton flash on cold
// load. Mirrors lib/ssr/insights.ts — the second POST caller of the shared
// transport (lib/ssr/_transport.ts), inheriting its Caddy-marker trust gate
// and analyst-clamp / X-Remote-Analyst promotion unchanged.
//
// WHY origin is SSR-seedable now (it wasn't before): the origin first-paint
// React Query key used to carry absolute start/end timestamps anchored to the
// client's `new Date()` at paint — server-unreproducible, so a seed would miss
// and double-fetch. We ported the network `range_token`/`anchor` relative-range
// wire contract to /api/origin/aggregates: the SERVER resolves the scan window
// from (token, quantized anchor) and the client keys its first-paint query on
// the SAME (rangeToken, anchor) pair instead of absolute timestamps. Both are
// server-reproducible, so the seed key byte-matches the client key.
//
// KEY-MATCH CONTRACT (load-bearing — a mismatch causes a double-fetch that is
// strictly worse than no SSR): the (rangeToken, anchor) this helper seeds MUST
// equal what OriginClient computes on first paint. Both sides:
//   - default rangeToken to "24h" (no ?range= on a cold load; the client's
//     filterStore.relativeRange is null at first paint — "24h" matches the 24h
//     store-default display window so the scan window and the chart x-axis agree), and
//   - quantize the anchor to the 60s grid via lib/time-window.ts (the byte-for-
//     byte FE port of backend/utils/time_window.py).
// The only divergence is the anchor instant: server-render vs client-paint are
// sub-second apart and almost always floor to the same 60s quantum. On the rare
// load that straddles a minute boundary the client anchor differs by one
// quantum → the seed misses → the client refetches (today's behavior). That
// degrades to the status-quo cold fetch for that one load; it never leaks and
// never crashes — identical risk class to insights' band-boundary race.
//
// anchor = quantizeAnchor(now): it no longer snaps to bootstrap.log_extents (the
// stale-latest-log snap and lib/log-extents-snap.ts were removed), so the only
// SSR/client divergence is the minute-boundary race above.
//
// The body mirrors OriginReportContent's client POST EXACTLY (the section list,
// limits, bucket, metric/percentile defaults) so the backend produces the same
// response shape the client would request. range_token/anchor drive the scan
// window; start_time/end_time are omitted (the keyed path ignores them).

import {
  fetchSsrRangePost,
  type SsrRangeSeed,
} from './upstream'

// Default first-paint section set — mirrors ORIGIN_SECTIONS in
// app/origin/_sections/OriginClient.tsx (the page renders every section). Kept
// in lockstep with the client constant; a divergence here would change the
// request body shape and the key.
export const ORIGIN_SSR_SECTIONS = [
  'summary',
  'timeseries',
  'slow_urls',
  'status_codes',
  'path_breakdown',
  'pop_latency',
  'ip_health',
] as const

// Default first-paint selection — mirrors the client's defaults
// (OriginClient.tsx) and, crucially, the first-paint value of every key
// element so the seed key byte-matches.
//
// bucketMinutes=60 is load-bearing for the key-match: useReportConfig derives
// config.effectiveInterval from the displayed window span, and the cold-load
// default window is exactly 24h (filterStore inits to [subDays(now,1), now]).
// At a 24h span useReportConfig DETERMINISTICALLY picks "1 hour" (spanHours>=24
// → '1 hour'; the '1 minute'/'1 second' branches require <24h), which the
// origin page's intervalMap maps to 60 bucket-minutes — independent of the
// exact `now`. The ReportLayout-supplied defaultInterval="1 minute" only seeds
// chartInterval's useState; the config useMemo overrides it for the 24h span.
// So 60 is the first-paint bucketMinutes regardless of wall-clock.
export const ORIGIN_SSR_DEFAULTS = {
  bucketMinutes: 60,
  metric: 'ttfb' as const,
  percentile: 'p95' as const,
  slowUrlsLimit: 20,
  slowUrlsMinRequests: 50,
  popLatencyLimit: 30,
  ipHealthLimit: 30,
} as const

/**
 * SSR-prefetch the default origin aggregates selection for the active service.
 *
 * @param serviceId   Active service id (from the SSR'd bootstrap). Without it the
 *                    backend's context resolution 400s — bail to client fetch.
 * @param now         Render instant; floored to the anchor quantum. Injectable
 *                    for tests.
 */
export async function fetchOriginServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/origin/aggregates',
    logPrefix: 'ssr/origin',
    now,
    body: {
      bucket_minutes: ORIGIN_SSR_DEFAULTS.bucketMinutes,
      split_by_leg: false,
      timeseries_metric: ORIGIN_SSR_DEFAULTS.metric,
      timeseries_percentile: ORIGIN_SSR_DEFAULTS.percentile,
      slow_urls_limit: ORIGIN_SSR_DEFAULTS.slowUrlsLimit,
      slow_urls_min_requests: ORIGIN_SSR_DEFAULTS.slowUrlsMinRequests,
      pop_latency_limit: ORIGIN_SSR_DEFAULTS.popLatencyLimit,
      ip_health_limit: ORIGIN_SSR_DEFAULTS.ipHealthLimit,
      sections: ORIGIN_SSR_SECTIONS,
    },
  })
}
