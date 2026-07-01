// Server-only SSR fetcher for the /security page. Pre-fetches the default
// /api/security/aggregates selection so the bots/header/network cards render
// from cache on first paint instead of paying the client round-trip + skeleton
// flash on cold load. Mirrors lib/ssr/origin.ts (the proven template) — a POST
// caller of the shared transport (lib/ssr/_transport.ts), inheriting its
// Caddy-marker trust gate + analyst-clamp / X-Remote-Analyst promotion.
//
// WHY security is SSR-seedable: the first-paint key used to carry absolute
// start/end timestamps anchored to the client's `new Date()` — unreproducible.
// We ported the range_token/anchor relative-range wire contract to
// /api/security/aggregates and re-keyed the client first-paint query on
// (rangeToken, anchor). Both are server-reproducible, so the seed key
// byte-matches the client key.
//
// KEY-MATCH CONTRACT (load-bearing — a mismatch double-fetches, worse than no
// SSR): the (rangeToken, anchor) + bucketSeconds this helper seeds MUST equal
// what SecurityClient computes on first paint:
//   ['security','aggregates', sid, rangeToken, anchor, filterPayload, bucketSeconds]
// On a cold load: rangeToken='24h' (no ?range= → filterStore.relativeRange null;
// matches the 24h store-default display window so the scan window and the chart
// x-axis range agree — the old 'auto' resolved to 30d for mature services and
// squashed the bars), anchor=quantizeAnchor(now) on the 60s grid, filterPayload={},
// bucketSeconds=3600 (default 24h span → "1 hour" → INTERVAL_SECONDS['1 hour']).
// The only divergence is the rare minute-boundary anchor straddle → one client
// refetch, never a leak/crash (identical risk class to origin/insights).

import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

// Default first-paint section set — mirrors SECURITY_SECTIONS in
// app/security/_sections/SecurityClient.tsx (BOTS ∪ HEADER_ANOMALIES ∪
// NETWORK). Kept in lockstep; a divergence changes the body shape and the key.
export const SECURITY_SSR_SECTIONS = [
  'ngwaf_verified_bots',
  'ngwaf_verified_bots_ts',
  'wellknown_bots',
  'tls_fingerprints',
  'fingerprint_coverage',
  'req_size_dist',
  'top_ips_header',
  'ipv6_adoption',
  'proxy_dist',
  'conn_reuse_dist',
] as const

// bucketSeconds=3600 is load-bearing for the key-match: ReportLayout derives it
// from config.effectiveInterval, and the cold-load default window is exactly 24h
// (filterStore inits to [subDays(now,1), now]). At a 24h span useReportConfig
// DETERMINISTICALLY picks "1 hour" → INTERVAL_SECONDS['1 hour'] = 3600,
// independent of the exact `now`. Security passes no defaultInterval, so the
// ReportLayout default ("1 hour") is also the auto-detected interval at 24h.
export const SECURITY_SSR_DEFAULTS = {
  bucketSeconds: 3600,
} as const

export interface SecuritySsrSeed {
  /** The /api/security/aggregates response body to seed into the cache. */
  data: unknown
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

/**
 * Resolve the default (rangeToken, anchor) pair the client computes on first
 * paint. Exported so the unit test can pin the key-match without a network
 * round-trip. `now` injectable for deterministic tests.
 */
export function resolveSecurityDefaultKey(now: Date = new Date()): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

/**
 * SSR-prefetch the default security aggregates selection for the active service.
 *
 * @param serviceId  Active service id (from the SSR'd bootstrap). Without it the
 *                   backend's context resolution 400s — bail to client fetch.
 * @param now        Render instant; floored to the anchor quantum. Injectable.
 */
export async function fetchSecurityServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SecuritySsrSeed | null> {
  const { rangeToken, anchor } = resolveSecurityDefaultKey(now)

  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/security/aggregates',
      logPrefix: 'ssr/security',
      method: 'POST',
      injectAdminToken: true,
      // Mirror SecurityBody's client request body EXACTLY. range_token + anchor
      // drive the scan window (the keyed path ignores start_time/end_time, so
      // they are omitted).
      body: {
        filters: {},
        bucket_seconds: SECURITY_SSR_DEFAULTS.bucketSeconds,
        sections: SECURITY_SSR_SECTIONS,
        range_token: rangeToken,
        anchor,
      },
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/security',
  )

  if (data == null) return null
  return { data, rangeToken, anchor }
}
