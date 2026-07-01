// Server-only SSR fetcher for the /performance page. Pre-fetches BOTH default
// /api/performance/aggregates selections (the "core" and "distributions"
// section groups the page fires on cold load) so the cards render from cache on
// first paint instead of paying two client round-trips + skeleton flashes.
// Replicates the /origin SSR template (commit 319c1b0) — POST callers of the
// shared transport, inheriting its Caddy-marker trust gate + analyst clamp.
//
// WHY two fetches: the performance page makes TWO useServiceQuery calls
// (coreQuery + distributionsQuery) with DIFFERENT section lists, so it has two
// first-paint keys. We seed both; each is byte-matched independently.
//
// KEY-MATCH CONTRACT (load-bearing): the (rangeToken, anchor) + sort this helper
// seeds MUST equal what PerformancePage computes on first paint:
//   ['performance','aggregates','core', sid, rangeToken, anchor, filterPayload, 'p99']
//   ['performance','aggregates','distributions', sid, rangeToken, anchor, filterPayload, 'p99']
// On a cold load: rangeToken='24h' (matches the 24h store-default display window
// so the scan window and the chart x-axis range agree; the old 'auto' resolved to
// 30d for mature services and squashed the bars), anchor=quantizeAnchor(now) on the 60s grid,
// filterPayload={}, sort='p99' (the literal default in both query keys). The
// performance page has NO bucket element in its keys (the request carries no
// bucket field). Only divergence is the rare minute-boundary anchor straddle →
// one client refetch, never a leak/crash.

import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

// Mirror PERFORMANCE_CORE_SECTIONS / PERFORMANCE_DISTRIBUTIONS_SECTIONS in
// app/performance/page.tsx. Kept in lockstep; a divergence changes the body
// shape (these are part of each POST body, not the key).
export const PERFORMANCE_CORE_SSR_SECTIONS = ['waterfall', 'scatter', 'top_urls', 'top_asns'] as const
export const PERFORMANCE_DISTRIBUTIONS_SSR_SECTIONS = ['ttl_dist'] as const

// 'p99' is the literal sort_by default in BOTH performance query keys (the page
// hard-codes 'p99'); it is a key element, so it must match the seed.
export const PERFORMANCE_SSR_DEFAULTS = {
  sortBy: 'p99' as const,
} as const

export interface PerformanceSsrSeed {
  /** /api/performance/aggregates response for the "core" section group. */
  coreData: unknown
  /** /api/performance/aggregates response for the "distributions" group. */
  distributionsData: unknown
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

export function resolvePerformanceDefaultKey(now: Date = new Date()): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

async function fetchSection(
  serviceId: string,
  rangeToken: string,
  anchor: string,
  sections: readonly string[],
): Promise<unknown> {
  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/performance/aggregates',
      logPrefix: 'ssr/performance',
      method: 'POST',
      injectAdminToken: true,
      // Mirror the client request body EXACTLY. range_token + anchor drive the
      // scan window (the keyed path ignores start_time/end_time, so omitted).
      body: {
        filters: {},
        sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
        sections,
        range_token: rangeToken,
        anchor,
      },
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/performance',
  )
}

/**
 * SSR-prefetch BOTH default performance selections for the active service.
 * Returns null when serviceId is absent OR either sub-fetch fails — the page
 * then degrades to the client-fetch path for both cards (no partial seed, which
 * keeps the failure mode simple: all-or-nothing, identical to a cold load).
 *
 * @param now  Render instant; floored to the anchor quantum. Injectable.
 */
export async function fetchPerformanceServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<PerformanceSsrSeed | null> {
  const { rangeToken, anchor } = resolvePerformanceDefaultKey(now)

  if (!serviceId) return null

  const [coreData, distributionsData] = await Promise.all([
    fetchSection(serviceId, rangeToken, anchor, PERFORMANCE_CORE_SSR_SECTIONS),
    fetchSection(serviceId, rangeToken, anchor, PERFORMANCE_DISTRIBUTIONS_SSR_SECTIONS),
  ])

  // All-or-nothing: if either sub-fetch failed (fail-closed → null), skip the
  // whole seed so the page client-fetches both cleanly rather than seeding one
  // key and double-fetching the other.
  if (coreData == null || distributionsData == null) return null
  return { coreData, distributionsData, rangeToken, anchor }
}
