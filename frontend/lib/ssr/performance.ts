// Server-only SSR fetcher for the /performance page. Pre-fetches the unified
// /api/performance/aggregates selection so the cards render from cache on
// first paint instead of paying client round-trips + skeleton flashes.
// Replicates the /origin SSR template (commit 319c1b0) — POST callers of the
// shared transport, inheriting its Caddy-marker trust gate + analyst clamp.

import {
  fetchSsrRangePost,
  type SsrRangeSeed,
} from './upstream'

// 'p99' is the literal sort_by default in the performance query key (the page
// hard-codes 'p99'); it is a key element, so it must match the seed.
export const PERFORMANCE_SSR_DEFAULTS = {
  sortBy: 'p99' as const,
} as const

/**
 * SSR-prefetch unified performance selection for the active service.
 * Returns null when serviceId is absent OR the fetch fails — the page
 * then degrades to the client-fetch path (no partial seed).
 *
 * @param now         Render instant; floored to the anchor quantum. Injectable.
 */
export async function fetchPerformanceServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/performance/aggregates',
    logPrefix: 'ssr/performance',
    now,
    body: {
      sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
      sections: undefined,
    },
  })
}
