// Server-only SSR fetcher for the /performance page. Pre-fetches the unified
// /api/performance/aggregates selection so the cards render from cache on
// first paint instead of paying client round-trips + skeleton flashes.
// Replicates the /origin SSR template (commit 319c1b0) — POST callers of the
// shared transport, inheriting its Caddy-marker trust gate + analyst clamp.

import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

// 'p99' is the literal sort_by default in the performance query key (the page
// hard-codes 'p99'); it is a key element, so it must match the seed.
export const PERFORMANCE_SSR_DEFAULTS = {
  sortBy: 'p99' as const,
} as const

export interface PerformanceSsrSeed {
  /** /api/performance/aggregates response for all sections. */
  performanceData: unknown
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

export function resolvePerformanceDefaultKey(
  now: Date = new Date(),
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  _logExtents?: unknown,
): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

async function fetchPerformanceData(
  serviceId: string,
  rangeToken: string,
  anchor: string,
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
        // Omit sections to fetch all sections in a single request
        sections: undefined,
        range_token: rangeToken,
        anchor,
      },
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/performance',
  )
}

/**
 * SSR-prefetch unified performance selection for the active service.
 * Returns null when serviceId is absent OR the fetch fails — the page
 * then degrades to the client-fetch path (no partial seed).
 *
 * @param now         Render instant; floored to the anchor quantum. Injectable.
 * @param logExtents  bootstrap.log_extents for serviceId, if available. See
 *                    resolvePerformanceDefaultKey.
 */
export async function fetchPerformanceServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
  logExtents?: unknown,
): Promise<PerformanceSsrSeed | null> {
  const { rangeToken, anchor } = resolvePerformanceDefaultKey(now, logExtents)

  if (!serviceId) return null

  const performanceData = await fetchPerformanceData(serviceId, rangeToken, anchor)

  if (performanceData == null) return null
  return { performanceData, rangeToken, anchor }
}
