import { quantizeAnchor } from '@/lib/time-window'
import { parseSsrJson, ssrUpstreamGet } from './_transport'

export interface AssetsSsrSeed {
  /** The /api/assets/aggregates response body to seed into the cache. */
  data: unknown
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

/**
 * Resolve the default (rangeToken, anchor) pair the client computes on first paint.
 */
export function resolveAssetsDefaultKey(
  now: Date = new Date(),
  _logExtents?: unknown,
): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

/**
 * SSR-prefetch the default assets aggregates selection for the active service.
 */
export async function fetchAssetsServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
  logExtents?: unknown,
): Promise<AssetsSsrSeed | null> {
  const { rangeToken, anchor } = resolveAssetsDefaultKey(now, logExtents)

  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/assets/aggregates',
      logPrefix: 'ssr/assets',
      method: 'POST',
      injectAdminToken: true,
      body: {
        filters: {},
        range_token: rangeToken,
        anchor: anchor,
      },
    }),
    'ssr/assets',
  )

  if (!data) return null

  return {
    data,
    rangeToken,
    anchor,
  }
}
