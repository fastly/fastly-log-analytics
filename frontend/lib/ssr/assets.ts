import {
  parseSsrJson,
  resolveDefaultSsrRangeKey,
  type SsrRangeSeed,
  ssrUpstreamGet,
} from './upstream'

export type AssetsSsrSeed = SsrRangeSeed
export const resolveAssetsDefaultKey = resolveDefaultSsrRangeKey

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
      extraHeaders: { 'x-service-id': serviceId },
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
