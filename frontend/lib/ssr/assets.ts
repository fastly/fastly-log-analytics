import {
  fetchSsrRangePost,
  resolveDefaultSsrRangeKey,
  type SsrRangeSeed,
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
  return fetchSsrRangePost({
    serviceId,
    path: '/api/assets/aggregates',
    logPrefix: 'ssr/assets',
    now,
    logExtents,
  })
}
