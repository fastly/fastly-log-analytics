import {
  fetchSsrRangePost,
  type SsrRangeSeed,
} from './upstream'

/**
 * SSR-prefetch the default assets aggregates selection for the active service.
 */
export async function fetchAssetsServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/assets/aggregates',
    logPrefix: 'ssr/assets',
    now,
  })
}
