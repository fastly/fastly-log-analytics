// Server-only SSR fetcher for the /streaming page. Pre-fetches the default
// /api/cmcd/aggregates selection so the CMCD streaming QoE cards render from
// cache on first paint instead of paying the client round-trip + skeleton flash
// on cold load. Mirrors lib/ssr/security.ts and lib/ssr/origin.ts.

import {
  fetchSsrRangePost,
  type SsrRangeSeed,
} from './upstream'

export const STREAMING_SSR_DEFAULTS = {
  bucketSeconds: 300,
  topN: 30,
} as const

export type StreamingSsrSeed = SsrRangeSeed

export async function fetchStreamingServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<StreamingSsrSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/cmcd/aggregates',
    logPrefix: 'ssr/streaming',
    now,
    timeoutMs: 8000,
    body: {
      bucket_seconds: STREAMING_SSR_DEFAULTS.bucketSeconds,
      top_n: STREAMING_SSR_DEFAULTS.topN,
    },
  })
}
