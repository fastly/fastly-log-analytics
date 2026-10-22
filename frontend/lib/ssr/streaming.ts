// Server-only SSR fetcher for the /streaming page. Pre-fetches the default
// /api/cmcd/aggregates selection so the CMCD streaming QoE cards render from
// cache on first paint instead of paying the client round-trip + skeleton flash
// on cold load. Mirrors lib/ssr/security.ts and lib/ssr/origin.ts.

import {
  parseSsrJson,
  resolveDefaultSsrRangeKey,
  type SsrRangeSeed,
  ssrUpstreamGet,
} from './upstream'

export const STREAMING_SSR_DEFAULTS = {
  bucketSeconds: 300,
  topN: 30,
} as const

export type StreamingSsrSeed = SsrRangeSeed

export const resolveStreamingDefaultKey = resolveDefaultSsrRangeKey

export async function fetchStreamingServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<StreamingSsrSeed | null> {
  const { rangeToken, anchor } = resolveStreamingDefaultKey(now)

  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/cmcd/aggregates',
      logPrefix: 'ssr/streaming',
      method: 'POST',
      injectAdminToken: true,
      timeoutMs: 8000,
      body: {
        filters: {},
        bucket_seconds: STREAMING_SSR_DEFAULTS.bucketSeconds,
        top_n: STREAMING_SSR_DEFAULTS.topN,
        range_token: rangeToken,
        anchor,
      },
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/streaming',
  )

  if (data == null) return null
  return { data, rangeToken, anchor }
}
