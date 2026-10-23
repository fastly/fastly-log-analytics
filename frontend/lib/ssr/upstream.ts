import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export interface SsrRangeSeed<T = unknown> {
  /** The upstream aggregates response body to seed into the cache. */
  data: T
  /** Relative-range token (e.g. '24h') — must equal the client key element. */
  rangeToken: string
  /** Quantized anchor (ISO-Z, 60s grid) — must equal the client key element. */
  anchor: string
}

/**
 * Resolve the default (rangeToken, anchor) pair the client computes on first paint.
 * Cold load defaults rangeToken to '24h' and floors the anchor to the 60s quantum.
 */
export function resolveDefaultSsrRangeKey(
  now: Date = new Date(),
): {
  rangeToken: string
  anchor: string
} {
  return { rangeToken: '24h', anchor: quantizeAnchor(now.toISOString(), now) }
}

export async function fetchSsrRangePost(options: {
  serviceId: string | undefined
  path: string
  logPrefix: string
  now?: Date
  timeoutMs?: number
  body?: Record<string, unknown>
}): Promise<SsrRangeSeed | null> {
  const { rangeToken, anchor } = resolveDefaultSsrRangeKey(options.now)
  if (!options.serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: options.path,
      logPrefix: options.logPrefix,
      method: 'POST',
      ...(options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : {}),
      body: {
        filters: {},
        ...options.body,
        range_token: rangeToken,
        anchor,
      },
      extraHeaders: { 'x-service-id': options.serviceId },
    }),
    options.logPrefix,
  )

  if (data == null) return null
  return { data, rangeToken, anchor }
}
