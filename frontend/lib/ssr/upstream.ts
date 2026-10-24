import type { Schema } from '@/types/api'
import { quantizeAnchor } from '@/lib/time-window'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export function shouldLoadRumScript({
  host,
  proxiedByCaddy,
  rumEnabled,
}: {
  host: string | null
  proxiedByCaddy: boolean
  rumEnabled: boolean
}): boolean {
  if (!rumEnabled || !proxiedByCaddy || !host) {
    return false
  }

  const normalizedHost = host.toLowerCase()
  const hostname = normalizedHost.startsWith('[')
    ? normalizedHost.slice(1, normalizedHost.indexOf(']'))
    : normalizedHost.split(':')[0]
  return (
    hostname !== 'localhost' &&
    hostname !== '127.0.0.1' &&
    hostname !== '::1' &&
    hostname !== '[::1]'
  )
}

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

export const PERFORMANCE_SSR_DEFAULTS = {
  sortBy: 'p99' as const,
} as const

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

type RumStatusResponse = Schema<'RumStatusResponse'>

export async function fetchRumStatusServerSide(
  serviceId: string | undefined,
): Promise<RumStatusResponse | null> {
  if (!serviceId) return null

  const path = `/api/services/${encodeURIComponent(serviceId)}/rum/status`

  return parseSsrJson<RumStatusResponse>(
    await ssrUpstreamGet({ path, logPrefix: 'ssr/rum-status' }),
    'ssr/rum-status',
  )
}

const STREAMING_SSR_DEFAULTS = {
  bucketSeconds: 300,
  topN: 30,
} as const

export async function fetchStreamingServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
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

export const SECURITY_SSR_SECTIONS = [
  'ngwaf_verified_bots',
  'ngwaf_verified_bots_ts',
  'wellknown_bots',
  'bot_efficiency',
  'tls_fingerprints',
  'fingerprint_coverage',
  'req_size_dist',
  'top_ips_header',
  'ipv6_adoption',
  'proxy_dist',
  'conn_reuse_dist',
  'proxies',
] as const

export const SECURITY_SSR_DEFAULTS = {
  bucketSeconds: 3600,
} as const

export async function fetchSecurityServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/security/aggregates',
    logPrefix: 'ssr/security',
    now,
    timeoutMs: 8000,
    body: {
      bucket_seconds: SECURITY_SSR_DEFAULTS.bucketSeconds,
      sections: SECURITY_SSR_SECTIONS,
    },
  })
}

export const ORIGIN_SSR_SECTIONS = [
  'summary',
  'timeseries',
  'slow_urls',
  'status_codes',
  'path_breakdown',
  'pop_latency',
  'ip_health',
] as const

export const ORIGIN_SSR_DEFAULTS = {
  bucketMinutes: 60,
  metric: 'ttfb' as const,
  percentile: 'p95' as const,
  slowUrlsLimit: 20,
  slowUrlsMinRequests: 50,
  popLatencyLimit: 30,
  ipHealthLimit: 30,
} as const

export async function fetchOriginServerSide(
  serviceId: string | undefined,
  now: Date = new Date(),
): Promise<SsrRangeSeed | null> {
  return fetchSsrRangePost({
    serviceId,
    path: '/api/origin/aggregates',
    logPrefix: 'ssr/origin',
    now,
    body: {
      bucket_minutes: ORIGIN_SSR_DEFAULTS.bucketMinutes,
      split_by_leg: false,
      timeseries_metric: ORIGIN_SSR_DEFAULTS.metric,
      timeseries_percentile: ORIGIN_SSR_DEFAULTS.percentile,
      slow_urls_limit: ORIGIN_SSR_DEFAULTS.slowUrlsLimit,
      slow_urls_min_requests: ORIGIN_SSR_DEFAULTS.slowUrlsMinRequests,
      pop_latency_limit: ORIGIN_SSR_DEFAULTS.popLatencyLimit,
      ip_health_limit: ORIGIN_SSR_DEFAULTS.ipHealthLimit,
      sections: ORIGIN_SSR_SECTIONS,
    },
  })
}
