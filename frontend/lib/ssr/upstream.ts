import { cookies, headers } from 'next/headers'
import { QueryClient, dehydrate } from '@tanstack/react-query'
import type { DehydratedState, QueryKey } from '@tanstack/react-query'
import { historyHoursFromExtents, pickInsightsDefault, quantizeAnchor } from "@/lib/utils";
import type { Schema } from '@/types/api'
import type { components } from '@/types/api.generated'

import { classifySsrRequest, parseSsrJson, registerSsrHeaders, ssrUpstreamGet } from '@/lib/api'

registerSsrHeaders({ cookies, headers })

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

interface InsightsSsrSeed {
  /** The /api/insights response body to seed into the cache, or null. */
  data: unknown
  /** Window token (string, e.g. '1') — must equal the client first-paint key element. */
  windowHours: string
  /** Baseline token (string, e.g. '168') — must equal the client first-paint key element. */
  baselineHours: string
}

/**
 * Resolve the default insights window/baseline tokens the same way the client
 * does on first paint. Exported so the unit test can pin the key-match without
 * a network round-trip.
 */
export function resolveInsightsDefault(
  earliestLogAt: string | null | undefined,
): { windowHours: string; baselineHours: string } {
  const picked = pickInsightsDefault(historyHoursFromExtents(earliestLogAt))
  return { windowHours: picked.window, baselineHours: picked.baseline }
}

/**
 * Pull `earliest_log_at` out of the bootstrap `log_extents` blob (typed as an
 * open record in the generated schema). Returns undefined when absent →
 * pickInsightsDefault falls back to STATIC_DEFAULT, matching the client.
 */
type LogExtentsLike = Pick<Schema<'LogExtentsResponse'>, 'earliest_log_at'>

function earliestFromLogExtents(
  logExtents: LogExtentsLike | null | undefined,
): string | null | undefined {
  return typeof logExtents?.earliest_log_at === 'string'
    ? logExtents.earliest_log_at
    : undefined
}

/**
 * SSR-prefetch the default insights selection for the active service.
 *
 * @param serviceId    Active service id (from the SSR'd bootstrap). Without it
 *                     the backend's context resolution 400s — bail to client.
 * @param logExtents   The bootstrap `log_extents` blob, used to compute the
 *                     adaptive default window/baseline (server-reproducible).
 */
export async function fetchInsightsServerSide(
  serviceId: string | undefined,
  logExtents: LogExtentsLike | null | undefined,
  initialWindow?: string | null,
  initialBaseline?: string | null,
): Promise<InsightsSsrSeed | null> {
  const defaults = resolveInsightsDefault(
    earliestFromLogExtents(logExtents),
  )
  const windowHours = initialWindow || defaults.windowHours
  const baselineHours = initialBaseline || defaults.baselineHours

  // Without a service id the backend 400s — return the tokens so the page can
  // still build the (unseeded) client key path, but no data to seed.
  if (!serviceId) return null

  const data = parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/insights',
      logPrefix: 'ssr/insights',
      method: 'POST',
      // Mirror InsightsBody's client request body EXACTLY (app/insights/page.tsx):
      // parseFloat(windowHours) / parseFloat(baselineHours), empty filters.
      body: {
        window_size_hrs: parseFloat(windowHours),
        baseline_hours: parseFloat(baselineHours),
        filters: {},
      },
      // Mirror the client openapi-fetch middleware, which stamps x-service-id
      // (lib/api.ts). build_request_context reads x-service-id (backend/core/request_context.py).
      extraHeaders: { 'x-service-id': serviceId },
    }),
    'ssr/insights',
  )

  if (data == null) return null
  return { data, windowHours, baselineHours }
}

// ── Bootstrap SSR ─────────────────────────────────────────────────────────────

type BootstrapResponse = components['schemas']['BootstrapResponse']

const _ADMIN_SSR_TTL_MS = 2000
const _adminInflightMap = new Map<string, Promise<BootstrapResponse | null>>()
const _adminCachedMap = new Map<string, { at: number; value: BootstrapResponse | null }>()

function _fetchBootstrapUpstream(serviceId?: string | null): Promise<BootstrapResponse | null> {
  const path = serviceId ? `/api/bootstrap?service_id=${encodeURIComponent(serviceId)}` : '/api/bootstrap'
  return ssrUpstreamGet({ path, logPrefix: 'ssr/bootstrap' }).then((resp) =>
    parseSsrJson<BootstrapResponse>(resp, 'ssr/bootstrap'),
  )
}

export async function fetchBootstrapServerSide(): Promise<BootstrapResponse | null> {
  const [hdrs, cookieJar] = await Promise.all([headers(), cookies()])
  const { admin: isAdmin } = classifySsrRequest(hdrs)
  const serviceId = hdrs.get('x-service-id')

  if (!isAdmin || cookieJar.get('analyst_session_id')) {
    return _fetchBootstrapUpstream(serviceId)
  }

  const cacheKey = serviceId || '__default__'
  const now = Date.now()
  const cached = _adminCachedMap.get(cacheKey)
  if (cached && now - cached.at < _ADMIN_SSR_TTL_MS) {
    return cached.value
  }
  const inflight = _adminInflightMap.get(cacheKey)
  if (inflight) return inflight

  const promise = (async () => {
    try {
      const value = await _fetchBootstrapUpstream(serviceId)
      if (!value?.settings?.is_remote_analyst) {
        _adminCachedMap.set(cacheKey, { at: Date.now(), value })
      }
      return value
    } finally {
      _adminInflightMap.delete(cacheKey)
    }
  })()
  _adminInflightMap.set(cacheKey, promise)
  return promise
}

// ── Seed Helpers ──────────────────────────────────────────────────────────────

export function seedDehydratedState(key: QueryKey, data: unknown): DehydratedState | null {
  if (data == null) return null
  const qc = new QueryClient()
  qc.setQueryData(key, data)
  return dehydrate(qc)
}

export const firstParam = (v: string | string[] | undefined): string | undefined =>
  Array.isArray(v) ? v[0] : v

export function resolveServerServiceId(
  params: { service?: string | string[] },
  bootstrap:
    | {
        active_service_id?: string | null
        services?: ReadonlyArray<{ id?: unknown; service_id?: unknown }> | null
      }
    | null
    | undefined,
  options?: { fallbackFirstService?: boolean },
): string | undefined {
  const urlServiceId = firstParam(params.service)
  if (urlServiceId) return urlServiceId
  if (bootstrap?.active_service_id) return bootstrap.active_service_id
  if (options?.fallbackFirstService) {
    const first = bootstrap?.services?.[0]
    const firstId = first?.id ?? first?.service_id
    if (typeof firstId === 'string') return firstId
  }
  return undefined
}

export async function resolvePageServiceId(
  params: { service?: string | string[] },
  options?: { fallbackFirstService?: boolean },
): Promise<string | undefined> {
  const urlServiceId = firstParam(params.service)
  if (urlServiceId) return urlServiceId
  const bootstrap = await fetchBootstrapServerSide()
  return resolveServerServiceId(params, bootstrap, options)
}

// ── Admin Gateway SSR Fetchers ────────────────────────────────────────────────

export async function fetchMetricHistoryBatchServerSide(): Promise<unknown | null> {
  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/admin/metric-history/batch?since=1h',
      logPrefix: 'ssr/admin-trends',
    }),
    'ssr/admin-trends',
  )
}

type AlertListResponse = components['schemas']['AlertListResponse']

export async function fetchAlertsServerSide(
  serviceId: string | undefined,
): Promise<AlertListResponse | null> {
  const path = serviceId
    ? `/api/alerts/${encodeURIComponent(serviceId)}`
    : '/api/alerts/'

  return parseSsrJson<AlertListResponse>(
    await ssrUpstreamGet({ path, logPrefix: 'ssr/alerts' }),
    'ssr/alerts',
  )
}

export async function fetchCronRunsServerSide(
  serviceId: string | undefined,
): Promise<unknown | null> {
  if (!serviceId) return null

  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/cron-runs?page=1&per_page=500',
      logPrefix: 'ssr/logs',
      extraHeaders: { 'X-Fastly-Service-Id': serviceId },
    }),
    'ssr/logs',
  )
}

export async function fetchCronScheduleServerSide(
  serviceId: string | undefined,
): Promise<unknown | null> {
  if (!serviceId) return null

  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/cron-schedule',
      logPrefix: 'ssr/logs-schedule',
      extraHeaders: { 'X-Fastly-Service-Id': serviceId },
    }),
    'ssr/logs-schedule',
  )
}

type TosPayload = components['schemas']['TosDocument']
type TosResult = TosPayload | 'unauthenticated' | null

export async function fetchTosServerSide(): Promise<TosResult> {
  const resp = await ssrUpstreamGet({ path: '/api/share/tos', logPrefix: 'ssr/tos' })
  if (resp?.statusCode === 401) return 'unauthenticated'
  return parseSsrJson<TosPayload>(resp, 'ssr/tos')
}

interface UsageLogSsrArgs {
  serviceId: string | undefined
  start: string
  end: string
  pageSize: number
  usageType?: string
  processContext?: string
  operationType?: string
}

export async function fetchUsageLogServerSide(args: UsageLogSsrArgs): Promise<unknown | null> {
  const query = new URLSearchParams({
    service_id: args.serviceId || '',
    start: args.start,
    end: args.end,
    page_size: String(args.pageSize),
  })
  if (args.usageType) query.set('usage_type', args.usageType)
  if (args.processContext) query.set('process_context', args.processContext)
  if (args.operationType) query.set('operation_type', args.operationType)

  return parseSsrJson(
    await ssrUpstreamGet({
      path: `/api/admin/usage-log?${query.toString()}`,
      logPrefix: 'ssr/usage_log',
    }),
    'ssr/usage_log',
  )
}
