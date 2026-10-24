import type { components } from '@/types/api.generated'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

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
