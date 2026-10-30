'use client'

import {
  queryOptions,
  useQuery,
  type QueryClient,
  type QueryKey,
  type UseQueryOptions,
} from '@tanstack/react-query'
import { client, unwrapClient } from '@/lib/api'
import type { components } from '@/types/api'

export const scoringAnalyticsCompositeKey = (serviceId: string | null | undefined) =>
  ['scoring-analytics-composite', serviceId] as const

export const scoringConfigCompositeKey = (serviceId: string | null | undefined) =>
  ['scoring-config-composite', serviceId] as const

export const scoringAnalyticsCompositeOptions = (
  serviceId: string | null | undefined,
  sinceHours: number,
) =>
  queryOptions({
    queryKey: [...scoringAnalyticsCompositeKey(serviceId), sinceHours] as const,
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/scoring/analytics', {
          params: { path: { service_id: serviceId ?? '' }, query: { since_hours: sinceHours } },
          signal,
        }),
      ),
  })

export const scoringConfigCompositeOptions = (serviceId: string | null | undefined) =>
  queryOptions({
    queryKey: scoringConfigCompositeKey(serviceId),
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/scoring/config', {
          params: { path: { service_id: serviceId ?? '' } },
          signal,
        }),
      ),
  })

export const SCORING_KEY_PREFIXES = [
  'scoring-status',
  'scoring-evaluation',
  'scoring-score-dist',
  'scoring-latency-timeseries',
  'scoring-fail-open-breakdown',
  'scoring-compliance',
  'scoring-top-flagged',
  'scoring-per-reason-auc',
  'scoring-threshold-preview',
  'scoring-threshold-committed',
  'scoring-enforce-threshold',
  'scoring-enforce-status-code',
  'scoring-l2-enforce',
  'scoring-exclude-regex',
  'scoring-matrix-versions',
  'scoring-audit',
  'scoring-session-events',
  'scoring-labels',
] as const

export function invalidateServiceScoringQueries(queryClient: QueryClient, serviceId: string): void {
  if (!serviceId) return
  queryClient.invalidateQueries({ queryKey: scoringAnalyticsCompositeKey(serviceId) })
  queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
  for (const prefix of SCORING_KEY_PREFIXES) {
    queryClient.invalidateQueries({ queryKey: [prefix, serviceId] })
  }
}

export function useScoringQuery<TData>(
  queryKey: QueryKey,
  fn: () => Promise<{ data?: TData; error?: unknown; response: Response }>,
  options?: Omit<UseQueryOptions<TData, Error>, 'queryKey' | 'queryFn'>,
) {
  return useQuery<TData, Error>({
    queryKey,
    queryFn: () => unwrapClient(fn()),
    staleTime: 60_000,
    ...options,
  })
}

export type LatencyRow = components['schemas']['ScoringLatencyRow']
export type LatencyResponse = components['schemas']['ScoringLatencyTimeseriesResponse']

export const usToMs = (us: number | null | undefined) => (us == null ? null : us / 1000)

export function deriveScorerSeries(data: LatencyResponse | undefined) {
  const rows = data?.rows ?? []
  return { rows, hours: rows.map((r) => r.hour), perMinute: data?.granularity === 'minute' }
}

export function scorerHourlyLayout(perMinute: boolean, yTitle: string, showlegend: boolean) {
  return {
    showlegend,
    margin: { l: 50, r: 20, t: showlegend ? 35 : 10, b: 40 },
    xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
    yaxis: { title: yTitle, rangemode: 'tozero' },
    legend: {
      orientation: 'h' as const,
      y: 1.05,
      x: 0.5,
      xanchor: 'center' as const,
      yanchor: 'bottom' as const,
      font: { size: 9.5 },
      bgcolor: 'transparent',
    },
  }
}

export function useScorerTimeseries(serviceId: string, sinceHours: number) {
  return useScoringQuery(['scoring-latency-timeseries', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/latency-timeseries', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
    }),
  )
}

export * from './help-content'
export * from './LabelsTab'
export * from './ThresholdSlider'
export * from './MatrixVersionsCard'
export * from './ExcludeRegexCard'
export * from './TopFlaggedTable'
export * from './StoragePanels'
export * from './StatusPanel'
