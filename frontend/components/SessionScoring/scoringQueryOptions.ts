'use client'

import { queryOptions, type QueryClient } from '@tanstack/react-query'
import { client, unwrapClient } from '@/lib/api'

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
