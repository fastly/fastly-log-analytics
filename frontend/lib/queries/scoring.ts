import { queryOptions } from '@tanstack/react-query'

import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'

/**
 * Session-scoring composite queries. The admin hover-prefetch
 * (AdminPrefetchLinks) and the Session Scoring page must share ONE definition:
 * a prefetch only warms the page if its key and queryFn byte-match.
 */

export const scoringAnalyticsCompositeKey = (serviceId: string | null | undefined) =>
  ['scoring-analytics-composite', serviceId] as const

export const scoringConfigCompositeKey = (serviceId: string | null | undefined) =>
  ['scoring-config-composite', serviceId] as const

export const scoringAnalyticsCompositeOptions = (serviceId: string | null | undefined, sinceHours: number) =>
  queryOptions({
    queryKey: [...scoringAnalyticsCompositeKey(serviceId), sinceHours] as const,
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/scoring/analytics', {
          params: {
            path: { service_id: serviceId ?? '' },
            query: { since_hours: sinceHours },
          },
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
