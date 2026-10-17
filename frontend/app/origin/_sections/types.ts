import type { components } from '@/types/api.generated'

type OriginAggregates = components['schemas']['OriginAggregatesResponse']

export type OriginSummary = OriginAggregates['summary']
export type OriginTimeseries = OriginAggregates['timeseries']
export type OriginSlowUrls = OriginAggregates['slow_urls']
export type OriginStatusCodes = OriginAggregates['status_codes']
export type OriginPopLatency = OriginAggregates['pop_latency']
export type OriginIpHealth = OriginAggregates['ip_health']

export type OriginSlowUrlRow = components['schemas']['OriginSlowUrlRow']
export type OriginPopLatencyRow = components['schemas']['OriginPopLatencyRow']
export type OriginIpHealthRow = components['schemas']['OriginIpHealthRow']

/** One section of the origin composite plus the bundle query's loading state. */
export interface SectionQuery<T> {
  data: T | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
}
