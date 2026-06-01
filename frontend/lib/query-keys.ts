import type { FiltersPayload } from '@/types/filters'

/**
 * Centralised React Query key factory.
 *
 * Every page that queries the backend should build its keys here so the shape
 * is consistent, typo-safe, and easy to invalidate by prefix.
 *
 * Usage:
 *   queryKey: queryKeys.filtered('dashboard', 'aggregates', serviceId, start, end, filters, metric, interval)
 *   queryKey: queryKeys.bootstrap()
 */
export const queryKeys = {
  bootstrap: () => ['bootstrap'] as const,

  logFieldsCatalog: (serviceId?: string) => (serviceId ? ['log-fields-catalog', serviceId] as const : ['log-fields-catalog'] as const),

  filtered: (
    domain: string,
    endpoint: string,
    serviceId: string | null,
    startTime: string,
    endTime: string,
    filters: FiltersPayload,
    ...extras: unknown[]
  ) => [domain, endpoint, serviceId, startTime, endTime, filters, ...extras] as const,
}
