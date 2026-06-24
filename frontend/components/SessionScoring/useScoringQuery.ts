'use client'

import { useQuery, type UseQueryOptions, type UseQueryResult } from '@tanstack/react-query'

import { client } from '@/lib/api'

/**
 * Shared React Query wrapper for the service-scoped session-scoring GET
 * endpoints (``/api/services/{service_id}/scoring/<endpoint>``).
 *
 * Every scoring card/chart repeated the same queryFn body: a
 * ``client.GET`` with ``path: { service_id }`` + a query object, a
 * ``if (!response.ok) throw`` guard, and a ``return data as T`` cast. This
 * centralizes that boilerplate; callers pass the query-key, endpoint slug,
 * query params, and result type, and get back the full ``useQuery`` result
 * (data / isLoading / isFetching / isError / error / refetch) unchanged.
 *
 * The openapi-fetch path is intentionally cast to ``any`` — the endpoint
 * slug is interpolated, so the per-route typing is lost; callers supply the
 * concrete ``T``. This mirrors what the inlined call sites already did.
 */
export function useScoringQuery<T>(
  queryKey: readonly unknown[],
  serviceId: string,
  endpoint: string,
  query: Record<string, unknown>,
  options?: Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, 'queryKey' | 'queryFn'>,
): UseQueryResult<T, Error> {
  return useQuery<T, Error, T, readonly unknown[]>({
    queryKey,
    queryFn: async () => {
      const { data, response } = await client.GET(
        `/api/services/{service_id}/scoring/${endpoint}` as any,
        { params: { path: { service_id: serviceId }, query } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as T
    },
    ...options,
  })
}
