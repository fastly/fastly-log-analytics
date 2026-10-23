'use client'

import { useQuery, type QueryClient, type UseQueryOptions, type UseQueryResult } from '@tanstack/react-query'
import { unwrapClient } from '@/lib/api-client'

/**
 * Shared React Query wrapper for the service-scoped session-scoring GET
 * endpoints (``/api/services/{service_id}/scoring/<endpoint>``).
 *
 * Every scoring card/chart repeated the same queryFn body: a typed
 * ``client.GET`` plus an ``if (!response.ok) throw`` guard. This centralizes
 * the guard; the caller passes the typed ``client.GET`` call itself, so the
 * path, query params and result type all come from the generated OpenAPI
 * schema with no cast. Returns the full ``useQuery`` result unchanged.
 */
type ScoringResult<T> = { data?: T; error?: unknown; response: Response }

export function useScoringQuery<T>(
  queryKey: readonly unknown[],
  request: () => Promise<ScoringResult<T>>,
  options?: Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, 'queryKey' | 'queryFn'>,
): UseQueryResult<T, Error> {
  return useQuery<T, Error, T, readonly unknown[]>({
    queryKey,
    queryFn: async () => unwrapClient(request()),
    ...options,
  })
}

export function invalidateServiceScoringQueries(queryClient: QueryClient, serviceId: string): void {
  void queryClient.invalidateQueries({
    predicate: (q) =>
      Array.isArray(q.queryKey) &&
      typeof q.queryKey[0] === 'string' &&
      q.queryKey[0].startsWith('scoring-') &&
      q.queryKey[1] === serviceId,
  })
}
