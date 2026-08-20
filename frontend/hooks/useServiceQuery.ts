'use client'

import { useQuery, type UseQueryOptions, type QueryFunctionContext } from '@tanstack/react-query'
import { useIsDataReady } from '@/hooks/useIsDataReady'

/**
 * Wraps useQuery with the standard enabled/placeholderData defaults every
 * data page uses: enabled when the service is ready, hold prior data while
 * the new fetch is in-flight.
 */
export function useServiceQuery<T>(
  queryKey: unknown[],
  queryFn: (context: QueryFunctionContext) => Promise<T>,
  options?: Omit<UseQueryOptions<T, Error, T, any>, 'queryKey' | 'queryFn' | 'placeholderData'>
) {
  const isReady = useIsDataReady()
  return useQuery<T, Error, T>({
    queryKey,
    queryFn,
    enabled: isReady && (options?.enabled ?? true),
    placeholderData: (previousData, previousQuery) => {
      if (!previousQuery) return undefined
      if (
        Array.isArray(queryKey) &&
        Array.isArray(previousQuery.queryKey) &&
        queryKey.length > 2 &&
        previousQuery.queryKey.length > 2 &&
        queryKey[2] !== previousQuery.queryKey[2]
      ) {
        return undefined
      }
      return previousData
    },
    ...options,
  })
}
