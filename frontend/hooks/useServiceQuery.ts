'use client'

import { useQuery, type UseQueryOptions, type QueryFunctionContext, type QueryKey } from '@tanstack/react-query'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { adminJsonResult } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import type { Simplify, WithRequired, components } from '@/types/api'

/**
 * `placeholderData` that holds the previous result across key changes within
 * one service (filter/range/page changes) but never across a service switch.
 * `keepPreviousData` alone would paint service A's rows under service B's
 * header until B's fetch lands. `serviceIdIndex` is the position of the
 * active service id in the query key (2 for `[domain, kind, serviceId, ...]`).
 */
export function keepPreviousWithinService(currentKey: QueryKey, serviceIdIndex = 2) {
  return <T>(previousData: T | undefined, previousQuery: { queryKey: QueryKey } | undefined): T | undefined => {
    if (!previousQuery) return undefined
    const prevKey = previousQuery.queryKey
    if (
      Array.isArray(currentKey) &&
      Array.isArray(prevKey) &&
      currentKey.length > serviceIdIndex &&
      prevKey.length > serviceIdIndex &&
      currentKey[serviceIdIndex] !== prevKey[serviceIdIndex]
    ) {
      return undefined
    }
    return previousData
  }
}

/**
 * Wraps useQuery with the standard enabled/placeholderData defaults every
 * data page uses: enabled when the service is ready, hold prior data while
 * the new fetch is in-flight.
 */
export function useServiceQuery<T>(
  queryKey: unknown[],
  queryFn: (context: QueryFunctionContext) => Promise<T>,
  options?: Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, 'queryKey' | 'queryFn' | 'placeholderData'>
) {
  const isReady = useIsDataReady()
  return useQuery<T, Error, T>({
    ...options,
    queryKey,
    queryFn,
    enabled: isReady && (options?.enabled ?? true),
    placeholderData: keepPreviousWithinService(queryKey),
  })
}

type RumStatusWire = WithRequired<components['schemas']['RumStatusResponse'], 'enabled'>

type RumStatus = Simplify<RumStatusWire & { unauthorized?: boolean }>

interface UseRumStatusOptions {
  enabled?: boolean
  refetchInterval?: number
}

/**
 * Single owner of the `['rum-status', serviceId]` cache entry. Every observer
 * must share one queryFn, otherwise whichever fetches last decides the cached
 * shape (403 → `{ unauthorized: true }` vs `null`).
 */
export function useRumStatus(
  serviceId: string | null | undefined,
  { enabled = true, refetchInterval }: UseRumStatusOptions = {},
) {
  return useQuery<RumStatus | null>({
    queryKey: queryKeys.rumStatus(serviceId),
    queryFn: async () => {
      if (!serviceId) return null
      const res = await adminJsonResult<'/api/services/{service_id}/rum/status', RumStatusWire>(
        '/api/services/{service_id}/rum/status',
        `/api/services/${serviceId}/rum/status`,
      )
      if (res.status === 403) return { enabled: false, unauthorized: true }
      return res.ok ? res.data : null
    },
    enabled: !!serviceId && enabled,
    refetchInterval,
  })
}
