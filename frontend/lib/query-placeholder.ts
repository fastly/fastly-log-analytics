import type { QueryKey } from '@tanstack/react-query'

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
