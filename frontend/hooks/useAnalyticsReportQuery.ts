'use client'

import { type UseQueryOptions } from '@tanstack/react-query'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { resolveRangeWire, type RangeWireArgs, type RangeWire } from '@/lib/range-wire'

/**
 * Context handed to the caller's fetcher. Carries the resolved range-wire so
 * the endpoint-specific POST body is just `{ filters, ...extra, ...rangeBody }`
 * — the caller keeps the typed `client.POST(path, …)` call (and thus full
 * openapi-fetch body/response typing) while this hook owns the range-wire
 * resolution, the query-key shape, and the useServiceQuery wiring.
 */
interface AnalyticsReportQueryContext {
  /** Body fragment to spread into the POST (token or absolute mode). */
  rangeBody: RangeWire['rangeBody']
  /** The resolved wire token, or null in absolute mode. */
  rangeToken: RangeWire['rangeToken']
  /** AbortSignal from React Query for request cancellation. */
  signal: AbortSignal | undefined
}

export interface AnalyticsReportQueryArgs<TData> {
  /**
   * Query-key namespace prefix, e.g. `['origin', 'aggregates']`. The full key
   * is `[...namespace, serviceId, rangeKey, anchor, filterPayload, ...keyParts]`
   * — byte-matching the SSR seed keys in `lib/ssr/*` (AGENTS.md SSR rule #6).
   */
  namespace: readonly unknown[]
  serviceId: string | null
  /** The debounced filter payload. Participates in the key only. */
  filterPayload: unknown
  /** Range-wire inputs (relativeRange, isAutoRange, startTime, endTime, anchor). */
  range: RangeWireArgs
  /** Extra key elements appended after filterPayload (bucket, metric, sections…). */
  keyParts?: readonly unknown[]
  /** Typed fetcher. Receives the resolved range-wire + abort signal. */
  queryFn: (ctx: AnalyticsReportQueryContext) => Promise<TData>
  queryOptions?: Omit<
    UseQueryOptions<TData, Error, TData, readonly unknown[]>,
    'queryKey' | 'queryFn' | 'placeholderData'
  >
}

/**
 * Centralizes the query shape shared by the SSR-seedable analytics pages
 * (origin / performance / security): resolve the range-wire contract once,
 * assemble the canonical query key, and run it through `useServiceQuery` with
 * the standard enabled/placeholderData defaults + signal cancellation.
 *
 * The produced key MUST stay identical to the SSR seed key so cold-load
 * hydration keeps hitting — see `__tests__/ssr/*.test.ts` key-match guards.
 */
export function useAnalyticsReportQuery<TData>({
  namespace,
  serviceId,
  filterPayload,
  range,
  keyParts,
  queryFn,
  queryOptions,
}: AnalyticsReportQueryArgs<TData>) {
  const { rangeKey, rangeBody, rangeToken } = resolveRangeWire(range)
  const queryKey: unknown[] = [
    ...namespace,
    serviceId,
    rangeKey,
    range.anchor,
    filterPayload,
    ...(keyParts ?? []),
  ]
  return useServiceQuery<TData>(
    queryKey,
    ({ signal }) => queryFn({ rangeBody, rangeToken, signal }),
    queryOptions,
  )
}
