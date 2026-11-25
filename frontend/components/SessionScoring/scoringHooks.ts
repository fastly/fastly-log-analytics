'use client'

import React from 'react'
import { useQuery, type QueryKey, type UseQueryOptions } from '@tanstack/react-query'
import { client, queryKeys, unwrapClient } from '@/lib/api'
import type { components } from '@/types/api'

export function useScoringQuery<TData>(
  queryKey: QueryKey,
  fn: (signal?: AbortSignal) => Promise<{ data?: TData; error?: unknown; response: Response }>,
  options?: Omit<UseQueryOptions<TData, Error>, 'queryKey' | 'queryFn'>,
) {
  return useQuery<TData, Error>({
    queryKey,
    queryFn: ({ signal }) => unwrapClient(fn(signal)),
    staleTime: 60_000,
    ...options,
  })
}

type LabelRowWire = components['schemas']['ScoringLabelRow']
export type LabelValue = LabelRowWire['label']
export type LabelRow = LabelRowWire

export interface ScoringLabelsResult {
  labels: LabelRow[]
  counts: Record<LabelValue, number>
  labelBySid: Map<string, LabelValue>
  idBySid: Map<string, string>
  isLoading: boolean
  isFetching: boolean
  error: (Error & { status?: number }) | null
}

export function useScoringLabels(
  serviceId: string,
  opts: { enabled?: boolean } = {},
): ScoringLabelsResult {
  const { enabled = true } = opts
  const q = useQuery({
    queryKey: queryKeys.scoringLabels(serviceId),
    enabled: enabled && !!serviceId,
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/scoring/labels', {
          params: { path: { service_id: serviceId } },
          signal,
        }),
      ),
    staleTime: 5 * 60_000,
  })
  const { labelBySid, idBySid } = React.useMemo(() => {
    const lab = new Map<string, LabelValue>()
    const id = new Map<string, string>()
    for (const l of q.data?.labels ?? []) {
      lab.set(l.sid, l.label)
      id.set(l.sid, l.id)
    }
    return { labelBySid: lab, idBySid: id }
  }, [q.data])
  return {
    labels: q.data?.labels ?? [],
    counts: q.data?.counts ?? { good: 0, bad: 0, neutral: 0 },
    labelBySid,
    idBySid,
    isLoading: q.isLoading,
    isFetching: q.isFetching,
    error: (q.error as (Error & { status?: number }) | null) ?? null,
  }
}
