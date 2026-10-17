'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'

import { client } from '@/lib/api'
import type { components } from '@/types/api'

type LabelRowWire = components['schemas']['ScoringLabelRow']

export type LabelValue = LabelRowWire['label']

export type LabelRow = LabelRowWire

export interface ScoringLabelsResult {
  labels: LabelRow[]
  counts: Record<LabelValue, number>
  labelBySid: Map<string, LabelValue>
  /** sid → label id, for callers that need the row id (delete / patch
   *  paths). Avoids the per-row labels.find(l => l.sid === sid) scan
   *  the SessionsTable Flag column was doing on every render. */
  idBySid: Map<string, string>
  isLoading: boolean
  isFetching: boolean
  error: (Error & { status?: number }) | null
}

interface Opts {
  /** Skip the fetch when false. Used by the dashboard which only wants
   *  labels when a service is selected. */
  enabled?: boolean
}

/**
 * Centralised query for /scoring/labels — used by the admin Labels tab,
 * the TopFlaggedTable's "currently labeled" badges, and the dashboard's
 * row-level Flag column. Returns a Map<sid, label> for O(1) lookup so
 * callers don't re-derive it per render.
 *
 * staleTime: 5 minutes. Labels only change on explicit user mutation
 * (which busts the cache via invalidateQueries), so a navigation
 * between the Overview and Labels tabs should reuse the cached data
 * instead of refetching it for every consumer.
 */
export function useScoringLabels(serviceId: string, opts: Opts = {}): ScoringLabelsResult {
  const { enabled = true } = opts
  const q = useQuery({
    queryKey: ['scoring-labels', serviceId],
    enabled: enabled && !!serviceId,
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/labels',
        { params: { path: { service_id: serviceId } } },
      )
      if (!response.ok || !data) throw new Error(`status ${response.status}`)
      return data
    },
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
