'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'

import { client } from '@/lib/api'

export type LabelValue = 'good' | 'bad' | 'neutral'

export interface LabelRow {
  id: string
  sid: string
  label: LabelValue
  notes?: string
  sample_ip?: string
  sample_ua?: string
  sample_url?: string
  flagged_by?: string
  created_at?: string
  updated_at?: string
}

export interface ScoringLabelsResult {
  labels: LabelRow[]
  counts: Record<LabelValue, number>
  labelBySid: Map<string, LabelValue>
  isLoading: boolean
  isFetching: boolean
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
 */
export function useScoringLabels(serviceId: string, opts: Opts = {}): ScoringLabelsResult {
  const { enabled = true } = opts
  const q = useQuery({
    queryKey: ['scoring-labels', serviceId],
    enabled: enabled && !!serviceId,
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/labels' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as { labels: LabelRow[]; counts: Record<LabelValue, number> }
    },
  })

  const labelBySid = React.useMemo(() => {
    const m = new Map<string, LabelValue>()
    for (const l of q.data?.labels ?? []) m.set(l.sid, l.label)
    return m
  }, [q.data])

  return {
    labels: q.data?.labels ?? [],
    counts: q.data?.counts ?? { good: 0, bad: 0, neutral: 0 },
    labelBySid,
    isLoading: q.isLoading,
    isFetching: q.isFetching,
  }
}
