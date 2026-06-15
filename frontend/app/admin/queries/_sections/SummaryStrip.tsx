'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'

import { Badge } from '@/components/ui/badge'

import { durationColor, formatDuration, useDocumentVisible } from '../_helpers'
import type { SummaryResponse } from '../_types'

/** Top-of-page strip with live counts + longest in-flight duration.
 *  Polls the cheap `/api/admin/queries/summary` endpoint independently
 *  from the main snapshot so the badge stays fresh even when the table
 *  hides the row that's driving the "longest" value.
 *
 *  Includes a screen-reader live region (`role="status"`, `aria-live=polite`)
 *  that announces the count only when it actually changes — without the
 *  memoisation the announcement would re-fire every 300ms poll. */
export function SummaryStrip() {
  const visible = useDocumentVisible()
  const { data } = useQuery<SummaryResponse>({
    queryKey: ['admin', 'query-monitor', 'summary'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/queries/summary', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: visible,
    // Adaptive cadence mirrors the snapshot query: 300 ms whenever
    // anything is running (badge tracks bursts in near-real-time),
    // 1500 ms when idle so the badge isn't ticking the backend 200x/min
    // just to read "0 active".
    refetchInterval: (query) => {
      const d = query.state.data as SummaryResponse | undefined
      return (d?.active_total && d.active_total > 0) ? 300 : 1500
    },
    refetchIntervalInBackground: false,
  })
  // Stable string for the live region. Only re-renders when the count
  // changes, so screen readers don't fire on every poll. Pluralise so
  // it reads as English, not "1 active queries".
  const liveLabel = React.useMemo(() => {
    if (!data) return ''
    return `${data.active_total} active ${data.active_total === 1 ? 'query' : 'queries'}`
  }, [data?.active_total])
  if (!data) return null
  return (
    <div className="flex items-center gap-3 text-sm">
      <Badge variant={data.active_total > 0 ? 'default' : 'outline'} className="gap-1">
        <span className="font-medium">{data.active_total}</span> active
      </Badge>
      {Object.entries(data.by_db_type).map(([db, n]) => (
        <Badge key={db} variant="outline" className="gap-1">
          {db} <span className="font-medium">{n}</span>
        </Badge>
      ))}
      {data.longest_ms > 0 && (
        <span className={`text-xs ${durationColor(data.longest_ms)}`}>
          longest: {formatDuration(data.longest_ms)}
        </span>
      )}
      <div role="status" aria-live="polite" className="sr-only">
        {liveLabel}
      </div>
    </div>
  )
}
