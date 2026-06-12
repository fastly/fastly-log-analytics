'use client'

import { useQuery } from '@tanstack/react-query'

import { Badge } from '@/components/ui/badge'

import { durationColor, formatDuration, useDocumentVisible } from '../_helpers'
import type { SummaryResponse } from '../_types'

/** Top-of-page strip with live counts + longest in-flight duration.
 *  Polls the cheap `/api/admin/queries/summary` endpoint independently
 *  from the main snapshot so the badge stays fresh even when the table
 *  hides the row that's driving the "longest" value. */
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
    // Same 300ms cadence as the snapshot — without it the badge lags the
    // table and the page feels inconsistent.
    refetchInterval: 300,
    refetchIntervalInBackground: false,
  })
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
    </div>
  )
}
