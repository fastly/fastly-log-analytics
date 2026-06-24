'use client'

import * as React from 'react'

import { Badge } from '@/components/ui/badge'

import { durationColor, formatDuration } from '../_helpers'
import type { SnapshotResponse, SummaryResponse } from '../_types'

interface SummaryStripProps {
  snapshot: SnapshotResponse | undefined
}

/** Top-of-page strip with live counts + longest in-flight duration.
 *  Derives every value from the snapshot the parent page already polls
 *  for, so this component piggybacks on that poll instead of firing its
 *  own duplicate `/api/admin/queries/summary` request at the same
 *  cadence (which was producing a second backend round-trip per tick).
 *
 *  Includes a screen-reader live region (`role="status"`, `aria-live=polite`)
 *  that announces the count only when it actually changes — without the
 *  memoisation the announcement would re-fire every snapshot tick. */
export function SummaryStrip({ snapshot }: SummaryStripProps) {
  const data = React.useMemo<SummaryResponse | null>(() => {
    if (!snapshot) return null
    const by_db_type: Record<string, number> = {}
    let longest_ms = 0
    for (const row of snapshot.active) {
      by_db_type[row.db_type] = (by_db_type[row.db_type] ?? 0) + 1
      if (row.duration_ms > longest_ms) longest_ms = row.duration_ms
    }
    return {
      active_total: snapshot.active.length,
      by_db_type,
      longest_ms,
    }
  }, [snapshot])
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
