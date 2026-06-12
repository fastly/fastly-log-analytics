'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

import { durationColor, formatDuration, kindBadgeVariant } from '../_helpers'
import type { ActiveOrPromotedRow, ActiveRow } from '../_types'

/** Table of currently-active queries plus rows promoted from the
 *  just-finished window. Empty state when there's nothing to show.
 *
 *  Row visual hierarchy:
 *  - active (live):  bright background tint, pulsing dot, left accent border
 *  - promoted:       faded, outcome badge instead of Kill button
 *  - cancelled:      dim opacity until the next deregister
 */
export function ActiveTable({
  rows,
  expandedQid,
  onToggleRow,
  onKill,
  cancellingQid,
}: {
  rows: ActiveOrPromotedRow[]
  expandedQid: number | null
  onToggleRow: (qid: number) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
}) {
  if (rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        No active queries. Long-running queries will appear here in real time.
      </div>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b text-xs text-muted-foreground">
          <tr className="text-left">
            <th className="px-3 py-2 w-6"></th>
            <th className="px-3 py-2">Source</th>
            <th className="px-3 py-2">Caller</th>
            <th className="px-3 py-2">DB</th>
            <th className="px-3 py-2">Service</th>
            <th className="px-3 py-2">Pool</th>
            <th className="px-3 py-2 text-right">Duration</th>
            <th className="px-3 py-2 text-right w-24">Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <ActiveTableRow
              key={row.query_id}
              row={row}
              expanded={expandedQid === row.query_id}
              cancelling={cancellingQid === row.query_id}
              onToggle={() => onToggleRow(row.query_id)}
              onKill={onKill}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ActiveTableRow({
  row,
  expanded,
  cancelling,
  onToggle,
  onKill,
}: {
  row: ActiveOrPromotedRow
  expanded: boolean
  cancelling: boolean
  onToggle: () => void
  onKill: (row: ActiveRow) => void
}) {
  const isCancelled = row.cancelled_at !== null
  const promoted = !!row._completed
  const rowClass = promoted
    ? 'opacity-60 bg-muted/10'
    : isCancelled
      ? 'opacity-60'
      : 'bg-primary/5 border-l-2 border-l-primary/60'

  return (
    <React.Fragment>
      <tr
        className={`border-b hover:bg-muted/30 cursor-pointer ${rowClass}`}
        onClick={onToggle}
      >
        <td className="px-3 py-2">
          {expanded ? (
            <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
          )}
        </td>
        <td className="px-3 py-2">
          <div className="flex items-center gap-2">
            <Badge variant={kindBadgeVariant(row.attribution.kind)} className="capitalize">
              {row.attribution.kind}
            </Badge>
            <span className="truncate max-w-xs" title={row.attribution.label}>
              {row.attribution.label}
            </span>
          </div>
        </td>
        <td
          className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs"
          title={row.attribution.caller_file}
        >
          {row.attribution.caller_file}
        </td>
        <td className="px-3 py-2">{row.db_type}</td>
        <td className="px-3 py-2 text-xs">{row.service_id ?? '—'}</td>
        <td className="px-3 py-2 text-xs font-mono">{row.attribution.pool_slot ?? '—'}</td>
        <td className={`px-3 py-2 text-right font-mono ${durationColor(row.duration_ms)}`}>
          <span className="inline-flex items-center gap-1.5">
            {!promoted && !isCancelled && (
              <span className="relative flex h-2 w-2" aria-hidden="true">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
              </span>
            )}
            {formatDuration(row.duration_ms)}
          </span>
        </td>
        <td className="px-3 py-2 text-right">
          {promoted ? (
            <Badge
              variant={
                row._completed!.outcome === 'ok'
                  ? 'outline'
                  : row._completed!.outcome === 'cancelled'
                    ? 'secondary'
                    : 'destructive'
              }
              className="capitalize"
            >
              {row._completed!.outcome === 'ok' ? '✓ done' : row._completed!.outcome}
            </Badge>
          ) : row.cancellable && !isCancelled ? (
            <Button
              variant="destructive"
              size="sm"
              className="h-7 px-2"
              disabled={cancelling}
              onClick={(e) => {
                e.stopPropagation()
                onKill(row)
              }}
            >
              {cancelling ? (
                'Cancelling…'
              ) : (
                <>
                  <X className="h-3 w-3 mr-1" /> Kill
                </>
              )}
            </Button>
          ) : isCancelled ? (
            <span className="text-xs text-muted-foreground">cancelling…</span>
          ) : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </td>
      </tr>
      {expanded && <ExpandedRow row={row} />}
    </React.Fragment>
  )
}

/** Drawer-like row that opens beneath an expanded row. Fetches the full
 *  SQL on demand (the snapshot only includes the first 200 chars) and
 *  re-polls every 2 s so the live duration ticks up while the user reads. */
function ExpandedRow({ row }: { row: ActiveRow }) {
  const { data: fullRow } = useQuery({
    queryKey: ['admin', 'query-monitor', 'detail', row.query_id],
    queryFn: async ({ signal }) => {
      const r = await fetch(`/api/admin/queries/${row.query_id}`, { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json() as Promise<ActiveRow>
    },
    refetchInterval: 2000,
    refetchIntervalInBackground: false,
  })
  const sql = fullRow?.sql ?? row.sql_preview
  const attr = row.attribution

  return (
    <tr className="bg-muted/20">
      <td colSpan={8} className="px-3 py-3">
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div>
              <span className="text-muted-foreground">Caller:</span>{' '}
              <span className="font-mono">
                {attr.caller_qualname} ({attr.caller_file})
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Request:</span>{' '}
              {attr.request_path || '—'}
              {attr.request_id ? ` · ${attr.request_id.slice(0, 8)}` : ''}
            </div>
            {attr.cron_job && (
              <div>
                <span className="text-muted-foreground">Cron:</span> {attr.cron_job}
                {attr.cron_run_id && ` (run ${attr.cron_run_id})`}
              </div>
            )}
            {attr.pool_slot && (
              <div>
                <span className="text-muted-foreground">Pool slot:</span>{' '}
                <span className="font-mono">{attr.pool_slot}</span>
              </div>
            )}
          </div>
          <pre className="bg-background border rounded p-3 text-xs overflow-x-auto whitespace-pre-wrap font-mono max-h-64">
            {sql}
          </pre>
          {row.sql_len > 200 && !fullRow?.sql && (
            <div className="text-xs text-muted-foreground">
              Loading full SQL ({row.sql_len} chars)…
            </div>
          )}
        </div>
      </td>
    </tr>
  )
}
