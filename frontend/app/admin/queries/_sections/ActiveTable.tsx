'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

import { durationColor, formatDuration, kindBadgeVariant } from '../_helpers'
import type { ActiveOrPromotedRow, ActiveRow } from '../_types'

const COLSPAN = 8

/** Table of currently-active queries plus rows promoted from the
 *  just-finished window. Empty state when there's nothing to show.
 *
 *  Row visual hierarchy:
 *  - active (live):  bright background tint, pulsing dot, left accent border
 *  - promoted:       faded, outcome badge instead of Kill button
 *  - cancelled:      dim opacity until the next deregister
 *  - focused:        ring around the row (driven by keyboard navigation)
 *
 *  When `groupByRun` is true, cron rows are bucketed by `cron_run_id` and
 *  rendered under a collapsible group header. Non-cron rows always render
 *  inline. Matches design doc §7 ("group by cron_run_id so an admin can
 *  see all 47 queries from sync tick 7f3a as one collapsible block").
 */
export function ActiveTable({
  rows,
  expandedQid,
  onToggleRow,
  onKill,
  cancellingQid,
  focusedQid = null,
  groupByRun = false,
}: {
  rows: ActiveOrPromotedRow[]
  expandedQid: number | null
  onToggleRow: (qid: number) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  focusedQid?: number | null
  groupByRun?: boolean
}) {
  if (rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        No active queries. Long-running queries will appear here in real time.
      </div>
    )
  }

  // When groupByRun is off, render the flat list. When on, cron rows get
  // bucketed under group headers; non-cron rows render in their original
  // position relative to each other.
  const groups = groupByRun ? buildGroups(rows) : null

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
          {groups ? (
            <GroupedRows
              groups={groups}
              expandedQid={expandedQid}
              focusedQid={focusedQid}
              cancellingQid={cancellingQid}
              onToggleRow={onToggleRow}
              onKill={onKill}
            />
          ) : (
            rows.map((row) => (
              <ActiveTableRow
                key={row.query_id}
                row={row}
                expanded={expandedQid === row.query_id}
                focused={focusedQid === row.query_id}
                cancelling={cancellingQid === row.query_id}
                onToggle={() => onToggleRow(row.query_id)}
                onKill={onKill}
              />
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}

/** A single render unit. Either an inline row or a group of cron rows
 *  sharing a cron_run_id (or no run id → "Ungrouped cron"). */
type GroupEntry =
  | { kind: 'row'; row: ActiveOrPromotedRow }
  | { kind: 'group'; key: string; label: string; rows: ActiveOrPromotedRow[] }

function buildGroups(rows: ActiveOrPromotedRow[]): GroupEntry[] {
  // First pass: collect cron rows into buckets keyed by cron_run_id (or a
  // sentinel for nulls). Preserve insertion order so non-cron rows interleave
  // naturally with the FIRST occurrence of each group.
  const buckets = new Map<string, ActiveOrPromotedRow[]>()
  const order: GroupEntry[] = []
  for (const row of rows) {
    if (row.attribution.kind !== 'cron') {
      order.push({ kind: 'row', row })
      continue
    }
    const runId = row.attribution.cron_run_id
    const job = row.attribution.cron_job ?? 'cron'
    const key = runId ? `run:${runId}` : `job:${job}::nullrun`
    if (!buckets.has(key)) {
      buckets.set(key, [])
      const label = runId
        ? `Cron: ${job} (run ${runId.slice(0, 8)})`
        : `Cron: ${job} (no run id)`
      order.push({ kind: 'group', key, label, rows: buckets.get(key)! })
    }
    buckets.get(key)!.push(row)
  }
  return order
}

function GroupedRows({
  groups,
  expandedQid,
  focusedQid,
  cancellingQid,
  onToggleRow,
  onKill,
}: {
  groups: GroupEntry[]
  expandedQid: number | null
  focusedQid: number | null
  cancellingQid: number | null
  onToggleRow: (qid: number) => void
  onKill: (row: ActiveRow) => void
}) {
  // Open all groups by default. The user can collapse a noisy cron job to
  // get it out of the way; the collapsed state is local to this mount.
  const [collapsed, setCollapsed] = React.useState<Set<string>>(new Set())
  const toggle = (key: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }
  return (
    <>
      {groups.map((entry) => {
        if (entry.kind === 'row') {
          return (
            <ActiveTableRow
              key={entry.row.query_id}
              row={entry.row}
              expanded={expandedQid === entry.row.query_id}
              focused={focusedQid === entry.row.query_id}
              cancelling={cancellingQid === entry.row.query_id}
              onToggle={() => onToggleRow(entry.row.query_id)}
              onKill={onKill}
            />
          )
        }
        const isCollapsed = collapsed.has(entry.key)
        const oldestMs = Math.max(...entry.rows.map((r) => r.duration_ms))
        return (
          <React.Fragment key={entry.key}>
            <tr
              className="bg-muted/40 hover:bg-muted/60 cursor-pointer border-b text-xs text-muted-foreground"
              onClick={() => toggle(entry.key)}
            >
              <td colSpan={COLSPAN} className="px-3 py-1.5">
                <span className="inline-flex items-center gap-2 font-medium">
                  {isCollapsed ? (
                    <ChevronRight className="h-3.5 w-3.5" />
                  ) : (
                    <ChevronDown className="h-3.5 w-3.5" />
                  )}
                  {entry.label}
                  <Badge variant="outline" className="text-[10px] font-normal">
                    {entry.rows.length} {entry.rows.length === 1 ? 'query' : 'queries'}
                  </Badge>
                  <span className={`tabular-nums ${durationColor(oldestMs)}`}>
                    oldest {formatDuration(oldestMs)}
                  </span>
                </span>
              </td>
            </tr>
            {!isCollapsed &&
              entry.rows.map((row) => (
                <ActiveTableRow
                  key={row.query_id}
                  row={row}
                  expanded={expandedQid === row.query_id}
                  focused={focusedQid === row.query_id}
                  cancelling={cancellingQid === row.query_id}
                  onToggle={() => onToggleRow(row.query_id)}
                  onKill={onKill}
                />
              ))}
          </React.Fragment>
        )
      })}
    </>
  )
}

function ActiveTableRow({
  row,
  expanded,
  focused,
  cancelling,
  onToggle,
  onKill,
}: {
  row: ActiveOrPromotedRow
  expanded: boolean
  focused: boolean
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
  const focusClass = focused ? 'outline outline-2 outline-primary outline-offset-[-2px]' : ''

  return (
    <React.Fragment>
      <tr
        data-qid={row.query_id}
        className={`border-b hover:bg-muted/30 cursor-pointer ${rowClass} ${focusClass}`}
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
          title={`${row.attribution.caller_qualname}\n${row.attribution.caller_file}`}
        >
          <div className="truncate">{row.attribution.caller_qualname}</div>
          <div className="truncate text-[10px] opacity-60">{row.attribution.caller_file}</div>
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
      <td colSpan={COLSPAN} className="px-3 py-3">
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
