'use client'

/**
 * Shared TanStack ColumnDef builders for the Live Query Monitor tables.
 *
 * Both ActiveTable and CompletedTable render through the project-standard
 * ``<DataTable>`` (column reorder, hide/show, resize, virtualization,
 * pagination). The set of columns differs between the two (Active has a
 * Kill action, Completed has Outcome + Memory + SQL preview), but Source
 * / Caller / DB / Service / Pool / Duration are identical and live here
 * so a future column addition lands once instead of twice.
 *
 * Why a builder rather than a const array: the Active table's Actions
 * cell needs callbacks (``onKill`` / ``cancellingQid``) the parent owns.
 * Builders take those as deps and return ``ColumnDef`` objects with the
 * closures already baked in.
 */

import * as React from 'react'
import type { ColumnDef, Row } from '@tanstack/react-table'
import { ChevronDown, ChevronRight, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { DataTableReadonly as DataTable } from '@/components/DataTable'

import {
  durationColor,
  formatDurationMs,
  formatMemoryMb,
  kindBadgeVariant,
  type ActiveOrPromotedRow,
  type ActiveRow,
  type GroupedCompletedRow,
} from '../_hooks/useFilteredActive'

// ── Shared cell renderers ─────────────────────────────────────────────────

/** What the shared Source / Caller cells read off a row (active and completed rows both carry it). */
type AttributedRow = Pick<ActiveRow, 'attribution'> & Pick<ActiveOrPromotedRow, '_groupedCount' | '_isGroupHead' | '_expandedChild'>

/** Build the Source cell renderer with cron-group toggle support. The cell
 *  shows: kind badge + label + (when a cron group) a chevron-prefixed
 *  ``×N`` badge that toggles per-run expansion via ``onToggleGroup``.
 *  Expanded sibling rows render with an inset chevron in place of the
 *  badge so the visual grouping is clear. */
function buildSourceCell(onToggleGroup: (runId: string) => void) {
  return function SourceCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
    const attr = row.original.attribution
    const groupCount: number | undefined = row.original._groupedCount
    const isGroupHead: boolean = !!row.original._isGroupHead
    const isExpandedChild: boolean = !!row.original._expandedChild
    return (
      <div className={`flex items-center gap-2 min-w-0 ${isExpandedChild ? 'pl-5' : ''}`}>
        <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize shrink-0">
          {attr.kind}
        </Badge>
        <span className="truncate text-xs" title={attr.label}>
          {attr.label}
        </span>
        {groupCount && groupCount > 1 ? (
          <button
            type="button"
            className="shrink-0 inline-flex items-center gap-0.5 rounded-md border border-input bg-secondary px-1.5 py-0.5 font-mono text-[10px] hover:bg-secondary/80"
            title={
              isGroupHead
                ? `Collapse this cron run (${groupCount} queries)`
                : `Expand this cron run (${groupCount} queries)`
            }
            onClick={(e) => {
              e.stopPropagation()
              const runId: string | null = row.original.attribution.cron_run_id
              if (runId) onToggleGroup(runId)
            }}
          >
            {isGroupHead ? (
              <ChevronDown className="h-2.5 w-2.5" />
            ) : (
              <ChevronRight className="h-2.5 w-2.5" />
            )}
            ×{groupCount}
          </button>
        ) : null}
      </div>
    )
  }
}

/** Caller cell: qualname primary, file:line secondary at 60% opacity. */
function callerCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
  const attr = row.original.attribution
  return (
    <div className="font-mono text-xs text-muted-foreground" title={`${attr.caller_qualname}\n${attr.caller_file}`}>
      <div className="truncate">{attr.caller_qualname}</div>
      <div className="truncate text-[10px] opacity-60">{attr.caller_file}</div>
    </div>
  )
}

// ── Active-table-specific cells ───────────────────────────────────────────

/** A live row this old gets the whole duration cell pulsing red — a
 *  noticeably stronger signal than the existing red text colour above
 *  10 s. 30 s is the "this is probably stuck" cutoff: legitimate cron
 *  scans can hit 10–25 s, so going lower would burn the alarm. */
const STUCK_LIVE_MS = 30_000

/** Duration cell that shows a pulsing dot for live rows + the outcome
 *  text for promoted (just-finished) rows. Live rows past STUCK_LIVE_MS
 *  pulse red across the whole cell, not just the dot. */
function activeDurationCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
  const r: ActiveOrPromotedRow = row.original
  const isCancelled = r.cancelled_at !== null
  const promoted = !!r._completed
  const stuck = !promoted && !isCancelled && r.duration_ms >= STUCK_LIVE_MS
  return (
    <span
      className={`inline-flex items-center gap-1.5 font-mono ${durationColor(r.duration_ms)} ${promoted ? 'opacity-60' : ''} ${stuck ? 'animate-pulse font-bold' : ''}`}
      title={stuck ? `Live query running for ${formatDurationMs(r.duration_ms)} — investigate` : undefined}
    >
      {!promoted && !isCancelled && (
        <span className="relative flex h-2 w-2" aria-hidden="true">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
        </span>
      )}
      {formatDurationMs(r.duration_ms)}
    </span>
  )
}

/** Actions cell: Kill button on live rows, outcome badge on promoted rows. */
function buildActionsCell(
  onKill: (row: ActiveRow) => void,
  cancellingQid: number | null,
) {
  return function ActionsCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
    const r: ActiveOrPromotedRow = row.original
    const isCancelled = r.cancelled_at !== null
    const promoted = !!r._completed
    const cancelling = cancellingQid === r.query_id

    if (promoted) {
      return (
        <Badge
          variant={
            r._completed!.outcome === 'ok'
              ? 'outline'
              : r._completed!.outcome === 'cancelled'
                ? 'secondary'
                : 'destructive'
          }
          className="capitalize"
        >
          {r._completed!.outcome === 'ok' ? '✓ done' : r._completed!.outcome}
        </Badge>
      )
    }
    if (isCancelled) {
      return <span className="text-xs text-muted-foreground">cancelling…</span>
    }
    if (!r.cancellable) {
      return <span className="text-xs text-muted-foreground">—</span>
    }
    return (
      <Button
        variant="destructive"
        size="sm"
        className="h-7 px-2"
        disabled={cancelling}
        // SRE-19: set expectations up front — Kill is a one-shot interrupt of
        // the currently-running statement, not a permanent block. A caller
        // that retries spawns a new query (new id) that shows up in this table.
        title="Interrupt the currently-running statement. A caller that retries will start a new query you may need to cancel again."
        onClick={(e) => {
          e.stopPropagation()
          onKill(r)
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
    )
  }
}

// ── Public column-def builders ────────────────────────────────────────────

export function buildActiveColumns(deps: {
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  showService: boolean
  showPool: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<ActiveOrPromotedRow>[] {
  const sourceCell = buildSourceCell(deps.onToggleGroup)
  const cols: ColumnDef<ActiveOrPromotedRow>[] = [
    {
      id: 'source',
      accessorFn: (r) => r.attribution.label,
      header: 'Source',
      size: 280,
      cell: sourceCell,
    },
    {
      id: 'caller',
      accessorFn: (r) => r.attribution.caller_qualname,
      header: 'Caller',
      size: 280,
      cell: callerCell,
    },
    {
      id: 'db_type',
      accessorKey: 'db_type',
      header: 'DB',
      size: 80,
      cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span>,
    },
  ]
  if (deps.showService) {
    cols.push({
      id: 'service_id',
      accessorFn: (r) => r.service_id ?? '',
      header: 'Service',
      size: 200,
      cell: ({ row }) => (
        <span className="text-xs font-mono truncate" title={row.original.service_id ?? ''}>
          {row.original.service_id ?? '—'}
        </span>
      ),
    })
  }
  if (deps.showPool) {
    cols.push({
      id: 'pool_slot',
      accessorFn: (r) => r.attribution.pool_slot ?? '',
      header: 'Pool',
      size: 200,
      cell: ({ row }) => (
        <span className="text-xs font-mono">{row.original.attribution.pool_slot ?? '—'}</span>
      ),
    })
  }
  cols.push(
    {
      id: 'duration_ms',
      accessorKey: 'duration_ms',
      header: 'Duration',
      size: 120,
      cell: activeDurationCell,
    },
    {
      id: 'actions',
      header: 'Actions',
      enableSorting: false,
      size: 110,
      cell: buildActionsCell(deps.onKill, deps.cancellingQid),
    },
  )
  return cols
}

export function buildCompletedColumns(opts: {
  showMemory: boolean
  showService: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<GroupedCompletedRow>[] {
  const sourceCell = buildSourceCell(opts.onToggleGroup)
  const cols: ColumnDef<GroupedCompletedRow>[] = [
    {
      id: 'outcome',
      accessorKey: 'outcome',
      header: 'Outcome',
      size: 130,
      cell: ({ row }) => {
        const r = row.original
        return (
          <div className="flex items-center gap-2">
            <Badge
              variant={
                r.outcome === 'ok'
                  ? 'outline'
                  : r.outcome === 'cancelled'
                    ? 'secondary'
                    : 'destructive'
              }
              className="capitalize"
            >
              {r.outcome}
            </Badge>
            {r.error_type && (
              <span className="text-xs text-red-600 truncate" title={r.error_message ?? ''}>
                {r.error_type}
              </span>
            )}
          </div>
        )
      },
    },
    {
      id: 'source',
      accessorFn: (r) => r.attribution.label,
      header: 'Source',
      size: 280,
      cell: sourceCell,
    },
    {
      id: 'caller',
      accessorFn: (r) => r.attribution.caller_qualname,
      header: 'Caller',
      size: 280,
      cell: callerCell,
    },
    {
      id: 'db_type',
      accessorKey: 'db_type',
      header: 'DB',
      size: 80,
      cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span>,
    },
    {
      id: 'duration_ms',
      accessorKey: 'duration_ms',
      header: 'Duration',
      size: 110,
      cell: ({ row }) => (
        <span className={`font-mono text-xs ${durationColor(row.original.duration_ms)}`}>
          {formatDurationMs(row.original.duration_ms)}
        </span>
      ),
    },
  ]
  if (opts.showService) {
    // Insert just before the Duration column — same position as the
    // Active table for visual consistency.
    cols.splice(cols.length - 1, 0, {
      id: 'service_id',
      accessorFn: (r) => r.service_id ?? '',
      header: 'Service',
      size: 200,
      cell: ({ row }) => (
        <span className="text-xs font-mono truncate" title={row.original.service_id ?? ''}>
          {row.original.service_id ?? '—'}
        </span>
      ),
    })
  }
  if (opts.showMemory) {
    cols.push({
      id: 'peak_memory_mb',
      accessorKey: 'peak_memory_mb',
      header: 'Memory',
      size: 100,
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground tabular-nums">
          {formatMemoryMb(row.original.peak_memory_mb) || '—'}
        </span>
      ),
    })
  }
  cols.push({
    id: 'sql',
    accessorKey: 'sql_preview',
    header: 'SQL',
    enableSorting: false,
    size: 400,
    cell: ({ row }) => (
      <span
        className="font-mono text-xs text-muted-foreground truncate block"
        title={row.original.sql_preview}
      >
        {row.original.sql_preview}
      </span>
    ),
  })
  return cols
}

export function ActiveTable({
  rows,
  onRowClick,
  onKill,
  cancellingQid,
  onToggleGroup,
}: {
  rows: ActiveOrPromotedRow[]
  onRowClick: (row: ActiveOrPromotedRow) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  onToggleGroup: (runId: string) => void
}) {
  const showService = rows.some((r) => r.service_id !== null && r.service_id !== undefined)
  const showPool = rows.some((r) => r.attribution.pool_slot !== null && r.attribution.pool_slot !== undefined)
  const columns = React.useMemo(
    () => buildActiveColumns({ onKill, cancellingQid, showService, showPool, onToggleGroup }),
    [onKill, cancellingQid, showService, showPool, onToggleGroup],
  )
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      getRowClassName={activeRowClassName}
      hideToolbar
      showPagination={false}
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      tableCaption="Active and just-finished queries"
    />
  )
}

function activeRowClassName(row: ActiveOrPromotedRow): string {
  if (row._expandedChild) return 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
  if (row._completed) return 'opacity-60'
  if (row.cancelled_at !== null) return 'opacity-50'
  return 'bg-primary/5 border-l-2 border-l-primary/60'
}

export function CompletedTable({
  rows,
  onRowClick,
  emptyMessage = 'No completed queries yet.',
  initialSorting,
  onToggleGroup,
}: {
  rows: GroupedCompletedRow[]
  onRowClick: (row: GroupedCompletedRow) => void
  emptyMessage?: string
  initialSorting?: { id: string; desc: boolean }[]
  onToggleGroup: (runId: string) => void
}) {
  const showMemory = rows.some((r) => r.peak_memory_mb !== null && r.peak_memory_mb !== undefined)
  const showService = rows.some((r) => r.service_id !== null && r.service_id !== undefined)
  const columns = React.useMemo(
    () => buildCompletedColumns({ showMemory, showService, onToggleGroup }),
    [showMemory, showService, onToggleGroup],
  )
  return (
    <DataTable<GroupedCompletedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      getRowClassName={completedRowClassName}
      hideToolbar
      showPagination={rows.length > 50}
      emptyMessage={emptyMessage}
      initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]}
      tableCaption="Completed queries"
    />
  )
}

function completedRowClassName(row: GroupedCompletedRow): string {
  if (row._expandedChild) return 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
  return ''
}
