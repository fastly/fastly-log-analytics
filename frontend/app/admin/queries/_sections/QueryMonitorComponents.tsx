'use client'

import * as React from 'react'
import type { ColumnDef, Row } from '@tanstack/react-table'
import { ChevronDown, ChevronRight, X } from 'lucide-react'
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  type ActiveOrPromotedRow,
  type ActiveRow,
  type AttributedRow,
  type GroupedCompletedRow,
  durationColor,
  formatDurationMs,
  formatMemoryMb,
  kindBadgeVariant,
} from './useQueryMonitorState'
import { outcomeVariant } from './QueryMonitorDialogs'

export {
  RowDetailDialog,
  FilterChipRow,
  PollingIndicator,
  ShortcutsHelp,
  SummaryStrip,
} from './QueryMonitorDialogs'

const STUCK_LIVE_MS = 30_000

function buildSourceCell(onToggleGroup: (runId: string) => void) {
  return function SourceCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
    const attr = row.original.attribution
    const groupCount = row.original._groupedCount
    const isGroupHead = !!row.original._isGroupHead
    return (
      <div
        className={`flex items-center gap-2 min-w-0 ${
          row.original._expandedChild ? 'pl-5' : ''
        }`}
      >
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
              if (attr.cron_run_id) onToggleGroup(attr.cron_run_id)
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

function callerCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
  const attr = row.original.attribution
  return (
    <div
      className="font-mono text-xs text-muted-foreground"
      title={`${attr.caller_qualname}\n${attr.caller_file}`}
    >
      <div className="truncate">{attr.caller_qualname}</div>
      <div className="truncate text-[10px]">{attr.caller_file}</div>
    </div>
  )
}

function buildServiceCol<T extends AttributedRow>(): ColumnDef<T> {
  return {
    id: 'service_id',
    accessorFn: (r) => r.service_id ?? '',
    header: 'Service',
    size: 200,
    cell: ({ row }) => (
      <span className="text-xs font-mono truncate" title={row.original.service_id ?? ''}>
        {row.original.service_id ?? '—'}
      </span>
    ),
  }
}

function buildSharedAttributionColumns<T extends AttributedRow>(
  onToggleGroup: (runId: string) => void,
): ColumnDef<T>[] {
  return [
    {
      id: 'source',
      accessorFn: (r) => r.attribution.label,
      header: 'Source',
      size: 280,
      cell: buildSourceCell(onToggleGroup),
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
}

function activeDurationCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
  const r = row.original
  const isCancelled = r.cancelled_at !== null
  const promoted = !!r._completed
  const stuck = !promoted && !isCancelled && r.duration_ms >= STUCK_LIVE_MS
  return (
    <span
      className={`inline-flex items-center gap-1.5 font-mono ${durationColor(r.duration_ms)} ${stuck ? 'animate-pulse font-bold' : ''}`}
      title={
        stuck
          ? `Live query running for ${formatDurationMs(r.duration_ms)} — investigate`
          : undefined
      }
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

function buildActionsCell(
  onKill: (row: ActiveRow) => void,
  cancellingQid: number | null,
) {
  return function ActionsCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
    const r = row.original
    if (r._completed) {
      const outcome = r._completed.outcome
      return (
        <Badge variant={outcomeVariant(outcome)} className="capitalize">
          {outcome === 'ok' ? '✓ done' : outcome}
        </Badge>
      )
    }
    if (r.cancelled_at !== null) {
      return <span className="text-xs text-muted-foreground">cancelling…</span>
    }
    if (!r.cancellable) {
      return <span className="text-xs text-muted-foreground">—</span>
    }
    const cancelling = cancellingQid === r.query_id
    return (
      <Button
        variant="destructive"
        size="sm"
        className="h-7 px-2"
        disabled={cancelling}
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

export function buildActiveColumns(deps: {
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  showService: boolean
  showPool: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<ActiveOrPromotedRow>[] {
  const cols = buildSharedAttributionColumns<ActiveOrPromotedRow>(deps.onToggleGroup)
  if (deps.showService) cols.push(buildServiceCol())
  if (deps.showPool) {
    cols.push({
      id: 'pool_slot',
      accessorFn: (r) => r.attribution.pool_slot ?? '',
      header: 'Pool',
      size: 200,
      cell: ({ row }) => (
        <span className="text-xs font-mono">
          {row.original.attribution.pool_slot ?? '—'}
        </span>
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
            <Badge variant={outcomeVariant(r.outcome)} className="capitalize">
              {r.outcome}
            </Badge>
            {r.error_type && (
              <span
                className="text-xs text-red-600 truncate"
                title={r.error_message ?? ''}
              >
                {r.error_type}
              </span>
            )}
          </div>
        )
      },
    },
    ...buildSharedAttributionColumns<GroupedCompletedRow>(opts.onToggleGroup),
  ]
  if (opts.showService) cols.push(buildServiceCol())
  cols.push({
    id: 'duration_ms',
    accessorKey: 'duration_ms',
    header: 'Duration',
    size: 110,
    cell: ({ row }) => (
      <span className={`font-mono text-xs ${durationColor(row.original.duration_ms)}`}>
        {formatDurationMs(row.original.duration_ms)}
      </span>
    ),
  })
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

const hasAnyVal = <T,>(rows: T[], pick: (r: T) => unknown) =>
  rows.some((r) => pick(r) != null)

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
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const showPool = hasAnyVal(rows, (r) => r.attribution.pool_slot)
  const columns = React.useMemo(
    () =>
      buildActiveColumns({
        onKill,
        cancellingQid,
        showService,
        showPool,
        onToggleGroup,
      }),
    [onKill, cancellingQid, showService, showPool, onToggleGroup],
  )
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={false}
      getRowClassName={(row) =>
        row._expandedChild
          ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
          : row._completed
            ? 'bg-muted/30'
            : row.cancelled_at !== null
              ? 'bg-muted/50'
              : 'bg-primary/5 border-l-2 border-l-primary/60'
      }
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      tableCaption="Active and just-finished queries"
    />
  )
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
  const showMemory = hasAnyVal(rows, (r) => r.peak_memory_mb)
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const columns = React.useMemo(
    () => buildCompletedColumns({ showMemory, showService, onToggleGroup }),
    [showMemory, showService, onToggleGroup],
  )
  return (
    <DataTable<GroupedCompletedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={rows.length > 50}
      getRowClassName={(row) =>
        row._expandedChild ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30' : ''
      }
      emptyMessage={emptyMessage}
      initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]}
      tableCaption="Completed queries"
    />
  )
}
