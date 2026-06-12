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
import { ColumnDef } from '@tanstack/react-table'
import { ArrowUpDown, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

import { durationColor, formatDuration, formatMemoryMb, kindBadgeVariant } from '../_helpers'
import type { ActiveOrPromotedRow, ActiveRow, CompletedRow } from '../_types'

// ── Header helper ──────────────────────────────────────────────────────────

function SortHeader({ label }: { label: string }) {
  return ({ column }: any) => (
    <Button
      variant="ghost"
      onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
      className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs"
    >
      {label}
      <ArrowUpDown className="ml-2 h-3 w-3" />
    </Button>
  )
}

// ── Shared cell renderers ─────────────────────────────────────────────────

/** Source cell: kind badge + attribution label, truncated. */
function sourceCell({ row }: { row: any }) {
  const attr = row.original.attribution
  return (
    <div className="flex items-center gap-2 min-w-0">
      <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize shrink-0">
        {attr.kind}
      </Badge>
      <span className="truncate text-xs" title={attr.label}>
        {attr.label}
      </span>
    </div>
  )
}

/** Caller cell: qualname primary, file:line secondary at 60% opacity. */
function callerCell({ row }: { row: any }) {
  const attr = row.original.attribution
  return (
    <div className="font-mono text-xs text-muted-foreground" title={`${attr.caller_qualname}\n${attr.caller_file}`}>
      <div className="truncate">{attr.caller_qualname}</div>
      <div className="truncate text-[10px] opacity-60">{attr.caller_file}</div>
    </div>
  )
}

// ── Active-table-specific cells ───────────────────────────────────────────

/** Duration cell that shows a pulsing dot for live rows + the outcome
 *  text for promoted (just-finished) rows. */
function activeDurationCell({ row }: { row: any }) {
  const r: ActiveOrPromotedRow = row.original
  const isCancelled = r.cancelled_at !== null
  const promoted = !!r._completed
  return (
    <span className={`inline-flex items-center gap-1.5 font-mono ${durationColor(r.duration_ms)} ${promoted ? 'opacity-60' : ''}`}>
      {!promoted && !isCancelled && (
        <span className="relative flex h-2 w-2" aria-hidden="true">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
        </span>
      )}
      {formatDuration(r.duration_ms)}
    </span>
  )
}

/** Actions cell: Kill button on live rows, outcome badge on promoted rows. */
function buildActionsCell(
  onKill: (row: ActiveRow) => void,
  cancellingQid: number | null,
) {
  return ({ row }: { row: any }) => {
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
}): ColumnDef<ActiveOrPromotedRow>[] {
  const cols: ColumnDef<ActiveOrPromotedRow>[] = [
    {
      id: 'source',
      accessorFn: (r) => r.attribution.label,
      header: SortHeader({ label: 'Source' }),
      size: 280,
      cell: sourceCell,
    },
    {
      id: 'caller',
      accessorFn: (r) => r.attribution.caller_qualname,
      header: SortHeader({ label: 'Caller' }),
      size: 280,
      cell: callerCell,
    },
    {
      id: 'db_type',
      accessorKey: 'db_type',
      header: SortHeader({ label: 'DB' }),
      size: 80,
      cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span>,
    },
  ]
  if (deps.showService) {
    cols.push({
      id: 'service_id',
      accessorFn: (r) => r.service_id ?? '',
      header: SortHeader({ label: 'Service' }),
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
      header: SortHeader({ label: 'Pool' }),
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
      header: SortHeader({ label: 'Duration' }),
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
}): ColumnDef<CompletedRow>[] {
  const cols: ColumnDef<CompletedRow>[] = [
    {
      id: 'outcome',
      accessorKey: 'outcome',
      header: SortHeader({ label: 'Outcome' }),
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
      header: SortHeader({ label: 'Source' }),
      size: 280,
      cell: sourceCell,
    },
    {
      id: 'caller',
      accessorFn: (r) => r.attribution.caller_qualname,
      header: SortHeader({ label: 'Caller' }),
      size: 280,
      cell: callerCell,
    },
    {
      id: 'db_type',
      accessorKey: 'db_type',
      header: SortHeader({ label: 'DB' }),
      size: 80,
      cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span>,
    },
    {
      id: 'duration_ms',
      accessorKey: 'duration_ms',
      header: SortHeader({ label: 'Duration' }),
      size: 110,
      cell: ({ row }) => (
        <span className={`font-mono text-xs ${durationColor(row.original.duration_ms)}`}>
          {formatDuration(row.original.duration_ms)}
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
      header: SortHeader({ label: 'Service' }),
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
      header: SortHeader({ label: 'Memory' }),
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
