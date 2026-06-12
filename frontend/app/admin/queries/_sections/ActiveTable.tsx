'use client'

/**
 * Active & Just-Finished panel — thin wrapper around the project's
 * standard ``<DataTable>``.
 *
 * Visual hierarchy via DataTable's opt-in ``getRowClassName`` hook (added
 * 2026-06-12): live rows get a subtle tinted bg + left accent border,
 * promoted (just-finished) rows fade to 60% opacity, cancelled rows dim.
 * The pulsing dot in the Duration cell + Kill button vs outcome badge in
 * the Actions cell are the other live-vs-promoted signals.
 *
 * Service + Pool columns auto-hide when every visible row has the value
 * empty — same pattern as the Memory column in CompletedTable. Keeps the
 * table compact when the filter narrows to a single service or to SQLite
 * (which has no pool concept).
 *
 * Inline expand drawer → ``RowDetailDialog`` opened on row click.
 * Cron-groups: ×N badge in the Source cell toggles per-run expansion;
 * sibling rows render with a muted-bg + left indent.
 */

import * as React from 'react'

import { DataTable } from '@/components/DataTable'

import { buildActiveColumns } from './queryColumns'
import type { ActiveOrPromotedRow, ActiveRow } from '../_types'

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
  // Memoise so DataTable's React.memo doesn't see a new columns array on
  // every snapshot poll — would defeat the row-level virtualisation memo.
  const columns = React.useMemo(
    () => buildActiveColumns({ onKill, cancellingQid, showService, showPool, onToggleGroup }),
    [onKill, cancellingQid, showService, showPool, onToggleGroup],
  )
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      getRowClassName={rowClassName}
      hideToolbar
      showPagination={false}
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      initialSorting={[{ id: 'duration_ms', desc: true }]}
      tableCaption="Active and just-finished queries"
    />
  )
}

function rowClassName(row: ActiveOrPromotedRow): string {
  // Expanded cron sibling rows are inset and muted — order matters: this
  // takes precedence over the live-tint so a group's children read as a
  // visual subtree, not as N separate live rows.
  if (row._expandedChild) return 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
  if (row._completed) return 'opacity-60'
  if (row.cancelled_at !== null) return 'opacity-50'
  return 'bg-primary/5 border-l-2 border-l-primary/60'
}
