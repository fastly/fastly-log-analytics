'use client'

/**
 * Active & Just-Finished panel — thin wrapper around the project's
 * standard ``<DataTable>``.
 *
 * Loses (vs the prior custom-HTML-table implementation):
 *   - Per-row tinted background for live rows / faded background for
 *     promoted rows (DataTable's MemoizedTableRow doesn't expose a
 *     ``getRowProps`` hook — cell-level styling only; the duration cell
 *     still shows the pulsing dot on live rows and fades on promoted).
 *   - Inline expand drawer (replaced by ``RowDetailDialog`` opened on
 *     row click).
 *   - Cron-grouping by run_id (TanStack's getGroupedRowModel has
 *     different semantics from the prior custom collapsible blocks;
 *     dropped for now — re-add as a separate feature if needed).
 *   - Focused row + arrow-key navigation (sort + search + filter chips
 *     cover the common "find the right row" cases).
 *
 * Gains:
 *   - Column reorder via drag-and-drop, hide/show via column-visibility
 *     menu, resize, virtualization, pagination — all from the shared
 *     DataTable. Consistent with every other table on the dashboard.
 */

import { DataTable } from '@/components/DataTable'

import { buildActiveColumns } from './queryColumns'
import type { ActiveOrPromotedRow, ActiveRow } from '../_types'

export function ActiveTable({
  rows,
  onRowClick,
  onKill,
  cancellingQid,
}: {
  rows: ActiveOrPromotedRow[]
  onRowClick: (row: ActiveOrPromotedRow) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
}) {
  const columns = buildActiveColumns({ onKill, cancellingQid })
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={false}
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      initialSorting={[{ id: 'duration_ms', desc: true }]}
      tableCaption="Active and just-finished queries"
    />
  )
}
