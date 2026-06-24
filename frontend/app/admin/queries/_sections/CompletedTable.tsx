'use client'

/**
 * Recently Completed + Notable Slow Queries panels — thin wrapper around
 * the project's standard ``<DataTable>``.
 *
 * Service + Memory columns auto-hide when every visible row has the value
 * empty. SQLite rows always have no Memory (probe is DuckDB-only); rows
 * from connections that bypass ``get_con`` (rare, but possible) have no
 * Service. Hiding empty columns keeps the table compact.
 *
 * Cron-groups: ×N badge in the Source cell toggles per-run expansion;
 * sibling rows render with a muted-bg + left indent.
 *
 * Row clicks open the shared ``RowDetailDialog`` for the full SQL +
 * attribution view.
 */

import * as React from 'react'

// Direct import (not via the barrel) so the /admin/queries bundle
// drops the @dnd-kit tree that the reorder-enabled DataTable pulls
// in.
import { DataTableReadonly as DataTable } from '@/components/DataTable/DataTableReadonly'

import { buildCompletedColumns } from './queryColumns'
import type { GroupedCompletedRow } from '../_types'

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
      getRowClassName={rowClassName}
      hideToolbar
      showPagination={rows.length > 50}
      emptyMessage={emptyMessage}
      initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]}
      tableCaption="Completed queries"
    />
  )
}

function rowClassName(row: GroupedCompletedRow): string {
  if (row._expandedChild) return 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
  return ''
}
