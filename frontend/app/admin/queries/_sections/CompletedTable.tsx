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
 * Row clicks open the shared ``RowDetailDialog`` for the full SQL +
 * attribution view.
 */

import * as React from 'react'

import { DataTable } from '@/components/DataTable'

import { buildCompletedColumns } from './queryColumns'
import type { GroupedCompletedRow } from '../_types'

export function CompletedTable({
  rows,
  onRowClick,
  emptyMessage = 'No completed queries yet.',
  initialSorting,
}: {
  rows: GroupedCompletedRow[]
  onRowClick: (row: GroupedCompletedRow) => void
  emptyMessage?: string
  initialSorting?: { id: string; desc: boolean }[]
}) {
  const showMemory = rows.some((r) => r.peak_memory_mb !== null && r.peak_memory_mb !== undefined)
  const showService = rows.some((r) => r.service_id !== null && r.service_id !== undefined)
  const columns = React.useMemo(
    () => buildCompletedColumns({ showMemory, showService }),
    [showMemory, showService],
  )
  return (
    <DataTable<GroupedCompletedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={rows.length > 50}
      emptyMessage={emptyMessage}
      initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]}
      tableCaption="Completed queries"
    />
  )
}
