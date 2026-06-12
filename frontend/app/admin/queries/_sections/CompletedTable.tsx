'use client'

/**
 * Recently Completed + Notable Slow Queries panels — thin wrapper around
 * the project's standard ``<DataTable>``.
 *
 * Caller picks the row set; this component just renders. The Memory
 * column hides when every visible row has ``peak_memory_mb === null``
 * (SQLite rows always do; DuckDB rows can if the probe failed). Row
 * clicks open the shared ``RowDetailDialog`` for the full SQL +
 * attribution view.
 *
 * Replaces the prior custom HTML table with all the same data, plus
 * column reorder / hide-show / resize / sort from the shared DataTable.
 */

import { DataTable } from '@/components/DataTable'

import { buildCompletedColumns } from './queryColumns'
import type { CompletedRow } from '../_types'

export function CompletedTable({
  rows,
  onRowClick,
  emptyMessage = 'No completed queries yet.',
  initialSorting,
}: {
  rows: CompletedRow[]
  onRowClick: (row: CompletedRow) => void
  emptyMessage?: string
  /** Default for Slow Queries panel: duration desc. Recently-Completed
   *  passes ``[{id: 'duration_ms', desc: true}]`` or omits to use the
   *  default; either way DataTable's sort state is internal so the
   *  operator can re-sort from the headers. */
  initialSorting?: { id: string; desc: boolean }[]
}) {
  const showMemory = rows.some((r) => r.peak_memory_mb !== null && r.peak_memory_mb !== undefined)
  const columns = buildCompletedColumns({ showMemory })
  return (
    <DataTable<CompletedRow, unknown>
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
