'use client'

import React from 'react'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { DataTable } from '@/components/DataTable'
import { Clock, Database } from 'lucide-react'

interface ResultsTableProps {
  data: any
  isPending: boolean
  isStructured: boolean
  columns: ColumnDef<any>[]
  structuredSorting: SortingState
  onStructuredSortingChange: (sorting: SortingState) => void
}

/**
 * Results display: row-count/elapsed-time header plus the DataTable.
 *
 * Structured mode is server-sorted (the SortingState is the SQL
 * ORDER BY input), so we control DataTable's sorting prop.
 * Raw mode owns its own sort state internally — clicking a
 * column header re-orders the already-fetched rows client side
 * without rewriting the user's SQL.
 */
export function ResultsTable({
  data,
  isPending,
  isStructured,
  columns,
  structuredSorting,
  onStructuredSortingChange,
}: ResultsTableProps) {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4 text-xs text-muted-foreground px-1">
        <span className="flex items-center gap-1">
          <Database className="h-3 w-3" />
          {data.data?.length || 0} rows returned
          {data.truncated && (
            <span className="text-amber-500 font-semibold ml-1">
              {data.total_rows && data.total_rows > 0
                ? `(Truncated to ${data.data?.length} of ${data.total_rows.toLocaleString()})`
                : `(Truncated to ${data.data?.length} — more available; add LIMIT to count)`}
            </span>
          )}
        </span>
        <span className="flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {data.elapsed_ms}ms execution time
        </span>
      </div>

      <div className="border rounded-lg bg-card overflow-hidden">
        {isStructured ? (
          <DataTable
            columns={columns}
            data={data.data || []}
            isLoading={isPending}
            sorting={structuredSorting}
            onSortingChange={onStructuredSortingChange}
          />
        ) : (
          <DataTable
            columns={columns}
            data={data.data || []}
            isLoading={isPending}
            initialSorting={[{ id: 'timestamp', desc: true }]}
          />
        )}
      </div>
    </div>
  )
}
