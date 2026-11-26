'use client'

import React from 'react'
import type { SortingState } from '@tanstack/react-table'
import type { ColumnDef } from '@/lib/table'
import { DataTable } from '@/components/DataTable'
import { Database, Clock } from 'lucide-react'
import { formatCount, cn } from '@/lib/utils'
import type { components } from '@/types/api.generated'

type QueryResponse = components['schemas']['QueryResponse']

export function ResultsTable({
  data,
  isPending,
  isStructured,
  columns,
  structuredSorting,
  onStructuredSortingChange,
}: {
  data: QueryResponse
  isPending: boolean
  isStructured: boolean
  columns: ColumnDef<Record<string, unknown>>[]
  structuredSorting: SortingState
  onStructuredSortingChange: (sorting: SortingState) => void
}) {
  return (
    <div
      className={cn('space-y-4 transition-opacity duration-150', isPending && 'opacity-60')}
      data-testid="query-results-table"
    >
      <div className="flex items-center gap-4 text-xs text-muted-foreground px-1">
        <span className="flex items-center gap-1">
          <Database className="h-3 w-3" />
          {data.data?.length || 0} rows returned
          {data.truncated && (
            <span className="text-amber-500 font-semibold ml-1">
              {data.total_rows && data.total_rows > 0
                ? `(Truncated to ${data.data?.length} of ${formatCount(data.total_rows)})`
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
        <DataTable
          tableCaption="Log Explorer Data Results"
          columns={columns}
          data={data.data || []}
          isLoading={isPending}
          {...(isStructured
            ? { sorting: structuredSorting, onSortingChange: onStructuredSortingChange }
            : { initialSorting: [{ id: 'timestamp', desc: true }] })}
        />
      </div>
    </div>
  )
}
