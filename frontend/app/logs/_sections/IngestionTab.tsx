'use client'

import React from 'react'
// Direct import (not via the barrel) so the page bundle drops the
// @dnd-kit tree that the reorder-enabled DataTable pulls in.
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { ColumnVisibilityDropdown } from '@/components/DataTable/ColumnVisibilityDropdown'
import { Input } from '@/components/ui/input'
import { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'

type IngestedFile = components['schemas']['IngestedFile']

export function IngestionTab({
  ingestedColumns,
  ingestedFiles,
  isLoadingIngested,
}: {
  ingestedColumns: ColumnDef<IngestedFile>[]
  ingestedFiles: components['schemas']['IngestedFilesResponse'] | undefined
  isLoadingIngested: boolean
}) {
  return (
    <DataTable
      columns={ingestedColumns}
      data={ingestedFiles?.files || []}
      isLoading={isLoadingIngested}
      searchKey="file_name"
      initialSorting={[{ id: 'ingested_at', desc: true }]}
      renderToolbar={(table) => (
        <div className="p-4 border-b flex flex-wrap items-center justify-between gap-4">
          <h3 className="text-sm font-medium">Log Ingestion History</h3>
          <div className="flex items-center gap-2 ml-auto">
            <Input
              placeholder="Filter by filename..."
              value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''}
              onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)}
              className="max-w-sm h-8"
            />
            <ColumnVisibilityDropdown table={table} compact />
          </div>
        </div>
      )}
    />
  )
}
