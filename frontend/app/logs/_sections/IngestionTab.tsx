'use client'

import React from 'react'
import { ChevronDown } from 'lucide-react'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { DataTable } from '@/components/DataTable'
import { Input } from '@/components/ui/input'
import { ColumnDef } from '@tanstack/react-table'

export function IngestionTab({
  ingestedColumns,
  ingestedFiles,
  isLoadingIngested,
}: {
  ingestedColumns: ColumnDef<any>[]
  ingestedFiles: any
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
            <DropdownMenu>
              <DropdownMenuTrigger className="inline-flex items-center justify-center whitespace-nowrap rounded-md text-xs font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-8 px-3 py-2">
                  Columns <ChevronDown className="ml-2 h-4 w-4" />
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                {table
                  .getAllColumns()
                  .filter((column: any) => column.getCanHide())
                  .map((column: any) => {
                    return (
                      <DropdownMenuCheckboxItem
                        key={column.id}
                        className="whitespace-nowrap"
                        checked={column.getIsVisible()}
                        onCheckedChange={(value) => column.toggleVisibility(!!value)}
                      >
                        {(column.columnDef.meta as any)?.label ??
                          (typeof column.columnDef.header === 'string'
                            ? column.columnDef.header
                            : column.id)}
                      </DropdownMenuCheckboxItem>
                    )
                  })}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      )}
    />
  )
}
