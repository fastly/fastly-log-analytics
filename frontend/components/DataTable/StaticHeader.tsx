'use client'

import * as React from 'react'
import { flexRender } from '@tanstack/react-table'
import { ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react'

import { TableHead } from '@/components/ui/table'

// Read-only header — same sort affordance and aria-sort wiring as the
// DraggableTableHeader in DataTable, minus the @dnd-kit/useSortable
// hook (no column reorder) and the resize handle (callers using the
// readonly variant don't need either). Sortable columns still expose
// the toggle button + ArrowUp/Down/UpDown icon; non-sortable columns
// render the label without a focusable target.
export const StaticTableHeader = ({ header }: { header: any }) => {
  const ariaSort: 'ascending' | 'descending' | 'none' | undefined =
    header.column.getCanSort()
      ? header.column.getIsSorted() === 'asc' ? 'ascending'
      : header.column.getIsSorted() === 'desc' ? 'descending'
      : 'none'
      : undefined

  return (
    <TableHead
      style={{ width: header.column.getSize(), whiteSpace: 'nowrap' }}
      aria-sort={ariaSort}
      className="relative group select-none border-r last:border-r-0 px-0"
    >
      <div className="flex items-center justify-between gap-1 w-full h-full pl-3 pr-2 overflow-hidden">
        {header.column.getCanSort() && !header.isPlaceholder ? (
          <button
            type="button"
            className="flex-1 flex items-center hover:text-foreground transition-colors overflow-hidden cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm text-left bg-transparent border-0 p-0 font-inherit"
            onClick={header.column.getToggleSortingHandler()}
          >
            <span className="truncate">
              {flexRender(header.column.columnDef.header, header.getContext())}
            </span>
            <span className="ml-2 flex items-center shrink-0">
              {{
                asc: <ArrowUp className="w-3.5 h-3.5" />,
                desc: <ArrowDown className="w-3.5 h-3.5" />,
              }[header.column.getIsSorted() as string] ?? (
                <ArrowUpDown className="w-3.5 h-3.5 opacity-0 group-hover:opacity-50 transition-opacity" />
              )}
            </span>
          </button>
        ) : (
          <div className="flex-1 flex items-center overflow-hidden">
            <span className="truncate">
              {header.isPlaceholder
                ? null
                : flexRender(header.column.columnDef.header, header.getContext())}
            </span>
          </div>
        )}
      </div>
    </TableHead>
  )
}
