'use client'

import * as React from 'react'
import { flexRender, VisibilityState } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

import { TableCell, TableRow } from '@/components/ui/table'

// Standard Cell Component (Cells don't need to be draggable, only headers do to set column order)
export const StandardTableCell = ({ cell }: { cell: any }) => {
  const isActions = cell.column.id === 'actions' || cell.column.id === 'selection'

  return (
    <TableCell
      className="pl-3 pr-2"
      style={{
        width: cell.column.getSize(),
      }}
    >
      <div className={cn(!isActions && "truncate")}>
        {flexRender(cell.column.columnDef.cell, cell.getContext())}
      </div>
    </TableCell>
  )
}

// Memoized Row Component to prevent redundant re-renders.
// columnVisibility is passed only so React.memo re-renders when visibility
// changes — TanStack Table's row references are stable across visibility
// updates, so without this prop the memo would return stale visible cells.
export const MemoizedTableRow = React.memo(({
  row,
  onRowClick,
  columns: _columns,
  columnVisibility: _columnVisibility,
}: {
  row: any,
  onRowClick?: (data: any) => void
  columns: any[]
  columnVisibility?: VisibilityState
}) => {
  return (
    <TableRow
      data-state={row.getIsSelected() && "selected"}
      className={cn(onRowClick && "cursor-pointer hover:bg-muted/50")}
      onClick={() => onRowClick && onRowClick(row.original)}
    >
      {row.getVisibleCells().map((cell: any) => (
        <StandardTableCell key={cell.id} cell={cell} />
      ))}
    </TableRow>
  )
})
MemoizedTableRow.displayName = 'MemoizedTableRow'
