'use client'

import * as React from 'react'
import { flexRender, VisibilityState } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

import { TableCell, TableRow } from '@/components/ui/table'

// Standard Cell Component (Cells don't need to be draggable, only headers do to set column order)
const StandardTableCell = ({ cell }: { cell: any }) => {
  const isActions = cell.column.id === 'actions'

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
//
// ``rowClassName`` is an optional per-row class derived from the row data.
// Opt-in — callers that don't pass it get the same behaviour as before, so
// existing consumers are unaffected. Used by the Live Query Monitor to
// tint live rows vs faded just-finished rows (the prior custom HTML table
// had row-level styling; DataTable cells can carry colour but the whole-
// row tint was lost in the move). Pass via DataTable's ``getRowClassName``
// prop.
export const MemoizedTableRow = React.memo(({
  row,
  onRowClick,
  rowClassName,
  columns: _columns,
  columnVisibility: _columnVisibility,
}: {
  row: any,
  onRowClick?: (data: any) => void
  rowClassName?: string
  columns: any[]
  columnVisibility?: VisibilityState
}) => {
  return (
    <TableRow
      data-state={row.getIsSelected() && "selected"}
      className={cn(onRowClick && "cursor-pointer hover:bg-muted/50", rowClassName)}
      onClick={() => onRowClick && onRowClick(row.original)}
    >
      {row.getVisibleCells().map((cell: any) => (
        <StandardTableCell key={cell.id} cell={cell} />
      ))}
    </TableRow>
  )
})
MemoizedTableRow.displayName = 'MemoizedTableRow'
