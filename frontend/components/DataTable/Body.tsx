'use client'

import * as React from 'react'
import { flexRender, type Cell, type ColumnDef, type Row, type VisibilityState } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

import { TableCell, TableRow } from '@/components/ui/table'

// Standard Cell Component (Cells don't need to be draggable, only headers do to set column order)
const StandardTableCell = <TData,>({ cell }: { cell: Cell<TData, unknown> }) => {
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
function TableRowImpl<TData, TValue>({
  row,
  onRowClick,
  rowClassName,
  columns: _columns,
  columnVisibility: _columnVisibility,
  columnSizing: _columnSizing,
}: {
  row: Row<TData>
  onRowClick?: (data: TData) => void
  rowClassName?: string
  columns: ColumnDef<TData, TValue>[]
  columnVisibility?: VisibilityState
  columnSizing?: Record<string, number>
}) {
  return (
    <TableRow
      data-state={row.getIsSelected() && "selected"}
      className={cn(onRowClick && "cursor-pointer hover:bg-muted/50", rowClassName)}
      onClick={() => onRowClick && onRowClick(row.original)}
    >
      {row.getVisibleCells().map((cell) => (
        <StandardTableCell key={cell.id} cell={cell} />
      ))}
    </TableRow>
  )
}
export const MemoizedTableRow = React.memo(TableRowImpl) as typeof TableRowImpl
;(MemoizedTableRow as React.FunctionComponent).displayName = 'MemoizedTableRow'
