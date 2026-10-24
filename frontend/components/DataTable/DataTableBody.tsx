'use client'

import * as React from 'react'
import { flexRender, type ColumnDef, type Row, type Table as TableInstance, type VisibilityState } from '@tanstack/react-table'
import type { Virtualizer } from '@tanstack/react-virtual'
import { Inbox } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { TableBody, TableCell, TableRow } from '@/components/ui/table'
import { cn } from '@/lib/utils'

interface TableRowProps<TData> {
  row: Row<TData>
  onRowClick?: (row: TData) => void
  rowClassName?: string
  columnVisibility: VisibilityState
  columnSizing?: Record<string, number>
}

function TableRowImpl<TData>({ row, onRowClick, rowClassName }: TableRowProps<TData>) {
  return (
    <TableRow
      data-state={row.getIsSelected() && 'selected'}
      onClick={() => onRowClick?.(row.original)}
      tabIndex={onRowClick ? 0 : undefined}
      onKeyDown={
        onRowClick
          ? (e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault()
                onRowClick(row.original)
              }
            }
          : undefined
      }
      className={cn(
        onRowClick &&
          'cursor-pointer hover:bg-muted/50 focus-visible:outline-none focus-visible:bg-muted/50 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring',
        rowClassName,
      )}
    >
      {row.getVisibleCells().map((cell) => {
        const meta = cell.column.columnDef.meta as { cellClassName?: string } | undefined
        return (
          <TableCell
            key={cell.id}
            style={{ width: cell.column.getSize(), maxWidth: cell.column.getSize() }}
            className={cn('truncate', meta?.cellClassName)}
          >
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </TableCell>
        )
      })}
    </TableRow>
  )
}

export const MemoizedTableRow = React.memo(TableRowImpl) as typeof TableRowImpl

interface DataTableBodyProps<TData, TValue> {
  table: TableInstance<TData>
  rows: Row<TData>[]
  rowVirtualizer: Virtualizer<HTMLDivElement, Element>
  columns: ColumnDef<TData, TValue>[]
  columnVisibility: VisibilityState
  columnSizing?: Record<string, number>
  isLoading?: boolean
  onRowClick?: (row: TData) => void
  getRowClassName?: (row: TData) => string
  emptyMessage?: string
  emptyHint?: React.ReactNode
  onClearFilter?: () => void
}

/**
 * The `<TableBody>` content shared verbatim by DataTable and
 * DataTableReadonly: the loading row, the virtualized row window with its
 * top/bottom presentation spacers, and the empty state (Inbox icon + message
 * + optional hint + optional Clear-filter CTA). Pulls no @dnd-kit
 * (MemoizedTableRow lives in ./Body, which is @dnd-kit-free), so the readonly
 * variant stays lean.
 */
export function DataTableBody<TData, TValue>({
  table,
  rows,
  rowVirtualizer,
  columns,
  columnVisibility,
  columnSizing,
  isLoading,
  onRowClick,
  getRowClassName,
  emptyMessage = 'No results.',
  emptyHint,
  onClearFilter,
}: DataTableBodyProps<TData, TValue>) {
  return (
    <TableBody>
      {isLoading ? (
        <TableRow>
          <TableCell
            colSpan={columns.length}
            className="h-24 text-center text-muted-foreground"
          >
            Loading...
          </TableCell>
        </TableRow>
      ) : table.getRowModel().rows?.length ? (
        <>
          {rowVirtualizer.getVirtualItems().length > 0 && rowVirtualizer.getVirtualItems()[0].start > 0 && (
            // Top virtualization spacer — pure layout, not a data row.
            // role="presentation" + aria-hidden hides it from AT row count
            // (otherwise SR says "row 1 of N+2").
            <TableRow role="presentation" aria-hidden="true">
              <TableCell role="none" colSpan={columns.length} style={{ height: rowVirtualizer.getVirtualItems()[0].start, padding: 0, border: 0 }} />
            </TableRow>
          )}
          {rowVirtualizer.getVirtualItems().map((virtualRow) => {
            const row = rows[virtualRow.index]
            return (
              <MemoizedTableRow
                key={row.id}
                row={row}
                onRowClick={onRowClick}
                rowClassName={getRowClassName ? getRowClassName(row.original) : undefined}
                columnVisibility={columnVisibility}
                columnSizing={columnSizing}
              />
            )
          })}
          {rowVirtualizer.getVirtualItems().length > 0 && rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end < rowVirtualizer.getTotalSize() && (
            // Bottom virtualization spacer — same a11y reasoning as the top.
            <TableRow role="presentation" aria-hidden="true">
              <TableCell role="none" colSpan={columns.length} style={{ height: rowVirtualizer.getTotalSize() - rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end, padding: 0, border: 0 }} />
            </TableRow>
          )}
        </>
      ) : (
        <TableRow>
          <TableCell
            colSpan={columns.length}
            className="h-32 text-center text-muted-foreground"
          >
            <div className="flex flex-col items-center justify-center gap-2 py-4">
              <Inbox className="h-7 w-7 opacity-30" aria-hidden="true" />
              <p className="text-sm font-medium">{emptyMessage}</p>
              {emptyHint && (
                <p className="text-xs text-muted-foreground">{emptyHint}</p>
              )}
              {onClearFilter && (
                <Button variant="outline" size="sm" className="mt-1" onClick={onClearFilter}>
                  Clear filter
                </Button>
              )}
            </div>
          </TableCell>
        </TableRow>
      )}
    </TableBody>
  )
}
