'use client'

import * as React from 'react'
import {
  ColumnDef,
  ColumnFiltersState,
  SortingState,
  VisibilityState,
  ColumnOrderState,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table'

import { useVirtualizer } from '@tanstack/react-virtual'

import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

import {
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core'
import { restrictToHorizontalAxis } from '@dnd-kit/modifiers'
import {
  arrayMove,
  SortableContext,
  horizontalListSortingStrategy,
} from '@dnd-kit/sortable'

import { DraggableTableHeader } from './Header'
import { MemoizedTableRow } from './Body'
import { DataTableToolbar } from './Toolbar'

interface DataTableProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]
  data: TData[]
  searchKey?: string
  isLoading?: boolean
  showPagination?: boolean
  sorting?: SortingState
  onSortingChange?: (sorting: SortingState) => void
  initialSorting?: SortingState
  initialVisibility?: VisibilityState
  initialColumnOrder?: string[]
  title?: React.ReactNode
  compactToolbar?: boolean
  extraToolbarContent?: React.ReactNode
  renderToolbar?: (table: any) => React.ReactNode
  hideToolbar?: boolean
  columnVisibility?: VisibilityState
  onColumnVisibilityChange?: (visibility: VisibilityState) => void
  emptyMessage?: string
  onRowClick?: (row: TData) => void
  tableCaption?: string
}

function DataTableImpl<TData, TValue>({
  columns,
  data,
  searchKey,
  isLoading,
  showPagination = true,
  sorting: controlledSorting,
  onSortingChange,
  initialSorting = [],
  initialVisibility = {},
  initialColumnOrder,
  title,
  compactToolbar = false,
  extraToolbarContent,
  renderToolbar,
  hideToolbar = false,
  columnVisibility: controlledVisibility,
  onColumnVisibilityChange,
  emptyMessage = "No results.",
  onRowClick,
  tableCaption
}: DataTableProps<TData, TValue>) {
  const isControlled = controlledVisibility !== undefined
  const isSortingControlled = controlledSorting !== undefined
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(initialSorting)
  const sorting = isSortingControlled ? controlledSorting : internalSorting

  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])
  const [internalVisibility, setInternalVisibility] = React.useState<VisibilityState>(initialVisibility)
  const columnVisibility = isControlled ? controlledVisibility! : internalVisibility
  const setColumnVisibility = (updater: VisibilityState | ((prev: VisibilityState) => VisibilityState)) => {
    const next = typeof updater === 'function' ? updater(columnVisibility) : updater
    if (isControlled) {
      onColumnVisibilityChange?.(next)
    } else {
      setInternalVisibility(next)
    }
  }

  const handleSortingChange = (updater: SortingState | ((prev: SortingState) => SortingState)) => {
    const next = typeof updater === 'function' ? updater(sorting) : updater
    if (isSortingControlled) {
      onSortingChange?.(next)
    } else {
      setInternalSorting(next)
    }
  }

  // Column order: derived from the ``columns`` prop by default so it stays
  // in lockstep with dynamic column-set changes (e.g. sessions/page.tsx
  // adds ja4/edge/rtt cols only after data lands with has_* flags).
  //
  // The previous useState+useEffect pattern lagged one render — between
  // the columns change and the effect that synced ``columnOrder``,
  // ``columnOrder`` still held the OLD ID list. The MemoizedTableRow
  // captured the cells in that old order while header rendering used
  // the new columns prop, so headers and cells visibly misaligned on
  // /sessions and any other table that ships dynamic columns (user
  // report 2026-06-10).
  //
  // ``userColumnOrder`` is the drag-reorder override; it survives across
  // re-renders only while the column SET (the set of IDs, not the order)
  // is unchanged. Adding or removing a column invalidates the override
  // and we fall back to the columns-array order so headers and cells
  // can't desync.
  const defaultColumnOrder = React.useMemo<ColumnOrderState>(() => {
    const allIds = columns.map(
      (column) => column.id as string || (column as any).accessorKey as string,
    )
    if (initialColumnOrder && initialColumnOrder.length > 0) {
      const validInitial = initialColumnOrder.filter((id) => allIds.includes(id))
      const remaining = allIds.filter((id) => !validInitial.includes(id))
      return [...validInitial, ...remaining]
    }
    return allIds
  }, [columns, initialColumnOrder])

  const [userColumnOrder, setUserColumnOrder] = React.useState<ColumnOrderState | null>(null)

  const columnOrder = React.useMemo<ColumnOrderState>(() => {
    if (!userColumnOrder) return defaultColumnOrder
    if (userColumnOrder.length !== defaultColumnOrder.length) return defaultColumnOrder
    const userSet = new Set(userColumnOrder)
    for (const id of defaultColumnOrder) {
      if (!userSet.has(id)) return defaultColumnOrder
    }
    return userColumnOrder
  }, [userColumnOrder, defaultColumnOrder])

  // Adapter for TanStack's ``OnChangeFn<ColumnOrderState>`` contract — the
  // table calls it with either a ColumnOrderState or an updater function.
  // We collapse both forms into a concrete ColumnOrderState and store it
  // on ``userColumnOrder`` (which is nullable; the updater needs the
  // derived ``columnOrder`` as its "previous" basis when no user override
  // exists yet).
  const setColumnOrder = React.useCallback(
    (next: ColumnOrderState | ((prev: ColumnOrderState) => ColumnOrderState)) => {
      const resolved = typeof next === 'function' ? next(columnOrder) : next
      setUserColumnOrder(resolved)
    },
    [columnOrder],
  )

  const [rowSelection, setRowSelection] = React.useState({})
  const [pagination, setPagination] = React.useState({
    pageIndex: 0,
    pageSize: 50,
  })

  // Memoize column defs
  const tableColumns = React.useMemo(() => columns, [columns])
  const tableData = React.useMemo(() => data, [data])

  React.useEffect(() => {
    if (Object.keys(initialVisibility).length > 0) {
      setColumnVisibility(initialVisibility)
    }
  }, [initialVisibility])

  const table = useReactTable({
    data: tableData,
    columns: tableColumns,
    onSortingChange: handleSortingChange,
    manualSorting: isSortingControlled,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    onColumnOrderChange: setColumnOrder,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: isSortingControlled ? undefined : getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    columnResizeMode: 'onChange',
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      columnOrder,
      rowSelection,
      pagination,
    },
  })

  const tableContainerRef = React.useRef<HTMLDivElement>(null)

  const { rows } = table.getRowModel()
  
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => 40,
    overscan: 10,
  })

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 5 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
    useSensor(KeyboardSensor)
  )

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (active && over && active.id !== over.id) {
      // Resolve indices against the CURRENT derived ``columnOrder`` (never
      // null) rather than the previous override state (which can be null
      // before the user drags anything). Otherwise the first drag from a
      // fresh table would hit ``null.indexOf``.
      const oldIndex = columnOrder.indexOf(active.id as string)
      const newIndex = columnOrder.indexOf(over.id as string)
      if (oldIndex < 0 || newIndex < 0) return
      setColumnOrder(arrayMove(columnOrder, oldIndex, newIndex))
    }
  }

  const tableHeader = React.useMemo(() => (
    <TableHeader>
      {table.getHeaderGroups().map((headerGroup) => (
        <TableRow key={headerGroup.id}>
          <SortableContext
            items={columnOrder}
            strategy={horizontalListSortingStrategy}
          >
            {headerGroup.headers.map((header) => (
              <DraggableTableHeader key={header.id} header={header} />
            ))}
          </SortableContext>
        </TableRow>
      ))}
    </TableHeader>
  // columnVisibility is a dep because TanStack Table's `table` ref is stable
  // across visibility changes — without it the header would show stale columns.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ), [table, columnOrder, columnVisibility])

  return (
    <div className="w-full">
      {!hideToolbar && (renderToolbar ? (
        renderToolbar(table)
      ) : (
        <DataTableToolbar
          table={table}
          title={title}
          searchKey={searchKey}
          compactToolbar={compactToolbar}
          extraToolbarContent={extraToolbarContent}
        />
      ))}

      <DndContext
        id="data-table-dnd"
        collisionDetection={closestCenter}
        modifiers={[restrictToHorizontalAxis]}
        onDragEnd={handleDragEnd}
        sensors={sensors}
      >
        <div ref={tableContainerRef} className="rounded-md border overflow-auto w-full max-h-[600px]">
          <Table style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}>
            <caption className="sr-only">
              {tableCaption || (typeof title === 'string' ? title : 'Data Table')}
            </caption>
            {tableHeader}
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
                    <TableRow>
                      <TableCell colSpan={columns.length} style={{ height: rowVirtualizer.getVirtualItems()[0].start, padding: 0, border: 0 }} />
                    </TableRow>
                  )}
                  {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const row = rows[virtualRow.index]
                    return (
                      <MemoizedTableRow
                        key={row.id}
                        row={row}
                        onRowClick={onRowClick}
                        columnVisibility={columnVisibility}
                        columns={columns}
                      />
                    )
                  })}
                  {rowVirtualizer.getVirtualItems().length > 0 && rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end < rowVirtualizer.getTotalSize() && (
                    <TableRow>
                      <TableCell colSpan={columns.length} style={{ height: rowVirtualizer.getTotalSize() - rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end, padding: 0, border: 0 }} />
                    </TableRow>
                  )}
                </>
              ) : (
                <TableRow>
                  <TableCell
                    colSpan={columns.length}
                    className="h-24 text-center text-muted-foreground"
                  >
                    {emptyMessage}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </DndContext>


      {showPagination && table.getFilteredRowModel().rows.length >= 19 && (
        <div className="flex items-center justify-end px-4 py-4 border-t">
          <div className="flex items-center space-x-6 lg:space-x-8">
            <div className="flex items-center space-x-2">
              <p className="text-sm font-medium">Rows per page</p>
              <Select
                value={`${table.getState().pagination.pageSize}`}
                onValueChange={(value) => {
                  table.setPageSize(Number(value))
                }}
              >
                <SelectTrigger className="h-8 w-[70px]">
                  <SelectValue placeholder={table.getState().pagination.pageSize} />
                </SelectTrigger>
                <SelectContent side="top">
                  {[10, 20, 50, 100, 500].map((pageSize) => (
                    <SelectItem key={pageSize} value={`${pageSize}`}>
                      {pageSize}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex w-[100px] items-center justify-center text-sm font-medium">
              Page {table.getState().pagination.pageIndex + 1} of{" "}
              {table.getPageCount() || 1}
            </div>
            <div className="flex items-center space-x-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => table.previousPage()}
                disabled={!table.getCanPreviousPage()}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => table.nextPage()}
                disabled={!table.getCanNextPage()}
              >
                Next
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// Cast preserves the generic type signature while adding memoization.
// Without this, callers would lose type inference on `columns` and `data`.
export const DataTable = React.memo(DataTableImpl) as typeof DataTableImpl
