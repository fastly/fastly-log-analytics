'use client'

import * as React from 'react'
import {
  ColumnDef,
  ColumnFiltersState,
  SortingState,
  VisibilityState,
  ColumnOrderState,
  ColumnResizeMode,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { ChevronDown, GripHorizontal, ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'

import { Button, buttonVariants } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Input } from '@/components/ui/input'
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
  useSortable,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

// Draggable Header Component
const DraggableTableHeader = ({ header }: { header: any }) => {
  const {
    attributes,
    isDragging,
    listeners,
    setNodeRef,
    transform,
    transition,
  } = useSortable({
    id: header.column.id,
  })

  const style: React.CSSProperties = {
    opacity: isDragging ? 1 : 1,
    transform: CSS.Translate.toString(transform),
    transition,
    whiteSpace: 'nowrap',
    width: header.column.getSize(),
    zIndex: isDragging ? 10 : 0,
    position: 'relative',
  }

  return (
    <TableHead 
      ref={setNodeRef} 
      style={style} 
      className={`relative z-0 group select-none border-r last:border-r-0 px-0 ${isDragging ? 'bg-accent shadow-md rounded-md ring-1 ring-border' : 'bg-transparent'}`}
    >
      <div className="flex items-center justify-between gap-1 w-full h-full pl-3 pr-2 overflow-hidden">
        <div 
          className={cn("flex-1 flex items-center hover:text-foreground transition-colors overflow-hidden", header.column.getCanSort() ? "cursor-pointer" : "")}
          onClick={header.column.getToggleSortingHandler()}
        >
          <span className="truncate">
            {header.isPlaceholder
              ? null
              : flexRender(
                  header.column.columnDef.header,
                  header.getContext()
                )}
          </span>
          {header.column.getCanSort() && !header.isPlaceholder && (
            <span className="ml-2 flex items-center shrink-0">
              {{
                asc: <ArrowUp className="w-3.5 h-3.5" />,
                desc: <ArrowDown className="w-3.5 h-3.5" />,
              }[header.column.getIsSorted() as string] ?? (
                <ArrowUpDown className="w-3.5 h-3.5 opacity-0 group-hover:opacity-50 transition-opacity" />
              )}
            </span>
          )}
        </div>
        <div 
          {...attributes} 
          {...listeners} 
          className="cursor-grab text-muted-foreground/30 hover:text-foreground active:cursor-grabbing p-1 rounded hover:bg-muted opacity-40 group-hover:opacity-100 transition-opacity shrink-0"
          title="Drag to reorder"
        >
          <GripHorizontal className="w-3.5 h-3.5" />
        </div>
        <div
          // Guard `getResizeHandler` against the column being removed
          // mid-render: when a column toggles off, the DOM header lingers
          // for one frame and tanstack-table's resize handler throws
          // "Column with id '<id>' does not exist" if the user happens to
          // touch the resize handle in that window. Lazy-call the handler
          // and swallow the lookup error.
          onMouseDown={(e) => {
            try { header.getResizeHandler()(e) } catch { /* stale header */ }
          }}
          onTouchStart={(e) => {
            try { header.getResizeHandler()(e) } catch { /* stale header */ }
          }}
          className={cn(
            "absolute right-0 top-0 h-full w-2 cursor-col-resize hover:bg-primary/30 transition-colors z-10 touch-none",
            header.column.getIsResizing() ? "bg-primary opacity-100" : "opacity-0 group-hover:opacity-100"
          )}
        />
      </div>
    </TableHead>  )
}

// Standard Cell Component (Cells don't need to be draggable, only headers do to set column order)
const StandardTableCell = ({ cell }: { cell: any }) => {
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
}

// Memoized Row Component to prevent redundant re-renders.
// columnVisibility is passed only so React.memo re-renders when visibility
// changes — TanStack Table's row references are stable across visibility
// updates, so without this prop the memo would return stale visible cells.
const MemoizedTableRow = React.memo(({
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
  onRowClick
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

  const [columnOrder, setColumnOrder] = React.useState<ColumnOrderState>([])
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

  // Ensure column order updates if columns array changes (e.g., dynamic queries), but respect initial order if provided initially
  React.useEffect(() => {
    if (initialColumnOrder && initialColumnOrder.length > 0) {
      // Find all column IDs
      const allIds = columns.map((column) => column.id as string || (column as any).accessorKey as string)
      // Filter initial order to only include valid IDs
      const validInitial = initialColumnOrder.filter(id => allIds.includes(id))
      // Append any remaining columns not in initialColumnOrder
      const remaining = allIds.filter(id => !validInitial.includes(id))
      setColumnOrder([...validInitial, ...remaining])
    } else {
      setColumnOrder(columns.map((column) => column.id as string || (column as any).accessorKey as string))
    }
  }, [columns, initialColumnOrder])

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

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 5 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
    useSensor(KeyboardSensor)
  )

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (active && over && active.id !== over.id) {
      setColumnOrder((columnOrder) => {
        const oldIndex = columnOrder.indexOf(active.id as string)
        const newIndex = columnOrder.indexOf(over.id as string)
        return arrayMove(columnOrder, oldIndex, newIndex)
      })
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
      <div className={cn("flex items-center gap-4", compactToolbar ? "mb-2" : "py-4 px-4")}>
        {title && (
          <div className="flex-1">{title}</div>
        )}
        {searchKey && (
          <Input
            placeholder={`Filter ${searchKey}...`}
            value={(table.getColumn(searchKey)?.getFilterValue() as string) ?? ""}
            onChange={(event) =>
              table.getColumn(searchKey)?.setFilterValue(event.target.value)
            }
            className="max-w-sm h-8"
          />
        )}
        {extraToolbarContent && (
          <div className="flex items-center gap-2">
            {extraToolbarContent}
          </div>
        )}
        <div className="ml-auto flex items-center gap-2">
          <DropdownMenu>
            <DropdownMenuTrigger
              className={buttonVariants({ variant: "outline", size: compactToolbar ? "sm" : "default", className: "h-8" })}
            >
              <span className={cn("flex items-center", compactToolbar && "text-xs")}>
                Columns <ChevronDown className="ml-2 h-4 w-4" />
              </span>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
              {table
                .getAllColumns()
                .filter((column) => column.getCanHide())
                .map((column) => {
                  return (
                    <DropdownMenuCheckboxItem
                      key={column.id}
                      className="whitespace-nowrap"
                      checked={column.getIsVisible()}
                      onCheckedChange={(value) =>
                        column.toggleVisibility(!!value)
                      }
                    >
                      {(column.columnDef.meta as any)?.label ?? (typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id)}
                    </DropdownMenuCheckboxItem>
                  )
                })}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
      ))}

      <DndContext
        id="data-table-dnd"
        collisionDetection={closestCenter}
        modifiers={[restrictToHorizontalAxis]}
        onDragEnd={handleDragEnd}
        sensors={sensors}
      >
        <div className="rounded-md border overflow-x-auto w-full">
          <Table style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}>
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
                table.getRowModel().rows.map((row) => (
                  <MemoizedTableRow
                    key={row.id}
                    row={row}
                    onRowClick={onRowClick}
                    columnVisibility={columnVisibility}
                    columns={columns}
                  />
                ))
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
