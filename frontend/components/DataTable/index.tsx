'use client'

import * as React from 'react'
import {
  flexRender, type Header, type ColumnDef, type ColumnFiltersState, type ColumnOrderState,
  type SortingState, type Row, type Table as ReactTable, type VisibilityState,
  getCoreRowModel, getFilteredRowModel, getPaginationRowModel, getSortedRowModel, useReactTable,
} from '@tanstack/react-table'
import { useVirtualizer, type Virtualizer } from '@tanstack/react-virtual'
import { ArrowDown, ArrowUp, ArrowUpDown, GripVertical, Inbox } from 'lucide-react'
import { DndContext, KeyboardSensor, MouseSensor, TouchSensor, closestCenter, useSensor, useSensors, type DragEndEvent } from '@dnd-kit/core'
import { restrictToHorizontalAxis } from '@dnd-kit/modifiers'
import { arrayMove, SortableContext, horizontalListSortingStrategy, useSortable } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { cn } from '@/lib/utils'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { getApiBase } from '@/lib/api'
import type { Schema } from '@/types/api'
import { ColumnVisibilityDropdown } from './DataTableCells'

export * from './DataTableCells'

export interface DataTableBaseProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]; data: TData[]; searchKey?: string; isLoading?: boolean; showPagination?: boolean
  sorting?: SortingState; onSortingChange?: (sorting: SortingState) => void; initialSorting?: SortingState; initialVisibility?: VisibilityState
  title?: React.ReactNode; compactToolbar?: boolean; extraToolbarContent?: React.ReactNode
  renderToolbar?: (table: ReactTable<TData>) => React.ReactNode; hideToolbar?: boolean
  columnVisibility?: VisibilityState; onColumnVisibilityChange?: (visibility: VisibilityState) => void
  emptyMessage?: string; emptyHint?: React.ReactNode; onClearFilter?: () => void
  onRowClick?: (row: TData) => void; getRowClassName?: (row: TData) => string; tableCaption?: string
}

interface UseDataTableStateArgs<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]; data?: TData[]; showPagination?: boolean
  controlledSorting?: SortingState; onSortingChange?: (sorting: SortingState) => void; initialSorting?: SortingState
  controlledVisibility?: VisibilityState; onColumnVisibilityChange?: (visibility: VisibilityState) => void; initialVisibility?: VisibilityState
  initialColumnOrder?: string[]; enableColumnOrder?: boolean
}

export function useDataTableState<TData, TValue>({
  columns, data = [], showPagination = true, controlledSorting, onSortingChange, initialSorting = [],
  controlledVisibility, onColumnVisibilityChange, initialVisibility = {}, initialColumnOrder, enableColumnOrder = false,
}: UseDataTableStateArgs<TData, TValue>) {
  const isControlled = controlledVisibility !== undefined
  const isSortingControlled = controlledSorting !== undefined
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(initialSorting)
  const sorting = isSortingControlled ? controlledSorting : internalSorting

  const [internalVisibility, setInternalVisibility] = React.useState<VisibilityState>(initialVisibility)
  const columnVisibility = isControlled ? controlledVisibility! : internalVisibility
  const setColumnVisibility = React.useCallback(
    (updater: VisibilityState | ((prev: VisibilityState) => VisibilityState)) => {
      if (isControlled) onColumnVisibilityChange?.(typeof updater === 'function' ? updater(controlledVisibility!) : updater)
      else setInternalVisibility(updater)
    },
    [isControlled, controlledVisibility, onColumnVisibilityChange],
  )

  const [stableData, setStableData] = React.useState<TData[]>(data)
  const isSameData = stableData === data || (Array.isArray(stableData) && Array.isArray(data) && stableData.length === data.length && stableData.every((row, i) => row === data[i]))
  if (!isSameData) setStableData(data)
  const tableData = isSameData ? stableData : data

  const nextVisibilityJson = JSON.stringify(initialVisibility)
  const [prevInitialVisibilityJson, setPrevInitialVisibilityJson] = React.useState(nextVisibilityJson)
  if (nextVisibilityJson !== prevInitialVisibilityJson) {
    setPrevInitialVisibilityJson(nextVisibilityJson)
    if (!isControlled && Object.keys(initialVisibility).length > 0) setInternalVisibility(initialVisibility)
  }

  const handleSortingChange = (updater: SortingState | ((prev: SortingState) => SortingState)) => {
    const next = typeof updater === 'function' ? updater(sorting) : updater
    if (isSortingControlled) onSortingChange?.(next)
    else setInternalSorting(next)
  }

  const defaultColumnOrder = React.useMemo<ColumnOrderState>(() => {
    if (!enableColumnOrder) return []
    const allIds = columns.map(columnIdOf)
    if (initialColumnOrder && initialColumnOrder.length > 0) {
      const validInitial = initialColumnOrder.filter((id) => allIds.includes(id))
      return [...validInitial, ...allIds.filter((id) => !validInitial.includes(id))]
    }
    return allIds
  }, [columns, initialColumnOrder, enableColumnOrder])

  const [userColumnOrder, setUserColumnOrder] = React.useState<ColumnOrderState | null>(null)
  const columnOrder = React.useMemo<ColumnOrderState>(() => {
    if (!userColumnOrder || userColumnOrder.length !== defaultColumnOrder.length) return defaultColumnOrder
    const userSet = new Set(userColumnOrder)
    for (const id of defaultColumnOrder) if (!userSet.has(id)) return defaultColumnOrder
    return userColumnOrder
  }, [userColumnOrder, defaultColumnOrder])

  const setColumnOrder = React.useCallback(
    (next: ColumnOrderState | ((prev: ColumnOrderState) => ColumnOrderState)) => {
      setUserColumnOrder(typeof next === 'function' ? next(columnOrder) : next)
    },
    [columnOrder],
  )

  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])
  const [rowSelection, setRowSelection] = React.useState({})
  const [pagination, setPagination] = React.useState({ pageIndex: 0, pageSize: 50 })
  const [columnSizing, setColumnSizing] = React.useState<Record<string, number>>({})
  const tableColumns = React.useMemo(() => columns, [columns])

  const table = useReactTable({
    data: tableData, columns: tableColumns, onSortingChange: handleSortingChange, manualSorting: isSortingControlled,
    onColumnFiltersChange: setColumnFilters, onPaginationChange: setPagination,
    ...(enableColumnOrder ? { onColumnOrderChange: setColumnOrder } : {}),
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: showPagination ? getPaginationRowModel() : undefined,
    getSortedRowModel: isSortingControlled ? undefined : getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onColumnVisibilityChange: setColumnVisibility, onRowSelectionChange: setRowSelection,
    columnResizeMode: 'onChange', onColumnSizingChange: setColumnSizing,
    state: {
      sorting, columnFilters, columnVisibility,
      ...(enableColumnOrder ? { columnOrder } : {}),
      rowSelection, pagination, columnSizing,
    },
  })

  const tableContainerRef = React.useRef<HTMLDivElement>(null)
  const { rows } = table.getRowModel()
  const rowVirtualizer = useVirtualizer({
    count: rows.length, getScrollElement: () => tableContainerRef.current, estimateSize: () => 40, overscan: 10,
  })

  return { table, rows, rowVirtualizer, tableContainerRef, columnVisibility, columnSizing, columnOrder, setColumnOrder }
}

// ─── DataTableBody / MemoizedTableRow ────────────────────────────────────────

interface TableRowProps<TData> {
  row: Row<TData>; onRowClick?: (row: TData) => void; rowClassName?: string
  columnVisibility: VisibilityState; columnSizing?: Record<string, number>
}

function TableRowImpl<TData>({ row, onRowClick, rowClassName }: TableRowProps<TData>) {
  return (
    <TableRow
      data-state={row.getIsSelected() && 'selected'}
      onClick={() => onRowClick?.(row.original)}
      tabIndex={onRowClick ? 0 : undefined}
      onKeyDown={onRowClick ? (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onRowClick(row.original)
        }
      } : undefined}
      className={cn(
        onRowClick && 'cursor-pointer hover:bg-muted/50 focus-visible:outline-none focus-visible:bg-muted/50 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring',
        rowClassName,
      )}
    >
      {row.getVisibleCells().map((cell) => {
        const meta = cell.column.columnDef.meta as { cellClassName?: string } | undefined
        return (
          <TableCell key={cell.id} style={{ width: cell.column.getSize(), maxWidth: cell.column.getSize() }} className={cn('truncate', meta?.cellClassName)}>
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </TableCell>
        )
      })}
    </TableRow>
  )
}

export const MemoizedTableRow = React.memo(TableRowImpl) as typeof TableRowImpl

interface DataTableBodyProps<TData, TValue> {
  table: ReactTable<TData>; rows: Row<TData>[]; rowVirtualizer: Virtualizer<HTMLDivElement, Element>
  columns: ColumnDef<TData, TValue>[]; columnVisibility: VisibilityState; columnSizing?: Record<string, number>
  isLoading?: boolean; onRowClick?: (row: TData) => void; getRowClassName?: (row: TData) => string
  emptyMessage?: string; emptyHint?: React.ReactNode; onClearFilter?: () => void
}

function VirtualSpacerRow({ colSpan, height }: { colSpan: number; height: number }) {
  if (height <= 0) return null
  return (
    <TableRow role="presentation" aria-hidden="true">
      <TableCell role="none" colSpan={colSpan} style={{ height, padding: 0, border: 0 }} />
    </TableRow>
  )
}

export function DataTableBody<TData, TValue>({
  table, rows, rowVirtualizer, columns, columnVisibility, columnSizing, isLoading,
  onRowClick, getRowClassName, emptyMessage = 'No results.', emptyHint, onClearFilter,
}: DataTableBodyProps<TData, TValue>) {
  const virtualItems = rowVirtualizer.getVirtualItems()
  const topPad = virtualItems.length > 0 ? virtualItems[0].start : 0
  const bottomPad = virtualItems.length > 0 ? rowVirtualizer.getTotalSize() - virtualItems[virtualItems.length - 1].end : 0

  return (
    <TableBody>
      {isLoading ? (
        <TableRow>
          <TableCell colSpan={columns.length} className="h-24 text-center text-muted-foreground">Loading...</TableCell>
        </TableRow>
      ) : table.getRowModel().rows?.length ? (
        <>
          <VirtualSpacerRow colSpan={columns.length} height={topPad} />
          {virtualItems.map((virtualRow) => {
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
          <VirtualSpacerRow colSpan={columns.length} height={bottomPad} />
        </>
      ) : (
        <TableRow>
          <TableCell colSpan={columns.length} className="h-32 text-center text-muted-foreground">
            <div className="flex flex-col items-center justify-center gap-2 py-4">
              <Inbox className="h-7 w-7 opacity-30" aria-hidden="true" />
              <p className="text-sm font-medium">{emptyMessage}</p>
              {emptyHint && <p className="text-xs text-muted-foreground">{emptyHint}</p>}
              {onClearFilter && <Button variant="outline" size="sm" className="mt-1" onClick={onClearFilter}>Clear filter</Button>}
            </div>
          </TableCell>
        </TableRow>
      )}
    </TableBody>
  )
}

// ─── StaticHeader / DraggableTableHeader ─────────────────────────────────────

export function getHeaderAriaSort<TData>(header: Header<TData, unknown>): 'ascending' | 'descending' | 'none' | undefined {
  if (!header.column.getCanSort()) return undefined
  const sorted = header.column.getIsSorted()
  return sorted === 'asc' ? 'ascending' : sorted === 'desc' ? 'descending' : 'none'
}

export function HeaderCellContent<TData>({ header, dragHandle }: { header: Header<TData, unknown>; dragHandle?: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-1 w-full h-full pl-3 pr-2 overflow-hidden">
      {header.column.getCanSort() && !header.isPlaceholder ? (
        <button
          type="button"
          className="flex-1 flex items-center hover:text-foreground transition-colors overflow-hidden cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm text-left bg-transparent border-0 p-0 font-inherit"
          onClick={header.column.getToggleSortingHandler()}
        >
          <span className="truncate">{flexRender(header.column.columnDef.header, header.getContext())}</span>
          <span className="ml-2 flex items-center shrink-0">
            {{ asc: <ArrowUp className="w-3.5 h-3.5" />, desc: <ArrowDown className="w-3.5 h-3.5" /> }[header.column.getIsSorted() as string] ?? (
              <ArrowUpDown className="w-3.5 h-3.5 opacity-0 group-hover:opacity-50 transition-opacity" />
            )}
          </span>
        </button>
      ) : (
        <div className="flex-1 flex items-center overflow-hidden">
          <span className="truncate">{header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}</span>
        </div>
      )}
      {dragHandle}
      <div
        role="presentation"
        onMouseDown={(e) => { try { header.getResizeHandler()(e) } catch { /* stale header */ } }}
        onTouchStart={(e) => { try { header.getResizeHandler()(e) } catch { /* stale header */ } }}
        className={cn(
          'absolute right-0 top-0 h-full w-2 cursor-col-resize hover:bg-primary/30 transition-colors z-10 touch-none',
          header.column.getIsResizing() ? 'bg-primary opacity-100' : 'opacity-0 group-hover:opacity-100',
        )}
      />
    </div>
  )
}

export const StaticTableHeader = <TData,>({ header }: { header: Header<TData, unknown> }) => (
  <TableHead
    style={{ width: header.column.getSize(), whiteSpace: 'nowrap' }}
    aria-sort={getHeaderAriaSort(header)}
    className="relative group select-none border-r last:border-r-0 px-0"
  >
    <HeaderCellContent header={header} />
  </TableHead>
)

export const DraggableTableHeader = <TData,>({ header }: { header: Header<TData, unknown> }) => {
  const { attributes, isDragging, listeners, setNodeRef, transform, transition } = useSortable({ id: header.column.id })
  return (
    <TableHead
      ref={setNodeRef}
      style={{
        opacity: isDragging ? 0.8 : 1, position: 'relative', transform: CSS.Translate.toString(transform),
        transition, whiteSpace: 'nowrap', width: header.column.getSize(), zIndex: isDragging ? 1 : 0,
      }}
      aria-sort={getHeaderAriaSort(header)}
      className={cn('relative group select-none border-r last:border-r-0 px-0', isDragging && 'bg-muted shadow-md z-10')}
    >
      <HeaderCellContent
        header={header}
        dragHandle={
          <div {...attributes} {...listeners} className="opacity-0 group-hover:opacity-50 hover:!opacity-100 cursor-grab active:cursor-grabbing p-1 rounded" title="Drag to reorder">
            <GripVertical className="w-3.5 h-3.5" />
          </div>
        }
      />
    </TableHead>
  )
}

// ─── DataTable ───────────────────────────────────────────────────────────────

interface DataTableToolbarProps<TData> {
  table: ReactTable<TData>; title?: React.ReactNode; searchKey?: string; compactToolbar?: boolean; extraToolbarContent?: React.ReactNode
}

export const DataTableToolbar = <TData,>({ table, title, searchKey, compactToolbar = false, extraToolbarContent }: DataTableToolbarProps<TData>) => (
  <div className={cn('flex items-center gap-4', compactToolbar ? 'mb-2' : 'py-4 px-4')}>
    {title && <div className="flex-1">{title}</div>}
    {searchKey && (
      <Input
        placeholder={`Filter ${searchKey}...`}
        value={(table.getColumn(searchKey)?.getFilterValue() as string) ?? ''}
        onChange={(event) => table.getColumn(searchKey)?.setFilterValue(event.target.value)}
        className="max-w-sm h-8"
      />
    )}
    {extraToolbarContent && <div className="flex items-center gap-2">{extraToolbarContent}</div>}
    <div className="ml-auto flex items-center gap-2">
      <ColumnVisibilityDropdown table={table} compact={compactToolbar} />
    </div>
  </div>
)

export function DataTablePagination<TData>({ table }: { table: ReactTable<TData> }) {
  if (table.getFilteredRowModel().rows.length < 19) return null
  return (
    <div className="flex items-center justify-end px-4 py-4 border-t">
      <div className="flex items-center space-x-6 lg:space-x-8">
        <div className="flex items-center space-x-2">
          <p className="text-sm font-medium">Rows per page</p>
          <Select value={`${table.getState().pagination.pageSize}`} onValueChange={(v) => table.setPageSize(Number(v))}>
            <SelectTrigger className="h-8 w-[70px]">
              <SelectValue placeholder={table.getState().pagination.pageSize} />
            </SelectTrigger>
            <SelectContent side="top">
              {[10, 20, 50, 100, 500].map((pageSize) => (
                <SelectItem key={pageSize} value={`${pageSize}`}>{pageSize}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex w-[100px] items-center justify-center text-sm font-medium">
          Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount() || 1}
        </div>
        <div className="flex items-center space-x-2">
          <Button variant="outline" size="sm" onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>Previous</Button>
          <Button variant="outline" size="sm" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>Next</Button>
        </div>
      </div>
    </div>
  )
}

export function columnIdOf<TData, TValue>(c: ColumnDef<TData, TValue>): string {
  return c.id || (typeof (c as { accessorKey?: string }).accessorKey === 'string' ? (c as { accessorKey: string }).accessorKey : '')
}

type UxEvent = Schema<'UxEventPayload'>

export function reportUxEvent(evt: UxEvent): void {
  if (typeof window === 'undefined') return
  try {
    const pathname = evt.pathname ?? window.location.pathname
    const body = JSON.stringify({ event: evt.event, pathname, component_id: evt.component_id, details: evt.details ?? {} })
    const url = `${getApiBase()}/api/ux-events`
    if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator && navigator.sendBeacon(url, new Blob([body], { type: 'application/json' }))) return
    void fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body, keepalive: true })
  } catch { /* Swallow — telemetry never breaks the page */ }
}

export interface DataTableProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  tableContainerClassName?: string; tableClassName?: string; headerRowClassName?: string
  initialColumnOrder?: string[]; enableColumnReorder?: boolean
}

function DataTableImpl<TData, TValue>({
  tableContainerClassName, tableClassName, headerRowClassName, columns, data, searchKey, isLoading,
  showPagination = true, sorting: controlledSorting, onSortingChange, initialSorting = [],
  initialVisibility = {}, initialColumnOrder, enableColumnReorder = true, title, compactToolbar = false,
  extraToolbarContent, renderToolbar, hideToolbar = false, columnVisibility: controlledVisibility,
  onColumnVisibilityChange, emptyMessage = 'No results.', emptyHint, onClearFilter, onRowClick,
  getRowClassName, tableCaption,
}: DataTableProps<TData, TValue>) {
  const {
    table, rows, rowVirtualizer, tableContainerRef, columnVisibility, columnSizing, columnOrder, setColumnOrder,
  } = useDataTableState({
    columns, data, showPagination, controlledSorting, onSortingChange, initialSorting,
    controlledVisibility, onColumnVisibilityChange, initialVisibility, initialColumnOrder, enableColumnOrder: enableColumnReorder,
  })

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 5 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
    useSensor(KeyboardSensor),
  )

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (active && over && active.id !== over.id) {
      const oldIndex = columnOrder.indexOf(active.id as string)
      const newIndex = columnOrder.indexOf(over.id as string)
      if (oldIndex < 0 || newIndex < 0) return
      setColumnOrder(arrayMove(columnOrder, oldIndex, newIndex))
      reportUxEvent({
        event: 'column_reordered',
        component_id: tableCaption ?? (typeof title === 'string' ? title : 'unnamed'),
        details: { column_id: String(active.id), from_index: oldIndex, to_index: newIndex, column_count: columnOrder.length },
      })
    }
  }

  const tableContainer = (
    <div
      ref={tableContainerRef}
      tabIndex={enableColumnReorder ? undefined : 0}
      className={cn(
        'rounded-md border overflow-auto w-full',
        !enableColumnReorder && 'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        tableContainerClassName || 'max-h-[min(600px,calc(100dvh-200px))]',
      )}
    >
      <Table scrollFocusable={false} className={tableClassName} style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}>
        <caption className="sr-only">{tableCaption || (typeof title === 'string' ? title : 'Data Table')}</caption>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className={headerRowClassName}>
              {enableColumnReorder ? (
                <SortableContext items={columnOrder} strategy={horizontalListSortingStrategy}>
                  {headerGroup.headers.map((header) => <DraggableTableHeader key={header.id} header={header} />)}
                </SortableContext>
              ) : (
                headerGroup.headers.map((header) => <StaticTableHeader key={header.id} header={header} />)
              )}
            </TableRow>
          ))}
        </TableHeader>
        <DataTableBody
          table={table} rows={rows} rowVirtualizer={rowVirtualizer} columns={columns}
          columnVisibility={columnVisibility} columnSizing={columnSizing} isLoading={isLoading}
          onRowClick={onRowClick} getRowClassName={getRowClassName} emptyMessage={emptyMessage}
          emptyHint={emptyHint} onClearFilter={onClearFilter}
        />
      </Table>
    </div>
  )

  return (
    <div className="w-full">
      {!hideToolbar && (renderToolbar ? renderToolbar(table) : (
        <DataTableToolbar table={table} title={title} searchKey={searchKey} compactToolbar={compactToolbar} extraToolbarContent={extraToolbarContent} />
      ))}
      {enableColumnReorder ? (
        <DndContext id="data-table-dnd" collisionDetection={closestCenter} modifiers={[restrictToHorizontalAxis]} onDragEnd={handleDragEnd} sensors={sensors}>
          {tableContainer}
        </DndContext>
      ) : tableContainer}
      {showPagination && <DataTablePagination table={table} />}
    </div>
  )
}

export const DataTable = React.memo(DataTableImpl) as typeof DataTableImpl
export type DataTableReadonlyProps<TData, TValue> = DataTableBaseProps<TData, TValue>
function DataTableReadonlyImpl<TData, TValue>(props: DataTableReadonlyProps<TData, TValue>) {
  return <DataTableImpl {...props} enableColumnReorder={false} />
}
export const DataTableReadonly = React.memo(DataTableReadonlyImpl) as typeof DataTableReadonlyImpl
