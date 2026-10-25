'use client'

import * as React from 'react'
import { flexRender, type Header, type ColumnDef, type Table as ReactTable } from '@tanstack/react-table'
import { ArrowDown, ArrowUp, ArrowUpDown, GripVertical } from 'lucide-react'
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

import {
  Table,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'
import { TimeAgo } from '@/components/TimeAgo'
import { ColumnVisibilityDropdown } from './ColumnVisibilityDropdown'
import { DataTableBody } from './DataTableBody'
import { useDataTableState, type DataTableBaseProps } from './useDataTableState'
import { getApiBase } from '@/lib/api'
import type { Schema } from '@/types/api'
import { cn } from '@/lib/utils'

export * from './ColumnVisibilityDropdown'

// ─── StaticHeader / DraggableTableHeader ─────────────────────────────────────

export function getHeaderAriaSort<TData>(
  header: Header<TData, unknown>,
): 'ascending' | 'descending' | 'none' | undefined {
  if (!header.column.getCanSort()) return undefined
  const sorted = header.column.getIsSorted()
  return sorted === 'asc' ? 'ascending' : sorted === 'desc' ? 'descending' : 'none'
}

export function HeaderCellContent<TData>({
  header,
  dragHandle,
}: {
  header: Header<TData, unknown>
  dragHandle?: React.ReactNode
}) {
  return (
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
      {dragHandle}
      <div
        role="presentation"
        onMouseDown={(e) => {
          try { header.getResizeHandler()(e) } catch { /* stale header */ }
        }}
        onTouchStart={(e) => {
          try { header.getResizeHandler()(e) } catch { /* stale header */ }
        }}
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
  const { attributes, isDragging, listeners, setNodeRef, transform, transition } = useSortable({
    id: header.column.id,
  })

  const style: React.CSSProperties = {
    opacity: isDragging ? 0.8 : 1,
    position: 'relative',
    transform: CSS.Translate.toString(transform),
    transition,
    whiteSpace: 'nowrap',
    width: header.column.getSize(),
    zIndex: isDragging ? 1 : 0,
  }

  return (
    <TableHead
      ref={setNodeRef}
      style={style}
      aria-sort={getHeaderAriaSort(header)}
      className={cn(
        'relative group select-none border-r last:border-r-0 px-0',
        isDragging && 'bg-muted shadow-md z-10',
      )}
    >
      <HeaderCellContent
        header={header}
        dragHandle={
          <div
            {...attributes}
            {...listeners}
            className="opacity-0 group-hover:opacity-50 hover:!opacity-100 cursor-grab active:cursor-grabbing p-1 rounded"
            title="Drag to reorder"
          >
            <GripVertical className="w-3.5 h-3.5" />
          </div>
        }
      />
    </TableHead>
  )
}

// ─── DateTimeCell ────────────────────────────────────────────────────────────

interface DateTimeCellProps {
  iso: string | null | undefined
  className?: string
  emptyFallback?: React.ReactNode
}

export function DateTimeCell({ iso, className, emptyFallback }: DateTimeCellProps) {
  // Absolute time (tooltip content) is static; the relative "X ago" trigger
  // ticks every second via the shared <TimeAgo> so table cells stop looking
  // frozen between query refetches. Only the inner text node re-renders.
  const { full, abbr } = useDateFormat()
  if (!iso) return <>{emptyFallback ?? <span className="text-muted-foreground/40">—</span>}</>
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className={
              className ??
              'text-muted-foreground whitespace-nowrap border-b border-dotted border-muted-foreground/30'
            }
          />
        }
      >
        <TimeAgo timestamp={iso} />
      </TooltipTrigger>
      <TooltipContent className="text-xs">
        {full(iso)} {abbr()}
      </TooltipContent>
    </Tooltip>
  )
}

// ─── DataTable ───────────────────────────────────────────────────────────────

interface DataTableToolbarProps<TData> {
  table: ReactTable<TData>
  title?: React.ReactNode
  searchKey?: string
  compactToolbar?: boolean
  extraToolbarContent?: React.ReactNode
}

export const DataTableToolbar = <TData,>({
  table,
  title,
  searchKey,
  compactToolbar = false,
  extraToolbarContent,
}: DataTableToolbarProps<TData>) => {
  return (
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
        <ColumnVisibilityDropdown table={table} compact={compactToolbar} />
      </div>
    </div>
  )
}

/**
 * Pagination footer shared by DataTable + DataTableReadonly.
 * The `>= 19` row-count gate and the [10, 20, 50, 100, 500] page
 * sizes are load-bearing — kept byte-identical to both former inline copies.
 */
export function DataTablePagination<TData>({ table }: { table: ReactTable<TData> }) {
  if (table.getFilteredRowModel().rows.length < 19) return null
  return (
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
          Page {table.getState().pagination.pageIndex + 1} of{' '}
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
  )
}

export function columnIdOf<TData, TValue>(c: ColumnDef<TData, TValue>): string {
  return (
    c.id ||
    (typeof (c as { accessorKey?: string }).accessorKey === 'string'
      ? (c as { accessorKey: string }).accessorKey
      : '')
  )
}

type UxEvent = Schema<'UxEventPayload'>

export function reportUxEvent(evt: UxEvent): void {
  if (typeof window === 'undefined') return
  try {
    const pathname = evt.pathname ?? window.location.pathname
    const body = JSON.stringify({
      event: evt.event,
      pathname,
      component_id: evt.component_id,
      details: evt.details ?? {},
    })
    const url = `${getApiBase()}/api/ux-events`

    if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator) {
      const blob = new Blob([body], { type: 'application/json' })
      if (navigator.sendBeacon(url, blob)) return
    }
    void fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      keepalive: true,
    })
  } catch {
    // Swallow — telemetry never breaks the page.
  }
}

export interface DataTableProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  tableContainerClassName?: string
  tableClassName?: string
  headerRowClassName?: string
  initialColumnOrder?: string[]
  enableColumnReorder?: boolean
}

function DataTableImpl<TData, TValue>({
  tableContainerClassName,
  tableClassName,
  headerRowClassName,
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
  enableColumnReorder = true,
  title,
  compactToolbar = false,
  extraToolbarContent,
  renderToolbar,
  hideToolbar = false,
  columnVisibility: controlledVisibility,
  onColumnVisibilityChange,
  emptyMessage = "No results.",
  emptyHint,
  onClearFilter,
  onRowClick,
  getRowClassName,
  tableCaption
}: DataTableProps<TData, TValue>) {
  const {
    table,
    rows,
    rowVirtualizer,
    tableContainerRef,
    columnVisibility,
    columnSizing,
    columnOrder,
    setColumnOrder,
  } = useDataTableState({
    columns,
    data,
    showPagination,
    controlledSorting,
    onSortingChange,
    initialSorting,
    controlledVisibility,
    onColumnVisibilityChange,
    initialVisibility,
    initialColumnOrder,
    enableColumnOrder: enableColumnReorder,
  })

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 5 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
    useSensor(KeyboardSensor)
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
        component_id:
          tableCaption ?? (typeof title === 'string' ? title : 'unnamed'),
        details: {
          column_id: String(active.id),
          from_index: oldIndex,
          to_index: newIndex,
          column_count: columnOrder.length,
        },
      })
    }
  }

  const tableContainer = (
    <div
      ref={tableContainerRef}
      tabIndex={enableColumnReorder ? undefined : 0}
      className={cn(
        "rounded-md border overflow-auto w-full",
        !enableColumnReorder && "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        tableContainerClassName || "max-h-[min(600px,calc(100dvh-200px))]",
      )}
    >
      <Table
        scrollFocusable={!enableColumnReorder}
        className={tableClassName}
        style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}
      >
        <caption className="sr-only">
          {tableCaption || (typeof title === 'string' ? title : 'Data Table')}
        </caption>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className={headerRowClassName}>
              {enableColumnReorder ? (
                <SortableContext
                  items={columnOrder}
                  strategy={horizontalListSortingStrategy}
                >
                  {headerGroup.headers.map((header) => (
                    <DraggableTableHeader key={header.id} header={header} />
                  ))}
                </SortableContext>
              ) : (
                headerGroup.headers.map((header) => (
                  <StaticTableHeader key={header.id} header={header} />
                ))
              )}
            </TableRow>
          ))}
        </TableHeader>
        <DataTableBody
          table={table}
          rows={rows}
          rowVirtualizer={rowVirtualizer}
          columns={columns}
          columnVisibility={columnVisibility}
          columnSizing={columnSizing}
          isLoading={isLoading}
          onRowClick={onRowClick}
          getRowClassName={getRowClassName}
          emptyMessage={emptyMessage}
          emptyHint={emptyHint}
          onClearFilter={onClearFilter}
        />
      </Table>
    </div>
  )

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

      {enableColumnReorder ? (
        <DndContext
          id="data-table-dnd"
          collisionDetection={closestCenter}
          modifiers={[restrictToHorizontalAxis]}
          onDragEnd={handleDragEnd}
          sensors={sensors}
        >
          {tableContainer}
        </DndContext>
      ) : (
        tableContainer
      )}

      {showPagination && <DataTablePagination table={table} />}
    </div>
  )
}

export const DataTable = React.memo(DataTableImpl) as typeof DataTableImpl

export type DataTableReadonlyProps<TData, TValue> = DataTableBaseProps<TData, TValue>

function DataTableReadonlyImpl<TData, TValue>({
  columns,
  data,
  searchKey,
  isLoading,
  showPagination = true,
  sorting: controlledSorting,
  onSortingChange,
  initialSorting = [],
  initialVisibility = {},
  title,
  compactToolbar = false,
  extraToolbarContent,
  renderToolbar,
  hideToolbar = false,
  columnVisibility: controlledVisibility,
  onColumnVisibilityChange,
  emptyMessage = 'No results.',
  emptyHint,
  onClearFilter,
  onRowClick,
  getRowClassName,
  tableCaption,
}: DataTableReadonlyProps<TData, TValue>) {
  const {
    table,
    rows,
    rowVirtualizer,
    tableContainerRef,
    columnVisibility,
    columnSizing,
  } = useDataTableState({
    columns,
    data,
    showPagination,
    controlledSorting,
    onSortingChange,
    initialSorting,
    controlledVisibility,
    onColumnVisibilityChange,
    initialVisibility,
  })

  return (
    <div className="w-full">
      {!hideToolbar &&
        (renderToolbar ? (
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

      <div
        ref={tableContainerRef}
        tabIndex={0}
        className="rounded-md border overflow-auto max-h-[min(600px,calc(100dvh-200px))] w-full focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Table
          scrollFocusable={false}
          style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}
        >
          <caption className="sr-only">
            {tableCaption || (typeof title === 'string' ? title : 'Data Table')}
          </caption>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <StaticTableHeader key={header.id} header={header} />
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <DataTableBody
            table={table}
            rows={rows}
            rowVirtualizer={rowVirtualizer}
            columns={columns}
            columnVisibility={columnVisibility}
            columnSizing={columnSizing}
            isLoading={isLoading}
            onRowClick={onRowClick}
            getRowClassName={getRowClassName}
            emptyMessage={emptyMessage}
            emptyHint={emptyHint}
            onClearFilter={onClearFilter}
          />
        </Table>
      </div>

      {showPagination && <DataTablePagination table={table} />}
    </div>
  )
}

export const DataTableReadonly = React.memo(DataTableReadonlyImpl) as typeof DataTableReadonlyImpl
