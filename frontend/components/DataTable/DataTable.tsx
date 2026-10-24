'use client'

import * as React from 'react'
import {
  Table,
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

import type { ColumnDef, Table as ReactTable } from '@tanstack/react-table'
import { DraggableTableHeader, StaticTableHeader } from './StaticHeader'
import { ColumnVisibilityDropdown } from './ColumnVisibilityDropdown'
import { DataTableBody } from './DataTableBody'
import { useDataTableState, type DataTableBaseProps } from './useDataTableState'
import { getApiBase } from '@/lib/api'
import type { Schema } from '@/types/api'
import { cn } from '@/lib/utils'

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
