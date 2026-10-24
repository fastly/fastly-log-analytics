'use client'

import * as React from 'react'
import {
  Table,
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
import { StaticTableHeader } from './StaticHeader'
import { DataTableToolbar } from './Toolbar'
import { DataTableBody } from './DataTableBody'
import { DataTablePagination } from './DataTablePagination'
import { useDataTableState, type DataTableBaseProps } from './useDataTableState'
import { reportUxEvent } from '@/lib/ux-telemetry'
import { cn } from '@/lib/utils'

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
