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
import { DataTableToolbar } from './Toolbar'
import { DataTableBody } from './DataTableBody'
import { DataTablePagination } from './DataTablePagination'
import { useDataTableState } from './useDataTableState'
import { reportUxEvent } from '@/lib/ux-telemetry'

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
  /** Optional secondary line shown beneath emptyMessage — usually a hint
   *  like "Try widening your time range" or "Adjust the FilterBar".
   *  Skipped when undefined so callers that don't opt in render the
   *  prior single-line empty state. */
  emptyHint?: React.ReactNode
  /** Optional clear-filter affordance shown in the empty state. When set,
   *  renders a small "Clear filter" button under the message; clicking
   *  fires this callback. Surfacing it on the empty row means users with
   *  an over-narrow filter aren't stuck staring at "No results." with no
   *  visible escape. */
  onClearFilter?: () => void
  onRowClick?: (row: TData) => void
  /** Optional per-row class hook. Receives the row's ``original`` data and
   *  returns a Tailwind class string (or empty). Lets callers tint live vs
   *  faded rows without forking the table component. Opt-in; tables that
   *  don't pass this prop render unchanged. */
  getRowClassName?: (row: TData) => string
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
  emptyHint,
  onClearFilter,
  onRowClick,
  getRowClassName,
  tableCaption
}: DataTableProps<TData, TValue>) {
  const { sorting, isSortingControlled, columnVisibility, setColumnVisibility, handleSortingChange } =
    useDataTableState({
      controlledSorting,
      onSortingChange,
      initialSorting,
      controlledVisibility,
      onColumnVisibilityChange,
      initialVisibility,
    })

  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])

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
    // The length + set-membership guard below is load-bearing: when a
    // dynamic column changes (add/remove), userColumnOrder still
    // references the OLD ids for one render. Without this check the
    // table renders stale ids and headers/cells desync until the next
    // userColumnOrder update.
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

  const tableColumns = React.useMemo(() => columns, [columns])
  // Stabilise the data reference. Callers commonly pass ``query?.rows || []``,
  // which is a BRAND-NEW array on every render while the query is
  // loading/empty. TanStack's autoResetPageIndex treats each new data identity
  // as a data change and fires setPagination → re-render → new array →
  // infinite loop ("Maximum update depth exceeded"), freezing the page (e.g.
  // the always-mounted Data Management tabs). Reuse the previous array whenever
  // the contents are referentially identical so autoReset still fires on a REAL
  // data change but not on an unstable-but-equal reference. Row arrays here are
  // bounded, so the per-change shallow scan is cheap.
  const prevDataRef = React.useRef(data)
  const tableData = React.useMemo(() => {
    const prev = prevDataRef.current
    if (
      Array.isArray(prev) &&
      Array.isArray(data) &&
      prev !== data &&
      prev.length === data.length &&
      prev.every((row, i) => row === data[i])
    ) {
      return prev
    }
    prevDataRef.current = data
    return data
  }, [data])

  React.useEffect(() => {
    if (Object.keys(initialVisibility).length > 0) {
      setColumnVisibility(initialVisibility)
    }
  }, [initialVisibility])

  const [columnSizing, setColumnSizing] = React.useState<Record<string, number>>({})

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
    onColumnSizingChange: setColumnSizing,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      columnOrder,
      rowSelection,
      pagination,
      columnSizing,
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
      // Fire a UX-telemetry event so we can later slice "which DataTable
      // callers actually get reordered" before deciding which of the
      // remaining ~25 callers should migrate to DataTableReadonly (which
      // drops @dnd-kit from the bundle). component_id falls back through
      // tableCaption → string-title → 'unnamed' so log slicing has a key
      // even when callers don't pass either.
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
  ), [table, columnOrder, columnVisibility, columnSizing])

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
        <div ref={tableContainerRef} className="rounded-md border overflow-auto w-full max-h-[min(600px,calc(100dvh-200px))]">
          <Table style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}>
            <caption className="sr-only">
              {tableCaption || (typeof title === 'string' ? title : 'Data Table')}
            </caption>
            {tableHeader}
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
      </DndContext>


      {showPagination && <DataTablePagination table={table} />}
    </div>
  )
}

// Cast preserves the generic type signature while adding memoization.
// Without this, callers would lose type inference on `columns` and `data`.
export const DataTable = React.memo(DataTableImpl) as typeof DataTableImpl
