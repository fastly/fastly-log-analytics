'use client'

import * as React from 'react'
import {
  type ColumnDef,
  type ColumnFiltersState,
  type ColumnOrderState,
  type SortingState,
  type Table as TableInstance,
  type VisibilityState,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import { columnIdOf } from './index'

export interface DataTableBaseProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]
  data: TData[]
  searchKey?: string
  isLoading?: boolean
  showPagination?: boolean
  sorting?: SortingState
  onSortingChange?: (sorting: SortingState) => void
  initialSorting?: SortingState
  initialVisibility?: VisibilityState
  title?: React.ReactNode
  compactToolbar?: boolean
  extraToolbarContent?: React.ReactNode
  renderToolbar?: (table: TableInstance<TData>) => React.ReactNode
  hideToolbar?: boolean
  columnVisibility?: VisibilityState
  onColumnVisibilityChange?: (visibility: VisibilityState) => void
  emptyMessage?: string
  emptyHint?: React.ReactNode
  onClearFilter?: () => void
  onRowClick?: (row: TData) => void
  getRowClassName?: (row: TData) => string
  tableCaption?: string
}

interface UseDataTableStateArgs<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]
  data?: TData[]
  showPagination?: boolean
  controlledSorting?: SortingState
  onSortingChange?: (sorting: SortingState) => void
  initialSorting?: SortingState
  controlledVisibility?: VisibilityState
  onColumnVisibilityChange?: (visibility: VisibilityState) => void
  initialVisibility?: VisibilityState
  initialColumnOrder?: string[]
  enableColumnOrder?: boolean
}

/**
 * Shared TanStack Table + virtualizer state for DataTable and DataTableReadonly.
 * Pulls no @dnd-kit, so importing it into the readonly variant keeps that bundle lean.
 */
export function useDataTableState<TData, TValue>({
  columns,
  data = [],
  showPagination = true,
  controlledSorting,
  onSortingChange,
  initialSorting = [],
  controlledVisibility,
  onColumnVisibilityChange,
  initialVisibility = {},
  initialColumnOrder,
  enableColumnOrder = false,
}: UseDataTableStateArgs<TData, TValue>) {
  const isControlled = controlledVisibility !== undefined
  const isSortingControlled = controlledSorting !== undefined
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(initialSorting)
  const sorting = isSortingControlled ? controlledSorting : internalSorting

  const [internalVisibility, setInternalVisibility] = React.useState<VisibilityState>(initialVisibility)
  const columnVisibility = isControlled ? controlledVisibility! : internalVisibility
  const setColumnVisibility = React.useCallback(
    (updater: VisibilityState | ((prev: VisibilityState) => VisibilityState)) => {
      if (isControlled) {
        const next = typeof updater === 'function' ? updater(controlledVisibility!) : updater
        onColumnVisibilityChange?.(next)
      } else {
        setInternalVisibility(updater)
      }
    },
    [isControlled, controlledVisibility, onColumnVisibilityChange],
  )

  const [stableData, setStableData] = React.useState<TData[]>(data)
  const isSameData =
    stableData === data ||
    (Array.isArray(stableData) &&
      Array.isArray(data) &&
      stableData.length === data.length &&
      stableData.every((row, i) => row === data[i]))
  if (!isSameData) {
    setStableData(data)
  }
  const tableData = isSameData ? stableData : data

  const nextVisibilityJson = JSON.stringify(initialVisibility)
  const [prevInitialVisibilityJson, setPrevInitialVisibilityJson] = React.useState(nextVisibilityJson)
  if (nextVisibilityJson !== prevInitialVisibilityJson) {
    setPrevInitialVisibilityJson(nextVisibilityJson)
    if (!isControlled && Object.keys(initialVisibility).length > 0) {
      setInternalVisibility(initialVisibility)
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

  const defaultColumnOrder = React.useMemo<ColumnOrderState>(() => {
    if (!enableColumnOrder) return []
    const allIds = columns.map(columnIdOf)
    if (initialColumnOrder && initialColumnOrder.length > 0) {
      const validInitial = initialColumnOrder.filter((id) => allIds.includes(id))
      const remaining = allIds.filter((id) => !validInitial.includes(id))
      return [...validInitial, ...remaining]
    }
    return allIds
  }, [columns, initialColumnOrder, enableColumnOrder])

  const [userColumnOrder, setUserColumnOrder] = React.useState<ColumnOrderState | null>(null)

  const columnOrder = React.useMemo<ColumnOrderState>(() => {
    if (!userColumnOrder || userColumnOrder.length !== defaultColumnOrder.length) return defaultColumnOrder
    const userSet = new Set(userColumnOrder)
    for (const id of defaultColumnOrder) {
      if (!userSet.has(id)) return defaultColumnOrder
    }
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
    data: tableData,
    columns: tableColumns,
    onSortingChange: handleSortingChange,
    manualSorting: isSortingControlled,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    ...(enableColumnOrder ? { onColumnOrderChange: setColumnOrder } : {}),
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: showPagination ? getPaginationRowModel() : undefined,
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
      ...(enableColumnOrder ? { columnOrder } : {}),
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

  return {
    table,
    rows,
    rowVirtualizer,
    tableContainerRef,
    columnVisibility,
    columnSizing,
    columnOrder,
    setColumnOrder,
  }
}
