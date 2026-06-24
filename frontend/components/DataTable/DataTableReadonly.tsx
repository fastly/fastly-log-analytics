'use client'

import * as React from 'react'
import {
  ColumnDef,
  ColumnFiltersState,
  SortingState,
  VisibilityState,
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

import { StaticTableHeader } from './StaticHeader'
import { DataTableToolbar } from './Toolbar'
import { DataTableBody } from './DataTableBody'
import { DataTablePagination } from './DataTablePagination'
import { useDataTableState } from './useDataTableState'

// Read-only variant of <DataTable>. Same TanStack Table-driven sorting,
// filtering, column visibility, pagination, and row virtualization —
// minus the drag-to-reorder columns + column-resize handles that pull
// the @dnd-kit/* tree in DataTable. Callers that don't actually need
// column reordering (status tables, single-purpose result lists) save
// ~25 KB gzip in their page bundle by importing this component
// directly:
//
//   import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'
//
// The default export from '@/components/DataTable' still re-exports
// DataTable for backwards compat — importing through the barrel pulls
// the full DataTable, which transitively pulls @dnd-kit, so the
// bundle-size win only applies when callers import from this file
// directly.
//
// API is intentionally a strict subset of DataTable's props. ``initial
// ColumnOrder`` is dropped (no column reorder) but every other prop
// behaves identically, so a migration is just swapping the import.

interface DataTableReadonlyProps<TData, TValue> {
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
  renderToolbar?: (table: any) => React.ReactNode
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
  const [rowSelection, setRowSelection] = React.useState({})
  const [pagination, setPagination] = React.useState({
    pageIndex: 0,
    pageSize: 50,
  })

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
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: isSortingControlled ? undefined : getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
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

  const tableHeader = React.useMemo(() => (
    <TableHeader>
      {table.getHeaderGroups().map((headerGroup) => (
        <TableRow key={headerGroup.id}>
          {headerGroup.headers.map((header) => (
            <StaticTableHeader key={header.id} header={header} />
          ))}
        </TableRow>
      ))}
    </TableHeader>
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ), [table, columnVisibility])

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
