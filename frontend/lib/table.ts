import {
  columnFilteringFeature, columnOrderingFeature, columnResizingFeature, columnSizingFeature, columnVisibilityFeature,
  createFilteredRowModel, createPaginatedRowModel, createSortedRowModel,
  filterFn_arrIncludes, filterFn_equals, filterFn_inDateRange, filterFn_inNumberRange, filterFn_includesString, filterFn_weakEquals,
  rowPaginationFeature, rowSelectionFeature, rowSortingFeature,
  sortFn_alphanumeric, sortFn_basic, sortFn_datetime, sortFn_text,
  tableFeatures,
  type CellContext as TableCellContext, type ColumnDef as TableColumnDef, type Header as TableHeader,
  type Row as TableRow, type RowData, type Table as TableInstance,
} from '@tanstack/react-table'

// Every feature, row model and fn the shared DataTable used under v8. The fn registries hold
// exactly the names 'auto' sorting and filtering can resolve to.
export const dataTableFeatures = tableFeatures({
  columnFilteringFeature, columnOrderingFeature, columnSizingFeature, columnResizingFeature, columnVisibilityFeature,
  rowPaginationFeature, rowSelectionFeature, rowSortingFeature,
  filteredRowModel: createFilteredRowModel(),
  sortedRowModel: createSortedRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  filterFns: {
    includesString: filterFn_includesString, equals: filterFn_equals, weakEquals: filterFn_weakEquals,
    arrIncludes: filterFn_arrIncludes, inNumberRange: filterFn_inNumberRange, inDateRange: filterFn_inDateRange,
  },
  sortFns: { alphanumeric: sortFn_alphanumeric, text: sortFn_text, datetime: sortFn_datetime, basic: sortFn_basic },
})
export type DataTableFeatures = typeof dataTableFeatures

// Plain render-only tables: core row model plus getVisibleCells, no sorting/filtering/pagination.
export const basicTableFeatures = tableFeatures({ columnVisibilityFeature })
export type BasicTableFeatures = typeof basicTableFeatures

export type ColumnDef<TData extends RowData, TValue = unknown> = TableColumnDef<DataTableFeatures, TData, TValue>
export type CellContext<TData extends RowData, TValue = unknown> = TableCellContext<DataTableFeatures, TData, TValue>
export type Header<TData extends RowData, TValue = unknown> = TableHeader<DataTableFeatures, TData, TValue>
export type Row<TData extends RowData> = TableRow<DataTableFeatures, TData>
export type Table<TData extends RowData> = TableInstance<DataTableFeatures, TData>

export type BasicColumnDef<TData extends RowData, TValue = unknown> = TableColumnDef<BasicTableFeatures, TData, TValue>
