import '@tanstack/react-table'

declare module '@tanstack/react-table' {
  // Column picker label shown instead of the (often JSX) header. The generics mirror the
  // library declaration so this merges into it.
  interface ColumnMeta<TData extends RowData, TValue> {
    label?: string
  }
}
