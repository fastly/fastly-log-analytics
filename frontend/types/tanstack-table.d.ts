import '@tanstack/react-table'

declare module '@tanstack/react-table' {
  // Column picker label shown instead of the (often JSX) header. The generics mirror the
  // library declaration so this merges into it.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<TData extends RowData, TValue> {
    label?: string
  }
}
