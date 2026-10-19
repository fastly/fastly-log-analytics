import type { ColumnDef } from '@tanstack/react-table'

/** A column's id: its explicit `id`, else the `accessorKey` it reads. */
export function columnIdOf<TData, TValue = unknown>(column: ColumnDef<TData, TValue>): string {
  return column.id ?? ('accessorKey' in column ? String(column.accessorKey) : '')
}
