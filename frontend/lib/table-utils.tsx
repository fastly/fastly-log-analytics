import React from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { DateTimeCell } from '@/components/DataTable'
import { FilterValueCell } from '@/components/FilterValueCell'

import { formatBytes, formatCount, formatCountMaybe } from '@/lib/format'

export const ingestedFilesColumns: ColumnDef<components['schemas']['IngestedFile']>[] = [
  {
    accessorKey: 'file_name',
    id: 'file_name',
    meta: { label: 'File Name' },
    header: 'File Name',
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.file_name}</span>,
  },
  {
    accessorKey: 'ingested_at',
    id: 'ingested_at',
    meta: { label: 'Ingested At' },
    header: 'Ingested At',
    cell: ({ row }) => <DateTimeCell iso={row.original.ingested_at} />,
  },
  {
    accessorKey: 'row_count',
    id: 'row_count',
    meta: { label: 'Rows' },
    header: 'Rows',
    cell: ({ row }) => (
      <span className="font-mono text-muted-foreground tabular-nums text-xs">
        {formatCount(row.original.row_count || 0)}
      </span>
    ),
  },
  {
    accessorKey: 'file_size_bytes',
    id: 'file_size_bytes',
    meta: { label: 'Size' },
    header: 'Size',
    cell: ({ row }) => {
      const bytes = row.original.file_size_bytes
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {bytes ? formatBytes(bytes) : '—'}
        </span>
      )
    },
  },
]

export const columnHeader = (label: string) =>
  function ColumnHeaderCell() {
    return <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{label}</span>
  }

/** The numeric columns a latency table reads off each row. */
export interface LatencyRow {
  requests?: number | null
  avg?: number | null
  p50?: number | null
  p95?: number | null
  p99?: number | null
}

export const textValue = (v: unknown): string => (typeof v === 'string' ? v : '')
export const numberValue = (v: unknown): number | null | undefined => (typeof v === 'number' ? v : null)
export const msCell = (value: unknown) => <span>{numberValue(value)?.toFixed(1)}ms</span>

/**
 * Creates standard performance/latency columns with a filter-value drill-down
 * cell (filter the current page or open the dashboard in a new tab).
 */
export const makeLatencyColumns = <TRow extends LatencyRow>(
  labelField: keyof TRow & string,
  labelName: string,
  filterField: keyof TRow & string,
): ColumnDef<TRow>[] => [
  {
    accessorKey: labelField,
    header: columnHeader(labelName),
    cell: (info) => {
      const val: unknown = info.row.original[filterField]
      const rawDisplay = info.getValue()
      const displayVal = typeof rawDisplay === 'string' || typeof rawDisplay === 'number' ? rawDisplay : null
      if (val == null) {
        return <span className="font-mono text-xs truncate block max-w-[300px]">{displayVal}</span>
      }
      return (
        <FilterValueCell
          filters={[{ column: filterField, value: String(val) }]}
          display={displayVal}
          className="font-mono text-xs"
          containerClassName="max-w-[300px]"
        />
      )
    }
  },
  { accessorKey: 'requests', header: columnHeader('Reqs'), cell: (info) => formatCountMaybe(numberValue(info.getValue())) ?? '0' },
  { accessorKey: 'avg', header: columnHeader('Avg (ms)'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p50', header: columnHeader('P50'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p95', header: columnHeader('P95'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p99', header: columnHeader('P99'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
]
