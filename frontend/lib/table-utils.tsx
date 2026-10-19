import React from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { FilterValueCell } from '@/components/FilterValueCell'

import { formatCountMaybe } from '@/lib/format'

const latencyHeader = (label: string) =>
  function LatencyHeaderCell() {
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

const numberValue = (v: unknown): number | null | undefined => (typeof v === 'number' ? v : null)

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
    header: latencyHeader(labelName),
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
  { accessorKey: 'requests', header: latencyHeader('Reqs'), cell: (info) => formatCountMaybe(numberValue(info.getValue())) ?? '0' },
  { accessorKey: 'avg', header: latencyHeader('Avg (ms)'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p50', header: latencyHeader('P50'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p95', header: latencyHeader('P95'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p99', header: latencyHeader('P99'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
]
