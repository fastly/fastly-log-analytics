import React from 'react'
import { ColumnDef } from '@tanstack/react-table'
import { FilterValueCell } from '@/components/FilterValueCell'

const latencyHeader = (label: string) => () => (
  <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{label}</span>
)

/**
 * Creates standard performance/latency columns with a filter-value drill-down
 * cell (filter the current page or open the dashboard in a new tab).
 */
export const makeLatencyColumns = (labelField: string, labelName: string, filterField: string): ColumnDef<any>[] => [
  {
    accessorKey: labelField,
    header: latencyHeader(labelName),
    cell: (info: any) => {
      const val = info.row.original[filterField]
      const displayVal = info.getValue()
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
  { accessorKey: 'requests', header: latencyHeader('Reqs'), cell: (info: any) => info.getValue()?.toLocaleString() ?? '0' },
  { accessorKey: 'avg', header: latencyHeader('Avg (ms)'), cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p50', header: latencyHeader('P50'), cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p95', header: latencyHeader('P95'), cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p99', header: latencyHeader('P99'), cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' },
]
