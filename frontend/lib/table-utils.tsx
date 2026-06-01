import React from 'react'
import Link from 'next/link'
import { ExternalLink } from 'lucide-react'
import { ColumnDef } from '@tanstack/react-table'

/**
 * Creates standard performance/latency columns with a dashboard drill-down link.
 */
export const makeLatencyColumns = (labelField: string, labelName: string, filterField: string): ColumnDef<any>[] => [
  {
    accessorKey: labelField,
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{labelName}</span>,
    cell: (info: any) => {
      const val = info.row.original[filterField]
      const displayVal = info.getValue()
      return (
        <div className="flex items-center gap-2 group max-w-[300px]">
          <span className="font-mono text-xs truncate">{displayVal}</span>
          {val != null && (
            <Link
              href={`/dashboard?filter_${filterField}=${encodeURIComponent(val)}`}
              className="opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
              title="View in Dashboard"
              target="_blank"
              rel="noopener noreferrer"
            >
              <ExternalLink className="h-3 w-3 text-muted-foreground hover:text-primary" />
            </Link>
          )}
        </div>
      )
    }
  },
  { 
    accessorKey: 'requests', 
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Reqs</span>, 
    cell: (info: any) => info.getValue()?.toLocaleString() ?? '0' 
  },
  { 
    accessorKey: 'avg', 
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Avg (ms)</span>, 
    cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' 
  },
  { 
    accessorKey: 'p50', 
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P50</span>, 
    cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' 
  },
  { 
    accessorKey: 'p95', 
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P95</span>, 
    cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' 
  },
  { 
    accessorKey: 'p99', 
    header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P99</span>, 
    cell: (info: any) => info.getValue()?.toFixed(2) ?? '0.00' 
  },
]
