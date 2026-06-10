'use client'

import React from 'react'
import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { formatBytes } from '@/lib/utils'
import { type UsageLogEntry, fmtCost } from './shared'

export function buildUsageLogColumns(
  full: (ts: string) => string,
): ColumnDef<UsageLogEntry>[] {
  return [
    {
      accessorKey: 'timestamp',
      header: 'Timestamp',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">
          {full(row.original.timestamp)}
        </span>
      ),
    },
    {
      accessorKey: 'service_id',
      header: 'Service',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.service_id ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'operation_class',
      header: 'Class',
      cell: ({ row }) => {
        const cls = row.original.operation_class
        if (!cls) return <span className="text-muted-foreground text-xs">—</span>
        const variant = cls === 'A' ? 'default' : cls === 'B' ? 'secondary' : 'outline'
        return <Badge variant={variant} className="text-[10px] px-1.5 py-0 font-mono">{cls === 'CDN' ? 'CDN' : `FOS ${cls}`}</Badge>
      },
    },
    {
      accessorKey: 'operation_type',
      header: 'Operation',
      cell: ({ row }) => (
        <span className="font-mono text-xs">{row.original.operation_type ?? '—'}</span>
      ),
    },
    {
      accessorKey: 'url',
      header: 'URL / Path',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.url ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'bytes',
      header: 'Bytes',
      cell: ({ row }) => row.original.bytes != null
        ? <span className="font-mono text-xs tabular-nums">{formatBytes(row.original.bytes)}</span>
        : <span className="text-muted-foreground text-xs">—</span>,
    },
    {
      accessorKey: 'function_name',
      header: 'Function',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">{row.original.function_name ?? '—'}</span>
      ),
    },
    {
      accessorKey: 'process_context',
      header: 'Process',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.process_context ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'status',
      header: 'Status',
      cell: ({ row }) => {
        const s = row.original.status
        return <Badge variant={s === 'OK' ? 'secondary' : 'destructive'} className="text-[10px] px-1.5 py-0">{s ?? '—'}</Badge>
      },
    },
    {
      accessorKey: 'estimated_cost',
      header: 'Est. Cost',
      cell: ({ row }) => row.original.estimated_cost != null
        ? <span className="font-mono text-xs tabular-nums">{fmtCost(row.original.estimated_cost)}</span>
        : <span className="text-muted-foreground text-xs">—</span>,
    },
  ]
}
