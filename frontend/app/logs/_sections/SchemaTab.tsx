'use client'

import React from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'

import { formatCountMaybe } from '@/lib/format'
import type { components } from '@/types/api.generated'

type SchemaColumn = components['schemas']['SchemaResponse']['schema'][number]

const hasStats = (col: SchemaColumn) => col.null_percentage !== undefined
const populatedPct = (col: SchemaColumn) =>
  col.null_percentage != null ? Math.max(0, 100 - col.null_percentage) : null

const SCHEMA_COLUMNS: ColumnDef<SchemaColumn>[] = [
  {
    id: 'name',
    header: 'Column Name',
    accessorFn: (c) => c.name,
    size: 220,
    cell: ({ row }) => <span className="font-mono text-xs font-bold">{row.original.name}</span>,
  },
  {
    id: 'type',
    header: 'DuckDB Type',
    accessorFn: (c) => c.type,
    size: 160,
    cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.type}</span>,
  },
  {
    id: 'populated',
    header: 'Populated %',
    accessorFn: (c) => populatedPct(c) ?? -1,
    size: 130,
    cell: ({ row }) => {
      const pct = populatedPct(row.original)
      const text = pct != null ? pct.toFixed(1) : '—'
      return (
        <span className="block font-mono text-xs text-right tabular-nums">
          {hasStats(row.original) ? (
            <span className={text === '0.0' ? 'text-muted-foreground/30' : ''}>{text}%</span>
          ) : '—'}
        </span>
      )
    },
  },
  {
    id: 'approx_unique',
    header: 'Approx Unique',
    accessorFn: (c) => c.approx_unique ?? -1,
    size: 140,
    cell: ({ row }) => (
      <span className="block font-mono text-xs text-right text-muted-foreground tabular-nums">
        {hasStats(row.original) ? formatCountMaybe(row.original.approx_unique) ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'min',
    header: 'Min Value',
    accessorFn: (c) => c.min ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.min ?? undefined}>
        {hasStats(row.original) ? row.original.min ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'max',
    header: 'Max Value',
    accessorFn: (c) => c.max ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.max ?? undefined}>
        {hasStats(row.original) ? row.original.max ?? '—' : '—'}
      </span>
    ),
  },
]

export function SchemaTab({
  schemaData,
  isLoadingSchema,
}: {
  schemaData: components['schemas']['SchemaResponse'] | undefined
  isLoadingSchema: boolean
}) {
  return (
    <>
      <div className="p-4 border-b flex justify-between items-center">
        <div>
          <h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3>
          <p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p>
        </div>
      </div>
      <DataTableReadonly
        columns={SCHEMA_COLUMNS}
        data={schemaData?.schema ?? []}
        isLoading={isLoadingSchema}
        hideToolbar
        showPagination={false}
        tableCaption="DuckDB Table Schema and Statistics"
      />
    </>
  )
}
