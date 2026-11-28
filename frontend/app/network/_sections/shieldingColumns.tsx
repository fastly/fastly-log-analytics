'use client'

import React from 'react'
import type { ColumnDef } from '@/lib/table'
import {
  FilterValueCell,
  PopLabel,
  columnHeader,
  msCell,
  numberValue,
  textValue,
} from '@/components/DataTable/DataTableCells'
import { cn, formatCount } from '@/lib/utils'
import type { ShieldingRowInput } from './shieldingConfig'

export const SHIELDING_COLUMNS: ColumnDef<ShieldingRowInput>[] = [
  {
    accessorKey: 'edge_pop', id: 'edge_pop', meta: { label: 'Edge POP' }, header: columnHeader('Edge POP'),
    cell: (info) => (
      <FilterValueCell
        filters={[{ column: 'pop', value: textValue(info.getValue()) }]}
        display={<PopLabel code={textValue(info.getValue())} />}
        className={cn('font-bold', info.row.original.anomaly_static ? 'text-destructive' : '')}
      />
    ),
  },
  {
    accessorKey: 'shield_pop', id: 'shield_pop', meta: { label: 'Shield POP' }, header: columnHeader('Shield POP'),
    cell: (info) => (
      <FilterValueCell
        filters={[{ column: 'shield_pop', value: textValue(info.getValue()) }]}
        display={<PopLabel code={textValue(info.getValue())} />}
        className="font-bold text-purple-500"
      />
    ),
  },
  {
    accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: columnHeader('Reqs'),
    cell: (info) => {
      const lowSample = info.row.original.low_sample
      return (
        <span
          className={cn('tabular-nums', lowSample ? 'text-muted-foreground' : '')}
          title={lowSample ? 'Fewer than 30 requests — percentiles unreliable, route not flagged' : undefined}
        >
          {formatCount(numberValue(info.getValue()) ?? 0)}
        </span>
      )
    },
  },
  ...([
    ['p50_ms', 'Median (P50)', 'P50 (E→S)'],
    ['p95_ms', 'P95 Latency', 'P95 (E→S)'],
    ['p99_ms', 'P99 Latency', 'P99 (E→S)'],
  ] as const).map(([key, label, hdr]): ColumnDef<ShieldingRowInput> => ({
    accessorKey: key, id: key, meta: { label }, header: columnHeader(hdr), cell: (info) => msCell(info.getValue()),
  })),
  {
    accessorKey: 'light_speed_rtt_ms', id: 'light_speed_rtt_ms', meta: { label: 'Light-Speed Floor' }, header: columnHeader('Light Floor'),
    cell: (info) => {
      const floor = numberValue(info.getValue())
      return <span className="text-muted-foreground">{floor != null ? `${floor.toFixed(1)}ms` : '—'}</span>
    },
  },
  {
    accessorKey: 'efficiency_ratio', id: 'efficiency_ratio', meta: { label: 'Efficiency' }, header: columnHeader('Efficiency'),
    cell: (info) => {
      const ratio = numberValue(info.getValue())
      if (ratio == null) return <span className="text-muted-foreground">—</span>
      const lowSample = info.row.original.low_sample
      const cls = lowSample || ratio < 0 ? 'bg-muted text-muted-foreground' : ratio < 1.5 ? 'bg-green-500/10 text-green-600 dark:text-green-400' : ratio < 3 ? 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400' : 'bg-destructive/10 text-destructive'
      return <span title={lowSample ? 'Fewer than 30 requests — too few samples to grade; not flagged' : undefined} className={cn('inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-bold tabular-nums', cls)}>{ratio.toFixed(1)}×</span>
    },
  },
]

const SHIELDING_LABELS: Record<string, string> = Object.fromEntries(
  SHIELDING_COLUMNS.map((c) => [c.id!, (c.meta as { label: string }).label]),
)
export const getShieldingLabels = (ids: string[]) => ids.map((id) => ({ id, label: SHIELDING_LABELS[id] || id }))
