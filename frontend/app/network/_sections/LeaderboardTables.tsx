'use client'

import React from 'react'
import type { ColumnDef } from '@tanstack/react-table'

import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'
import { FilterValueCell } from '@/components/FilterValueCell'
import { Badge } from '@/components/ui/badge'
import { formatCount } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { components } from '@/types/api'
import { HealthBadge } from '../help-content'

type AsnRow = components['schemas']['NetworkLeaderboardRow']
type MetroRow = components['schemas']['NetworkMetroRow']

const rttMs = (us: number | null | undefined) => (us != null ? `${(us / 1000).toFixed(1)}ms` : '—')
const healthDelta = (a: AsnRow) => (a.health_score_now ?? 0) - (a.health_score_1h_ago ?? 0)

const ASN_COLUMNS: ColumnDef<AsnRow>[] = [
  {
    id: 'asn',
    header: 'ASN',
    accessorFn: (a) => a.label,
    size: 240,
    cell: ({ row }) => (
      <span className="text-xs font-medium">
        <FilterValueCell
          filters={[{ column: 'asn', value: String(row.original.asn) }]}
          display={row.original.label}
        />
      </span>
    ),
  },
  {
    id: 'requests',
    header: 'Requests',
    accessorFn: (a) => a.total_reqs ?? 0,
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>
    ),
  },
  {
    id: 'health',
    header: 'Health Score',
    accessorFn: (a) => a.health_score_now ?? -1,
    size: 120,
    cell: ({ row }) => (
      <span className="block text-right">
        <HealthBadge score={row.original.health_score_now ?? null} />
      </span>
    ),
  },
  {
    id: 'p95',
    header: 'P95 RTT',
    accessorFn: (a) => a.p95_rtt_us ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{rttMs(row.original.p95_rtt_us)}</span>
    ),
  },
  {
    id: 'p99',
    header: 'P99 RTT',
    accessorFn: (a) => a.p99_rtt_us ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{rttMs(row.original.p99_rtt_us)}</span>
    ),
  },
  {
    id: 'delta',
    header: '1h Change',
    accessorFn: healthDelta,
    size: 110,
    cell: ({ row }) => {
      const delta = healthDelta(row.original)
      return (
        <span className="block text-right font-mono text-xs">
          <span className={delta > 0 ? 'text-green-500' : delta < 0 ? 'text-red-500' : 'text-muted-foreground'}>
            {delta > 0 ? '+' : ''}{delta.toFixed(1)}
          </span>
        </span>
      )
    },
  },
  {
    id: 'trend',
    header: 'Trend',
    accessorFn: (a) => a.trend ?? 'stable',
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right">
        <Badge
          variant={row.original.trend === 'degrading' ? 'destructive' : 'outline'}
          className={cn('text-[10px]', row.original.trend === 'improving' ? 'text-green-600 dark:text-green-400 border-green-300 dark:border-green-700' : '')}
        >
          {(row.original.trend ?? 'stable').toUpperCase()}
        </Badge>
      </span>
    ),
  },
]

const METRO_COLUMNS: ColumnDef<MetroRow>[] = [
  {
    id: 'city',
    header: 'Metro / City',
    accessorFn: (m) => m.city,
    size: 260,
    cell: ({ row }) => {
      const m = row.original
      return (
        <span className="text-xs font-medium">
          {m.raw_city ? (
            <FilterValueCell
              filters={[
                { column: 'city', value: m.raw_city },
                ...(m.region ? [{ column: 'region', value: m.region }] : []),
                ...(m.country ? [{ column: 'country', value: m.country }] : []),
              ]}
              display={m.city}
            />
          ) : (
            <span>{m.city}</span>
          )}
        </span>
      )
    },
  },
  {
    id: 'requests',
    header: 'Requests',
    accessorFn: (m) => m.total_reqs ?? 0,
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>
    ),
  },
  {
    id: 'health',
    header: 'Health Score',
    accessorFn: (m) => m.health_score ?? -1,
    size: 120,
    cell: ({ row }) => (
      <span className="block text-right">
        <HealthBadge score={row.original.health_score ?? null} />
      </span>
    ),
  },
]

export function AsnLeaderboardTable({ rows }: { rows: AsnRow[] }) {
  return (
    <DataTableReadonly
      columns={ASN_COLUMNS}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="ASN performance leaderboard"
    />
  )
}

export function MetroLeaderboardTable({ rows }: { rows: MetroRow[] }) {
  return (
    <DataTableReadonly
      columns={METRO_COLUMNS}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="Metro and region leaderboard"
    />
  )
}
