'use client'

import React from 'react'
import type { ColumnDef } from '@/lib/table'
import { DataTableReadonly, FilterValueCell } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import type { components } from '@/types/api'
import { cn, formatCount } from '@/lib/utils'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import {
  AsnLeaderboardHelp,
  HealthBadge,
  MetroLeaderboardHelp,
} from './NetworkHelp'
import { approxMark, boundMark, readRollupMeta, type NetworkRollupMeta } from '@/components/network/rollupLabels'

export type AsnRow = components['schemas']['NetworkLeaderboardRow']
export type MetroRow = components['schemas']['NetworkMetroRow']

const rttMs = (us: number | null | undefined) => (us != null ? `${(us / 1000).toFixed(1)}ms` : '—')
const healthDelta = (a: AsnRow): number | null =>
  a.health_score_now != null && a.health_score_1h_ago != null
    ? a.health_score_now - a.health_score_1h_ago
    : null

const ApproxHealth = ({ mark, score }: { mark: string; score: number | null }) => (
  <span className="block text-right">{mark && <span className="mr-1 text-xs text-muted-foreground">{mark}</span>}<HealthBadge score={score} /></span>
)

export const asnColumns = (meta?: NetworkRollupMeta): ColumnDef<AsnRow>[] => [
  {
    id: 'asn', header: 'ASN', accessorFn: (a) => a.label, size: 240,
    cell: ({ row }) => (
      <span className="text-xs font-medium">
        <FilterValueCell filters={[{ column: 'asn', value: String(row.original.asn) }]} display={row.original.label} />
      </span>
    ),
  },
  {
    id: 'requests', header: 'Requests', accessorFn: (a) => a.total_reqs ?? 0, size: 110,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{boundMark(meta, 'leaderboard')}{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (a) => a.health_score_now ?? -1, size: 120,
    cell: ({ row }) => <ApproxHealth mark={approxMark(meta, 'health_score_now')} score={row.original.health_score_now ?? null} />,
  },
  {
    id: 'p95', header: 'P95 RTT', accessorFn: (a) => a.p95_rtt_us ?? -1, size: 100,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{rttMs(row.original.p95_rtt_us)}</span>,
  },
  {
    id: 'p99', header: 'P99 RTT', accessorFn: (a) => a.p99_rtt_us ?? -1, size: 100,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{rttMs(row.original.p99_rtt_us)}</span>,
  },
  {
    id: 'delta', header: '1h Change', accessorFn: (a) => healthDelta(a) ?? Number.NEGATIVE_INFINITY, size: 110,
    cell: ({ row }) => {
      const delta = healthDelta(row.original)
      if (delta == null) {
        return <span className="block text-right font-mono text-xs text-muted-foreground">—</span>
      }
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
    id: 'trend', header: 'Trend', accessorFn: (a) => a.trend ?? 'stable', size: 110,
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

export const metroColumns = (meta?: NetworkRollupMeta): ColumnDef<MetroRow>[] => [
  {
    id: 'city', header: 'Metro / City', accessorFn: (m) => m.city, size: 260,
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
          ) : <span>{m.city}</span>}
        </span>
      )
    },
  },
  {
    id: 'requests', header: 'Requests', accessorFn: (m) => m.total_reqs ?? 0, size: 110,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{boundMark(meta, 'metro_leaderboard')}{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (m) => m.health_score ?? -1, size: 120,
    cell: ({ row }) => <ApproxHealth mark={approxMark(meta, 'health_score')} score={row.original.health_score ?? null} />,
  },
]

export function LeaderboardStatusBody<T>({
  isLoading, rows, isActiveField, emptyActiveMsg, emptyInactiveMsg, children,
}: {
  isLoading: boolean; rows: readonly T[] | undefined; isActiveField: boolean
  emptyActiveMsg: string; emptyInactiveMsg: string; children: (rows: readonly T[]) => React.ReactNode
}) {
  if (isLoading) return <div className="space-y-2 p-4"><SkeletonGrid count={5} height="48px" className="rounded-md" /></div>
  if (!rows || rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        <p className="mb-1">No data available</p>
        <p className="text-[10px] opacity-70">{isActiveField ? emptyActiveMsg : emptyInactiveMsg}</p>
      </div>
    )
  }
  return <>{children(rows)}</>
}

export function NetworkLeaderboardsSection({
  data,
  isCoreLoadingInitial,
  isFetching,
  error,
  isFieldActive,
}: {
  data: components['schemas']['NetworkHealthResponse'] | undefined
  isCoreLoadingInitial: boolean
  isFetching: boolean
  error: unknown
  isFieldActive: (field: string) => boolean
}) {
  const rollup = readRollupMeta(data)
  const asnCols = React.useMemo(() => asnColumns(rollup), [rollup])
  const metroCols = React.useMemo(() => metroColumns(rollup), [rollup])
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:420px]">
      <AnalyticsCard
        title="ASN Performance Leaderboard" className="flex flex-col min-h-[420px] [contain-intrinsic-size:420px]"
        contentClassName="p-0 flex-1" helpTitle="ASN Performance Leaderboard" helpContent={<AsnLeaderboardHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null} loadingMessage="Compiling ASN leaderboard..."
      >
        <LeaderboardStatusBody
          isLoading={isCoreLoadingInitial} rows={data?.leaderboard} isActiveField={isFieldActive('asn')}
          emptyActiveMsg="No ASN data in this time range."
          emptyInactiveMsg="Requires Network Quality (Group F) fields to be enabled in Fastly logging."
        >
          {(rows) => <DataTableReadonly columns={asnCols} data={[...rows]} hideToolbar showPagination={false} tableCaption="ASN performance leaderboard" />}
        </LeaderboardStatusBody>
      </AnalyticsCard>

      {data?.has_metro && (
        <AnalyticsCard
          title="Metro / Region Leaderboard" className="flex flex-col min-h-[420px] [contain-intrinsic-size:420px]"
          contentClassName="p-0 flex-1" helpTitle="Metro / Region Leaderboard" helpContent={<MetroLeaderboardHelp />}
          isFetching={isFetching} error={error as AnalyticsCardError | null} loadingMessage="Compiling Metro leaderboard..."
        >
          <LeaderboardStatusBody
            isLoading={isCoreLoadingInitial} rows={data?.metro_leaderboard} isActiveField={isFieldActive('metro')}
            emptyActiveMsg="No metro data in this time range."
            emptyInactiveMsg="Requires Geolocation (Group D/E) fields to be enabled in Fastly logging."
          >
            {(rows) => <DataTableReadonly columns={metroCols} data={[...rows]} hideToolbar showPagination={false} tableCaption="Metro and region leaderboard" />}
          </LeaderboardStatusBody>
        </AnalyticsCard>
      )}
    </div>
  )
}
