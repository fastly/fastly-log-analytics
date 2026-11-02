'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import type { ColumnDef } from '@tanstack/react-table'
import {
  DataTableReadonly,
  FilterValueCell,
  PopLabel,
} from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import type { components, PlotlyLayout, PlotlyTrace } from '@/types/api'
import { cn, formatCount } from '@/lib/utils'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/confirm-dialog'
import {
  AlertCircle,
  Globe,
  Zap,
  Activity,
} from 'lucide-react'
import {
  AsnLeaderboardHelp,
  AvgRttHelp,
  GlobalHealthHelp,
  HealthBadge,
  MetroLeaderboardHelp,
  NetworkQualityHelp,
  WorstAsnHelp,
  WorstRegionHelp,
} from './NetworkHelp'

const PlotlyChart = dynamic(() => import('@/components/PlotlyChart').then((mod) => mod.PlotlyChart), {
  ssr: false, loading: () => <Skeleton className="w-full h-[300px]" />,
})

export const HEATMAP_LAYOUT: PlotlyLayout = { xaxis: { tickangle: -45 } }
export const QUALITY_SCATTER_LAYOUT: PlotlyLayout = { xaxis: { title: { text: 'TCP RTT (ms)' } }, yaxis: { title: { text: 'TTFB (ms)' } } }

export type QualityBarRow = components['schemas']['NetworkQualityBarRow']
export type AsnRow = components['schemas']['NetworkLeaderboardRow']
export type MetroRow = components['schemas']['NetworkMetroRow']

const rttMs = (us: number | null | undefined) => (us != null ? `${(us / 1000).toFixed(1)}ms` : '—')
const healthDelta = (a: AsnRow) => (a.health_score_now ?? 0) - (a.health_score_1h_ago ?? 0)

export const ASN_COLUMNS: ColumnDef<AsnRow>[] = [
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
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (a) => a.health_score_now ?? -1, size: 120,
    cell: ({ row }) => <span className="block text-right"><HealthBadge score={row.original.health_score_now ?? null} /></span>,
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
    id: 'delta', header: '1h Change', accessorFn: healthDelta, size: 110,
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

export const METRO_COLUMNS: ColumnDef<MetroRow>[] = [
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
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (m) => m.health_score ?? -1, size: 120,
    cell: ({ row }) => <span className="block text-right"><HealthBadge score={row.original.health_score ?? null} /></span>,
  },
]

export function RttBarList({ rows, filterColumn }: { rows: readonly QualityBarRow[]; filterColumn?: string }) {
  if (!rows?.length) return <div className="p-6 text-center text-sm text-muted-foreground">No data in this range</div>
  const max = Math.max(...rows.map((r) => r.rtt_ms), 1)
  return (
    <div className="space-y-1.5 p-3">
      {rows.map((r) => {
        const filterVal = r.value ?? r.label
        const display = filterColumn === 'pop' ? <PopLabel code={filterVal} /> : r.label
        return (
          <div key={filterVal} className="flex items-center gap-2 text-xs">
            <div className="w-48 truncate shrink-0" title={r.label}>
              {filterColumn ? <FilterValueCell filters={[{ column: filterColumn, value: filterVal }]} display={display} /> : <span>{display}</span>}
            </div>
            <div className="flex-1 h-4 bg-muted/40 rounded-sm overflow-hidden">
              <div className="h-full bg-primary/70" style={{ width: `${(r.rtt_ms / max) * 100}%` }} />
            </div>
            <div className="w-16 text-right font-mono shrink-0">{r.rtt_ms.toFixed(1)}ms</div>
            <div className="w-16 text-right font-mono text-muted-foreground shrink-0">{formatCount(r.reqs ?? 0)}</div>
          </div>
        )
      })}
    </div>
  )
}

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

export function NetworkKpiGrid({
  summary,
  isCoreLoadingInitial,
}: {
  summary: components['schemas']['NetworkHealthResponse']['summary'] | undefined
  isCoreLoadingInitial: boolean
}) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 [contain-intrinsic-size:120px] [content-visibility:auto]">
      {[
        {
          title: 'Global Health', icon: Activity, loadingMessage: 'Compiling ASN health matrix...',
          value: <>{summary?.global_health_score ?? '--'}/100</>, sub: 'Average across top ASNs',
          helpTitle: 'Global Health Score', helpContent: <GlobalHealthHelp />,
        },
        {
          title: 'Avg RTT', icon: Zap, loadingMessage: 'Analyzing TCP round-trip times...',
          value: <>{summary?.avg_rtt_ms ?? '--'}ms</>, sub: 'Across all regions',
          helpTitle: 'Average RTT', helpContent: <AvgRttHelp />,
        },
        {
          title: 'Worst ASN', icon: AlertCircle, iconClassName: 'text-red-500', loadingMessage: 'Scanning ASN degradation...',
          value: <span className="text-sm truncate block">{summary?.worst_asn?.label ?? '--'}</span>,
          sub: <span className="text-red-500 font-medium">Score: {summary?.worst_asn?.score ?? '--'}</span>,
          helpTitle: 'Worst-Performing ASN', helpContent: <WorstAsnHelp />,
        },
        {
          title: 'Worst Region', icon: Globe, loadingMessage: 'Evaluating regional latency...',
          value: <span className="text-sm truncate block">{summary?.worst_country?.label ?? '--'}</span>,
          sub: <span className="text-red-500 font-medium">Score: {summary?.worst_country?.score ?? '--'}</span>,
          helpTitle: 'Worst-Performing Region', helpContent: <WorstRegionHelp />,
        },
      ].map((card) => (
        <StatCard key={card.title} {...card} loading={isCoreLoadingInitial} className="min-h-[110px]" />
      ))}
    </div>
  )
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
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:420px] [content-visibility:auto]">
      <AnalyticsCard
        title="ASN Performance Leaderboard" className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
        contentClassName="p-0 flex-1" helpTitle="ASN Performance Leaderboard" helpContent={<AsnLeaderboardHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null} loadingMessage="Compiling ASN leaderboard..."
      >
        <LeaderboardStatusBody
          isLoading={isCoreLoadingInitial} rows={data?.leaderboard} isActiveField={isFieldActive('asn')}
          emptyActiveMsg="No ASN data in this time range."
          emptyInactiveMsg="Requires Network Quality (Group F) fields to be enabled in Fastly logging."
        >
          {(rows) => <DataTableReadonly columns={ASN_COLUMNS} data={[...rows]} hideToolbar showPagination={false} tableCaption="ASN performance leaderboard" />}
        </LeaderboardStatusBody>
      </AnalyticsCard>

      {data?.has_metro && (
        <AnalyticsCard
          title="Metro / Region Leaderboard" className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
          contentClassName="p-0 flex-1" helpTitle="Metro / Region Leaderboard" helpContent={<MetroLeaderboardHelp />}
          isFetching={isFetching} error={error as AnalyticsCardError | null} loadingMessage="Compiling Metro leaderboard..."
        >
          <LeaderboardStatusBody
            isLoading={isCoreLoadingInitial} rows={data?.metro_leaderboard} isActiveField={isFieldActive('metro')}
            emptyActiveMsg="No metro data in this time range."
            emptyInactiveMsg="Requires Geolocation (Group D/E) fields to be enabled in Fastly logging."
          >
            {(rows) => <DataTableReadonly columns={METRO_COLUMNS} data={[...rows]} hideToolbar showPagination={false} tableCaption="Metro and region leaderboard" />}
          </LeaderboardStatusBody>
        </AnalyticsCard>
      )}
    </div>
  )
}

export function NetworkQualitySection({
  qualityData,
  isQualityLoadingInitial,
  isFetching,
  error,
  regionCountry,
  setRegionCountry,
  qualityScatterTrace,
}: {
  qualityData: components['schemas']['NetworkQualityResponse'] | undefined
  isQualityLoadingInitial: boolean
  isFetching: boolean
  error: unknown
  regionCountry: string
  setRegionCountry: (country: string) => void
  qualityScatterTrace: PlotlyTrace[]
}) {
  const qualityBreakdowns: Array<{
    title: string; loadingMessage: string; rows: readonly QualityBarRow[]; filterColumn: string; headerAction?: React.ReactNode
  }> = [
    { title: 'Avg RTT by Country', loadingMessage: 'Analyzing RTT by country...', rows: qualityData?.by_country ?? [], filterColumn: 'country' },
    { title: 'Avg RTT by ASN', loadingMessage: 'Analyzing RTT by ASN...', rows: qualityData?.by_asn ?? [], filterColumn: 'asn' },
    {
      title: 'Avg RTT by Region', loadingMessage: 'Analyzing RTT by region...', rows: qualityData?.by_region ?? [], filterColumn: 'region',
      headerAction: (
        <select
          aria-label="Region breakdown country" value={regionCountry}
          onChange={(e) => setRegionCountry(e.target.value)} className="h-7 rounded-md border bg-background px-2 text-xs"
        >
          {(qualityData?.countries ?? []).length === 0 && <option value={regionCountry}>{regionCountry}</option>}
          {(qualityData?.countries ?? []).map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      ),
    },
    { title: 'Avg RTT by POP', loadingMessage: 'Analyzing RTT by POP...', rows: qualityData?.by_pop ?? [], filterColumn: 'pop' },
  ]

  return (
    <div className="space-y-4 [contain-intrinsic-size:400px] [content-visibility:auto]">
      <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Network Quality</h2>
      {isQualityLoadingInitial ? (
        <SkeletonGrid count={4} height="180px" className="rounded-xl" />
      ) : (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {qualityBreakdowns.map((b) => (
              <AnalyticsCard
                key={b.title} title={b.title} contentClassName="p-0" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
                isFetching={isFetching} error={error as AnalyticsCardError | null}
                loadingMessage={b.loadingMessage} className="[contain-intrinsic-size:240px] [content-visibility:auto]" headerAction={b.headerAction}
              >
                <RttBarList rows={b.rows} filterColumn={b.filterColumn} />
              </AnalyticsCard>
            ))}
          </div>
          {qualityScatterTrace.length > 0 && (
            <AnalyticsCard
              title="RTT vs TTFB" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
              isFetching={isFetching} error={error as AnalyticsCardError | null}
              loadingMessage="Plotting RTT vs TTFB correlation..." className="[contain-intrinsic-size:380px] [content-visibility:auto]"
            >
              <PlotlyChart data={qualityScatterTrace} layout={QUALITY_SCATTER_LAYOUT} height={360} />
            </AnalyticsCard>
          )}
        </>
      )}
    </div>
  )
}
