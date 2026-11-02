'use client'
import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw, TrendingUp } from 'lucide-react'
import type { components } from '@/types/api.generated'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { cn } from '@/lib/utils'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/confirm-dialog'

export type TrendWindow = '1h' | '6h' | '24h' | '7d'
export const WINDOWS: { label: string; value: TrendWindow }[] = [
  { label: 'Last hour', value: '1h' }, { label: 'Last 6h', value: '6h' }, { label: 'Last 24h', value: '24h' }, { label: 'Last 7 days', value: '7d' },
]
export const SERIES_LAYOUT: { title: string; description: string; metric: string; unit: string; yDomain?: [number | 'auto', number | 'auto']; aggregateBy?: 'max-per-ts' }[] = [
  { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m', unit: '' },
  { title: 'Memory used', description: 'Physical memory consumption.', metric: 'mem_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Data disk used', description: 'Disk consumption on the data mount.', metric: 'disk_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Boot disk used', description: 'Disk consumption on the root mount.', metric: 'disk_used_pct_root', unit: '%', yDomain: [0, 100] },
  { title: 'Active DuckDB queries', description: 'Concurrent in-flight query count from the registry.', metric: 'active_query_count', unit: '' },
  { title: 'Pool wait p95', description: 'Worst-case DuckDB pool checkout wait (ms) — max across services per timestamp.', metric: 'pool_wait_p95_ms', unit: 'ms', aggregateBy: 'max-per-ts' },
  { title: 'Ingest lag', description: 'Seconds since the most recent ingest per service — max across services per timestamp.', metric: 'ingest_lag_s', unit: 's', aggregateBy: 'max-per-ts' },
]

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']

function maxAcrossServicesPerTs(series: Record<string, TrendPoint[]>, metric: string): TrendPoint[] {
  const keys = Object.keys(series).filter((k) => k.startsWith(`${metric}|`))
  if (!keys.length) return []
  const byTs = new Map<string, number>()
  for (const k of keys) for (const p of series[k] ?? []) if (p.value > (byTs.get(p.ts) ?? 0)) byTs.set(p.ts, p.value)
  return Array.from(byTs.entries()).sort(([a], [b]) => a.localeCompare(b)).map(([ts, value]) => ({ ts, value }))
}

function formatTrendValue(v: number, unit: string): string {
  return unit === '%' ? `${v.toFixed(1)}%` : unit === 'ms' ? `${v.toFixed(1)}ms` : unit === 's' ? `${v.toFixed(0)}s` : v.toFixed(2)
}

export function AdminTrendsClient() {
  const [window, setWindow] = React.useState<TrendWindow>('1h')
  const { data: trends, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch(window),
    queryFn: async ({ signal }) => (await unwrapClient(client.GET('/api/admin/metric-history/batch', { params: { query: { since: window } }, signal }))) as TrendBatch,
    refetchInterval: 60_000, refetchIntervalInBackground: false, staleTime: 60_000,
  })
  const series = trends?.series ?? {}
  const isRefreshing = isFetching && !isLoading
  return (
    <div data-testid="admin-trends-root" className="container mx-auto p-4 space-y-4">
      <PageHeader title="Operational Trends" description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots." icon={TrendingUp}>
        <div className="flex items-center gap-2">
          <Button type="button" variant="outline" size="sm" data-testid="admin-trends-refresh-btn" onClick={() => void refetch()} disabled={isFetching}>
            <RefreshCw className={cn('mr-1.5 h-3.5 w-3.5', isRefreshing && 'animate-spin')} />Refresh
          </Button>
          <BackToAdminLink variant="ghost" prefetch={false} />
        </div>
      </PageHeader>
      <div data-testid="admin-trends-toolbar" className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]">
        {WINDOWS.map((w) => (
          <Button key={w.value} type="button" data-testid={`admin-trends-window-${w.value}`} aria-pressed={window === w.value} variant={window === w.value ? 'default' : 'outline'} size="sm" onClick={() => setWindow(w.value)}>
            {w.label}
          </Button>
        ))}
      </div>
      <div data-testid="admin-trends-grid-reservation" className={cn('grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]', isRefreshing && 'opacity-40 pointer-events-none transition-opacity')}>
        {SERIES_LAYOUT.map((cfg) => {
          const points = cfg.aggregateBy === 'max-per-ts' ? maxAcrossServicesPerTs(series, cfg.metric) : series[cfg.metric] ?? []
          const latest = points.length ? points[points.length - 1].value : null
          return (
            <div key={cfg.metric} data-testid={`admin-trends-card-${cfg.metric}`}>
              <AnalyticsCard title={cfg.title} description={cfg.description} isLoading={isLoading} isFetching={isFetching} loadingMessage="Loading system vital trends..." error={error as AnalyticsCardError | null} isEmpty={!isLoading && !error && points.length === 0}>
                <div className="flex items-baseline justify-between mb-2">
                  <div data-testid={`admin-trends-latest-${cfg.metric}`} className="text-2xl font-semibold tabular-nums">{latest === null ? '–' : formatTrendValue(latest, cfg.unit)}</div>
                  <div className="text-xs text-muted-foreground">{points.length > 0 ? `${points.length} samples` : isLoading ? 'loading…' : 'no samples yet'}</div>
                </div>
                <div className={cn('text-foreground/70', latest === null && 'opacity-40')}>
                  <Sparkline points={points} yDomain={cfg.yDomain} height={120} label={cfg.title} formatValue={(v) => formatTrendValue(v, cfg.unit)} />
                </div>
              </AnalyticsCard>
            </div>
          )
        })}
      </div>
    </div>
  )
}
export default AdminTrendsClient
