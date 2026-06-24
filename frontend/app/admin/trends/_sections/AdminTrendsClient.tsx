'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { TrendingUp } from 'lucide-react'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Sparkline } from '@/components/Sparkline'
import { Button } from '@/components/ui/button'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import { client } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { components } from '@/types/api.generated'

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']

const WINDOWS: { label: string; value: '1h' | '24h' | '7d' }[] = [
  { label: 'Last hour', value: '1h' },
  { label: 'Last 24h', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
]

// Series displayed on the page, top-down. Each entry binds a metric key
// (global → bare metric name, per-service → metric + '|svc'; we resolve
// per-service inline below) to a display label, unit, and chart domain.
const SERIES_LAYOUT: {
  title: string
  description: string
  metric: string
  unit: string
  yDomain?: [number | 'auto', number | 'auto']
  aggregateBy?: 'max-per-ts'
}[] = [
  { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m', unit: '' },
  { title: 'Memory used', description: 'Physical memory consumption.', metric: 'mem_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Data disk used', description: 'Disk consumption on the data mount.', metric: 'disk_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Boot disk used', description: 'Disk consumption on the root mount.', metric: 'disk_used_pct_root', unit: '%', yDomain: [0, 100] },
  { title: 'Active DuckDB queries', description: 'Concurrent in-flight query count from the registry.', metric: 'active_query_count', unit: '' },
  { title: 'Pool wait p95', description: 'Worst-case DuckDB pool checkout wait (ms) — max across services per timestamp.', metric: 'pool_wait_p95_ms', unit: 'ms', aggregateBy: 'max-per-ts' },
  { title: 'Ingest lag', description: 'Seconds since the most recent ingest per service — max across services per timestamp.', metric: 'ingest_lag_s', unit: 's', aggregateBy: 'max-per-ts' },
]

function maxAcrossServicesPerTs(series: Record<string, TrendPoint[]>, metric: string): TrendPoint[] {
  const keys = Object.keys(series).filter((k) => k.startsWith(`${metric}|`))
  if (!keys.length) return []
  const byTs = new Map<string, number>()
  for (const k of keys) {
    for (const p of series[k] ?? []) {
      const prev = byTs.get(p.ts) ?? 0
      if (p.value > prev) byTs.set(p.ts, p.value)
    }
  }
  return Array.from(byTs.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([ts, value]) => ({ ts, value }))
}

function formatValue(v: number, unit: string): string {
  if (unit === '%') return `${v.toFixed(1)}%`
  if (unit === 'ms') return `${v.toFixed(1)}ms`
  if (unit === 's') return `${v.toFixed(0)}s`
  return v.toFixed(2)
}

export default function AdminTrendsClient() {
  // SEED-KEY PIN: '1h' is the cold-load default that the RSC shell
  // (../page.tsx) pre-seeds into ['admin','metric-history-batch','1h'] via
  // HydrationBoundary. If you change this default, update the seed literal in
  // page.tsx too or the SSR seed will silently miss and refetch on mount.
  const [window, setWindow] = React.useState<'1h' | '24h' | '7d'>('1h')

  const { data: trends, isLoading, isFetching, error } = useQuery({
    queryKey: ['admin', 'metric-history-batch', window],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/admin/metric-history/batch', {
        params: { query: { since: window } },
        signal,
      })
      return data as TrendBatch
    },
    // 60s sampler cadence — refetch matches that. Background-tab is off
    // so a backgrounded /admin/trends tab doesn't keep pulling.
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    staleTime: 60_000,
  })

  const series = trends?.series ?? {}

  return (
    <div className="container mx-auto p-4 space-y-4">
      <PageHeader
        title="Operational Trends"
        description="Sampled every 60 s. Retained 30 days. Backed by data/system/system_metrics.db."
        icon={TrendingUp}
      >
        <BackToAdminLink variant="ghost" prefetch={false} />
      </PageHeader>

      <div className="flex gap-2">
        {WINDOWS.map((w) => (
          <Button
            key={w.value}
            variant={window === w.value ? 'default' : 'outline'}
            size="sm"
            onClick={() => setWindow(w.value)}
          >
            {w.label}
          </Button>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {SERIES_LAYOUT.map((cfg) => {
          const points =
            cfg.aggregateBy === 'max-per-ts'
              ? maxAcrossServicesPerTs(series, cfg.metric)
              : series[cfg.metric] ?? []
          const latest = points.length ? points[points.length - 1].value : null
          return (
            <AnalyticsCard
              key={cfg.metric}
              title={cfg.title}
              description={cfg.description}
              isLoading={isLoading}
              isFetching={isFetching}
              error={error as AnalyticsCardError | null}
              isEmpty={!isLoading && !error && points.length === 0}
            >
              <div className="flex items-baseline justify-between mb-2">
                <div className="text-2xl font-semibold tabular-nums">
                  {latest === null ? '–' : formatValue(latest, cfg.unit)}
                </div>
                <div className="text-xs text-muted-foreground">
                  {points.length > 0 ? `${points.length} samples` : isLoading ? 'loading…' : 'no samples yet'}
                </div>
              </div>
              <div className={cn('text-foreground/70', latest === null && 'opacity-40')}>
                <Sparkline
                  points={points}
                  yDomain={cfg.yDomain}
                  height={120}
                  label={cfg.title}
                  formatValue={(v) => formatValue(v, cfg.unit)}
                />
              </div>
            </AnalyticsCard>
          )
        })}
      </div>
    </div>
  )
}
