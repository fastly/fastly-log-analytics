'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw, TrendingUp } from 'lucide-react'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Sparkline } from '@/components/Sparkline'
import { Button } from '@/components/ui/button'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { cn } from '@/lib/utils'
import type { components } from '@/types/api.generated'
import { queryKeys } from '@/lib/query-keys'
import { SERIES_LAYOUT, WINDOWS, type TrendWindow } from './trends-config'

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
export type { TrendWindow }

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
  const [window, setWindow] = React.useState<TrendWindow>('1h')

  const { data: trends, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch(window),
    queryFn: async ({ signal }) => {
      return (await unwrapClient(
        client.GET('/api/admin/metric-history/batch', {
          params: { query: { since: window } },
          signal,
        }),
      )) as TrendBatch
    },
    // 60s sampler cadence — refetch matches that. Background-tab is off
    // so a backgrounded /admin/trends tab doesn't keep pulling.
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    staleTime: 60_000,
  })

  const series = trends?.series ?? {}
  const isRefreshing = isFetching && !isLoading

  return (
    <div data-testid="admin-trends-root" className="container mx-auto p-4 space-y-4">
      <PageHeader
        title="Operational Trends"
        description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots."
        icon={TrendingUp}
      >
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            data-testid="admin-trends-refresh-btn"
            onClick={() => void refetch()}
            disabled={isFetching}
          >
            <RefreshCw className={cn('mr-1.5 h-3.5 w-3.5', isRefreshing && 'animate-spin')} />
            Refresh
          </Button>
          <BackToAdminLink variant="ghost" prefetch={false} />
        </div>
      </PageHeader>

      <div
        data-testid="admin-trends-toolbar"
        className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        {WINDOWS.map((w) => (
          <Button
            key={w.value}
            type="button"
            data-testid={`admin-trends-window-${w.value}`}
            aria-pressed={window === w.value}
            variant={window === w.value ? 'default' : 'outline'}
            size="sm"
            onClick={() => setWindow(w.value)}
          >
            {w.label}
          </Button>
        ))}
      </div>

      <div
        data-testid="admin-trends-grid-reservation"
        className={cn(
          'grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]',
          isRefreshing && 'opacity-40 pointer-events-none transition-opacity',
        )}
      >
        {SERIES_LAYOUT.map((cfg) => {
          const points =
            cfg.aggregateBy === 'max-per-ts'
              ? maxAcrossServicesPerTs(series, cfg.metric)
              : series[cfg.metric] ?? []
          const latest = points.length ? points[points.length - 1].value : null
          return (
            <div key={cfg.metric} data-testid={`admin-trends-card-${cfg.metric}`}>
              <AnalyticsCard
                title={cfg.title}
                description={cfg.description}
                isLoading={isLoading}
                isFetching={isFetching}
                loadingMessage="Loading system vital trends..."
                error={error as AnalyticsCardError | null}
                isEmpty={!isLoading && !error && points.length === 0}
              >
                <div className="flex items-baseline justify-between mb-2">
                  <div
                    data-testid={`admin-trends-latest-${cfg.metric}`}
                    className="text-2xl font-semibold tabular-nums"
                  >
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
            </div>
          )
        })}
      </div>
    </div>
  )
}
