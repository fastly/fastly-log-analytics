'use client'

import React from 'react'
import { ArrowRight } from 'lucide-react'
import Link from 'next/link'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useServiceStore } from '@/stores/filterStore'
import { formatDate, buildServiceHref, CHART_COLORS } from '@/lib/utils'
import {
  type MetricsData,
  type MetricsTick,
  type ErrorEvent,
  type TabCommonProps,
  RealtimeMetricCard,
  RealtimeChart,
  ConnectionBadge,
  FreshnessBadge,
  RtDownBanner,
  MetricCardGrid,
  TabPanelShell,
  seriesTraces,
} from './ControlRoomWidgets'

function ErrorFilterCell({
  filterKey,
  value,
  activeServiceId,
  className = 'px-3 py-2',
  bold = false,
}: {
  filterKey: string
  value: string | number
  activeServiceId: string | null
  className?: string
  bold?: boolean
}) {
  const str = String(value)
  return (
    <td className={className} title={filterKey === 'url' ? str : undefined}>
      {str ? (
        <Link
          className={`${bold ? 'font-semibold ' : ''}underline decoration-dotted underline-offset-2`}
          href={buildServiceHref(`/dashboard?filter_${filterKey}=${encodeURIComponent(str)}`, activeServiceId)}
          prefetch={false}
        >
          {value}
        </Link>
      ) : (
        '(unknown)'
      )}
    </td>
  )
}

function RecentErrorsCard({
  events,
  connected,
  timezone,
}: {
  events: ErrorEvent[]
  connected: boolean
  timezone: string
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  return (
    <Card className="contain-intrinsic-size-[320px]">
      <CardHeader className="flex flex-row items-center justify-between gap-3">
        <CardTitle>Live errors</CardTitle>
        <Badge variant={connected ? 'success' : 'destructive'}>{connected ? 'Listening' : 'Disconnected'}</Badge>
      </CardHeader>
      <CardContent>
        {events.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            {connected ? 'No recent 4xx/5xx errors.' : 'Listening for errors...'}
          </p>
        ) : (
          <div className="overflow-x-auto rounded-lg border">
            <table className="w-full text-sm">
              <caption className="sr-only">Recent HTTP errors</caption>
              <thead className="bg-muted/50 text-left">
                <tr>
                  {['Time', 'Status', 'Path', 'PoP', 'Client IP'].map((h) => (
                    <th key={h} className="px-3 py-2 font-medium">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.event_id} className="border-t">
                    <td className="whitespace-nowrap px-3 py-2">{formatDate(event.timestamp, timezone, 'h:mm:ss a')}</td>
                    <ErrorFilterCell filterKey="status" value={event.status} activeServiceId={activeServiceId} bold />
                    <ErrorFilterCell
                      filterKey="url"
                      value={event.url}
                      activeServiceId={activeServiceId}
                      className="max-w-sm truncate px-3 py-2"
                    />
                    <ErrorFilterCell filterKey="pop" value={event.pop} activeServiceId={activeServiceId} />
                    <ErrorFilterCell
                      filterKey="client_ip"
                      value={event.client_ip}
                      activeServiceId={activeServiceId}
                      className="px-3 py-2 font-mono"
                    />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function OverviewTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
  errorEvents,
  errorsConnected,
  timezone,
}: TabCommonProps & {
  errorEvents: ErrorEvent[]
  errorsConnected: boolean
  timezone: string
}) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="overview">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        cards={[
          {
            title: 'Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.requests_per_second,
            helpText:
              'Total HTTP requests per second across all Fastly edge PoPs. Rolling averages smooth out per-second spikes — the 60s average is most representative of sustained load.',
          },
          {
            title: 'Error Rate',
            decimals: 2,
            suffix: '%',
            rateFn: (d) => d.error_rate * 100,
            thresholds: { warn: 1, critical: 5, direction: 'above' },
            helpText:
              'Percentage of responses with 4xx or 5xx status codes. Card turns amber above 1% and red above 5%.',
          },
          {
            title: 'Cache Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => d.cache_hit_ratio * 100,
            thresholds: { warn: 90, critical: 50, direction: 'below' },
            helpText:
              "Percentage of requests served from Fastly's edge cache without reaching your origin. Higher is better — below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content.",
          },
          {
            title: 'Bandwidth',
            decimals: 2,
            suffix: ' Mbps',
            rateFn: (d) => d.bandwidth_mbps,
            helpText:
              'Total bandwidth delivered across all PoPs in megabits per second, including both cached and origin-fetched content.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Total Requests"
          yAxisSuffix=" req/s"
          helpText="Per-second request count across all PoPs. Sudden spikes may indicate traffic surges, bot activity, or DDoS events."
          traces={seriesTraces(series, [
            ['Requests', CHART_COLORS.info, (d) => d.total_requests ?? d.requests_per_second],
          ])}
        />
        <RealtimeChart
          title="Status Code Breakdown"
          yAxisSuffix=" req/s"
          stacked
          helpText="Per-second breakdown by HTTP status class. 2xx = success, 3xx = redirects, 4xx = client errors (broken links, auth failures), 5xx = origin/server errors."
          traces={seriesTraces(series, [
            ['1xx', CHART_COLORS.neutral, (d) => d.status_breakdown?.status_1xx ?? 0],
            ['2xx', '#22c55e', (d) => d.status_breakdown?.status_2xx ?? 0],
            ['3xx', CHART_COLORS.info, (d) => d.status_breakdown?.status_3xx ?? 0],
            ['4xx', '#eab308', (d) => d.status_breakdown?.status_4xx ?? 0],
            ['5xx', CHART_COLORS.danger, (d) => d.status_breakdown?.status_5xx ?? 0],
          ])}
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Performance"
          yAxisSuffix=" req/s"
          stacked
          helpText="Hit = served from edge cache, Miss = fetched from origin and cached, Pass = bypassed cache entirely (uncacheable or VCL pass rules). A growing Pass ratio may indicate misconfigured cache rules."
          traces={seriesTraces(series, [
            ['Hit', '#22c55e', (d) => d.total_hits ?? 0],
            ['Miss', '#f97316', (d) => d.total_miss ?? 0],
            ['Pass', CHART_COLORS.neutral, (d) => d.total_pass ?? 0],
          ])}
        />
        <RealtimeChart
          title="Origin Load"
          dualYAxis
          yAxisSuffix=" req/s"
          y2AxisSuffix="%"
          helpText="Left axis: requests reaching your origin per second. Right axis: cache hit ratio — when it drops, more requests flow through to origin."
          traces={seriesTraces(series, [
            ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
            ['Cache Hit %', '#22c55e', (d) => (d.cache_hit_ratio ?? 0) * 100, 'y2'],
          ])}
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="2xx/3xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(
            series,
            (
              [
                ['200', '#22c55e'],
                ['204', '#86efac'],
                ['301', CHART_COLORS.info],
                ['302', '#93c5fd'],
                ['304', '#6366f1'],
              ] as const
            ).map(([c, col]) => [c, col, (d: MetricsData) => d.status_detail?.[c] ?? 0]),
          )}
        />
        <RealtimeChart
          title="4xx/5xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(
            series,
            (
              [
                ['400', '#eab308'],
                ['401', CHART_COLORS.warning],
                ['403', '#f97316'],
                ['404', '#fb923c'],
                ['429', CHART_COLORS.danger],
                ['500', '#dc2626'],
                ['502', '#b91c1c'],
                ['503', '#991b1b'],
                ['504', '#7f1d1d'],
              ] as const
            ).map(([c, col]) => [c, col, (d: MetricsData) => d.status_detail?.[c] ?? 0]),
          )}
        />
      </div>
      <RecentErrorsCard events={errorEvents} connected={errorsConnected} timezone={timezone} />
    </TabPanelShell>
  )
}

export function MobileOverview({
  connected,
  lastTickTime,
  timezone,
  tick,
  rtDown,
}: {
  connected: boolean
  lastTickTime: Date | null
  timezone: string
  tick: MetricsTick | null
  rtDown: boolean
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const data = tick?.data
  return (
    <div className="space-y-4 md:hidden">
      <div className="flex items-center gap-2 flex-wrap">
        <ConnectionBadge connected={connected} />
        <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
      </div>
      {rtDown && <RtDownBanner />}
      <Card>
        <CardHeader>
          <CardTitle>Live Overview</CardTitle>
        </CardHeader>
        <CardContent>
          {data ? (
            <div className={`grid grid-cols-2 gap-3 ${rtDown ? 'opacity-50' : ''}`}>
              <RealtimeMetricCard title="Requests/s" value={data.requests_per_second} suffix=" req/s" />
              <RealtimeMetricCard
                title="Error Rate"
                value={Number((data.error_rate * 100).toFixed(2))}
                suffix="%"
                thresholds={{ warn: 1, critical: 5, direction: 'above' }}
              />
              <RealtimeMetricCard
                title="Cache Hit"
                value={Number((data.cache_hit_ratio * 100).toFixed(1))}
                suffix="%"
                thresholds={{ warn: 90, critical: 50, direction: 'below' }}
              />
              <RealtimeMetricCard title="Bandwidth" value={Number(data.bandwidth_mbps.toFixed(2))} suffix=" Mbps" />
            </div>
          ) : (
            <p className="text-muted-foreground text-sm">Waiting for metrics stream...</p>
          )}
        </CardContent>
      </Card>
      <Link href={buildServiceHref('/dashboard', activeServiceId)} prefetch={false}>
        <Button variant="outline" size="sm" className="w-full">
          View full dashboard
          <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </Link>
    </div>
  )
}
