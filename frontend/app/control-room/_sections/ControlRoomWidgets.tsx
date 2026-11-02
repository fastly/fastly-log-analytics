'use client'

import React, { useState, useMemo } from 'react'
import { ArrowRight, AlertTriangle, HelpCircle } from 'lucide-react'
import Link from 'next/link'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import type { components } from '@/types/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { PlotlyChart } from '@/components/PlotlyChart'
import { useNowMs } from '@/hooks/useSSE'
import { useMounted } from '@/hooks/useBootstrap'
import { adminJson } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import { formatCount, formatDate, buildServiceHref, cn } from '@/lib/utils'
import {
  type MetricsData,
  type MetricsTick,
  type RealtimeStreamState,
  type ErrorEvent,
  type RollingAvgFn,
  type SeriesFn,
  type TabCommonProps,
  type MetricCardSpec,
  useTickHistory,
  useErrorStream,
  useRealtimeStream,
  makeRates,
  seriesTraces,
} from './useControlRoomStream'

export {
  type MetricsData,
  type MetricsTick,
  type RealtimeStreamState,
  type ErrorEvent,
  type RollingAvgFn,
  type SeriesFn,
  type TabCommonProps,
  type MetricCardSpec,
  useTickHistory,
  useErrorStream,
  useRealtimeStream,
  seriesTraces,
}

interface RealtimeMetricCardProps {
  title: string
  value: number
  suffix?: string
  rates?: Array<{ label: string; value: number }>
  thresholds?: { warn: number; critical: number; direction: 'above' | 'below' }
  dimmed?: boolean
  helpText?: string
}

function getThresholdLevel(
  value: number,
  thresholds: RealtimeMetricCardProps['thresholds'],
): 'ok' | 'warn' | 'critical' {
  if (!thresholds) return 'ok'
  return thresholds.direction === 'above'
    ? value > thresholds.critical
      ? 'critical'
      : value > thresholds.warn
        ? 'warn'
        : 'ok'
    : value < thresholds.critical
      ? 'critical'
      : value < thresholds.warn
        ? 'warn'
        : 'ok'
}

function formatRate(value: number, suffix?: string): string {
  if (suffix?.includes('%')) return `${value.toFixed(1)}%`
  if (suffix?.includes('Mbps')) return `${value.toFixed(1)}`
  return value >= 1000 ? `${(value / 1000).toFixed(1)}k` : value < 10 ? value.toFixed(1) : Math.round(value).toString()
}

const LEVEL_CLASSES = {
  ok: '',
  warn: 'bg-amber-50 dark:bg-amber-950/20',
  critical: 'bg-red-50 dark:bg-red-950/20',
} as const

function RealtimeCardShell({
  title,
  helpText,
  className,
  headerClassName = 'px-4 pt-4 pb-0',
  children,
}: {
  title: string
  helpText?: string
  className?: string
  headerClassName?: string
  children: React.ReactNode
}) {
  const [helpOpen, setHelpOpen] = useState(false)
  return (
    <Card className={className}>
      <CardHeader className={headerClassName}>
        <div className="flex items-center justify-between">
          <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</CardTitle>
          {helpText && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`About ${title}`}
              className="h-6 w-6 text-muted-foreground hover:text-foreground"
              onClick={() => setHelpOpen(true)}
            >
              <HelpCircle className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent className="relative px-4 pb-4 pt-0">{children}</CardContent>
      {helpText && (
        <HelpDialog open={helpOpen} onOpenChange={setHelpOpen} title={title}>
          <p>{helpText}</p>
        </HelpDialog>
      )}
    </Card>
  )
}

export function RealtimeMetricCard({
  title,
  value,
  suffix,
  rates,
  thresholds,
  dimmed,
  helpText,
}: RealtimeMetricCardProps) {
  return (
    <RealtimeCardShell
      title={title}
      helpText={helpText}
      headerClassName="relative px-4 pt-4 pb-0"
      className={cn(
        'relative overflow-hidden transition-colors',
        LEVEL_CLASSES[getThresholdLevel(value, thresholds)],
        dimmed && 'border-dashed border-muted-foreground/40 bg-muted/5',
      )}
    >
      <div className="flex items-baseline min-h-[36px] whitespace-nowrap">
        <span className="text-3xl font-bold tabular-nums">
          {value}
          {suffix && <span className="text-lg font-normal text-muted-foreground ml-1">{suffix.trim()}</span>}
        </span>
      </div>
      {rates && (
        <div className="mt-1 min-h-[16px] text-xs text-muted-foreground tabular-nums whitespace-nowrap truncate">
          {rates.map((r, i) => (
            <span key={r.label}>
              {i > 0 && <span className="mx-1">&middot;</span>}
              {r.label}: {formatRate(r.value, suffix)}
            </span>
          ))}
        </div>
      )}
    </RealtimeCardShell>
  )
}

interface RealtimeChartProps {
  title: string
  traces: Array<{ y: number[]; name: string; color?: string; yaxis?: 'y' | 'y2' }>
  height?: number
  className?: string
  stacked?: boolean
  dualYAxis?: boolean
  helpText?: string
  yAxisSuffix?: string
  y2AxisSuffix?: string
}

const CHART_WINDOW = 60
const STATIC_CONFIG = { displayModeBar: false, responsive: true }

function makeTimeLabels(count: number): string[] {
  const now = Date.now()
  return Array.from({ length: count }, (_, i) => {
    const d = new Date(now - (count - 1 - i) * 1000)
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`
  })
}

export function RealtimeChart({
  title,
  traces,
  height = 180,
  className,
  stacked,
  dualYAxis,
  helpText,
  yAxisSuffix,
  y2AxisSuffix,
}: RealtimeChartProps) {
  const mounted = useMounted()
  const pointCount = traces[0]?.y.length ?? CHART_WINDOW
  const data = useMemo(() => {
    if (!mounted) return []
    const labels = makeTimeLabels(pointCount)
    return traces.map((trace) => ({
      type: 'bar' as const,
      x: labels,
      y: trace.y,
      name: trace.name,
      marker: { color: trace.color },
      yaxis: trace.yaxis || 'y',
      hovertemplate: `%{x} · %{y:,.4~g}<extra>${trace.name}</extra>`,
    }))
  }, [mounted, traces, pointCount])

  const hasLegend = traces.length > 1
  const layout = useMemo(() => {
    const l: Record<string, unknown> = {
      xaxis: { type: 'category', tickangle: -45, nticks: 6, tickfont: { size: 9 }, range: [-0.5, CHART_WINDOW - 0.5] },
      yaxis: {
        showgrid: true,
        gridcolor: 'rgba(128,128,128,0.15)',
        zeroline: false,
        autorange: true,
        rangemode: 'tozero',
        exponentformat: 'SI',
        ...(yAxisSuffix ? { ticksuffix: yAxisSuffix } : {}),
      },
      barmode: stacked ? 'stack' : 'group',
      bargap: 0.02,
      margin: {
        l: yAxisSuffix ? 55 : 42,
        r: dualYAxis ? (y2AxisSuffix ? 55 : 42) : 15,
        t: hasLegend ? 32 : 8,
        b: 32,
      },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      showlegend: hasLegend,
      legend: {
        orientation: 'h',
        y: 1.05,
        x: 0.5,
        xanchor: 'center',
        yanchor: 'bottom',
        font: { size: 9.5 },
        bgcolor: 'transparent',
      },
      uirevision: 'stable',
      font: { size: 11 },
      hovermode: 'x unified',
    }
    if (dualYAxis) {
      l.yaxis2 = {
        side: 'right',
        overlaying: 'y',
        showgrid: false,
        zeroline: false,
        autorange: true,
        rangemode: 'tozero',
        exponentformat: 'SI',
        ...(y2AxisSuffix ? { ticksuffix: y2AxisSuffix } : {}),
      }
    }
    return l
  }, [stacked, dualYAxis, hasLegend, yAxisSuffix, y2AxisSuffix])

  return (
    <RealtimeCardShell title={title} helpText={helpText} className={className}>
      <PlotlyChart data={data} layout={layout} config={STATIC_CONFIG} height={height} a11yTitle={title} />
    </RealtimeCardShell>
  )
}

const FRESHNESS_VARIANTS = { fresh: 'success', stale: 'warning', expired: 'destructive' } as const
function getFreshnessLevel(lastTick: Date | null): keyof typeof FRESHNESS_VARIANTS {
  if (!lastTick) return 'expired'
  const ageMs = Date.now() - lastTick.getTime()
  return ageMs < 10_000 ? 'fresh' : ageMs < 30_000 ? 'stale' : 'expired'
}

export function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <Badge variant={connected ? 'success' : 'destructive'} className="gap-1.5 min-w-[116px] justify-center">
      <span
        className={`inline-block h-2 w-2 rounded-full ${connected ? 'bg-emerald-500 dark:bg-emerald-400' : 'bg-red-500 dark:bg-red-400'}`}
      />
      {connected ? 'Connected' : 'Disconnected'}
    </Badge>
  )
}

export function FreshnessBadge({ lastTickTime, timezone }: { lastTickTime: Date | null; timezone: string }) {
  useNowMs()
  return (
    <Badge variant={FRESHNESS_VARIANTS[getFreshnessLevel(lastTickTime)]} className="gap-1.5 min-w-[220px] justify-center tabular-nums whitespace-nowrap">
      {lastTickTime ? `Fresh as of ${formatDate(lastTickTime, timezone, 'h:mm:ss a')}` : 'No data yet'}
    </Badge>
  )
}

export function RtDownBanner() {
  return (
    <Alert variant="destructive" className="mb-4">
      <AlertTriangle className="h-4 w-4" />
      <AlertDescription>Fastly RT unavailable — showing last-known-good values.</AlertDescription>
    </Alert>
  )
}

type CorrelatorResult = components['schemas']['CorrelateResponse']

export function CorrelatorLeaderboard({
  dimension,
  serviceId,
  refreshInterval = 30_000,
  limit = 10,
}: {
  dimension: string
  serviceId: string | null
  refreshInterval?: number
  limit?: number
}) {
  const { data, isFetching: loading } = useQuery<CorrelatorResult | null>({
    queryKey: ['control-room', 'correlate', serviceId, dimension, limit],
    queryFn: ({ signal }) =>
      adminJson<'/api/services/{service_id}/control-room/correlate', CorrelatorResult, 'post'>(
        '/api/services/{service_id}/control-room/correlate',
        `/api/services/${serviceId}/control-room/correlate`,
        {
          method: 'POST',
          signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ dimension, window_minutes: 60, limit }),
        },
      ),
    enabled: !!serviceId,
    placeholderData: keepPreviousData,
    refetchInterval: refreshInterval,
    refetchIntervalInBackground: false,
    retry: false,
  })
  const maxCount = data?.top?.[0]?.count ?? 1
  return (
    <div className="space-y-3">
      {data?.freshness && (
        <div className="flex justify-end">
          <Badge variant={data.freshness.lag_seconds > 60 ? 'warning' : 'success'} className="text-xs">
            {data.freshness.lag_seconds > 0 ? `${data.freshness.lag_seconds}s behind` : 'live'}
          </Badge>
        </div>
      )}
      <div className={`space-y-1.5 transition-opacity ${loading && data ? 'opacity-40 pointer-events-none' : ''}`}>
        {loading && !data && (
          <div className="text-sm text-muted-foreground py-8 text-center animate-pulse">Loading leaderboard...</div>
        )}
        {data?.top?.map((item, i) => (
          <div key={item.value} className="group flex items-center gap-2 text-sm">
            <span className="w-5 text-right text-xs text-muted-foreground tabular-nums shrink-0">{i + 1}</span>
            <div className="flex-1 min-w-0 relative">
              <div
                className="absolute inset-y-0 left-0 rounded-sm bg-blue-500/15 dark:bg-blue-400/15 transition-all"
                style={{ width: `${Math.max(4, (item.count / maxCount) * 100)}%` }}
              />
              <div className="relative flex items-center justify-between px-2 py-1">
                <span className="truncate text-xs font-mono">{item.value || '(empty)'}</span>
                <span className="text-xs tabular-nums text-muted-foreground ml-2 shrink-0">
                  {formatCount(item.count)}
                </span>
              </div>
            </div>
          </div>
        ))}
        {data?.top?.length === 0 && <div className="text-sm text-muted-foreground py-8 text-center">No data in window</div>}
      </div>
    </div>
  )
}

const GRID_COL_CLASSES: Record<2 | 3 | 4 | 5, string> = {
  2: 'grid gap-6 md:grid-cols-2',
  3: 'grid gap-6 md:grid-cols-2 lg:grid-cols-3',
  4: 'grid gap-6 md:grid-cols-2 lg:grid-cols-4',
  5: 'grid gap-6 md:grid-cols-2 lg:grid-cols-5',
}

export function MetricCardGrid({
  cols = 4,
  data,
  cards = [],
  rollingAvg,
  rtDown,
  dimmed,
  prefix,
}: {
  cols?: 2 | 3 | 4 | 5
  data?: MetricsData
  cards?: MetricCardSpec[]
  rollingAvg?: RollingAvgFn
  rtDown?: boolean
  dimmed?: boolean
  prefix?: React.ReactNode
}) {
  const isDimmed = rtDown ?? dimmed ?? false
  return (
    <div className={GRID_COL_CLASSES[cols]}>
      {prefix}
      {cards.map((item) => {
        const rawVal = item.value ?? (data && item.rateFn ? item.rateFn(data) : 0)
        return (
          <RealtimeMetricCard
            key={item.title}
            title={item.title}
            value={item.decimals !== undefined ? Number(rawVal.toFixed(item.decimals)) : rawVal}
            suffix={item.suffix}
            rates={item.rates ?? (item.rateFn && rollingAvg ? makeRates(rollingAvg, item.rateFn) : undefined)}
            thresholds={
              item.thresholds
                ? {
                    warn: item.thresholds.warn,
                    critical: item.thresholds.critical,
                    direction: (item.thresholds.direction as 'above' | 'below' | undefined) ?? 'above',
                  }
                : undefined
            }
            dimmed={isDimmed}
            helpText={item.helpText}
          />
        )
      })}
    </div>
  )
}

export function HistoricalLink({ href, label }: { href: string; label: string }) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  return (
    <div className="mt-4 flex justify-end">
      <Link href={buildServiceHref(href, activeServiceId)} prefetch={false}>
        <Button variant="outline" size="sm">
          View historical {label} data
          <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </Link>
    </div>
  )
}

export function TabPanelShell({
  rtDown,
  historicalHref,
  historicalLabel,
  children,
}: {
  rtDown: boolean
  historicalHref?: string
  historicalLabel?: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      {children}
      {historicalHref && historicalLabel && <HistoricalLink href={historicalHref} label={historicalLabel} />}
    </div>
  )
}

export function SectionHeading({ children }: { children: React.ReactNode }) {
  return <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">{children}</h3>
}
