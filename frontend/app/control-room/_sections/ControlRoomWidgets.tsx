'use client'

import React, { useState, useEffect, useMemo, useCallback } from 'react'
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
import { useSSE, useNowMs, type SSELine } from '@/hooks/useSSE'
import { useMounted } from '@/hooks/useBootstrap'
import { adminJson, adminJsonResult, getPageLoadId } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import { formatCount, formatDate, buildServiceHref, cn } from '@/lib/utils'

export interface MetricsData {
  requests_per_second: number
  error_rate: number
  cache_hit_ratio: number
  bandwidth_mbps: number
  total_requests?: number
  total_hits?: number
  total_miss?: number
  total_pass?: number
  total_errors?: number
  status_breakdown?: Record<string, number>
  estimated_cost_usd?: number
  origin_requests_per_second?: number
  origin_bandwidth_mbps?: number
  origin_offload?: number
  shield_requests?: number
  shield_hit_ratio?: number
  pass_requests?: number
  synth_requests?: number
  waf_blocked?: number
  waf_logged?: number
  waf_passed?: number
  pop_count?: number
  degraded_pops?: string[]
  hit_latency_ms?: number
  miss_latency_ms?: number
  pass_latency_ms?: number
  http2?: number
  http3?: number
  ipv6?: number
  tls_v12?: number
  tls_v13?: number
  h2_pct?: number
  h3_pct?: number
  ipv6_pct?: number
  tls12_pct?: number
  tls13_pct?: number
  ddos_action_blackhole?: number
  ddos_action_tarpit?: number
  ddos_action_close?: number
  ddos_action_downgrade?: number
  ddos_detect?: number
  ddos_mitigate?: number
  status_detail?: Record<string, number>
  object_size_distribution?: Record<string, number>
  miss_histogram?: Record<string, number>
  origin_fetches?: number
  origin_revalidations?: number
  origin_cache_fetches?: number
  shield_hit_requests?: number
  shield_miss_requests?: number
  shield_revalidations?: number
  shield_fetch_body_bytes?: number
  request_collapse_usable?: number
  request_collapse_unusable?: number
  segblock_origin_fetches?: number
  segblock_shield_fetches?: number
  bot_challenge_starts?: number
  bot_challenges_issued?: number
  bot_challenges_succeeded?: number
  bot_challenges_failed?: number
  bot_detected?: number
  bot_verified?: number
  bot_ai_crawlers?: number
  compute_exec_time_ms?: number
  compute_req_time_ms?: number
  compute_ram_used?: number
  compute_bereq_errors?: number
  compute_guest_errors?: number
  compute_resource_exceeded?: number
  restarts?: number
  vcl_recv_count?: number
  vcl_recv_time_ms?: number
  vcl_fetch_count?: number
  vcl_fetch_time_ms?: number
  vcl_deliver_count?: number
  vcl_deliver_time_ms?: number
  vcl_error_count?: number
  vcl_error_time_ms?: number
  top_pops?: Array<{
    name: string
    requests: number
    errors: number
    hits: number
    miss: number
    hit_ratio: number
    error_rate: number
  }>
  all_pops?: Record<string, { r: number; e: number }>
}

export interface MetricsTick {
  event: string
  event_schema_version: number
  timestamp: string
  status: 'ok' | 'rt_down'
  data: MetricsData
  aggregate_delay?: number
}

interface TickEntry {
  timestamp: number
  data: MetricsData
}

const MAX_ENTRIES = 60
const EMPTY_DATA: MetricsData = {
  requests_per_second: 0,
  error_rate: 0,
  cache_hit_ratio: 0,
  bandwidth_mbps: 0,
}

export function useTickHistory(allTicks: MetricsTick[]) {
  const history = useMemo<TickEntry[]>(() => {
    const entries = (allTicks.length > MAX_ENTRIES ? allTicks.slice(-MAX_ENTRIES) : allTicks).map((t) => ({
      timestamp: new Date(t.timestamp).getTime(),
      data: t.data,
    }))
    return entries.length < MAX_ENTRIES
      ? [...Array.from({ length: MAX_ENTRIES - entries.length }, () => ({ timestamp: 0, data: EMPTY_DATA })), ...entries]
      : entries
  }, [allTicks])

  const prevTick = allTicks.length >= 2 ? allTicks[allTicks.length - 2] : null
  const series = useCallback((extractor: (d: MetricsData) => number): number[] => history.map((e) => extractor(e.data)), [history])
  const rollingAvg = useCallback(
    (extractor: (d: MetricsData) => number, windowSize: number): number => {
      const window = history.filter((e) => e.timestamp > 0).slice(-windowSize)
      return window.length === 0 ? 0 : window.reduce((sum, e) => sum + extractor(e.data), 0) / window.length
    },
    [history],
  )
  return { history, prevTick, series, rollingAvg }
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
      <div className="flex items-baseline">
        <span className="text-3xl font-bold tabular-nums">{value}</span>
        {suffix && <span className="text-lg font-normal text-muted-foreground ml-1">{suffix.trim()}</span>}
      </div>
      {rates && rates.length > 0 && (
        <div className="mt-1 text-xs text-muted-foreground tabular-nums">
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

export interface RealtimeStreamState {
  connected: boolean
  lastTickTime: Date | null
  latestTick: MetricsTick | null
  allTicks: MetricsTick[]
  rtDown: boolean
}

export interface ErrorEvent {
  event_id: string
  timestamp: string
  status: number
  url: string
  pop: string
  client_ip: string
}

function parseErrorEvent(line: SSELine): ErrorEvent | null {
  if (line.event !== 'error_event' && line.type !== 'error_event') return null
  const v = line.data as Partial<ErrorEvent> | undefined
  if (!v || typeof v !== 'object' || typeof v.event_id !== 'string' || typeof v.timestamp !== 'string' || typeof v.status !== 'number') {
    return null
  }
  return {
    event_id: v.event_id,
    timestamp: v.timestamp,
    status: v.status,
    url: typeof v.url === 'string' ? v.url : '',
    pop: typeof v.pop === 'string' ? v.pop : '',
    client_ip: typeof v.client_ip === 'string' ? v.client_ip : '',
  }
}

export function useErrorStream(): { connected: boolean; events: ErrorEvent[] } {
  const { lines, status, start, reset } = useSSE({ maxLines: 500 })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  useEffect(() => {
    if (!activeServiceId) return
    const pageLoadId = getPageLoadId()
    start(
      `/api/services/${activeServiceId}/control-room/error-stream`,
      undefined,
      pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined,
    )
    return () => reset()
  }, [activeServiceId, start, reset])

  const events = useMemo(() => {
    const seen = new Set<string>()
    return lines
      .map(parseErrorEvent)
      .filter((e): e is ErrorEvent => e !== null && !seen.has(e.event_id) && !!seen.add(e.event_id))
      .slice(-20)
  }, [lines])

  return { connected: status === 'streaming', events }
}

function parseMetricsTick(line: SSELine): MetricsTick | null {
  if (line.event !== 'metrics_tick' && line.type !== 'metrics_tick') return null
  const metricsData = line.data as MetricsData | undefined
  if (!metricsData || typeof metricsData !== 'object') return null
  return {
    event: 'metrics_tick',
    event_schema_version: (line.event_schema_version as number) ?? 1,
    timestamp: (line.timestamp as string) ?? new Date().toISOString(),
    status: (line.status as 'ok' | 'rt_down') ?? 'ok',
    data: metricsData,
    aggregate_delay: line.aggregate_delay as number | undefined,
  }
}

function isTick(t: unknown): t is MetricsTick {
  return (
    !!t &&
    typeof t === 'object' &&
    (t as Record<string, unknown>).event === 'metrics_tick' &&
    !!(t as Record<string, unknown>).data
  )
}

export function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, reset } = useSSE({ maxLines: 600 })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const [seed, setSeed] = useState<{ ticks: MetricsTick[]; timestamps: Set<string> }>({
    ticks: [],
    timestamps: new Set(),
  })

  useEffect(() => {
    if (!activeServiceId) return
    const ac = new AbortController()
    const pageLoadId = getPageLoadId()
    start(
      `/api/services/${activeServiceId}/realtime-stream`,
      undefined,
      pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined,
    )
    ;(async () => {
      let seedTicks: MetricsTick[] = []
      try {
        const res = await adminJsonResult(
          '/api/services/{service_id}/realtime-seed',
          `/api/services/${activeServiceId}/realtime-seed`,
          { signal: ac.signal },
        )
        if (res.ok) seedTicks = ((res.data.ticks ?? []) as unknown[]).filter(isTick)
      } catch {
        /* Seed is best-effort */
      }
      if (!ac.signal.aborted) {
        setSeed({ ticks: seedTicks, timestamps: new Set(seedTicks.map((t) => t.timestamp)) })
      }
    })()
    return () => {
      ac.abort()
      reset()
      setSeed({ ticks: [], timestamps: new Set() })
    }
  }, [activeServiceId, start, reset])

  const allTicks = useMemo(
    () => [
      ...seed.ticks,
      ...lines.map(parseMetricsTick).filter((t): t is MetricsTick => t !== null && !seed.timestamps.has(t.timestamp)),
    ],
    [lines, seed],
  )
  const latestTick = allTicks.length > 0 ? allTicks[allTicks.length - 1] : null
  return {
    connected: status === 'streaming',
    lastTickTime: latestTick ? new Date(latestTick.timestamp) : null,
    latestTick,
    allTicks,
    rtDown: latestTick?.status === 'rt_down',
  }
}

const FRESHNESS_VARIANTS = { fresh: 'success', stale: 'warning', expired: 'destructive' } as const
function getFreshnessLevel(lastTick: Date | null): keyof typeof FRESHNESS_VARIANTS {
  if (!lastTick) return 'expired'
  const ageMs = Date.now() - lastTick.getTime()
  return ageMs < 10_000 ? 'fresh' : ageMs < 30_000 ? 'stale' : 'expired'
}

export function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <Badge variant={connected ? 'success' : 'destructive'} className="gap-1.5">
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
    <Badge variant={FRESHNESS_VARIANTS[getFreshnessLevel(lastTickTime)]} className="gap-1.5">
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

export type RollingAvgFn = (extractor: (d: MetricsData) => number, windowSize: number) => number
export type SeriesFn = (extractor: (d: MetricsData) => number) => number[]
export interface TabCommonProps {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: SeriesFn
  rollingAvg: RollingAvgFn
}

export interface MetricCardSpec {
  title: string
  value?: number
  decimals?: number
  suffix?: string
  rateFn?: (d: MetricsData) => number
  rates?: Array<{ label: string; value: number }>
  thresholds?: { warn: number; critical: number; direction?: 'above' | 'below' | string }
  helpText?: string
}

const GRID_COL_CLASSES: Record<2 | 3 | 4 | 5, string> = {
  2: 'grid gap-6 md:grid-cols-2',
  3: 'grid gap-6 md:grid-cols-2 lg:grid-cols-3',
  4: 'grid gap-6 md:grid-cols-2 lg:grid-cols-4',
  5: 'grid gap-6 md:grid-cols-2 lg:grid-cols-5',
}

function makeRates(rollingAvg: RollingAvgFn, fn: (d: MetricsData) => number) {
  return [
    { label: '5s', value: rollingAvg(fn, 5) },
    { label: '30s', value: rollingAvg(fn, 30) },
    { label: '60s', value: rollingAvg(fn, 60) },
  ]
}

export function seriesTraces(
  series: SeriesFn,
  specs: ReadonlyArray<readonly [name: string, color: string, fn: (d: MetricsData) => number, yaxis?: 'y' | 'y2']>,
) {
  return specs.map(([name, color, fn, yaxis]) => ({ y: series(fn), name, color, ...(yaxis ? { yaxis } : {}) }))
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
