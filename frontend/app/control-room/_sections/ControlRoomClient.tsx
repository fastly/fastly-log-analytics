'use client'

import { useState, useEffect, useMemo, useCallback } from 'react'
import { Radio, ArrowRight, AlertTriangle, BarChart3, Gauge, HardDrive, Shield, Network, Settings, HelpCircle } from 'lucide-react'
import Link from 'next/link'
import { ReportShell } from '@/components/ReportLayout'
import type { components } from '@/types/api'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { PlotlyChart } from '@/components/PlotlyChart'
import { useSSE, useTimezone, useNowMs, type SSELine } from '@/hooks/useSSE'
import { adminJson, adminJsonResult, getPageLoadId } from '@/lib/api'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/filterStore'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import { formatDate, formatCount } from '@/lib/format'
import { buildServiceHref, cn } from '@/lib/utils'
import dynamic from 'next/dynamic'
import { CHART_COLORS } from '@/lib/chart-helpers'

export interface MetricsData {
  requests_per_second: number; error_rate: number; cache_hit_ratio: number; bandwidth_mbps: number
  total_requests?: number; total_hits?: number; total_miss?: number; total_pass?: number; total_errors?: number
  status_breakdown?: Record<string, number>; estimated_cost_usd?: number
  origin_requests_per_second?: number; origin_bandwidth_mbps?: number; origin_offload?: number
  shield_requests?: number; shield_hit_ratio?: number; pass_requests?: number; synth_requests?: number
  waf_blocked?: number; waf_logged?: number; waf_passed?: number
  pop_count?: number; degraded_pops?: string[]
  hit_latency_ms?: number; miss_latency_ms?: number; pass_latency_ms?: number
  http2?: number; http3?: number; ipv6?: number; tls_v12?: number; tls_v13?: number
  h2_pct?: number; h3_pct?: number; ipv6_pct?: number; tls12_pct?: number; tls13_pct?: number
  ddos_action_blackhole?: number; ddos_action_tarpit?: number; ddos_action_close?: number; ddos_action_downgrade?: number
  ddos_detect?: number; ddos_mitigate?: number
  status_detail?: Record<string, number>; object_size_distribution?: Record<string, number>; miss_histogram?: Record<string, number>
  origin_fetches?: number; origin_revalidations?: number; origin_cache_fetches?: number
  shield_hit_requests?: number; shield_miss_requests?: number; shield_revalidations?: number; shield_fetch_body_bytes?: number
  request_collapse_usable?: number; request_collapse_unusable?: number
  segblock_origin_fetches?: number; segblock_shield_fetches?: number
  bot_challenge_starts?: number; bot_challenges_issued?: number; bot_challenges_succeeded?: number; bot_challenges_failed?: number
  bot_detected?: number; bot_verified?: number; bot_ai_crawlers?: number
  compute_exec_time_ms?: number; compute_req_time_ms?: number; compute_ram_used?: number
  compute_bereq_errors?: number; compute_guest_errors?: number; compute_resource_exceeded?: number
  restarts?: number
  vcl_recv_count?: number; vcl_recv_time_ms?: number; vcl_fetch_count?: number; vcl_fetch_time_ms?: number
  vcl_deliver_count?: number; vcl_deliver_time_ms?: number; vcl_error_count?: number; vcl_error_time_ms?: number
  top_pops?: Array<{ name: string; requests: number; errors: number; hits: number; miss: number; hit_ratio: number; error_rate: number }>
  all_pops?: Record<string, { r: number; e: number }>
}

export interface MetricsTick {
  event: string; event_schema_version: number; timestamp: string; status: 'ok' | 'rt_down'; data: MetricsData; aggregate_delay?: number
}

interface TickEntry { timestamp: number; data: MetricsData }
const MAX_ENTRIES = 60
const EMPTY_DATA: MetricsData = { requests_per_second: 0, error_rate: 0, cache_hit_ratio: 0, bandwidth_mbps: 0 }

export function useTickHistory(allTicks: MetricsTick[]) {
  const history = useMemo<TickEntry[]>(() => {
    const window = allTicks.length > MAX_ENTRIES ? allTicks.slice(allTicks.length - MAX_ENTRIES) : allTicks
    const entries = window.map((t) => ({ timestamp: new Date(t.timestamp).getTime(), data: t.data }))
    if (entries.length < MAX_ENTRIES) {
      const pad = Array.from({ length: MAX_ENTRIES - entries.length }, () => ({ timestamp: 0, data: EMPTY_DATA }))
      return [...pad, ...entries]
    }
    return entries
  }, [allTicks])

  const prevTick = allTicks.length >= 2 ? allTicks[allTicks.length - 2] : null
  const series = useCallback((extractor: (d: MetricsData) => number): number[] => history.map((e) => extractor(e.data)), [history])
  const rollingAvg = useCallback((extractor: (d: MetricsData) => number, windowSize: number): number => {
    const realEntries = history.filter((e) => e.timestamp > 0)
    if (realEntries.length === 0) return 0
    const window = realEntries.slice(-windowSize)
    return window.reduce((sum, e) => sum + extractor(e.data), 0) / window.length
  }, [history])

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

function getThresholdLevel(value: number, thresholds: RealtimeMetricCardProps['thresholds']): 'ok' | 'warn' | 'critical' {
  if (!thresholds) return 'ok'
  const { warn, critical, direction } = thresholds
  if (direction === 'above') return value > critical ? 'critical' : value > warn ? 'warn' : 'ok'
  return value < critical ? 'critical' : value < warn ? 'warn' : 'ok'
}

function formatRate(value: number, suffix?: string): string {
  if (suffix?.includes('%')) return `${value.toFixed(1)}%`
  if (suffix?.includes('Mbps')) return `${value.toFixed(1)}`
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
  return value < 10 ? value.toFixed(1) : Math.round(value).toString()
}

const LEVEL_CLASSES = { ok: '', warn: 'bg-amber-50 dark:bg-amber-950/20', critical: 'bg-red-50 dark:bg-red-950/20' } as const

export function RealtimeMetricCard({ title, value, suffix, rates, thresholds, dimmed, helpText }: RealtimeMetricCardProps) {
  const [helpOpen, setHelpOpen] = useState(false)
  const level = getThresholdLevel(value, thresholds)

  return (
    <Card className={cn('relative overflow-hidden transition-colors', LEVEL_CLASSES[level], dimmed && 'border-dashed border-muted-foreground/40 bg-muted/5')}>
      <CardHeader className="relative px-4 pt-4 pb-0">
        <div className="flex items-center justify-between">
          <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</CardTitle>
          {helpText && (
            <Button variant="ghost" size="icon" aria-label={`About ${title}`} className="h-6 w-6 text-muted-foreground hover:text-foreground" onClick={() => setHelpOpen(true)}>
              <HelpCircle className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent className="relative px-4 pb-4 pt-0">
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
      </CardContent>
      {helpText && <HelpDialog open={helpOpen} onOpenChange={setHelpOpen} title={title}><p>{helpText}</p></HelpDialog>}
    </Card>
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
  const now = new Date()
  return Array.from({ length: count }, (_, i) => {
    const d = new Date(now.getTime() - (count - 1 - i) * 1000)
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`
  })
}

export function RealtimeChart({ title, traces, height = 180, className, stacked, dualYAxis, helpText, yAxisSuffix, y2AxisSuffix }: RealtimeChartProps) {
  const [helpOpen, setHelpOpen] = useState(false)
  const pointCount = traces[0]?.y.length ?? CHART_WINDOW
  const labels = useMemo(() => makeTimeLabels(pointCount), [traces, pointCount])
  const data = useMemo(() => traces.map((trace) => ({
    type: 'bar' as const, x: labels, y: trace.y, name: trace.name,
    marker: { color: trace.color }, yaxis: trace.yaxis || 'y',
    hovertemplate: `%{x} · %{y:,.4~g}<extra>${trace.name}</extra>`,
  })), [traces, labels])

  const hasLegend = traces.length > 1
  const layout = useMemo(() => {
    const l: Record<string, unknown> = {
      xaxis: { type: 'category', tickangle: -45, nticks: 6, tickfont: { size: 9 }, range: [-0.5, CHART_WINDOW - 0.5] },
      yaxis: { showgrid: true, gridcolor: 'rgba(128,128,128,0.15)', zeroline: false, autorange: true, rangemode: 'tozero', exponentformat: 'SI', ...(yAxisSuffix ? { ticksuffix: yAxisSuffix } : {}) },
      barmode: stacked ? 'stack' : 'group', bargap: 0.02,
      margin: { l: yAxisSuffix ? 55 : 42, r: dualYAxis ? (y2AxisSuffix ? 55 : 42) : 15, t: hasLegend ? 32 : 8, b: 32 },
      plot_bgcolor: 'transparent', paper_bgcolor: 'transparent', showlegend: hasLegend,
      legend: { orientation: 'h', y: 1.05, x: 0.5, xanchor: 'center', yanchor: 'bottom', font: { size: 9.5 }, bgcolor: 'transparent' },
      uirevision: 'stable', font: { size: 11 }, hovermode: 'x unified',
    }
    if (dualYAxis) {
      l.yaxis2 = { side: 'right', overlaying: 'y', showgrid: false, zeroline: false, autorange: true, rangemode: 'tozero', exponentformat: 'SI', ...(y2AxisSuffix ? { ticksuffix: y2AxisSuffix } : {}) }
    }
    return l
  }, [stacked, dualYAxis, hasLegend, yAxisSuffix, y2AxisSuffix])

  return (
    <Card className={className}>
      <CardHeader className="px-4 pt-4 pb-0">
        <div className="flex items-center justify-between">
          <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</CardTitle>
          {helpText && (
            <Button variant="ghost" size="icon" aria-label={`About ${title}`} className="h-6 w-6 text-muted-foreground hover:text-foreground" onClick={() => setHelpOpen(true)}>
              <HelpCircle className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent className="px-4 pb-4 pt-0">
        <PlotlyChart data={data} layout={layout} config={STATIC_CONFIG} height={height} a11yTitle={title} />
      </CardContent>
      {helpText && <HelpDialog open={helpOpen} onOpenChange={setHelpOpen} title={title}><p>{helpText}</p></HelpDialog>}
    </Card>
  )
}

const PopTrafficMap = dynamic(() => import('@/components/Map/PopTrafficMap').then((m) => m.PopTrafficMap), { ssr: false })

// ---------------------------------------------------------------------------
// Tab definitions
// ---------------------------------------------------------------------------

const TABS = [
  { id: 'overview', label: 'Summary', icon: BarChart3, historicalHref: '/dashboard' },
  { id: 'origin', label: 'CDN & Caching', icon: HardDrive, historicalHref: '/origin' },
  { id: 'security', label: 'Security', icon: Shield, historicalHref: '/security' },
  { id: 'performance', label: 'Performance', icon: Gauge, historicalHref: '/performance' },
  { id: 'network', label: 'Network', icon: Network, historicalHref: '/network' },
  { id: 'admin', label: 'Admin', icon: Settings, historicalHref: null, adminOnly: true },
] as const

type TabId = (typeof TABS)[number]['id']

// ---------------------------------------------------------------------------
// useRealtimeStream - wraps useSSE for the realtime endpoint
// ---------------------------------------------------------------------------

interface RealtimeStreamState {
  connected: boolean; lastTickTime: Date | null; latestTick: MetricsTick | null; allTicks: MetricsTick[]; rtDown: boolean
}

interface ErrorEvent {
  event_id: string; timestamp: string; status: number; url: string; pop: string; client_ip: string
}

function parseErrorEvent(line: SSELine): ErrorEvent | null {
  if (line.event !== 'error_event' && line.type !== 'error_event') return null
  const data = line.data
  if (!data || typeof data !== 'object') return null
  const value = data as Partial<ErrorEvent>
  if (typeof value.event_id !== 'string' || typeof value.timestamp !== 'string' || typeof value.status !== 'number') return null
  return {
    event_id: value.event_id, timestamp: value.timestamp, status: value.status,
    url: typeof value.url === 'string' ? value.url : '',
    pop: typeof value.pop === 'string' ? value.pop : '',
    client_ip: typeof value.client_ip === 'string' ? value.client_ip : '',
  }
}

// Permanent streams keep a bounded tail: the consumers only render the last 20
// error events / last 60 tick-history entries (useTickHistory MAX_ENTRIES).
const ERROR_STREAM_MAX_LINES = 500
const REALTIME_STREAM_MAX_LINES = 600

function useErrorStream(): { connected: boolean; events: ErrorEvent[] } {
  const { lines, status, start, reset } = useSSE({ maxLines: ERROR_STREAM_MAX_LINES })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  useEffect(() => {
    if (!activeServiceId) return
    const pageLoadId = getPageLoadId()
    start(`/api/services/${activeServiceId}/control-room/error-stream`, undefined, pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined)
    return () => reset()
  }, [activeServiceId, start, reset])

  const events = useMemo(() => {
    const seen = new Set<string>()
    return lines
      .map(parseErrorEvent)
      .filter((event): event is ErrorEvent => event !== null)
      .filter((event) => {
        if (seen.has(event.event_id)) return false
        seen.add(event.event_id)
        return true
      })
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
  if (!t || typeof t !== 'object') return false
  const obj = t as Record<string, unknown>
  return obj.event === 'metrics_tick' && !!obj.data
}

function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, reset } = useSSE({ maxLines: REALTIME_STREAM_MAX_LINES })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const [seed, setSeed] = useState<{ ticks: MetricsTick[]; timestamps: Set<string> }>({ ticks: [], timestamps: new Set() })

  useEffect(() => {
    if (!activeServiceId) return
    const ac = new AbortController()
    const pageLoadId = getPageLoadId()
    // Start SSE stream immediately in parallel with realtime-seed so the
    // first live metrics_tick is not blocked by the 150ms–1,200ms seed fetch.
    start(`/api/services/${activeServiceId}/realtime-stream`, undefined, pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined)
    ;(async () => {
      let seedTicks: MetricsTick[] = []
      try {
        const res = await adminJsonResult('/api/services/{service_id}/realtime-seed', `/api/services/${activeServiceId}/realtime-seed`, { signal: ac.signal })
        if (res.ok) {
          const rawTicks: unknown[] = res.data.ticks ?? []
          seedTicks = rawTicks.filter(isTick)
        }
      } catch {
        // Seed is best-effort
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

  const allTicks = useMemo(() => {
    const sseTicks = lines.map(parseMetricsTick).filter((t): t is MetricsTick => t !== null).filter((t) => !seed.timestamps.has(t.timestamp))
    return [...seed.ticks, ...sseTicks]
  }, [lines, seed])

  const latestTick = allTicks.length > 0 ? allTicks[allTicks.length - 1] : null
  const lastTickTime = latestTick ? new Date(latestTick.timestamp) : null

  return { connected: status === 'streaming', lastTickTime, latestTick, allTicks, rtDown: latestTick?.status === 'rt_down' }
}

// ---------------------------------------------------------------------------
// Freshness helpers & Status indicators
// ---------------------------------------------------------------------------

type FreshnessLevel = 'fresh' | 'stale' | 'expired'

function getFreshnessLevel(lastTick: Date | null): FreshnessLevel {
  if (!lastTick) return 'expired'
  const ageMs = Date.now() - lastTick.getTime()
  return ageMs < 10_000 ? 'fresh' : ageMs < 30_000 ? 'stale' : 'expired'
}

const FRESHNESS_VARIANTS = { fresh: 'success', stale: 'warning', expired: 'destructive' } as const

function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <Badge variant={connected ? 'success' : 'destructive'} className="gap-1.5">
      <span className={`inline-block h-2 w-2 rounded-full ${connected ? 'bg-emerald-500 dark:bg-emerald-400' : 'bg-red-500 dark:bg-red-400'}`} />
      {connected ? 'Connected' : 'Disconnected'}
    </Badge>
  )
}

function FreshnessBadge({ lastTickTime, timezone }: { lastTickTime: Date | null; timezone: string }) {
  // Subscribe to the shared 1 Hz ticker so the freshness level re-evaluates.
  useNowMs()
  const level = getFreshnessLevel(lastTickTime)
  return (
    <Badge variant={FRESHNESS_VARIANTS[level]} className="gap-1.5">
      {lastTickTime ? `Fresh as of ${formatDate(lastTickTime, timezone, 'h:mm:ss a')}` : 'No data yet'}
    </Badge>
  )
}

function RtDownBanner() {
  return (
    <Alert variant="destructive" className="mb-4">
      <AlertTriangle className="h-4 w-4" />
      <AlertDescription>Fastly RT unavailable — showing last-known-good values.</AlertDescription>
    </Alert>
  )
}

// ---------------------------------------------------------------------------
// Correlator Leaderboard - auto-refresh Top-N from /correlate endpoint
// ---------------------------------------------------------------------------

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
    queryFn: async ({ signal }) => {
      return adminJson<'/api/services/{service_id}/control-room/correlate', CorrelatorResult, 'post'>(
        '/api/services/{service_id}/control-room/correlate',
        `/api/services/${serviceId}/control-room/correlate`,
        {
          method: 'POST',
          signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ dimension, window_minutes: 60, limit }),
        },
      )
    },
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
        {data?.top?.length === 0 && (
          <div className="text-sm text-muted-foreground py-8 text-center">No data in window</div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Overview tab (live data)
// ---------------------------------------------------------------------------

type RollingAvgFn = (extractor: (d: MetricsData) => number, windowSize: number) => number
type SeriesFn = (extractor: (d: MetricsData) => number) => number[]

interface TabCommonProps {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: SeriesFn
  rollingAvg: RollingAvgFn
}

interface MetricCardSpec {
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

function seriesTraces(
  series: SeriesFn,
  specs: ReadonlyArray<readonly [name: string, color: string, fn: (d: MetricsData) => number, yaxis?: 'y' | 'y2']>,
) {
  return specs.map(([name, color, fn, yaxis]) => ({
    y: series(fn),
    name,
    color,
    ...(yaxis ? { yaxis } : {}),
  }))
}

function MetricCardGrid({
  cols = 4,
  data,
  items,
  cards,
  rollingAvg,
  rtDown,
  dimmed,
  prefix,
}: {
  cols?: 2 | 3 | 4 | 5
  data?: MetricsData
  items?: MetricCardSpec[]
  cards?: MetricCardSpec[]
  rollingAvg?: RollingAvgFn
  rtDown?: boolean
  dimmed?: boolean
  prefix?: React.ReactNode
}) {
  const list = items ?? cards ?? []
  const isDimmed = rtDown ?? dimmed ?? false
  return (
    <div className={GRID_COL_CLASSES[cols]}>
      {prefix}
      {list.map((item) => {
        const rawVal = item.value ?? (data && item.rateFn ? item.rateFn(data) : 0)
        const val = item.decimals !== undefined ? Number(rawVal.toFixed(item.decimals)) : rawVal
        return (
          <RealtimeMetricCard
            key={item.title}
            title={item.title}
            value={val}
            suffix={item.suffix}
            rates={item.rates ?? (item.rateFn && rollingAvg ? makeRates(rollingAvg, item.rateFn) : undefined)}
            thresholds={item.thresholds ? {
              warn: item.thresholds.warn,
              critical: item.thresholds.critical,
              direction: (item.thresholds.direction as 'above' | 'below' | undefined) ?? 'above',
            } : undefined}
            dimmed={isDimmed}
            helpText={item.helpText}
          />
        )
      })}
    </div>
  )
}

function TabPanelShell({
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
      {historicalHref && historicalLabel && (
        <HistoricalLink href={historicalHref} label={historicalLabel} />
      )}
    </div>
  )
}

function SectionHeading({ children }: { children: React.ReactNode }) {
  return (
    <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">
      {children}
    </h3>
  )
}

function OverviewTab({
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
        items={[
          {
            title: 'Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.requests_per_second,
            helpText: 'Total HTTP requests per second across all Fastly edge PoPs. Rolling averages smooth out per-second spikes — the 60s average is most representative of sustained load.',
          },
          {
            title: 'Error Rate',
            decimals: 2,
            suffix: '%',
            rateFn: (d) => d.error_rate * 100,
            thresholds: { warn: 1, critical: 5, direction: 'above' },
            helpText: 'Percentage of responses with 4xx or 5xx status codes. Card turns amber above 1% and red above 5%.',
          },
          {
            title: 'Cache Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => d.cache_hit_ratio * 100,
            thresholds: { warn: 90, critical: 50, direction: 'below' },
            helpText: "Percentage of requests served from Fastly's edge cache without reaching your origin. Higher is better — below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content.",
          },
          {
            title: 'Bandwidth',
            decimals: 2,
            suffix: ' Mbps',
            rateFn: (d) => d.bandwidth_mbps,
            helpText: 'Total bandwidth delivered across all PoPs in megabits per second, including both cached and origin-fetched content.',
          },
        ]}
      />

      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Total Requests"
          helpText="Per-second request count across all PoPs. Sudden spikes may indicate traffic surges, bot activity, or DDoS events."
          yAxisSuffix=" req/s"
          traces={seriesTraces(series, [
            ['Requests', CHART_COLORS.info, (d) => d.total_requests ?? d.requests_per_second],
          ])}
        />
        <RealtimeChart
          title="Status Code Breakdown"
          helpText="Per-second breakdown by HTTP status class. 2xx = success, 3xx = redirects, 4xx = client errors (broken links, auth failures), 5xx = origin/server errors."
          yAxisSuffix=" req/s"
          stacked
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
          helpText="Hit = served from edge cache, Miss = fetched from origin and cached, Pass = bypassed cache entirely (uncacheable or VCL pass rules). A growing Pass ratio may indicate misconfigured cache rules."
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(series, [
            ['Hit', '#22c55e', (d) => d.total_hits ?? 0],
            ['Miss', '#f97316', (d) => d.total_miss ?? 0],
            ['Pass', CHART_COLORS.neutral, (d) => d.total_pass ?? 0],
          ])}
        />
        <RealtimeChart
          title="Origin Load"
          helpText="Left axis: requests reaching your origin per second. Right axis: cache hit ratio — when it drops, more requests flow through to origin."
          dualYAxis
          yAxisSuffix=" req/s"
          y2AxisSuffix="%"
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
            ).map(([code, color]) => [code, color, (d: MetricsData) => d.status_detail?.[code] ?? 0]),
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
            ).map(([code, color]) => [code, color, (d: MetricsData) => d.status_detail?.[code] ?? 0]),
          )}
        />
      </div>

      <RecentErrorsCard events={errorEvents} connected={errorsConnected} timezone={timezone} />
    </TabPanelShell>
  )
}

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
          href={buildServiceHref(
            `/dashboard?filter_${filterKey}=${encodeURIComponent(str)}`,
            activeServiceId,
          )}
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
        <Badge variant={connected ? 'success' : 'destructive'}>
          {connected ? 'Listening' : 'Disconnected'}
        </Badge>
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
                    <th key={h} className="px-3 py-2 font-medium">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.event_id} className="border-t">
                    <td className="whitespace-nowrap px-3 py-2">
                      {formatDate(event.timestamp, timezone, 'h:mm:ss a')}
                    </td>
                    <ErrorFilterCell filterKey="status" value={event.status} activeServiceId={activeServiceId} bold />
                    <ErrorFilterCell filterKey="url" value={event.url} activeServiceId={activeServiceId} className="max-w-sm truncate px-3 py-2" />
                    <ErrorFilterCell filterKey="pop" value={event.pop} activeServiceId={activeServiceId} />
                    <ErrorFilterCell filterKey="client_ip" value={event.client_ip} activeServiceId={activeServiceId} className="px-3 py-2 font-mono" />
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

// ---------------------------------------------------------------------------
// Performance tab
// ---------------------------------------------------------------------------

function PerformanceTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="performance">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        items={[
          {
            title: 'Cache Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => d.cache_hit_ratio * 100,
            thresholds: { warn: 90, critical: 50, direction: 'below' },
            helpText: 'Percentage of requests served from edge cache. Below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content.',
          },
          {
            title: 'Origin Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.origin_requests_per_second ?? 0,
            helpText: "Requests that reached your origin server per second. Lower is better — it means more traffic is being served from Fastly's edge cache.",
          },
          {
            title: 'Bandwidth Offload',
            decimals: 1,
            suffix: '%',
            thresholds: { warn: 80, critical: 50, direction: 'below' },
            rateFn: (d) => (d.origin_offload ?? 0) * 100,
            helpText: 'Percentage of total bandwidth served from cache vs. origin. Higher means less origin load and lower egress costs.',
          },
          {
            title: 'Pass-through',
            suffix: ' req',
            rateFn: (d) => d.pass_requests ?? 0,
            helpText: 'Requests that bypassed cache entirely — either uncacheable content types or explicit VCL pass rules. Every pass request hits your origin.',
          },
        ]}
      />
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        items={[
          {
            title: 'Collapse Usable',
            suffix: ' req',
            rateFn: (d) => d.request_collapse_usable ?? 0,
            helpText: 'Requests eligible for request collapsing — multiple identical requests are collapsed into a single origin fetch. Higher counts mean more origin load reduction.',
          },
          {
            title: 'Collapse Unusable',
            suffix: ' req',
            rateFn: (d) => d.request_collapse_unusable ?? 0,
            helpText: 'Requests that could not be collapsed (e.g., different Vary headers, POST requests). High unusable counts may indicate opportunities to improve cache key design.',
          },
          {
            title: 'Segment Origin Fetches',
            suffix: ' req',
            rateFn: (d) => d.segblock_origin_fetches ?? 0,
            helpText: 'Segment block fetches to origin for large objects. Fastly breaks large files into segments cached independently, enabling range requests and parallel fetching.',
          },
          {
            title: 'Segment Shield Fetches',
            suffix: ' req',
            rateFn: (d) => d.segblock_shield_fetches ?? 0,
            helpText: 'Segment block fetches handled at the shield PoP. When these are high relative to origin fetches, segmented caching is working well.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Hit Ratio"
          yAxisSuffix="%"
          traces={seriesTraces(series, [
            ['Hit Ratio %', '#22c55e', (d) => d.cache_hit_ratio * 100],
          ])}
        />
        <RealtimeChart
          title="Origin Latency Breakdown"
          yAxisSuffix=" req/s"
          traces={seriesTraces(series, [
            ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
            ['Pass', '#f97316', (d) => d.pass_requests ?? 0],
          ])}
        />
      </div>
      <RealtimeChart
        title="Cache Path Latency (ms)"
        helpText="Time-to-first-byte by cache path. Hit latency is edge-only (fast). Miss includes origin round-trip plus caching. Pass includes origin round-trip with no caching overhead."
        yAxisSuffix=" ms"
        traces={seriesTraces(series, [
          ['Hit', '#22c55e', (d) => d.hit_latency_ms ?? 0],
          ['Miss', '#f97316', (d) => d.miss_latency_ms ?? 0],
          ['Pass', CHART_COLORS.neutral, (d) => d.pass_latency_ms ?? 0],
        ])}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Object Size Distribution"
          helpText="Distribution of response body sizes. Large objects may benefit from segmented caching; many small objects benefit from request collapsing."
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(
            series,
            (
              [
                ['<1KB', CHART_COLORS.neutral, '1k'],
                ['1-10KB', CHART_COLORS.info, '10k'],
                ['10-100KB', '#22c55e', '100k'],
                ['100KB-1MB', '#eab308', '1m'],
                ['1-10MB', '#f97316', '10m'],
                ['10-100MB', CHART_COLORS.danger, '100m'],
                ['100MB-1GB', '#8b5cf6', '1g'],
                ['Other', '#6b7280', 'other'],
              ] as const
            ).map(([name, color, k]) => [name, color, (d: MetricsData) => d.object_size_distribution?.[k] ?? 0]),
          )}
        />
        <RealtimeChart
          title="Origin Response Time (ms)"
          helpText="Histogram of origin response times for cache misses. A shift toward higher buckets (>250ms) signals origin degradation or upstream latency."
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(series, [
            ...(
              [
                ['<10ms', '#22c55e', '10'],
                ['10-20ms', '#86efac', '20'],
                ['20-30ms', CHART_COLORS.info, '30'],
                ['30-60ms', '#93c5fd', '60'],
                ['60-120ms', '#eab308', '120'],
                ['120-250ms', '#f97316', '250'],
                ['250-500ms', CHART_COLORS.danger, '500'],
                ['500ms-1s', '#dc2626', '1000'],
              ] as const
            ).map(([name, color, k]) => [name, color, (d: MetricsData) => d.miss_histogram?.[k] ?? 0] as const),
            [
              '>1s',
              '#7f1d1d',
              (d: MetricsData) => {
                const h = d.miss_histogram ?? {}
                const known = ['10', '20', '30', '60', '120', '250', '500', '1000']
                return Object.entries(h).reduce((sum, [k, v]) => (known.includes(k) ? sum : sum + v), 0)
              },
            ],
          ])}
        />
      </div>
    </TabPanelShell>
  )
}

// ---------------------------------------------------------------------------
// Origin tab
// ---------------------------------------------------------------------------

function OriginTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="origin">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Origin Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.origin_requests_per_second ?? 0,
            helpText: 'Requests per second reaching your origin server. Lower is better — cache and shield should absorb most traffic before it gets here.',
          },
          {
            title: 'Origin Bandwidth',
            decimals: 2,
            suffix: ' Mbps',
            rateFn: (d) => d.origin_bandwidth_mbps ?? 0,
            helpText: 'Bandwidth transferred from your origin server to Fastly. This is your origin egress — lower means better cache offload.',
          },
          {
            title: 'Shield Requests',
            suffix: ' req',
            rateFn: (d) => d.shield_requests ?? 0,
            helpText: 'Requests routed through the shield PoP — a mid-tier cache between edge PoPs and your origin. The shield absorbs duplicate misses from multiple edge PoPs.',
          },
          {
            title: 'Shield Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => (d.shield_hit_ratio ?? 0) * 100,
            helpText: 'Percentage of shield requests served from the shield cache. Higher means fewer requests reach your origin.',
          },
          {
            title: 'Origin Fetches',
            suffix: ' req',
            rateFn: (d) => d.origin_fetches ?? 0,
            helpText: 'Total fetch requests sent from Fastly to your origin. Includes full fetches, revalidations, and cache-eligible fetches. This is the broadest measure of origin load.',
          },
          {
            title: 'Revalidations',
            suffix: ' req',
            rateFn: (d) => d.origin_revalidations ?? 0,
            helpText: 'Conditional requests (If-Modified-Since/If-None-Match) sent to your origin. When the origin returns 304 Not Modified, Fastly reuses the cached copy — cheaper than a full fetch.',
          },
          {
            title: 'Cache Fetches',
            suffix: ' req',
            rateFn: (d) => d.origin_cache_fetches ?? 0,
            helpText: 'Origin fetches that returned a cacheable response (the response will be stored and served to future requests). A low ratio of cache fetches to total fetches may indicate your origin is sending too many no-cache/private headers.',
          },
        ]}
      />
      <RealtimeChart
        title="Origin Load"
        dualYAxis
        yAxisSuffix=" req/s"
        y2AxisSuffix="%"
        traces={seriesTraces(series, [
          ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
          ['Shield Hit %', '#22c55e', (d) => (d.shield_hit_ratio ?? 0) * 100, 'y2'],
        ])}
      />
      <RealtimeChart
        title="Origin Fetch Breakdown"
        helpText="Fetches = full origin requests. Revalidations = conditional requests (If-Modified-Since/ETag) that are cheaper than full fetches. Cache Fetches = requests to origin that resulted in a cacheable response."
        yAxisSuffix=" req/s"
        stacked
        traces={seriesTraces(series, [
          ['Fetches', CHART_COLORS.info, (d) => d.origin_fetches ?? 0],
          ['Revalidations', '#22c55e', (d) => d.origin_revalidations ?? 0],
          ['Cache Fetches', '#eab308', (d) => d.origin_cache_fetches ?? 0],
        ])}
      />

      <SectionHeading>Shield Efficiency</SectionHeading>
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Shield Hits',
            rateFn: (d) => d.shield_hit_requests ?? 0,
            helpText: "Requests served from the shield PoP's cache, preventing duplicate origin fetches from multiple edge PoPs.",
          },
          {
            title: 'Shield Misses',
            rateFn: (d) => d.shield_miss_requests ?? 0,
            helpText: 'Requests that missed the shield cache and were forwarded to your origin. Every shield miss becomes an origin fetch.',
          },
          {
            title: 'Shield Revalidations',
            rateFn: (d) => d.shield_revalidations ?? 0,
            helpText: 'Conditional requests at the shield tier — the shield checks with origin whether stale content is still valid. A 304 response avoids transferring the full object.',
          },
          {
            title: 'Shield Fetch Bytes',
            decimals: 2,
            suffix: ' MB',
            rateFn: (d) => (d.shield_fetch_body_bytes ?? 0) / 1_000_000,
            helpText: 'Total bytes fetched through the shield tier from origin. This represents the bandwidth your origin actually serves — compare against total edge bandwidth to measure offload efficiency.',
          },
        ]}
      />
      <RealtimeChart
        title="Shield Hit/Miss/Revalidation"
        helpText="Shield-tier cache breakdown. Hits = served from shield cache. Misses = forwarded to origin. Revalidations = conditional requests (304 responses) that avoid full origin fetches."
        yAxisSuffix=" req/s"
        stacked
        traces={seriesTraces(series, [
          ['Hits', '#22c55e', (d) => d.shield_hit_requests ?? 0],
          ['Misses', '#f97316', (d) => d.shield_miss_requests ?? 0],
          ['Revalidations', CHART_COLORS.info, (d) => d.shield_revalidations ?? 0],
        ])}
      />
    </TabPanelShell>
  )
}

// ---------------------------------------------------------------------------
// Security tab
// ---------------------------------------------------------------------------

function SecurityTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  const issued = data?.bot_challenges_issued ?? 0
  const succeeded = data?.bot_challenges_succeeded ?? 0
  const failed = data?.bot_challenges_failed ?? 0
  const challengePassRate = issued > 0 ? ((succeeded / issued) * 100).toFixed(1) : '0.0'

  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="security">
      <MetricCardGrid
        data={data} rollingAvg={rollingAvg} dimmed={rtDown}
        cards={[
          { title: 'WAF Blocked', suffix: ' req', rateFn: (d) => d.waf_blocked ?? 0, helpText: 'Requests blocked by the Web Application Firewall. These matched a WAF rule configured to block — the request never reached your origin.' },
          { title: 'WAF Logged', suffix: ' req', rateFn: (d) => d.waf_logged ?? 0, helpText: 'Requests that triggered a WAF rule in logging mode — suspicious but allowed through. Use logged events to tune rules before switching them to block mode.' },
          { title: '4xx Responses', suffix: ' req', rateFn: (d) => d.status_breakdown?.status_4xx ?? 0, helpText: 'Client error responses (400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 429 Rate Limited). Spikes may indicate scanning activity, broken links, or auth issues.' },
          { title: '5xx Responses', suffix: ' req', rateFn: (d) => d.status_breakdown?.status_5xx ?? 0, thresholds: { warn: 1, critical: 10, direction: 'above' }, helpText: 'Server error responses. 502/503 typically indicate origin unreachable or overloaded. 500 indicates an application error. Any sustained 5xx traffic warrants immediate investigation.' },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="WAF Events"
          helpText="Blocked = attack prevented at the edge. Logged = suspicious traffic allowed through for monitoring. Passed = clean traffic that matched no WAF rules."
          yAxisSuffix=" req/s" stacked
          traces={seriesTraces(series, [
            ['Blocked', CHART_COLORS.danger, (d) => d.waf_blocked ?? 0],
            ['Logged', '#eab308', (d) => d.waf_logged ?? 0],
            ['Passed', '#22c55e', (d) => d.waf_passed ?? 0],
          ])}
        />
        <RealtimeChart
          title="Error Codes"
          helpText="4xx errors are client-side (bad requests, auth failures, not found). 5xx errors are server-side (origin errors, timeouts). Correlated spikes suggest an incident."
          yAxisSuffix=" req/s"
          traces={seriesTraces(series, [
            ['4xx', '#eab308', (d) => d.status_breakdown?.status_4xx ?? 0],
            ['5xx', CHART_COLORS.danger, (d) => d.status_breakdown?.status_5xx ?? 0],
          ])}
        />
      </div>

      <SectionHeading>Bot Challenge Funnel</SectionHeading>
      <MetricCardGrid
        dimmed={rtDown}
        cards={[
          { title: 'Challenges Issued', value: issued, suffix: ' req', helpText: 'Number of bot challenges (CAPTCHAs, JS challenges) issued to suspicious clients. High counts indicate active bot pressure.' },
          { title: 'Challenges Passed', value: succeeded, suffix: ' req', helpText: 'Clients that successfully completed the challenge — likely legitimate users or sophisticated bots.' },
          { title: 'Challenges Failed', value: failed, suffix: ' req', helpText: 'Clients that failed the challenge — almost certainly automated bots unable to solve CAPTCHAs or execute JavaScript.' },
          { title: 'Pass Rate', value: Number(challengePassRate), suffix: '%', helpText: 'Percentage of issued challenges that were passed. Very low rates suggest the challenges are effectively blocking bots. Very high rates may mean challenges are too easy or mostly hitting real users.' },
        ]}
      />
      <MetricCardGrid
        cols={3} dimmed={rtDown}
        cards={[
          { title: 'Bots Detected', value: data?.bot_detected ?? 0, suffix: ' req', helpText: "Requests classified as bot traffic by Fastly's bot detection engine, based on behavioral fingerprinting and reputation data." },
          { title: 'Verified Bots', value: data?.bot_verified ?? 0, suffix: ' req', helpText: 'Verified legitimate bots like Googlebot, Bingbot, and other search engine crawlers that have been authenticated via reverse DNS or IP verification.' },
          { title: 'AI Crawlers', value: data?.bot_ai_crawlers ?? 0, suffix: ' req', helpText: 'Requests from identified AI training crawlers (GPTBot, ClaudeBot, etc.). These can consume significant bandwidth — consider blocking via robots.txt or WAF rules.' },
        ]}
      />
      <RealtimeChart
        title="Bot Challenge Pipeline"
        helpText="Bot management funnel. Passed = completed challenge (likely human). Failed = failed challenge (likely bot). Detected = classified as bot by fingerprinting. AI Crawlers = identified AI training crawlers."
        yAxisSuffix=" req/s" stacked
        traces={seriesTraces(series, [
          ['Passed', '#22c55e', (d) => d.bot_challenges_succeeded ?? 0],
          ['Failed', CHART_COLORS.danger, (d) => d.bot_challenges_failed ?? 0],
          ['Detected', '#eab308', (d) => d.bot_detected ?? 0],
          ['AI Crawlers', '#8b5cf6', (d) => d.bot_ai_crawlers ?? 0],
        ])}
      />

      <SectionHeading>DDoS Protection</SectionHeading>
      <MetricCardGrid
        cols={2} dimmed={rtDown}
        cards={[
          { title: 'DDoS Detected', value: data?.ddos_detect ?? 0, suffix: ' req', helpText: "Requests classified as distributed denial-of-service by Fastly's automatic DDoS protection. Detection is always on — no configuration required." },
          { title: 'DDoS Mitigated', value: data?.ddos_mitigate ?? 0, suffix: ' req', helpText: 'DDoS requests that were actively mitigated (blackholed, tarpitted, closed, or downgraded). The gap between detected and mitigated indicates requests still under analysis.' },
        ]}
      />
      <RealtimeChart
        title="DDoS Mitigation Actions"
        helpText="Blackhole = all traffic dropped. Tarpit = connections intentionally slowed. Close = connections terminated. Downgrade = service quality reduced. Any non-zero values indicate active DDoS mitigation."
        yAxisSuffix=" req/s" stacked
        traces={seriesTraces(series, [
          ['Blackhole', '#1e293b', (d) => d.ddos_action_blackhole ?? 0],
          ['Tarpit', '#7c3aed', (d) => d.ddos_action_tarpit ?? 0],
          ['Close', CHART_COLORS.danger, (d) => d.ddos_action_close ?? 0],
          ['Downgrade', '#eab308', (d) => d.ddos_action_downgrade ?? 0],
        ])}
      />

      <SectionHeading>WAF Signal Classification</SectionHeading>
      <Card><CardContent className="pt-4"><CorrelatorLeaderboard dimension="waf_sig" serviceId={activeServiceId} limit={5} /></CardContent></Card>
    </TabPanelShell>
  )
}

// ---------------------------------------------------------------------------
// Network tab & Admin Health tab
// ---------------------------------------------------------------------------

function NetworkTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  const degradedCount = data?.degraded_pops?.length ?? 0
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="network">
      <MetricCardGrid
        cols={3} data={data} rollingAvg={rollingAvg} dimmed={rtDown}
        cards={[
          { title: 'Active PoPs', rateFn: (d) => d.pop_count ?? 0, helpText: 'Number of Fastly Points of Presence actively serving traffic for this service. Each PoP is a data center location worldwide.' },
          { title: 'Healthy PoPs', rateFn: (d) => (d.pop_count ?? 0) - (d.degraded_pops?.length ?? 0), helpText: 'PoPs with error rates below the degradation threshold. A sudden drop may indicate a regional issue.' },
          { title: 'Degraded PoPs', thresholds: { warn: 1, critical: 3, direction: 'above' }, rateFn: (d) => d.degraded_pops?.length ?? 0, helpText: 'PoPs with elevated error rates. Low-traffic PoPs may appear degraded during traffic lulls.' },
        ]}
      />
      {degradedCount > 0 && !rtDown && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>{degradedCount} PoP(s) showing elevated errors: {data?.degraded_pops?.join(', ')}</AlertDescription>
        </Alert>
      )}
      <RealtimeChart
        title="PoP Count" yAxisSuffix=" PoPs"
        traces={seriesTraces(series, [['Active PoPs', CHART_COLORS.info, (d) => d.pop_count ?? 0], ['Degraded', CHART_COLORS.danger, (d) => d.degraded_pops?.length ?? 0]])}
      />

      <SectionHeading>Traffic Distribution</SectionHeading>
      <Card><CardContent className="pt-4"><PopTrafficMap allPops={data?.all_pops ?? {}} className="h-[420px]" /></CardContent></Card>

      <SectionHeading>Protocol Adoption</SectionHeading>
      <MetricCardGrid
        cols={5} dimmed={rtDown}
        cards={([['HTTP/2', data?.h2_pct], ['HTTP/3', data?.h3_pct], ['IPv6', data?.ipv6_pct], ['TLS 1.2', data?.tls12_pct], ['TLS 1.3', data?.tls13_pct]] as const).map(([title, pct]) => ({
          title, value: Number((pct ?? 0).toFixed(1)), suffix: '%',
        }))}
      />
      <RealtimeChart
        title="Protocol Distribution"
        helpText="HTTP/2 uses multiplexed TCP streams. HTTP/3 uses QUIC (UDP-based) for better performance on lossy networks. 'Other' includes HTTP/1.1 and legacy protocols."
        yAxisSuffix=" req/s" stacked
        traces={seriesTraces(series, [
          ['HTTP/2', CHART_COLORS.info, (d) => d.http2 ?? 0],
          ['HTTP/3', '#22c55e', (d) => d.http3 ?? 0],
          ['Other', CHART_COLORS.neutral, (d) => Math.max(0, (d.total_requests ?? 0) - (d.http2 ?? 0) - (d.http3 ?? 0))],
        ])}
      />
    </TabPanelShell>
  )
}

function AdminHealthTab({ tick, rtDown, connected, series, rollingAvg }: Omit<TabCommonProps, 'historicalHref'> & { connected: boolean }) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown}>
      <MetricCardGrid
        cols={3} data={data} rollingAvg={rollingAvg} dimmed={rtDown}
        prefix={
          <Card>
            <CardHeader className="pb-0">
              <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">RT Stream</CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-3xl font-bold tabular-nums ${!connected ? 'opacity-50' : ''}`}>
                {connected ? (rtDown ? 'Degraded' : 'Healthy') : 'Disconnected'}
              </div>
            </CardContent>
          </Card>
        }
        cards={[
          { title: 'Active PoPs', rateFn: (d) => d.pop_count ?? 0 },
          { title: 'Degraded PoPs', rateFn: (d) => d.degraded_pops?.length ?? 0, thresholds: { warn: 1, critical: 3, direction: 'above' } },
        ]}
      />

      <SectionHeading>Scorer Health (Compute@Edge)</SectionHeading>
      <MetricCardGrid
        cols={3} data={data} rollingAvg={rollingAvg} dimmed={rtDown}
        cards={[
          { title: 'Execution Time', decimals: 1, suffix: ' ms', thresholds: { warn: 50, critical: 200, direction: 'above' }, rateFn: (d) => d.compute_exec_time_ms ?? 0, helpText: 'Compute@Edge Wasm execution time (CPU-only, excludes I/O waits). The scorer is cold on every request — watch for execution time creep after code changes.' },
          { title: 'Request Time', decimals: 1, suffix: ' ms', thresholds: { warn: 100, critical: 500, direction: 'above' }, rateFn: (d) => d.compute_req_time_ms ?? 0, helpText: 'Total Compute@Edge request time including I/O (backend fetches, KV lookups). The gap between this and execution time is I/O wait.' },
          { title: 'RAM Used', value: Number(((data?.compute_ram_used ?? 0) / 1_048_576).toFixed(1)), suffix: ' MB', helpText: 'Peak memory usage of the Compute@Edge scorer per request. The Wasm runtime allocates a fresh heap per request — if this climbs toward the limit, the scorer may start hitting Resource Exceeded errors.' },
          { title: 'Backend Errors', value: data?.compute_bereq_errors ?? 0, thresholds: { warn: 1, critical: 10, direction: 'above' }, helpText: 'Failed backend requests from the Compute scorer (e.g., origin unreachable). The scorer fails open on these — sessions are passed without scoring.' },
          { title: 'Guest Errors', value: data?.compute_guest_errors ?? 0, thresholds: { warn: 1, critical: 5, direction: 'above' }, helpText: 'Wasm runtime errors in the scorer (panics, traps, invalid memory access). Any non-zero value warrants investigation — it means the scorer code is hitting an unhandled edge case.' },
          { title: 'Resource Exceeded', value: data?.compute_resource_exceeded ?? 0, thresholds: { warn: 1, critical: 5, direction: 'above' }, helpText: 'Requests where the scorer exceeded its CPU or memory allocation. The scorer is cold on every request — if this grows, the scorer binary may need optimization or wasm-opt tuning.' },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Compute Latency (ms)"
          helpText="Execution = Wasm CPU time only. Request = total time including I/O waits. The gap between them is I/O wait (backend fetches, KV lookups). Watch for execution time creep after scorer code changes."
          yAxisSuffix=" ms"
          traces={seriesTraces(series, [['Execution', CHART_COLORS.info, (d) => d.compute_exec_time_ms ?? 0], ['Request', '#f97316', (d) => d.compute_req_time_ms ?? 0]])}
        />
        <RealtimeChart
          title="Compute Errors"
          helpText="Backend = origin request failures. Guest = Wasm runtime panics/traps. Resource Limit = CPU or memory exceeded. Any sustained non-zero errors mean the scorer is degraded and sessions are failing open."
          yAxisSuffix=" err/s" stacked
          traces={seriesTraces(series, [
            ['Backend', CHART_COLORS.danger, (d) => d.compute_bereq_errors ?? 0],
            ['Guest', '#eab308', (d) => d.compute_guest_errors ?? 0],
            ['Resource Limit', '#8b5cf6', (d) => d.compute_resource_exceeded ?? 0],
          ])}
        />
      </div>

      <SectionHeading>VCL Execution</SectionHeading>
      <MetricCardGrid
        cols={5} data={data} rollingAvg={rollingAvg} dimmed={rtDown}
        cards={[
          { title: 'Restarts', suffix: '/s', rateFn: (d) => d.restarts ?? 0, thresholds: { warn: 1, critical: 10, direction: 'above' }, helpText: 'VCL restart count per second. Restarts re-enter vcl_recv with modified state — used for routing fallbacks but expensive. High rates indicate a misconfigured restart loop.' },
          { title: 'vcl_recv', value: Number((data?.vcl_recv_time_ms ?? 0).toFixed(1)), suffix: ' ms', helpText: 'Time in vcl_recv — request routing, ACL checks, header manipulation. High values may indicate complex routing logic or excessive table lookups.' },
          { title: 'vcl_fetch', value: Number((data?.vcl_fetch_time_ms ?? 0).toFixed(1)), suffix: ' ms', helpText: 'Time in vcl_fetch — origin response processing, cache TTL setting, header rewriting. Only runs on cache misses.' },
          { title: 'vcl_deliver', value: Number((data?.vcl_deliver_time_ms ?? 0).toFixed(1)), suffix: ' ms', helpText: 'Time in vcl_deliver — final response modification before sending to the client. Runs on every request (hits and misses).' },
          { title: 'vcl_error', value: Number((data?.vcl_error_time_ms ?? 0).toFixed(1)), suffix: ' ms', helpText: 'Time in vcl_error — synthetic error response generation. Only runs when VCL explicitly triggers an error or the origin is unreachable.' },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="VCL Subroutine Time (ms)"
          helpText="Time spent in each VCL subroutine per request. recv = request handling, fetch = origin response processing, deliver = client response, error = error page generation. High recv time may indicate complex routing logic."
          yAxisSuffix=" ms" stacked
          traces={seriesTraces(series, [
            ['recv', CHART_COLORS.info, (d) => d.vcl_recv_time_ms ?? 0],
            ['fetch', '#f97316', (d) => d.vcl_fetch_time_ms ?? 0],
            ['deliver', '#22c55e', (d) => d.vcl_deliver_time_ms ?? 0],
            ['error', CHART_COLORS.danger, (d) => d.vcl_error_time_ms ?? 0],
          ])}
        />
        <RealtimeChart
          title="VCL Subroutine Calls"
          helpText="Number of times each VCL subroutine was invoked. Call count × time per call = total VCL overhead. A high error count means many requests are hitting synthetic error responses."
          yAxisSuffix=" calls/s" stacked
          traces={seriesTraces(series, [
            ['recv', CHART_COLORS.info, (d) => d.vcl_recv_count ?? 0],
            ['fetch', '#f97316', (d) => d.vcl_fetch_count ?? 0],
            ['deliver', '#22c55e', (d) => d.vcl_deliver_count ?? 0],
            ['error', CHART_COLORS.danger, (d) => d.vcl_error_count ?? 0],
          ])}
        />
      </div>

      <div className="flex justify-end">
        <Link href={buildServiceHref('/admin/trends', activeServiceId)} prefetch={false}>
          <Button variant="outline" size="sm">
            View admin dashboard
            <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
          </Button>
        </Link>
      </div>
    </TabPanelShell>
  )
}

function HistoricalLink({ href, label }: { href: string; label: string }) {
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

function TabContent({
  tabId, historicalHref, tick, rtDown, connected, series, rollingAvg, errorEvents, errorsConnected, timezone,
}: {
  tabId: string; historicalHref: string | null; tick: MetricsTick | null; rtDown: boolean; connected: boolean
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
  errorEvents: ErrorEvent[]; errorsConnected: boolean; timezone: string
}) {
  const commonProps = { tick, rtDown, series, rollingAvg }
  switch (tabId) {
    case 'overview':
      return <OverviewTab {...commonProps} historicalHref={historicalHref ?? '/dashboard'} errorEvents={errorEvents} errorsConnected={errorsConnected} timezone={timezone} />
    case 'performance': return <PerformanceTab {...commonProps} historicalHref={historicalHref ?? '/performance'} />
    case 'origin': return <OriginTab {...commonProps} historicalHref={historicalHref ?? '/origin'} />
    case 'security': return <SecurityTab {...commonProps} historicalHref={historicalHref ?? '/security'} />
    case 'network': return <NetworkTab {...commonProps} historicalHref={historicalHref ?? '/network'} />
    case 'admin': return <AdminHealthTab {...commonProps} connected={connected} />
    default: return null
  }
}

function MobileOverview({ connected, lastTickTime, timezone, tick, rtDown }: {
  connected: boolean; lastTickTime: Date | null; timezone: string; tick: MetricsTick | null; rtDown: boolean
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
        <CardHeader><CardTitle>Live Overview</CardTitle></CardHeader>
        <CardContent>
          {data ? (
            <div className={`grid grid-cols-2 gap-3 ${rtDown ? 'opacity-50' : ''}`}>
              <RealtimeMetricCard title="Requests/s" value={data.requests_per_second} suffix=" req/s" />
              <RealtimeMetricCard title="Error Rate" value={Number((data.error_rate * 100).toFixed(2))} suffix="%" thresholds={{ warn: 1, critical: 5, direction: 'above' }} />
              <RealtimeMetricCard title="Cache Hit" value={Number((data.cache_hit_ratio * 100).toFixed(1))} suffix="%" thresholds={{ warn: 90, critical: 50, direction: 'below' }} />
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

export default function ControlRoomClient() {
  const [activeTab, setActiveTab] = useState<TabId>('overview')
  const isAnalyst = useIsAnalyst()
  const timezone = useTimezone()
  const { connected, lastTickTime, latestTick, allTicks, rtDown } = useRealtimeStream()
  const { connected: errorsConnected, events: errorEvents } = useErrorStream()
  const { series, rollingAvg } = useTickHistory(allTicks)

  const visibleTabs = useMemo(() => TABS.filter((tab) => !('adminOnly' in tab && tab.adminOnly) || !isAnalyst), [isAnalyst])

  const headerActions = (
    <div className="flex items-center gap-2 flex-wrap">
      <ConnectionBadge connected={connected} />
      <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
    </div>
  )

  return (
    <ReportShell title="Control Room" icon={Radio} description="Real-time SRE monitoring" headerActions={headerActions}>
      <MobileOverview connected={connected} lastTickTime={lastTickTime} timezone={timezone} tick={latestTick} rtDown={rtDown} />
      <div className="hidden md:block">
        <Tabs defaultValue="overview" value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
          <TabsList className="mb-4 flex-wrap">
            {visibleTabs.map((tab) => (
              <TabsTrigger key={tab.id} value={tab.id}>
                <tab.icon className="h-4 w-4" />
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>

          {visibleTabs.map((tab) => (
            <TabsContent key={tab.id} value={tab.id}>
              <TabContent
                tabId={tab.id}
                historicalHref={tab.historicalHref}
                tick={latestTick}
                rtDown={rtDown}
                connected={connected}
                series={series}
                rollingAvg={rollingAvg}
                errorEvents={errorEvents}
                errorsConnected={errorsConnected}
                timezone={timezone}
              />
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </ReportShell>
  )
}
