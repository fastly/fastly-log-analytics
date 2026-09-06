'use client'

import { useState, useEffect, useMemo, useCallback } from 'react'
import { Radio, ArrowRight, AlertTriangle, BarChart3, Gauge, HardDrive, Shield, Network, Settings } from 'lucide-react'
import Link from 'next/link'
import { ReportShell } from '@/components/ReportShell'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/serviceStore'
import { useAdminTokenStore } from '@/stores/adminTokenStore'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { useTimezone } from '@/hooks/useTimezone'
import { formatDate } from '@/lib/date'
import { buildServiceHref } from '@/lib/navigation'
import dynamic from 'next/dynamic'
import { useTickHistory, type MetricsData, type MetricsTick } from './useTickHistory'
import { RealtimeMetricCard } from './RealtimeMetricCard'

const RealtimeChart = dynamic(() => import('./RealtimeChart').then((m) => m.RealtimeChart), {
  ssr: false,
})

const PopTrafficMap = dynamic(
  () => import('@/components/Map/PopTrafficMap').then((m) => m.PopTrafficMap),
  { ssr: false },
)

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
  connected: boolean
  lastTickTime: Date | null
  latestTick: MetricsTick | null
  allTicks: MetricsTick[]
  rtDown: boolean
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

function authHeaders(): Record<string, string> {
  const token = useAdminTokenStore.getState().token
  return token ? { 'X-Admin-Token': token } : {}
}

function isTick(t: unknown): t is MetricsTick {
  if (!t || typeof t !== 'object') return false
  const obj = t as Record<string, unknown>
  return obj.event === 'metrics_tick' && !!obj.data
}

function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, reset } = useSSE()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const [seed, setSeed] = useState<{ ticks: MetricsTick[]; timestamps: Set<string> }>({
    ticks: [],
    timestamps: new Set(),
  })

  useEffect(() => {
    if (!activeServiceId) return
    const ac = new AbortController()
    ;(async () => {
      let seedTicks: MetricsTick[] = []
      try {
        const res = await fetch(`/api/services/${activeServiceId}/realtime-seed`, { signal: ac.signal, headers: authHeaders() })
        if (res.ok) {
          const json = await res.json()
          seedTicks = (json.ticks ?? []).filter(isTick)
        }
      } catch {
        // Seed is best-effort
      }
      if (!ac.signal.aborted) {
        setSeed({ ticks: seedTicks, timestamps: new Set(seedTicks.map((t) => t.timestamp)) })
        start(`/api/services/${activeServiceId}/realtime-stream`)
      }
    })()
    return () => {
      ac.abort()
      reset()
      setSeed({ ticks: [], timestamps: new Set() })
    }
  }, [activeServiceId, start, reset])

  const allTicks = useMemo(() => {
    const sseTicks = lines
      .map(parseMetricsTick)
      .filter((t): t is MetricsTick => t !== null)
      .filter((t) => !seed.timestamps.has(t.timestamp))
    return [...seed.ticks, ...sseTicks]
  }, [lines, seed])

  const latestTick = allTicks.length > 0 ? allTicks[allTicks.length - 1] : null
  const lastTickTime = latestTick ? new Date() : null

  return {
    connected: status === 'streaming',
    lastTickTime,
    latestTick,
    allTicks,
    rtDown: latestTick?.status === 'rt_down',
  }
}

// ---------------------------------------------------------------------------
// Freshness helpers
// ---------------------------------------------------------------------------

type FreshnessLevel = 'fresh' | 'stale' | 'expired'

function getFreshnessLevel(lastTick: Date | null): FreshnessLevel {
  if (!lastTick) return 'expired'
  const ageMs = Date.now() - lastTick.getTime()
  if (ageMs < 10_000) return 'fresh'
  if (ageMs < 30_000) return 'stale'
  return 'expired'
}

function freshnessVariant(level: FreshnessLevel) {
  switch (level) {
    case 'fresh':
      return 'success' as const
    case 'stale':
      return 'warning' as const
    case 'expired':
      return 'destructive' as const
  }
}

// ---------------------------------------------------------------------------
// Status indicators
// ---------------------------------------------------------------------------

function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <Badge variant={connected ? 'success' : 'destructive'} className="gap-1.5">
      <span
        className={`inline-block h-2 w-2 rounded-full ${
          connected ? 'bg-emerald-500 dark:bg-emerald-400' : 'bg-red-500 dark:bg-red-400'
        }`}
      />
      {connected ? 'Connected' : 'Disconnected'}
    </Badge>
  )
}

function FreshnessBadge({ lastTickTime, timezone }: { lastTickTime: Date | null; timezone: string }) {
  const [, setTick] = useState(0)

  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1_000)
    return () => clearInterval(id)
  }, [])

  const level = getFreshnessLevel(lastTickTime)
  const label = lastTickTime
    ? `Fresh as of ${formatDate(lastTickTime, timezone, 'h:mm:ss a')}`
    : 'No data yet'

  return (
    <Badge variant={freshnessVariant(level)} className="gap-1.5">
      {label}
    </Badge>
  )
}

// ---------------------------------------------------------------------------
// RT Down banner
// ---------------------------------------------------------------------------

function RtDownBanner() {
  return (
    <Alert variant="destructive" className="mb-4">
      <AlertTriangle className="h-4 w-4" />
      <AlertDescription>
        Fastly RT unavailable — showing last-known-good values.
      </AlertDescription>
    </Alert>
  )
}

// ---------------------------------------------------------------------------
// Correlator Leaderboard - auto-refresh Top-N from /correlate endpoint
// ---------------------------------------------------------------------------

interface CorrelatorResult {
  dimension: string
  top: Array<{ value: string; count: number }>
  freshness: { latest_log_at: string | null; lag_seconds: number }
}

function CorrelatorLeaderboard({
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
  const [data, setData] = useState<CorrelatorResult | null>(null)
  const [loading, setLoading] = useState(false)

  const fetchData = useCallback(async () => {
    if (!serviceId) return
    setLoading(true)
    try {
      const res = await fetch(`/api/services/${serviceId}/control-room/correlate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...authHeaders() },
        body: JSON.stringify({ dimension, window_minutes: 60, limit }),
      })
      if (res.ok) {
        setData(await res.json())
      }
    } catch {
      // best-effort
    } finally {
      setLoading(false)
    }
  }, [serviceId, dimension, limit])

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      if (!cancelled) await fetchData()
    }
    run()
    const id = setInterval(fetchData, refreshInterval)
    return () => { cancelled = true; clearInterval(id) }
  }, [fetchData, refreshInterval])

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
      <div className="space-y-1.5">
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
                  {item.count.toLocaleString()}
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

function OverviewTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {

  const data = tick?.data
  const rps = (d: MetricsData) => d.requests_per_second
  const errRate = (d: MetricsData) => d.error_rate * 100
  const hitRatio = (d: MetricsData) => d.cache_hit_ratio * 100
  const bw = (d: MetricsData) => d.bandwidth_mbps

  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="Requests/s"
          value={data?.requests_per_second ?? 0}
          suffix=" req/s"
          rates={[{ label: '5s', value: rollingAvg(rps, 5) }, { label: '30s', value: rollingAvg(rps, 30) }, { label: '60s', value: rollingAvg(rps, 60) }]}
          dimmed={rtDown}
          helpText="Total HTTP requests per second across all Fastly edge PoPs. Rolling averages smooth out per-second spikes — the 60s average is most representative of sustained load."
        />
        <RealtimeMetricCard
          title="Error Rate"
          value={Number(((data?.error_rate ?? 0) * 100).toFixed(2))}
          suffix="%"
          rates={[{ label: '5s', value: rollingAvg(errRate, 5) }, { label: '30s', value: rollingAvg(errRate, 30) }, { label: '60s', value: rollingAvg(errRate, 60) }]}
          thresholds={{ warn: 1, critical: 5, direction: 'above' }}
          dimmed={rtDown}
          helpText="Percentage of responses with 4xx or 5xx status codes. Card turns amber above 1% and red above 5%."
        />
        <RealtimeMetricCard
          title="Cache Hit Ratio"
          value={Number(((data?.cache_hit_ratio ?? 0) * 100).toFixed(1))}
          suffix="%"
          rates={[{ label: '5s', value: rollingAvg(hitRatio, 5) }, { label: '30s', value: rollingAvg(hitRatio, 30) }, { label: '60s', value: rollingAvg(hitRatio, 60) }]}
          thresholds={{ warn: 90, critical: 50, direction: 'below' }}
          dimmed={rtDown}
          helpText="Percentage of requests served from Fastly's edge cache without reaching your origin. Higher is better — below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content."
        />
        <RealtimeMetricCard
          title="Bandwidth"
          value={Number((data?.bandwidth_mbps ?? 0).toFixed(2))}
          suffix=" Mbps"
          rates={[{ label: '5s', value: rollingAvg(bw, 5) }, { label: '30s', value: rollingAvg(bw, 30) }, { label: '60s', value: rollingAvg(bw, 60) }]}
          dimmed={rtDown}
          helpText="Total bandwidth delivered across all PoPs in megabits per second, including both cached and origin-fetched content."
        />
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Total Requests"
          helpText="Per-second request count across all PoPs. Sudden spikes may indicate traffic surges, bot activity, or DDoS events."
          yAxisSuffix=" req/s"
          traces={[
            {
              y: series((d) => d.total_requests ?? d.requests_per_second),
              name: 'Requests',
              color: '#3b82f6',
            },
          ]}
        />
        <RealtimeChart
          title="Status Code Breakdown"
          helpText="Per-second breakdown by HTTP status class. 2xx = success, 3xx = redirects, 4xx = client errors (broken links, auth failures), 5xx = origin/server errors."
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.status_breakdown?.status_1xx ?? 0), name: '1xx', color: '#94a3b8' },
            { y: series((d) => d.status_breakdown?.status_2xx ?? 0), name: '2xx', color: '#22c55e' },
            { y: series((d) => d.status_breakdown?.status_3xx ?? 0), name: '3xx', color: '#3b82f6' },
            { y: series((d) => d.status_breakdown?.status_4xx ?? 0), name: '4xx', color: '#eab308' },
            { y: series((d) => d.status_breakdown?.status_5xx ?? 0), name: '5xx', color: '#ef4444' },
          ]}
        />
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Performance"
          helpText="Hit = served from edge cache, Miss = fetched from origin and cached, Pass = bypassed cache entirely (uncacheable or VCL pass rules). A growing Pass ratio may indicate misconfigured cache rules."
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.total_hits ?? 0), name: 'Hit', color: '#22c55e' },
            { y: series((d) => d.total_miss ?? 0), name: 'Miss', color: '#f97316' },
            { y: series((d) => d.total_pass ?? 0), name: 'Pass', color: '#94a3b8' },
          ]}
        />
        <RealtimeChart
          title="Origin Load"
          helpText="Left axis: requests reaching your origin per second. Right axis: cache hit ratio — when it drops, more requests flow through to origin."
          dualYAxis
          yAxisSuffix=" req/s"
          y2AxisSuffix="%"
          traces={[
            {
              y: series((d) => d.origin_requests_per_second ?? 0),
              name: 'Origin req/s',
              color: '#3b82f6',
            },
            {
              y: series((d) => (d.cache_hit_ratio ?? 0) * 100),
              name: 'Cache Hit %',
              color: '#22c55e',
              yaxis: 'y2',
            },
          ]}
        />
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="2xx/3xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.status_detail?.["200"] ?? 0), name: '200', color: '#22c55e' },
            { y: series((d) => d.status_detail?.["204"] ?? 0), name: '204', color: '#86efac' },
            { y: series((d) => d.status_detail?.["301"] ?? 0), name: '301', color: '#3b82f6' },
            { y: series((d) => d.status_detail?.["302"] ?? 0), name: '302', color: '#93c5fd' },
            { y: series((d) => d.status_detail?.["304"] ?? 0), name: '304', color: '#6366f1' },
          ]}
        />
        <RealtimeChart
          title="4xx/5xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.status_detail?.["400"] ?? 0), name: '400', color: '#eab308' },
            { y: series((d) => d.status_detail?.["401"] ?? 0), name: '401', color: '#f59e0b' },
            { y: series((d) => d.status_detail?.["403"] ?? 0), name: '403', color: '#f97316' },
            { y: series((d) => d.status_detail?.["404"] ?? 0), name: '404', color: '#fb923c' },
            { y: series((d) => d.status_detail?.["429"] ?? 0), name: '429', color: '#ef4444' },
            { y: series((d) => d.status_detail?.["500"] ?? 0), name: '500', color: '#dc2626' },
            { y: series((d) => d.status_detail?.["502"] ?? 0), name: '502', color: '#b91c1c' },
            { y: series((d) => d.status_detail?.["503"] ?? 0), name: '503', color: '#991b1b' },
            { y: series((d) => d.status_detail?.["504"] ?? 0), name: '504', color: '#7f1d1d' },
          ]}
        />
      </div>

      <HistoricalLink href={historicalHref} label="overview" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Performance tab
// ---------------------------------------------------------------------------

function PerformanceTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const data = tick?.data
  const hitRatio = (d: MetricsData) => d.cache_hit_ratio * 100
  const originRps = (d: MetricsData) => d.origin_requests_per_second ?? 0
  const passReq = (d: MetricsData) => d.pass_requests ?? 0
  const offload = (d: MetricsData) => (d.origin_offload ?? 0) * 100
  const collapseUsable = (d: MetricsData) => d.request_collapse_usable ?? 0
  const collapseUnusable = (d: MetricsData) => d.request_collapse_unusable ?? 0
  const segOrigin = (d: MetricsData) => d.segblock_origin_fetches ?? 0
  const segShield = (d: MetricsData) => d.segblock_shield_fetches ?? 0
  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="Cache Hit Ratio"
          value={Number(((data?.cache_hit_ratio ?? 0) * 100).toFixed(1))}
          suffix="%"
          rates={[{ label: '5s', value: rollingAvg(hitRatio, 5) }, { label: '30s', value: rollingAvg(hitRatio, 30) }, { label: '60s', value: rollingAvg(hitRatio, 60) }]}
          thresholds={{ warn: 90, critical: 50, direction: 'below' }}
          dimmed={rtDown}
          helpText="Percentage of requests served from edge cache. Below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content."
        />
        <RealtimeMetricCard
          title="Origin Requests/s"
          value={data?.origin_requests_per_second ?? 0}
          suffix=" req/s"
          rates={[{ label: '5s', value: rollingAvg(originRps, 5) }, { label: '30s', value: rollingAvg(originRps, 30) }, { label: '60s', value: rollingAvg(originRps, 60) }]}
          dimmed={rtDown}
          helpText="Requests that reached your origin server per second. Lower is better — it means more traffic is being served from Fastly's edge cache."
        />
        <RealtimeMetricCard
          title="Bandwidth Offload"
          value={Number(((data?.origin_offload ?? 0) * 100).toFixed(1))}
          suffix="%"
          thresholds={{ warn: 80, critical: 50, direction: 'below' }}
          rates={[{ label: '5s', value: rollingAvg(offload, 5) }, { label: '30s', value: rollingAvg(offload, 30) }, { label: '60s', value: rollingAvg(offload, 60) }]}
          dimmed={rtDown}
          helpText="Percentage of total bandwidth served from cache vs. origin. Higher means less origin load and lower egress costs."
        />
        <RealtimeMetricCard
          title="Pass-through"
          value={data?.pass_requests ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(passReq, 5) }, { label: '30s', value: rollingAvg(passReq, 30) }, { label: '60s', value: rollingAvg(passReq, 60) }]}
          dimmed={rtDown}
          helpText="Requests that bypassed cache entirely — either uncacheable content types or explicit VCL pass rules. Every pass request hits your origin."
        />
      </div>

      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="Collapse Usable"
          value={data?.request_collapse_usable ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(collapseUsable, 5) }, { label: '30s', value: rollingAvg(collapseUsable, 30) }, { label: '60s', value: rollingAvg(collapseUsable, 60) }]}
          dimmed={rtDown}
          helpText="Requests eligible for request collapsing — multiple identical requests are collapsed into a single origin fetch. Higher counts mean more origin load reduction."
        />
        <RealtimeMetricCard
          title="Collapse Unusable"
          value={data?.request_collapse_unusable ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(collapseUnusable, 5) }, { label: '30s', value: rollingAvg(collapseUnusable, 30) }, { label: '60s', value: rollingAvg(collapseUnusable, 60) }]}
          dimmed={rtDown}
          helpText="Requests that could not be collapsed (e.g., different Vary headers, POST requests). High unusable counts may indicate opportunities to improve cache key design."
        />
        <RealtimeMetricCard
          title="Segment Origin Fetches"
          value={data?.segblock_origin_fetches ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(segOrigin, 5) }, { label: '30s', value: rollingAvg(segOrigin, 30) }, { label: '60s', value: rollingAvg(segOrigin, 60) }]}
          dimmed={rtDown}
          helpText="Segment block fetches to origin for large objects. Fastly breaks large files into segments cached independently, enabling range requests and parallel fetching."
        />
        <RealtimeMetricCard
          title="Segment Shield Fetches"
          value={data?.segblock_shield_fetches ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(segShield, 5) }, { label: '30s', value: rollingAvg(segShield, 30) }, { label: '60s', value: rollingAvg(segShield, 60) }]}
          dimmed={rtDown}
          helpText="Segment block fetches handled at the shield PoP. When these are high relative to origin fetches, segmented caching is working well."
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Hit Ratio"
          yAxisSuffix="%"
          traces={[
            {
              y: series((d) => d.cache_hit_ratio * 100),
              name: 'Hit Ratio %',
              color: '#22c55e',
              },
          ]}
        />
        <RealtimeChart
          title="Origin Latency Breakdown"
          yAxisSuffix=" req/s"
          traces={[
            { y: series((d) => d.origin_requests_per_second ?? 0), name: 'Origin req/s', color: '#3b82f6' },
            { y: series((d) => d.pass_requests ?? 0), name: 'Pass', color: '#f97316' },
          ]}
        />
      </div>
      <RealtimeChart
        title="Cache Path Latency (ms)"
        helpText="Time-to-first-byte by cache path. Hit latency is edge-only (fast). Miss includes origin round-trip plus caching. Pass includes origin round-trip with no caching overhead."
        yAxisSuffix=" ms"
        traces={[
          { y: series((d) => d.hit_latency_ms ?? 0), name: 'Hit', color: '#22c55e' },
          { y: series((d) => d.miss_latency_ms ?? 0), name: 'Miss', color: '#f97316' },
          { y: series((d) => d.pass_latency_ms ?? 0), name: 'Pass', color: '#94a3b8' },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Object Size Distribution"
          helpText="Distribution of response body sizes. Large objects may benefit from segmented caching; many small objects benefit from request collapsing."
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.object_size_distribution?.["1k"] ?? 0), name: '<1KB', color: '#94a3b8' },
            { y: series((d) => d.object_size_distribution?.["10k"] ?? 0), name: '1-10KB', color: '#3b82f6' },
            { y: series((d) => d.object_size_distribution?.["100k"] ?? 0), name: '10-100KB', color: '#22c55e' },
            { y: series((d) => d.object_size_distribution?.["1m"] ?? 0), name: '100KB-1MB', color: '#eab308' },
            { y: series((d) => d.object_size_distribution?.["10m"] ?? 0), name: '1-10MB', color: '#f97316' },
            { y: series((d) => d.object_size_distribution?.["100m"] ?? 0), name: '10-100MB', color: '#ef4444' },
            { y: series((d) => d.object_size_distribution?.["1g"] ?? 0), name: '100MB-1GB', color: '#8b5cf6' },
            { y: series((d) => d.object_size_distribution?.["other"] ?? 0), name: 'Other', color: '#6b7280' },
          ]}
        />
        <RealtimeChart
          title="Origin Response Time (ms)"
          helpText="Histogram of origin response times for cache misses. A shift toward higher buckets (>250ms) signals origin degradation or upstream latency."
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.miss_histogram?.["10"] ?? 0), name: '<10ms', color: '#22c55e' },
            { y: series((d) => d.miss_histogram?.["20"] ?? 0), name: '10-20ms', color: '#86efac' },
            { y: series((d) => d.miss_histogram?.["30"] ?? 0), name: '20-30ms', color: '#3b82f6' },
            { y: series((d) => d.miss_histogram?.["60"] ?? 0), name: '30-60ms', color: '#93c5fd' },
            { y: series((d) => d.miss_histogram?.["120"] ?? 0), name: '60-120ms', color: '#eab308' },
            { y: series((d) => d.miss_histogram?.["250"] ?? 0), name: '120-250ms', color: '#f97316' },
            { y: series((d) => d.miss_histogram?.["500"] ?? 0), name: '250-500ms', color: '#ef4444' },
            { y: series((d) => d.miss_histogram?.["1000"] ?? 0), name: '500ms-1s', color: '#dc2626' },
            { y: series((d) => {
              const h = d.miss_histogram ?? {}
              const known = ["10","20","30","60","120","250","500","1000"]
              return Object.entries(h).reduce((sum, [k, v]) => known.includes(k) ? sum : sum + v, 0)
            }), name: '>1s', color: '#7f1d1d' },
          ]}
        />
      </div>
      <HistoricalLink href={historicalHref} label="performance" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Origin tab
// ---------------------------------------------------------------------------

function OriginTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const data = tick?.data
  const originRps = (d: MetricsData) => d.origin_requests_per_second ?? 0
  const originBw = (d: MetricsData) => d.origin_bandwidth_mbps ?? 0
  const shieldReq = (d: MetricsData) => d.shield_requests ?? 0
  const shieldHit = (d: MetricsData) => (d.shield_hit_ratio ?? 0) * 100
  const originFetches = (d: MetricsData) => d.origin_fetches ?? 0
  const originReval = (d: MetricsData) => d.origin_revalidations ?? 0
  const originCacheFetches = (d: MetricsData) => d.origin_cache_fetches ?? 0
  const shieldHitReq = (d: MetricsData) => d.shield_hit_requests ?? 0
  const shieldMissReq = (d: MetricsData) => d.shield_miss_requests ?? 0
  const shieldReval = (d: MetricsData) => d.shield_revalidations ?? 0
  const shieldFetchBytes = (d: MetricsData) => (d.shield_fetch_body_bytes ?? 0) / 1_000_000
  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="Origin Requests/s"
          value={data?.origin_requests_per_second ?? 0}
          suffix=" req/s"
          rates={[{ label: '5s', value: rollingAvg(originRps, 5) }, { label: '30s', value: rollingAvg(originRps, 30) }, { label: '60s', value: rollingAvg(originRps, 60) }]}
          dimmed={rtDown}
          helpText="Requests per second reaching your origin server. Lower is better — cache and shield should absorb most traffic before it gets here."
        />
        <RealtimeMetricCard
          title="Origin Bandwidth"
          value={Number((data?.origin_bandwidth_mbps ?? 0).toFixed(2))}
          suffix=" Mbps"
          rates={[{ label: '5s', value: rollingAvg(originBw, 5) }, { label: '30s', value: rollingAvg(originBw, 30) }, { label: '60s', value: rollingAvg(originBw, 60) }]}
          dimmed={rtDown}
          helpText="Bandwidth transferred from your origin server to Fastly. This is your origin egress — lower means better cache offload."
        />
        <RealtimeMetricCard
          title="Shield Requests"
          value={data?.shield_requests ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(shieldReq, 5) }, { label: '30s', value: rollingAvg(shieldReq, 30) }, { label: '60s', value: rollingAvg(shieldReq, 60) }]}
          dimmed={rtDown}
          helpText="Requests routed through the shield PoP — a mid-tier cache between edge PoPs and your origin. The shield absorbs duplicate misses from multiple edge PoPs."
        />
        <RealtimeMetricCard
          title="Shield Hit Ratio"
          value={Number(((data?.shield_hit_ratio ?? 0) * 100).toFixed(1))}
          suffix="%"
          rates={[{ label: '5s', value: rollingAvg(shieldHit, 5) }, { label: '30s', value: rollingAvg(shieldHit, 30) }, { label: '60s', value: rollingAvg(shieldHit, 60) }]}
          dimmed={rtDown}
          helpText="Percentage of shield requests served from the shield cache. Higher means fewer requests reach your origin."
        />
        <RealtimeMetricCard
          title="Origin Fetches"
          value={data?.origin_fetches ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(originFetches, 5) }, { label: '30s', value: rollingAvg(originFetches, 30) }, { label: '60s', value: rollingAvg(originFetches, 60) }]}
          dimmed={rtDown}
          helpText="Total fetch requests sent from Fastly to your origin. Includes full fetches, revalidations, and cache-eligible fetches. This is the broadest measure of origin load."
        />
        <RealtimeMetricCard
          title="Revalidations"
          value={data?.origin_revalidations ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(originReval, 5) }, { label: '30s', value: rollingAvg(originReval, 30) }, { label: '60s', value: rollingAvg(originReval, 60) }]}
          dimmed={rtDown}
          helpText="Conditional requests (If-Modified-Since/If-None-Match) sent to your origin. When the origin returns 304 Not Modified, Fastly reuses the cached copy — cheaper than a full fetch."
        />
        <RealtimeMetricCard
          title="Cache Fetches"
          value={data?.origin_cache_fetches ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(originCacheFetches, 5) }, { label: '30s', value: rollingAvg(originCacheFetches, 30) }, { label: '60s', value: rollingAvg(originCacheFetches, 60) }]}
          dimmed={rtDown}
          helpText="Origin fetches that returned a cacheable response (the response will be stored and served to future requests). A low ratio of cache fetches to total fetches may indicate your origin is sending too many no-cache/private headers."
        />
      </div>
      <RealtimeChart
        title="Origin Load"
        dualYAxis
        yAxisSuffix=" req/s"
        y2AxisSuffix="%"
        traces={[
          { y: series((d) => d.origin_requests_per_second ?? 0), name: 'Origin req/s', color: '#3b82f6' },
          { y: series((d) => (d.shield_hit_ratio ?? 0) * 100), name: 'Shield Hit %', color: '#22c55e', yaxis: 'y2' },
        ]}
      />
      <RealtimeChart
        title="Origin Fetch Breakdown"
        helpText="Fetches = full origin requests. Revalidations = conditional requests (If-Modified-Since/ETag) that are cheaper than full fetches. Cache Fetches = requests to origin that resulted in a cacheable response."
        yAxisSuffix=" req/s"
        stacked
        traces={[
          { y: series((d) => d.origin_fetches ?? 0), name: 'Fetches', color: '#3b82f6' },
          { y: series((d) => d.origin_revalidations ?? 0), name: 'Revalidations', color: '#22c55e' },
          { y: series((d) => d.origin_cache_fetches ?? 0), name: 'Cache Fetches', color: '#eab308' },
        ]}
      />

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">Shield Efficiency</h3>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="Shield Hits"
          value={data?.shield_hit_requests ?? 0}
          rates={[{ label: '5s', value: rollingAvg(shieldHitReq, 5) }, { label: '30s', value: rollingAvg(shieldHitReq, 30) }, { label: '60s', value: rollingAvg(shieldHitReq, 60) }]}
          dimmed={rtDown}
          helpText="Requests served from the shield PoP's cache, preventing duplicate origin fetches from multiple edge PoPs."
        />
        <RealtimeMetricCard
          title="Shield Misses"
          value={data?.shield_miss_requests ?? 0}
          rates={[{ label: '5s', value: rollingAvg(shieldMissReq, 5) }, { label: '30s', value: rollingAvg(shieldMissReq, 30) }, { label: '60s', value: rollingAvg(shieldMissReq, 60) }]}
          dimmed={rtDown}
          helpText="Requests that missed the shield cache and were forwarded to your origin. Every shield miss becomes an origin fetch."
        />
        <RealtimeMetricCard
          title="Shield Revalidations"
          value={data?.shield_revalidations ?? 0}
          rates={[{ label: '5s', value: rollingAvg(shieldReval, 5) }, { label: '30s', value: rollingAvg(shieldReval, 30) }, { label: '60s', value: rollingAvg(shieldReval, 60) }]}
          dimmed={rtDown}
          helpText="Conditional requests at the shield tier — the shield checks with origin whether stale content is still valid. A 304 response avoids transferring the full object."
        />
        <RealtimeMetricCard
          title="Shield Fetch Bytes"
          value={Number(((data?.shield_fetch_body_bytes ?? 0) / 1_000_000).toFixed(2))}
          suffix=" MB"
          rates={[{ label: '5s', value: rollingAvg(shieldFetchBytes, 5) }, { label: '30s', value: rollingAvg(shieldFetchBytes, 30) }, { label: '60s', value: rollingAvg(shieldFetchBytes, 60) }]}
          dimmed={rtDown}
          helpText="Total bytes fetched through the shield tier from origin. This represents the bandwidth your origin actually serves — compare against total edge bandwidth to measure offload efficiency."
        />
      </div>
      <RealtimeChart
        title="Shield Hit/Miss/Revalidation"
        helpText="Shield-tier cache breakdown. Hits = served from shield cache. Misses = forwarded to origin. Revalidations = conditional requests (304 responses) that avoid full origin fetches."
        yAxisSuffix=" req/s"
        stacked
        traces={[
          { y: series((d) => d.shield_hit_requests ?? 0), name: 'Hits', color: '#22c55e' },
          { y: series((d) => d.shield_miss_requests ?? 0), name: 'Misses', color: '#f97316' },
          { y: series((d) => d.shield_revalidations ?? 0), name: 'Revalidations', color: '#3b82f6' },
        ]}
      />
      <HistoricalLink href={historicalHref} label="origin" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Security tab
// ---------------------------------------------------------------------------

function SecurityTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const data = tick?.data
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const wafBlocked = (d: MetricsData) => d.waf_blocked ?? 0
  const wafLogged = (d: MetricsData) => d.waf_logged ?? 0
  const s4xx = (d: MetricsData) => d.status_breakdown?.status_4xx ?? 0
  const s5xx = (d: MetricsData) => d.status_breakdown?.status_5xx ?? 0

  const issued = data?.bot_challenges_issued ?? 0
  const succeeded = data?.bot_challenges_succeeded ?? 0
  const failed = data?.bot_challenges_failed ?? 0
  const challengePassRate = issued > 0 ? ((succeeded / issued) * 100).toFixed(1) : '0.0'

  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard
          title="WAF Blocked"
          value={data?.waf_blocked ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(wafBlocked, 5) }, { label: '30s', value: rollingAvg(wafBlocked, 30) }, { label: '60s', value: rollingAvg(wafBlocked, 60) }]}
          dimmed={rtDown}
          helpText="Requests blocked by the Web Application Firewall. These matched a WAF rule configured to block — the request never reached your origin."
        />
        <RealtimeMetricCard
          title="WAF Logged"
          value={data?.waf_logged ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(wafLogged, 5) }, { label: '30s', value: rollingAvg(wafLogged, 30) }, { label: '60s', value: rollingAvg(wafLogged, 60) }]}
          dimmed={rtDown}
          helpText="Requests that triggered a WAF rule in logging mode — suspicious but allowed through. Use logged events to tune rules before switching them to block mode."
        />
        <RealtimeMetricCard
          title="4xx Responses"
          value={data?.status_breakdown?.status_4xx ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(s4xx, 5) }, { label: '30s', value: rollingAvg(s4xx, 30) }, { label: '60s', value: rollingAvg(s4xx, 60) }]}
          dimmed={rtDown}
          helpText="Client error responses (400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 429 Rate Limited). Spikes may indicate scanning activity, broken links, or auth issues."
        />
        <RealtimeMetricCard
          title="5xx Responses"
          value={data?.status_breakdown?.status_5xx ?? 0}
          suffix=" req"
          rates={[{ label: '5s', value: rollingAvg(s5xx, 5) }, { label: '30s', value: rollingAvg(s5xx, 30) }, { label: '60s', value: rollingAvg(s5xx, 60) }]}
          thresholds={{ warn: 1, critical: 10, direction: 'above' }}
          dimmed={rtDown}
          helpText="Server error responses. 502/503 typically indicate origin unreachable or overloaded. 500 indicates an application error. Any sustained 5xx traffic warrants immediate investigation."
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="WAF Events"
          helpText="Blocked = attack prevented at the edge. Logged = suspicious traffic allowed through for monitoring. Passed = clean traffic that matched no WAF rules."
          yAxisSuffix=" req/s"
          stacked
          traces={[
            { y: series((d) => d.waf_blocked ?? 0), name: 'Blocked', color: '#ef4444' },
            { y: series((d) => d.waf_logged ?? 0), name: 'Logged', color: '#eab308' },
            { y: series((d) => d.waf_passed ?? 0), name: 'Passed', color: '#22c55e' },
          ]}
        />
        <RealtimeChart
          title="Error Codes"
          helpText="4xx errors are client-side (bad requests, auth failures, not found). 5xx errors are server-side (origin errors, timeouts). Correlated spikes suggest an incident."
          yAxisSuffix=" req/s"
          traces={[
            { y: series((d) => d.status_breakdown?.status_4xx ?? 0), name: '4xx', color: '#eab308' },
            { y: series((d) => d.status_breakdown?.status_5xx ?? 0), name: '5xx', color: '#ef4444' },
          ]}
        />
      </div>

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">Bot Challenge Funnel</h3>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <RealtimeMetricCard title="Challenges Issued" value={issued} suffix=" req" dimmed={rtDown} helpText="Number of bot challenges (CAPTCHAs, JS challenges) issued to suspicious clients. High counts indicate active bot pressure." />
        <RealtimeMetricCard title="Challenges Passed" value={succeeded} suffix=" req" dimmed={rtDown} helpText="Clients that successfully completed the challenge — likely legitimate users or sophisticated bots." />
        <RealtimeMetricCard title="Challenges Failed" value={failed} suffix=" req" dimmed={rtDown} helpText="Clients that failed the challenge — almost certainly automated bots unable to solve CAPTCHAs or execute JavaScript." />
        <RealtimeMetricCard title="Pass Rate" value={Number(challengePassRate)} suffix="%" dimmed={rtDown} helpText="Percentage of issued challenges that were passed. Very low rates suggest the challenges are effectively blocking bots. Very high rates may mean challenges are too easy or mostly hitting real users." />
      </div>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        <RealtimeMetricCard title="Bots Detected" value={data?.bot_detected ?? 0} suffix=" req" dimmed={rtDown} helpText="Requests classified as bot traffic by Fastly's bot detection engine, based on behavioral fingerprinting and reputation data." />
        <RealtimeMetricCard title="Verified Bots" value={data?.bot_verified ?? 0} suffix=" req" dimmed={rtDown} helpText="Verified legitimate bots like Googlebot, Bingbot, and other search engine crawlers that have been authenticated via reverse DNS or IP verification." />
        <RealtimeMetricCard title="AI Crawlers" value={data?.bot_ai_crawlers ?? 0} suffix=" req" dimmed={rtDown} helpText="Requests from identified AI training crawlers (GPTBot, ClaudeBot, etc.). These can consume significant bandwidth — consider blocking via robots.txt or WAF rules." />
      </div>
      <RealtimeChart
        title="Bot Challenge Pipeline"
        helpText="Bot management funnel. Passed = completed challenge (likely human). Failed = failed challenge (likely bot). Detected = classified as bot by fingerprinting. AI Crawlers = identified AI training crawlers."
        yAxisSuffix=" req/s"
        stacked
        traces={[
          { y: series((d) => d.bot_challenges_succeeded ?? 0), name: 'Passed', color: '#22c55e' },
          { y: series((d) => d.bot_challenges_failed ?? 0), name: 'Failed', color: '#ef4444' },
          { y: series((d) => d.bot_detected ?? 0), name: 'Detected', color: '#eab308' },
          { y: series((d) => d.bot_ai_crawlers ?? 0), name: 'AI Crawlers', color: '#8b5cf6' },
        ]}
      />

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">DDoS Protection</h3>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeMetricCard title="DDoS Detected" value={data?.ddos_detect ?? 0} suffix=" req" dimmed={rtDown} helpText="Requests classified as distributed denial-of-service by Fastly's automatic DDoS protection. Detection is always on — no configuration required." />
        <RealtimeMetricCard title="DDoS Mitigated" value={data?.ddos_mitigate ?? 0} suffix=" req" dimmed={rtDown} helpText="DDoS requests that were actively mitigated (blackholed, tarpitted, closed, or downgraded). The gap between detected and mitigated indicates requests still under analysis." />
      </div>
      <RealtimeChart
        title="DDoS Mitigation Actions"
        helpText="Blackhole = all traffic dropped. Tarpit = connections intentionally slowed. Close = connections terminated. Downgrade = service quality reduced. Any non-zero values indicate active DDoS mitigation."
        yAxisSuffix=" req/s"
        stacked
        traces={[
          { y: series((d) => d.ddos_action_blackhole ?? 0), name: 'Blackhole', color: '#1e293b' },
          { y: series((d) => d.ddos_action_tarpit ?? 0), name: 'Tarpit', color: '#7c3aed' },
          { y: series((d) => d.ddos_action_close ?? 0), name: 'Close', color: '#ef4444' },
          { y: series((d) => d.ddos_action_downgrade ?? 0), name: 'Downgrade', color: '#eab308' },
        ]}
      />

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">WAF Signal Classification</h3>
      <Card>
        <CardContent className="pt-4">
          <CorrelatorLeaderboard dimension="waf_sig" serviceId={activeServiceId} limit={5} />
        </CardContent>
      </Card>

      <HistoricalLink href={historicalHref} label="security" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Network tab
// ---------------------------------------------------------------------------

function NetworkTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const data = tick?.data
  const degradedCount = data?.degraded_pops?.length ?? 0
  const popCount = data?.pop_count ?? 0
  const healthyCount = popCount - degradedCount
  const pops = (d: MetricsData) => d.pop_count ?? 0
  const healthy = (d: MetricsData) => (d.pop_count ?? 0) - (d.degraded_pops?.length ?? 0)
  const degraded = (d: MetricsData) => d.degraded_pops?.length ?? 0
  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        <RealtimeMetricCard
          title="Active PoPs"
          value={popCount}
          rates={[{ label: '5s', value: rollingAvg(pops, 5) }, { label: '30s', value: rollingAvg(pops, 30) }, { label: '60s', value: rollingAvg(pops, 60) }]}
          dimmed={rtDown}
          helpText="Number of Fastly Points of Presence actively serving traffic for this service. Each PoP is a data center location worldwide."
        />
        <RealtimeMetricCard
          title="Healthy PoPs"
          value={healthyCount}
          rates={[{ label: '5s', value: rollingAvg(healthy, 5) }, { label: '30s', value: rollingAvg(healthy, 30) }, { label: '60s', value: rollingAvg(healthy, 60) }]}
          dimmed={rtDown}
          helpText="PoPs with error rates below the degradation threshold. A sudden drop may indicate a regional issue."
        />
        <RealtimeMetricCard
          title="Degraded PoPs"
          value={degradedCount}
          thresholds={{ warn: 1, critical: 3, direction: 'above' }}
          rates={[{ label: '5s', value: rollingAvg(degraded, 5) }, { label: '30s', value: rollingAvg(degraded, 30) }, { label: '60s', value: rollingAvg(degraded, 60) }]}
          dimmed={rtDown}
          helpText="PoPs with elevated error rates. Low-traffic PoPs may appear degraded during traffic lulls."
        />
      </div>
      {degradedCount > 0 && !rtDown && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {degradedCount} PoP(s) showing elevated errors: {data?.degraded_pops?.join(', ')}
          </AlertDescription>
        </Alert>
      )}
      <RealtimeChart
        title="PoP Count"
        yAxisSuffix=" PoPs"
        traces={[
          { y: series((d) => d.pop_count ?? 0), name: 'Active PoPs', color: '#3b82f6' },
          { y: series((d) => d.degraded_pops?.length ?? 0), name: 'Degraded', color: '#ef4444' },
        ]}
      />

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">Traffic Distribution</h3>
      <Card>
        <CardContent className="pt-4">
          <PopTrafficMap allPops={data?.all_pops ?? {}} className="h-[420px]" />
        </CardContent>
      </Card>

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">Protocol Adoption</h3>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-5">
        <RealtimeMetricCard
          title="HTTP/2"
          value={Number((data?.h2_pct ?? 0).toFixed(1))}
          suffix="%"
          dimmed={rtDown}
        />
        <RealtimeMetricCard
          title="HTTP/3"
          value={Number((data?.h3_pct ?? 0).toFixed(1))}
          suffix="%"
          dimmed={rtDown}
        />
        <RealtimeMetricCard
          title="IPv6"
          value={Number((data?.ipv6_pct ?? 0).toFixed(1))}
          suffix="%"
          dimmed={rtDown}
        />
        <RealtimeMetricCard
          title="TLS 1.2"
          value={Number((data?.tls12_pct ?? 0).toFixed(1))}
          suffix="%"
          dimmed={rtDown}
        />
        <RealtimeMetricCard
          title="TLS 1.3"
          value={Number((data?.tls13_pct ?? 0).toFixed(1))}
          suffix="%"
          dimmed={rtDown}
        />
      </div>
      <RealtimeChart
        title="Protocol Distribution"
        helpText="HTTP/2 uses multiplexed TCP streams. HTTP/3 uses QUIC (UDP-based) for better performance on lossy networks. 'Other' includes HTTP/1.1 and legacy protocols."
        yAxisSuffix=" req/s"
        stacked
        traces={[
          { y: series((d) => d.http2 ?? 0), name: 'HTTP/2', color: '#3b82f6' },
          { y: series((d) => d.http3 ?? 0), name: 'HTTP/3', color: '#22c55e' },
          { y: series((d) => Math.max(0, (d.total_requests ?? 0) - (d.http2 ?? 0) - (d.http3 ?? 0))), name: 'Other', color: '#94a3b8' },
        ]}
      />

      <HistoricalLink href={historicalHref} label="network" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Admin Health tab
// ---------------------------------------------------------------------------

function AdminHealthTab({
  tick,
  rtDown,
  connected,
  series,
  rollingAvg,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  connected: boolean
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const data = tick?.data
  const pops = (d: MetricsData) => d.pop_count ?? 0
  const degraded = (d: MetricsData) => d.degraded_pops?.length ?? 0
  const computeExec = (d: MetricsData) => d.compute_exec_time_ms ?? 0
  const computeReq = (d: MetricsData) => d.compute_req_time_ms ?? 0
  const restarts = (d: MetricsData) => d.restarts ?? 0
  return (
    <div className="space-y-6">
      {rtDown && <RtDownBanner />}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader className="pb-0">
            <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              RT Stream
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className={`text-3xl font-bold tabular-nums ${!connected ? 'opacity-50' : ''}`}>
              {connected ? (rtDown ? 'Degraded' : 'Healthy') : 'Disconnected'}
            </div>
          </CardContent>
        </Card>
        <RealtimeMetricCard
          title="Active PoPs"
          value={data?.pop_count ?? 0}
          rates={[{ label: '5s', value: rollingAvg(pops, 5) }, { label: '30s', value: rollingAvg(pops, 30) }, { label: '60s', value: rollingAvg(pops, 60) }]}
          dimmed={rtDown}
        />
        <RealtimeMetricCard
          title="Degraded PoPs"
          value={data?.degraded_pops?.length ?? 0}
          thresholds={{ warn: 1, critical: 3, direction: 'above' }}
          rates={[{ label: '5s', value: rollingAvg(degraded, 5) }, { label: '30s', value: rollingAvg(degraded, 30) }, { label: '60s', value: rollingAvg(degraded, 60) }]}
          dimmed={rtDown}
        />
      </div>

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">Scorer Health (Compute@Edge)</h3>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        <RealtimeMetricCard
          title="Execution Time"
          value={Number((data?.compute_exec_time_ms ?? 0).toFixed(1))}
          suffix=" ms"
          thresholds={{ warn: 50, critical: 200, direction: 'above' }}
          rates={[{ label: '5s', value: rollingAvg(computeExec, 5) }, { label: '30s', value: rollingAvg(computeExec, 30) }, { label: '60s', value: rollingAvg(computeExec, 60) }]}
          dimmed={rtDown}
          helpText="Compute@Edge Wasm execution time (CPU-only, excludes I/O waits). The scorer is cold on every request — watch for execution time creep after code changes."
        />
        <RealtimeMetricCard
          title="Request Time"
          value={Number((data?.compute_req_time_ms ?? 0).toFixed(1))}
          suffix=" ms"
          thresholds={{ warn: 100, critical: 500, direction: 'above' }}
          rates={[{ label: '5s', value: rollingAvg(computeReq, 5) }, { label: '30s', value: rollingAvg(computeReq, 30) }, { label: '60s', value: rollingAvg(computeReq, 60) }]}
          dimmed={rtDown}
          helpText="Total Compute@Edge request time including I/O (backend fetches, KV lookups). The gap between this and execution time is I/O wait."
        />
        <RealtimeMetricCard
          title="RAM Used"
          value={Number(((data?.compute_ram_used ?? 0) / 1_048_576).toFixed(1))}
          suffix=" MB"
          dimmed={rtDown}
          helpText="Peak memory usage of the Compute@Edge scorer per request. The Wasm runtime allocates a fresh heap per request — if this climbs toward the limit, the scorer may start hitting Resource Exceeded errors."
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        <RealtimeMetricCard
          title="Backend Errors"
          value={data?.compute_bereq_errors ?? 0}
          thresholds={{ warn: 1, critical: 10, direction: 'above' }}
          dimmed={rtDown}
          helpText="Failed backend requests from the Compute scorer (e.g., origin unreachable). The scorer fails open on these — sessions are passed without scoring."
        />
        <RealtimeMetricCard
          title="Guest Errors"
          value={data?.compute_guest_errors ?? 0}
          thresholds={{ warn: 1, critical: 5, direction: 'above' }}
          dimmed={rtDown}
          helpText="Wasm runtime errors in the scorer (panics, traps, invalid memory access). Any non-zero value warrants investigation — it means the scorer code is hitting an unhandled edge case."
        />
        <RealtimeMetricCard
          title="Resource Exceeded"
          value={data?.compute_resource_exceeded ?? 0}
          thresholds={{ warn: 1, critical: 5, direction: 'above' }}
          dimmed={rtDown}
          helpText="Requests where the scorer exceeded its CPU or memory allocation. The scorer is cold on every request — if this grows, the scorer binary may need optimization or wasm-opt tuning."
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Compute Latency (ms)"
          helpText="Execution = Wasm CPU time only. Request = total time including I/O waits. The gap between them is I/O wait (backend fetches, KV lookups). Watch for execution time creep after scorer code changes."
          yAxisSuffix=" ms"
          traces={[
            { y: series((d) => d.compute_exec_time_ms ?? 0), name: 'Execution', color: '#3b82f6' },
            { y: series((d) => d.compute_req_time_ms ?? 0), name: 'Request', color: '#f97316' },
          ]}
        />
        <RealtimeChart
          title="Compute Errors"
          helpText="Backend = origin request failures. Guest = Wasm runtime panics/traps. Resource Limit = CPU or memory exceeded. Any sustained non-zero errors mean the scorer is degraded and sessions are failing open."
          yAxisSuffix=" err/s"
          stacked
          traces={[
            { y: series((d) => d.compute_bereq_errors ?? 0), name: 'Backend', color: '#ef4444' },
            { y: series((d) => d.compute_guest_errors ?? 0), name: 'Guest', color: '#eab308' },
            { y: series((d) => d.compute_resource_exceeded ?? 0), name: 'Resource Limit', color: '#8b5cf6' },
          ]}
        />
      </div>

      <h3 className="text-sm font-semibold tracking-wide uppercase text-muted-foreground border-b pb-2">VCL Execution</h3>
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-5">
        <RealtimeMetricCard title="Restarts" value={data?.restarts ?? 0} suffix="/s" rates={[{ label: '5s', value: rollingAvg(restarts, 5) }, { label: '30s', value: rollingAvg(restarts, 30) }, { label: '60s', value: rollingAvg(restarts, 60) }]} thresholds={{ warn: 1, critical: 10, direction: 'above' }} dimmed={rtDown} helpText="VCL restart count per second. Restarts re-enter vcl_recv with modified state — used for routing fallbacks but expensive. High rates indicate a misconfigured restart loop." />
        <RealtimeMetricCard title="vcl_recv" value={Number((data?.vcl_recv_time_ms ?? 0).toFixed(1))} suffix=" ms" dimmed={rtDown} helpText="Time in vcl_recv — request routing, ACL checks, header manipulation. High values may indicate complex routing logic or excessive table lookups." />
        <RealtimeMetricCard title="vcl_fetch" value={Number((data?.vcl_fetch_time_ms ?? 0).toFixed(1))} suffix=" ms" dimmed={rtDown} helpText="Time in vcl_fetch — origin response processing, cache TTL setting, header rewriting. Only runs on cache misses." />
        <RealtimeMetricCard title="vcl_deliver" value={Number((data?.vcl_deliver_time_ms ?? 0).toFixed(1))} suffix=" ms" dimmed={rtDown} helpText="Time in vcl_deliver — final response modification before sending to the client. Runs on every request (hits and misses)." />
        <RealtimeMetricCard title="vcl_error" value={Number((data?.vcl_error_time_ms ?? 0).toFixed(1))} suffix=" ms" dimmed={rtDown} helpText="Time in vcl_error — synthetic error response generation. Only runs when VCL explicitly triggers an error or the origin is unreachable." />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="VCL Subroutine Time (ms)"
          helpText="Time spent in each VCL subroutine per request. recv = request handling, fetch = origin response processing, deliver = client response, error = error page generation. High recv time may indicate complex routing logic."
          yAxisSuffix=" ms"
          stacked
          traces={[
            { y: series((d) => d.vcl_recv_time_ms ?? 0), name: 'recv', color: '#3b82f6' },
            { y: series((d) => d.vcl_fetch_time_ms ?? 0), name: 'fetch', color: '#f97316' },
            { y: series((d) => d.vcl_deliver_time_ms ?? 0), name: 'deliver', color: '#22c55e' },
            { y: series((d) => d.vcl_error_time_ms ?? 0), name: 'error', color: '#ef4444' },
          ]}
        />
        <RealtimeChart
          title="VCL Subroutine Calls"
          helpText="Number of times each VCL subroutine was invoked. Call count × time per call = total VCL overhead. A high error count means many requests are hitting synthetic error responses."
          yAxisSuffix=" calls/s"
          stacked
          traces={[
            { y: series((d) => d.vcl_recv_count ?? 0), name: 'recv', color: '#3b82f6' },
            { y: series((d) => d.vcl_fetch_count ?? 0), name: 'fetch', color: '#f97316' },
            { y: series((d) => d.vcl_deliver_count ?? 0), name: 'deliver', color: '#22c55e' },
            { y: series((d) => d.vcl_error_count ?? 0), name: 'error', color: '#ef4444' },
          ]}
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
    </div>
  )
}

// ---------------------------------------------------------------------------
// Historical link helper
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tab content router
// ---------------------------------------------------------------------------

function TabContent({
  tabId,
  historicalHref,
  tick,
  rtDown,
  connected,
  series,
  rollingAvg,
}: {
  tabId: string
  historicalHref: string | null
  tick: MetricsTick | null
  rtDown: boolean
  connected: boolean
  series: (extractor: (d: MetricsData) => number) => number[]
  rollingAvg: (extractor: (d: MetricsData) => number, windowSize: number) => number
}) {
  const commonProps = { tick, rtDown, series, rollingAvg }
  switch (tabId) {
    case 'overview':
      return <OverviewTab {...commonProps} historicalHref={historicalHref ?? '/dashboard'} />
    case 'performance':
      return <PerformanceTab {...commonProps} historicalHref={historicalHref ?? '/performance'} />
    case 'origin':
      return <OriginTab {...commonProps} historicalHref={historicalHref ?? '/origin'} />
    case 'security':
      return <SecurityTab {...commonProps} historicalHref={historicalHref ?? '/security'} />
    case 'network':
      return <NetworkTab {...commonProps} historicalHref={historicalHref ?? '/network'} />
    case 'admin':
      return <AdminHealthTab {...commonProps} connected={connected} />
    default:
      return null
  }
}

// ---------------------------------------------------------------------------
// Mobile overview (stripped-down for < 768px)
// ---------------------------------------------------------------------------

function MobileOverview({ connected, lastTickTime, timezone, tick, rtDown }: {
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
              <RealtimeMetricCard
                title="Requests/s"
                value={data.requests_per_second}
                suffix=" req/s"
              />
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
              <RealtimeMetricCard
                title="Bandwidth"
                value={Number(data.bandwidth_mbps.toFixed(2))}
                suffix=" Mbps"
              />
            </div>
          ) : (
            <p className="text-muted-foreground text-sm">
              Waiting for metrics stream...
            </p>
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

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function ControlRoomClient() {
  const [activeTab, setActiveTab] = useState<TabId>('overview')
  const isAnalyst = useIsAnalyst()
  const timezone = useTimezone()
  const { connected, lastTickTime, latestTick, allTicks, rtDown } = useRealtimeStream()
  const { series, rollingAvg } = useTickHistory(allTicks)

  const visibleTabs = useMemo(
    () => TABS.filter((tab) => !('adminOnly' in tab && tab.adminOnly) || !isAnalyst),
    [isAnalyst],
  )

  const headerActions = (
    <div className="flex items-center gap-2 flex-wrap">
      <ConnectionBadge connected={connected} />
      <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
    </div>
  )

  return (
    <ReportShell
      title="Control Room"
      icon={Radio}
      description="Real-time SRE monitoring"
      headerActions={headerActions}
    >
      {/* Mobile: show stripped-down overview only */}
      <MobileOverview
        connected={connected}
        lastTickTime={lastTickTime}
        timezone={timezone}
        tick={latestTick}
        rtDown={rtDown}
      />

      {/* Desktop: full tabbed interface */}
      <div className="hidden md:block">
        <Tabs
          defaultValue="overview"
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as TabId)}
        >
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
              />
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </ReportShell>
  )
}
