'use client'

import { useState, useEffect, useMemo } from 'react'
import { Radio, ArrowRight, AlertTriangle } from 'lucide-react'
import Link from 'next/link'
import { ReportShell } from '@/components/ReportShell'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/serviceStore'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { useTimezone } from '@/hooks/useTimezone'
import { formatDate } from '@/lib/date'

// ---------------------------------------------------------------------------
// Tab definitions
// ---------------------------------------------------------------------------

const TABS = [
  { id: 'overview', label: 'Overview', historicalHref: '/dashboard' },
  { id: 'performance', label: 'Performance', historicalHref: '/performance' },
  { id: 'origin', label: 'Origin', historicalHref: '/origin' },
  { id: 'security', label: 'Security', historicalHref: '/security' },
  { id: 'network', label: 'Network', historicalHref: '/network' },
  { id: 'sessions', label: 'Sessions', historicalHref: '/sessions' },
  { id: 'cost', label: 'Cost', historicalHref: '/usage' },
  { id: 'insights', label: 'Insights', historicalHref: '/insights' },
  { id: 'admin', label: 'Admin', historicalHref: null, adminOnly: true },
] as const

type TabId = (typeof TABS)[number]['id']

// ---------------------------------------------------------------------------
// Metrics tick data shape
// ---------------------------------------------------------------------------

interface MetricsData {
  requests_per_second: number
  error_rate: number
  cache_hit_ratio: number
  bandwidth_mbps: number
  status_breakdown?: Record<string, number>
  estimated_cost_usd?: number
  origin_requests_per_second?: number
  origin_bandwidth_mbps?: number
  shield_requests?: number
  shield_hit_ratio?: number
  pass_requests?: number
  synth_requests?: number
  waf_blocked?: number
  waf_logged?: number
  waf_passed?: number
  pop_count?: number
  degraded_pops?: string[]
}

interface MetricsTick {
  event: string
  event_schema_version: number
  timestamp: string
  status: 'ok' | 'rt_down'
  data: MetricsData
  aggregate_delay?: number
}

// ---------------------------------------------------------------------------
// useRealtimeStream - wraps useSSE for the realtime endpoint
// ---------------------------------------------------------------------------

interface RealtimeStreamState {
  connected: boolean
  lastTickTime: Date | null
  latestTick: MetricsTick | null
  rtDown: boolean
}

function parseMetricsTick(line: SSELine): MetricsTick | null {
  if (line.event !== 'metrics_tick' && line.type !== 'metrics_tick') return null
  const data = (line.data ?? line) as Record<string, unknown>
  if (!data || typeof data !== 'object') return null
  const metricsData = data.data as MetricsData | undefined
  if (!metricsData) return null
  return {
    event: 'metrics_tick',
    event_schema_version: (data.event_schema_version as number) ?? 1,
    timestamp: (data.timestamp as string) ?? new Date().toISOString(),
    status: (data.status as 'ok' | 'rt_down') ?? 'ok',
    data: metricsData,
    aggregate_delay: data.aggregate_delay as number | undefined,
  }
}

function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, stop } = useSSE()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  useEffect(() => {
    if (!activeServiceId) return
    start(`/api/services/${activeServiceId}/realtime-stream`)
    return () => {
      stop()
    }
  }, [activeServiceId, start, stop])

  const { latestTick, lastTickTime } = useMemo(() => {
    if (lines.length === 0) return { latestTick: null, lastTickTime: null }
    const latest = lines[lines.length - 1]
    if (!latest) return { latestTick: null, lastTickTime: null }
    const tick = parseMetricsTick(latest)
    return {
      latestTick: tick,
      lastTickTime: tick ? new Date() : null,
    }
  }, [lines])

  return {
    connected: status === 'streaming',
    lastTickTime,
    latestTick,
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
// Metric card
// ---------------------------------------------------------------------------

function MetricCard({
  title,
  value,
  unit,
  dimmed,
}: {
  title: string
  value: string | number
  unit?: string
  dimmed?: boolean
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className={`text-2xl font-bold tabular-nums ${dimmed ? 'opacity-50' : ''}`}>
          {value}
          {unit && <span className="text-muted-foreground ml-1 text-sm font-normal">{unit}</span>}
        </div>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Overview tab (live data)
// ---------------------------------------------------------------------------

function OverviewTab({
  tick,
  rtDown,
  historicalHref,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="Requests/s"
          value={data?.requests_per_second ?? 0}
          unit="req/s"
          dimmed={rtDown}
        />
        <MetricCard
          title="Error Rate"
          value={`${((data?.error_rate ?? 0) * 100).toFixed(2)}%`}
          dimmed={rtDown}
        />
        <MetricCard
          title="Cache Hit Ratio"
          value={`${((data?.cache_hit_ratio ?? 0) * 100).toFixed(1)}%`}
          dimmed={rtDown}
        />
        <MetricCard
          title="Bandwidth"
          value={(data?.bandwidth_mbps ?? 0).toFixed(2)}
          unit="Mbps"
          dimmed={rtDown}
        />
      </div>
      <HistoricalLink href={historicalHref} label="overview" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Cost tab (live data)
// ---------------------------------------------------------------------------

function CostTab({
  tick,
  rtDown,
  historicalHref,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  const bwGb = (data?.bandwidth_mbps ?? 0) / 8 / 1000
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <MetricCard
          title="Estimated Cost"
          value={`$${(data?.estimated_cost_usd ?? 0).toFixed(4)}`}
          unit="/tick"
          dimmed={rtDown}
        />
        <MetricCard
          title="Requests Billed"
          value={data?.requests_per_second ?? 0}
          unit="req/s"
          dimmed={rtDown}
        />
        <MetricCard
          title="Bandwidth"
          value={bwGb.toFixed(4)}
          unit="GB/s"
          dimmed={rtDown}
        />
      </div>
      <HistoricalLink href={historicalHref} label="cost" />
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
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="Cache Hit Ratio"
          value={`${((data?.cache_hit_ratio ?? 0) * 100).toFixed(1)}%`}
          dimmed={rtDown}
        />
        <MetricCard
          title="Origin Requests/s"
          value={data?.origin_requests_per_second ?? 0}
          unit="req/s"
          dimmed={rtDown}
        />
        <MetricCard
          title="Pass-through"
          value={data?.pass_requests ?? 0}
          unit="req"
          dimmed={rtDown}
        />
        <MetricCard
          title="Synth Responses"
          value={data?.synth_requests ?? 0}
          unit="req"
          dimmed={rtDown}
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
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="Origin Requests/s"
          value={data?.origin_requests_per_second ?? 0}
          unit="req/s"
          dimmed={rtDown}
        />
        <MetricCard
          title="Origin Bandwidth"
          value={(data?.origin_bandwidth_mbps ?? 0).toFixed(2)}
          unit="Mbps"
          dimmed={rtDown}
        />
        <MetricCard
          title="Shield Requests"
          value={data?.shield_requests ?? 0}
          unit="req"
          dimmed={rtDown}
        />
        <MetricCard
          title="Shield Hit Ratio"
          value={`${((data?.shield_hit_ratio ?? 0) * 100).toFixed(1)}%`}
          dimmed={rtDown}
        />
      </div>
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
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  const s4xx = data?.status_breakdown?.status_4xx ?? 0
  const s5xx = data?.status_breakdown?.status_5xx ?? 0
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <MetricCard
          title="WAF Blocked"
          value={data?.waf_blocked ?? 0}
          unit="req"
          dimmed={rtDown}
        />
        <MetricCard
          title="WAF Logged"
          value={data?.waf_logged ?? 0}
          unit="req"
          dimmed={rtDown}
        />
        <MetricCard
          title="4xx Responses"
          value={s4xx}
          unit="req"
          dimmed={rtDown}
        />
        <MetricCard
          title="5xx Responses"
          value={s5xx}
          unit="req"
          dimmed={rtDown}
        />
      </div>
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
}: {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
}) {
  const data = tick?.data
  const degradedCount = data?.degraded_pops?.length ?? 0
  const popCount = data?.pop_count ?? 0
  const healthyCount = popCount - degradedCount
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <MetricCard
          title="Active PoPs"
          value={popCount}
          dimmed={rtDown}
        />
        <MetricCard
          title="Healthy PoPs"
          value={healthyCount}
          dimmed={rtDown}
        />
        <MetricCard
          title="Degraded PoPs"
          value={degradedCount}
          dimmed={rtDown}
        />
      </div>
      {degradedCount > 0 && !rtDown && (
        <Alert variant="destructive" className="mt-4">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {degradedCount} PoP(s) showing elevated errors: {data?.degraded_pops?.join(', ')}
          </AlertDescription>
        </Alert>
      )}
      <HistoricalLink href={historicalHref} label="network" />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tabs requiring historical (ingested) log data
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Admin Health tab
// ---------------------------------------------------------------------------

function AdminHealthTab({
  tick,
  rtDown,
  connected,
}: {
  tick: MetricsTick | null
  rtDown: boolean
  connected: boolean
}) {
  const data = tick?.data
  return (
    <div>
      {rtDown && <RtDownBanner />}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <MetricCard
          title="RT Stream"
          value={connected ? (rtDown ? 'Degraded' : 'Healthy') : 'Disconnected'}
          dimmed={!connected}
        />
        <MetricCard
          title="Active PoPs"
          value={data?.pop_count ?? 0}
          dimmed={rtDown}
        />
        <MetricCard
          title="Degraded PoPs"
          value={data?.degraded_pops?.length ?? 0}
          dimmed={rtDown}
        />
      </div>
      <div className="mt-4 flex justify-end">
        <Link href="/admin/trends">
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
// Tabs requiring historical (ingested) log data
// ---------------------------------------------------------------------------

function HistoricalDataRequired({
  label,
  historicalHref,
}: {
  label: string
  historicalHref: string | null
}) {
  return (
    <div className="grid gap-4 md:grid-cols-1">
      <Card>
        <CardContent className="pt-6">
          <p className="text-muted-foreground text-sm">
            {label} requires ingested log data and is available in the historical view.
          </p>
          {historicalHref && (
            <div className="mt-4">
              <Link href={historicalHref}>
                <Button variant="outline" size="sm">
                  View {label.toLowerCase()} data
                  <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
                </Button>
              </Link>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Historical link helper
// ---------------------------------------------------------------------------

function HistoricalLink({ href, label }: { href: string; label: string }) {
  return (
    <div className="mt-4 flex justify-end">
      <Link href={href}>
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
  label,
  tick,
  rtDown,
  connected,
}: {
  tabId: string
  historicalHref: string | null
  label: string
  tick: MetricsTick | null
  rtDown: boolean
  connected: boolean
}) {
  switch (tabId) {
    case 'overview':
      return <OverviewTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/dashboard'} />
    case 'performance':
      return <PerformanceTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/performance'} />
    case 'origin':
      return <OriginTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/origin'} />
    case 'security':
      return <SecurityTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/security'} />
    case 'network':
      return <NetworkTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/network'} />
    case 'cost':
      return <CostTab tick={tick} rtDown={rtDown} historicalHref={historicalHref ?? '/usage'} />
    case 'admin':
      return <AdminHealthTab tick={tick} rtDown={rtDown} connected={connected} />
    case 'sessions':
    case 'insights':
      return <HistoricalDataRequired label={label} historicalHref={historicalHref} />
    default:
      return <HistoricalDataRequired label={label} historicalHref={historicalHref} />
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
              <div>
                <div className="text-muted-foreground text-xs">Requests/s</div>
                <div className="text-lg font-bold tabular-nums">{data.requests_per_second}</div>
              </div>
              <div>
                <div className="text-muted-foreground text-xs">Error Rate</div>
                <div className="text-lg font-bold tabular-nums">{(data.error_rate * 100).toFixed(2)}%</div>
              </div>
              <div>
                <div className="text-muted-foreground text-xs">Cache Hit</div>
                <div className="text-lg font-bold tabular-nums">{(data.cache_hit_ratio * 100).toFixed(1)}%</div>
              </div>
              <div>
                <div className="text-muted-foreground text-xs">Bandwidth</div>
                <div className="text-lg font-bold tabular-nums">{data.bandwidth_mbps.toFixed(2)} Mbps</div>
              </div>
            </div>
          ) : (
            <p className="text-muted-foreground text-sm">
              Waiting for metrics stream...
            </p>
          )}
        </CardContent>
      </Card>
      <Link href="/dashboard">
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
  const { connected, lastTickTime, latestTick, rtDown } = useRealtimeStream()

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
      description="Real-time SRE & FinOps monitoring"
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
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>

          {visibleTabs.map((tab) => (
            <TabsContent key={tab.id} value={tab.id}>
              <TabContent
                tabId={tab.id}
                historicalHref={tab.historicalHref}
                label={tab.label}
                tick={latestTick}
                rtDown={rtDown}
                connected={connected}
              />
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </ReportShell>
  )
}
