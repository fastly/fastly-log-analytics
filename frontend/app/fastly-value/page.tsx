'use client'

import React, { useState } from 'react'
import {
  Shield,
  ShieldAlert,
  ShieldCheck,
  AlertTriangle,
  Network,
  Globe,
  Bot,
  Users,
  Gauge,
  HardDrive,
  ImageIcon,
  BarChart3,
  Layers,
  TrendingUp,
  TrendingDown,
  Zap,
  Timer,
  ArrowDownToLine,
  Wifi,
  Lock,
  Activity,
  DollarSign,
  Sparkles,
  ArrowRight,
} from 'lucide-react'
import { useAnalyticsReportQuery } from '@/hooks/useDashboardCards'
import { useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { client, unwrapClient } from "@/lib/api";
import { ReportLayout } from '@/components/ReportLayout'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly, FilterValueCell } from '@/components/DataTable'
import { StatCard } from '@/components/ui/confirm-dialog'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatBytes, formatCompactCount, formatCurrency, formatMs, formatPctValue, CHART_COLORS } from "@/lib/utils";
import type { components } from '@/types/api.generated'
import type { FiltersPayload } from '@/types/api'
import { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'

type ValueData = components['schemas']['ValueSummaryResponse']
type CachingData = components['schemas']['CachingMetrics']
type SecurityData = components['schemas']['SecurityMetrics']
type BotData = components['schemas']['BotMetrics']
type PerformanceData = components['schemas']['PerformanceMetrics']
type NetworkData = components['schemas']['NetworkMetrics']

const TABS = [
  { id: 'summary', label: 'Summary', icon: BarChart3 },
  { id: 'caching', label: 'CDN & Caching', icon: HardDrive },
  { id: 'assets', label: 'Assets & Shield', icon: Layers },
  { id: 'security', label: 'Security', icon: Shield },
  { id: 'bots', label: 'Bot Management', icon: Bot },
  { id: 'performance', label: 'Performance', icon: Gauge },
  { id: 'network', label: 'Network', icon: Network },
  { id: 'io', label: 'Image Optimizer', icon: ImageIcon },
] as const

const SUMMARY_SECTIONS = ['overview', 'caching', 'network'] as const

type TabId = (typeof TABS)[number]['id']

function groupCategoryTimeSeries(
  timeSeries: { time?: string | null; value: number; category?: string | null }[] | null | undefined,
  defaultCategory: string,
): Record<string, { x: string[]; y: number[] }> {
  const grouped: Record<string, { x: string[]; y: number[] }> = {}
  if (!timeSeries?.length) return grouped
  for (const p of timeSeries) {
    const cat = p.category ?? defaultCategory
    if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
    grouped[cat].x.push(p.time ?? '')
    grouped[cat].y.push(p.value)
  }
  return grouped
}

const OFFLOAD_LAYOUT = {
  yaxis: { title: 'Offload %', ticksuffix: '%', range: [0, 100] },
  xaxis: { type: 'date' as const },
  margin: { l: 50, r: 20, t: 10, b: 40 },
}

const CACHE_STATE_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const THREAT_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const SIGNAL_LAYOUT = {
  yaxis: { title: 'Count' },
  xaxis: { type: 'category' as const },
  margin: { l: 60, r: 20, t: 10, b: 80 },
}

const BOT_DONUT_LAYOUT = {
  margin: { l: 20, r: 20, t: 10, b: 10 },
  showlegend: true,
  legend: { orientation: 'h' as const, y: -0.15 },
}

const TOP_BOTS_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'category' as const },
  margin: { l: 60, r: 20, t: 10, b: 100 },
}

const LATENCY_LAYOUT = {
  yaxis: { title: 'Latency (ms)', ticksuffix: 'ms' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const PROTOCOL_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

export function OffloadAndCacheCharts({ caching }: { caching?: CachingData | null }) {
  const offloadData = React.useMemo(() => {
    if (!caching?.offload_time_series?.length) return []
    return [{
      x: caching.offload_time_series.map(p => p.time),
      y: caching.offload_time_series.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Offload %',
      line: { color: CHART_COLORS.success },
    }]
  }, [caching])

  const cacheStateData = React.useMemo(() => {
    if (!caching?.cache_state_time_series?.length) return []
    const grouped = groupCategoryTimeSeries(caching.cache_state_time_series, 'OTHER')
    const colors: Record<string, string> = {
      HIT: CHART_COLORS.success,
      MISS: CHART_COLORS.danger,
      PASS: CHART_COLORS.warning,
      OTHER: '#6b7280',
    }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [caching])

  return (
    <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
      <AnalyticsCard title="Origin Offload Trend" isEmpty={!offloadData.length}>
        <PlotlyChart data={offloadData} layout={OFFLOAD_LAYOUT} a11yTitle="Origin offload percentage over time" />
      </AnalyticsCard>
      <AnalyticsCard title="Cache State Breakdown" isEmpty={!cacheStateData.length}>
        <PlotlyChart data={cacheStateData} layout={CACHE_STATE_LAYOUT} a11yTitle="Cache state breakdown over time" />
      </AnalyticsCard>
    </div>
  )
}

function SummaryValueTab({ data, loading }: { data?: ValueData; loading?: boolean }) {
  const overview = data?.overview
  const caching = data?.caching
  const network = data?.network

  return (
    <div className="space-y-6 pt-4">
      {/* Hero KPIs */}
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Total Requests"
          icon={Gauge}
          loading={loading}
          value={overview?.total_requests != null ? formatCompactCount(overview.total_requests) : '—'}
          sub="processed at the edge"
          tooltip="Total requests processed by Fastly in the selected time range"
        />
        <StatCard
          title="Bandwidth Delivered"
          icon={HardDrive}
          loading={loading}
          value={overview?.total_bandwidth_bytes != null ? formatBytes(overview.total_bandwidth_bytes) : '—'}
          sub={overview?.origin_offload_pct != null ? `${overview.origin_offload_pct}% served from cache` : ''}
          tooltip="Total bandwidth delivered from edge and origin combined"
        />
        <StatCard
          title="Cache Acceleration"
          icon={Zap}
          loading={loading}
          value={overview?.cache_acceleration_factor != null ? `${overview.cache_acceleration_factor}x` : '—'}
          sub="faster than origin"
          tooltip="How much faster cached responses are compared to origin fetches"
        />
        <StatCard
          title="Threats Blocked"
          icon={Shield}
          loading={loading}
          value={overview?.threats_blocked != null ? formatCompactCount(overview.threats_blocked) : '—'}
          sub="attacks blocked by WAF"
          tooltip="Total requests blocked by Fastly's WAF and DDoS mitigation"
        />
      </div>

      {/* Expanded KPIs */}
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Origin Offload"
          icon={ArrowDownToLine}
          loading={loading}
          value={formatPctValue(caching?.origin_offload_pct)}
          sub="requests served from cache"
        />
        <StatCard
          title="Bandwidth Saved"
          icon={HardDrive}
          loading={loading}
          value={caching?.bandwidth_saved_bytes != null ? formatBytes(caching.bandwidth_saved_bytes) : '—'}
          sub="served from edge cache"
        />
        {network?.http3_pct != null && (
          <StatCard
            title="HTTP/3 Adoption"
            icon={Wifi}
            loading={loading}
            value={formatPctValue(network.http3_pct)}
            sub="of requests using QUIC"
          />
        )}
        {network?.tls_pct != null && (
          <StatCard
            title="TLS Encrypted"
            icon={Lock}
            loading={loading}
            value={formatPctValue(network.tls_pct)}
            sub={network?.ipv6_pct != null ? `${network.ipv6_pct}% IPv6` : ''}
          />
        )}
      </div>

      {/* Charts */}
      <OffloadAndCacheCharts caching={caching} />
    </div>
  )
}

function CachingValueTab({ data }: { data?: CachingData | null }) {
  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No caching data available for this service.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Origin Offload"
          icon={HardDrive}
          value={data.origin_offload_pct != null ? `${data.origin_offload_pct}%` : '—'}
          sub="requests served from cache"
        />
        <StatCard
          title="Bandwidth Saved"
          icon={ArrowDownToLine}
          value={data.bandwidth_saved_bytes != null ? formatBytes(data.bandwidth_saved_bytes) : '—'}
          sub="served from edge cache"
        />
        <StatCard
          title="Shield Effectiveness"
          icon={Shield}
          value={data.shield_effectiveness_pct != null ? `${data.shield_effectiveness_pct}%` : '—'}
          sub="shield hit ratio"
        />
        <StatCard
          title="Cache Breakdown"
          icon={Layers}
          value={data.hit_requests != null ? formatCompactCount(data.hit_requests) + ' hits' : '—'}
          sub={[
            data.miss_requests != null ? `${formatCompactCount(data.miss_requests)} miss` : null,
            data.pass_requests != null ? `${formatCompactCount(data.pass_requests)} pass` : null,
          ].filter(Boolean).join(' · ')}
        />
      </div>

      <OffloadAndCacheCharts caching={data} />
    </div>
  )
}

function SecurityValueTab({ data }: { data?: SecurityData | null }) {
  const threatData = React.useMemo(() => {
    if (!data?.threat_time_series?.length) return []
    const grouped = groupCategoryTimeSeries(data.threat_time_series, 'blocked')
    const colors: Record<string, string> = { blocked: CHART_COLORS.danger, logged: CHART_COLORS.warning }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat.charAt(0).toUpperCase() + cat.slice(1),
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  const signalData = React.useMemo(() => {
    if (!data?.top_waf_signals?.length) return []
    return [{
      x: data.top_waf_signals.map(s => s.signal),
      y: data.top_waf_signals.map(s => s.count),
      type: 'bar',
      marker: { color: '#6366f1' },
    }]
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No WAF data available. Enable WAF logging to see security metrics.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Threats Blocked"
          icon={ShieldAlert}
          value={data.waf_blocked != null ? formatCompactCount(data.waf_blocked) : '—'}
          sub="requests blocked by WAF"
        />
        <StatCard
          title="Threats Logged"
          icon={Shield}
          value={data.waf_logged != null ? formatCompactCount(data.waf_logged) : '—'}
          sub="flagged but allowed"
        />
        <StatCard
          title="WAF Passed"
          icon={ShieldCheck}
          value={data.waf_passed != null ? formatCompactCount(data.waf_passed) : '—'}
          sub="clean requests"
        />
        <StatCard
          title="WAF Signals"
          icon={AlertTriangle}
          value={data.top_waf_signals?.length ?? 0}
          sub="distinct signal types detected"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title="Threat Timeline" isEmpty={!threatData.length}>
          <PlotlyChart data={threatData} layout={THREAT_LAYOUT} a11yTitle="WAF blocked and logged requests over time" />
        </AnalyticsCard>
        <AnalyticsCard title="Top WAF Signals" isEmpty={!signalData.length}>
          <PlotlyChart data={signalData} layout={SIGNAL_LAYOUT} a11yTitle="Top WAF signal types by count" />
        </AnalyticsCard>
      </div>
    </div>
  )
}

function BotValueTab({ data }: { data?: BotData | null }) {
  const donutData = React.useMemo(() => {
    if (!data?.total_requests || !data?.bot_requests) return []
    const human = data.total_requests - data.bot_requests
    return [{
      values: [data.bot_requests, human],
      labels: ['Bot Traffic', 'Human Traffic'],
      type: 'pie',
      hole: 0.5,
      marker: { colors: [CHART_COLORS.warning, CHART_COLORS.success] },
      textinfo: 'label+percent',
    }]
  }, [data])

  const topBotsData = React.useMemo(() => {
    if (!data?.top_bots?.length) return []
    return [{
      x: data.top_bots.map(b => b.name),
      y: data.top_bots.map(b => b.count),
      type: 'bar',
      marker: { color: CHART_COLORS.warning },
    }]
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No bot data available. Bot detection requires User-Agent logging.</p>
  }

  const botPct = data.total_requests && data.bot_requests
    ? ((data.bot_requests / data.total_requests) * 100).toFixed(1)
    : null

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-3">
        <StatCard
          title="Bot Requests"
          icon={Bot}
          value={data.bot_requests != null ? formatCompactCount(data.bot_requests) : '—'}
          sub={botPct ? `${botPct}% of all traffic` : ''}
        />
        <StatCard
          title="Verified Bots"
          icon={ShieldCheck}
          value={data.verified_bots != null ? formatCompactCount(data.verified_bots) : '—'}
          sub="known good bots (Googlebot, etc.)"
        />
        <StatCard
          title="Total Requests"
          icon={Users}
          value={data.total_requests != null ? formatCompactCount(data.total_requests) : '—'}
          sub="all traffic in range"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title="Bot vs Human Traffic" isEmpty={!donutData.length}>
          <PlotlyChart data={donutData} layout={BOT_DONUT_LAYOUT} a11yTitle="Bot versus human traffic breakdown" />
        </AnalyticsCard>
        <AnalyticsCard title="Top Bots" isEmpty={!topBotsData.length}>
          <PlotlyChart data={topBotsData} layout={TOP_BOTS_LAYOUT} a11yTitle="Top bot names by request count" />
        </AnalyticsCard>
      </div>
    </div>
  )
}

function PerformanceValueTab({ data }: { data?: PerformanceData | null }) {
  const latencyData = React.useMemo(() => {
    if (!data?.latency_time_series?.length) return []
    const grouped = groupCategoryTimeSeries(data.latency_time_series, 'hit')
    const colors: Record<string, string> = { hit: CHART_COLORS.success, miss: CHART_COLORS.danger }
    const names: Record<string, string> = { hit: 'Cache HIT', miss: 'Cache MISS' }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'scatter',
      mode: 'lines',
      name: names[cat] ?? cat,
      line: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No performance data available for this service.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Cache Acceleration"
          icon={Zap}
          value={data.cache_accel_factor != null ? `${data.cache_accel_factor}x` : '—'}
          sub="faster than origin"
          tooltip="Ratio of miss latency to hit latency — how much faster cached responses are"
        />
        <StatCard
          title="Avg Hit Latency"
          icon={Timer}
          value={formatMs(data.avg_hit_latency_ms)}
          sub="cached response time"
        />
        <StatCard
          title="Avg Miss Latency"
          icon={TrendingDown}
          value={formatMs(data.avg_miss_latency_ms)}
          sub="origin fetch time"
        />
        <StatCard
          title="P99 Latency"
          icon={Gauge}
          value={formatMs(data.p99_latency_ms)}
          sub="99th percentile"
        />
      </div>

      <div className="grid gap-4 grid-cols-1">
        <AnalyticsCard title="Hit vs Miss Latency" isEmpty={!latencyData.length}>
          <PlotlyChart data={latencyData} layout={LATENCY_LAYOUT} a11yTitle="Cache hit versus miss latency over time" />
        </AnalyticsCard>
      </div>
    </div>
  )
}

function NetworkValueTab({ data }: { data?: NetworkData | null }) {
  const protocolData = React.useMemo(() => {
    if (!data?.protocol_time_series?.length) return []
    const grouped = groupCategoryTimeSeries(data.protocol_time_series, 'Other')
    const colors: Record<string, string> = {
      'HTTP/3': CHART_COLORS.success,
      'HTTP/2': '#6366f1',
      'HTTP/1.1': CHART_COLORS.warning,
      'Other': '#6b7280',
    }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No network protocol data available for this service.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="HTTP/3"
          icon={Wifi}
          value={formatPctValue(data.http3_pct)}
          sub="of requests using QUIC"
        />
        <StatCard
          title="TLS"
          icon={Lock}
          value={formatPctValue(data.tls_pct)}
          sub="encrypted connections"
        />
        <StatCard
          title="IPv6"
          icon={Globe}
          value={formatPctValue(data.ipv6_pct)}
          sub="of requests over IPv6"
        />
        <StatCard
          title="HTTP/2"
          icon={Network}
          value={formatPctValue(data.h2_pct)}
          sub="multiplexed connections"
        />
      </div>

      <div className="grid gap-4 grid-cols-1">
        <AnalyticsCard title="Protocol Adoption Trend" isEmpty={!protocolData.length}>
          <PlotlyChart data={protocolData} layout={PROTOCOL_LAYOUT} a11yTitle="HTTP protocol version distribution over time" />
        </AnalyticsCard>
      </div>
    </div>
  )
}

interface FastlyValueBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: FiltersPayload
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

function FastlyValueBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: FastlyValueBodyProps) {
  const [activeTab, setActiveTab] = useState<TabId>('summary')

  const sections = React.useMemo(() => {
    if (activeTab === 'summary') return [...SUMMARY_SECTIONS]
    if (activeTab === 'assets') return ['overview'] // dummy section, query disabled
    return [activeTab]
  }, [activeTab]) as ("security" | "caching" | "bots" | "performance" | "network" | "io" | "overview")[]

  const valueQuery = useAnalyticsReportQuery<ValueData | undefined>({
    namespace: ['value', 'summary'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: ['1 day', activeTab],
    queryFn: async ({ rangeBody, signal }) => {
      return unwrapClient(client.POST('/api/value/summary', {
        signal,
        body: {
          filters: filterPayload,
          chart_interval: '1 day',
          sections,
          ...rangeBody,
        },
      }))
    },
    queryOptions: {
      enabled: activeTab !== 'assets',
    },
  })

  const data = valueQuery.data
  const hasActiveSectionData =
    activeTab === 'summary'
      ? Boolean(data?.overview || data?.caching || data?.network)
      : activeTab === 'caching'
        ? Boolean(data?.caching)
        : activeTab === 'security'
          ? Boolean(data?.security)
          : activeTab === 'bots'
            ? Boolean(data?.bots)
            : activeTab === 'performance'
              ? Boolean(data?.performance)
              : activeTab === 'network'
                ? Boolean(data?.network)
                : activeTab === 'io'
                  ? Boolean(data?.io)
                  : true
  const loading = activeTab !== 'assets' && (valueQuery.isLoading || (valueQuery.isFetching && !hasActiveSectionData))
  const refreshing = activeTab !== 'assets' && valueQuery.isFetching && hasActiveSectionData

  return (
    <div className="space-y-6">
      <Tabs
        value={activeTab}
        onValueChange={(v) => setActiveTab(v as TabId)}
      >
        <TabsList className="flex flex-wrap h-auto gap-1">
          {TABS.map((tab) => (
            <TabsTrigger key={tab.id} value={tab.id}>
              <tab.icon className="h-4 w-4" />
              {tab.label}
            </TabsTrigger>
          ))}
        </TabsList>

        {loading ? (
          <div className="space-y-4 pt-4">
            <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
            </div>
            <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
              <Skeleton className="h-72" />
              <Skeleton className="h-72" />
            </div>
          </div>
        ) : (
          <div className={refreshing ? 'opacity-40 pointer-events-none transition-opacity' : 'transition-opacity'}>
            <TabsContent value="summary">
              <SummaryValueTab data={data} loading={loading} />
            </TabsContent>
            <TabsContent value="caching">
              <CachingValueTab data={data?.caching} />
            </TabsContent>
            <TabsContent value="security">
              <SecurityValueTab data={data?.security} />
            </TabsContent>
            <TabsContent value="bots">
              <BotValueTab data={data?.bots} />
            </TabsContent>
            <TabsContent value="performance">
              <PerformanceValueTab data={data?.performance} />
            </TabsContent>
            <TabsContent value="network">
              <NetworkValueTab data={data?.network} />
            </TabsContent>
            <TabsContent value="assets">
              <AssetsReportContent
                startTime={startTime}
                endTime={endTime}
                activeServiceId={activeServiceId}
                filterPayload={filterPayload}
                relativeRange={relativeRange}
                isAutoRange={isAutoRange}
                anchor={anchor}
              />
            </TabsContent>
            <TabsContent value="io">
              <IOValueTab data={data?.io} loading={loading} />
            </TabsContent>
          </div>
        )}
      </Tabs>
    </div>
  )
}

export function FastlyValueClient() {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()

  return (
    <ReportLayout
      title="Service Summary"
      description="Executive summary of how Fastly delivers value across all products."
      icon={TrendingUp}
      defaultInterval="1 day"
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <FastlyValueBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}

export default FastlyValueClient


type IOData = components['schemas']['IOMetrics']

type IOOpportunity = components['schemas']['IOOpportunityRow']

type ConversionPair = NonNullable<IOData['io_format_conversion_pairs']>[number]

const OPPORTUNITY_COLUMNS: ColumnDef<IOOpportunity>[] = [
  {
    id: 'url',
    header: 'URL',
    accessorFn: (o) => o.url ?? '',
    size: 420,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="font-mono text-xs">
        <FilterValueCell
          filters={[{ column: 'url', value: row.original.url ?? '' }]}
          display={truncateUrl(row.original.url ?? '')}
        />
      </span>
    ),
  },
  {
    id: 'request_count',
    header: 'Requests',
    accessorFn: (o) => o.request_count ?? 0,
    size: 110,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="block text-right text-sm">{formatCompactCount(row.original.request_count ?? 0)}</span>
    ),
  },
  {
    id: 'avg_kb',
    header: 'Avg KB',
    accessorFn: (o) => o.avg_kb ?? -1,
    size: 100,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="block text-right text-sm">
        {row.original.avg_kb != null ? row.original.avg_kb.toFixed(1) : '—'}
      </span>
    ),
  },
  {
    id: 'total_bytes',
    header: 'Total Bandwidth',
    accessorFn: (o) => o.total_bytes ?? 0,
    size: 150,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="block text-right text-sm">{formatBytes(row.original.total_bytes ?? 0)}</span>
    ),
  },
]

const CONVERSION_COLUMNS: ColumnDef<ConversionPair>[] = [
  {
    id: 'input_format',
    header: 'From',
    accessorFn: (p) => p.input_format,
    size: 140,
    enableSorting: false,
    cell: ({ row }) => <span className="font-mono text-xs uppercase">{row.original.input_format}</span>,
  },
  {
    id: 'output_format',
    header: 'To',
    accessorFn: (p) => p.output_format,
    size: 140,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="flex items-center gap-1 font-mono text-xs uppercase">
        <ArrowRight className="h-3 w-3 text-muted-foreground" aria-hidden="true" />
        {row.original.output_format}
      </span>
    ),
  },
  {
    id: 'count',
    header: 'Count',
    accessorFn: (p) => p.count ?? 0,
    size: 110,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="block text-right text-sm">{formatCompactCount(row.original.count ?? 0)}</span>
    ),
  },
  {
    id: 'avg_ratio',
    header: 'Avg Ratio',
    accessorFn: (p) => p.avg_ratio ?? -1,
    size: 110,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="block text-right text-sm">
        {row.original.avg_ratio != null ? `${row.original.avg_ratio}x` : '—'}
      </span>
    ),
  },
]

const VOLUME_LAYOUT = {
  yaxis: { title: 'IO Transforms', ticksuffix: '' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const SAVINGS_LAYOUT = {
  yaxis: { title: 'Bandwidth Saved', ticksuffix: '' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const DONUT_LAYOUT = {
  margin: { l: 20, r: 20, t: 10, b: 10 },
  showlegend: true,
  legend: { orientation: 'h' as const, y: -0.15 },
}

const FORMAT_COLORS: Record<string, string> = {
  webp: '#8b5cf6',
  avif: '#06b6d4',
  jpeg: CHART_COLORS.warning,
  png: CHART_COLORS.success,
  gif: CHART_COLORS.danger,
  jpegxl: CHART_COLORS.info,
  svg: '#6366f1',
  mp4: '#ec4899',
}

function truncateUrl(url: string, max = 60): string {
  if (url.length <= max) return url
  return url.slice(0, max - 1) + '…'
}

function OpportunitiesTable({ opps }: { opps: IOOpportunity[] }) {
  if (!opps.length) return null
  return (
    <AnalyticsCard
      title="Top Image URLs"
      description="Highest-bandwidth image URLs that could benefit from IO optimization"
    >
      <DataTableReadonly
        columns={OPPORTUNITY_COLUMNS}
        data={opps}
        hideToolbar
        showPagination={false}
        tableCaption="Top image URLs"
      />
    </AnalyticsCard>
  )
}

function IOUpsellView({ data }: { data: IOData }) {
  const opps = data.optimization_opportunities ?? []

  return (
    <div className="space-y-6 pt-4">
      <div className="rounded-lg border border-purple-500/20 bg-purple-500/5 p-4">
        <p className="text-sm text-muted-foreground">
          <strong className="text-foreground">Image Optimizer is not enabled on this service.</strong>{' '}
          Fastly IO automatically converts images to modern formats (WebP, AVIF) and resizes on the fly,
          reducing bandwidth by up to 40%.
        </p>
      </div>

      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Image Requests"
          icon={ImageIcon}
          value={data.image_request_count != null ? formatCompactCount(data.image_request_count) : '—'}
          sub="detected via URL pattern"
        />
        <StatCard
          title="Image Bandwidth"
          icon={Activity}
          value={data.image_bandwidth_bytes != null ? formatBytes(data.image_bandwidth_bytes) : '—'}
          sub="serving unoptimized images"
        />
        <StatCard
          title="Potential Savings"
          icon={TrendingDown}
          value={data.estimated_savings_bytes != null ? formatBytes(data.estimated_savings_bytes) : '—'}
          sub="estimated at ~40% compression"
        />
        <StatCard
          title="Optimization Candidates"
          icon={Zap}
          value={opps.length > 0 ? String(opps.length) : '—'}
          sub="high-bandwidth image URLs"
        />
      </div>

      <OpportunitiesTable opps={opps} />
    </div>
  )
}

function padSinglePoint(points: { time: string; value: number }[]): { time: string; value: number }[] {
  if (points.length !== 1) return points
  const d = new Date(points[0].time)
  const prev = new Date(d); prev.setDate(prev.getDate() - 1)
  const next = new Date(d); next.setDate(next.getDate() + 1)
  const fmt = (dt: Date) => dt.toISOString().slice(0, 10)
  return [{ time: fmt(prev), value: 0 }, points[0], { time: fmt(next), value: 0 }]
}

function IOEnabledView({ data }: { data: IOData }) {
  const trendData = React.useMemo(() => {
    const hasTransforms = data.io_time_series?.some(p => p.value > 0)
    if (hasTransforms) {
      const pts = padSinglePoint(data.io_time_series!.map(p => ({ time: p.time, value: p.value })))
      return [{
        x: pts.map(p => p.time),
        y: pts.map(p => p.value),
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        name: 'IO Transforms',
        line: { color: '#8b5cf6' },
      }]
    }
    if (data.io_compression_time_series?.length) {
      const pts = padSinglePoint(data.io_compression_time_series.map(p => ({ time: p.time, value: p.value })))
      return [{
        x: pts.map(p => p.time),
        y: pts.map(p => p.value),
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        name: 'Bandwidth Saved (bytes)',
        line: { color: CHART_COLORS.success },
      }]
    }
    return []
  }, [data])

  const donutData = React.useMemo(() => {
    if (data.format_distribution?.length) {
      const dist = data.format_distribution
      return [{
        labels: dist.map(d => d.format.toUpperCase()),
        values: dist.map(d => d.count),
        type: 'pie',
        hole: 0.5,
        marker: { colors: dist.map(d => FORMAT_COLORS[d.format] ?? CHART_COLORS.neutral) },
        textinfo: 'label+percent',
        hovertemplate: '%{label}: %{value:,} transforms (%{percent})<extra></extra>',
      }]
    }
    const pairs = data.io_format_conversion_pairs
    if (pairs?.length) {
      const counts: Record<string, number> = {}
      for (const p of pairs) {
        const fmt = (p.output_format ?? 'other').toLowerCase()
        counts[fmt] = (counts[fmt] ?? 0) + (p.count ?? 0)
      }
      const labels = Object.keys(counts)
      return [{
        labels: labels.map(f => f.toUpperCase()),
        values: labels.map(f => counts[f]),
        type: 'pie',
        hole: 0.5,
        marker: { colors: labels.map(f => FORMAT_COLORS[f] ?? CHART_COLORS.neutral) },
        textinfo: 'label+percent',
        hovertemplate: '%{label}: %{value:,} conversions (%{percent})<extra></extra>',
      }]
    }
    return []
  }, [data])

  const savingsData = React.useMemo(() => {
    if (!data.io_compression_time_series?.length) return []
    const pts = padSinglePoint(data.io_compression_time_series.map(p => ({ time: p.time, value: p.value })))
    return [{
      x: pts.map(p => p.time),
      y: pts.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Bandwidth Saved',
      line: { color: CHART_COLORS.success },
    }]
  }, [data])

  const opps = data.optimization_opportunities ?? []

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="IO Transforms"
          icon={ImageIcon}
          value={data.io_transforms != null ? formatCompactCount(data.io_transforms) : '—'}
          sub={data.io_pct_of_traffic != null ? `${data.io_pct_of_traffic}% of traffic` : ''}
        />
        <StatCard
          title="IO Bandwidth"
          icon={Activity}
          value={data.io_bandwidth_bytes != null ? formatBytes(data.io_bandwidth_bytes) : '—'}
          sub="optimized image delivery"
        />
        <StatCard
          title="Estimated IO Cost"
          icon={DollarSign}
          value={data.io_estimated_cost_usd != null ? formatCurrency(data.io_estimated_cost_usd) : '—'}
          sub="at $0.0025/transform"
        />
        <StatCard
          title="Modern Format Adoption"
          icon={Sparkles}
          value={data.modern_format_pct != null ? `${data.modern_format_pct}%` : '—'}
          sub="WebP + AVIF + JPEGXL"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title={data.io_time_series?.some(p => p.value > 0) ? 'IO Volume Trend' : 'IO Savings Trend'} isEmpty={!trendData.length}>
          <PlotlyChart
            data={trendData}
            layout={data.io_time_series?.some(p => p.value > 0) ? VOLUME_LAYOUT : SAVINGS_LAYOUT}
            a11yTitle="Image Optimizer activity over time"
          />
        </AnalyticsCard>
        {donutData.length ? (
          <AnalyticsCard title="Format Distribution">
            <PlotlyChart data={donutData} layout={DONUT_LAYOUT} a11yTitle="Image format distribution" />
          </AnalyticsCard>
        ) : (
          <AnalyticsCard title="Format Distribution">
            <div className="flex items-center justify-center h-48 text-sm text-muted-foreground">
              Per-format breakdown not available from the Stats API
            </div>
          </AnalyticsCard>
        )}
      </div>

      {data.io_actual_bandwidth_saved_bytes != null && (
        <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
          <StatCard
            title="Actual Bandwidth Saved"
            icon={TrendingDown}
            value={formatBytes(data.io_actual_bandwidth_saved_bytes)}
            sub={data.io_actual_compression_ratio != null ? `${data.io_actual_compression_ratio}x compression ratio` : ''}
          />
        </div>
      )}

      {data.io_format_conversion_pairs?.length ? (
        <AnalyticsCard
          title="Format Conversions"
          description="Input → output format pairs by volume"
        >
          <DataTableReadonly
            columns={CONVERSION_COLUMNS}
            data={data.io_format_conversion_pairs ?? []}
            hideToolbar
            showPagination={false}
            tableCaption="Image format conversions"
          />
        </AnalyticsCard>
      ) : null}

      {savingsData.length > 0 && (
        <AnalyticsCard title="Compression Savings Trend" isEmpty={false}>
          <PlotlyChart data={savingsData} layout={SAVINGS_LAYOUT} a11yTitle="IO bandwidth savings over time" />
        </AnalyticsCard>
      )}

      <OpportunitiesTable opps={opps} />
    </div>
  )
}

export function IOValueTab({ data, loading }: { data?: IOData | null; loading?: boolean }) {
  if (loading) {
    return (
      <div className="space-y-6 pt-4">
        <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
          <SkeletonGrid count={4} height="6rem" />
        </div>
        <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
          <Skeleton className="h-64 rounded-xl" />
          <Skeleton className="h-64 rounded-xl" />
        </div>
      </div>
    )
  }

  if (!data) {
    return (
      <p className="text-muted-foreground py-8 text-center">
        No image traffic detected for this service.
      </p>
    )
  }

  if (data.io_transforms != null) {
    return <IOEnabledView data={data} />
  }

  return <IOUpsellView data={data} />
}
