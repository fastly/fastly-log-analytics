'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { client, unwrapClient } from '@/lib/api'
import { useServiceQuery, useReportRangeAnchor, usePlotlyRangeZoom } from '@/hooks/useDashboardCards'
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload, PlotlyLayout, PlotlyTrace } from '@/types/api'
import { resolveRangeWire } from '@/lib/utils'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard, TimeSeriesChart, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { formatDuration, formatDate, formatCountMaybe } from '@/lib/format'
import { Play, Users, Wifi, BarChart3, Gauge, Globe, Film, AlertTriangle, TrendingUp, Clock, UserPlus } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { StatCard } from '@/components/ui/confirm-dialog'
import { DataTable } from '@/components/DataTable'
import { CHART_COLORS } from '@/lib/chart-helpers'

export const OBJECT_TYPE_LABELS: Record<string, string> = {
  m: 'Manifest', a: 'Audio', v: 'Video', av: 'Muxed A/V',
  i: 'Init Segment', c: 'Caption', tt: 'Timed Text', k: 'Crypto Key', o: 'Other',
}

export const STREAMING_FORMAT_LABELS: Record<string, string> = {
  d: 'DASH', h: 'HLS', s: 'Smooth', o: 'Other',
}

export const STREAM_TYPE_LABELS: Record<string, string> = { v: 'VOD', l: 'Live' }

export function cmcdLabel(map: Record<string, string>, code: string | null | undefined): string {
  if (!code) return 'Unknown'
  return map[code] ?? code
}

export const STREAMING_INFO = {
  active_sessions: {
    title: 'Total Sessions',
    body: <p className="text-sm text-muted-foreground">Count of distinct CMCD session IDs (<code>sid</code>) seen in the selected time range. Each video player session generates a unique ID that persists across all segment requests.</p>,
  },
  rebuffer_rate: {
    title: 'Rebuffer Rate',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Percentage of sessions that experienced at least one buffer starvation event. Reported by the player via the CMCD <code>bs</code> (Buffer Starvation) flag.</p>
        <p>A high rebuffer rate indicates viewers are experiencing playback interruptions — typically caused by insufficient throughput relative to the selected bitrate.</p>
      </div>
    ),
  },
  avg_bitrate: {
    title: 'Average Bitrate',
    body: <p className="text-sm text-muted-foreground">Mean encoded bitrate (<code>br</code>) across video segment requests, in kilobits per second. This reflects the quality level the player&apos;s ABR algorithm selected, not the delivery speed.</p>,
  },
  avg_buffer: {
    title: 'Average Buffer Length',
    body: <p className="text-sm text-muted-foreground">Mean buffer length (<code>bl</code>) in milliseconds at the time each segment was requested. Higher values indicate the player has more content buffered ahead — a healthy buffer is typically 10–30 seconds.</p>,
  },
  median_throughput: {
    title: 'Median Throughput',
    body: <p className="text-sm text-muted-foreground">Median measured throughput (<code>mtp</code>) in kbps, as reported by the player. This is the player&apos;s estimate of available bandwidth, used by ABR algorithms to select bitrate levels.</p>,
  },
  peak_viewers: {
    title: 'Peak Viewers',
    body: <p className="text-sm text-muted-foreground">Maximum number of concurrent sessions in any single time bucket during the selected range. Represents the peak audience size.</p>,
  },
  avg_session_duration: {
    title: 'Avg Session Duration',
    body: <p className="text-sm text-muted-foreground">Average time span between the first and last CDN request for each session (<code>sid</code>). Approximates viewing duration — actual watch time may be slightly longer since the last segment fetch occurs before playback ends.</p>,
  },
  active_viewers: {
    title: 'Active Viewers',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Number of distinct CMCD sessions (<code>sid</code>) active in each time bucket. A session is &ldquo;active&rdquo; if at least one request carrying its ID appears in the bucket.</p>
        <p>The <strong>Rebuffer Rate</strong> overlay shows what percentage of those sessions experienced buffer starvation (<code>bs</code>) — correlating audience size with quality degradation.</p>
      </div>
    ),
  },
  session_starts: {
    title: 'Session Starts',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Number of new sessions that started in each time bucket — counted as sessions whose first CDN request falls within the bucket. Reveals traffic arrival patterns: release timing, marketing spikes, and audience ramp-up.</p>
        <p>Compare with Active Viewers above: a spike in starts without a corresponding rise in active viewers suggests short viewing sessions or high bounce rates.</p>
      </div>
    ),
  },
  session_duration: {
    title: 'Session Duration Distribution',
    body: <p className="text-sm text-muted-foreground">Distribution of session durations, approximated as the time between first and last CDN request per session. Shows content engagement — are viewers watching for seconds or hours?</p>,
  },
  buffer_health: {
    title: 'Buffer Health',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Tracks buffer depth and starvation over time. The <strong>p50</strong> and <strong>p95</strong> lines show the median and 95th-percentile buffer length across all video segment requests in each time bucket.</p>
        <p>The <strong>Starvation Rate</strong> bars show what percentage of requests in each bucket had the buffer starvation flag (<code>bs</code>) set — indicating the player&apos;s buffer ran dry and playback stalled.</p>
      </div>
    ),
  },
  bitrate_quality: {
    title: 'Bitrate & Quality',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p><strong>Avg Bitrate</strong> shows the mean encoded bitrate (<code>br</code>) of video segments over time. Drops indicate the ABR algorithm is downshifting quality, typically due to congestion.</p>
        <p><strong>Utilization</strong> is the ratio of selected bitrate to the top available bitrate (<code>br/tb</code>). A value of 1.0 means the player is at the highest quality tier.</p>
      </div>
    ),
  },
  throughput: {
    title: 'Measured Throughput',
    body: <p className="text-sm text-muted-foreground">Throughput percentiles (p50, p95, p99) as measured by the player (<code>mtp</code>). This is the player&apos;s bandwidth estimate used for ABR decisions — a wide gap between p50 and p95 suggests variable network conditions across your audience.</p>,
  },
  startup: {
    title: 'Startup Requests',
    body: <p className="text-sm text-muted-foreground">Percentage of requests marked as startup (<code>su</code>) over time. Startup requests are the initial segments fetched when a viewer begins playback. A sustained high ratio may indicate frequent session restarts or short viewing sessions.</p>,
  },
  top_content: {
    title: 'Top Content',
    body: <p className="text-sm text-muted-foreground">Breakdown by Content ID (<code>cid</code>). Shows session count, rebuffer rate, average bitrate, and average buffer length per content item. Useful for identifying specific titles with quality issues.</p>,
  },
  object_type_dist: {
    title: 'Object Type Distribution',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Distribution of CMCD object types (<code>ot</code>) across all requests. Types: <strong>Video</strong> (v), <strong>Audio</strong> (a), <strong>Manifest</strong> (m), <strong> Init Segment</strong> (i), <strong>Caption</strong> (c), <strong>Crypto Key</strong> (k).</p>
        <p>Requests without a CMCD payload appear as &ldquo;Unknown&rdquo; — these are non-media requests (page loads, API calls) that pass through the same CDN service.</p>
      </div>
    ),
  },
  rebuffer_by_country: {
    title: 'Rebuffer Rate by Country',
    body: <p className="text-sm text-muted-foreground">Rebuffer rate and session count broken down by viewer country (from the CDN edge POP&apos;s GeoIP). High rebuffer rates in specific regions may indicate peering issues or insufficient edge capacity.</p>,
  },
  rebuffer_by_asn: {
    title: 'Rebuffer Rate by ASN',
    body: <p className="text-sm text-muted-foreground">Rebuffer rate broken down by the viewer&apos;s Autonomous System (ISP/network provider). Useful for identifying ISP-specific delivery issues — e.g., a single ISP with a disproportionately high rebuffer rate may indicate a peering or last-mile problem.</p>,
  },
}

const SESSIONS_LAYOUT: PlotlyLayout = {
  yaxis: { title: { text: 'Active Viewers' } },
  yaxis2: { title: { text: 'Rebuffer Rate (%)' }, overlaying: 'y', side: 'right' as const, rangemode: 'tozero' },
}
const SESSION_STARTS_LAYOUT: PlotlyLayout = { yaxis: { title: { text: 'New Sessions' } }, barcornerradius: 3 }
const BUFFER_LAYOUT: PlotlyLayout = {
  yaxis: { title: { text: 'Buffer Length (ms)' } },
  yaxis2: { title: { text: 'Starvation Rate (%)' }, overlaying: 'y', side: 'right' as const },
}
const BITRATE_LAYOUT: PlotlyLayout = {
  yaxis: { title: { text: 'Bitrate (kbps)' } },
  yaxis2: { title: { text: 'Utilization Ratio' }, overlaying: 'y', side: 'right' as const },
}
const THROUGHPUT_LAYOUT: PlotlyLayout = { yaxis: { title: { text: 'Throughput (kbps)' } } }
const STARTUP_LAYOUT: PlotlyLayout = { yaxis: { title: { text: 'Startup Ratio (%)' } } }
const DURATION_DIST_LAYOUT: PlotlyLayout = { yaxis: { title: { text: 'Sessions' } }, barcornerradius: 3 }
const EMPTY_PLOT_LAYOUT: PlotlyLayout = {}
const EMPTY_ROWS: never[] = []

const CONTENT_COLUMNS = [
  { accessorKey: 'content_id', header: 'Content ID' },
  { accessorKey: 'session_count', header: 'Sessions' },
  { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
  { accessorKey: 'avg_bitrate', header: 'Avg Bitrate (kbps)' },
  { accessorKey: 'avg_buffer_length', header: 'Avg Buffer (ms)' },
]
const REBUFFER_COUNTRY_COLUMNS = [
  { accessorKey: 'country', header: 'Country' },
  { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
  { accessorKey: 'session_count', header: 'Sessions' },
  { accessorKey: 'median_throughput', header: 'Median Throughput (kbps)' },
]
const REBUFFER_ASN_COLUMNS = [
  { accessorKey: 'label', header: 'ASN' },
  { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
  { accessorKey: 'session_count', header: 'Sessions' },
  { accessorKey: 'avg_bitrate', header: 'Avg Bitrate (kbps)' },
  { accessorKey: 'avg_buffer_length', header: 'Avg Buffer (ms)' },
]
const STREAMING_FORMAT_COLUMNS = [
  { accessorKey: 'streaming_format', header: 'Format' },
  { accessorKey: 'session_count', header: 'Sessions' },
]

interface StreamingBodyProps {
  activeServiceId: string | null
  filterPayload: FiltersPayload
  startTime: string | null
  endTime: string | null
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  timezone: string
}

function StreamingBody({
  activeServiceId, filterPayload, startTime, endTime, relativeRange, isAutoRange, anchor, timezone,
}: StreamingBodyProps) {
  const setRange = useFilterStore((s) => s.setRange)
  const queryClient = useQueryClient()
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })

  React.useEffect(() => () => { queryClient.cancelQueries({ queryKey: ['cmcd'] }) }, [queryClient])

  const handleChartRelayout = usePlotlyRangeZoom(timezone, setRange)

  const cmcdQuery = useServiceQuery(
    ['cmcd', 'aggregates', activeServiceId, rangeKey, anchor, filterPayload],
    ({ signal }) =>
      unwrapClient(
        client.POST('/api/cmcd/aggregates', {
          signal,
          body: { filters: filterPayload, bucket_seconds: 300, top_n: 30, ...rangeBody },
        }),
      ),
    { staleTime: 30_000, refetchInterval: 30_000 },
  )

  const data = cmcdQuery.data
  const overview = data?.overview
  const hasStreamingData = Boolean(data?.available && data?.has_data)
  const queryStatus = {
    isLoading: cmcdQuery.isLoading,
    isFetching: cmcdQuery.isFetching,
    error: cmcdQuery.error as AnalyticsCardError | null,
  }
  const tsProps = { startTime, endTime, timezone, height: '100%', onRelayout: handleChartRelayout }

  // ── Chart data ──────────────────────────────────────────────────────────

  const bufferHealthData = React.useMemo(() => {
    const ts = data?.buffer_health_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss"))
    return [
      { x, y: ts.map((d) => d.p50_buffer), name: 'p50 Buffer', type: 'scatter' as const, line: { color: '#6366f1' } },
      { x, y: ts.map((d) => d.p95_buffer), name: 'p95 Buffer', type: 'scatter' as const, line: { color: '#a78bfa', dash: 'dot' } },
      { x, y: ts.map((d) => d.starvation_rate), name: 'Starvation Rate', type: 'bar' as const, yaxis: 'y2', marker: { color: CHART_COLORS.danger, opacity: 0.4 } },
    ]
  }, [data?.buffer_health_ts, timezone])

  const bitrateData = React.useMemo(() => {
    const ts = data?.bitrate_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss"))
    const traces: PlotlyTrace[] = [
      { x, y: ts.map((d) => d.avg_bitrate), name: 'Avg Bitrate', type: 'scatter' as const, line: { color: CHART_COLORS.success } },
    ]
    if (ts.some((d) => d.utilization_ratio != null)) {
      traces.push({
        x, y: ts.map((d) => d.utilization_ratio), name: 'Utilization',
        type: 'scatter' as const, yaxis: 'y2', line: { color: CHART_COLORS.warning, dash: 'dot' },
      })
    }
    return traces
  }, [data?.bitrate_ts, timezone])

  const throughputData = React.useMemo(() => {
    const ts = data?.throughput_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss"))
    return [
      { x, y: ts.map((d) => d.p50), name: 'p50', type: 'scatter' as const, line: { color: '#6366f1' } },
      { x, y: ts.map((d) => d.p95), name: 'p95', type: 'scatter' as const, line: { color: '#a78bfa', dash: 'dot' } },
      { x, y: ts.map((d) => d.p99), name: 'p99', type: 'scatter' as const, line: { color: '#ec4899', dash: 'dash' } },
    ]
  }, [data?.throughput_ts, timezone])

  const startupData = React.useMemo(() => {
    const ts = data?.startup_ts
    if (!ts?.length) return []
    return [{
      x: ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss")),
      y: ts.map((d) => d.startup_ratio),
      type: 'scatter' as const, fill: 'tozeroy' as const, line: { color: '#6366f1' },
    }]
  }, [data?.startup_ts, timezone])

  const sessionsData = React.useMemo(() => {
    const ts = data?.sessions_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss"))
    const traces: PlotlyTrace[] = [{
      x, y: ts.map((d) => d.concurrent_sessions), name: 'Active Viewers',
      type: 'scatter' as const, fill: 'tozeroy' as const, line: { color: '#6366f1' },
    }]
    if (ts.some((d) => d.rebuffer_session_pct != null)) {
      traces.push({
        x, y: ts.map((d) => d.rebuffer_session_pct), name: 'Rebuffer Rate',
        type: 'scatter' as const, yaxis: 'y2', line: { color: CHART_COLORS.danger, dash: 'dot' },
      })
    }
    return traces
  }, [data?.sessions_ts, timezone])

  const sessionStartsData = React.useMemo(() => {
    const ts = data?.sessions_ts
    if (!ts?.length) return []
    return [{
      x: ts.map((d) => formatDate(d.bucket, timezone, "yyyy-MM-dd HH:mm:ss")),
      y: ts.map((d) => d.new_sessions),
      name: 'New Sessions', type: 'bar' as const, marker: { color: CHART_COLORS.success },
    }]
  }, [data?.sessions_ts, timezone])

  const durationDistData = React.useMemo(() => {
    const dist = data?.session_duration_dist
    if (!dist?.length) return []
    return [{ x: dist.map((d) => d.duration_bucket), y: dist.map((d) => d.session_count), type: 'bar' as const, marker: { color: '#6366f1' } }]
  }, [data?.session_duration_dist])

  const objectTypeData = React.useMemo(() => {
    const dist = data?.object_type_dist
    if (!dist?.length) return []
    return [{
      labels: dist.map((d) => cmcdLabel(OBJECT_TYPE_LABELS, d.object_type ?? '')),
      values: dist.map((d) => d.request_count),
      type: 'pie' as const, hole: 0.4,
    }]
  }, [data?.object_type_dist])

  const streamingFormatRows = React.useMemo(() => {
    const dist = data?.streaming_format_dist
    if (!dist?.length) return []
    return dist.map((d) => ({ ...d, streaming_format: cmcdLabel(STREAMING_FORMAT_LABELS, d.streaming_format ?? '') }))
  }, [data?.streaming_format_dist])

  if (data && !data.available) {
    return (
      <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
        <AlertTriangle className="h-12 w-12 text-muted-foreground" />
        <div>
          <h3 className="text-lg font-medium">CMCD data not available</h3>
          <p className="text-sm text-muted-foreground mt-1">
            CMCD collection is not enabled for this service. Enable it in Admin settings to start collecting streaming analytics.
          </p>
        </div>
      </div>
    )
  }

  const statItems = [
    { title: 'Total Sessions', value: hasStreamingData ? (formatCountMaybe(overview?.active_sessions) ?? '—') : '—', sub: 'in selected range', icon: Play, info: STREAMING_INFO.active_sessions },
    { title: 'Peak Viewers', value: hasStreamingData ? (formatCountMaybe(overview?.peak_viewers) ?? '—') : '—', sub: 'max concurrent', icon: TrendingUp, info: STREAMING_INFO.peak_viewers },
    { title: 'Avg Duration', value: hasStreamingData && overview?.avg_session_duration != null ? formatDuration(overview.avg_session_duration) : '—', sub: 'per session', icon: Clock, info: STREAMING_INFO.avg_session_duration },
    { title: 'Rebuffer Rate', value: hasStreamingData && overview?.rebuffer_rate != null ? `${overview.rebuffer_rate}%` : '—', sub: 'sessions with starvation', icon: AlertTriangle, info: STREAMING_INFO.rebuffer_rate },
    { title: 'Avg Bitrate', value: hasStreamingData && overview?.avg_bitrate != null ? `${Math.round(overview.avg_bitrate)} kbps` : '—', sub: 'video segments', icon: Gauge, info: STREAMING_INFO.avg_bitrate },
    { title: 'Avg Buffer', value: hasStreamingData && overview?.avg_buffer_length != null ? `${Math.round(overview.avg_buffer_length)} ms` : '—', sub: 'video segments', icon: BarChart3, info: STREAMING_INFO.avg_buffer },
    { title: 'Median Throughput', value: hasStreamingData && overview?.median_throughput != null ? `${Math.round(overview.median_throughput)} kbps` : '—', sub: 'measured throughput', icon: Wifi, info: STREAMING_INFO.median_throughput },
  ]

  const overviewGrid = (
    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4 mb-6">
      {statItems.map((item) => (
        <StatCard
          key={item.title}
          title={item.title}
          value={item.value}
          sub={item.sub}
          icon={item.icon}
          loading={cmcdQuery.isLoading}
          helpTitle={item.info.title}
          helpContent={item.info.body}
        />
      ))}
    </div>
  )

  if (data && !hasStreamingData) {
    return (
      <>
        {overviewGrid}
        <AnalyticsCard title="Streaming Telemetry" icon={<Play className="h-4 w-4" />} {...queryStatus} isEmpty className="h-[320px]">
          <div />
        </AnalyticsCard>
      </>
    )
  }

  return (
    <>
      {overviewGrid}

      <AnalyticsCard
        title="Active Viewers"
        icon={<Users className="h-4 w-4" />}
        {...queryStatus}
        isEmpty={!data?.sessions_ts?.length}
        className="h-[320px] mb-6"
        contentClassName="p-2"
        helpTitle={STREAMING_INFO.active_viewers.title}
        helpContent={STREAMING_INFO.active_viewers.body}
      >
        <TimeSeriesChart data={sessionsData} layout={SESSIONS_LAYOUT} {...tsProps} />
      </AnalyticsCard>

      <AnalyticsCard
        title="Session Starts"
        icon={<UserPlus className="h-4 w-4" />}
        {...queryStatus}
        isEmpty={!data?.sessions_ts?.length}
        className="h-[280px] mb-6"
        contentClassName="p-2"
        helpTitle={STREAMING_INFO.session_starts.title}
        helpContent={STREAMING_INFO.session_starts.body}
      >
        <TimeSeriesChart data={sessionStartsData} layout={SESSION_STARTS_LAYOUT} {...tsProps} />
      </AnalyticsCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Buffer Health"
          icon={<BarChart3 className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.buffer_health_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.buffer_health.title}
          helpContent={STREAMING_INFO.buffer_health.body}
        >
          <TimeSeriesChart data={bufferHealthData} layout={BUFFER_LAYOUT} {...tsProps} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Bitrate & Quality"
          icon={<Gauge className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.bitrate_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.bitrate_quality.title}
          helpContent={STREAMING_INFO.bitrate_quality.body}
        >
          <TimeSeriesChart data={bitrateData} layout={BITRATE_LAYOUT} {...tsProps} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Measured Throughput"
          icon={<Wifi className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.throughput_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.throughput.title}
          helpContent={STREAMING_INFO.throughput.body}
        >
          <TimeSeriesChart data={throughputData} layout={THROUGHPUT_LAYOUT} {...tsProps} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Startup Requests"
          icon={<Play className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.startup_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.startup.title}
          helpContent={STREAMING_INFO.startup.body}
        >
          <TimeSeriesChart data={startupData} layout={STARTUP_LAYOUT} {...tsProps} />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Top Content"
          icon={<Film className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.top_content?.length}
          helpTitle={STREAMING_INFO.top_content.title}
          helpContent={STREAMING_INFO.top_content.body}
        >
          <DataTable columns={CONTENT_COLUMNS} data={data?.top_content ?? EMPTY_ROWS} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Session Duration"
          icon={<Clock className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.session_duration_dist?.length}
          className="h-[320px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.session_duration.title}
          helpContent={STREAMING_INFO.session_duration.body}
        >
          <PlotlyChart data={durationDistData} layout={DURATION_DIST_LAYOUT} height="100%" />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Object Type Distribution"
          icon={<BarChart3 className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.object_type_dist?.length}
          className="h-[280px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.object_type_dist.title}
          helpContent={STREAMING_INFO.object_type_dist.body}
        >
          <PlotlyChart data={objectTypeData} layout={EMPTY_PLOT_LAYOUT} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="Streaming Formats"
          icon={<Play className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!streamingFormatRows.length}
        >
          <DataTable columns={STREAMING_FORMAT_COLUMNS} data={streamingFormatRows} />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Rebuffer Rate by Country"
          icon={<Globe className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.rebuffer_by_country?.length}
          helpTitle={STREAMING_INFO.rebuffer_by_country.title}
          helpContent={STREAMING_INFO.rebuffer_by_country.body}
        >
          <DataTable columns={REBUFFER_COUNTRY_COLUMNS} data={data?.rebuffer_by_country ?? EMPTY_ROWS} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Rebuffer Rate by ASN"
          icon={<Wifi className="h-4 w-4" />}
          {...queryStatus}
          isEmpty={!data?.rebuffer_by_asn?.length}
          helpTitle={STREAMING_INFO.rebuffer_by_asn.title}
          helpContent={STREAMING_INFO.rebuffer_by_asn.body}
        >
          <DataTable columns={REBUFFER_ASN_COLUMNS} data={data?.rebuffer_by_asn ?? EMPTY_ROWS} />
        </AnalyticsCard>
      </div>
    </>
  )
}

export default function StreamingClient({ nowServerStr }: { nowServerStr?: string }) {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Streaming"
      description="CMCD-powered video streaming quality analytics — buffer health, bitrate, throughput, and rebuffering."
      icon={Play}
    >
      {({ startTime, endTime, activeServiceId, filterPayload, timezone }) => (
        <StreamingBody
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          startTime={startTime}
          endTime={endTime}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
          timezone={timezone}
        />
      )}
    </ReportLayout>
  )
}
