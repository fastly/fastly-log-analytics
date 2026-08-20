'use client'

import React from 'react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useFilterStore } from '@/stores/filterStore'
import { quantizeAnchor } from '@/lib/time-window'
import type { FiltersPayload } from '@/types/filters'
import { resolveRangeWire } from '@/lib/range-wire'
import { PlotlyChart } from '@/components/PlotlyChart'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { parseFromInput } from '@/lib/date'
import { Play, Users, Wifi, BarChart3, Gauge, Globe, Film, AlertTriangle, TrendingUp, Clock, UserPlus } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/stat-card'
import { DataTable } from '@/components/DataTable'
import { STREAMING_INFO, OBJECT_TYPE_LABELS, STREAMING_FORMAT_LABELS, cmcdLabel } from './streamingInfo'

function _formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  const h = Math.floor(seconds / 3600)
  const m = Math.round((seconds % 3600) / 60)
  return m > 0 ? `${h}h ${m}m` : `${h}h`
}

const SESSIONS_LAYOUT = {
  yaxis: { title: { text: 'Active Viewers' } },
  yaxis2: { title: { text: 'Rebuffer Rate (%)' }, overlaying: 'y', side: 'right' as const, rangemode: 'tozero' },
}

const SESSION_STARTS_LAYOUT = {
  yaxis: { title: { text: 'New Sessions' } },
  barcornerradius: 3,
}

const BUFFER_LAYOUT = {
  yaxis: { title: { text: 'Buffer Length (ms)' } },
  yaxis2: { title: { text: 'Starvation Rate (%)' }, overlaying: 'y', side: 'right' as const },
}

const BITRATE_LAYOUT = {
  yaxis: { title: { text: 'Bitrate (kbps)' } },
  yaxis2: { title: { text: 'Utilization Ratio' }, overlaying: 'y', side: 'right' as const },
}

const THROUGHPUT_LAYOUT = {
  yaxis: { title: { text: 'Throughput (kbps)' } },
}

const STARTUP_LAYOUT = {
  yaxis: { title: { text: 'Startup Ratio (%)' } },
}

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
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  timezone,
}: StreamingBodyProps) {
  const setRange = useFilterStore((s) => s.setRange)
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })

  const handleChartRelayout = React.useCallback((event: Record<string, unknown>) => {
    if (event?.['xaxis.autorange'] === true || event?.['xaxis.showspikes'] !== undefined) return
    const x0 = (event?.['xaxis.range[0]'] ?? (event?.['xaxis.range'] as unknown[])?.[0]) as string | number | undefined
    const x1 = (event?.['xaxis.range[1]'] ?? (event?.['xaxis.range'] as unknown[])?.[1]) as string | number | undefined
    if (x0 === undefined || x1 === undefined) return
    try {
      const toLocalStr = (val: string | number) => {
        if (typeof val === 'number') {
          const d = new Date(val)
          const pad = (n: number) => n.toString().padStart(2, '0')
          return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
        }
        return val.replace(' ', 'T')
      }
      const parsedStart = parseFromInput(toLocalStr(x0), timezone)
      const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
      if (parsedStart && parsedEnd) setRange(parsedStart, parsedEnd)
    } catch { /* ignore unparseable zoom events */ }
  }, [setRange, timezone])

  const cmcdQuery = useServiceQuery(
    ['cmcd', 'aggregates', activeServiceId, rangeKey, anchor, filterPayload],
    async ({ signal }) => {
      const { data } = await client.POST('/api/cmcd/aggregates', {
        signal,
        body: {
          filters: filterPayload,
          bucket_seconds: 300,
          top_n: 30,
          ...rangeBody,
        },
      })
      return data
    },
    { refetchInterval: 30_000 },
  )

  const data = cmcdQuery.data
  const overview = data?.overview as {
    active_sessions?: number
    peak_viewers?: number
    avg_session_duration?: number
    rebuffer_rate?: number
    avg_bitrate?: number
    avg_buffer_length?: number
    median_throughput?: number
  } | undefined

  // ── Chart data ──────────────────────────────────────────────────────────

  const bufferHealthData = React.useMemo(() => {
    const ts = data?.buffer_health_ts
    if (!ts?.length) return []
    return [
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.p50_buffer),
        name: 'p50 Buffer',
        type: 'scatter' as const,
        line: { color: '#6366f1' },
      },
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.p95_buffer),
        name: 'p95 Buffer',
        type: 'scatter' as const,
        line: { color: '#a78bfa', dash: 'dot' },
      },
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.starvation_rate),
        name: 'Starvation Rate',
        type: 'bar' as const,
        yaxis: 'y2',
        marker: { color: '#ef4444', opacity: 0.4 },
      },
    ]
  }, [data?.buffer_health_ts])

  const bitrateData = React.useMemo(() => {
    const ts = data?.bitrate_ts
    if (!ts?.length) return []
    const traces: Record<string, unknown>[] = [
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.avg_bitrate),
        name: 'Avg Bitrate',
        type: 'scatter' as const,
        line: { color: '#10b981' },
      },
    ]
    if (ts.some((d) => d.utilization_ratio != null)) {
      traces.push({
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.utilization_ratio),
        name: 'Utilization',
        type: 'scatter' as const,
        yaxis: 'y2',
        line: { color: '#f59e0b', dash: 'dot' },
      })
    }
    return traces
  }, [data?.bitrate_ts])

  const throughputData = React.useMemo(() => {
    const ts = data?.throughput_ts
    if (!ts?.length) return []
    return [
      { x: ts.map((d) => d.bucket), y: ts.map((d) => d.p50), name: 'p50', type: 'scatter' as const, line: { color: '#6366f1' } },
      { x: ts.map((d) => d.bucket), y: ts.map((d) => d.p95), name: 'p95', type: 'scatter' as const, line: { color: '#a78bfa', dash: 'dot' } },
      { x: ts.map((d) => d.bucket), y: ts.map((d) => d.p99), name: 'p99', type: 'scatter' as const, line: { color: '#ec4899', dash: 'dash' } },
    ]
  }, [data?.throughput_ts])

  const startupData = React.useMemo(() => {
    const ts = data?.startup_ts
    if (!ts?.length) return []
    return [
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.startup_ratio),
        type: 'scatter' as const,
        fill: 'tozeroy' as const,
        line: { color: '#6366f1' },
      },
    ]
  }, [data?.startup_ts])

  const sessionsData = React.useMemo(() => {
    const ts = data?.sessions_ts
    if (!ts?.length) return []
    const traces: Record<string, unknown>[] = [
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.concurrent_sessions),
        name: 'Active Viewers',
        type: 'scatter' as const,
        fill: 'tozeroy' as const,
        line: { color: '#6366f1' },
      },
    ]
    if (ts.some((d) => d.rebuffer_session_pct != null)) {
      traces.push({
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.rebuffer_session_pct),
        name: 'Rebuffer Rate',
        type: 'scatter' as const,
        yaxis: 'y2',
        line: { color: '#ef4444', dash: 'dot' },
      })
    }
    return traces
  }, [data?.sessions_ts])

  const sessionStartsData = React.useMemo(() => {
    const ts = data?.sessions_ts
    if (!ts?.length) return []
    return [
      {
        x: ts.map((d) => d.bucket),
        y: ts.map((d) => d.new_sessions),
        name: 'New Sessions',
        type: 'bar' as const,
        marker: { color: '#10b981' },
      },
    ]
  }, [data?.sessions_ts])

  const durationDistData = React.useMemo(() => {
    const dist = data?.session_duration_dist
    if (!dist?.length) return []
    return [
      {
        x: dist.map((d) => d.duration_bucket),
        y: dist.map((d) => d.session_count),
        type: 'bar' as const,
        marker: { color: '#6366f1' },
      },
    ]
  }, [data?.session_duration_dist])

  const objectTypeData = React.useMemo(() => {
    const dist = data?.object_type_dist
    if (!dist?.length) return []
    return [
      {
        labels: dist.map((d) => cmcdLabel(OBJECT_TYPE_LABELS, d.object_type as string)),
        values: dist.map((d) => d.request_count),
        type: 'pie' as const,
        hole: 0.4,
      },
    ]
  }, [data?.object_type_dist])

  const streamingFormatRows = React.useMemo(() => {
    const dist = data?.streaming_format_dist
    if (!dist?.length) return []
    return dist.map((d) => ({
      ...d,
      streaming_format: cmcdLabel(STREAMING_FORMAT_LABELS, d.streaming_format as string),
    }))
  }, [data?.streaming_format_dist])

  // ── Not available state ─────────────────────────────────────────────────

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

  // ── Content ─────────────────────────────────────────────────────────────

  const contentColumns = [
    { accessorKey: 'content_id', header: 'Content ID' },
    { accessorKey: 'session_count', header: 'Sessions' },
    { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
    { accessorKey: 'avg_bitrate', header: 'Avg Bitrate (kbps)' },
    { accessorKey: 'avg_buffer_length', header: 'Avg Buffer (ms)' },
  ]

  const rebufferCountryColumns = [
    { accessorKey: 'country', header: 'Country' },
    { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
    { accessorKey: 'session_count', header: 'Sessions' },
    { accessorKey: 'median_throughput', header: 'Median Throughput (kbps)' },
  ]

  const rebufferAsnColumns = [
    { accessorKey: 'label', header: 'ASN' },
    { accessorKey: 'rebuffer_rate', header: 'Rebuffer Rate %' },
    { accessorKey: 'session_count', header: 'Sessions' },
    { accessorKey: 'avg_bitrate', header: 'Avg Bitrate (kbps)' },
    { accessorKey: 'avg_buffer_length', header: 'Avg Buffer (ms)' },
  ]

  const streamingFormatColumns = [
    { accessorKey: 'streaming_format', header: 'Format' },
    { accessorKey: 'session_count', header: 'Sessions' },
  ]

  return (
    <>
      {/* Overview Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4 mb-6">
        <StatCard
          title="Total Sessions"
          value={overview?.active_sessions?.toLocaleString() ?? '—'}
          sub="in selected range"
          icon={Play}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.active_sessions.title}
          helpContent={STREAMING_INFO.active_sessions.body}
        />
        <StatCard
          title="Peak Viewers"
          value={overview?.peak_viewers?.toLocaleString() ?? '—'}
          sub="max concurrent"
          icon={TrendingUp}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.peak_viewers.title}
          helpContent={STREAMING_INFO.peak_viewers.body}
        />
        <StatCard
          title="Avg Duration"
          value={overview?.avg_session_duration != null ? _formatDuration(overview.avg_session_duration) : '—'}
          sub="per session"
          icon={Clock}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.avg_session_duration.title}
          helpContent={STREAMING_INFO.avg_session_duration.body}
        />
        <StatCard
          title="Rebuffer Rate"
          value={overview?.rebuffer_rate != null ? `${overview.rebuffer_rate}%` : '—'}
          sub="sessions with starvation"
          icon={AlertTriangle}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.rebuffer_rate.title}
          helpContent={STREAMING_INFO.rebuffer_rate.body}
        />
        <StatCard
          title="Avg Bitrate"
          value={overview?.avg_bitrate != null ? `${Math.round(overview.avg_bitrate)} kbps` : '—'}
          sub="video segments"
          icon={Gauge}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.avg_bitrate.title}
          helpContent={STREAMING_INFO.avg_bitrate.body}
        />
        <StatCard
          title="Avg Buffer"
          value={overview?.avg_buffer_length != null ? `${Math.round(overview.avg_buffer_length)} ms` : '—'}
          sub="video segments"
          icon={BarChart3}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.avg_buffer.title}
          helpContent={STREAMING_INFO.avg_buffer.body}
        />
        <StatCard
          title="Median Throughput"
          value={overview?.median_throughput != null ? `${Math.round(overview.median_throughput)} kbps` : '—'}
          sub="measured throughput"
          icon={Wifi}
          loading={cmcdQuery.isLoading}
          helpTitle={STREAMING_INFO.median_throughput.title}
          helpContent={STREAMING_INFO.median_throughput.body}
        />
      </div>

      {/* Active Viewers */}
      <AnalyticsCard
        title="Active Viewers"
        icon={<Users className="h-4 w-4" />}
        isLoading={cmcdQuery.isLoading}
        isFetching={cmcdQuery.isFetching}
        error={cmcdQuery.error as AnalyticsCardError | null}
        isEmpty={!data?.sessions_ts?.length}
        className="h-[320px] mb-6"
        contentClassName="p-2"
        helpTitle={STREAMING_INFO.active_viewers.title}
        helpContent={STREAMING_INFO.active_viewers.body}
      >
        <TimeSeriesChart
          data={sessionsData}
          layout={SESSIONS_LAYOUT}
          startTime={startTime}
          endTime={endTime}
          timezone={timezone}
          height="100%"
          onRelayout={handleChartRelayout}
        />
      </AnalyticsCard>

      {/* Session Starts */}
      <AnalyticsCard
        title="Session Starts"
        icon={<UserPlus className="h-4 w-4" />}
        isLoading={cmcdQuery.isLoading}
        isFetching={cmcdQuery.isFetching}
        error={cmcdQuery.error as AnalyticsCardError | null}
        isEmpty={!data?.sessions_ts?.length}
        className="h-[280px] mb-6"
        contentClassName="p-2"
        helpTitle={STREAMING_INFO.session_starts.title}
        helpContent={STREAMING_INFO.session_starts.body}
      >
        <TimeSeriesChart
          data={sessionStartsData}
          layout={SESSION_STARTS_LAYOUT}
          startTime={startTime}
          endTime={endTime}
          timezone={timezone}
          height="100%"
          onRelayout={handleChartRelayout}
        />
      </AnalyticsCard>

      {/* Time Series Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Buffer Health"
          icon={<BarChart3 className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.buffer_health_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.buffer_health.title}
          helpContent={STREAMING_INFO.buffer_health.body}
        >
          <TimeSeriesChart
            data={bufferHealthData}
            layout={BUFFER_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
            onRelayout={handleChartRelayout}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Bitrate & Quality"
          icon={<Gauge className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.bitrate_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.bitrate_quality.title}
          helpContent={STREAMING_INFO.bitrate_quality.body}
        >
          <TimeSeriesChart
            data={bitrateData}
            layout={BITRATE_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
            onRelayout={handleChartRelayout}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Measured Throughput"
          icon={<Wifi className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.throughput_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.throughput.title}
          helpContent={STREAMING_INFO.throughput.body}
        >
          <TimeSeriesChart
            data={throughputData}
            layout={THROUGHPUT_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
            onRelayout={handleChartRelayout}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Startup Requests"
          icon={<Play className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.startup_ts?.length}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.startup.title}
          helpContent={STREAMING_INFO.startup.body}
        >
          <TimeSeriesChart
            data={startupData}
            layout={STARTUP_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
            onRelayout={handleChartRelayout}
          />
        </AnalyticsCard>
      </div>

      {/* Content & Distribution */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Top Content"
          icon={<Film className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.top_content?.length}
          helpTitle={STREAMING_INFO.top_content.title}
          helpContent={STREAMING_INFO.top_content.body}
        >
          <DataTable columns={contentColumns} data={data?.top_content ?? []} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Session Duration"
          icon={<Clock className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.session_duration_dist?.length}
          className="h-[320px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.session_duration.title}
          helpContent={STREAMING_INFO.session_duration.body}
        >
          <PlotlyChart data={durationDistData} layout={{ yaxis: { title: { text: 'Sessions' } }, barcornerradius: 3 }} height="100%" />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Object Type Distribution"
          icon={<BarChart3 className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.object_type_dist?.length}
          className="h-[280px]"
          contentClassName="p-2"
          helpTitle={STREAMING_INFO.object_type_dist.title}
          helpContent={STREAMING_INFO.object_type_dist.body}
        >
          <PlotlyChart data={objectTypeData} layout={{}} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="Streaming Formats"
          icon={<Play className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!streamingFormatRows.length}
        >
          <DataTable columns={streamingFormatColumns} data={streamingFormatRows} />
        </AnalyticsCard>
      </div>

      {/* Geographic Breakdown */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Rebuffer Rate by Country"
          icon={<Globe className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.rebuffer_by_country?.length}
          helpTitle={STREAMING_INFO.rebuffer_by_country.title}
          helpContent={STREAMING_INFO.rebuffer_by_country.body}
        >
          <DataTable columns={rebufferCountryColumns} data={data?.rebuffer_by_country ?? []} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Rebuffer Rate by ASN"
          icon={<Wifi className="h-4 w-4" />}
          isLoading={cmcdQuery.isLoading}
          isFetching={cmcdQuery.isFetching}
          error={cmcdQuery.error as AnalyticsCardError | null}
          isEmpty={!data?.rebuffer_by_asn?.length}
          helpTitle={STREAMING_INFO.rebuffer_by_asn.title}
          helpContent={STREAMING_INFO.rebuffer_by_asn.body}
        >
          <DataTable columns={rebufferAsnColumns} data={data?.rebuffer_by_asn ?? []} />
        </AnalyticsCard>
      </div>
    </>
  )
}

export default function StreamingClient() {
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = React.useMemo(() => quantizeAnchor(storeEndTime), [storeEndTime])

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
