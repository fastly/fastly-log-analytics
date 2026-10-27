'use client'

import React from 'react'
import { parseAsInteger, useQueryState } from 'nuqs'
import { AlertTriangle, BarChart3, Clock, Gauge, Play, TrendingUp, Wifi } from 'lucide-react'
import { client, unwrapClient } from '@/lib/api'
import {
  usePlotlyRangeZoom,
  useReportCancelQueries,
  useReportPageState,
  useServiceQuery,
} from '@/hooks/useReportPageState'
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload, PlotlyTrace } from '@/types/api'
import { CHART_COLORS, formatCountMaybe, formatDate, formatDuration, resolveRangeWire } from '@/lib/utils'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ReportLayout } from '@/components/ReportLayout'
import { StatCard } from '@/components/ui/confirm-dialog'
import {
  cmcdLabel,
  OBJECT_TYPE_LABELS,
  STREAMING_FORMAT_LABELS,
  STREAMING_INFO,
  STREAM_TYPE_LABELS,
} from './StreamingConstants'
import { StreamingChartsGrid } from './StreamingChartsGrid'

export {
  OBJECT_TYPE_LABELS,
  STREAMING_FORMAT_LABELS,
  STREAM_TYPE_LABELS,
  cmcdLabel,
  STREAMING_INFO,
} from './StreamingConstants'

const CMCD_CANCEL_KEYS: ReadonlyArray<readonly unknown[]> = [['cmcd']]

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
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })

  useReportCancelQueries(CMCD_CANCEL_KEYS)

  const handleChartRelayout = usePlotlyRangeZoom(timezone, setRange)
  const [bucketSeconds] = useQueryState('bucket_seconds', parseAsInteger.withDefault(300))
  const [topN] = useQueryState('top_n', parseAsInteger.withDefault(30))

  const cmcdQuery = useServiceQuery(
    ['cmcd', 'aggregates', activeServiceId, rangeKey, anchor, filterPayload, ...(bucketSeconds !== 300 || topN !== 30 ? [bucketSeconds, topN] : [])],
    ({ signal }) =>
      unwrapClient(
        client.POST('/api/cmcd/aggregates', {
          signal,
          body: { filters: filterPayload, bucket_seconds: bucketSeconds, top_n: topN, ...rangeBody },
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

  const bufferHealthData = React.useMemo(() => {
    const ts = data?.buffer_health_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
    return [
      { x, y: ts.map((d) => d.p50_buffer), name: 'p50 Buffer', type: 'scatter' as const, line: { color: '#6366f1' } },
      { x, y: ts.map((d) => d.p95_buffer), name: 'p95 Buffer', type: 'scatter' as const, line: { color: '#a78bfa', dash: 'dot' } },
      { x, y: ts.map((d) => d.starvation_rate), name: 'Starvation Rate', type: 'bar' as const, yaxis: 'y2', marker: { color: CHART_COLORS.danger, opacity: 0.4 } },
    ]
  }, [data?.buffer_health_ts, timezone])

  const bitrateData = React.useMemo(() => {
    const ts = data?.bitrate_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
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
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
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
      x: ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss')),
      y: ts.map((d) => d.startup_ratio),
      type: 'scatter' as const, fill: 'tozeroy' as const, line: { color: '#6366f1' },
    }]
  }, [data?.startup_ts, timezone])

  const sessionsData = React.useMemo(() => {
    const ts = data?.sessions_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
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
      x: ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss')),
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
      <StreamingChartsGrid
        data={data}
        queryStatus={queryStatus}
        tsProps={tsProps}
        sessionsData={sessionsData}
        sessionStartsData={sessionStartsData}
        bufferHealthData={bufferHealthData}
        bitrateData={bitrateData}
        throughputData={throughputData}
        startupData={startupData}
        durationDistData={durationDistData}
        objectTypeData={objectTypeData}
        streamingFormatRows={streamingFormatRows}
      />
    </>
  )
}

export default function StreamingClient({ nowServerStr }: { nowServerStr?: string }) {
  const { relativeRange, isAutoRange, anchor } = useReportPageState(nowServerStr)

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
