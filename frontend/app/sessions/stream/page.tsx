'use client'

import React, { Suspense, useMemo } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { parseAsString, useQueryState } from 'nuqs'
import { useQuery } from '@tanstack/react-query'
import {
  Film, Clock, Gauge, BarChart3, AlertTriangle, Wifi,
  ArrowUp, ArrowDown, ArrowLeft, Activity, Layers,
} from 'lucide-react'

import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useBootstrap'
import { NoServiceSelected } from '@/components/ReportLayout'
import { PageHeader, StatCard } from '@/components/ui/confirm-dialog'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable, PopLabel } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button, buttonVariants } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { OBJECT_TYPE_LABELS } from '@/app/streaming/_sections/StreamingClient'
import { formatDuration, CHART_COLORS } from '@/lib/utils'
import {
  useStreamAggregates,
  type BitrateShifts,
  type BitrateTier,
  type BufferBucket,
  type NetworkPoint,
  type ObjectTypeDist,
  type StarvationEvent,
  type StreamAggregates,
  type StreamRow as Row,
  type StreamSummary,
  type TimelinePoint,
} from './_components/streamAggregates'

const TIMELINE_LAYOUT = {
  yaxis: { title: { text: 'kbps' } },
  yaxis2: { title: { text: 'Buffer (ms)' }, overlaying: 'y', side: 'right' as const, rangemode: 'tozero' },
  legend: { orientation: 'h' as const, y: -0.15 },
  margin: { r: 60, t: 40 },
}

const NETWORK_LAYOUT = {
  yaxis: { title: { text: 'Throughput (kbps)' } },
  yaxis2: { title: { text: 'RTT (ms)' }, overlaying: 'y', side: 'right' as const, rangemode: 'tozero' },
}
const SEGMENT_BAR_LAYOUT = { yaxis: { title: { text: 'Segments' } }, barcornerradius: 3 }
const OT_LAYOUT = {}

function StreamTimeline({ timeline, isLoading }: { timeline: TimelinePoint[]; isLoading?: boolean }) {
  const { traces, shapes, annotations } = React.useMemo(() => {
    if (!timeline.length) return { traces: [], shapes: [], annotations: [] }
    const ts = timeline.map((p) => p.timestamp)
    const traces: Record<string, unknown>[] = [
      { x: ts, y: timeline.map((p) => p.bitrate), name: 'Bitrate', type: 'scatter' as const, line: { color: CHART_COLORS.success, shape: 'hv' } },
      { x: ts, y: timeline.map((p) => p.throughput), name: 'Throughput', type: 'scatter' as const, line: { color: CHART_COLORS.warning, dash: 'dot' } },
      {
        x: ts, y: timeline.map((p) => p.buffer), name: 'Buffer', type: 'scatter' as const, yaxis: 'y2',
        fill: 'tozeroy' as const, line: { color: '#6366f1' }, fillcolor: 'rgba(99, 102, 241, 0.1)',
      },
    ]
    const shapes = timeline.filter((p) => p.isRebuffer).map((p) => ({
      type: 'line' as const, x0: p.timestamp, x1: p.timestamp, y0: 0, y1: 1, yref: 'paper' as const,
      line: { color: CHART_COLORS.danger, width: 1.5, dash: 'dash' as const },
    }))
    const annotations = timeline.filter((p) => p.isStartup).map((p) => ({
      x: p.timestamp, y: 1.05, yref: 'paper' as const, yanchor: 'bottom' as const,
      text: 'SU', showarrow: false, font: { size: 9, color: '#6366f1' }, bgcolor: 'rgba(99, 102, 241, 0.1)', borderpad: 2,
    }))
    return { traces, shapes, annotations }
  }, [timeline])

  const layout = React.useMemo(() => ({ ...TIMELINE_LAYOUT, shapes, annotations }), [shapes, annotations])

  return (
    <AnalyticsCard
      title="Session Timeline" icon={<Activity className="h-4 w-4" />} isLoading={isLoading} isEmpty={!timeline.length}
      className="h-[400px] mb-6" contentClassName="p-2"
      description="Bitrate and throughput (left axis) with buffer depth (right axis). Red dashed lines mark rebuffer events."
    >
      <PlotlyChart data={traces} layout={layout} height="100%" />
    </AnalyticsCard>
  )
}

function SummaryCards({ aggregates }: { aggregates: StreamAggregates }) {
  const { summary: s } = aggregates
  const cards = [
    { title: 'Duration', value: s.durationSeconds != null ? formatDuration(s.durationSeconds) : '—', sub: `${s.totalRequests} requests`, icon: Clock },
    { title: 'Avg Bitrate', value: s.avgBitrate != null ? `${Math.round(s.avgBitrate)} kbps` : '—', sub: `${s.videoRequests} video segments`, icon: Gauge },
    { title: 'Top Bitrate', value: s.topBitrate != null ? `${Math.round(s.topBitrate)} kbps` : '—', sub: 'highest available tier', icon: ArrowUp },
    { title: 'Utilization', value: s.utilization != null ? `${Math.round(s.utilization * 100)}%` : '—', sub: 'avg / top bitrate', icon: Activity },
    { title: 'Rebuffers', value: s.rebufferCount, sub: 'buffer starvation events', icon: AlertTriangle, iconClassName: s.rebufferCount > 0 ? 'text-destructive' : undefined },
    { title: 'Startup Time', value: s.startupTimeMs != null ? `${(s.startupTimeMs / 1000).toFixed(1)}s` : '—', sub: 'time to first video', icon: Film },
    { title: 'Buffer Health', value: s.bufferHealthPct != null ? `${Math.round(s.bufferHealthPct)}%` : '—', sub: 'segments with buffer > 0', icon: BarChart3 },
  ]
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4">
      {cards.map((c) => <StatCard key={c.title} {...c} />)}
    </div>
  )
}

function BitrateAnalysis({ aggregates, isLoading }: { aggregates: StreamAggregates; isLoading?: boolean }) {
  const chartData = React.useMemo(() => (
    aggregates.bitrateTiers.length ? [{
      x: aggregates.bitrateTiers.map((t) => `${t.bitrate} kbps`),
      y: aggregates.bitrateTiers.map((t) => t.count),
      type: 'bar' as const,
      marker: { color: CHART_COLORS.success },
      text: aggregates.bitrateTiers.map((t) => `${t.pct.toFixed(0)}%`),
      textposition: 'auto' as const,
    }] : []
  ), [aggregates.bitrateTiers])

  return (
    <AnalyticsCard
      title="Bitrate Distribution" icon={<Gauge className="h-4 w-4" />} isLoading={isLoading}
      isEmpty={!aggregates.bitrateTiers.length} className="h-[360px]" contentClassName="p-2"
      headerAction={
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          <span className="flex items-center gap-1"><ArrowUp className="h-3 w-3 text-green-500" />{aggregates.bitrateShifts.upshifts} up</span>
          <span className="flex items-center gap-1"><ArrowDown className="h-3 w-3 text-red-500" />{aggregates.bitrateShifts.downshifts} down</span>
        </div>
      }
    >
      <PlotlyChart data={chartData} layout={SEGMENT_BAR_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

function BufferAnalysis({ aggregates, isLoading }: { aggregates: StreamAggregates; isLoading?: boolean }) {
  const chartData = React.useMemo(() => (
    aggregates.bufferBuckets.length ? [{
      x: aggregates.bufferBuckets.map((b) => b.label),
      y: aggregates.bufferBuckets.map((b) => b.count),
      type: 'bar' as const,
      marker: {
        color: aggregates.bufferBuckets.map((b) =>
          b.label === '0 (empty)' ? CHART_COLORS.danger : b.label === '< 2s' ? CHART_COLORS.warning : '#6366f1',
        ),
      },
      text: aggregates.bufferBuckets.map((b) => `${b.pct.toFixed(0)}%`),
      textposition: 'auto' as const,
    }] : []
  ), [aggregates.bufferBuckets])

  return (
    <AnalyticsCard
      title="Buffer Depth Distribution" icon={<BarChart3 className="h-4 w-4" />} isLoading={isLoading}
      isEmpty={!aggregates.bufferBuckets.length} className="h-[360px]" contentClassName="p-2"
    >
      <PlotlyChart data={chartData} layout={SEGMENT_BAR_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

function NetworkAnalysis({ aggregates, isLoading }: { aggregates: StreamAggregates; isLoading?: boolean }) {
  const chartData = React.useMemo(() => {
    const nt = aggregates.networkTimeline
    if (!nt.length) return []
    const traces: Record<string, unknown>[] = []
    if (nt.some((p) => p.throughput != null)) {
      traces.push({ x: nt.map((p) => p.timestamp), y: nt.map((p) => p.throughput), name: 'Throughput', type: 'scatter' as const, line: { color: CHART_COLORS.warning } })
    }
    if (nt.some((p) => p.rtt != null)) {
      traces.push({ x: nt.map((p) => p.timestamp), y: nt.map((p) => p.rtt), name: 'RTT', type: 'scatter' as const, yaxis: 'y2', line: { color: CHART_COLORS.danger, dash: 'dot' } })
    }
    return traces
  }, [aggregates.networkTimeline])

  return (
    <AnalyticsCard
      title="Network Conditions" icon={<Wifi className="h-4 w-4" />} isLoading={isLoading}
      isEmpty={!aggregates.networkTimeline.length} className="h-[360px]" contentClassName="p-2"
      headerAction={
        aggregates.pops.length > 0 ? (
          <div className="flex items-center gap-1 text-xs text-muted-foreground">
            <span>POPs:</span>
            {aggregates.pops.map((pop) => <Badge key={pop} variant="secondary" className="text-[10px] px-1.5 py-0"><PopLabel code={pop} /></Badge>)}
          </div>
        ) : undefined
      }
    >
      <PlotlyChart data={chartData} layout={NETWORK_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

const CONTENT_COLUMNS = [
  { accessorKey: 'id', header: 'Content ID' },
  { accessorKey: 'count', header: 'Segments' },
]

function ContentBreakdown({ aggregates, isLoading }: { aggregates: StreamAggregates; isLoading?: boolean }) {
  const pieData = React.useMemo(() => (
    aggregates.objectTypeDist.length ? [{
      labels: aggregates.objectTypeDist.map((d) => d.label),
      values: aggregates.objectTypeDist.map((d) => d.count),
      type: 'pie' as const,
      hole: 0.4,
    }] : []
  ), [aggregates.objectTypeDist])

  return (
    <AnalyticsCard
      title="Content Breakdown" icon={<Layers className="h-4 w-4" />} isLoading={isLoading}
      isEmpty={!aggregates.objectTypeDist.length && !aggregates.contentIds.length}
      className="min-h-[360px]" contentClassName="p-2"
    >
      <div className="flex flex-col gap-4 h-full">
        {aggregates.objectTypeDist.length > 0 && (
          <div className="h-[200px]"><PlotlyChart data={pieData} layout={OT_LAYOUT} height="100%" /></div>
        )}
        {aggregates.contentIds.length > 0 && <DataTable columns={CONTENT_COLUMNS} data={aggregates.contentIds} />}
      </div>
    </AnalyticsCard>
  )
}

const STARVATION_COLUMNS = [
  { accessorKey: 'timestamp', header: 'Time' },
  { accessorKey: 'bitrate', header: 'Bitrate (kbps)' },
  { accessorKey: 'buffer', header: 'Buffer (ms)' },
  { accessorKey: 'throughput', header: 'Throughput (kbps)' },
  {
    accessorKey: 'url',
    header: 'URL',
    cell: ({ row }: { row: { original: { url: string | null } } }) => (
      <span className="text-xs truncate max-w-[300px] inline-block font-mono">{row.original.url ?? '—'}</span>
    ),
  },
]

function StarvationTable({ events }: { events: StarvationEvent[] }) {
  return (
    <AnalyticsCard
      title="Buffer Starvation Events" icon={<AlertTriangle className="h-4 w-4" />} isEmpty={!events.length}
      description="Requests where the player reported buffer starvation (cmcd_bs=true)."
    >
      <DataTable columns={STARVATION_COLUMNS} data={events} />
    </AnalyticsCard>
  )
}

function StreamDetailClient() {
  const searchParams = useSearchParams()
  let nuqsToken: string | null = null
  try {
    ;[nuqsToken] = useQueryState('token', parseAsString)
  } catch {
    // Fallback when NuqsAdapter is not mounted in unit tests
  }
  const token = nuqsToken ?? searchParams.get('token')
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()

  const { data: detailData, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['stream-detail', activeServiceId, token],
    queryFn: async ({ signal }) => unwrapClient(client.POST('/api/sessions/detail', { signal, body: { session_token: token! } })),
    enabled: !!activeServiceId && !!token,
    retry: false,
  })

  const aggregates = useStreamAggregates(detailData?.data)

  if (!activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Film} message="Select a service to view stream details." />
  }

  if (!token) {
    return (
      <div className="space-y-6">
        <PageHeader title="Stream Details" icon={Film} />
        <Alert role="status" data-testid="stream-detail-parameter-required">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>No session token</AlertTitle>
          <AlertDescription>
            Navigate here from a streaming session in the{' '}
            <Link href="/sessions" className="underline hover:text-primary">Sessions</Link> page.
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  const showLoading = isLoading || (!activeServiceId && !bootstrapResolved)
  const isPartial = detailData?.data?.length === 500

  return (
    <div data-testid="stream-detail-client" className="space-y-6">
      <PageHeader
        title="Stream Session Details"
        description="CMCD streaming quality metrics and request-level timeline for a single session."
        icon={Film}
      >
        <Link href="/sessions" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" />Sessions
        </Link>
      </PageHeader>

      {showLoading ? (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4 min-h-[120px] [contain-intrinsic-size:120px]">
            {['duration', 'bitrate', 'top-br', 'util', 'rebuf', 'startup', 'health'].map((k) => (
              <Skeleton key={k} className="h-[120px] rounded-lg" />
            ))}
          </div>
          <Skeleton className="h-[260px] w-full rounded-lg" />
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <Skeleton className="h-[360px] rounded-lg" />
            <Skeleton className="h-[360px] rounded-lg" />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <Skeleton className="h-[360px] rounded-lg" />
            <Skeleton className="h-[360px] rounded-lg" />
          </div>
        </>
      ) : aggregates ? (
        <SummaryCards aggregates={aggregates} />
      ) : !isError && detailData ? (
        <Alert role="status">
          <Film className="h-4 w-4" />
          <AlertTitle>No requests found</AlertTitle>
          <AlertDescription>No streaming records are available for this session in your accessible time window.</AlertDescription>
        </Alert>
      ) : null}

      {isError && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Failed to load session</AlertTitle>
          <AlertDescription className="space-y-2">
            <span className="font-mono text-xs block">{extractApiError(error)}</span>
            <Button variant="outline" size="sm" onClick={() => { void refetch() }}>Retry</Button>
          </AlertDescription>
        </Alert>
      )}

      {isPartial && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Partial data</AlertTitle>
          <AlertDescription>Showing the first 500 requests — analysis covers a partial session.</AlertDescription>
        </Alert>
      )}

      {aggregates && aggregates.summary.videoRequests === 0 && (
        <Alert role="status">
          <Film className="h-4 w-4" />
          <AlertTitle>No video segment requests</AlertTitle>
          <AlertDescription>
            This session only contains {aggregates.summary.totalRequests} request(s) without video segments (e.g. manifests or playlists). Video-specific streaming metrics require media segment requests.
          </AlertDescription>
        </Alert>
      )}

      {!showLoading && aggregates && (
        <>
          <div className="min-h-[260px] [contain-intrinsic-size:260px]">
            <StreamTimeline timeline={aggregates.timeline} isLoading={isLoading} />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <BitrateAnalysis aggregates={aggregates} isLoading={isLoading} />
            <BufferAnalysis aggregates={aggregates} isLoading={isLoading} />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <NetworkAnalysis aggregates={aggregates} isLoading={isLoading} />
            <ContentBreakdown aggregates={aggregates} isLoading={isLoading} />
          </div>
          {aggregates.starvationEvents.length > 0 && <StarvationTable events={aggregates.starvationEvents} />}
        </>
      )}
    </div>
  )
}

export default function StreamDetailPage() {
  return (
    <Suspense fallback={null}>
      <StreamDetailClient />
    </Suspense>
  )
}
