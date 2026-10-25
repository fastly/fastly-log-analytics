'use client'

import React, { Suspense, useMemo } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
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
import { DataTable } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button, buttonVariants } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { OBJECT_TYPE_LABELS } from '@/app/streaming/_sections/StreamingClient'

import { formatDuration } from '@/lib/format'
import { CHART_COLORS } from '@/lib/chart-helpers'

type Row = Record<string, unknown>

function num(v: unknown): number | null {
  if (v == null) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function str(v: unknown): string | null {
  if (v == null) return null
  return String(v)
}

function bool(v: unknown): boolean {
  return v === true || v === 1 || v === '1' || v === 'true'
}

export interface StreamSummary {
  durationSeconds: number | null
  avgBitrate: number | null
  topBitrate: number | null
  utilization: number | null
  rebufferCount: number
  startupTimeMs: number | null
  bufferHealthPct: number | null
  totalRequests: number
  videoRequests: number
}

export interface TimelinePoint {
  timestamp: string
  bitrate: number | null
  buffer: number | null
  throughput: number | null
  isRebuffer: boolean
  isStartup: boolean
}

export interface BitrateTier {
  bitrate: number
  count: number
  pct: number
}

export interface BitrateShifts {
  upshifts: number
  downshifts: number
}

export interface BufferBucket {
  label: string
  count: number
  pct: number
}

export interface StarvationEvent {
  timestamp: string
  bitrate: number | null
  buffer: number | null
  throughput: number | null
  url: string | null
}

export interface ObjectTypeDist {
  type: string
  label: string
  count: number
  pct: number
}

export interface NetworkPoint {
  timestamp: string
  throughput: number | null
  rtt: number | null
}

export interface StreamAggregates {
  summary: StreamSummary
  timeline: TimelinePoint[]
  bitrateTiers: BitrateTier[]
  bitrateShifts: BitrateShifts
  bufferBuckets: BufferBucket[]
  starvationEvents: StarvationEvent[]
  objectTypeDist: ObjectTypeDist[]
  networkTimeline: NetworkPoint[]
  pops: string[]
  contentIds: { id: string; count: number }[]
}

function computeAggregates(rows: Row[]): StreamAggregates {
  const sorted = [...rows].sort((a, b) => {
    const ta = str(a.timestamp) ?? ''
    const tb = str(b.timestamp) ?? ''
    return ta < tb ? -1 : ta > tb ? 1 : 0
  })

  const explicitVideo = sorted.filter(r => {
    const ot = str(r.cmcd_ot)
    return ot === 'v' || ot === 'av'
  })
  const videoRows = explicitVideo.length > 0
    ? explicitVideo
    : sorted.filter(r => num(r.cmcd_br) !== null || num(r.cmcd_bl) !== null)

  const timestamps = sorted.map(r => str(r.timestamp)).filter(Boolean) as string[]
  let durationSeconds: number | null = null
  if (timestamps.length >= 2) {
    const first = new Date(timestamps[0]).getTime()
    const last = new Date(timestamps[timestamps.length - 1]).getTime()
    durationSeconds = (last - first) / 1000
  }

  const videoBitrates = videoRows.map(r => num(r.cmcd_br)).filter((v): v is number => v !== null)
  const avgBitrate = videoBitrates.length ? videoBitrates.reduce((a, b) => a + b, 0) / videoBitrates.length : null

  const videoTopBitrates = videoRows.map(r => num(r.cmcd_tb)).filter((v): v is number => v !== null)
  const topBitrate = videoTopBitrates.length
    ? Math.max(...videoTopBitrates)
    : (videoBitrates.length ? Math.max(...videoBitrates) : null)

  const utilization = avgBitrate != null && topBitrate != null && topBitrate > 0
    ? avgBitrate / topBitrate
    : null

  const rebufferCount = sorted.filter(r => bool(r.cmcd_bs)).length

  let startupTimeMs: number | null = null
  if (sorted.length > 0) {
    const firstTs = new Date(str(sorted[0].timestamp) ?? '').getTime()
    const firstNonStartup = sorted.find(r => !bool(r.cmcd_su) && (str(r.cmcd_ot) === 'v' || str(r.cmcd_ot) === 'av' || num(r.cmcd_br) !== null))
    if (firstNonStartup) {
      const nsTs = new Date(str(firstNonStartup.timestamp) ?? '').getTime()
      startupTimeMs = nsTs - firstTs
    }
  }

  const videoBuffers = videoRows.map(r => num(r.cmcd_bl)).filter((v): v is number => v !== null)
  const bufferHealthPct = videoBuffers.length
    ? (videoBuffers.filter(b => b > 0).length / videoBuffers.length) * 100
    : null

  const summary: StreamSummary = {
    durationSeconds,
    avgBitrate,
    topBitrate,
    utilization,
    rebufferCount,
    startupTimeMs,
    bufferHealthPct,
    totalRequests: sorted.length,
    videoRequests: videoRows.length,
  }

  const timeline: TimelinePoint[] = videoRows.map(r => ({
    timestamp: str(r.timestamp) ?? '',
    bitrate: num(r.cmcd_br),
    buffer: num(r.cmcd_bl),
    throughput: num(r.cmcd_mtp),
    isRebuffer: bool(r.cmcd_bs),
    isStartup: bool(r.cmcd_su),
  }))

  const tierCounts = new Map<number, number>()
  for (const br of videoBitrates) tierCounts.set(br, (tierCounts.get(br) ?? 0) + 1)
  const bitrateTiers: BitrateTier[] = [...tierCounts.entries()]
    .sort((a, b) => b[0] - a[0])
    .map(([bitrate, count]) => ({
      bitrate,
      count,
      pct: videoBitrates.length ? (count / videoBitrates.length) * 100 : 0,
    }))

  let upshifts = 0
  let downshifts = 0
  for (let i = 1; i < videoBitrates.length; i++) {
    if (videoBitrates[i] > videoBitrates[i - 1]) upshifts++
    else if (videoBitrates[i] < videoBitrates[i - 1]) downshifts++
  }

  const bucketDefs: [string, (v: number) => boolean][] = [
    ['0 (empty)', v => v === 0],
    ['< 2s', v => v > 0 && v < 2000],
    ['2–10s', v => v >= 2000 && v < 10000],
    ['10–30s', v => v >= 10000 && v < 30000],
    ['30s+', v => v >= 30000],
  ]
  const bufferBuckets: BufferBucket[] = bucketDefs.map(([label, pred]) => {
    const count = videoBuffers.filter(pred).length
    return {
      label,
      count,
      pct: videoBuffers.length ? (count / videoBuffers.length) * 100 : 0,
    }
  })

  const starvationEvents: StarvationEvent[] = sorted
    .filter(r => bool(r.cmcd_bs))
    .map(r => ({
      timestamp: str(r.timestamp) ?? '',
      bitrate: num(r.cmcd_br),
      buffer: num(r.cmcd_bl),
      throughput: num(r.cmcd_mtp),
      url: str(r.url),
    }))

  const otCounts = new Map<string, number>()
  for (const r of sorted) {
    const ot = str(r.cmcd_ot) ?? 'unknown'
    otCounts.set(ot, (otCounts.get(ot) ?? 0) + 1)
  }
  const objectTypeDist: ObjectTypeDist[] = [...otCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([type, count]) => ({
      type,
      label: OBJECT_TYPE_LABELS[type] ?? type,
      count,
      pct: sorted.length ? (count / sorted.length) * 100 : 0,
    }))

  const networkTimeline: NetworkPoint[] = sorted
    .filter(r => num(r.cmcd_mtp) !== null || num(r.tcp_rtt) !== null)
    .map(r => ({
      timestamp: str(r.timestamp) ?? '',
      throughput: num(r.cmcd_mtp),
      rtt: num(r.tcp_rtt),
    }))

  const popSet = new Set<string>()
  for (const r of sorted) {
    const p = str(r.pop)
    if (p) popSet.add(p)
  }

  const cidCounts = new Map<string, number>()
  for (const r of sorted) {
    const cid = str(r.cmcd_cid)
    if (cid) cidCounts.set(cid, (cidCounts.get(cid) ?? 0) + 1)
  }
  const contentIds = [...cidCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([id, count]) => ({ id, count }))

  return {
    summary,
    timeline,
    bitrateTiers,
    bitrateShifts: { upshifts, downshifts },
    bufferBuckets,
    starvationEvents,
    objectTypeDist,
    networkTimeline,
    pops: [...popSet],
    contentIds,
  }
}

export function useStreamAggregates(rows: Row[] | undefined): StreamAggregates | null {
  return useMemo(() => {
    if (!rows?.length) return null
    return computeAggregates(rows)
  }, [rows])
}

const TIMELINE_LAYOUT = {
  yaxis: { title: { text: 'kbps' } },
  yaxis2: {
    title: { text: 'Buffer (ms)' },
    overlaying: 'y',
    side: 'right' as const,
    rangemode: 'tozero',
  },
  legend: { orientation: 'h' as const, y: -0.15 },
  margin: { r: 60, t: 40 },
}

interface StreamTimelineProps {
  timeline: TimelinePoint[]
  isLoading?: boolean
}

export function StreamTimeline({ timeline, isLoading }: StreamTimelineProps) {
  const { traces, shapes, annotations } = React.useMemo(() => {
    if (!timeline.length) return { traces: [], shapes: [], annotations: [] }

    const ts = timeline.map(p => p.timestamp)

    const traces: Record<string, unknown>[] = [
      {
        x: ts,
        y: timeline.map(p => p.bitrate),
        name: 'Bitrate',
        type: 'scatter' as const,
        line: { color: CHART_COLORS.success, shape: 'hv' },
      },
      {
        x: ts,
        y: timeline.map(p => p.throughput),
        name: 'Throughput',
        type: 'scatter' as const,
        line: { color: CHART_COLORS.warning, dash: 'dot' },
      },
      {
        x: ts,
        y: timeline.map(p => p.buffer),
        name: 'Buffer',
        type: 'scatter' as const,
        yaxis: 'y2',
        fill: 'tozeroy' as const,
        line: { color: '#6366f1' },
        fillcolor: 'rgba(99, 102, 241, 0.1)',
      },
    ]

    const shapes = timeline
      .filter(p => p.isRebuffer)
      .map(p => ({
        type: 'line' as const,
        x0: p.timestamp,
        x1: p.timestamp,
        y0: 0,
        y1: 1,
        yref: 'paper' as const,
        line: { color: CHART_COLORS.danger, width: 1.5, dash: 'dash' as const },
      }))

    const annotations = timeline
      .filter(p => p.isStartup)
      .map(p => ({
        x: p.timestamp,
        y: 1.05,
        yref: 'paper' as const,
        yanchor: 'bottom' as const,
        text: 'SU',
        showarrow: false,
        font: { size: 9, color: '#6366f1' },
        bgcolor: 'rgba(99, 102, 241, 0.1)',
        borderpad: 2,
      }))

    return { traces, shapes, annotations }
  }, [timeline])

  const layout = React.useMemo(() => ({
    ...TIMELINE_LAYOUT,
    shapes,
    annotations,
  }), [shapes, annotations])

  return (
    <AnalyticsCard
      title="Session Timeline"
      icon={<Activity className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!timeline.length}
      className="h-[400px] mb-6"
      contentClassName="p-2"
      description="Bitrate and throughput (left axis) with buffer depth (right axis). Red dashed lines mark rebuffer events."
    >
      <PlotlyChart data={traces} layout={layout} height="100%" />
    </AnalyticsCard>
  )
}

const NETWORK_LAYOUT = {
  yaxis: { title: { text: 'Throughput (kbps)' } },
  yaxis2: {
    title: { text: 'RTT (ms)' },
    overlaying: 'y',
    side: 'right' as const,
    rangemode: 'tozero',
  },
}

const BUFFER_DIST_LAYOUT = {
  yaxis: { title: { text: 'Segments' } },
  barcornerradius: 3,
}

const BITRATE_DIST_LAYOUT = {
  yaxis: { title: { text: 'Segments' } },
  barcornerradius: 3,
}

const OT_LAYOUT = {}

export function StreamDetailClient() {
  const searchParams = useSearchParams()
  const token = searchParams.get('token')
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()

  const {
    data: detailData,
    isLoading,
    isError,
    error,
    refetch,
  } = useQuery({
    queryKey: ['stream-detail', activeServiceId, token],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.POST('/api/sessions/detail', {
          signal,
          body: { session_token: token! },
        }),
      )
    },
    enabled: !!activeServiceId && !!token,
  })

  const aggregates = useStreamAggregates(detailData?.data)

  if (!activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Film} message="Select a service to view stream details." />
  }

  if (!token) {
    return (
      <div className="space-y-6">
        <PageHeader title="Stream Details" icon={Film} />
        <Alert>
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

  if (!activeServiceId) return null

  const isPartial = detailData?.data?.length === 500

  return (
    <div data-testid="stream-detail-client" className="space-y-6">
      <PageHeader
        title="Stream Session Details"
        description="CMCD streaming quality metrics and request-level timeline for a single session."
        icon={Film}
      >
        <Link href="/sessions" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" />
          Sessions
        </Link>
      </PageHeader>

      {isError && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Failed to load session</AlertTitle>
          <AlertDescription className="space-y-2">
            <span className="font-mono text-xs block">{extractApiError(error)}</span>
            <Button variant="outline" size="sm" onClick={() => { void refetch() }}>
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      )}

      {isPartial && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Partial data</AlertTitle>
          <AlertDescription>
            Showing the first 500 requests — analysis covers a partial session.
          </AlertDescription>
        </Alert>
      )}

      {aggregates && aggregates.summary.videoRequests === 0 && (
        <Alert>
          <Film className="h-4 w-4" />
          <AlertTitle>No video segment requests</AlertTitle>
          <AlertDescription>
            This session only contains {aggregates.summary.totalRequests} request(s) without video segments (e.g. manifests or playlists). Video-specific streaming metrics require media segment requests.
          </AlertDescription>
        </Alert>
      )}

      {/* Summary KPIs */}
      {isLoading ? (
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4 min-h-[120px] [contain-intrinsic-size:120px]">
          {['duration', 'bitrate', 'top-br', 'util', 'rebuf', 'startup', 'health'].map(k => (
            <Skeleton key={k} className="h-[120px] rounded-lg" />
          ))}
        </div>
      ) : aggregates ? (
        <SummaryCards aggregates={aggregates} />
      ) : null}

      {/* Timeline chart */}
      {aggregates && (
        <div className="min-h-[260px] [contain-intrinsic-size:260px]">
          <StreamTimeline timeline={aggregates.timeline} isLoading={isLoading} />
        </div>
      )}

      {/* Analysis sections */}
      {aggregates && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <BitrateAnalysis aggregates={aggregates} isLoading={isLoading} />
            <BufferAnalysis aggregates={aggregates} isLoading={isLoading} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 min-h-[360px] [contain-intrinsic-size:360px]">
            <NetworkAnalysis aggregates={aggregates} isLoading={isLoading} />
            <ContentBreakdown aggregates={aggregates} isLoading={isLoading} />
          </div>

          {aggregates.starvationEvents.length > 0 && (
            <StarvationTable events={aggregates.starvationEvents} />
          )}
        </>
      )}
    </div>
  )
}

function SummaryCards({ aggregates }: { aggregates: NonNullable<ReturnType<typeof useStreamAggregates>> }) {
  const { summary } = aggregates
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4">
      <StatCard
        title="Duration"
        value={summary.durationSeconds != null ? formatDuration(summary.durationSeconds) : '—'}
        sub={`${summary.totalRequests} requests`}
        icon={Clock}
      />
      <StatCard
        title="Avg Bitrate"
        value={summary.avgBitrate != null ? `${Math.round(summary.avgBitrate)} kbps` : '—'}
        sub={`${summary.videoRequests} video segments`}
        icon={Gauge}
      />
      <StatCard
        title="Top Bitrate"
        value={summary.topBitrate != null ? `${Math.round(summary.topBitrate)} kbps` : '—'}
        sub="highest available tier"
        icon={ArrowUp}
      />
      <StatCard
        title="Utilization"
        value={summary.utilization != null ? `${Math.round(summary.utilization * 100)}%` : '—'}
        sub="avg / top bitrate"
        icon={Activity}
      />
      <StatCard
        title="Rebuffers"
        value={summary.rebufferCount}
        sub="buffer starvation events"
        icon={AlertTriangle}
        iconClassName={summary.rebufferCount > 0 ? 'text-destructive' : undefined}
      />
      <StatCard
        title="Startup Time"
        value={summary.startupTimeMs != null ? `${(summary.startupTimeMs / 1000).toFixed(1)}s` : '—'}
        sub="time to first video"
        icon={Film}
      />
      <StatCard
        title="Buffer Health"
        value={summary.bufferHealthPct != null ? `${Math.round(summary.bufferHealthPct)}%` : '—'}
        sub="segments with buffer > 0"
        icon={BarChart3}
      />
    </div>
  )
}

function BitrateAnalysis({ aggregates, isLoading }: { aggregates: NonNullable<ReturnType<typeof useStreamAggregates>>; isLoading?: boolean }) {
  const chartData = React.useMemo(() => {
    if (!aggregates.bitrateTiers.length) return []
    return [{
      x: aggregates.bitrateTiers.map(t => `${t.bitrate} kbps`),
      y: aggregates.bitrateTiers.map(t => t.count),
      type: 'bar' as const,
      marker: { color: CHART_COLORS.success },
      text: aggregates.bitrateTiers.map(t => `${t.pct.toFixed(0)}%`),
      textposition: 'auto' as const,
    }]
  }, [aggregates.bitrateTiers])

  return (
    <AnalyticsCard
      title="Bitrate Distribution"
      icon={<Gauge className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!aggregates.bitrateTiers.length}
      className="h-[360px]"
      contentClassName="p-2"
      headerAction={
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          <span className="flex items-center gap-1">
            <ArrowUp className="h-3 w-3 text-green-500" />
            {aggregates.bitrateShifts.upshifts} up
          </span>
          <span className="flex items-center gap-1">
            <ArrowDown className="h-3 w-3 text-red-500" />
            {aggregates.bitrateShifts.downshifts} down
          </span>
        </div>
      }
    >
      <PlotlyChart data={chartData} layout={BITRATE_DIST_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

function BufferAnalysis({ aggregates, isLoading }: { aggregates: NonNullable<ReturnType<typeof useStreamAggregates>>; isLoading?: boolean }) {
  const chartData = React.useMemo(() => {
    if (!aggregates.bufferBuckets.length) return []
    return [{
      x: aggregates.bufferBuckets.map(b => b.label),
      y: aggregates.bufferBuckets.map(b => b.count),
      type: 'bar' as const,
      marker: {
        color: aggregates.bufferBuckets.map(b =>
          b.label === '0 (empty)' ? CHART_COLORS.danger :
          b.label === '< 2s' ? CHART_COLORS.warning :
          '#6366f1'
        ),
      },
      text: aggregates.bufferBuckets.map(b => `${b.pct.toFixed(0)}%`),
      textposition: 'auto' as const,
    }]
  }, [aggregates.bufferBuckets])

  return (
    <AnalyticsCard
      title="Buffer Depth Distribution"
      icon={<BarChart3 className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!aggregates.bufferBuckets.length}
      className="h-[360px]"
      contentClassName="p-2"
    >
      <PlotlyChart data={chartData} layout={BUFFER_DIST_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

function NetworkAnalysis({ aggregates, isLoading }: { aggregates: NonNullable<ReturnType<typeof useStreamAggregates>>; isLoading?: boolean }) {
  const chartData = React.useMemo(() => {
    const nt = aggregates.networkTimeline
    if (!nt.length) return []
    const traces: Record<string, unknown>[] = []
    if (nt.some(p => p.throughput != null)) {
      traces.push({
        x: nt.map(p => p.timestamp),
        y: nt.map(p => p.throughput),
        name: 'Throughput',
        type: 'scatter' as const,
        line: { color: CHART_COLORS.warning },
      })
    }
    if (nt.some(p => p.rtt != null)) {
      traces.push({
        x: nt.map(p => p.timestamp),
        y: nt.map(p => p.rtt),
        name: 'RTT',
        type: 'scatter' as const,
        yaxis: 'y2',
        line: { color: CHART_COLORS.danger, dash: 'dot' },
      })
    }
    return traces
  }, [aggregates.networkTimeline])

  return (
    <AnalyticsCard
      title="Network Conditions"
      icon={<Wifi className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!aggregates.networkTimeline.length}
      className="h-[360px]"
      contentClassName="p-2"
      headerAction={
        aggregates.pops.length > 0 ? (
          <div className="flex items-center gap-1 text-xs text-muted-foreground">
            <span>POPs:</span>
            {aggregates.pops.map(pop => (
              <Badge key={pop} variant="secondary" className="text-[10px] px-1.5 py-0">
                {pop}
              </Badge>
            ))}
          </div>
        ) : undefined
      }
    >
      <PlotlyChart data={chartData} layout={NETWORK_LAYOUT} height="100%" />
    </AnalyticsCard>
  )
}

function ContentBreakdown({ aggregates, isLoading }: { aggregates: NonNullable<ReturnType<typeof useStreamAggregates>>; isLoading?: boolean }) {
  const pieData = React.useMemo(() => {
    if (!aggregates.objectTypeDist.length) return []
    return [{
      labels: aggregates.objectTypeDist.map(d => d.label),
      values: aggregates.objectTypeDist.map(d => d.count),
      type: 'pie' as const,
      hole: 0.4,
    }]
  }, [aggregates.objectTypeDist])

  const contentColumns = React.useMemo(() => [
    { accessorKey: 'id', header: 'Content ID' },
    { accessorKey: 'count', header: 'Segments' },
  ], [])

  return (
    <AnalyticsCard
      title="Content Breakdown"
      icon={<Layers className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!aggregates.objectTypeDist.length && !aggregates.contentIds.length}
      className="min-h-[360px]"
      contentClassName="p-2"
    >
      <div className="flex flex-col gap-4 h-full">
        {aggregates.objectTypeDist.length > 0 && (
          <div className="h-[200px]">
            <PlotlyChart data={pieData} layout={OT_LAYOUT} height="100%" />
          </div>
        )}
        {aggregates.contentIds.length > 0 && (
          <DataTable columns={contentColumns} data={aggregates.contentIds} />
        )}
      </div>
    </AnalyticsCard>
  )
}

function StarvationTable({ events }: { events: NonNullable<ReturnType<typeof useStreamAggregates>>['starvationEvents'] }) {
  const columns = React.useMemo(() => [
    { accessorKey: 'timestamp', header: 'Time' },
    { accessorKey: 'bitrate', header: 'Bitrate (kbps)' },
    { accessorKey: 'buffer', header: 'Buffer (ms)' },
    { accessorKey: 'throughput', header: 'Throughput (kbps)' },
    {
      accessorKey: 'url',
      header: 'URL',
      cell: ({ row }: { row: { original: { url: string | null } } }) => (
        <span className="text-xs truncate max-w-[300px] inline-block font-mono">
          {row.original.url ?? '—'}
        </span>
      ),
    },
  ], [])

  return (
    <AnalyticsCard
      title="Buffer Starvation Events"
      icon={<AlertTriangle className="h-4 w-4" />}
      isEmpty={!events.length}
      description="Requests where the player reported buffer starvation (cmcd_bs=true)."
    >
      <DataTable columns={columns} data={events} />
    </AnalyticsCard>
  )
}

export default function StreamDetailPage() {
  return (
    <Suspense fallback={null}>
      <StreamDetailClient />
    </Suspense>
  )
}
