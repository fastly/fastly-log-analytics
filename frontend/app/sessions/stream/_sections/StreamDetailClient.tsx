'use client'

import React from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import {
  Film, Clock, Gauge, BarChart3, AlertTriangle, Wifi,
  ArrowUp, ArrowDown, ArrowLeft, Activity, Layers,
} from 'lucide-react'

import { client, extractApiError } from '@/lib/api'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button, buttonVariants } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'

import { useStreamAggregates } from './useStreamAggregates'
import { StreamTimeline } from './StreamTimeline'

import { formatDuration } from '@/lib/date'

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
      const { data } = await client.POST('/api/sessions/detail', {
        signal,
        body: { session_token: token! },
      })
      return data
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
    <div className="space-y-6">
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

      {/* Summary KPIs */}
      {isLoading ? (
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-4">
          {['duration', 'bitrate', 'top-br', 'util', 'rebuf', 'startup', 'health'].map(k => (
            <Skeleton key={k} className="h-[120px] rounded-lg" />
          ))}
        </div>
      ) : aggregates ? (
        <SummaryCards aggregates={aggregates} />
      ) : null}

      {/* Timeline chart */}
      {aggregates && (
        <StreamTimeline timeline={aggregates.timeline} isLoading={isLoading} />
      )}

      {/* Analysis sections */}
      {aggregates && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <BitrateAnalysis aggregates={aggregates} isLoading={isLoading} />
            <BufferAnalysis aggregates={aggregates} isLoading={isLoading} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
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

// ── Section components ──────────────────────────────────────────────────────

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
      marker: { color: '#10b981' },
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
          b.label === '0 (empty)' ? '#ef4444' :
          b.label === '< 2s' ? '#f59e0b' :
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
        line: { color: '#f59e0b' },
      })
    }
    if (nt.some(p => p.rtt != null)) {
      traces.push({
        x: nt.map(p => p.timestamp),
        y: nt.map(p => p.rtt),
        name: 'RTT',
        type: 'scatter' as const,
        yaxis: 'y2',
        line: { color: '#ef4444', dash: 'dot' },
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
