'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { BarChart3, Gauge, Play, UserPlus, Users, Wifi } from 'lucide-react'
import { AnalyticsCard, TimeSeriesChart, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable } from '@/components/DataTable'
import { Skeleton } from '@/components/ui/skeleton'
import type { components, PlotlyTrace } from '@/types/api'
import {
  BITRATE_LADDER_COLUMNS,
  BITRATE_LAYOUT,
  BUFFER_LAYOUT,
  SESSIONS_LAYOUT,
  SESSION_STARTS_LAYOUT,
  STARTUP_LAYOUT,
  STREAMING_INFO,
  STREAM_TYPE_COLUMNS,
  THROUGHPUT_LAYOUT,
} from './StreamingConstants'

const StreamingSecondaryCharts = dynamic(
  () => import('./StreamingSecondaryCharts').then((m) => m.StreamingSecondaryCharts),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-6 mb-6" aria-busy="true">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Skeleton className="h-[320px] w-full rounded-xl" />
          <Skeleton className="h-[320px] w-full rounded-xl" />
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Skeleton className="h-[280px] w-full rounded-xl" />
          <Skeleton className="h-[280px] w-full rounded-xl" />
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Skeleton className="h-[280px] w-full rounded-xl" />
          <Skeleton className="h-[280px] w-full rounded-xl" />
        </div>
      </div>
    ),
  },
)

type CmcdAggregatesResponse = components['schemas']['CmcdAggregatesResponse']
type CmcdBitrateLadderRow = NonNullable<CmcdAggregatesResponse['bitrate_ladder']>[number]
type CmcdStreamTypeRow = NonNullable<CmcdAggregatesResponse['stream_type_dist']>[number]

export interface StreamingChartsGridProps {
  data: CmcdAggregatesResponse | undefined
  queryStatus: {
    isLoading: boolean
    isFetching: boolean
    error: AnalyticsCardError | null
  }
  extendedQueryStatus?: {
    isLoading: boolean
    isFetching: boolean
    error: AnalyticsCardError | null
  }
  extendedSectionRef?: React.RefObject<HTMLDivElement | null>
  tsProps: {
    startTime: string | null
    endTime: string | null
    timezone: string
    height: string
    onRelayout: (event: Readonly<Record<string, unknown>>) => void
  }
  sessionsData: PlotlyTrace[]
  sessionStartsData: PlotlyTrace[]
  bufferHealthData: PlotlyTrace[]
  bitrateData: PlotlyTrace[]
  throughputData: PlotlyTrace[]
  startupData: PlotlyTrace[]
  durationDistData: PlotlyTrace[]
  objectTypeData: PlotlyTrace[]
  streamingFormatRows: Array<{ streaming_format: string; session_count: number }>
  bitrateLadderRows: CmcdBitrateLadderRow[]
  streamTypeRows: CmcdStreamTypeRow[]
}

export function StreamingChartsGrid({
  data,
  queryStatus,
  extendedQueryStatus = queryStatus,
  extendedSectionRef,
  tsProps,
  sessionsData,
  sessionStartsData,
  bufferHealthData,
  bitrateData,
  throughputData,
  startupData,
  durationDistData,
  objectTypeData,
  streamingFormatRows,
  bitrateLadderRows,
  streamTypeRows,
}: StreamingChartsGridProps) {
  return (
    <>
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

      <StreamingSecondaryCharts
        data={data}
        queryStatus={queryStatus}
        durationDistData={durationDistData}
        objectTypeData={objectTypeData}
        streamingFormatRows={streamingFormatRows}
      />

      <div ref={extendedSectionRef} className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="ABR Bitrate Ladder"
          icon={<Gauge className="h-4 w-4" />}
          {...extendedQueryStatus}
          isEmpty={!bitrateLadderRows.length}
          helpTitle={STREAMING_INFO.bitrate_ladder.title}
          helpContent={STREAMING_INFO.bitrate_ladder.body}
        >
          <DataTable columns={BITRATE_LADDER_COLUMNS} data={bitrateLadderRows} />
        </AnalyticsCard>

        <AnalyticsCard
          title="Stream Type (Live vs VOD)"
          icon={<Play className="h-4 w-4" />}
          {...extendedQueryStatus}
          isEmpty={!streamTypeRows.length}
          helpTitle={STREAMING_INFO.stream_type_dist.title}
          helpContent={STREAMING_INFO.stream_type_dist.body}
        >
          <DataTable columns={STREAM_TYPE_COLUMNS} data={streamTypeRows} />
        </AnalyticsCard>
      </div>
    </>
  )
}
