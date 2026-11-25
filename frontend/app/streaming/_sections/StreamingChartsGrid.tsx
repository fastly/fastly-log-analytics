'use client'

import React from 'react'
import { BarChart3, Clock, Film, Gauge, Globe, Play, UserPlus, Users, Wifi } from 'lucide-react'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard, TimeSeriesChart, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable } from '@/components/DataTable'
import type { components, PlotlyTrace } from '@/types/api'
import {
  BITRATE_LADDER_COLUMNS,
  BITRATE_LAYOUT,
  BUFFER_LAYOUT,
  CONTENT_COLUMNS,
  DURATION_DIST_LAYOUT,
  EMPTY_PLOT_LAYOUT,
  EMPTY_ROWS,
  REBUFFER_ASN_COLUMNS,
  REBUFFER_COUNTRY_COLUMNS,
  SESSIONS_LAYOUT,
  SESSION_STARTS_LAYOUT,
  STARTUP_LAYOUT,
  STREAMING_FORMAT_COLUMNS,
  STREAMING_INFO,
  STREAM_TYPE_COLUMNS,
  THROUGHPUT_LAYOUT,
} from './StreamingConstants'

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

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
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
