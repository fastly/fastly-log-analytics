'use client'

import React from 'react'
import { BarChart3, Clock, Film, Globe, Play, Wifi } from 'lucide-react'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable } from '@/components/DataTable'
import type { components, PlotlyTrace } from '@/types/api'
import {
  CONTENT_COLUMNS,
  DURATION_DIST_LAYOUT,
  EMPTY_PLOT_LAYOUT,
  EMPTY_ROWS,
  REBUFFER_ASN_COLUMNS,
  REBUFFER_COUNTRY_COLUMNS,
  STREAMING_FORMAT_COLUMNS,
  STREAMING_INFO,
} from './StreamingConstants'

type CmcdAggregatesResponse = components['schemas']['CmcdAggregatesResponse']

export interface StreamingSecondaryChartsProps {
  data: CmcdAggregatesResponse | undefined
  queryStatus: {
    isLoading: boolean
    isFetching: boolean
    error: AnalyticsCardError | null
  }
  durationDistData: PlotlyTrace[]
  objectTypeData: PlotlyTrace[]
  streamingFormatRows: Array<{ streaming_format: string; session_count: number }>
}

export function StreamingSecondaryCharts({
  data,
  queryStatus,
  durationDistData,
  objectTypeData,
  streamingFormatRows,
}: StreamingSecondaryChartsProps) {
  return (
    <>
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
    </>
  )
}
