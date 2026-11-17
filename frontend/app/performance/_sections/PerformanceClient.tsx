'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { Clock, Network, Server, Shield, Zap } from 'lucide-react'
import { client, unwrapClient } from '@/lib/api'
import {
  useAnalyticsReportQuery,
  useColumnVisibility,
  useReportPageState,
} from '@/hooks/useReportPageState'
import { PlotlyChart } from '@/components/PlotlyChart'
import { ColumnVisibilityDropdown, DataTable } from '@/components/DataTable'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { boundMark, readRollupMeta, RollupDataThrough } from '@/components/network/rollupLabels'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { hasFilterValues, PERFORMANCE_ROLLUP_SECTIONS } from '@/lib/cloud-engine/rollup-sections'
import { CHART_COLORS, type RangeWire } from '@/lib/utils'
import type { components, FiltersPayload, PlotlyTrace } from '@/types/api'
import {
  ASN_COLUMN_IDS,
  asnColumns,
  CacheTtlHelp,
  OriginVsEdgeHelp,
  PERF_SORT_BY,
  SCATTER_LAYOUT,
  SlowestNetworksHelp,
  SlowestUrlsHelp,
  TTL_DIST_LAYOUT,
  URL_COLUMN_IDS,
  urlColumns,
  WATERFALL_LAYOUT,
} from './PerformanceHelp'

export {
  SlowestUrlsHelp,
  SlowestNetworksHelp,
  CacheTtlHelp,
  OriginVsEdgeHelp,
} from './PerformanceHelp'

type PerformanceSection = NonNullable<components['schemas']['PerformanceRequest']['sections']>[number]
const RAW_SECTIONS: PerformanceSection[] = ['top_urls', 'top_asns', 'scatter']

interface PerformanceBodyProps {
  activeServiceId: string | null
  filterPayload: FiltersPayload
  startTime: string | null
  endTime: string | null
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  getFieldLabel: (field: string) => string
  urlVisibility: ReturnType<typeof useColumnVisibility>[0]
  setUrlVisibility: ReturnType<typeof useColumnVisibility>[1]
  onUrlVisChange: ReturnType<typeof useColumnVisibility>[2]
  asnVisibility: ReturnType<typeof useColumnVisibility>[0]
  setAsnVisibility: ReturnType<typeof useColumnVisibility>[1]
  onAsnVisChange: ReturnType<typeof useColumnVisibility>[2]
}

function PerformanceBody({
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  getFieldLabel,
  urlVisibility,
  setUrlVisibility,
  onUrlVisChange,
  asnVisibility,
  setAsnVisibility,
  onAsnVisChange,
}: PerformanceBodyProps) {
  const [sortBy] = useQueryState('sort_by', parseAsStringLiteral(PERF_SORT_BY).withDefault('p99'))
  // Cloud answers the waterfall and TTL buckets from summaries only when unfiltered; read them apart
  // so they paint without waiting on the raw scan the URL and network rankings need.
  const rollupFirst = isCloudMode() && !hasFilterValues(filterPayload)
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }
  const postPerformance = (sections: PerformanceSection[] | undefined, rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) =>
    unwrapClient(
      client.POST('/api/performance/aggregates', {
        signal,
        body: {
          filters: filterPayload,
          sort_by: sortBy,
          sections,
          ...rangeBody,
        },
      }),
    )

  const performanceQuery = useAnalyticsReportQuery({
    namespace: ['performance', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: rollupFirst ? [sortBy, RAW_SECTIONS] : [sortBy],
    queryFn: ({ rangeBody, signal }) => postPerformance(rollupFirst ? RAW_SECTIONS : undefined, rangeBody, signal),
  })

  const rollupQuery = useAnalyticsReportQuery({
    namespace: ['performance', 'aggregates', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    queryOptions: { enabled: rollupFirst },
    queryFn: ({ rangeBody, signal }) => postPerformance([...PERFORMANCE_ROLLUP_SECTIONS], rangeBody, signal),
  })

  const coreData = performanceQuery.data
  const distQuery = rollupFirst ? rollupQuery : performanceQuery
  const distData = distQuery.data
  const rollup = rollupFirst ? readRollupMeta(distData) : undefined

  const ttlDist = distData?.ttl_dist
  const ttlDistData = React.useMemo((): PlotlyTrace[] => {
    if (!ttlDist?.length) return []
    return [
      {
        x: ttlDist.map((d) => d.bucket),
        y: ttlDist.map((d) => d.count),
        type: 'bar',
        marker: { color: '#6366f1' },
      },
    ]
  }, [ttlDist])

  const scatterRows = coreData?.scatter
  const scatterData = React.useMemo((): PlotlyTrace[] => {
    if (!scatterRows?.length) return []
    const isHit = (c: string | null | undefined) => c?.startsWith('HIT') === true
    const hit = scatterRows.filter((d) => isHit(d.cache))
    const miss = scatterRows.filter((d) => !isHit(d.cache))
    return [
      {
        x: hit.map((d) => d.origin),
        y: hit.map((d) => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'HIT',
        marker: { color: CHART_COLORS.success, size: 4, opacity: 0.6 },
      },
      {
        x: miss.map((d) => d.origin),
        y: miss.map((d) => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'MISS',
        marker: { color: CHART_COLORS.warning, size: 4, opacity: 0.6 },
      },
    ]
  }, [scatterRows])

  const avg = distData?.waterfall?.avg
  const waterfallData = React.useMemo((): PlotlyTrace[] => {
    if (!avg) return []
    return [
      { x: [avg.edge_processing || 0], y: ['Edge Processing'], type: 'bar', orientation: 'h', marker: { color: '#8b5cf6' }, showlegend: false },
      { x: [avg.origin_wait || 0], y: ['Origin TTFB Wait'], type: 'bar', orientation: 'h', marker: { color: CHART_COLORS.warning }, showlegend: false },
      { x: [avg.origin_download || 0], y: ['Origin Download'], type: 'bar', orientation: 'h', marker: { color: '#ec4899' }, showlegend: false },
      { x: [avg.client_download || 0], y: ['Client Download'], type: 'bar', orientation: 'h', marker: { color: CHART_COLORS.success }, showlegend: false },
    ]
  }, [avg])

  return (
    <>
      {rollup && <div className="mb-4"><RollupDataThrough meta={rollup} /></div>}
      <div className="mb-6">
        <AnalyticsCard
          title="End-to-End Latency Waterfall (Average)"
          icon={<Network className="h-4 w-4" />}
          isLoading={distQuery.isLoading}
          isFetching={distQuery.isFetching}
          loadingMessage="Calculating latency percentiles..."
          error={distQuery.error as AnalyticsCardError | null}
          isEmpty={!avg}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={
            <div className="space-y-4">
              <p>Breakdown of where request time is spent, averaged across the selected time window and filters.</p>
              <ul className="list-disc pl-4 space-y-2">
                <li><strong>Edge Processing:</strong> Time Fastly spends before sending the first byte (WAF, VCL processing) outside of origin wait.</li>
                <li><strong>Origin TTFB Wait:</strong> Time Fastly waits for the origin to send the first byte (Origin TTFB).</li>
                <li><strong>Origin Download:</strong> Time taken to download the rest of the response from the origin (Origin TTLB - Origin TTFB).</li>
                <li><strong>Client Download:</strong> Time taken to finish sending the response to the client after edge/origin processing.</li>
              </ul>
            </div>
          }
        >
          <PlotlyChart data={waterfallData} layout={WATERFALL_LAYOUT} height="100%" />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Slowest URLs"
          icon={<Server className="h-4 w-4" />}
          headerAction={
            <div className="flex items-center gap-2">
              {coreData?._approx === true && (
                <span
                  title="Latency percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Request counts are exact."
                  className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help"
                >
                  <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
                  Approximate
                </span>
              )}
              <ColumnVisibilityDropdown
                columns={URL_COLUMN_IDS.map((id) => ({ id, label: getFieldLabel(id) }))}
                visibility={urlVisibility}
                onChange={onUrlVisChange}
              />
            </div>
          }
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling endpoint latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_urls?.length ?? 0) === 0}
          className="min-h-[560px] [contain-intrinsic-size:560px] [content-visibility:auto]"
          contentClassName="p-0"
          helpContent={<SlowestUrlsHelp />}
        >
          <DataTable
            columns={urlColumns}
            data={coreData?.top_urls || []}
            hideToolbar
            columnVisibility={urlVisibility}
            onColumnVisibilityChange={setUrlVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Slowest Networks"
          icon={<Shield className="h-4 w-4" />}
          headerAction={
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown
                columns={ASN_COLUMN_IDS.map((id) => ({ id, label: getFieldLabel(id) }))}
                visibility={asnVisibility}
                onChange={onAsnVisChange}
              />
            </div>
          }
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling network latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_asns?.length ?? 0) === 0}
          className="min-h-[560px] [contain-intrinsic-size:560px] [content-visibility:auto]"
          contentClassName="p-0"
          helpContent={<SlowestNetworksHelp />}
        >
          <DataTable
            columns={asnColumns}
            data={coreData?.top_asns || []}
            hideToolbar
            columnVisibility={asnVisibility}
            onColumnVisibilityChange={setAsnVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title={boundMark(rollup, 'ttl_dist') ? 'Cache TTL Distribution (≥ counts)' : 'Cache TTL Distribution'}
          icon={<Clock className="h-4 w-4" />}
          isLoading={distQuery.isLoading}
          isFetching={distQuery.isFetching}
          loadingMessage="Profiling cache TTL distribution..."
          error={distQuery.error as AnalyticsCardError | null}
          isEmpty={(distData?.ttl_dist?.length ?? 0) === 0}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={<CacheTtlHelp />}
        >
          <PlotlyChart data={ttlDistData} layout={TTL_DIST_LAYOUT} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="Origin vs Edge Processing (ms)"
          icon={<Zap className="h-4 w-4" />}
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling origin vs edge latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.scatter?.length ?? 0) === 0}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={<OriginVsEdgeHelp />}
        >
          <PlotlyChart data={scatterData} layout={SCATTER_LAYOUT} height="100%" />
        </AnalyticsCard>
      </div>
    </>
  )
}

export default function PerformanceClient({
  nowServerStr,
}: { nowServerStr?: string; params?: unknown; searchParams?: unknown } = {}) {
  const { relativeRange, isAutoRange, anchor, getFieldLabel } = useReportPageState(nowServerStr)
  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [asnVisibility, setAsnVisibility, onAsnVisChange] = useColumnVisibility()

  return (
    <ReportLayout
      title="Performance"
      description="Analyze latency, cache efficiency, and origin vs edge processing time."
      icon={Zap}
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <PerformanceBody
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          startTime={startTime}
          endTime={endTime}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
          getFieldLabel={getFieldLabel}
          urlVisibility={urlVisibility}
          setUrlVisibility={setUrlVisibility}
          onUrlVisChange={onUrlVisChange}
          asnVisibility={asnVisibility}
          setAsnVisibility={setAsnVisibility}
          onAsnVisChange={onAsnVisChange}
        />
      )}
    </ReportLayout>
  )
}
