'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { Server } from 'lucide-react'
import { client, unwrapClient } from '@/lib/api'
import type { components, PlotlyTrace } from '@/types/api'
import {
  CHART_COLORS,
  formatDate,
  INTERVAL_SECONDS,
  statusHex,
  useTimeseriesToTraces,
  type RangeWire,
  type RangeWireArgs,
} from '@/lib/utils'
import {
  useAnalyticsReportQuery,
  useColumnVisibility,
  useReportPageState,
} from '@/hooks/useReportPageState'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { ReportLayout, type ReportLayoutRenderProps } from '@/components/ReportLayout'
import { approxMark, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { hasFilterValues, originRollupSections } from '@/lib/cloud-engine/rollup-sections'
import {
  Aggregates,
  ApproxBadge,
  type OriginAggregates,
  type OriginIpHealth,
  type OriginIpHealthRow,
  type OriginPopLatency,
  type OriginPopLatencyRow,
  type OriginSlowUrlRow,
  type OriginSlowUrls,
  type OriginStatusCodes,
  type OriginSummary,
  type OriginTimeseries,
  type SectionQuery,
} from './Aggregates'
import {
  ORIGIN_METRICS,
  ORIGIN_PERCENTILES,
  Timeseries,
  type OriginMetric,
  type OriginPercentile,
  type TimeseriesProps,
} from './Timeseries'
import { LatencyHeatmap, type LatencyHeatmapProps } from './LatencyHeatmap'

export {
  Aggregates,
  ApproxBadge,
  Timeseries,
  LatencyHeatmap,
  type OriginAggregates,
  type OriginSummary,
  type OriginTimeseries,
  type OriginSlowUrls,
  type OriginStatusCodes,
  type OriginPopLatency,
  type OriginIpHealth,
  type OriginSlowUrlRow,
  type OriginPopLatencyRow,
  type OriginIpHealthRow,
  type SectionQuery,
  type OriginMetric,
  type OriginPercentile,
  type TimeseriesProps,
  type LatencyHeatmapProps,
}

type OriginSection = NonNullable<components['schemas']['OriginAggregatesRequest']['sections']>[number]

// Must stay in lockstep with ORIGIN_SSR_SECTIONS in lib/ssr/origin.ts
const ORIGIN_SECTIONS: OriginSection[] = [
  'summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health',
]

type OriginReportContentProps = ReportLayoutRenderProps &
  Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'>

const INTERVAL_MAP: Record<string, number> = { '1 second': 1 / 60, '1 minute': 1, '1 hour': 60, '1 day': 1440 }
const TREND_MAP: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }

function OriginReportContent({
  startTime, endTime, timezone, activeServiceId, filterPayload, config,
  trend, setTrend, intervalButtons, relativeRange, isAutoRange, anchor,
}: OriginReportContentProps) {
  const [originMetric, setOriginMetric] = useQueryState('metric', parseAsStringLiteral(ORIGIN_METRICS).withDefault('ttfb'))
  const [originPercentile, setOriginPercentile] = useQueryState('percentile', parseAsStringLiteral(ORIGIN_PERCENTILES).withDefault('p95'))
  const { isFieldActive } = useActiveLogFields()
  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [popVisibility, setPopVisibility, onPopVisChange] = useColumnVisibility()
  const [ipVisibility, setIpVisibility, onIpVisChange] = useColumnVisibility()
  const bucketMinutes = INTERVAL_MAP[config.effectiveInterval] || 5

  // Cloud answers these sections from summaries only when unfiltered; read them apart so they paint
  // without waiting on the raw scan the remaining sections need.
  const rollupSections: OriginSection[] = isCloudMode() && !hasFilterValues(filterPayload)
    ? originRollupSections({ metric: originMetric, bucketMinutes, splitByLeg: false })
    : []
  const rawSections = ORIGIN_SECTIONS.filter((section) => !rollupSections.includes(section))
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }

  const postOrigin = (sections: OriginSection[], rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) =>
    unwrapClient(
      client.POST('/api/origin/aggregates', {
        signal,
        body: {
          filters: filterPayload,
          bucket_minutes: bucketMinutes,
          split_by_leg: false,
          timeseries_metric: originMetric,
          timeseries_percentile: originPercentile,
          slow_urls_limit: 20,
          slow_urls_min_requests: 50,
          pop_latency_limit: 30,
          ip_health_limit: 30,
          sections,
          ...rangeBody,
        },
      }),
      'Failed to load origin aggregates',
    )

  const bundle = useAnalyticsReportQuery<OriginAggregates | undefined>({
    namespace: ['origin', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [bucketMinutes, originMetric, originPercentile, rawSections],
    queryFn: ({ rangeBody, signal }) => postOrigin(rawSections, rangeBody, signal),
  })

  const rollupQuery = useAnalyticsReportQuery<OriginAggregates | undefined>({
    namespace: ['origin', 'aggregates', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [bucketMinutes, originMetric, originPercentile, rollupSections],
    queryOptions: { enabled: rollupSections.length > 0 },
    queryFn: ({ rangeBody, signal }) => postOrigin(rollupSections, rangeBody, signal),
  })
  const rollup = rollupSections.length > 0 ? readRollupMeta(rollupQuery.data) : undefined

  const { isLoading, isFetching, error } = bundle
  const summary = React.useMemo(() => ({ data: bundle.data?.summary, isLoading, isFetching, error }), [bundle.data?.summary, isLoading, isFetching, error])
  const tsSource = rollupSections.includes('timeseries') ? rollupQuery : bundle
  const tsData = tsSource.data?.timeseries
  const tsApprox = approxMark(rollup, 'timeseries.value') !== ''
  const originTs = React.useMemo(() => ({
    data: tsApprox && tsData
      ? { ...tsData, _approx: true, _approx_note: 'Percentiles are estimated from summary histograms (≈). Miss counts are exact.' }
      : tsData,
    isLoading: tsSource.isLoading, isFetching: tsSource.isFetching, error: tsSource.error,
  }), [tsData, tsApprox, tsSource.isLoading, tsSource.isFetching, tsSource.error])
  const slowUrls = React.useMemo(() => ({ data: bundle.data?.slow_urls, isLoading, isFetching, error }), [bundle.data?.slow_urls, isLoading, isFetching, error])
  const statusCodes = React.useMemo(() => ({ data: bundle.data?.status_codes, isLoading, isFetching, error }), [bundle.data?.status_codes, isLoading, isFetching, error])
  const popLatency = React.useMemo(() => ({ data: bundle.data?.pop_latency, isLoading, isFetching, error }), [bundle.data?.pop_latency, isLoading, isFetching, error])
  const ipSource = rollupSections.includes('ip_health') ? rollupQuery : bundle
  const ipHealth = React.useMemo(
    () => ({ data: ipSource.data?.ip_health, isLoading: ipSource.isLoading, isFetching: ipSource.isFetching, error: ipSource.error }),
    [ipSource.data?.ip_health, ipSource.isLoading, ipSource.isFetching, ipSource.error],
  )

  const baseOriginTraces = useTimeseriesToTraces(
    originTs.data?.series,
    React.useMemo(() => [{ key: 'value', name: originMetric === 'ttfb' ? 'Origin TTFB' : 'Origin TTLB', color: CHART_COLORS.danger, fill: 'tozeroy' }], [originMetric]),
    timezone,
  )

  const originSeries = originTs.data?.series
  const originTsChartData = React.useMemo(() => {
    const traces: PlotlyTrace[] = baseOriginTraces.map((t) => ({ ...t, mode: 'lines+markers', marker: { size: 4, color: t.line?.color } }))
    if (trend !== 'off' && originSeries?.length) {
      const xValues = originSeries.map((d) => formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss'))
      const yValues = originSeries.map((d) => Number(d.value) || 0)
      const n = yValues.length
      const windowSize =
        trend === 'auto'
          ? Math.floor(n / (n > 1000 ? 20 : n > 100 ? 10 : 5))
          : Math.floor((TREND_MAP[trend] ?? 0) / (INTERVAL_SECONDS[config.effectiveInterval as keyof typeof INTERVAL_SECONDS] ?? 60))

      if (windowSize > 1) {
        const trendY = new Array(n).fill(null)
        for (let i = windowSize - 1; i < n; i++) {
          let sum = 0, count = 0
          for (let j = 0; j < windowSize; j++) {
            const v = yValues[i - j]
            if (v != null) { sum += v; count++ }
          }
          trendY[i] = count > 0 ? sum / count : null
        }
        traces.push({
          x: xValues,
          y: trendY,
          type: 'scatter',
          mode: 'lines',
          name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
          line: { color: '#f97316', width: 3 },
        })
      }
    }
    return traces
  }, [baseOriginTraces, originSeries, timezone, trend, config.effectiveInterval])

  const statusRows = statusCodes.data?.rows
  const statusData = React.useMemo((): PlotlyTrace[] => {
    if (!statusRows?.length) return []
    return [{
      values: statusRows.map((r) => r.count),
      labels: statusRows.map((r) => (r.status === -1 ? 'Other' : `HTTP ${r.status}`)),
      type: 'pie',
      hole: 0.4,
      marker: { colors: statusRows.map((r) => (r.status === -1 ? CHART_COLORS.neutral : statusHex(r.status))) },
    }]
  }, [statusRows])

  if (summary.data?.has_data === false) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <div className="bg-muted h-16 w-16 rounded-full flex items-center justify-center mb-4">
          <Server className="h-8 w-8 text-muted-foreground" />
        </div>
        {isFieldActive('ottfb') ? (
          <>
            <h3 className="text-lg font-bold">No Origin Metrics Yet</h3>
            <p className="text-muted-foreground max-w-md mt-2">
              No origin/shield requests in this time range. Origin metrics only populate on cache MISS/PASS.
            </p>
          </>
        ) : (
          <>
            <h3 className="text-lg font-bold">No Origin Metrics Found</h3>
            <p className="text-muted-foreground max-w-md mt-2">
              Origin metrics require Group L (Origin Metrics) to be enabled in your log settings.
              If recently enabled, it may take a few minutes for data to appear.
            </p>
          </>
        )}
      </div>
    )
  }

  return (
    <>
      <RollupDataThroughSlot reserve={rollupSections.length > 0} meta={rollup} className="mb-4" />
      <Aggregates summary={summary} />
      <Timeseries
        originTs={originTs}
        originTsChartData={originTsChartData}
        statusCodes={statusCodes}
        statusData={statusData}
        originMetric={originMetric}
        setOriginMetric={setOriginMetric}
        originPercentile={originPercentile}
        setOriginPercentile={setOriginPercentile}
        trend={trend}
        setTrend={setTrend}
        config={config}
        intervalButtons={intervalButtons}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />
      <LatencyHeatmap
        slowUrls={slowUrls}
        popLatency={popLatency}
        ipHealth={ipHealth}
        ipRollup={rollup}
        summary={summary}
        urlVisibility={urlVisibility}
        setUrlVisibility={setUrlVisibility}
        onUrlVisChange={onUrlVisChange}
        popVisibility={popVisibility}
        setPopVisibility={setPopVisibility}
        onPopVisChange={onPopVisChange}
        ipVisibility={ipVisibility}
        setIpVisibility={setIpVisibility}
        onIpVisChange={onIpVisChange}
      />
    </>
  )
}

export default function OriginClient({
  nowServerStr,
}: { nowServerStr?: string; params?: unknown; searchParams?: unknown } = {}) {
  const { relativeRange, isAutoRange, anchor } = useReportPageState(nowServerStr)
  return (
    <ReportLayout
      title="Origin Performance"
      description="Real-time backend health, fetch timing, and error analysis."
      icon={Server}
      defaultInterval="1 minute"
    >
      {(props) => (
        <OriginReportContent
          {...props}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}
