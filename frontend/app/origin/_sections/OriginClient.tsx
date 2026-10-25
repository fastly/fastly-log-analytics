'use client'

import { useTimeseriesToTraces } from '@/lib/chart-helpers'
import React from 'react'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import type { components } from '@/types/api'
import type { PlotlyTrace } from '@/types/plotly'
import type { RangeWireArgs } from '@/lib/utils'
import type { ReportLayoutRenderProps } from '@/components/ReportLayout'
import { useAnalyticsReportQuery } from '@/hooks/useDashboardCards'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { Server, Activity } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { INTERVAL_SECONDS, TRENDS } from '@/lib/utils'
import { formatDate } from '@/lib/date'
import { LatencyHeatmap } from './LatencyHeatmap'
import { CHART_COLORS, statusHex } from '@/lib/chart-helpers'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { formatCount } from '@/lib/format'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { makeTimeXAxis } from '@/lib/chart-helpers'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'

// P-4 slice 4: section-selector mirroring /security, /network, /dashboard,
// /performance. The origin page renders every section, so the list is the
// constant full set — declared explicitly so the backend's _expand_sections
// gets the standardized selector contract and a future feature flag can
// drop a section without an API change. No FE call-split: the shared
// parquet-scan + lat_us materialization on the backend is the floor cost
// of the request, and splitting across HTTP requests would re-pay it per
// call (see commit 9007f9d's 4-branch intra-request fan-out).
//
// MUST stay in lockstep with ORIGIN_SSR_SECTIONS in lib/ssr/origin.ts — the
// section list is part of the SSR-seed query key, so a divergence here would
// miss the dehydrated cache and double-fetch on first paint.
const ORIGIN_SECTIONS: NonNullable<components['schemas']['OriginAggregatesRequest']['sections']> = [
  'summary',
  'timeseries',
  'slow_urls',
  'status_codes',
  'path_breakdown',
  'pop_latency',
  'ip_health',
]

type OriginAggregates = components['schemas']['OriginAggregatesResponse']

export type OriginSummary = OriginAggregates['summary']
export type OriginTimeseries = OriginAggregates['timeseries']
export type OriginSlowUrls = OriginAggregates['slow_urls']
export type OriginStatusCodes = OriginAggregates['status_codes']
export type OriginPopLatency = OriginAggregates['pop_latency']
export type OriginIpHealth = OriginAggregates['ip_health']

export type OriginSlowUrlRow = components['schemas']['OriginSlowUrlRow']
export type OriginPopLatencyRow = components['schemas']['OriginPopLatencyRow']
export type OriginIpHealthRow = components['schemas']['OriginIpHealthRow']

/** One section of the origin composite plus the bundle query's loading state. */
export interface SectionQuery<T> {
  data: T | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
}

/**
 * Small "Approximate" pill shown on origin panels whose wide-window
 * (>= 48h, unfiltered) values are served from per-hour rollups. Across
 * hours the percentiles are request-weighted averages of per-hour
 * percentiles rather than an exact cross-window MEDIAN/APPROX_QUANTILE,
 * so the backend sets ``_approx: true`` on those section payloads. Counts
 * (request volume, error rate) stay exact. Markup mirrors the original
 * inline badge on the summary card so every origin panel reads the same.
 */
export function ApproxBadge({ message }: { message: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger render={<span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help" />}>
          <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
          Approximate
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-[260px] text-xs">
          {message}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function Aggregates({ summary }: { summary: SectionQuery<OriginSummary> }) {
  const error = summary.error as AnalyticsCardError | null
  const isApprox = summary.data?._approx === true
  return (
    <div className="mb-6">
      {isApprox && (
        <div className="flex justify-end mb-2">
          <ApproxBadge message={summary.data?._approx_note ?? "Origin TTFB percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Error rate and fetch volume are exact."} />
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <AnalyticsCard
        title="Origin TTFB (P50)"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin TTFB (P50)..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>Median time taken by your backend to start returning a response after Fastly forwards a request. Lower is better.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">{summary.data?.ottfb_p50_ms?.toFixed(1)}ms</div>
          <div className="text-xs text-muted-foreground mt-1">Median backend response time</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Origin TTFB (P95)"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin TTFB (P95)..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>The 95th percentile of backend response times. Indicates the tail latency experienced by the slowest 5% of requests.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">{summary.data?.ottfb_p95_ms?.toFixed(1)}ms</div>
          <div className="text-xs text-muted-foreground mt-1">Tail latency (95th percentile)</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Origin Error Rate"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin Error Rate..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>Percentage of cache miss/pass requests where the backend returned a 5xx HTTP status code.</p>}
      >
        <div className="flex flex-col">
          <div className={cn("text-3xl font-bold", (summary.data?.origin_error_rate || 0) > 0.01 ? "text-destructive" : "")}>
            {((summary.data?.origin_error_rate || 0) * 100).toFixed(2)}%
          </div>
          <div className="text-xs text-muted-foreground mt-1">Percentage of 5xx responses</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Fetch Volume"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Fetch Volume..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>The total number of requests sent to the backend (cache misses and passes) during this time window.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">
            {formatCount((summary.data?.total_misses || 0) + (summary.data?.total_passes || 0))}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Total cache misses & passes</div>
        </div>
      </AnalyticsCard>
      </div>
    </div>
  )
}

type OriginMetric = 'ttfb' | 'ttlb'
type OriginPercentile = 'p50' | 'p95' | 'p99'

interface TimeseriesProps {
  originTs: SectionQuery<OriginTimeseries>
  originTsChartData: PlotlyTrace[]
  statusCodes: SectionQuery<OriginStatusCodes>
  statusData: PlotlyTrace[]
  originMetric: OriginMetric
  setOriginMetric: (metric: OriginMetric) => void
  originPercentile: OriginPercentile
  setOriginPercentile: (percentile: OriginPercentile) => void
  trend: string
  setTrend: (trend: string) => void
  config: ReportConfiguration
  intervalButtons: React.ReactNode
  startTime: string | null
  endTime: string | null
  timezone: string
}

export function Timeseries({
  originTs,
  originTsChartData,
  statusCodes,
  statusData,
  originMetric,
  setOriginMetric,
  originPercentile,
  setOriginPercentile,
  trend,
  setTrend,
  config,
  intervalButtons,
  startTime,
  endTime,
  timezone,
}: TimeseriesProps) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
      <AnalyticsCard
        title="Origin Latency"
        icon={<Activity className="h-4 w-4" />}
        className="lg:col-span-2 h-[400px] [contain-intrinsic-size:400px] [content-visibility:auto]"
        isLoading={originTs.isLoading}
        isFetching={originTs.isFetching}
        loadingMessage="Loading origin latency timeseries..."
        error={originTs.error as AnalyticsCardError | null}
        helpContent={<p>Time to First Byte (TTFB) measures the time to receive the first byte of the response headers from the origin. Time to Last Byte (TTLB) measures the time to receive the full response body.</p>}
        headerAction={
          <div className="flex items-center gap-2">
            {originTs.data?._approx === true && (
              <ApproxBadge message={originTs.data?._approx_note ?? "Latency percentiles on wide windows are request-weighted averages of per-minute percentiles. Request volume is exact."} />
            )}
            <ButtonGroup>
              {(['ttfb', 'ttlb'] as const).map(m => (
                <Button
                  key={m}
                  variant={originMetric === m ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginMetric(m))}
                  className={cn(
                    "h-9 text-xs px-2 shadow-none transition-colors uppercase sm:h-7 sm:text-[11px]",
                    originMetric === m ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                  )}
                >
                  {m}
                </Button>
              ))}
            </ButtonGroup>
            <ButtonGroup>
              {(['p50', 'p95', 'p99'] as const).map(p => (
                <Button
                  key={p}
                  variant={originPercentile === p ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginPercentile(p))}
                  className={cn(
                    "h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]",
                    originPercentile === p ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                  )}
                >
                  {p}
                </Button>
              ))}
            </ButtonGroup>
            <div className="ml-2">
              {intervalButtons}
            </div>
          </div>
        }
      >
        {originTs.isLoading || (originTs.isFetching && originTsChartData.length === 0) ? (
          <div className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
            <span className="text-muted-foreground text-sm animate-pulse">Crunching logs...</span>
          </div>
        ) : originTsChartData.length === 0 ? (
          <div className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
            <div className="flex flex-col items-center text-muted-foreground">
              <span className="text-sm font-medium">No data available</span>
              <span className="text-xs mt-1">No origin timing data found for this period.</span>
            </div>
          </div>
        ) : (
          <div className="flex flex-col h-full">
            <div className="relative flex-1 mb-4">
              <PlotlyChart
                data={originTsChartData}
                layout={{
                  hovermode: 'x unified',
                  yaxis: { title: 'ms', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
                  xaxis: makeTimeXAxis(startTime, endTime, timezone),
                }}
                height="100%"
              />
            </div>
            <div className="mt-auto pt-2 border-t flex items-center gap-2 relative z-10">
              <span className="text-[10px] uppercase font-bold text-muted-foreground">Trend:</span>
              <ButtonGroup className="bg-muted/50 p-1">
                {TRENDS.map(t => (
                  <Button
                    key={t.value}
                    variant={trend === t.value ? 'secondary' : 'ghost'}
                    size="sm"
                    onClick={() => React.startTransition(() => setTrend(t.value))}
                    disabled={!config.validTrends.has(t.value)}
                    className="h-9 text-xs px-2 shadow-none disabled:opacity-30 sm:h-7 sm:text-[11px]"
                  >
                    {t.label}
                  </Button>
                ))}
              </ButtonGroup>
            </div>
          </div>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Status Code Distribution"
        icon={<Activity className="h-4 w-4" />}
        isLoading={statusCodes.isLoading}
        isFetching={statusCodes.isFetching}
        loadingMessage="Loading origin status code breakdown..."
        error={statusCodes.error as AnalyticsCardError | null}
        isEmpty={!statusData?.length}
        className="h-[400px] [contain-intrinsic-size:400px] [content-visibility:auto]"
        contentClassName="p-2"
        helpContent={<p>A breakdown of the HTTP status codes returned directly by your backend servers during the selected time period.</p>}
      >
        <PlotlyChart
          data={statusData}
          height="100%"
        />
      </AnalyticsCard>
    </div>
  )
}

type OriginReportContentProps = ReportLayoutRenderProps &
  Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'>

function OriginReportContent({
  startTime,
  endTime,
  timezone,
  activeServiceId,
  filterPayload,
  config,
  trend,
  setTrend,
  intervalButtons,
  relativeRange,
  isAutoRange,
  anchor,
}: OriginReportContentProps) {
  const [originMetric, setOriginMetric] = React.useState<'ttfb' | 'ttlb'>('ttfb')
  const [originPercentile, setOriginPercentile] = React.useState<'p50' | 'p95' | 'p99'>('p95')

  // Distinguish "Group L not enabled" from "enabled but no origin requests in
  // this window yet" so a fresh/low-traffic service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()

  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [popVisibility, setPopVisibility, onPopVisChange] = useColumnVisibility()
  const [ipVisibility, setIpVisibility, onIpVisChange] = useColumnVisibility()

  // Composite endpoint: one parquet scan → one shared TEMP TABLE → six
  // sub-aggregations. Backend at /api/origin/aggregates. The granular
  // /api/origin/{summary,timeseries,slow-urls,status-codes,pop-latency,
  // ip-health} endpoints still exist on the server for rollback safety
  // but should no longer fire from this page. Per-card pseudo-query
  // objects below preserve the {data, isLoading, isFetching} shape the
  // existing section components consume so the migration is invisible
  // to <Aggregates>/<Timeseries>/<LatencyHeatmap>.
  // ChartInterval (backend/models/metrics.py) only emits these 4 values;
  // any other key here would be dead.
  const intervalMap: Record<string, number> = {
    "1 second": 1 / 60,
    "1 minute": 1,
    "1 hour": 60,
    "1 day": 1440,
  }
  const bucketMinutes = intervalMap[config.effectiveInterval] || 5

  // SSR-seed key contract (lib/ssr/origin.ts + app/origin/page.tsx): in token
  // mode the first-paint key keys on the SERVER-REPRODUCIBLE (rangeKey, anchor)
  // pair instead of the client-now()-anchored start/end, so the seed in page.tsx
  // byte-matches and the cards paint from cache. On a cold load rangeKey is
  // "24h" (resolveRangeWire's token default). A custom absolute range instead
  // keys on "abs:<start>|<end>" and sends the explicit bounds, so it scans
  // exactly what the chart x-axis (hard-clamped to startTime/endTime) displays.
  const bundle = useAnalyticsReportQuery<OriginAggregates | undefined>({
    namespace: ['origin', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [bucketMinutes, originMetric, originPercentile, ORIGIN_SECTIONS],
    queryFn: async ({ rangeBody, signal }) => {
      return unwrapClient(
        client.POST('/api/origin/aggregates', {
          signal,
          body: {
            // Token mode → {range_token, anchor} (server resolves the window).
            // Custom mode → {start_time, end_time} (backend falls back to these
            // when range_token is absent).
            filters: filterPayload,
            bucket_minutes: bucketMinutes,
            split_by_leg: false,
            timeseries_metric: originMetric,
            timeseries_percentile: originPercentile,
            slow_urls_limit: 20,
            // URLs with <50 reqs in the window almost never make the
            // top-20 by p95 — a single outlier dominates and the row
            // isn't statistically meaningful. Raising the HAVING from
            // 10 to 50 cuts URL-cardinality 60-80 % (Zipfian) on the
            // GROUP BY + APPROX_QUANTILE sorts; slow_urls section
            // section on prod-tunnel-admin/7d drops ~1.3 s of the
            // current ~2.2 s max.
            slow_urls_min_requests: 50,
            pop_latency_limit: 30,
            ip_health_limit: 30,
            sections: ORIGIN_SECTIONS,
            ...rangeBody,
          },
        }),
        'Failed to load origin aggregates',
      )
    },
  })

  // useMemo so identity stays stable across re-renders for the same bundle
  // tick; the section components are dumb consumers and re-renders fan out
  // through the existing isLoading/isFetching propagation.
  const isLoading = bundle.isLoading
  const isFetching = bundle.isFetching
  const error = bundle.error
  const summary = React.useMemo(() => ({ data: bundle.data?.summary, isLoading, isFetching, error }), [bundle.data?.summary, isLoading, isFetching, error])
  const originTs = React.useMemo(() => ({ data: bundle.data?.timeseries, isLoading, isFetching, error }), [bundle.data?.timeseries, isLoading, isFetching, error])
  const slowUrls = React.useMemo(() => ({ data: bundle.data?.slow_urls, isLoading, isFetching, error }), [bundle.data?.slow_urls, isLoading, isFetching, error])
  const statusCodes = React.useMemo(() => ({ data: bundle.data?.status_codes, isLoading, isFetching, error }), [bundle.data?.status_codes, isLoading, isFetching, error])
  const popLatency = React.useMemo(() => ({ data: bundle.data?.pop_latency, isLoading, isFetching, error }), [bundle.data?.pop_latency, isLoading, isFetching, error])
  const ipHealth = React.useMemo(() => ({ data: bundle.data?.ip_health, isLoading, isFetching, error }), [bundle.data?.ip_health, isLoading, isFetching, error])

  const baseOriginTraces = useTimeseriesToTraces(originTs.data?.series, React.useMemo(() => [
    { key: 'value', name: originMetric === 'ttfb' ? 'Origin TTFB' : 'Origin TTLB', color: CHART_COLORS.danger, fill: 'tozeroy' }
  ], [originMetric]), timezone)

  const originSeries = originTs.data?.series
  const originTsChartData = React.useMemo(() => {
    // Clone to avoid mutating the hook's cached array
    const traces: PlotlyTrace[] = baseOriginTraces.map(t => ({
      ...t,
      mode: 'lines+markers',
      marker: { size: 4, color: t.line?.color }
    }))

    if (trend !== 'off' && originSeries?.length) {
      const time_series = originSeries
      const xValues = time_series.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
      const yValues = time_series.map((d) => Number(d.value) || 0)
      const n = yValues.length
      let windowSize = 0
      if (trend === 'auto') {
        if (n > 1000) windowSize = Math.floor(n / 20)
        else if (n > 100) windowSize = Math.floor(n / 10)
        else windowSize = Math.floor(n / 5)
      } else {
        const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
        const actualInterval = config.effectiveInterval
        windowSize = Math.floor((trendMap[trend] ?? 0) / (INTERVAL_SECONDS[actualInterval as keyof typeof INTERVAL_SECONDS] ?? 60))
      }

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
          line: { color: '#f97316', width: 3 }
        })
      }
    }

    return traces
  }, [baseOriginTraces, originSeries, timezone, trend, config.effectiveInterval])

  const statusRows = statusCodes.data?.rows
  const statusData = React.useMemo((): PlotlyTrace[] => {
    if (!statusRows?.length) return []
    // N-8: backend bucketizes any status outside 100-599 as -1; map to a
    // single "Other" slice so the donut doesn't fabricate plausible-looking
    // status codes like "HTTP 829" from synthetic / corrupt origin values.
    return [{
      values: statusRows.map((r) => r.count),
      labels: statusRows.map((r) => r.status === -1 ? 'Other' : `HTTP ${r.status}`),
      type: 'pie',
      hole: 0.4,
      marker: {
        colors: statusRows.map((r) =>
          r.status === -1 ? CHART_COLORS.neutral : statusHex(r.status)
        )
      }
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

export default function OriginClient({ nowServerStr }: { nowServerStr?: string }) {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Origin Performance"
      description="Real-time backend health, fetch timing, and error analysis."
      icon={Server}
      defaultInterval="1 minute"
    >
      {(props) => <OriginReportContent {...props} relativeRange={relativeRange} isAutoRange={isAutoRange} anchor={anchor} />}
    </ReportLayout>
  )
}
