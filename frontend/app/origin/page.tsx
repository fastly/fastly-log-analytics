'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { Server, Activity, MapPin, Globe } from 'lucide-react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import { client, unwrapClient } from '@/lib/api'
import type { components, PlotlyTrace } from '@/types/api'
import { INTERVAL_SECONDS, TRENDS, cn, formatBytes, formatCount, formatDate, CHART_COLORS, statusHex, makeTimeXAxis, useTimeseriesToTraces, type RangeWireArgs } from "@/lib/utils";
import { useAnalyticsReportQuery, useReportRangeAnchor, type ReportConfiguration } from '@/hooks/useDashboardCards'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { ReportLayout, type ReportLayoutRenderProps } from '@/components/ReportLayout'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown, FilterValueCell, PopLabel, columnHeader, msCell } from '@/components/DataTable'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'

// Must stay in lockstep with ORIGIN_SSR_SECTIONS in lib/ssr/origin.ts
const ORIGIN_SECTIONS: NonNullable<components['schemas']['OriginAggregatesRequest']['sections']> = [
  'summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health',
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

export interface SectionQuery<T> {
  data: T | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
}

export function ApproxBadge({ message }: { message: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger render={<span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help" />}>
          <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
          Approximate
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-[260px] text-xs">{message}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function Aggregates({ summary }: { summary: SectionQuery<OriginSummary> }) {
  const error = summary.error as AnalyticsCardError | null
  const d = summary.data
  const kpis = [
    {
      title: 'Origin TTFB (P50)',
      help: 'Median time taken by your backend to start returning a response after Fastly forwards a request. Lower is better.',
      value: `${d?.ottfb_p50_ms?.toFixed(1)}ms`,
      sub: 'Median backend response time',
      danger: false,
    },
    {
      title: 'Origin TTFB (P95)',
      help: 'The 95th percentile of backend response times. Indicates the tail latency experienced by the slowest 5% of requests.',
      value: `${d?.ottfb_p95_ms?.toFixed(1)}ms`,
      sub: 'Tail latency (95th percentile)',
      danger: false,
    },
    {
      title: 'Origin Error Rate',
      help: 'Percentage of cache miss/pass requests where the backend returned a 5xx HTTP status code.',
      value: `${((d?.origin_error_rate || 0) * 100).toFixed(2)}%`,
      sub: 'Percentage of 5xx responses',
      danger: (d?.origin_error_rate || 0) > 0.01,
    },
    {
      title: 'Fetch Volume',
      help: 'The total number of requests sent to the backend (cache misses and passes) during this time window.',
      value: formatCount((d?.total_misses || 0) + (d?.total_passes || 0)),
      sub: 'Total cache misses & passes',
      danger: false,
    },
  ]

  return (
    <div className="mb-6">
      {d?._approx === true && (
        <div className="flex justify-end mb-2">
          <ApproxBadge message={d?._approx_note ?? 'Origin TTFB percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Error rate and fetch volume are exact.'} />
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {kpis.map((kpi) => (
          <AnalyticsCard
            key={kpi.title}
            title={kpi.title}
            isLoading={summary.isLoading}
            isFetching={summary.isFetching}
            loadingMessage={`Loading ${kpi.title}...`}
            error={error}
            className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
            helpContent={<p>{kpi.help}</p>}
          >
            <div className="flex flex-col">
              <div className={cn('text-3xl font-bold', kpi.danger ? 'text-destructive' : '')}>{kpi.value}</div>
              <div className="text-xs text-muted-foreground mt-1">{kpi.sub}</div>
            </div>
          </AnalyticsCard>
        ))}
      </div>
    </div>
  )
}

const ORIGIN_METRICS = ['ttfb', 'ttlb'] as const
const ORIGIN_PERCENTILES = ['p50', 'p95', 'p99'] as const
type OriginMetric = (typeof ORIGIN_METRICS)[number]
type OriginPercentile = (typeof ORIGIN_PERCENTILES)[number]

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
  originTs, originTsChartData, statusCodes, statusData, originMetric, setOriginMetric,
  originPercentile, setOriginPercentile, trend, setTrend, config, intervalButtons, startTime, endTime, timezone,
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
              <ApproxBadge message={originTs.data?._approx_note ?? 'Latency percentiles on wide windows are request-weighted averages of per-minute percentiles. Request volume is exact.'} />
            )}
            <ButtonGroup>
              {(['ttfb', 'ttlb'] as const).map((m) => (
                <Button
                  key={m}
                  variant={originMetric === m ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginMetric(m))}
                  className={cn(
                    'h-9 text-xs px-2 shadow-none transition-colors uppercase sm:h-7 sm:text-[11px]',
                    originMetric === m ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'hover:text-primary hover:bg-muted',
                  )}
                >
                  {m}
                </Button>
              ))}
            </ButtonGroup>
            <ButtonGroup>
              {(['p50', 'p95', 'p99'] as const).map((p) => (
                <Button
                  key={p}
                  variant={originPercentile === p ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginPercentile(p))}
                  className={cn(
                    'h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]',
                    originPercentile === p ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'hover:text-primary hover:bg-muted',
                  )}
                >
                  {p}
                </Button>
              ))}
            </ButtonGroup>
            <div className="ml-2">{intervalButtons}</div>
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
                {TRENDS.map((t) => (
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
        <PlotlyChart data={statusData} height="100%" />
      </AnalyticsCard>
    </div>
  )
}

type OriginReportContentProps = ReportLayoutRenderProps & Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'>

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

  const bundle = useAnalyticsReportQuery<OriginAggregates | undefined>({
    namespace: ['origin', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [bucketMinutes, originMetric, originPercentile, ORIGIN_SECTIONS],
    queryFn: async ({ rangeBody, signal }) =>
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
            sections: ORIGIN_SECTIONS,
            ...rangeBody,
          },
        }),
        'Failed to load origin aggregates',
      ),
  })

  const { isLoading, isFetching, error } = bundle
  const summary = React.useMemo(() => ({ data: bundle.data?.summary, isLoading, isFetching, error }), [bundle.data?.summary, isLoading, isFetching, error])
  const originTs = React.useMemo(() => ({ data: bundle.data?.timeseries, isLoading, isFetching, error }), [bundle.data?.timeseries, isLoading, isFetching, error])
  const slowUrls = React.useMemo(() => ({ data: bundle.data?.slow_urls, isLoading, isFetching, error }), [bundle.data?.slow_urls, isLoading, isFetching, error])
  const statusCodes = React.useMemo(() => ({ data: bundle.data?.status_codes, isLoading, isFetching, error }), [bundle.data?.status_codes, isLoading, isFetching, error])
  const popLatency = React.useMemo(() => ({ data: bundle.data?.pop_latency, isLoading, isFetching, error }), [bundle.data?.pop_latency, isLoading, isFetching, error])
  const ipHealth = React.useMemo(() => ({ data: bundle.data?.ip_health, isLoading, isFetching, error }), [bundle.data?.ip_health, isLoading, isFetching, error])

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

export default function OriginClient({ nowServerStr }: { nowServerStr?: string; params?: unknown; searchParams?: unknown } = {}) {
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

type LatencyRow = { p50_ms?: number | null; p95_ms?: number | null; requests: number }

const requestsCol = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: columnHeader('Reqs'),
  cell: ({ row }) => formatCount(row.original.requests),
})
const p50Col = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: columnHeader('P50'),
  cell: ({ row }) => msCell(row.original.p50_ms),
})
const p95Col = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: columnHeader('P95'),
  cell: ({ row }) => msCell(row.original.p95_ms),
})

const URL_COLUMNS: ColumnDef<OriginSlowUrlRow>[] = [
  {
    accessorKey: 'url', id: 'url', meta: { label: 'URL' }, header: columnHeader('URL'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'url', value: row.original.url }]} className="font-mono text-xs" containerClassName="max-w-[400px]" />,
  },
  requestsCol(),
  p50Col(),
  p95Col(),
  { accessorKey: 'p99_ms', id: 'p99_ms', meta: { label: 'P99 Latency' }, header: columnHeader('P99'), cell: ({ row }) => msCell(row.original.p99_ms) },
]

const POP_COLUMNS: ColumnDef<OriginPopLatencyRow>[] = [
  {
    accessorKey: 'pop', id: 'pop', meta: { label: 'POP' }, header: columnHeader('POP'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'pop', value: row.original.pop }]} display={<PopLabel code={row.original.pop} />} className="font-bold" />,
  },
  requestsCol(),
  p50Col(),
  {
    accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: columnHeader('P95'),
    cell: ({ row }) => <span className={cn(row.original.elevated ? 'text-destructive font-bold' : '')}>{row.original.p95_ms?.toFixed(1)}ms</span>,
  },
]

const IP_COLUMNS: ColumnDef<OriginIpHealthRow>[] = [
  {
    accessorKey: 'oip', id: 'oip', meta: { label: 'Origin IP' }, header: columnHeader('Origin IP'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'oip', value: row.original.oip }]} className="font-mono text-xs" />,
  },
  requestsCol(),
  p50Col(),
  p95Col(),
  {
    accessorKey: 'error_pct', id: 'error_pct', meta: { label: '5xx Errors %' }, header: columnHeader('5xx %'),
    cell: ({ row }) => <span className={cn((row.original.error_pct ?? 0) > 1 ? 'text-destructive font-bold' : '')}>{row.original.error_pct}%</span>,
  },
]

const COLUMN_LABELS: Record<string, string> = {
  url: 'URL', pop: 'POP', oip: 'Origin IP', requests: 'Requests',
  p50_ms: 'Median (P50)', p95_ms: 'P95 Latency', p99_ms: 'P99 Latency', error_pct: 'Error Rate %',
}
const getLabels = (ids: string[]) => ids.map((id) => ({ id, label: COLUMN_LABELS[id] || id }))

interface LatencyHeatmapProps {
  slowUrls: SectionQuery<OriginSlowUrls>
  popLatency: SectionQuery<OriginPopLatency>
  ipHealth: SectionQuery<OriginIpHealth>
  summary: SectionQuery<OriginSummary>
  urlVisibility: VisibilityState
  setUrlVisibility: (visibility: VisibilityState) => void
  onUrlVisChange: (id: string, visible: boolean) => void
  popVisibility: VisibilityState
  setPopVisibility: (visibility: VisibilityState) => void
  onPopVisChange: (id: string, visible: boolean) => void
  ipVisibility: VisibilityState
  setIpVisibility: (visibility: VisibilityState) => void
  onIpVisChange: (id: string, visible: boolean) => void
}

export function LatencyHeatmap({
  slowUrls, popLatency, ipHealth, summary,
  urlVisibility, setUrlVisibility, onUrlVisChange,
  popVisibility, setPopVisibility, onPopVisChange,
  ipVisibility, setIpVisibility, onIpVisChange,
}: LatencyHeatmapProps) {
  const { isFieldActive } = useActiveLogFields()

  const renderTableCard = <TRow,>(cfg: {
    title: string
    icon: React.ReactNode
    query: {
      data?: { rows?: TRow[]; _approx?: boolean | null; _approx_note?: string | null }
      isLoading: boolean
      isFetching: boolean
      error: unknown
    }
    loadingMessage: string
    help: string
    defaultApproxNote: string
    columnIds: string[]
    visibility: VisibilityState
    setVisibility: (v: VisibilityState) => void
    onVisChange: (id: string, visible: boolean) => void
    columns: ColumnDef<TRow>[]
    emptyMessage: string
  }) => (
    <AnalyticsCard
      title={cfg.title}
      icon={cfg.icon}
      isLoading={cfg.query.isLoading}
      isFetching={cfg.query.isFetching}
      loadingMessage={cfg.loadingMessage}
      error={cfg.query.error as AnalyticsCardError | null}
      className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
      contentClassName="p-0"
      helpContent={<p>{cfg.help}</p>}
      headerAction={
        <div className="flex items-center gap-2">
          {cfg.query.data?._approx === true && <ApproxBadge message={cfg.query.data?._approx_note ?? cfg.defaultApproxNote} />}
          <ColumnVisibilityDropdown columns={getLabels(cfg.columnIds)} visibility={cfg.visibility} onChange={cfg.onVisChange} />
        </div>
      }
    >
      <DataTable
        columns={cfg.columns}
        data={cfg.query.data?.rows || []}
        emptyMessage={cfg.query.isLoading ? '' : cfg.emptyMessage}
        hideToolbar
        columnVisibility={cfg.visibility}
        onColumnVisibilityChange={cfg.setVisibility}
      />
    </AnalyticsCard>
  )

  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {renderTableCard({
          title: 'Slowest URLs at Origin',
          icon: <Server className="h-4 w-4" />,
          query: slowUrls,
          loadingMessage: 'Loading slowest URLs at origin...',
          help: 'A list of specific URLs that take the longest time to fetch from the origin.',
          defaultApproxNote: 'P50/P95/P99 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact.',
          columnIds: ['url', 'requests', 'p50_ms', 'p95_ms', 'p99_ms'],
          visibility: urlVisibility,
          setVisibility: setUrlVisibility,
          onVisChange: onUrlVisChange,
          columns: URL_COLUMNS,
          emptyMessage: isFieldActive('ottfb') ? 'No slow URLs in this time range.' : 'Requires Origin Metrics (Group L) fields to be enabled.',
        })}
        {renderTableCard({
          title: 'Origin Performance by POP',
          icon: <MapPin className="h-4 w-4" />,
          query: popLatency,
          loadingMessage: 'Loading origin performance by POP...',
          help: 'Backend latency aggregated by Fastly POP location.',
          defaultApproxNote: 'P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact.',
          columnIds: ['pop', 'requests', 'p50_ms', 'p95_ms'],
          visibility: popVisibility,
          setVisibility: setPopVisibility,
          onVisChange: onPopVisChange,
          columns: POP_COLUMNS,
          emptyMessage: isFieldActive('ottfb') && isFieldActive('pop')
            ? 'No POP latency data in this time range.'
            : 'Requires Origin Metrics (Group L) and Infrastructure (Group C) fields to be enabled.',
        })}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {renderTableCard({
          title: 'Origin IP Health',
          icon: <Globe className="h-4 w-4" />,
          query: ipHealth,
          loadingMessage: 'Loading origin IP health...',
          help: 'Latency and error rates for individual backend IP addresses.',
          defaultApproxNote: 'P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts and the 5xx error rate are exact.',
          columnIds: ['oip', 'requests', 'p50_ms', 'p95_ms', 'error_pct'],
          visibility: ipVisibility,
          setVisibility: setIpVisibility,
          onVisChange: onIpVisChange,
          columns: IP_COLUMNS,
          emptyMessage: isFieldActive('oip') ? 'No origin IP data in this time range.' : 'Requires Origin Metrics (Group L) fields to be enabled.',
        })}

        <AnalyticsCard
          title="Origin Payload Size"
          icon={<Globe className="h-4 w-4" />}
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          loadingMessage="Loading origin payload size..."
          error={summary.error as AnalyticsCardError | null}
          className="min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]"
          helpContent={<p>The median size of the response body transferred from the origin to Fastly.</p>}
        >
          <div className="flex flex-col items-center justify-center py-4 text-center">
            <div className="text-2xl font-bold mb-1">
              {summary.data?.obytes_p50 != null ? formatBytes(summary.data.obytes_p50) : 'N/A'}
            </div>
            <div className="text-xs text-muted-foreground">Median Response Size (obytes)</div>
            <div className="w-full h-2 bg-muted rounded-full mt-4 overflow-hidden flex">
              <div
                className="bg-primary h-full transition-all"
                style={{ width: `${Math.min(100, ((summary.data?.ottfb_p50_ms || 0) / (summary.data?.ottlb_p50_ms || 1)) * 100)}%` }}
              />
            </div>
            <div className="flex justify-between w-full mt-1 text-[10px] uppercase font-bold text-muted-foreground">
              <span>TTFB</span>
              <span>TTLB</span>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    </>
  )
}
