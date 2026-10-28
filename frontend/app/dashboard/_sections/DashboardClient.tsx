'use client'

import React from 'react'
import { useRouter } from 'next/navigation'
import { parseAsString, useQueryState } from 'nuqs'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError, unwrapClient, STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates, isStaleDashboardViewError } from '@/lib/api'
import type { ChartMetric, FiltersPayload, PlotlyLayout, PlotlyRelayoutEvent, PlotlyTrace } from '@/types/api'
import type { components } from '@/types/api.generated'
import { useFilterStore, useServiceStore } from '@/stores/filterStore'
import {
  useReportRangeAnchor, useAnalyticsReportQuery, useDashboardCards, useCardVisibility, usePlotlyRangeZoom,
  CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT,
  type CardCategory, type CardTint, type DashboardCard, type ReportConfiguration,
} from '@/hooks/useDashboardCards'
import { useIsDataReady, useActiveLogFields, useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { Button, buttonVariants } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { LayoutDashboard, ArrowRight, ChevronDown } from 'lucide-react'
import { TimeSeriesChart } from '@/components/AnalyticsCard'
import { ReportLayout } from '@/components/ReportLayout'
import { useShallow } from 'zustand/react/shallow'
import { cn, TRENDS, buildTrafficData, TIME_HOVER_LAYOUT, makeTimeXAxis, type BuildTrafficDataParams } from '@/lib/utils'
import { useLocalStorageString } from '@/hooks/useSSE'
import {
  Sparkline, CardGrid, GeoMap, DashboardHeader,
  type SparklineProps, type DashboardCatalog, type DashboardAggregates,
  type CardGridProps, type GeoMapProps, type DashboardHeaderProps,
} from './DashboardWidgets'

export {
  CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT, type CardCategory, type CardTint,
  Sparkline, CardGrid, GeoMap, DashboardHeader,
  type SparklineProps, type DashboardCatalog, type DashboardAggregates,
  type CardGridProps, type GeoMapProps, type DashboardHeaderProps,
}

export interface DashboardBodyProps {
  startTime: string | null; endTime: string | null; timezone: string; activeServiceId: string | null
  filterPayload: FiltersPayload; config: ReportConfiguration; metric?: string; setMetric?: (metric: string) => void
  trend: string; setTrend: (trend: string) => void; intervalButtons: React.ReactNode
  allCards: DashboardCard[]; visibleCards: Set<string>; relativeRange: string | null; isAutoRange: boolean; anchor: string
}

interface BuildChartLayoutParams {
  trafficData: PlotlyTrace[]; aggregates: Pick<DashboardAggregates, 'metric'> | null | undefined
  metric: string; startTime: string | null; endTime: string | null; timezone: string; catalog: DashboardCatalog | null | undefined
}

export function buildChartLayout({ trafficData, aggregates, metric, startTime, endTime, timezone, catalog }: BuildChartLayoutParams): PlotlyLayout {
  const actualMetric = aggregates?.metric || metric
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)
  return {
    ...TIME_HOVER_LAYOUT,
    barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
    bargap: 0.1, bargroupgap: 0.05,
    showlegend: trafficData.some(t => t.showlegend !== false),
    yaxis: {
      title: metricField?.unit || (actualMetric === 'requests' ? 'reqs' : ''),
      ticksuffix: metricField?.unit || '', separatethousands: true, exponentformat: 'none',
    },
    xaxis: makeTimeXAxis(startTime, endTime, timezone),
  }
}

export const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'

// ── TrafficChart ─────────────────────────────────────────────────────────────

interface TrafficChartProps {
  catalog: DashboardCatalog | undefined; metric: string; setMetric: (m: string) => void
  trend: string; setTrend: (t: string) => void; config: ReportConfiguration; intervalButtons: React.ReactNode
  trafficData: PlotlyTrace[]; chartLayout: PlotlyLayout; hiddenCategories: ReadonlySet<string>
  toggleCategory: (cat: string) => void; isReady: boolean; isLoadingAggs: boolean; isFetchingAggs: boolean
  transformPending: boolean; aggregates: DashboardAggregates | null | undefined
  onChartRelayout: (event: PlotlyRelayoutEvent) => void; startTime: string | null; endTime: string | null; timezone: string
}

const SHORT_METRIC_LABELS: Record<string, string> = {
  requests: 'Reqs', hit_rate: 'CHR', '5xx': '5xx', '4xx': '4xx',
  p50_latency: 'p50', p95_latency: 'p95', p99_latency: 'p99',
  throughput: 'Throughput', req_size: 'Req Size', ttfb: 'TTFB',
}
const LATENCY_IDS = ['p50_latency', 'p95_latency', 'p99_latency']
const METRIC_ORDER = ['requests', '5xx', '4xx', 'hit_rate']

export function TrafficChart({
  catalog, metric, setMetric, trend, setTrend, config, intervalButtons,
  trafficData, chartLayout, hiddenCategories, toggleCategory,
  isReady, isLoadingAggs, isFetchingAggs, transformPending, aggregates,
  onChartRelayout, startTime, endTime, timezone,
}: TrafficChartProps) {
  const { isFieldActive } = useActiveLogFields()
  const btnCls = (active: boolean) => cn(
    'h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]',
    active ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'hover:text-primary hover:bg-muted',
  )

  const metricButtons = (() => {
    const metricsFields = catalog?.fields?.filter((f) => f.group === 'METRICS') || []
    const otherMetrics = metricsFields.filter((f) => !LATENCY_IDS.includes(f.id))
    const orderedMetrics = [
      ...METRIC_ORDER.map(id => otherMetrics.find((f) => f.id === id)).filter((f) => f != null),
      ...otherMetrics.filter((f) => !METRIC_ORDER.includes(f.id)),
    ]
    const elements = orderedMetrics.map(m => (
      <Button key={m.id} variant={metric === m.id ? 'default' : 'ghost'} size="sm" onClick={() => React.startTransition(() => setMetric(m.id))} aria-pressed={metric === m.id} className={btnCls(metric === m.id)}>
        {SHORT_METRIC_LABELS[m.id] || m.label}
      </Button>
    ))
    const isLatency = metric.endsWith('_latency')
    const latLabel = isLatency ? metric.split('_')[0] : 'p95'
    const latencyDropdown = (
      <DropdownMenu key="latency">
        <DropdownMenuTrigger className={cn(buttonVariants({ variant: isLatency ? 'default' : 'ghost', size: 'sm' }), btnCls(isLatency))}>
          Latency ({latLabel}) <ChevronDown className="ml-1 h-3 w-3" />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {(['p50', 'p95', 'p99'] as const).map((p) => (
            <DropdownMenuItem key={p} onClick={() => setMetric(`${p}_latency`)} className="text-xs">{p} Latency</DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
    )
    const chrIndex = orderedMetrics.findIndex(m => m.id === 'hit_rate')
    if (chrIndex !== -1) elements.splice(chrIndex + 1, 0, latencyDropdown)
    else elements.push(latencyDropdown)
    return elements
  })()

  const emptyMessage =
    metric === 'ttfb_client' && !isFieldActive('ttfb')
      ? 'Requires Infrastructure (Group C) fields to be enabled in Fastly logging.'
      : metric === 'req_size' && !isFieldActive('req_bytes')
        ? 'Requires Request Identity (Group A) fields to be enabled in Fastly logging.'
        : 'No logs found for this period.'

  return (
    <div className="border rounded-lg p-4 flex flex-col relative overflow-hidden">
      <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-3 mb-4 relative z-10">
        <div className="flex flex-row items-center gap-2 xl:gap-4 flex-wrap">
          <h3 className="text-sm font-medium whitespace-nowrap hidden sm:block">Traffic over Time</h3>
          <div className="flex flex-row items-center gap-2">
            <ButtonGroup>{metricButtons}</ButtonGroup>
            {intervalButtons}
          </div>
        </div>
        <div className="flex items-center gap-3">
          {isFetchingAggs && !isLoadingAggs && (
            <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-wider animate-pulse">
              <span className="w-1.5 h-1.5 rounded-full bg-primary" />Updating
            </div>
          )}
        </div>
      </div>

      {trafficData.length > 1 && trafficData[0]?.type === 'bar' && (
        <div className="flex items-center gap-2 mb-2 relative z-10 flex-wrap">
          <ButtonGroup>
            {trafficData.filter(t => t.type === 'bar').map(trace => {
              const name = trace.name ?? ''
              const isHidden = hiddenCategories.has(name)
              return (
                <Button key={name} variant={isHidden ? 'ghost' : 'default'} size="sm" onClick={() => React.startTransition(() => toggleCategory(name))} className={btnCls(!isHidden)}>
                  <span className="w-1.5 h-1.5 rounded-full mr-1.5" style={{ backgroundColor: String(trace.marker?.color ?? '') }} />{name}
                </Button>
              )
            })}
          </ButtonGroup>
        </div>
      )}

      <div className="relative flex-1 mb-4">
        {(!isReady || !aggregates) || (isFetchingAggs && trafficData.length === 0) || (transformPending && trafficData.length === 0) ? (
          <div data-empty-placeholder="true" className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
            <span className="text-muted-foreground text-sm animate-pulse">{!isReady ? 'Initializing...' : 'Crunching logs...'}</span>
          </div>
        ) : trafficData.length === 0 ? (
          <div data-empty-placeholder="true" className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
            <div className="flex flex-col items-center text-muted-foreground text-center px-4">
              <span className="text-sm font-medium">No data available</span>
              <span className="text-xs mt-1">{emptyMessage}</span>
            </div>
          </div>
        ) : (
          <div className={cn('transition-opacity duration-100', isFetchingAggs && 'opacity-40 pointer-events-none')}>
            <TimeSeriesChart data={trafficData} layout={chartLayout} height={300} onRelayout={onChartRelayout} startTime={startTime} endTime={endTime} timezone={timezone} />
          </div>
        )}
      </div>

      <div className="mt-auto pt-2 border-t flex items-center gap-2 relative z-10">
        <span className="text-[10px] uppercase font-bold text-muted-foreground">Trend:</span>
        <ButtonGroup className="bg-muted/50 p-1">
          {TRENDS.map(t => (
            <Button key={t.value} variant={trend === t.value ? 'secondary' : 'ghost'} size="sm" onClick={() => React.startTransition(() => setTrend(t.value))} disabled={!config.validTrends.has(t.value)} aria-pressed={trend === t.value} className="h-9 text-xs px-2 shadow-none disabled:opacity-30 sm:h-7 sm:text-[11px]">
              {t.label}
            </Button>
          ))}
        </ButtonGroup>
      </div>
    </div>
  )
}

const WORKER_THRESHOLD = 2000

export function buildTrafficDataAsync(params: BuildTrafficDataParams): Promise<PlotlyTrace[]> {
  const n = params.aggregates?.time_series?.length ?? 0
  if (typeof window === 'undefined' || process.env.NODE_ENV === 'test' || n < WORKER_THRESHOLD) return Promise.resolve(buildTrafficData(params))
  return new Promise((resolve, reject) => {
    let worker: Worker
    try { worker = new Worker(new URL('../../../lib/workers/chartDataWorker.ts', import.meta.url)) }
    catch { resolve(buildTrafficData(params)); return }
    worker.onmessage = (event) => {
      worker.terminate()
      if (event.data?.success) resolve(event.data.data)
      else reject(new Error(event.data?.error ?? 'chartDataWorker failed'))
    }
    worker.onerror = () => {
      worker.terminate()
      try { resolve(buildTrafficData(params)) } catch (sync) { reject(sync) }
    }
    worker.postMessage(params)
  })
}

export type DashboardSection = NonNullable<components['schemas']['AggregatesRequest']['sections']>[number]

interface DashboardBundleArgs {
  startTime: string | null; endTime: string | null; relativeRange: string | null; isAutoRange: boolean; anchor: string
  filterPayload: FiltersPayload; metric: string; interval: string; enabled: boolean; fields?: string[]; sections?: DashboardSection[]
}

export function useDashboardBundle({
  startTime, endTime, relativeRange, isAutoRange, anchor,
  filterPayload, metric, interval, enabled, fields, sections,
}: DashboardBundleArgs) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  return useAnalyticsReportQuery({
    namespace: ['dashboard', 'bundle'], serviceId: activeServiceId, filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [metric, interval, fields, sections],
    queryFn: async ({ rangeBody, signal }) => {
      const body = await unwrapClient(
        client.POST('/api/dashboard/bundle', {
          signal, body: { filters: filterPayload, chart_metric: metric as ChartMetric, chart_interval: interval, fields, sections, ...rangeBody },
        }),
      )
      if (body?.aggregates) throwIfStaleAggregates(body.aggregates, { startTime, endTime }, Object.keys(filterPayload).length > 0)
      return body
    },
    queryOptions: { enabled, ...STALE_VIEW_RETRY_OPTIONS, refetchIntervalInBackground: false },
  })
}

const DASHBOARD_SECTIONS: DashboardSection[] = ['core', 'topten', 'bots']

function DashboardBody({
  startTime, endTime, timezone, activeServiceId, filterPayload, config,
  metric: metricProp, setMetric: setMetricProp, trend, setTrend,
  intervalButtons, allCards, visibleCards, relativeRange, isAutoRange, anchor,
}: DashboardBodyProps) {
  const { data: catalog } = useLogFieldsCatalog()
  const { addFilter, setRange, compareMode, compareStartTime, compareEndTime } = useFilterStore(
    useShallow(state => ({
      addFilter: state.addFilter, setRange: state.setRange, compareMode: state.compareMode,
      compareStartTime: state.compareStartTime, compareEndTime: state.compareEndTime,
    })),
  )

  const [localMetric, setLocalMetric] = React.useState('requests')
  let nuqsMetric: string | null = null
  let setNuqsMetric: ((val: string | null) => Promise<URLSearchParams>) | null = null
  try {
    ;[nuqsMetric, setNuqsMetric] = useQueryState(
      'metric', parseAsString.withDefault('requests').withOptions({ clearOnDefault: true, history: 'replace', shallow: true }),
    )
  } catch { /* Fallback when NuqsAdapter is not mounted in unit tests */ }
  const metric = metricProp ?? nuqsMetric ?? localMetric
  const setMetric = React.useCallback((nextMetric: string) => {
    if (setMetricProp) { setMetricProp(nextMetric); return }
    setLocalMetric(nextMetric)
    if (setNuqsMetric) void setNuqsMetric(nextMetric)
  }, [setMetricProp, setNuqsMetric])
  const router = useRouter()

  const [hiddenState, setHiddenState] = React.useState<{ metric: string; set: ReadonlySet<string> }>(() => ({ metric: 'requests', set: new Set() }))
  const hiddenCategories = React.useMemo<ReadonlySet<string>>(
    () => (hiddenState.metric === metric ? hiddenState.set : new Set<string>()),
    [hiddenState, metric],
  )

  const toggleCategory = React.useCallback((cat: string) => {
    setHiddenState((prev) => {
      const next = new Set(prev.metric === metric ? prev.set : [])
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return { metric, set: next }
    })
  }, [metric])

  const [collapsedRaw, writeCollapsedRaw] = useLocalStorageString(COLLAPSED_SECTIONS_KEY)
  const collapsedSections = React.useMemo<Set<string>>(() => {
    try { return collapsedRaw ? new Set<string>(JSON.parse(collapsedRaw)) : new Set<string>() }
    catch { return new Set<string>() }
  }, [collapsedRaw])

  const toggleSectionCollapsed = React.useCallback((id: string) => {
    const next = new Set(collapsedSections)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    writeCollapsedRaw(JSON.stringify([...next]))
  }, [collapsedSections, writeCollapsedRaw])

  const isReady = useIsDataReady()
  const bundleQuery = useDashboardBundle({
    startTime, endTime, relativeRange, isAutoRange, anchor,
    filterPayload, metric, interval: config.effectiveInterval, enabled: isReady, sections: DASHBOARD_SECTIONS,
  })

  const aggregates = bundleQuery.data?.aggregates
  const isStalePreparing = bundleQuery.isError && isStaleDashboardViewError(bundleQuery.error)
  const isLoadingAggs = bundleQuery.isLoading || (isStalePreparing && !aggregates)
  const isFetchingAggs = bundleQuery.isFetching && !isStalePreparing

  const { data: compareAggregates, error: compareError, refetch: refetchCompare } = useQuery({
    queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(client.POST('/api/dashboard/aggregates', {
        signal, body: { start_time: compareStartTime!, end_time: compareEndTime!, filters: filterPayload, chart_metric: metric as ChartMetric, chart_interval: config.effectiveInterval },
      }))
      return throwIfStaleAggregates(data, { startTime: compareStartTime, endTime: compareEndTime }, Object.keys(filterPayload).length > 0)
    },
    enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
    ...STALE_VIEW_RETRY_OPTIONS,
  })

  const trafficParams = React.useMemo(
    () => ({
      aggregates, compareAggregates, compareMode, compareStartTime, startTime, endTime,
      trend, timezone, metric, effectiveInterval: config.effectiveInterval, hiddenCategories, catalog,
    }),
    [aggregates, compareAggregates, compareMode, compareStartTime, startTime, endTime, trend, timezone, metric, config.effectiveInterval, hiddenCategories, catalog],
  )

  const [transformed, setTransformed] = React.useState<{ params: typeof trafficParams; traces: PlotlyTrace[] }>(
    () => ({ params: trafficParams, traces: buildTrafficData(trafficParams) }),
  )
  const trafficData = transformed.traces
  const transformPending = transformed.params !== trafficParams

  React.useEffect(() => {
    if (transformed.params === trafficParams) return
    let cancelled = false
    buildTrafficDataAsync(trafficParams)
      .then((traces) => { if (!cancelled) setTransformed({ params: trafficParams, traces }) })
      .catch(() => { if (!cancelled) setTransformed({ params: trafficParams, traces: buildTrafficData(trafficParams) }) })
    return () => { cancelled = true }
  }, [trafficParams, transformed.params])

  const chartLayout = React.useMemo(
    () => buildChartLayout({ trafficData, aggregates, metric, startTime, endTime, timezone, catalog }),
    [trafficData, aggregates, metric, startTime, endTime, timezone, catalog],
  )

  const handleRowClick = React.useCallback((column: string, value: string | number) => {
    React.startTransition(() => { addFilter(column, String(value), 'include') })
  }, [addFilter])
  const handleChartRelayout = usePlotlyRangeZoom(timezone, setRange)
  const handleCountryClick = React.useCallback((countryName: string) => {
    React.startTransition(() => { addFilter('country', countryName, 'include') })
  }, [addFilter])
  const visibleCardList = React.useMemo(() => allCards.filter((c) => visibleCards.has(c.id)), [allCards, visibleCards])

  return (
    <>
      {isStalePreparing ? (
        <div role="status" className="mb-6 flex items-center gap-2 rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100">
          <span className="h-2 w-2 animate-pulse rounded-full bg-amber-500" aria-hidden />
          <span>Preparing your data — newly ingested logs are still being indexed. This will refresh automatically.</span>
        </div>
      ) : bundleQuery.isError ? (
        <div role="alert" className="mb-6 flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100">
          <div className="font-semibold">Failed to load dashboard data.</div>
          <div className="font-mono text-xs opacity-80 break-all">{extractApiError(bundleQuery.error)}</div>
          <button type="button" onClick={() => { void bundleQuery.refetch() }} className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900">Retry</button>
        </div>
      ) : null}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <TrafficChart
          catalog={catalog} metric={metric} setMetric={setMetric} trend={trend} setTrend={setTrend}
          config={config} intervalButtons={intervalButtons} trafficData={trafficData} chartLayout={chartLayout}
          hiddenCategories={hiddenCategories} toggleCategory={toggleCategory} isReady={isReady}
          isLoadingAggs={isLoadingAggs} isFetchingAggs={isFetchingAggs} transformPending={transformPending}
          aggregates={aggregates} onChartRelayout={handleChartRelayout} startTime={startTime} endTime={endTime} timezone={timezone}
        />
        <GeoMap isReady={isReady} isFetchingAggs={isFetchingAggs} aggregates={aggregates} catalog={catalog} onCountryClick={handleCountryClick} />
      </div>

      <CardGrid
        visibleCardList={visibleCardList} isReady={isReady} isFetchingAggs={isFetchingAggs}
        aggregates={aggregates} compareAggregates={compareAggregates} compareMode={compareMode}
        topBotsData={bundleQuery.data?.top_bots} topBotsError={bundleQuery.error} onRetryTopBots={() => { void bundleQuery.refetch() }}
        compareError={compareError} onRetryCompare={() => { void refetchCompare() }}
        collapsedSections={collapsedSections} toggleSectionCollapsed={toggleSectionCollapsed} onRowClick={handleRowClick}
      />

      <div className="border rounded-lg bg-card p-6 flex flex-col md:flex-row items-center justify-between gap-4 shadow-sm">
        <div className="space-y-1">
          <h3 className="font-semibold text-sm">Raw Request Log Inspector</h3>
          <p className="text-xs text-muted-foreground">Inspect detailed parameters, search specific fields, and write advanced analytical queries.</p>
        </div>
        <Button
          variant="outline"
          onClick={() => {
            const params = new URLSearchParams()
            if (activeServiceId) params.set('service', activeServiceId)
            if (startTime) params.set('start_time', startTime)
            if (endTime) params.set('end_time', endTime)
            if (filterPayload) params.set('filters', JSON.stringify(filterPayload))
            const qs = params.toString()
            router.push(qs ? `/query?${qs}` : '/query')
          }}
        >
          See Raw Logs <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </div>
    </>
  )
}

export default function DashboardClient({ nowServerStr }: { nowServerStr?: string }) {
  const allCards = useDashboardCards()
  const allCardIds = React.useMemo(() => allCards.map((c) => c.id), [allCards])
  const inFormatCardIds = React.useMemo(() => allCards.filter((c) => c.inActiveFormat).map((c) => c.id), [allCards])
  const { visibleCards, toggleCard, showAll, reset: resetCards } = useCardVisibility('dashboard_cards', allCardIds, inFormatCardIds)
  const visibleCardsCount = React.useMemo(() => allCards.reduce((acc, c) => acc + (visibleCards.has(c.id) ? 1 : 0), 0), [allCards, visibleCards])
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Dashboard" description="Drill down into traffic details and analyze request trends." icon={LayoutDashboard} defaultInterval="1 hour"
      headerActions={<DashboardHeader visibleCardsCount={visibleCardsCount} allCards={allCards} visibleCards={visibleCards} onToggleCard={toggleCard} onShowAll={showAll} onResetCards={resetCards} />}
    >
      {(ctx) => <DashboardBody {...ctx} allCards={allCards} visibleCards={visibleCards} relativeRange={relativeRange} isAutoRange={isAutoRange} anchor={anchor} />}
    </ReportLayout>
  )
}
