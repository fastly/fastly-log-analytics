'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { useRouter } from 'next/navigation'
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
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Button, buttonVariants } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { LayoutDashboard, ArrowRight, EyeOff, ChevronDown, ChevronRight, Bot } from 'lucide-react'
import { TimeSeriesChart, LazyMount, CardErrorState } from '@/components/AnalyticsCard'
import { TopTenTable } from '@/components/Dashboard/TopTenTable'
import { ReportLayout } from '@/components/ReportLayout'
import { useShallow } from 'zustand/react/shallow'
import { cn, TRENDS, buildTrafficData, TIME_HOVER_LAYOUT, makeTimeXAxis, type BuildTrafficDataParams } from '@/lib/utils'
import { useDateFormat, useLocalStorageString } from '@/hooks/useSSE'

export { CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT, type CardCategory, type CardTint }

type CatalogEntry = components['schemas']['LogFieldCatalogEntry']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']
type Aggregates = components['schemas']['AggregatesResponse']
type TopBots = components['schemas']['SecurityTopBotsResponse']

export type DashboardCatalog = {
  fields?: Pick<CatalogEntry, 'id' | 'label' | 'group' | 'unit' | 'precision'>[] | null
  groups?: Pick<CatalogGroup, 'id' | 'label'>[] | null
}
export type DashboardAggregates = Partial<Pick<Aggregates, 'data' | 'map_data' | 'time_series' | 'metric' | 'interval'>>

export interface DashboardBodyProps {
  startTime: string | null
  endTime: string | null
  timezone: string
  activeServiceId: string | null
  filterPayload: FiltersPayload
  config: ReportConfiguration
  metric?: string
  setMetric?: (metric: string) => void
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  allCards: DashboardCard[]
  visibleCards: Set<string>
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

interface BuildChartLayoutParams {
  trafficData: PlotlyTrace[]
  aggregates: Pick<DashboardAggregates, 'metric'> | null | undefined
  metric: string
  startTime: string | null
  endTime: string | null
  timezone: string
  catalog: DashboardCatalog | null | undefined
}

export function buildChartLayout({ trafficData, aggregates, metric, startTime, endTime, timezone, catalog }: BuildChartLayoutParams): PlotlyLayout {
  const actualMetric = aggregates?.metric || metric
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)
  return {
    ...TIME_HOVER_LAYOUT,
    barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
    bargap: 0.1,
    bargroupgap: 0.05,
    showlegend: trafficData.some(t => t.showlegend !== false),
    yaxis: {
      title: metricField?.unit || (actualMetric === 'requests' ? 'reqs' : ''),
      ticksuffix: metricField?.unit || '',
      separatethousands: true,
      exponentformat: 'none',
    },
    xaxis: makeTimeXAxis(startTime, endTime, timezone),
  }
}

export const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'

// ── Sparkline ────────────────────────────────────────────────────────────────

type SparklinePoint = { ts: string; value: number }

export type SparklineProps = {
  points: SparklinePoint[]
  yDomain?: [number | 'auto', number | 'auto']
  height?: number
  stroke?: string
  formatValue?: (value: number) => string
  hoverThreshold?: number
  interactive?: boolean
  label?: string
}

export function Sparkline({
  points, yDomain = ['auto', 'auto'], height = 28, stroke = 'currentColor',
  formatValue, hoverThreshold = 48, interactive, label = 'trend',
}: SparklineProps) {
  if (!points || points.length < 2) return null

  const showHover = interactive ?? height >= hoverThreshold
  const width = 100
  let lo = Infinity, hi = -Infinity
  for (const p of points) {
    if (p.value < lo) lo = p.value
    if (p.value > hi) hi = p.value
  }
  const [minRaw, maxRaw] = yDomain
  const min = minRaw === 'auto' ? lo : minRaw
  const max = maxRaw === 'auto' ? hi : maxRaw
  const range = max - min || 1
  const yMin = min - (maxRaw === 'auto' ? range * 0.05 : 0)
  const yMax = max + (minRaw === 'auto' ? range * 0.05 : 0)
  const ySpan = yMax - yMin || 1
  const stepX = width / (points.length - 1)
  const pts = points.map((p, i) => ({ x: i * stepX, y: height - ((p.value - yMin) / ySpan) * height, raw: p }))
  const polylinePoints = pts.map((c) => `${c.x.toFixed(2)},${c.y.toFixed(2)}`).join(' ')

  if (!showHover) {
    return (
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" height={height} preserveAspectRatio="none" aria-hidden="true" style={{ display: 'block' }}>
        <polyline points={polylinePoints} fill="none" stroke={stroke} strokeWidth={1.25} strokeOpacity={0.55} vectorEffect="non-scaling-stroke" />
      </svg>
    )
  }

  return <InteractiveSparkline width={width} height={height} stroke={stroke} pts={pts} polylinePoints={polylinePoints} formatValue={formatValue} label={label} />
}

function InteractiveSparkline({
  width, height, stroke, pts, polylinePoints, formatValue, label,
}: {
  width: number; height: number; stroke: string
  pts: { x: number; y: number; raw: SparklinePoint }[]
  polylinePoints: string; formatValue?: (value: number) => string; label: string
}) {
  const svgRef = React.useRef<SVGSVGElement>(null)
  const cachedRectRef = React.useRef<DOMRect | null>(null)
  const fmt = useDateFormat()

  const hoverKey = `${pts.length}|${pts[0]?.raw.ts}|${pts[pts.length - 1]?.raw.ts}`
  const [hover, setHover] = React.useState<{ key: string; idx: number | null }>({ key: hoverKey, idx: null })
  const hoverIdx = hover.key === hoverKey ? hover.idx : null
  const setHoverIdx = React.useCallback(
    (update: number | null | ((prev: number | null) => number | null)) =>
      setHover((h) => {
        const prev = h.key === hoverKey ? h.idx : null
        return { key: hoverKey, idx: typeof update === 'function' ? update(prev) : update }
      }),
    [hoverKey],
  )

  React.useEffect(() => {
    const invalidate = () => { cachedRectRef.current = null }
    window.addEventListener('resize', invalidate)
    return () => window.removeEventListener('resize', invalidate)
  }, [])

  const updateHover = React.useCallback((clientX: number) => {
    const el = svgRef.current
    if (!el) return
    const rect = cachedRectRef.current ?? (cachedRectRef.current = el.getBoundingClientRect())
    if (rect.width <= 0) return
    const ratio = (clientX - rect.left) / rect.width
    const idx = Math.max(0, Math.min(pts.length - 1, Math.round(ratio * (pts.length - 1))))
    setHoverIdx((prev) => (prev === idx ? prev : idx))
  }, [pts.length, setHoverIdx])

  const onPointerEnter = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => { cachedRectRef.current = e.currentTarget.getBoundingClientRect() }, [])
  const onPointerMove = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => updateHover(e.clientX), [updateHover])
  const onPointerDown = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    cachedRectRef.current = e.currentTarget.getBoundingClientRect()
    updateHover(e.clientX)
  }, [updateHover])
  const onPointerLeave = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    if (e.pointerType !== 'touch' && e.pointerType !== 'pen') setHoverIdx(null)
  }, [setHoverIdx])
  const onPointerCancel = React.useCallback(() => setHoverIdx(null), [setHoverIdx])

  React.useEffect(() => {
    if (hoverIdx === null) return
    const onDocPointerDown = (e: PointerEvent) => {
      if (e.pointerType !== 'touch' && e.pointerType !== 'pen') return
      if (svgRef.current && e.target instanceof Node && svgRef.current.contains(e.target)) return
      setHoverIdx(null)
    }
    document.addEventListener('pointerdown', onDocPointerDown)
    return () => document.removeEventListener('pointerdown', onDocPointerDown)
  }, [hoverIdx, setHoverIdx])

  const hovered = hoverIdx !== null ? pts[hoverIdx] : null
  const latest = pts[pts.length - 1]
  const fmtVal = (v: number) => (formatValue ? formatValue(v) : v.toFixed(2))
  const latestText = fmtVal(latest.raw.value)

  const tsPattern = React.useMemo(() => {
    if (!hovered) return 'HH:mm'
    return fmt.format(hovered.raw.ts, 'yyyy-MM-dd') === fmt.format(new Date(), 'yyyy-MM-dd') ? 'HH:mm' : 'MMM d, HH:mm'
  }, [hovered, fmt])

  return (
    <div className="relative" style={{ width: '100%', height }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        preserveAspectRatio="none"
        role="img"
        aria-label={`${label} sparkline, latest ${latestText}, ${pts.length} samples`}
        style={{ display: 'block', touchAction: 'manipulation' }}
        onPointerEnter={onPointerEnter}
        onPointerMove={onPointerMove}
        onPointerDown={onPointerDown}
        onPointerLeave={onPointerLeave}
        onPointerCancel={onPointerCancel}
      >
        <polyline points={polylinePoints} fill="none" stroke={stroke} strokeWidth={1.25} strokeOpacity={0.55} vectorEffect="non-scaling-stroke" />
        {hovered && (
          <line x1={hovered.x} x2={hovered.x} y1={0} y2={height} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.55} strokeWidth={0.5} vectorEffect="non-scaling-stroke" />
        )}
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
      </svg>
      {hovered && (
        <span
          aria-hidden
          style={{
            position: 'absolute', left: `${(hovered.x / width) * 100}%`, top: `${(hovered.y / height) * 100}%`,
            width: 6, height: 6, marginLeft: -3, marginTop: -3, borderRadius: '50%', background: stroke, pointerEvents: 'none',
          }}
        />
      )}
      <Tooltip open={hovered !== null} onOpenChange={(open) => { if (!open) setHoverIdx(null) }}>
        <TooltipTrigger
          render={
            <span
              aria-hidden
              style={{
                position: 'absolute', left: hovered ? `${(hovered.x / width) * 100}%` : 0, top: hovered ? `${(hovered.y / height) * 100}%` : 0,
                width: 1, height: 1, pointerEvents: 'none',
              }}
            />
          }
        />
        {hovered && (
          <TooltipContent side="top" sideOffset={8} className="text-xs max-w-[200px] p-2 space-y-1">
            <div className="font-mono tabular-nums">{fmtVal(hovered.raw.value)}</div>
            <div className="text-muted-foreground">{fmt.format(hovered.raw.ts, tsPattern)}</div>
          </TooltipContent>
        )}
      </Tooltip>
    </div>
  )
}

// ── CardGrid ─────────────────────────────────────────────────────────────────

interface CardGridProps {
  visibleCardList: DashboardCard[]
  isReady: boolean
  isFetchingAggs: boolean
  aggregates: DashboardAggregates | null | undefined
  compareAggregates: DashboardAggregates | null | undefined
  compareMode: boolean
  topBotsData: TopBots | null | undefined
  topBotsError?: unknown
  onRetryTopBots?: () => void
  compareError?: unknown
  onRetryCompare?: () => void
  collapsedSections: Set<string>
  toggleSectionCollapsed: (id: string) => void
  onRowClick: (column: string, value: string | number) => void
}

function CardSkeleton({ isReady }: { isReady: boolean }) {
  return (
    <div
      data-empty-placeholder="true"
      className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20 [content-visibility:auto] [contain-intrinsic-size:300px]"
    >
      <span className="text-muted-foreground text-xs animate-pulse">{!isReady ? 'Initializing...' : 'Loading...'}</span>
    </div>
  )
}

export function CardGrid({
  visibleCardList, isReady, isFetchingAggs, aggregates, compareAggregates, compareMode,
  topBotsData, topBotsError, onRetryTopBots, compareError, onRetryCompare,
  collapsedSections, toggleSectionCollapsed, onRowClick,
}: CardGridProps) {
  if (visibleCardList.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        {CARD_CATEGORIES.map((cat) => (
          <section key={`skel-${cat.id}`} className={cn('rounded-lg border', cat.tint.bg, cat.tint.border)}>
            <div className="w-full flex items-center gap-2 px-4 py-2.5">
              <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
              <span className={cn('inline-block w-1.5 h-1.5 rounded-full', cat.tint.dot)} />
              <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground">{cat.label}</h3>
              <span className="text-[10px] text-muted-foreground/60 font-mono">{cat.cardIds.length}</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1">
              {cat.cardIds.map((id) => <CardSkeleton key={`skel-${cat.id}-${id}`} isReady={isReady} />)}
            </div>
          </section>
        ))}
      </div>
    )
  }

  const visibleById = new Map(visibleCardList.map((c) => [c.id, c]))
  const renderCard = (card: DashboardCard) => {
    const isBotCard = card.id === '_bot_name' || card.id === '_ngwaf_bot_name'
    if (isBotCard && topBotsError) {
      return <CardErrorState key={card.id} icon={<Bot className="h-4 w-4" />} title={card.label} message="Failed to load bot data." onRetry={() => onRetryTopBots?.()} />
    }
    if (!isReady || !aggregates || (isBotCard && !topBotsData)) {
      return <CardSkeleton key={card.id} isReady={isReady} />
    }
    if (isBotCard) {
      const botItems = card.id === '_bot_name' ? (topBotsData?.bots ?? []) : (topBotsData?.ngwaf_bots ?? [])
      return (
        <LazyMount key={card.id} minHeight={300}>
          <TopTenTable
            title={card.label}
            icon={card.id === '_bot_name' ? <Bot className="h-4 w-4" /> : undefined}
            field={card.id}
            inActiveFormat={card.inActiveFormat}
            data={{
              total: botItems.reduce((acc, b) => acc + b.request_count, 0),
              top: botItems.map((b) => ({
                value: card.id === '_bot_name' ? ('id' in b && b.id ? b.id : b.name) : b.name,
                label: b.name,
                count: b.request_count,
              })),
            }}
            compareData={undefined}
            onRowClick={onRowClick}
          />
        </LazyMount>
      )
    }
    return (
      <LazyMount key={card.id} minHeight={300}>
        <TopTenTable
          title={card.label}
          field={card.id}
          inActiveFormat={card.inActiveFormat}
          data={aggregates?.data?.[card.id]}
          compareData={compareMode ? compareAggregates?.data?.[card.id] : undefined}
          onRowClick={onRowClick}
        />
      </LazyMount>
    )
  }

  const sections = CARD_CATEGORIES.map(cat => ({
    ...cat,
    cards: cat.cardIds.flatMap(id => visibleById.get(id) ?? []),
  })).filter(s => s.cards.length > 0)

  const customCards = visibleCardList.filter((c) => !CATEGORIZED_CARD_IDS.has(c.id))
  if (customCards.length > 0) sections.push({ id: 'custom', label: 'Custom', cardIds: [], cards: customCards, tint: CUSTOM_TINT })

  return (
    <div className={cn('flex flex-col gap-4 transition-opacity duration-100', isFetchingAggs && 'opacity-40 pointer-events-none')}>
      {compareMode && compareError ? (
        <div role="alert" className="flex items-center justify-between gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
          <span>Failed to load comparison data — cards show the current period only.</span>
          {onRetryCompare && (
            <button type="button" onClick={() => onRetryCompare()} className="rounded border border-destructive/40 px-2 py-0.5 hover:bg-destructive/10">
              Retry
            </button>
          )}
        </div>
      ) : null}
      {sections.map(section => {
        const isCollapsed = collapsedSections.has(section.id)
        const Chevron = isCollapsed ? ChevronRight : ChevronDown
        return (
          <section key={section.id} className={cn('rounded-lg border', section.tint.bg, section.tint.border)}>
            <button
              type="button"
              onClick={() => toggleSectionCollapsed(section.id)}
              aria-expanded={!isCollapsed}
              aria-controls={`section-${section.id}-cards`}
              className="w-full flex items-center gap-2 px-4 py-2.5 text-left hover:bg-black/[0.02] dark:hover:bg-white/[0.03] rounded-t-lg transition-colors group"
            >
              <Chevron className="h-3.5 w-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
              <span className={cn('inline-block w-1.5 h-1.5 rounded-full', section.tint.dot)} />
              <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground group-hover:text-foreground transition-colors">
                {section.label}
              </h3>
              <span className="text-[10px] text-muted-foreground/60 font-mono">{section.cards.length}</span>
            </button>
            {!isCollapsed && (
              <div id={`section-${section.id}-cards`} className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1">
                {section.cards.map((card) => renderCard(card))}
              </div>
            )}
          </section>
        )
      })}
    </div>
  )
}

// ── TrafficChart ─────────────────────────────────────────────────────────────

interface TrafficChartProps {
  catalog: DashboardCatalog | undefined
  metric: string
  setMetric: (m: string) => void
  trend: string
  setTrend: (t: string) => void
  config: ReportConfiguration
  intervalButtons: React.ReactNode
  trafficData: PlotlyTrace[]
  chartLayout: PlotlyLayout
  hiddenCategories: ReadonlySet<string>
  toggleCategory: (cat: string) => void
  isReady: boolean
  isLoadingAggs: boolean
  isFetchingAggs: boolean
  transformPending: boolean
  aggregates: DashboardAggregates | null | undefined
  onChartRelayout: (event: PlotlyRelayoutEvent) => void
  startTime: string | null
  endTime: string | null
  timezone: string
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
      <Button
        key={m.id}
        variant={metric === m.id ? 'default' : 'ghost'}
        size="sm"
        onClick={() => React.startTransition(() => setMetric(m.id))}
        aria-pressed={metric === m.id}
        className={btnCls(metric === m.id)}
      >
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
              <span className="w-1.5 h-1.5 rounded-full bg-primary" />
              Updating
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
                <Button
                  key={name}
                  variant={isHidden ? 'ghost' : 'default'}
                  size="sm"
                  onClick={() => React.startTransition(() => toggleCategory(name))}
                  className={btnCls(!isHidden)}
                >
                  <span className="w-1.5 h-1.5 rounded-full mr-1.5" style={{ backgroundColor: String(trace.marker?.color ?? '') }} />
                  {name}
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
              <span className="text-xs mt-1">
                {metric === 'ttfb_client' && !isFieldActive('ttfb')
                  ? 'Requires Infrastructure (Group C) fields to be enabled in Fastly logging.'
                  : metric === 'req_size' && !isFieldActive('req_bytes')
                    ? 'Requires Request Identity (Group A) fields to be enabled in Fastly logging.'
                    : 'No logs found for this period.'}
              </span>
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
            <Button
              key={t.value}
              variant={trend === t.value ? 'secondary' : 'ghost'}
              size="sm"
              onClick={() => React.startTransition(() => setTrend(t.value))}
              disabled={!config.validTrends.has(t.value)}
              aria-pressed={trend === t.value}
              className="h-9 text-xs px-2 shadow-none disabled:opacity-30 sm:h-7 sm:text-[11px]"
            >
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
  if (typeof window === 'undefined' || process.env.NODE_ENV === 'test' || n < WORKER_THRESHOLD) {
    return Promise.resolve(buildTrafficData(params))
  }
  return new Promise((resolve, reject) => {
    let worker: Worker
    try {
      worker = new Worker(new URL('../../../lib/workers/chartDataWorker.ts', import.meta.url))
    } catch {
      resolve(buildTrafficData(params))
      return
    }
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
  startTime: string | null
  endTime: string | null
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  filterPayload: FiltersPayload
  metric: string
  interval: string
  enabled: boolean
  fields?: string[]
  sections?: DashboardSection[]
}

export function useDashboardBundle({
  startTime, endTime, relativeRange, isAutoRange, anchor,
  filterPayload, metric, interval, enabled, fields, sections,
}: DashboardBundleArgs) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  return useAnalyticsReportQuery({
    namespace: ['dashboard', 'bundle'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [metric, interval, fields, sections],
    queryFn: async ({ rangeBody, signal }) => {
      const body = await unwrapClient(
        client.POST('/api/dashboard/bundle', {
          signal,
          body: { filters: filterPayload, chart_metric: metric as ChartMetric, chart_interval: interval, fields, sections, ...rangeBody },
        }),
      )
      if (body?.aggregates) {
        throwIfStaleAggregates(body.aggregates, { startTime, endTime }, Object.keys(filterPayload).length > 0)
      }
      return body
    },
    queryOptions: { enabled, ...STALE_VIEW_RETRY_OPTIONS, refetchIntervalInBackground: false },
  })
}

const ChoroplethMap = dynamic(
  () => import('@/components/Map/ChoroplethMap').then((m) => ({ default: m.ChoroplethMap })),
  {
    ssr: false,
    loading: () => (
      <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded" aria-busy="true">
        <span className="text-muted-foreground text-xs animate-pulse">Loading map…</span>
      </div>
    ),
  },
)

interface GeoMapProps {
  isReady: boolean
  isFetchingAggs: boolean
  aggregates: DashboardAggregates | null | undefined
  catalog: DashboardCatalog | undefined
  onCountryClick: (countryName: string) => void
}

export function GeoMap({ isReady, isFetchingAggs, aggregates, catalog, onCountryClick }: GeoMapProps) {
  const { isFieldActive } = useActiveLogFields()
  const emptyReason = (() => {
    if (isFieldActive('country')) return 'No country data in this time range.'
    const countryField = catalog?.fields?.find((f) => f.id === 'country')
    const groupMeta = countryField?.group ? catalog?.groups?.find((g) => g.id === countryField.group) : undefined
    return groupMeta
      ? `Requires ${groupMeta.label} fields to be enabled in Fastly logging.`
      : 'Requires Geolocation fields to be enabled in Fastly logging.'
  })()

  return (
    <div className={cn('border rounded-lg p-4 flex flex-col transition-opacity duration-100', isFetchingAggs && 'opacity-40 pointer-events-none')}>
      <h3 className="text-sm font-medium mb-4 text-foreground">Requests by Country</h3>
      {(!isReady || !aggregates) || (isFetchingAggs && (!aggregates?.map_data || aggregates.map_data.length === 0)) ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
          <span className="text-muted-foreground text-sm animate-pulse">{!isReady ? 'Initializing...' : 'Mapping traffic...'}</span>
        </div>
      ) : !aggregates?.map_data || aggregates.map_data.length === 0 ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
          <div className="flex flex-col items-center text-foreground text-center px-4">
            <span className="text-sm font-medium mb-1">No data available</span>
            <span className="text-[10px] text-muted-foreground">{emptyReason}</span>
          </div>
        </div>
      ) : (
        <ChoroplethMap data={aggregates?.map_data || []} className="flex-1 min-h-[300px]" onCountryClick={onCountryClick} />
      )}
    </div>
  )
}

interface DashboardHeaderProps {
  visibleCardsCount: number
  allCards: Array<{ id: string; label: string; inActiveFormat?: boolean }>
  visibleCards: Set<string>
  onToggleCard: (id: string) => void
  onShowAll: () => void
  onResetCards: () => void
}

export function DashboardHeader({
  visibleCardsCount, allCards, visibleCards, onToggleCard, onShowAll, onResetCards,
}: DashboardHeaderProps) {
  return (
    <Popover>
      <PopoverTrigger
        aria-label={`Cards ${visibleCardsCount}, toggle visible dashboard cards`}
        render={
          <Button variant="outline" size="sm" className="h-9 gap-1.5">
            <span className="flex items-center gap-1.5">
              <LayoutDashboard className="h-4 w-4" />
              <span className="hidden sm:inline text-xs">Cards</span>{' '}
              <Badge variant="secondary" className="h-4 text-[10px] px-1.5">{visibleCardsCount}</Badge>
            </span>
          </Button>
        }
      />
      <PopoverContent align="end" className="w-72 p-3">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Visible cards</p>
        <div className="grid grid-cols-2 gap-x-4 gap-y-2">
          {allCards.map(card => (
            <div key={card.id} className="flex items-center gap-2">
              <Checkbox
                id={`card-${card.id}`}
                checked={visibleCards.has(card.id)}
                onCheckedChange={() => React.startTransition(() => onToggleCard(card.id))}
              />
              <Label htmlFor={`card-${card.id}`} className="text-xs cursor-pointer leading-tight inline-flex items-center gap-1">
                {card.label}
                {card.inActiveFormat === false && (
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <span
                          className="inline-flex items-center rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label="Not in active log format"
                        />
                      }
                    >
                      <EyeOff className="h-3 w-3 text-muted-foreground/70" />
                    </TooltipTrigger>
                    <TooltipContent><p className="text-[10px]">Not in active log format.</p></TooltipContent>
                  </Tooltip>
                )}
              </Label>
            </div>
          ))}
        </div>
        <div className="flex gap-2 mt-4 pt-3 border-t">
          <Button variant="ghost" size="sm" className="text-xs h-7 flex-1" onClick={() => React.startTransition(() => onShowAll())}>Show all</Button>
          <Button variant="ghost" size="sm" className="text-xs h-7 flex-1" onClick={() => React.startTransition(() => onResetCards())}>Reset</Button>
        </div>
      </PopoverContent>
    </Popover>
  )
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
      addFilter: state.addFilter,
      setRange: state.setRange,
      compareMode: state.compareMode,
      compareStartTime: state.compareStartTime,
      compareEndTime: state.compareEndTime,
    })),
  )

  const [localMetric, setLocalMetric] = React.useState('requests')
  const metric = metricProp ?? localMetric
  const setMetric = setMetricProp ?? setLocalMetric
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
    filterPayload, metric, interval: config.effectiveInterval,
    enabled: isReady, sections: DASHBOARD_SECTIONS,
  })

  const aggregates = bundleQuery.data?.aggregates
  const isStalePreparing = bundleQuery.isError && isStaleDashboardViewError(bundleQuery.error)
  const isLoadingAggs = bundleQuery.isLoading || (isStalePreparing && !aggregates)
  const isFetchingAggs = bundleQuery.isFetching && !isStalePreparing

  const { data: compareAggregates, error: compareError, refetch: refetchCompare } = useQuery({
    queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(client.POST('/api/dashboard/aggregates', {
        signal,
        body: {
          start_time: compareStartTime!, end_time: compareEndTime!,
          filters: filterPayload, chart_metric: metric as ChartMetric, chart_interval: config.effectiveInterval,
        },
      }))
      return throwIfStaleAggregates(data, { startTime: compareStartTime, endTime: compareEndTime }, Object.keys(filterPayload).length > 0)
    },
    enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
    ...STALE_VIEW_RETRY_OPTIONS,
  })

  const topBotsData = bundleQuery.data?.top_bots
  const topBotsError = bundleQuery.error
  const refetchTopBots = bundleQuery.refetch

  const trafficParams = React.useMemo(
    () => ({
      aggregates, compareAggregates, compareMode, compareStartTime,
      startTime, endTime, trend, timezone, metric,
      effectiveInterval: config.effectiveInterval, hiddenCategories, catalog,
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
          <button type="button" onClick={() => { void bundleQuery.refetch() }} className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900">
            Retry
          </button>
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
        topBotsData={topBotsData} topBotsError={topBotsError} onRetryTopBots={() => { void refetchTopBots() }}
        compareError={compareError} onRetryCompare={() => { void refetchCompare() }}
        collapsedSections={collapsedSections} toggleSectionCollapsed={toggleSectionCollapsed} onRowClick={handleRowClick}
      />

      <div className="border rounded-lg bg-card p-6 flex flex-col md:flex-row items-center justify-between gap-4 shadow-sm">
        <div className="space-y-1">
          <h3 className="font-semibold text-sm">Raw Request Log Inspector</h3>
          <p className="text-xs text-muted-foreground">
            Inspect detailed parameters, search specific fields, and write advanced analytical queries.
          </p>
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
      title="Dashboard"
      description="Drill down into traffic details and analyze request trends."
      icon={LayoutDashboard}
      defaultInterval="1 hour"
      headerActions={
        <DashboardHeader
          visibleCardsCount={visibleCardsCount}
          allCards={allCards}
          visibleCards={visibleCards}
          onToggleCard={toggleCard}
          onShowAll={showAll}
          onResetCards={resetCards}
        />
      }
    >
      {(ctx) => (
        <DashboardBody
          {...ctx}
          allCards={allCards}
          visibleCards={visibleCards}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}
