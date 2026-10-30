'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import type { components } from '@/types/api.generated'
import {
  CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT,
  type DashboardCard,
} from '@/hooks/useDashboardCards'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { LayoutDashboard, EyeOff, ChevronDown, ChevronRight, Bot } from 'lucide-react'
import { LazyMount, CardErrorState } from '@/components/AnalyticsCard'
import { TopTenTable } from '@/components/Dashboard/TopTenTable'
import { cn } from '@/lib/utils'
import { useDateFormat } from '@/hooks/useSSE'

type CatalogEntry = components['schemas']['LogFieldCatalogEntry']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']
type Aggregates = components['schemas']['AggregatesResponse']
type TopBots = components['schemas']['SecurityTopBotsResponse']

export type DashboardCatalog = {
  fields?: Pick<CatalogEntry, 'id' | 'label' | 'group' | 'unit' | 'precision'>[] | null
  groups?: Pick<CatalogGroup, 'id' | 'label'>[] | null
}
export type DashboardAggregates = Partial<Pick<Aggregates, 'data' | 'map_data' | 'time_series' | 'metric' | 'interval'>>

// ── Sparkline ────────────────────────────────────────────────────────────────

type SparklinePoint = { ts: string; value: number }

export type SparklineProps = {
  points: SparklinePoint[]; yDomain?: [number | 'auto', number | 'auto']; height?: number; stroke?: string
  formatValue?: (value: number) => string; hoverThreshold?: number; interactive?: boolean; label?: string
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
    const idx = Math.max(0, Math.min(pts.length - 1, Math.round(((clientX - rect.left) / rect.width) * (pts.length - 1))))
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
  const fmtVal = (v: number) => (formatValue ? formatValue(v) : v.toFixed(2))
  const latestText = fmtVal(pts[pts.length - 1].raw.value)
  const tsPattern = React.useMemo(
    () => (!hovered || fmt.format(hovered.raw.ts, 'yyyy-MM-dd') === fmt.format(new Date(), 'yyyy-MM-dd') ? 'HH:mm' : 'MMM d, HH:mm'),
    [hovered, fmt],
  )
  const hoverLeft = hovered ? `${(hovered.x / width) * 100}%` : 0
  const hoverTop = hovered ? `${(hovered.y / height) * 100}%` : 0

  return (
    <div className="relative" style={{ width: '100%', height }}>
      <svg
        ref={svgRef} viewBox={`0 0 ${width} ${height}`} width="100%" height={height} preserveAspectRatio="none"
        role="img" aria-label={`${label} sparkline, latest ${latestText}, ${pts.length} samples`}
        style={{ display: 'block', touchAction: 'manipulation' }}
        onPointerEnter={onPointerEnter} onPointerMove={onPointerMove} onPointerDown={onPointerDown}
        onPointerLeave={onPointerLeave} onPointerCancel={onPointerCancel}
      >
        <polyline points={polylinePoints} fill="none" stroke={stroke} strokeWidth={1.25} strokeOpacity={0.55} vectorEffect="non-scaling-stroke" />
        {hovered && <line x1={hovered.x} x2={hovered.x} y1={0} y2={height} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.55} strokeWidth={0.5} vectorEffect="non-scaling-stroke" />}
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
      </svg>
      {hovered && <span aria-hidden style={{ position: 'absolute', left: hoverLeft, top: hoverTop, width: 6, height: 6, marginLeft: -3, marginTop: -3, borderRadius: '50%', background: stroke, pointerEvents: 'none' }} />}
      <Tooltip open={hovered !== null} onOpenChange={(open) => { if (!open) setHoverIdx(null) }}>
        <TooltipTrigger render={<span aria-hidden style={{ position: 'absolute', left: hoverLeft, top: hoverTop, width: 1, height: 1, pointerEvents: 'none' }} />} />
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

export interface CardGridProps {
  visibleCardList: DashboardCard[]; isReady: boolean; isFetchingAggs: boolean
  aggregates: DashboardAggregates | null | undefined; compareAggregates: DashboardAggregates | null | undefined
  compareMode: boolean; topBotsData: TopBots | null | undefined; topBotsError?: unknown; onRetryTopBots?: () => void
  compareError?: unknown; onRetryCompare?: () => void
  collapsedSections: Set<string>; toggleSectionCollapsed: (id: string) => void
  onRowClick: (column: string, value: string | number) => void
}

function CardSkeleton({ isReady }: { isReady: boolean }) {
  return (
    <div data-empty-placeholder="true" className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20 [content-visibility:auto] [contain-intrinsic-size:300px]">
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
    if (isReady) return null
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
    const botItems = isBotCard ? (card.id === '_bot_name' ? (topBotsData?.bots ?? []) : (topBotsData?.ngwaf_bots ?? [])) : null
    const tableData = botItems
      ? {
          total: botItems.reduce((acc, b) => acc + b.request_count, 0),
          top: botItems.map((b) => ({
            value: card.id === '_bot_name' ? ('id' in b && b.id ? b.id : b.name) : b.name,
            label: b.name, count: b.request_count,
          })),
        }
      : aggregates?.data?.[card.id]

    return (
      <LazyMount key={card.id} minHeight={300}>
        <TopTenTable
          title={card.label} icon={card.id === '_bot_name' ? <Bot className="h-4 w-4" /> : undefined}
          field={card.id} inActiveFormat={card.inActiveFormat} data={tableData}
          compareData={isBotCard ? undefined : (compareMode ? compareAggregates?.data?.[card.id] : undefined)}
          onRowClick={onRowClick}
        />
      </LazyMount>
    )
  }

  const sections = CARD_CATEGORIES.map(cat => ({ ...cat, cards: cat.cardIds.flatMap(id => visibleById.get(id) ?? []) })).filter(s => s.cards.length > 0)
  const customCards = visibleCardList.filter((c) => !CATEGORIZED_CARD_IDS.has(c.id))
  if (customCards.length > 0) sections.push({ id: 'custom', label: 'Custom', cardIds: [], cards: customCards, tint: CUSTOM_TINT })

  return (
    <div className={cn('flex flex-col gap-4 transition-opacity duration-100', isFetchingAggs && 'opacity-40 pointer-events-none')}>
      {compareMode && compareError ? (
        <div role="alert" className="flex items-center justify-between gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
          <span>Failed to load comparison data — cards show the current period only.</span>
          {onRetryCompare && <button type="button" onClick={() => onRetryCompare()} className="rounded border border-destructive/40 px-2 py-0.5 hover:bg-destructive/10">Retry</button>}
        </div>
      ) : null}
      {sections.map(section => {
        const isCollapsed = collapsedSections.has(section.id)
        const Chevron = isCollapsed ? ChevronRight : ChevronDown
        return (
          <section key={section.id} className={cn('rounded-lg border', section.tint.bg, section.tint.border)}>
            <button
              type="button" onClick={() => toggleSectionCollapsed(section.id)} aria-expanded={!isCollapsed} aria-controls={`section-${section.id}-cards`}
              className="w-full flex items-center gap-2 px-4 py-2.5 text-left hover:bg-black/[0.02] dark:hover:bg-white/[0.03] rounded-t-lg transition-colors group"
            >
              <Chevron className="h-3.5 w-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
              <span className={cn('inline-block w-1.5 h-1.5 rounded-full', section.tint.dot)} />
              <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground group-hover:text-foreground transition-colors">{section.label}</h3>
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

// ── GeoMap & DashboardHeader ─────────────────────────────────────────────────

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

export interface GeoMapProps {
  isReady: boolean; isFetchingAggs: boolean; aggregates: DashboardAggregates | null | undefined
  catalog: DashboardCatalog | undefined; onCountryClick: (countryName: string) => void
}

export function GeoMap({ isReady, isFetchingAggs, aggregates, catalog, onCountryClick }: GeoMapProps) {
  const { isFieldActive } = useActiveLogFields()
  const emptyReason = (() => {
    if (isFieldActive('country')) return 'No country data in this time range.'
    const countryField = catalog?.fields?.find((f) => f.id === 'country')
    const groupMeta = countryField?.group ? catalog?.groups?.find((g) => g.id === countryField.group) : undefined
    return groupMeta ? `Requires ${groupMeta.label} fields to be enabled in Fastly logging.` : 'Requires Geolocation fields to be enabled in Fastly logging.'
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

export interface DashboardHeaderProps {
  visibleCardsCount: number; allCards: Array<{ id: string; label: string; inActiveFormat?: boolean }>
  visibleCards: Set<string>; onToggleCard: (id: string) => void; onShowAll: () => void; onResetCards: () => void
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
              <Checkbox id={`card-${card.id}`} checked={visibleCards.has(card.id)} onCheckedChange={() => React.startTransition(() => onToggleCard(card.id))} />
              <Label htmlFor={`card-${card.id}`} className="text-xs cursor-pointer leading-tight inline-flex items-center gap-1">
                {card.label}
                {card.inActiveFormat === false && (
                  <Tooltip>
                    <TooltipTrigger render={<span className="inline-flex items-center rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" tabIndex={0} role="button" aria-label="Not in active log format" />}>
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
