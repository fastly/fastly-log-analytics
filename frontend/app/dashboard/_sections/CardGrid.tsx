'use client'

import React from 'react'
import { TopTenTable } from '@/components/Dashboard/TopTenTable'
import { LazyMount } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/AnalyticsCard'
import { ChevronDown, ChevronRight, Bot } from 'lucide-react'
import { cn } from '@/lib/utils'
import { TIME_HOVER_LAYOUT, makeTimeXAxis } from '@/lib/chart-helpers'
import {
  CARD_CATEGORIES,
  CATEGORIZED_CARD_IDS,
  CUSTOM_TINT,
  type CardCategory,
  type CardTint,
  type DashboardCard,
  type ReportConfiguration,
} from '@/hooks/useDashboardCards'
import type { FiltersPayload } from '@/types/api'
import type { components } from '@/types/api.generated'
import type { PlotlyLayout, PlotlyTrace } from '@/types/api'

export { CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT, type CardCategory, type CardTint }

type CatalogEntry = components['schemas']['LogFieldCatalogEntry']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']
type Aggregates = components['schemas']['AggregatesResponse']

/** The slice of the log-fields catalog the dashboard cards read. */
export type DashboardCatalog = {
  fields?: Pick<CatalogEntry, 'id' | 'label' | 'group' | 'unit' | 'precision'>[] | null
  groups?: Pick<CatalogGroup, 'id' | 'label'>[] | null
}

/** The slice of the aggregates payload each dashboard section reads. */
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

export function buildChartLayout({
  trafficData,
  aggregates,
  metric,
  startTime,
  endTime,
  timezone,
  catalog,
}: BuildChartLayoutParams): PlotlyLayout {
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
      exponentformat: 'none'
    },
    xaxis: makeTimeXAxis(startTime, endTime, timezone),
  }
}

export const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'

type TopBots = components['schemas']['SecurityTopBotsResponse']

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

export function CardGrid({
  visibleCardList,
  isReady,
  isFetchingAggs,
  aggregates,
  compareAggregates,
  compareMode,
  topBotsData,
  topBotsError,
  onRetryTopBots,
  compareError,
  onRetryCompare,
  collapsedSections,
  toggleSectionCollapsed,
  onRowClick,
}: CardGridProps) {
  // ── Aggregation cards ── //
  // When the catalog query hasn't returned yet ``visibleCardList`` is
  // empty (it's ``allCards.filter(c => visibleCards.has(c.id))`` and
  // allCards is [] until catalog loads). Render the section structure
  // from CARD_CATEGORIES — a STATIC const — so the cards section
  // always occupies its eventual vertical space. Without this, the
  // section is completely absent during the catalog-loading gap and
  // the raw-logs table (which loads ~500 ms faster) renders at the
  // top and then gets shoved DOWN by ~3000-4000 px when the real
  // cards arrive. That's the "page jumps" UX bug the user
  // reported 2026-06-06.
  //
  // The skeleton renders ALL categories at their full default card
  // count. When real data arrives, hidden categories collapse (a
  // small downward adjustment) but the gross layout is already
  // reserved. Most users haven't hidden any categories so the
  // swap is invisible.
  if (visibleCardList.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        {CARD_CATEGORIES.map((cat) => (
          <section
            key={`skel-${cat.id}`}
            className={cn("rounded-lg border", cat.tint.bg, cat.tint.border)}
          >
            <div className="w-full flex items-center gap-2 px-4 py-2.5">
              <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
              <span className={cn("inline-block w-1.5 h-1.5 rounded-full", cat.tint.dot)} />
              <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground">
                {cat.label}
              </h3>
              <span className="text-[10px] text-muted-foreground/60 font-mono">
                {cat.cardIds.length}
              </span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1">
              {cat.cardIds.map((id) => (
                <div
                  key={`skel-${cat.id}-${id}`}
                  data-empty-placeholder="true"
                  className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20 [content-visibility:auto] [contain-intrinsic-size:300px]"
                >
                  <span className="text-muted-foreground text-xs animate-pulse">
                    {!isReady ? 'Initializing...' : 'Loading...'}
                  </span>
                </div>
              ))}
            </div>
          </section>
        ))}
      </div>
    )
  }

  const visibleById = new Map(visibleCardList.map((c) => [c.id, c]))
  // Wrap each card in LazyMount so the FIRST dashboard paint
  // only mounts the cards above the fold (~5-10) instead of
  // all 86. Off-screen cards land as the user scrolls — the
  // rootMargin of 600px (one screen) pre-mounts before the
  // user actually reaches them, so they feel instant. Cuts
  // initial DOM nodes from ~860 to ~100 and skips ~80
  // TopTenTable mount cycles on first render. The loading
  // placeholder branch is NOT wrapped — it's already cheap
  // and we want every "Initializing..." tile visible.
  const renderCard = (card: DashboardCard) => {
    // Show "Loading…" whenever aggregates haven't arrived yet — covers the
    // gap between catalog-loaded (visibleCardList populated) and the aggs
    // query actually firing (isLoadingAggs is false but data is still
    // undefined). Without this, individual cards flash "No data available"
    // for a beat before the real data lands.
    const isBotCard = card.id === '_bot_name' || card.id === '_ngwaf_bot_name'

    // A top-bots 5xx leaves topBotsData undefined forever. Without this branch
    // the card below would sit on its "Loading…" placeholder indefinitely
    // (isCardLoading stays true), indistinguishable from a slow fetch. Surface
    // the error + a Retry instead.
    if (isBotCard && topBotsError) {
      return (
        <CardErrorState
          key={card.id}
          icon={<Bot className="h-4 w-4" />}
          title={card.label}
          message="Failed to load bot data."
          onRetry={() => onRetryTopBots?.()}
        />
      )
    }

    const isCardLoading =
      !isReady ||
      !aggregates ||
      (isBotCard && !topBotsData)

    if (isCardLoading) {
      return (
        <div key={card.id} data-empty-placeholder="true" className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20 [content-visibility:auto] [contain-intrinsic-size:300px]">
          <span className="text-muted-foreground text-xs animate-pulse">
            {!isReady ? 'Initializing...' : 'Loading...'}
          </span>
        </div>
      )
    }
    if (card.id === '_bot_name') {
      return (
        <LazyMount key={card.id} minHeight={300}>
          <TopTenTable
            title={card.label}
            icon={<Bot className="h-4 w-4" />}
            field="_bot_name"
            inActiveFormat={card.inActiveFormat}
            data={{
              total: topBotsData?.bots?.reduce((acc, b) => acc + b.request_count, 0) || 0,
              top: (topBotsData?.bots ?? []).map((b) => ({ value: b.id ?? b.name, label: b.name, count: b.request_count }))
            }}
            compareData={undefined}
            onRowClick={onRowClick}
          />
        </LazyMount>
      )
    }
    if (card.id === '_ngwaf_bot_name') {
      return (
        <LazyMount key={card.id} minHeight={300}>
          <TopTenTable
            title={card.label}
            field="_ngwaf_bot_name"
            inActiveFormat={card.inActiveFormat}
            data={{
              total: (topBotsData?.ngwaf_bots ?? []).reduce((acc, b) => acc + b.request_count, 0),
              top: (topBotsData?.ngwaf_bots ?? []).map((b) => ({ value: b.name, label: b.name, count: b.request_count }))
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
  if (customCards.length > 0) {
    sections.push({ id: 'custom', label: 'Custom', cardIds: [], cards: customCards, tint: CUSTOM_TINT })
  }

  return (
    <div className={cn("flex flex-col gap-4 transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
      {compareMode && compareError ? (
        <div
          role="alert"
          className="flex items-center justify-between gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
        >
          <span>Failed to load comparison data — cards show the current period only.</span>
          {onRetryCompare && (
            <button
              type="button"
              onClick={() => onRetryCompare()}
              className="rounded border border-destructive/40 px-2 py-0.5 hover:bg-destructive/10"
            >
              Retry
            </button>
          )}
        </div>
      ) : null}
      {sections.map(section => {
        const isCollapsed = collapsedSections.has(section.id)
        const Chevron = isCollapsed ? ChevronRight : ChevronDown
        return (
          <section
            key={section.id}
            className={cn("rounded-lg border", section.tint.bg, section.tint.border)}
          >
            <button
              type="button"
              onClick={() => toggleSectionCollapsed(section.id)}
              aria-expanded={!isCollapsed}
              aria-controls={`section-${section.id}-cards`}
              className="w-full flex items-center gap-2 px-4 py-2.5 text-left hover:bg-black/[0.02] dark:hover:bg-white/[0.03] rounded-t-lg transition-colors group"
            >
              <Chevron className="h-3.5 w-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
              <span className={cn("inline-block w-1.5 h-1.5 rounded-full", section.tint.dot)} />
              <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground group-hover:text-foreground transition-colors">
                {section.label}
              </h3>
              <span className="text-[10px] text-muted-foreground/60 font-mono">
                {section.cards.length}
              </span>
            </button>
            {!isCollapsed && (
              <div
                id={`section-${section.id}-cards`}
                className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1"
              >
                {section.cards.map((card) => renderCard(card))}
              </div>
            )}
          </section>
        )
      })}
    </div>
  )
}
