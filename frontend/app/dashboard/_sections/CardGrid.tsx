'use client'

import React from 'react'
import { TopTenTable } from '@/components/Dashboard/TopTenTable'
import { LazyMount } from '@/components/LazyMount'
import { ChevronDown, ChevronRight, Bot } from 'lucide-react'
import { cn } from '@/lib/utils'
import { CARD_CATEGORIES, CATEGORIZED_CARD_IDS, CUSTOM_TINT } from './categories'

export interface CardGridProps {
  visibleCardList: any[]
  isReady: boolean
  isLoadingAggs: boolean
  isFetchingAggs: boolean
  aggregates: any
  compareAggregates: any
  compareMode: boolean
  topBotsData: any
  collapsedSections: Set<string>
  toggleSectionCollapsed: (id: string) => void
  onRowClick: (column: string, value: string | number) => void
}

export function CardGrid({
  visibleCardList,
  isReady,
  isLoadingAggs,
  isFetchingAggs,
  aggregates,
  compareAggregates,
  compareMode,
  topBotsData,
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

  const visibleById = new Map(visibleCardList.map((c: any) => [c.id, c]))
  // Wrap each card in LazyMount so the FIRST dashboard paint
  // only mounts the cards above the fold (~5-10) instead of
  // all 86. Off-screen cards land as the user scrolls — the
  // rootMargin of 600px (one screen) pre-mounts before the
  // user actually reaches them, so they feel instant. Cuts
  // initial DOM nodes from ~860 to ~100 and skips ~80
  // TopTenTable mount cycles on first render. The loading
  // placeholder branch is NOT wrapped — it's already cheap
  // and we want every "Initializing..." tile visible.
  const renderCard = (card: any) => {
    // Show "Loading…" whenever aggregates haven't arrived yet — covers the
    // gap between catalog-loaded (visibleCardList populated) and the aggs
    // query actually firing (isLoadingAggs is false but data is still
    // undefined). Without this, individual cards flash "No data available"
    // for a beat before the real data lands.
    const isCardLoading =
      !isReady ||
      !aggregates ||
      ((card.id === '_bot_name' || card.id === '_ngwaf_bot_name') && !topBotsData)

    if (isCardLoading) {
      return (
        <div key={card.id} className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20 [content-visibility:auto] [contain-intrinsic-size:300px]">
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
              total: topBotsData?.bots?.reduce((acc: number, b: any) => acc + b.request_count, 0) || 0,
              top: (topBotsData?.bots ?? []).map((b: any) => ({ value: b.id, label: b.name, count: b.request_count }))
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
              total: (topBotsData?.ngwaf_bots ?? []).reduce((acc: number, b: any) => acc + b.request_count, 0),
              top: (topBotsData?.ngwaf_bots ?? []).map((b: any) => ({ value: b.name, label: b.name, count: b.request_count }))
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
    cards: cat.cardIds.map(id => visibleById.get(id)).filter(Boolean),
  })).filter(s => s.cards.length > 0)

  const customCards = visibleCardList.filter((c: any) => !CATEGORIZED_CARD_IDS.has(c.id))
  if (customCards.length > 0) {
    sections.push({ id: 'custom', label: 'Custom', cardIds: [], cards: customCards, tint: CUSTOM_TINT })
  }

  return (
    <div className={cn("flex flex-col gap-4 transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
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
                {section.cards.map((card: any) => renderCard(card))}
              </div>
            )}
          </section>
        )
      })}
    </div>
  )
}
