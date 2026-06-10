'use client'

import React from 'react'
import { useRouter } from 'next/navigation'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { client } from '@/lib/api'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates } from '@/lib/staleViewRetry'
import { useFilterStore } from '@/stores/filterStore'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { DashboardHeader } from '@/components/Dashboard/DashboardHeader'
import { Button } from '@/components/ui/button'
import { parseFromInput } from '@/lib/date'
import { LayoutDashboard, ArrowRight } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { useShallow } from 'zustand/react/shallow'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useDashboardCards } from '@/hooks/useDashboardCards'


import { TrafficChart } from './_sections/TrafficChart'
import { GeoMap } from './_sections/GeoMap'
import { CardGrid } from './_sections/CardGrid'
import { buildTrafficData, buildChartLayout } from './_sections/chartHelpers'
import { COLLAPSED_SECTIONS_KEY } from './_sections/categories'
import type { DashboardBodyProps } from './_sections/types'

// ── DashboardBody ──────────────────────────────────────────────────────────────
//
// Lifted out of the ReportLayout render-prop so all hooks (useQuery,
// useServiceQuery, useState, useMemo, useCallback) live at the top of a
// stable component. Before the lift, the render-prop child was an arrow
// function recreated on every parent re-render, which violated the rules
// of hooks and caused the local-dev duplicate-fetch pattern flagged in
// the Phase 0 audit. Same shape as InsightsBody (item 31, commit 7329f02).
//
// Card visibility (`allCards`, `visibleCards`) stays in DashboardPage so
// the header's DashboardHeader can drive the toggles; both are passed
// down here for the cards grid.
function DashboardBody({
  startTime,
  endTime,
  timezone,
  activeServiceId,
  filterPayload,
  config,
  trend,
  setTrend,
  intervalButtons,
  allCards,
  visibleCards,
}: DashboardBodyProps) {
  const { data: catalog } = useLogFieldsCatalog()

  const {
    addFilter,
    setRange,
    compareMode,
    compareStartTime,
    compareEndTime,
  } = useFilterStore(useShallow(state => ({
    addFilter: state.addFilter,
    setRange: state.setRange,
    compareMode: state.compareMode,
    compareStartTime: state.compareStartTime,
    compareEndTime: state.compareEndTime,
  })))

  const [metric, setMetric] = React.useState("requests")
  const router = useRouter()

  const [hiddenCategories, setHiddenCategories] = React.useState<Set<string>>(new Set())

  const toggleCategory = React.useCallback((cat: string) => {
    setHiddenCategories(prev => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return next
    })
  }, [])

  // Collapsed-section state, persisted to localStorage so user's choices stick
  // across reloads. Lazy initializer reads localStorage once on mount.
  const [collapsedSections, setCollapsedSections] = React.useState<Set<string>>(() => {
    if (typeof window === 'undefined') return new Set()
    try {
      const raw = localStorage.getItem(COLLAPSED_SECTIONS_KEY)
      return raw ? new Set<string>(JSON.parse(raw)) : new Set()
    } catch {
      return new Set()
    }
  })

  const toggleSectionCollapsed = React.useCallback((id: string) => {
    setCollapsedSections(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try {
        localStorage.setItem(COLLAPSED_SECTIONS_KEY, JSON.stringify([...next]))
      } catch { /* ignore quota / private-mode errors */ }
      return next
    })
  }, [])

  // Clear hidden categories when metric changes to avoid confusing states
  React.useEffect(() => {
    setHiddenCategories(new Set())
  }, [metric])

  const isReady = useIsDataReady()

  const { data: aggregates, isLoading: isLoadingAggs, isFetching: isFetchingAggs } = useServiceQuery(
    ['dashboard', 'aggregates', activeServiceId, startTime, endTime, filterPayload, metric, config.effectiveInterval],
    async ({ signal }) => {
      const { data } = await client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          chart_metric: metric as any,
          chart_interval: config.effectiveInterval
        }
      })
      return throwIfStaleAggregates(data)
    },
    STALE_VIEW_RETRY_OPTIONS,
  )

  const { data: compareAggregates } = useQuery({
    queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: compareStartTime!,
          end_time: compareEndTime!,
          filters: filterPayload,
          chart_metric: metric as any,
          chart_interval: config.effectiveInterval
        }
      })
      return throwIfStaleAggregates(data)
    },
    enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
    ...STALE_VIEW_RETRY_OPTIONS,
  })

  const { data: topBotsData } = useQuery({
    queryKey: ['dashboard', 'top-bots', activeServiceId, startTime, endTime, filterPayload],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/security/top-bots", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
        }
      })
      return data
    },
    enabled: isReady,
    placeholderData: keepPreviousData,
  })

  // ── Chart data ────────────────────────────────────────────────────────────

  const trafficData = React.useMemo(
    () => buildTrafficData({
      aggregates,
      compareAggregates,
      compareMode,
      compareStartTime,
      startTime,
      trend,
      timezone,
      metric,
      effectiveInterval: config.effectiveInterval,
      hiddenCategories,
      catalog,
    }),
    [aggregates, compareAggregates, compareMode, compareStartTime, startTime, trend, timezone, metric, config.effectiveInterval, hiddenCategories, catalog],
  )

  const chartLayout = React.useMemo(
    () => buildChartLayout({
      trafficData,
      aggregates,
      metric,
      startTime,
      endTime,
      timezone,
      catalog,
    }),
    [trafficData, aggregates, metric, startTime, endTime, timezone, catalog],
  )

  const handleRowClick = React.useCallback((column: string, value: string | number) => {
    React.startTransition(() => {
      addFilter(column, String(value), 'include')
    })
  }, [addFilter])

  const handleChartRelayout = React.useCallback((event: any) => {
    // Skip non-range events (autorange toggle, spike config, etc.)
    if (event?.['xaxis.autorange'] === true || event?.['xaxis.showspikes'] !== undefined) return

    const x0 = event?.['xaxis.range[0]'] ?? event?.['xaxis.range']?.[0]
    const x1 = event?.['xaxis.range[1]'] ?? event?.['xaxis.range']?.[1]

    if (x0 === undefined || x1 === undefined) return

    try {
      const toLocalStr = (val: string | number) => {
        if (typeof val === 'number') {
          const d = new Date(val)
          const pad = (n: number) => n.toString().padStart(2, '0')
          return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
        }
        return val.replace(' ', 'T')
      }
      const parsedStart = parseFromInput(toLocalStr(x0), timezone)
      const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
      if (parsedStart && parsedEnd) {
        setRange(parsedStart, parsedEnd)
      }
    } catch (e) {
      console.error("Failed to parse chart relayout event", e)
    }
  }, [setRange, timezone])

  const handleCountryClick = React.useCallback((countryName: string) => {
    React.startTransition(() => {
      addFilter('country', countryName, 'include')
    })
  }, [addFilter])

  const visibleCardList = React.useMemo(
    () => allCards.filter((c: any) => visibleCards.has(c.id)),
    [allCards, visibleCards]
  )

  return (
    <>
      {/* ── Main charts ── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <TrafficChart
          catalog={catalog}
          metric={metric}
          setMetric={setMetric}
          trend={trend}
          setTrend={setTrend}
          config={config}
          intervalButtons={intervalButtons}
          trafficData={trafficData}
          chartLayout={chartLayout}
          hiddenCategories={hiddenCategories}
          toggleCategory={toggleCategory}
          isReady={isReady}
          isLoadingAggs={isLoadingAggs}
          isFetchingAggs={isFetchingAggs}
          aggregates={aggregates}
          onChartRelayout={handleChartRelayout}
          startTime={startTime}
          endTime={endTime}
          timezone={timezone}
        />

        <GeoMap
          isReady={isReady}
          isLoadingAggs={isLoadingAggs}
          isFetchingAggs={isFetchingAggs}
          aggregates={aggregates}
          catalog={catalog}
          onCountryClick={handleCountryClick}
        />
      </div>

      {/* ── Aggregation cards ── */}
      <CardGrid
        visibleCardList={visibleCardList}
        isReady={isReady}
        isLoadingAggs={isLoadingAggs}
        isFetchingAggs={isFetchingAggs}
        aggregates={aggregates}
        compareAggregates={compareAggregates}
        compareMode={compareMode}
        topBotsData={topBotsData}
        collapsedSections={collapsedSections}
        toggleSectionCollapsed={toggleSectionCollapsed}
        onRowClick={handleRowClick}
      />

      {/* ── Raw logs CTA ── */}
      {/* Dashboard previously rendered a full DataTable here fed by
       *  /api/dashboard/raw, which forced a wide parquet read (~13 cols
       *  by default, expandable to ~75) on every dashboard load. The
       *  unified /query explorer now owns raw inspection; this CTA
       *  hands off the current time window + filter state via URL
       *  params so the explorer opens pre-scoped. */}
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

// ── Page ───────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  // Persist filter state to URL so back-nav, refresh, and shared links
  // all round-trip the user's current dashboard view. See
  // hydration happens in AppLayout

  const allCards = useDashboardCards()

  const { visibleCards, toggleCard, showAll, reset: resetCards } = useCardVisibility(
    'dashboard_cards',
    allCards.map((c: any) => c.id),
    allCards.filter((c: any) => c.inActiveFormat).map((c: any) => c.id),
  )

  return (
    <ReportLayout
      title="Dashboard"
      description="Drill down into traffic details and analyze request trends."
      icon={LayoutDashboard}
      defaultInterval="1 minute"
      headerActions={
        <DashboardHeader
          visibleCardsCount={visibleCards.size}
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
          startTime={ctx.startTime}
          endTime={ctx.endTime}
          timezone={ctx.timezone}
          activeServiceId={ctx.activeServiceId}
          filterPayload={ctx.filterPayload}
          config={ctx.config}
          trend={ctx.trend}
          setTrend={ctx.setTrend}
          intervalButtons={ctx.intervalButtons}
          allCards={allCards}
          visibleCards={visibleCards}
        />
      )}
    </ReportLayout>
  )
}
