'use client'

import React from 'react'
import { useReportRangeAnchor } from '@/hooks/useReportRangeAnchor'
import { useRouter } from 'next/navigation'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { useQuery } from '@tanstack/react-query'
import { useDashboardBundle, type DashboardSection } from '@/hooks/useDashboardBundle'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import type { ChartMetric } from '@/types/api'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates, isStaleDashboardViewError } from '@/lib/staleViewRetry'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { DashboardHeader } from '@/components/Dashboard/DashboardHeader'
import { Button } from '@/components/ui/button'
import { parseFromInput } from '@/lib/date'
import { LayoutDashboard, ArrowRight } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { buildServiceHref } from '@/lib/navigation'
import { useShallow } from 'zustand/react/shallow'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useDashboardCards } from '@/hooks/useDashboardCards'


import { TrafficChart } from './TrafficChart'
import { GeoMap } from './GeoMap'
import { CardGrid } from './CardGrid'
import { buildTrafficData, buildChartLayout } from './chartHelpers'
import { buildTrafficDataAsync } from '@/lib/workers/buildTrafficData'
import { COLLAPSED_SECTIONS_KEY } from './categories'
import { useLocalStorageString } from '@/hooks/useLocalStorageString'
import type { PlotlyRelayoutEvent, PlotlyTrace } from '@/types/plotly'
import type { DashboardBodyProps } from './types'

// P-4 slice 3: mirror the section-selector pattern used by /security and
// /network. The dashboard page consumes all three sections, so the list
// is constant — declaring it explicitly is what lets the backend know to
// keep emitting every block (and what positions us to drop sections per
// feature flag without an API change).
const DASHBOARD_SECTIONS: DashboardSection[] = ['core', 'topten', 'bots']

// ── DashboardBody ──────────────────────────────────────────────────────────────
//
// Lifted out of the ReportLayout render-prop so all hooks (useQuery,
// useServiceQuery, useState, useMemo, useCallback) live at the top of a
// stable component. Before the lift, the render-prop child was an arrow
// function recreated on every parent re-render, which violated the rules
// of hooks and caused the local-dev duplicate-fetch pattern flagged in
// the Phase 0 audit. Same shape as InsightsBody (item 31, commit 7329f02).
//
// Card visibility (`allCards`, `visibleCards`) stays in DashboardClient so
// the header's DashboardHeader can drive the toggles; both are passed
// down here for the cards grid.
function DashboardBody({
  startTime,
  endTime,
  timezone,
  activeServiceId,
  filterPayload,
  config,
  metric: metricProp,
  setMetric: setMetricProp,
  trend,
  setTrend,
  intervalButtons,
  allCards,
  visibleCards,
  relativeRange,
  isAutoRange,
  anchor,
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

  const [localMetric, setLocalMetric] = React.useState("requests")
  const metric = metricProp ?? localMetric
  const setMetric = setMetricProp ?? setLocalMetric
  const router = useRouter()

  // Hidden legend categories are scoped to the metric they were hidden under: switching metric
  // drops them (derived, so no reset effect), and toggling re-scopes the set to the current one.
  const [hiddenState, setHiddenState] = React.useState<{ metric: string; set: ReadonlySet<string> }>(
    () => ({ metric: 'requests', set: new Set() }),
  )
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

  // Collapsed-section state, persisted to localStorage so user's choices stick
  // across reloads. SSR-safe: start EMPTY so the server and the first client
  // render agree (reading localStorage in the initializer diverged the first
  // client render from the server for any user who had collapsed a section →
  // React #418). The persisted set is loaded right after mount.
  const [collapsedRaw, writeCollapsedRaw] = useLocalStorageString(COLLAPSED_SECTIONS_KEY)
  const collapsedSections = React.useMemo<Set<string>>(() => {
    try {
      return collapsedRaw ? new Set<string>(JSON.parse(collapsedRaw)) : new Set<string>()
    } catch {
      return new Set<string>()
    }
  }, [collapsedRaw])

  const toggleSectionCollapsed = React.useCallback((id: string) => {
    const next = new Set(collapsedSections)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    writeCollapsedRaw(JSON.stringify([...next]))
  }, [collapsedSections, writeCollapsedRaw])

  const isReady = useIsDataReady()

  // Composite /api/dashboard/bundle returns full aggregates +
  // security/top-bots in ONE round-trip. Reading aggregates + top_bots
  // directly off bundleQuery.data (instead of going through separate
  // useQuery calls that read seeded cache keys) avoids the React Query
  // staleTime gotcha that would otherwise make a second useQuery
  // refetch on mount despite the cache being warm — turning what
  // should be a one-request page back into a two-request page. It is
  // also what lets the whole page paint from one cached bundle: the bundle
  // carries top_bots too, so the bots cards don't fire their own request.
  // The dashboard is NOT SSR-seeded (app/dashboard/page.tsx is a plain
  // client wrapper); the bundle is fetched client-side on mount.
  //
  // Compare-mode keeps its own dedicated /api/dashboard/aggregates
  // call below — it only fires when the user explicitly enables
  // compare, so it's not part of the cold-load path.
  const bundleQuery = useDashboardBundle({
    startTime,
    endTime,
    relativeRange,
    isAutoRange,
    anchor,
    filterPayload,
    metric,
    interval: config.effectiveInterval,
    enabled: isReady,
    sections: DASHBOARD_SECTIONS,
  })

  const aggregates = bundleQuery.data?.aggregates
  // A surviving stale-view symptom (e.g. a fresh install whose first
  // Iceberg view / rollup build outlasts the fast retry budget) is benign
  // and self-resolving — STALE_VIEW_RETRY_OPTIONS keeps polling until the
  // view is consistent. Treat it as "still preparing" rather than a hard
  // failure: no scary red banner, and keep the chart/cards on their
  // loading skeleton instead of flashing "No data available".
  const isStalePreparing = bundleQuery.isError && isStaleDashboardViewError(bundleQuery.error)
  const hasDashboardData = Boolean(aggregates)
  const isLoadingAggs = bundleQuery.isLoading || (isStalePreparing && !hasDashboardData)
  const isFetchingAggs = bundleQuery.isFetching && !isStalePreparing

  const { data: compareAggregates, error: compareError, refetch: refetchCompare } = useQuery({
    queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: compareStartTime!,
          end_time: compareEndTime!,
          filters: filterPayload,
          chart_metric: metric as ChartMetric,
          chart_interval: config.effectiveInterval
        }
      }))
      return throwIfStaleAggregates(
        data,
        { startTime: compareStartTime, endTime: compareEndTime },
        Object.keys(filterPayload).length > 0,
      )
    },
    enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
    ...STALE_VIEW_RETRY_OPTIONS,
  })

  // top_bots rides in the composite bundle response (one round-trip) and is
  // read directly off bundleQuery.data — same pattern as aggregates above — so
  // the cached bundle paints the bots cards with NO separate fetch. (/api/security/top-bots has no keyed path, so a dedicated query keyed
  // on the server-reproducible token isn't available; reading off the bundle is
  // the single-fetch win. Error/retry fall
  // back to the bundle's own state, identical to how aggregates errors surface.)
  const topBotsData = bundleQuery.data?.top_bots
  const topBotsError = bundleQuery.error
  const refetchTopBots = bundleQuery.refetch

  // ── Chart data ────────────────────────────────────────────────────────────
  //
  // Two paths:
  //   - Small datasets (24h @ 1-min ≈ 1440 points, default): sync via
  //     useMemo. Cheap. Render path unchanged.
  //   - Large datasets (7d/30d, especially with trend windowing which
  //     is O(n²)): async via Web Worker so the transform doesn't block
  //     React's render loop. buildTrafficDataAsync() picks the right
  //     path based on n.
  //
  // The useState + effect is the smallest change that lets the same
  // render tree consume both sync and async results. Initial value is
  // [] so the chart shows the existing skeleton/empty state during
  // the first worker round-trip, then re-renders with traces.

  const trafficParams = React.useMemo(
    () => ({
      aggregates,
      compareAggregates,
      compareMode,
      compareStartTime,
      startTime,
      endTime,
      trend,
      timezone,
      metric,
      effectiveInterval: config.effectiveInterval,
      hiddenCategories,
      catalog,
    }),
    [aggregates, compareAggregates, compareMode, compareStartTime, startTime, endTime, trend, timezone, metric, config.effectiveInterval, hiddenCategories, catalog],
  )

  // The traces last produced for a given params object. While the worker round-trip for the
  // CURRENT trafficParams is in flight, `transformed.params !== trafficParams` — that is the
  // pending signal TrafficChart uses to keep the skeleton up instead of flashing "No data
  // available" before the transform has actually produced traces.
  const [transformed, setTransformed] = React.useState<{ params: typeof trafficParams; traces: PlotlyTrace[] }>(
    () => ({ params: trafficParams, traces: buildTrafficData(trafficParams) }),
  )
  const trafficData = transformed.traces
  const transformPending = transformed.params !== trafficParams

  React.useEffect(() => {
    if (transformed.params === trafficParams) return
    let cancelled = false
    buildTrafficDataAsync(trafficParams)
      .then((traces) => {
        // Avoid landing a stale result after a fast user filter
        // change: only commit if this effect's params are still the
        // active ones.
        if (!cancelled) setTransformed({ params: trafficParams, traces })
      })
      .catch(() => {
        // Async failure path falls back to sync (matches the
        // worker-construction fallback inside buildTrafficDataAsync
        // for the case where the promise rejected for a real reason).
        if (!cancelled) setTransformed({ params: trafficParams, traces: buildTrafficData(trafficParams) })
      })
    return () => {
      cancelled = true
    }
  }, [trafficParams, transformed.params])

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

  const handleChartRelayout = React.useCallback((event: PlotlyRelayoutEvent) => {
    // Skip non-range events (autorange toggle, spike config, etc.)
    if (event['xaxis.autorange'] === true || event['xaxis.showspikes'] !== undefined) return

    const nested = Array.isArray(event['xaxis.range']) ? event['xaxis.range'] : []
    const bound = (flat: unknown, fallback: unknown): string | number | undefined => {
      const v = flat ?? fallback
      return typeof v === 'string' || typeof v === 'number' ? v : undefined
    }
    const x0 = bound(event['xaxis.range[0]'], nested[0])
    const x1 = bound(event['xaxis.range[1]'], nested[1])

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
    () => allCards.filter((c) => visibleCards.has(c.id)),
    [allCards, visibleCards]
  )

  return (
    <>
      {/* Surface a bundle fetch failure inline. Without this the chart +
          every card sit forever on their "Crunching logs…"/"Loading…"
          placeholder, indistinguishable from data still loading, with no
          retry. Mirrors the /sessions banner. STALE_VIEW_RETRY_OPTIONS
          does not retry non-stale 5xx, so Retry is the recovery path.

          The stale-view symptom is benign (the view is catching up to
          freshly-ingested data) and STALE_VIEW_RETRY_OPTIONS auto-polls
          until it clears, so show a calm "preparing" notice for it rather
          than the red error banner reserved for real failures. */}
      {isStalePreparing ? (
        <div
          role="status"
          className="mb-6 flex items-center gap-2 rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100"
        >
          <span className="h-2 w-2 animate-pulse rounded-full bg-amber-500" aria-hidden />
          <span>Preparing your data — newly ingested logs are still being indexed. This will refresh automatically.</span>
        </div>
      ) : bundleQuery.isError ? (
        <div
          role="alert"
          className="mb-6 flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100"
        >
          <div className="font-semibold">Failed to load dashboard data.</div>
          <div className="font-mono text-xs opacity-80 break-all">{extractApiError(bundleQuery.error)}</div>
          <button
            type="button"
            onClick={() => { void bundleQuery.refetch() }}
            className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
          >
            Retry
          </button>
        </div>
      ) : null}

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
          transformPending={transformPending}
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
        topBotsError={topBotsError}
        onRetryTopBots={() => { void refetchTopBots() }}
        compareError={compareError}
        onRetryCompare={() => { void refetchCompare() }}
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

// ── Client ───────────────────────────────────────────────────────────────────

export default function DashboardClient({ nowServerStr }: { nowServerStr?: string }) {
  // Persist filter state to URL so back-nav, refresh, and shared links
  // all round-trip the user's current dashboard view. Hydration happens
  // in AppLayout.

  const allCards = useDashboardCards()
  const allCardIds = React.useMemo(() => allCards.map((c) => c.id), [allCards])
  const inFormatCardIds = React.useMemo(
    () => allCards.filter((c) => c.inActiveFormat).map((c) => c.id),
    [allCards],
  )

  const { visibleCards, toggleCard, showAll, reset: resetCards } = useCardVisibility(
    'dashboard_cards',
    allCardIds,
    inFormatCardIds,
  )
  const visibleCardsCount = React.useMemo(
    () => allCards.reduce((acc, c) => acc + (visibleCards.has(c.id) ? 1 : 0), 0),
    [allCards, visibleCards],
  )

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
          startTime={ctx.startTime}
          endTime={ctx.endTime}
          timezone={ctx.timezone}
          activeServiceId={ctx.activeServiceId}
          filterPayload={ctx.filterPayload}
          config={ctx.config}
          metric={ctx.metric}
          setMetric={ctx.setMetric}
          trend={ctx.trend}
          setTrend={ctx.setTrend}
          intervalButtons={ctx.intervalButtons}
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
