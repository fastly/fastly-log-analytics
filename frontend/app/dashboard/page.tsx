'use client'

import React from 'react'
import { useRouter } from 'next/navigation'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useDashboardBundle, type DashboardSection } from '@/hooks/useDashboardBundle'
import { useQueryClient } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import type { ChartMetric } from '@/types/api'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates, isStaleDashboardViewError } from '@/lib/staleViewRetry'
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
import { buildTrafficDataAsync } from '@/lib/workers/buildTrafficData'
import { COLLAPSED_SECTIONS_KEY } from './_sections/categories'
import type { DashboardBodyProps } from './_sections/types'

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
  // across reloads. SSR-safe: start EMPTY so the server and the first client
  // render agree (reading localStorage in the initializer diverged the first
  // client render from the server for any user who had collapsed a section →
  // React #418). The persisted set is loaded right after mount.
  const [collapsedSections, setCollapsedSections] = React.useState<Set<string>>(new Set())
  React.useEffect(() => {
    try {
      const raw = localStorage.getItem(COLLAPSED_SECTIONS_KEY)
      if (raw) setCollapsedSections(new Set<string>(JSON.parse(raw)))
    } catch {
      /* ignore malformed / unavailable storage */
    }
  }, [])

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
  const queryClient = useQueryClient()

  // Composite /api/dashboard/bundle returns full aggregates +
  // security/top-bots in ONE round-trip. Reading aggregates directly
  // off bundleQuery.data (instead of going through a separate
  // useQuery that reads the seeded cache) avoids the React Query
  // staleTime gotcha that would otherwise make the second useQuery
  // refetch on mount despite the cache being warm — turning what
  // should be a one-request page back into a two-request page.
  //
  // Compare-mode keeps its own dedicated /api/dashboard/aggregates
  // call below — it only fires when the user explicitly enables
  // compare, so it's not part of the cold-load path.
  const bundleQuery = useDashboardBundle({
    startTime,
    endTime,
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
  const isLoadingAggs = bundleQuery.isLoading || isStalePreparing
  const isFetchingAggs = bundleQuery.isFetching

  const { data: compareAggregates, error: compareError, refetch: refetchCompare } = useQuery({
    queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: compareStartTime!,
          end_time: compareEndTime!,
          filters: filterPayload,
          chart_metric: metric as ChartMetric,
          chart_interval: config.effectiveInterval
        }
      })
      return throwIfStaleAggregates(data, { startTime: compareStartTime, endTime: compareEndTime })
    },
    enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
    ...STALE_VIEW_RETRY_OPTIONS,
  })

  const { data: topBotsData, error: topBotsError, refetch: refetchTopBots } = useQuery({
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
    // Gated on the bundle fetch so cold load reads from the seeded
    // cache instead of firing its own request (perf audit D-4). The
    // bundle's queryFn calls queryClient.setQueryData on the top-bots
    // cache key, and topBots has its own dedicated cache key (unlike
    // aggregates which now reads bundleQuery.data directly), so this
    // gating + cache-seed pattern still applies for top-bots.
    enabled: isReady && bundleQuery.data !== undefined,
    placeholderData: keepPreviousData,
  })

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
      trend,
      timezone,
      metric,
      effectiveInterval: config.effectiveInterval,
      hiddenCategories,
      catalog,
    }),
    [aggregates, compareAggregates, compareMode, compareStartTime, startTime, trend, timezone, metric, config.effectiveInterval, hiddenCategories, catalog],
  )

  const [trafficData, setTrafficData] = React.useState<any[]>(() => buildTrafficData(trafficParams))
  // True while the worker round-trip for the current trafficParams is in
  // flight. TrafficChart uses this to keep the skeleton up instead of
  // flashing "No data available" when the bundle has arrived but the
  // transform hasn't produced traces yet.
  const [transformPending, setTransformPending] = React.useState(true)

  React.useEffect(() => {
    let cancelled = false
    setTransformPending(true)
    buildTrafficDataAsync(trafficParams)
      .then((traces) => {
        // Avoid landing a stale result after a fast user filter
        // change: only commit if this effect's params are still the
        // active ones.
        if (!cancelled) {
          setTrafficData(traces)
          setTransformPending(false)
        }
      })
      .catch(() => {
        // Async failure path falls back to sync (matches the
        // worker-construction fallback inside buildTrafficDataAsync
        // for the case where the promise rejected for a real reason).
        if (!cancelled) {
          setTrafficData(buildTrafficData(trafficParams))
          setTransformPending(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [trafficParams])

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
