'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { useRouter } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import type { ChartMetric } from '@/types/api'
import type { components } from '@/types/api.generated'
import type { FiltersPayload } from '@/types/filters'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates, isStaleDashboardViewError } from '@/lib/api'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { useAnalyticsReportQuery } from '@/hooks/useDashboardCards'
import { useIsDataReady } from '@/hooks/useBootstrap'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { LayoutDashboard, ArrowRight, EyeOff } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { useShallow } from 'zustand/react/shallow'
import { useActiveLogFields, useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { useDashboardCards, useCardVisibility, usePlotlyRangeZoom } from '@/hooks/useDashboardCards'
import { cn } from '@/lib/utils'


import { TrafficChart } from './TrafficChart'
import {
  CardGrid,
  buildChartLayout,
  CARD_CATEGORIES,
  CATEGORIZED_CARD_IDS,
  CUSTOM_TINT,
  COLLAPSED_SECTIONS_KEY,
  type DashboardAggregates,
  type DashboardBodyProps,
  type DashboardCatalog,
  type CardTint,
  type CardCategory,
} from './CardGrid'
export {
  buildChartLayout,
  CARD_CATEGORIES,
  CATEGORIZED_CARD_IDS,
  CUSTOM_TINT,
  COLLAPSED_SECTIONS_KEY,
  type DashboardAggregates,
  type DashboardBodyProps,
  type DashboardCatalog,
  type CardTint,
  type CardCategory,
}
import { buildTrafficData, type BuildTrafficDataParams } from '@/lib/traffic-chart'
import { useLocalStorageString } from '@/hooks/useSSE'
import type { PlotlyTrace } from '@/types/plotly'

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
      if (event.data?.success) {
        resolve(event.data.data)
      } else {
        reject(new Error(event.data?.error ?? 'chartDataWorker failed'))
      }
    }
    worker.onerror = () => {
      worker.terminate()
      try {
        resolve(buildTrafficData(params))
      } catch (sync) {
        reject(sync)
      }
    }
    worker.postMessage(params)
  })
}

/**
 * Composite dashboard fetch — /api/dashboard/bundle returns the two
 * queries the dashboard page mounts on every cold load (aggregates +
 * security/top-bots) in a single round-trip.
 *
 * Pattern: the bundle's queryFn seeds the SAME cache keys the
 * existing dedicated hooks use, then returns the merged result.
 * The dedicated hooks gate on this query being in-flight, so:
 *   - cold load: bundle fires → seeds caches → dedicated hooks read
 *     cache, no fetch.
 *   - warm cache (returning to dashboard): bundle hits its own cache,
 *     same seed re-applies (no-op for unchanged data), dedicated
 *     hooks already had cache.
 *
 * Saves one RTT per cold dashboard load on prod (~150-200 ms via
 * Caddy + Fastly).
 *
 * Compare mode keeps its own dedicated /api/dashboard/aggregates
 * call — it only fires when the user explicitly enables compare, so
 * it's not part of the cold-load path.
 */
/**
 * P-4 slice 3: section-selector alias mirroring /security and /network.
 * The dashboard bundle's three logical sections — 'core' (time_series +
 * map_data + conn_requests + total_rows), 'topten' (the ~85 top-N cards),
 * and 'bots' (top_bots) — are forwarded to the backend so future server-
 * side gating can suppress unwanted blocks without an API rev. Until the
 * backend honors the field it's a no-op: the bundle still returns the
 * full payload and seeds the same cache keys.
 *
 * The bundle stays a single round-trip rather than fanning out into
 * three parallel POSTs because (1) execute_top_n_rollups does ONE
 * merged scan across the entire field batch — splitting top-N across
 * HTTP requests would re-enumerate the rollup directory and rebuild
 * the active-hour live_temp per request, and (2) the existing seeding
 * contract that other dedicated hooks rely on would need a wider
 * migration.
 */
export type DashboardSection = NonNullable<components['schemas']['AggregatesRequest']['sections']>[number]

interface DashboardBundleArgs {
  // startTime/endTime drive the stale-view extents heuristic + the chart's
  // x-axis display range, AND (in custom-range mode) the scan window itself.
  startTime: string | null
  endTime: string | null
  // Time-range wire inputs (lib/range-wire.ts). In token mode the bundle key +
  // body key on the server-reproducible relative token + quantized anchor so the
  // SSR seed key byte-matches the client first-paint key. In custom-absolute
  // mode (relativeRange null + isAutoRange false) the key/body carry the explicit
  // start/end bounds instead, so a custom range scans exactly what it displays.
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
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  filterPayload,
  metric,
  interval,
  enabled,
  fields,
  sections,
}: DashboardBundleArgs) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // Token mode (preset pill / cold-load default) keys on the server-reproducible
  // (rangeKey, anchor) so the SSR seed in app/dashboard/page.tsx byte-matches and
  // paints from the dehydrated cache on first paint. Custom-absolute mode keys on
  // an "abs:<start>|<end>" identity so distinct custom ranges don't collide.
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
          body: {
            // Token mode → {range_token, anchor} + display bounds (server
            // resolves the window from the token and ignores the bounds; they
            // only matter if the token isn't recognized — see lib/range-wire.ts).
            // Custom mode → {start_time, end_time} only.
            filters: filterPayload,
            chart_metric: metric as ChartMetric,
            chart_interval: interval,
            fields: fields,
            sections: sections,
            ...rangeBody,
          },
        }),
      )
      if (body?.aggregates) {
        // Stale-view guard: throws if the response is the empty-schema
        // placeholder from a mid-commit window — STALE_VIEW_RETRY_OPTIONS retries
        // once. The window passed here is the DISPLAY range (startTime/endTime);
        // it only steers the "empty because outside data extents" heuristic, not
        // the scan (which the server resolved from the token). Returned in-place
        // on body.aggregates — the page reads aggregates + top_bots directly off
        // bundleQuery.data (no separate cache keys), so no setQueryData fan-out.
        throwIfStaleAggregates(
          body.aggregates,
          { startTime, endTime },
          Object.keys(filterPayload).length > 0,
        )
      }
      return body
    },
    queryOptions: {
      enabled,
      ...STALE_VIEW_RETRY_OPTIONS,
      refetchIntervalInBackground: false,
    },
  })
}

// ChoroplethMap pulls in d3-geo and the world-110m topojson. Static-import
// blocked the dashboard's initial JS parse/eval; dynamic-import slices it
// off the critical path so the rest of the page paints immediately.
// ssr:false because d3-geo uses canvas/SVG measurement APIs that don't
// work in the server-render pass.
const ChoroplethMap = dynamic(
  () => import('@/components/Map/ChoroplethMap').then((m) => ({ default: m.ChoroplethMap })),
  {
    ssr: false,
    loading: () => (
      <div
        data-empty-placeholder="true"
        className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded"
        aria-busy="true"
      >
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

export function GeoMap({
  isReady,
  isFetchingAggs,
  aggregates,
  catalog,
  onCountryClick,
}: GeoMapProps) {
  // Distinguish "field group not enabled" from "enabled but no data in this
  // window yet" so a low-traffic/fresh service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  return (
    <div className={cn("border rounded-lg p-4 flex flex-col transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
      {/* text-foreground anchors the card title to the high-contrast
          token — without it the h3 inherited a muted color from a
          parent and axe flagged it (2.7:1, below AA 4.5:1). */}
      <h3 className="text-sm font-medium mb-4 text-foreground">Requests by Country</h3>
      {(!isReady || !aggregates) || (isFetchingAggs && (!aggregates?.map_data || aggregates.map_data.length === 0)) ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
          <span className="text-muted-foreground text-sm animate-pulse">
            {!isReady ? 'Initializing...' : 'Mapping traffic...'}
          </span>
        </div>
      ) : !aggregates?.map_data || aggregates.map_data.length === 0 ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
          {/* text-foreground (not text-muted-foreground) + remove
              opacity-70 on the detail line — both were below AA
              contrast on bg-muted/10. */}
          <div className="flex flex-col items-center text-foreground text-center px-4">
            <span className="text-sm font-medium mb-1">No data available</span>
            <span className="text-[10px] text-muted-foreground">
              {(() => {
                if (isFieldActive('country')) {
                  return "No country data in this time range."
                }
                const countryField = catalog?.fields?.find((f) => f.id === 'country')
                const groupId = countryField?.group
                if (groupId) {
                  const groupMeta = catalog?.groups?.find((g) => g.id === groupId)
                  if (groupMeta) {
                    return `Requires ${groupMeta.label} fields to be enabled in Fastly logging.`
                  }
                }
                return "Requires Geolocation fields to be enabled in Fastly logging."
              })()}
            </span>
          </div>
        </div>
      ) : (
        <ChoroplethMap
          data={aggregates?.map_data || []}
          className="flex-1 min-h-[300px]"
          onCountryClick={onCountryClick}
        />
      )}
    </div>
  )
}

interface DashboardHeaderProps {
  visibleCardsCount: number
  allCards: Array<{ id: string, label: string, inActiveFormat?: boolean }>
  visibleCards: Set<string>
  onToggleCard: (id: string) => void
  onShowAll: () => void
  onResetCards: () => void
}

export function DashboardHeader({
  visibleCardsCount,
  allCards,
  visibleCards,
  onToggleCard,
  onShowAll,
  onResetCards
}: DashboardHeaderProps) {
  return (
    <Popover>
      {/* aria-label lives on the PopoverTrigger directly (not on
          the Button render-prop) so it survives SSR — Base UI's
          render-prop merge drops aria-label from the inner element
          on the server pass, leaving the SSR'd button with no
          accessible name. The visible "Cards" text is
          `hidden sm:inline` so on <640px viewports it's empty too —
          the label is the only reliable name across both states. The
          name must contain the visible text ("Cards" + count, separated
          by the {' '} text node) or axe flags label-content-name-mismatch. */}
      <PopoverTrigger
        aria-label={`Cards ${visibleCardsCount}, toggle visible dashboard cards`}
        render={
          <Button variant="outline" size="sm" className="h-9 gap-1.5">
            <span className="flex items-center gap-1.5">
              <LayoutDashboard className="h-4 w-4" />
              <span className="hidden sm:inline text-xs">Cards</span>
              {' '}
              <Badge variant="secondary" className="h-4 text-[10px] px-1.5">
                {visibleCardsCount}
              </Badge>
            </span>
          </Button>
        }
      />
      <PopoverContent align="end" className="w-72 p-3">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Visible cards
        </p>
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
                    {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so
                        keyboard users can focus the EyeOff icon and reveal
                        the tooltip. A Button here would intercept clicks
                        meant for the parent Label/Checkbox. */}
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
                    <TooltipContent>
                      <p className="text-[10px]">Not in active log format.</p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </Label>
            </div>
          ))}
        </div>
        <div className="flex gap-2 mt-4 pt-3 border-t">
          <Button
            variant="ghost" size="sm" className="text-xs h-7 flex-1"
            onClick={() => React.startTransition(() => onShowAll())}
          >
            Show all
          </Button>
          <Button
            variant="ghost" size="sm" className="text-xs h-7 flex-1"
            onClick={() => React.startTransition(() => onResetCards())}
          >
            Reset
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}

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

  const handleChartRelayout = usePlotlyRangeZoom(timezone, setRange)

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
