'use client'

import React from 'react'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { client } from '@/lib/api'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { useServiceStore } from '@/stores/serviceStore'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates, isStaleDashboardViewError } from '@/lib/staleViewRetry'
import { formatValue } from '@/lib/format'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { BarChart3, EyeOff } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTheme } from 'next-themes'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ReportShell } from '@/components/ReportShell'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import { useViewMetricUrlSync } from '@/hooks/useViewMetricUrlSync'
import { useDashboardCards } from '@/hooks/useDashboardCards'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'

const CHART_CARD_IDS = new Set([
  'ip', 'country', 'city', 'host', 'url', 'method', 'ua', 'status', 'cache',
  'backend', 'waf', 'waf_resp', 'waf_ms', 'waf_sig', 'ja3', 'ja4', 'asn',
  'edge', 'proto', 'tls', 'referer', 'p_type', 'p_desc', 'pop',
])

const VISIBILITY_KEY = 'fastly_charts_card_visibility'

export default function ChartsPage() {
  const allCards = useDashboardCards()
  const { data: catalog } = useLogFieldsCatalog()

  const chartCards = React.useMemo(() => {
    return allCards.filter((c: any) => CHART_CARD_IDS.has(c.id))
  }, [allCards])

  // Subscribe ONLY to startTime + endTime so unrelated filterStore
  // mutations (pills, edgeOnly, compareMode) don't trigger a re-render
  // of the charts grid. Without useShallow, every filterStore tick
  // forces re-render of N PlotlyChart memos.
  const { startTime, endTime } = useFilterStore(
    useShallow(s => ({ startTime: s.startTime, endTime: s.endTime }))
  )
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  const { visibleCards, toggleCard, reset: resetCards } = useCardVisibility(
    VISIBILITY_KEY,
    chartCards.map((c: any) => c.id),
    chartCards.filter((c: any) => c.inActiveFormat).map((c: any) => c.id),
  )

  // Pass `true` so the FilterBar's "Edge only" toggle reaches the chart
  // aggregates request — same wiring as ReportLayout.
  const filterPayload = useDebouncedFilterPayload(true)

  useViewMetricUrlSync()

  const chartFields = React.useMemo(() => Array.from(CHART_CARD_IDS), [])
  const { data: aggregates, isLoading, isFetching, error } = useServiceQuery(
    ['charts', 'aggregates', activeServiceId, startTime, endTime, filterPayload, chartFields],
    async ({ signal }) => {
      const { data } = await client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: startTime,
          end_time: endTime,
          filters: filterPayload,
          chart_interval: '1 hour',
          chart_metric: 'requests',
          // Charts only renders the fields in CHART_CARD_IDS; pass the
          // explicit list so the backend's top_n_rollups only computes
          // those (vs the full ~25-field default — half of which the
          // chart page throws away). Backend already honours `fields`.
          fields: chartFields,
          // Charts also doesn't render the time-series chart, the
          // conn_requests histogram, or the world map — opting out of
          // each shaves the per-section SQL the page would never read.
          // /dashboard keeps these defaults (true).
          include_time_series: false,
          include_conn_requests: false,
          include_map_data: false,
        }
      })
      return throwIfStaleAggregates(
        data,
        { startTime, endTime },
        Object.keys(filterPayload).length > 0,
      )
    },
    STALE_VIEW_RETRY_OPTIONS,
  )

  // Stable reference so PlotlyChart's React.memo doesn't re-render every
  // card on every parent re-render (the previous inline object was a new
  // identity each render).
  const chartLayout = React.useMemo(() => ({
    showlegend: true,
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
  }), [])

  // A surviving stale-view symptom is benign and self-resolving
  // (STALE_VIEW_RETRY_OPTIONS keeps polling until the view is consistent),
  // so present it as "still loading" rather than surfacing a per-card
  // error. Common on a fresh install whose first view/rollup build
  // outlasts the fast retry budget.
  const isStalePreparing = isStaleDashboardViewError(error)
  const isLoadingInitial = isLoading || isStalePreparing || (isFetching && !aggregates)
  const cardError = isStalePreparing ? null : (error as AnalyticsCardError | null)

  const headerActions = (
    <div className="flex items-center gap-3">
      {isFetching && !isLoadingInitial && <UpdatingBadge />}
      <Popover>
        <PopoverTrigger
          aria-label="Toggle visible charts"
          render={<Button variant="outline" size="sm" className="h-9 gap-1.5" />}
        >
          <span className="flex items-center gap-1.5">
            <BarChart3 className="h-4 w-4" />
            <span className="hidden sm:inline text-xs">Charts</span>
            <Badge variant="secondary" className="h-4 text-[10px] px-1.5">
              {visibleCards.size}
            </Badge>
          </span>
        </PopoverTrigger>
        <PopoverContent align="end" className="w-64 p-3">
          <div className="p-3 border-b bg-muted/30">
            <h4 className="font-medium text-sm">Visible Charts</h4>
            <p className="text-xs text-muted-foreground">Select which distributions to show.</p>
          </div>
          <ScrollArea className="h-[300px] p-3">
            <div className="space-y-2">
              {chartCards.map((card: any) => (
                <div key={card.id} className="flex items-center gap-2">
                  <Checkbox
                    id={`toggle-${card.id}`}
                    checked={visibleCards.has(card.id)}
                    onCheckedChange={() => toggleCard(card.id)}
                  />
                  <label htmlFor={`toggle-${card.id}`} className="text-sm font-medium leading-none cursor-pointer inline-flex items-center gap-1">
                    {card.label}
                    {card.inActiveFormat === false && (
                      <Tooltip>
                        <TooltipTrigger render={<span className="inline-flex items-center" />}>
                          <EyeOff className="h-3 w-3 text-muted-foreground/70" />
                        </TooltipTrigger>
                        <TooltipContent>
                          <p className="text-[10px]">Not in active log format.</p>
                        </TooltipContent>
                      </Tooltip>
                    )}
                  </label>
                </div>
              ))}
            </div>
          </ScrollArea>
          <div className="p-2 border-t bg-muted/30">
            <Button variant="ghost" size="sm" className="w-full text-xs h-7" onClick={resetCards}>
              Reset to Defaults
            </Button>
          </div>
        </PopoverContent>
      </Popover>
    </div>
  )

  return (
    <ReportShell
      title="Distribution Charts"
      description="Visualizing the Top 10 distributions for key log fields."
      icon={BarChart3}
      headerActions={headerActions}
    >
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {chartCards.filter((c: any) => visibleCards.has(c.id)).map((card: any) => {
          const cardData = aggregates?.data?.[card.id]?.top || []

          return (
            <AnalyticsCard
              key={card.id}
              title={
                <span className="inline-flex items-center gap-1.5">
                  {card.label} (Top 10)
                  {card.inActiveFormat === false && (
                    <Tooltip>
                      <TooltipTrigger render={<span className="inline-flex items-center" />}>
                        <EyeOff className="h-3 w-3 text-muted-foreground/70" />
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="text-[10px]">Not currently being logged — showing historical data only.</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                </span>
              }
              isLoading={isLoadingInitial}
              isFetching={isFetching}
              error={cardError}
              className="h-[400px]"
              contentClassName="flex flex-col justify-center min-h-0 relative"
            >
              {cardData.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
                  {/* `card.inActiveFormat` is the authoritative "is this field
                      actually being logged?" signal (same one driving the EyeOff
                      tooltip). When the field IS in the active format there's
                      simply no data in this window yet — show a neutral message
                      instead of a misleading "Requires …" hint. */}
                  {card.inActiveFormat === false ? (
                    <>
                      <span className="text-sm font-medium mb-1">No data available</span>
                      {(() => {
                        const fieldId = card.id
                        const fieldMeta = catalog?.fields?.find(f => f.id === fieldId)
                        const groupId = fieldMeta?.group

                        if (groupId) {
                          const groupMeta = catalog?.groups?.find(g => g.id === groupId)
                          if (groupMeta) {
                            return (
                              <span className="text-[10px] opacity-70">
                                Requires {groupMeta.label} fields to be enabled in Fastly logging.
                              </span>
                            )
                          }
                        } else if (fieldId === 'ua' || fieldId === '_bot_name') {
                          return <span className="text-[10px] opacity-70">Requires User-Agent field to be enabled in Fastly logging.</span>
                        } else if (fieldId === 'ja3' || fieldId === 'ja4') {
                          return <span className="text-[10px] opacity-70">Requires TLS Fingerprinting (JA3/JA4) fields to be enabled in Fastly logging.</span>
                        }
                        return null
                      })()}
                    </>
                  ) : (
                    <span className="text-sm font-medium mb-1">No data in this time range yet.</span>
                  )}
                </div>
              ) : (
                <PlotlyChart
                  height="100%"
                  data={[{
                    type: 'pie',
                    labels: cardData.map((d: any) => {
                      const val = d.label || formatValue(card.id, d.value)
                      return val.length > 50 ? val.substring(0, 47) + '...' : val
                    }),
                    values: cardData.map((d: any) => d.count),
                    hole: 0.4,
                    textinfo: 'percent',
                    hoverinfo: 'label+value+percent',
                    marker: {
                      colors: [
                        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
                        '#ec4899', '#06b6d4', '#f97316', '#6366f1', '#14b8a6',
                      ],
                    },
                  }]}
                  layout={chartLayout}
                />
              )}
            </AnalyticsCard>
          )
        })}
      </div>
    </ReportShell>
  )
}
