'use client'

import React from 'react'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { client } from '@/lib/api'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterPayload } from '@/hooks/useFilterPayload'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates } from '@/lib/staleViewRetry'
import { formatValue } from '@/lib/format'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { BarChart3, EyeOff } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTheme } from 'next-themes'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ReportShell } from '@/components/ReportShell'
import { useUrlFilterSync } from '@/hooks/useUrlFilterSync'
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

  const { startTime, endTime } = useFilterStore()
  const { activeServiceId } = useServiceStore()
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  const { visibleCards, toggleCard, reset: resetCards } = useCardVisibility(
    VISIBILITY_KEY,
    chartCards.map((c: any) => c.id),
    chartCards.filter((c: any) => c.inActiveFormat).map((c: any) => c.id),
  )

  const filterPayload = useFilterPayload()

  useUrlFilterSync()

  const { data: aggregates, isLoading, isFetching } = useServiceQuery(
    ['charts', 'aggregates', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const { data } = await client.POST("/api/dashboard/aggregates", { signal,
        body: {
          start_time: startTime,
          end_time: endTime,
          filters: filterPayload,
          chart_interval: '1 hour',
          chart_metric: 'requests'
        }
      })
      return throwIfStaleAggregates(data)
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

  const isLoadingInitial = isLoading || (isFetching && !aggregates)

  const headerActions = (
    <div className="flex items-center gap-3">
      {isFetching && !isLoadingInitial && (
        <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-wider animate-pulse">
          <span className="w-1.5 h-1.5 rounded-full bg-primary" />
          Updating
        </div>
      )}
      <Popover>
        <PopoverTrigger render={<Button variant="outline" size="sm" className="h-9 gap-1.5" />}>
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
              className="h-[400px]"
              contentClassName="flex flex-col justify-center min-h-0 relative"
            >
              {cardData.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
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
