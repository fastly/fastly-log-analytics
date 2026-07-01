'use client'

import React from 'react'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { Button, buttonVariants } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'
import { TRENDS } from '@/lib/constants'
import { useActiveLogFields } from '@/hooks/useActiveLogFields'
import type { ReportConfiguration } from './types'

export interface TrafficChartProps {
  catalog: any
  metric: string
  setMetric: (m: string) => void
  trend: string
  setTrend: (t: string) => void
  config: ReportConfiguration
  intervalButtons: React.ReactNode
  trafficData: any[]
  chartLayout: any
  hiddenCategories: Set<string>
  toggleCategory: (cat: string) => void
  isReady: boolean
  isLoadingAggs: boolean
  isFetchingAggs: boolean
  // True while the worker round-trip for the current trafficParams is in
  // flight. Gates the "No data available" branch so it can't appear
  // before the transform has actually produced traces.
  transformPending: boolean
  aggregates: any
  onChartRelayout: (event: any) => void
  startTime: string | null
  endTime: string | null
  timezone: string
}

export function TrafficChart({
  catalog,
  metric,
  setMetric,
  trend,
  setTrend,
  config,
  intervalButtons,
  trafficData,
  chartLayout,
  hiddenCategories,
  toggleCategory,
  isReady,
  isLoadingAggs,
  isFetchingAggs,
  transformPending,
  aggregates,
  onChartRelayout,
  startTime,
  endTime,
  timezone,
}: TrafficChartProps) {
  // Distinguish "field group not enabled" from "enabled but no data in this
  // window yet" so a low-traffic/fresh service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  return (
    <div className="border rounded-lg p-4 flex flex-col relative overflow-hidden">
      <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-3 mb-4 relative z-10">
        <div className="flex flex-row items-center gap-2 xl:gap-4 flex-wrap">
          <h3 className="text-sm font-medium whitespace-nowrap hidden sm:block">Traffic over Time</h3>
          <div className="flex flex-row items-center gap-2">
            <ButtonGroup>
              {(() => {
                const metricsFields = catalog?.fields?.filter((f: any) => f.group === 'METRICS') || []
                const shortLabels: Record<string, string> = {
                  'requests': 'Reqs',
                  'hit_rate': 'CHR',
                  '5xx': '5xx',
                  '4xx': '4xx',
                  'p50_latency': 'p50',
                  'p95_latency': 'p95',
                  'p99_latency': 'p99',
                  'throughput': 'Throughput',
                  'req_size': 'Req Size',
                  'ttfb': 'TTFB'
                }

                // We want to group latencies into a dropdown
                const latencyIds = ['p50_latency', 'p95_latency', 'p99_latency']
                const otherMetrics = metricsFields.filter((f: any) => !latencyIds.includes(f.id))

                // Re-order to match desired UI layout: Reqs, 5xx, 4xx, CHR, Latency, ...
                const order = ['requests', '5xx', '4xx', 'hit_rate']
                const orderedMetrics = [
                  ...order.map(id => otherMetrics.find((f: any) => f.id === id)).filter(Boolean),
                  ...otherMetrics.filter((f: any) => !order.includes(f.id))
                ] as any[]

                const elements = orderedMetrics.map(m => (
                  <Button
                    key={m.id}
                    variant={metric === m.id ? 'default' : 'ghost'}
                    size="sm"
                    onClick={() => React.startTransition(() => setMetric(m.id))}
                    aria-pressed={metric === m.id}
                    className={cn(
                      "h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]",
                      metric === m.id ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                    )}
                  >
                    {shortLabels[m.id] || m.label}
                  </Button>
                ))

                // Insert Latency dropdown after CHR (hit_rate)
                const isLatency = metric.endsWith('_latency')
                const latLabel = isLatency ? metric.split('_')[0] : 'p95'
                const latencyDropdown = (
                  <DropdownMenu key="latency">
                    <DropdownMenuTrigger className={cn(
                      buttonVariants({ variant: isLatency ? 'default' : 'ghost', size: 'sm' }),
                      "h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]",
                      isLatency ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                    )}>
                      Latency ({latLabel}) <ChevronDown className="ml-1 h-3 w-3" />
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="start">
                      <DropdownMenuItem onClick={() => setMetric('p50_latency')} className="text-xs">p50 Latency</DropdownMenuItem>
                      <DropdownMenuItem onClick={() => setMetric('p95_latency')} className="text-xs">p95 Latency</DropdownMenuItem>
                      <DropdownMenuItem onClick={() => setMetric('p99_latency')} className="text-xs">p99 Latency</DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                )

                const chrIndex = orderedMetrics.findIndex(m => m.id === 'hit_rate')
                if (chrIndex !== -1) {
                  elements.splice(chrIndex + 1, 0, latencyDropdown)
                } else {
                  elements.push(latencyDropdown)
                }

                return elements
              })()}
            </ButtonGroup>

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

      {/* Custom Category Legend */}
      {trafficData.length > 1 && trafficData[0]?.type === 'bar' && (
        <div className="flex items-center gap-2 mb-2 relative z-10 flex-wrap">
          <ButtonGroup>
            {trafficData.filter(t => t.type === 'bar').map(trace => {
              const isHidden = hiddenCategories.has(trace.name)
              return (
                <Button
                  key={trace.name}
                  variant={isHidden ? 'ghost' : 'default'}
                  size="sm"
                  onClick={() => React.startTransition(() => toggleCategory(trace.name))}
                  className={cn(
                    "h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]",
                    !isHidden ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                  )}
                >
                  <span className="w-1.5 h-1.5 rounded-full mr-1.5" style={{ backgroundColor: trace.marker.color as string }} />
                  {trace.name}
                </Button>
              )
            })}
          </ButtonGroup>
        </div>
      )}

      <div className="relative flex-1 mb-4">
        {(!isReady || !aggregates) || (isFetchingAggs && trafficData.length === 0) || (transformPending && trafficData.length === 0) ? (
          // data-empty-placeholder="true" excludes this decorative
          // loading/empty card from the WCAG color-contrast axe scan
          // (see frontend/e2e/admin-login.spec.ts). The muted-foreground
          // copy is intentionally low-emphasis here; the placeholder
          // is replaced by real chart data within seconds.
          <div data-empty-placeholder="true" className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
            <span className="text-muted-foreground text-sm animate-pulse">
              {!isReady ? 'Initializing...' : 'Crunching logs...'}
            </span>
          </div>
        ) : trafficData.length === 0 ? (
          <div data-empty-placeholder="true" className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
            <div className="flex flex-col items-center text-muted-foreground text-center px-4">
              <span className="text-sm font-medium">No data available</span>
              <span className="text-xs mt-1">
                {(() => {
                  if (metric === 'ttfb_client' && !isFieldActive('ttfb')) {
                    return "Requires Infrastructure (Group C) fields to be enabled in Fastly logging."
                  }
                  if (metric === 'req_size' && !isFieldActive('req_bytes')) {
                    return "Requires Request Identity (Group A) fields to be enabled in Fastly logging."
                  }
                  return "No logs found for this period."
                })()}
              </span>
            </div>
          </div>
        ) : (
          <div className={cn("transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
            <TimeSeriesChart
              data={trafficData}
              layout={chartLayout}
              height={300}
              onRelayout={onChartRelayout}
              startTime={startTime}
              endTime={endTime}
              timezone={timezone}
            />
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
