'use client'

import React from 'react'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ApproxBadge } from './ApproxBadge'
import { cn } from '@/lib/utils'
import { Activity } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { makeTimeXAxis } from '@/lib/chart-helpers'
import { TRENDS } from '@/lib/constants'

export function Timeseries({
  originTs,
  originTsChartData,
  statusCodes,
  statusData,
  originMetric,
  setOriginMetric,
  originPercentile,
  setOriginPercentile,
  trend,
  setTrend,
  config,
  intervalButtons,
  startTime,
  endTime,
  timezone,
}: any) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
      <AnalyticsCard
        title="Origin Latency"
        icon={<Activity className="h-4 w-4" />}
        className="lg:col-span-2 h-[400px]"
        isLoading={originTs.isLoading}
        isFetching={originTs.isFetching}
        error={originTs.error as AnalyticsCardError | null}
        helpContent={<p>Time to First Byte (TTFB) measures the time to receive the first byte of the response headers from the origin. Time to Last Byte (TTLB) measures the time to receive the full response body.</p>}
        headerAction={
          <div className="flex items-center gap-2">
            {originTs.data?._approx === true && (
              <ApproxBadge message="Latency percentiles on wide windows are request-weighted averages of per-minute percentiles. Request volume is exact." />
            )}
            <ButtonGroup>
              {(['ttfb', 'ttlb'] as const).map(m => (
                <Button
                  key={m}
                  variant={originMetric === m ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginMetric(m))}
                  className={cn(
                    "h-9 text-xs px-2 shadow-none transition-colors uppercase sm:h-7 sm:text-[11px]",
                    originMetric === m ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                  )}
                >
                  {m}
                </Button>
              ))}
            </ButtonGroup>
            <ButtonGroup>
              {(['p50', 'p95', 'p99'] as const).map(p => (
                <Button
                  key={p}
                  variant={originPercentile === p ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setOriginPercentile(p))}
                  className={cn(
                    "h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]",
                    originPercentile === p ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                  )}
                >
                  {p}
                </Button>
              ))}
            </ButtonGroup>
            <div className="ml-2">
              {intervalButtons}
            </div>
          </div>
        }
      >
        {originTs.isLoading || (originTs.isFetching && originTsChartData.length === 0) ? (
          <div className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
            <span className="text-muted-foreground text-sm animate-pulse">Crunching logs...</span>
          </div>
        ) : originTsChartData.length === 0 ? (
          <div className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
            <div className="flex flex-col items-center text-muted-foreground">
              <span className="text-sm font-medium">No data available</span>
              <span className="text-xs mt-1">No origin timing data found for this period.</span>
            </div>
          </div>
        ) : (
          <div className="flex flex-col h-full">
            <div className="relative flex-1 mb-4">
              <PlotlyChart
                data={originTsChartData}
                layout={{
                  hovermode: 'x unified',
                  yaxis: { title: 'ms', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
                  xaxis: makeTimeXAxis(startTime, endTime, timezone),
                }}
                height="100%"
              />
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
                    className="h-9 text-xs px-2 shadow-none disabled:opacity-30 sm:h-7 sm:text-[11px]"
                  >
                    {t.label}
                  </Button>
                ))}
              </ButtonGroup>
            </div>
          </div>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Status Code Distribution"
        icon={<Activity className="h-4 w-4" />}
        isLoading={statusCodes.isLoading}
        isFetching={statusCodes.isFetching}
        error={statusCodes.error as AnalyticsCardError | null}
        isEmpty={!statusData?.length}
        className="h-[400px]"
        contentClassName="p-2"
        helpContent={<p>A breakdown of the HTTP status codes returned directly by your backend servers during the selected time period.</p>}
      >
        <PlotlyChart
          data={statusData}
          height="100%"
        />
      </AnalyticsCard>
    </div>
  )
}
