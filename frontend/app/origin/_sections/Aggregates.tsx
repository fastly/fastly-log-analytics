'use client'

import React from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, formatCount } from '@/lib/utils'
import type { components } from '@/types/api'

export type OriginAggregates = components['schemas']['OriginAggregatesResponse']
export type OriginSummary = OriginAggregates['summary']
export type OriginTimeseries = OriginAggregates['timeseries']
export type OriginSlowUrls = OriginAggregates['slow_urls']
export type OriginStatusCodes = OriginAggregates['status_codes']
export type OriginPopLatency = OriginAggregates['pop_latency']
export type OriginIpHealth = OriginAggregates['ip_health']
export type OriginSlowUrlRow = components['schemas']['OriginSlowUrlRow']
export type OriginPopLatencyRow = components['schemas']['OriginPopLatencyRow']
export type OriginIpHealthRow = components['schemas']['OriginIpHealthRow']

export interface SectionQuery<T> {
  data: T | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
}

export function ApproxBadge({ message }: { message: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger
          render={
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help" />
          }
        >
          <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
          Approximate
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-[260px] text-xs">
          {message}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function Aggregates({ summary }: { summary: SectionQuery<OriginSummary> }) {
  const error = summary.error as AnalyticsCardError | null
  const d = summary.data
  const kpis = [
    {
      title: 'Origin TTFB (P50)',
      help: 'Median time taken by your backend to start returning a response after Fastly forwards a request. Lower is better.',
      value: `${d?.ottfb_p50_ms?.toFixed(1)}ms`,
      sub: 'Median backend response time',
      danger: false,
    },
    {
      title: 'Origin TTFB (P95)',
      help: 'The 95th percentile of backend response times. Indicates the tail latency experienced by the slowest 5% of requests.',
      value: `${d?.ottfb_p95_ms?.toFixed(1)}ms`,
      sub: 'Tail latency (95th percentile)',
      danger: false,
    },
    {
      title: 'Origin Error Rate',
      help: 'Percentage of cache miss/pass requests where the backend returned a 5xx HTTP status code.',
      value: `${((d?.origin_error_rate || 0) * 100).toFixed(2)}%`,
      sub: 'Percentage of 5xx responses',
      danger: (d?.origin_error_rate || 0) > 0.01,
    },
    {
      title: 'Fetch Volume',
      help: 'The total number of requests sent to the backend (cache misses and passes) during this time window.',
      value: formatCount((d?.total_misses || 0) + (d?.total_passes || 0)),
      sub: 'Total cache misses & passes',
      danger: false,
    },
  ]

  return (
    <div className="mb-6">
      <div className="flex justify-end items-center min-h-[24px] mb-2">
        {d?._approx === true && (
          <ApproxBadge
            message={
              d?._approx_note ??
              'Origin TTFB percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Error rate and fetch volume are exact.'
            }
          />
        )}
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {kpis.map((kpi) => (
          <AnalyticsCard
            key={kpi.title}
            title={kpi.title}
            isLoading={summary.isLoading}
            isFetching={summary.isFetching}
            loadingMessage={`Loading ${kpi.title}...`}
            error={error}
            className="h-[145px] [contain-intrinsic-size:145px] [content-visibility:auto]"
            helpContent={<p>{kpi.help}</p>}
          >
            <div className="flex flex-col">
              <div className={cn('text-3xl font-bold', kpi.danger ? 'text-destructive' : '')}>
                {kpi.value}
              </div>
              <div className="text-xs text-muted-foreground mt-1">{kpi.sub}</div>
            </div>
          </AnalyticsCard>
        ))}
      </div>
    </div>
  )
}
