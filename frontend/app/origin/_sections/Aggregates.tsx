'use client'

import React from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { formatCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type OriginAggregates = components['schemas']['OriginAggregatesResponse']

export type OriginSummary = OriginAggregates['summary']
export type OriginTimeseries = OriginAggregates['timeseries']
export type OriginSlowUrls = OriginAggregates['slow_urls']
export type OriginStatusCodes = OriginAggregates['status_codes']
export type OriginPopLatency = OriginAggregates['pop_latency']
export type OriginIpHealth = OriginAggregates['ip_health']

export type OriginSlowUrlRow = components['schemas']['OriginSlowUrlRow']
export type OriginPopLatencyRow = components['schemas']['OriginPopLatencyRow']
export type OriginIpHealthRow = components['schemas']['OriginIpHealthRow']

/** One section of the origin composite plus the bundle query's loading state. */
export interface SectionQuery<T> {
  data: T | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
}

/**
 * Small "Approximate" pill shown on origin panels whose wide-window
 * (>= 48h, unfiltered) values are served from per-hour rollups. Across
 * hours the percentiles are request-weighted averages of per-hour
 * percentiles rather than an exact cross-window MEDIAN/APPROX_QUANTILE,
 * so the backend sets ``_approx: true`` on those section payloads. Counts
 * (request volume, error rate) stay exact. Markup mirrors the original
 * inline badge on the summary card so every origin panel reads the same.
 */
export function ApproxBadge({ message }: { message: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger render={<span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help" />}>
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
  const isApprox = summary.data?._approx === true
  return (
    <div className="mb-6">
      {isApprox && (
        <div className="flex justify-end mb-2">
          <ApproxBadge message={summary.data?._approx_note ?? "Origin TTFB percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Error rate and fetch volume are exact."} />
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <AnalyticsCard
        title="Origin TTFB (P50)"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin TTFB (P50)..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>Median time taken by your backend to start returning a response after Fastly forwards a request. Lower is better.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">{summary.data?.ottfb_p50_ms?.toFixed(1)}ms</div>
          <div className="text-xs text-muted-foreground mt-1">Median backend response time</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Origin TTFB (P95)"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin TTFB (P95)..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>The 95th percentile of backend response times. Indicates the tail latency experienced by the slowest 5% of requests.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">{summary.data?.ottfb_p95_ms?.toFixed(1)}ms</div>
          <div className="text-xs text-muted-foreground mt-1">Tail latency (95th percentile)</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Origin Error Rate"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Origin Error Rate..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>Percentage of cache miss/pass requests where the backend returned a 5xx HTTP status code.</p>}
      >
        <div className="flex flex-col">
          <div className={cn("text-3xl font-bold", (summary.data?.origin_error_rate || 0) > 0.01 ? "text-destructive" : "")}>
            {((summary.data?.origin_error_rate || 0) * 100).toFixed(2)}%
          </div>
          <div className="text-xs text-muted-foreground mt-1">Percentage of 5xx responses</div>
        </div>
      </AnalyticsCard>
      <AnalyticsCard
        title="Fetch Volume"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        loadingMessage="Loading Fetch Volume..."
        error={error}
        className="min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]"
        helpContent={<p>The total number of requests sent to the backend (cache misses and passes) during this time window.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">
            {formatCount((summary.data?.total_misses || 0) + (summary.data?.total_passes || 0))}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Total cache misses & passes</div>
        </div>
      </AnalyticsCard>
      </div>
    </div>
  )
}
