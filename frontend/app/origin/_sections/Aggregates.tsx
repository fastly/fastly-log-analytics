'use client'

import React from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { cn } from '@/lib/utils'
import { ApproxBadge } from './ApproxBadge'

export function Aggregates({ summary }: { summary: any }) {
  const error = summary.error as AnalyticsCardError | null
  // Backend sets ``_approx: true`` on long-window panels (>= 48h) that
  // are served from the per-hour origin_summary rollup — percentiles are
  // request-weighted averages of per-hour percentiles rather than an
  // exact cross-window MEDIAN/APPROX_QUANTILE. Counts (error rate, fetch
  // volume) are still exact. Surface this so analysts know which numbers
  // tolerate a small approximation.
  const isApprox = summary.data?._approx === true
  return (
    <div className="mb-6">
      {isApprox && (
        <div className="flex justify-end mb-2">
          <ApproxBadge message="Origin TTFB percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Error rate and fetch volume are exact." />
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <AnalyticsCard
        title="Origin TTFB (P50)"
        isLoading={summary.isLoading}
        isFetching={summary.isFetching}
        error={error}
        className="h-auto"
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
        error={error}
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
        error={error}
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
        error={error}
        helpContent={<p>The total number of requests sent to the backend (cache misses and passes) during this time window.</p>}
      >
        <div className="flex flex-col">
          <div className="text-3xl font-bold">
            {((summary.data?.total_misses || 0) + (summary.data?.total_passes || 0)).toLocaleString()}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Total cache misses & passes</div>
        </div>
      </AnalyticsCard>
      </div>
    </div>
  )
}
