'use client'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { ScorerLatencyHelp } from '@/components/SessionScoring/help-content'
import {
  useScorerTimeseries,
  usToMs,
  type LatencyRow,
} from '@/components/SessionScoring/useScorerTimeseries'

interface ScorerLatencyChartProps {
  serviceId: string
  sinceHours?: number
}

// Latency lines. Accessor form keeps it type-safe without keyof gymnastics.
// Edge round-trip ramps darker blue for higher percentiles; the scorer's own
// Wasm exec time renders dashed emerald so the two families read apart — when
// exec sits far below the round-trip, the cost is network/cold-start, not
// compute.
const LATENCY_SERIES: {
  name: string
  get: (r: LatencyRow) => number | null | undefined
  color: string
  dash?: 'dot' | 'dash'
}[] = [
  { name: 'p50 rtt', get: (r) => r.rtt_p50_us, color: '#60a5fa' },
  { name: 'p95 rtt', get: (r) => r.rtt_p95_us, color: '#3b82f6' },
  { name: 'p99 rtt', get: (r) => r.rtt_p99_us, color: '#1d4ed8' },
  { name: 'p50 exec', get: (r) => r.exec_p50_us, color: '#34d399', dash: 'dot' },
  { name: 'p95 exec', get: (r) => r.exec_p95_us, color: '#10b981', dash: 'dot' },
]

/**
 * Time series of the scorer Compute leg's latency: edge round-trip
 * percentiles (p50/p95/p99) plus the Wasm exec percentiles (dashed), all on a
 * single millisecond axis. The companion ScorerErrorsChart carries the
 * fail-open count — they were one dual-axis chart until the axes proved too
 * easy to misread. The lines only fill in once the service has been
 * re-provisioned with the edge_score_rtt_us / edge_score_exec_us columns
 * (``has_latency``).
 */
export function ScorerLatencyChart({ serviceId, sinceHours = 24 }: ScorerLatencyChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)

  const rows = data?.rows ?? []
  const hours = rows.map((r) => r.hour)
  const perMinute = data?.granularity === 'minute'
  // Only draw the latency lines when the columns exist AND at least one bucket
  // has a value — avoids an empty axis on freshly-provisioned services that
  // haven't taken traffic yet.
  const hasLatency = (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  const traces = LATENCY_SERIES.map((s) => ({
    type: 'scatter' as const,
    mode: 'lines' as const,
    name: s.name,
    x: hours,
    y: rows.map((r) => usToMs(s.get(r))),
    line: { color: s.color, width: 2, dash: s.dash },
    connectgaps: true,
  }))

  return (
    <AnalyticsCard
      title={`Scorer latency — last ${sinceHours}h`}
      description={`Edge round-trip (p50/p95/p99) vs Wasm exec (dashed) for the Compute scorer leg.${perMinute ? ' Bucketed per minute.' : ''}`}
      helpContent={<ScorerLatencyHelp />}
      helpTitle="About Scorer Latency"
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      className="min-h-[320px]"
    >
      {rows.length > 0 && !hasLatency ? (
        // Rows exist (the service is enabled and taking traffic) but the
        // latency columns aren't recorded yet. The generic empty-state copy
        // ("expand the time range") would be misleading here — and the
        // fail-open chart next door still renders — so explain the real fix.
        <div className="flex h-full items-center justify-center p-4 text-center">
          <p className="max-w-xs text-xs text-muted-foreground">
            Latency columns not present. Re-provision this service to record{' '}
            <code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>.
          </p>
        </div>
      ) : (
        <PlotlyChart
          data={traces}
          layout={{
            showlegend: true,
            margin: { l: 50, r: 20, t: 10, b: 40 },
            xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
            yaxis: {
              title: 'Round-trip / exec (ms)',
              rangemode: 'tozero',
            },
          }}
          height="100%"
          a11yTitle={`Scorer latency percentiles over the last ${sinceHours} hours`}
        />
      )}
    </AnalyticsCard>
  )
}
