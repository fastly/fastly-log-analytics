'use client'

import { ScorerLatencyHelp } from '@/components/SessionScoring/help-content'
import { ScorerSeriesCard } from '@/components/SessionScoring/ScorerSeriesCard'
import {
  scorerHourlyLayout,
  usToMs,
  type LatencyRow,
} from '@/components/SessionScoring/useScorerTimeseries'
import { CHART_COLORS, denseBarTrace, lineTrace, withSecondaryAxis } from '@/lib/chart-builders'

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
  { name: 'p95 rtt', get: (r) => r.rtt_p95_us, color: CHART_COLORS.info },
  { name: 'p99 rtt', get: (r) => r.rtt_p99_us, color: '#1d4ed8' },
  { name: 'p50 exec', get: (r) => r.exec_p50_us, color: '#34d399', dash: 'dot' },
  { name: 'p95 exec', get: (r) => r.exec_p95_us, color: CHART_COLORS.success, dash: 'dot' },
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
  // Only draw the latency lines when the columns exist AND at least one bucket
  // has a value — avoids an empty axis on freshly-provisioned services that
  // haven't taken traffic yet.
  const hasLatency = ({ data, rows }: { data?: { has_latency?: boolean }; rows: LatencyRow[] }) =>
    (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer latency — last ${sinceHours}h`}
      description={(perMinute) =>
        `Edge round-trip (p50/p95/p99) vs Wasm exec (dashed) for the Compute scorer leg.${perMinute ? ' Bucketed per minute.' : ''}`}
      help={<ScorerLatencyHelp />}
      helpTitle="About Scorer Latency"
      a11yTitle={`Scorer latency percentiles over the last ${sinceHours} hours`}
      toggle={{ id: 'show-volume', label: 'Show volume' }}
      showToggle={hasLatency}
      placeholder={(series) =>
        series.rows.length > 0 && !hasLatency(series) ? (
          // Rows exist (the service is enabled and taking traffic) but the
          // latency columns aren't recorded yet. The generic empty-state copy
          // ("expand the time range") would be misleading here — and the
          // fail-open chart next door still renders — so explain the real fix.
          <div className="flex h-full items-center justify-center p-4 text-center">
            <p className="max-w-xs text-xs text-muted-foreground">
              Waiting for traffic logged with the scorer latency fields (
              <code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>). These are added to the
              log format on enable and fill in once scored requests flow through and are ingested.
            </p>
          </div>
        ) : null}
      build={({ rows, hours, perMinute }, showVolume) => {
        // Volume bars are zero-filled; the latency lines stay on the sparse
        // `hours` axis with connectgaps — a missing bucket has no defined
        // percentile, so it must not become a 0 dip.
        const volumeTrace = denseBarTrace({
          name: 'Inspected requests',
          rows,
          getTime: (r) => r.hour,
          getValue: (r) => r.total_count,
          intervalSeconds: perMinute ? 60 : 3600,
          color: 'rgba(148, 163, 184, 0.12)',
          yaxis: 'y2',
          showlegend: true,
        })
        const latencyTraces = LATENCY_SERIES.map((s) =>
          lineTrace({
            name: s.name,
            x: hours,
            y: rows.map((r) => usToMs(s.get(r))),
            color: s.color,
            dash: s.dash,
            connectgaps: true,
          }),
        )
        const base = scorerHourlyLayout(perMinute, 'Round-trip / exec (ms)', true)
        return {
          traces: [...(showVolume ? [volumeTrace] : []), ...latencyTraces],
          layout: showVolume ? withSecondaryAxis(base, 'Inspected requests') : base,
        }
      }}
    />
  )
}
