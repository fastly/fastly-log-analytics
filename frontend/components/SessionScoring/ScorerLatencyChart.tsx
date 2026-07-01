'use client'

import { useState } from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { denseTimeGrid } from '@/lib/chart-helpers'
import { ScorerLatencyHelp } from '@/components/SessionScoring/help-content'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import {
  deriveScorerSeries,
  scorerHourlyLayout,
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
  const [showVolume, setShowVolume] = useState(true)

  const { rows, hours, perMinute } = deriveScorerSeries(data)
  // Only draw the latency lines when the columns exist AND at least one bucket
  // has a value — avoids an empty axis on freshly-provisioned services that
  // haven't taken traffic yet.
  const hasLatency = (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  // Fill empty buckets with 0 so the volume bars stay even-width on a sparse
  // window and a quiet bucket reads as "0 inspected" rather than a gap. The
  // latency lines below stay on the sparse `hours` axis with connectgaps — a
  // missing bucket has no defined percentile, so it must not become a 0 dip.
  const cleanHours = hours.filter((h): h is string => h != null)
  const volGrid = denseTimeGrid(cleanHours, perMinute ? 60 : 3600) ?? cleanHours
  const volByMs = new Map<number, number>()
  for (const r of rows) {
    if (r.hour != null) volByMs.set(Date.parse(r.hour), r.total_count ?? 0)
  }
  const volumeTrace = {
    type: 'bar' as const,
    name: 'Inspected requests',
    x: volGrid,
    y: volGrid.map((h) => volByMs.get(Date.parse(h)) ?? 0),
    yaxis: 'y2' as const,
    marker: { color: 'rgba(148, 163, 184, 0.12)' },
    showlegend: true,
  }

  const latencyTraces = LATENCY_SERIES.map((s) => ({
    type: 'scatter' as const,
    mode: 'lines' as const,
    name: s.name,
    x: hours,
    y: rows.map((r) => usToMs(s.get(r))),
    line: { color: s.color, width: 2, dash: s.dash },
    connectgaps: true,
  }))

  const traces = [
    ...(showVolume ? [volumeTrace] : []),
    ...latencyTraces,
  ]

  const baseLayout = scorerHourlyLayout(perMinute, 'Round-trip / exec (ms)', true)
  const layout = {
    ...baseLayout,
    ...(showVolume
      ? {
          yaxis2: {
            title: 'Inspected requests',
            overlaying: 'y' as const,
            side: 'right' as const,
            showgrid: false,
            rangemode: 'tozero' as const,
          },
          margin: { ...baseLayout.margin, r: 50 },
        }
      : {}),
  }

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
      headerAction={
        rows.length > 0 && hasLatency ? (
          <div className="flex items-center gap-2 mr-1">
            <Switch
              id="show-volume"
              checked={showVolume}
              onCheckedChange={setShowVolume}
            />
            <Label htmlFor="show-volume" className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground">
              Show volume
            </Label>
          </div>
        ) : undefined
      }
    >
      {rows.length > 0 && !hasLatency ? (
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
      ) : (
        <PlotlyChart
          data={traces}
          layout={layout}
          height="100%"
          a11yTitle={`Scorer latency percentiles over the last ${sinceHours} hours`}
        />
      )}
    </AnalyticsCard>
  )
}
