'use client'

import { useState, type ReactNode } from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { ScorerErrorsHelp, ScorerLatencyHelp } from '@/components/SessionScoring/help-content'
import {
  deriveScorerSeries,
  scorerHourlyLayout,
  useScorerTimeseries,
  usToMs,
  type LatencyResponse,
  type LatencyRow,
} from '@/components/SessionScoring/useScorerTimeseries'
import { CHART_COLORS, denseBarTrace, lineTrace, withSecondaryAxis } from '@/lib/chart-builders'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'

type Derived = ReturnType<typeof deriveScorerSeries>

interface ScorerSeriesCardProps {
  serviceId: string
  sinceHours: number
  title: string
  /** Receives the per-minute flag so the card can say "Bucketed per minute". */
  description: (perMinute: boolean) => string
  help: ReactNode
  helpTitle: string
  a11yTitle: string
  toggle: { id: string; label: string }
  /** Traces + layout for the current toggle state. */
  build: (series: Derived & { data: LatencyResponse | undefined }, toggleOn: boolean) => {
    traces: PlotlyTrace[]
    layout: PlotlyLayout
  }
  /** Whether the toggle is offered for this data (default: whenever rows exist). */
  showToggle?: (series: Derived & { data: LatencyResponse | undefined }) => boolean
  /** Replaces the chart (e.g. a "waiting for fields" explanation). */
  placeholder?: (series: Derived & { data: LatencyResponse | undefined }) => ReactNode | null
}

/**
 * Card shell shared by the scorer latency and fail-open charts: one timeseries
 * query, one optional secondary-series toggle, one PlotlyChart. Each chart
 * supplies only its traces/layout.
 */
export function ScorerSeriesCard({
  serviceId,
  sinceHours,
  title,
  description,
  help,
  helpTitle,
  a11yTitle,
  toggle,
  build,
  showToggle,
  placeholder,
}: ScorerSeriesCardProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)
  const [toggleOn, setToggleOn] = useState(true)

  const series = { ...deriveScorerSeries(data), data }
  const { rows, perMinute } = series
  const { traces, layout } = build(series, toggleOn)
  const offerToggle = rows.length > 0 && (showToggle ? showToggle(series) : true)
  const replacement = placeholder?.(series) ?? null

  return (
    <AnalyticsCard
      title={title}
      description={description(perMinute)}
      helpContent={help}
      helpTitle={helpTitle}
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      className="min-h-[320px]"
      headerAction={
        offerToggle ? (
          <div className="flex items-center gap-2 mr-1">
            <Switch id={toggle.id} checked={toggleOn} onCheckedChange={setToggleOn} />
            <Label htmlFor={toggle.id} className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground">
              {toggle.label}
            </Label>
          </div>
        ) : undefined
      }
    >
      {replacement ?? <PlotlyChart data={traces} layout={layout} height="100%" a11yTitle={a11yTitle} />}
    </AnalyticsCard>
  )
}

// ─── ScorerErrorsChart ───────────────────────────────────────────────────────

interface ScorerErrorsChartProps {
  serviceId: string
  sinceHours?: number
}

function roundToTwo(num: number): number {
  return Math.round((num + Number.EPSILON) * 100) / 100
}

/**
 * Hourly fail-open count for the scorer Compute leg: requests that timed out
 * or failed auth and were let through unscored. Companion to
 * ScorerLatencyChart — read the two together, since fail-opens tend to follow
 * latency. Unlike the latency lines, this series renders on any enabled
 * service (no ``has_latency`` gate).
 */
export function ScorerErrorsChart({ serviceId, sinceHours = 24 }: ScorerErrorsChartProps) {
  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer errors — last ${sinceHours}h`}
      description={(perMinute) =>
        `Fail-open errors — requests that timed out or failed auth and were let through unscored.${perMinute ? ' Bucketed per minute.' : ''}`}
      help={<ScorerErrorsHelp />}
      helpTitle="About Scorer Errors"
      a11yTitle={`Scorer fail-open errors over the last ${sinceHours} hours`}
      toggle={{ id: 'show-rate', label: 'Show rate (%)' }}
      build={({ rows, hours, perMinute }, showRate) => {
        const errorTrace = denseBarTrace({
          name: 'Fail-opens',
          rows,
          getTime: (r) => r.hour,
          getValue: (r) => r.fail_open_count,
          intervalSeconds: perMinute ? 60 : 3600,
          color: 'rgba(225,29,72,0.45)',
        })
        // The rate line stays sparse — a missing bucket has no defined rate.
        const rateTrace = lineTrace({
          name: 'Error rate (%)',
          x: hours,
          y: rows.map((r) => {
            const total = r.total_count ?? 0
            return total > 0 ? roundToTwo(((r.fail_open_count ?? 0) / total) * 100) : 0
          }),
          color: '#e11d48',
          yaxis: 'y2',
        })
        const base = scorerHourlyLayout(perMinute, 'Fail-opens', showRate)
        return {
          traces: [errorTrace, ...(showRate ? [rateTrace] : [])],
          layout: showRate ? withSecondaryAxis(base, 'Error rate (%)', { ticksuffix: '%' }) : base,
        }
      }}
    />
  )
}

// ─── ScorerLatencyChart ──────────────────────────────────────────────────────

interface ScorerLatencyChartProps {
  serviceId: string
  sinceHours?: number
}

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
 * single millisecond axis.
 */
export function ScorerLatencyChart({ serviceId, sinceHours = 24 }: ScorerLatencyChartProps) {
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
          <div className="flex h-full items-center justify-center p-4 text-center">
            <p className="max-w-xs text-xs text-muted-foreground">
              Waiting for traffic logged with the scorer latency fields (
              <code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>). These are added to the
              log format on enable and fill in once scored requests flow through and are ingested.
            </p>
          </div>
        ) : null}
      build={({ rows, hours, perMinute }, showVolume) => {
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
