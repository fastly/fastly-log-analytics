'use client'

import React, { useState, type ReactNode } from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { client } from '@/lib/api'
import {
  CHART_COLORS,
  denseBarTrace,
  denseTimeGrid,
  lineTrace,
  withSecondaryAxis,
} from '@/lib/utils'
import type { components, PlotlyLayout, PlotlyTrace } from '@/types/api'
import {
  ComplianceHelp,
  ScoreDistHelp,
  ScorerErrorsHelp,
  ScorerLatencyHelp,
} from './help-content'
import {
  deriveScorerSeries,
  scorerHourlyLayout,
  useScorerTimeseries,
  useScoringQuery,
  usToMs,
  type LatencyResponse,
  type LatencyRow,
} from '@/components/SessionScoring/ScoringAdminPanels'

export interface HourlyRow {
  hour: string
  count: number
}

export interface StackedHourlyBarChartProps<R extends HourlyRow> {
  title: string
  description: string
  isLoading?: boolean
  isFetching?: boolean
  isEmpty?: boolean
  error?: AnalyticsCardError | null
  rows: R[]
  categoryKey: keyof R & string
  colors: Record<string, string>
  categoryOrder?: readonly string[]
  helpContent?: React.ReactNode
  helpTitle?: string
}

export function StackedHourlyBarChart<R extends HourlyRow>({
  title,
  description,
  isLoading,
  isFetching,
  isEmpty,
  error,
  rows,
  categoryKey,
  colors,
  categoryOrder,
  helpContent,
  helpTitle,
}: StackedHourlyBarChartProps<R>) {
  const traces = React.useMemo((): PlotlyTrace[] => {
    const presentHours = Array.from(new Set(rows.map((r) => r.hour))).sort()
    const xGrid = denseTimeGrid(presentHours, 3600) ?? presentHours
    const cats = categoryOrder ?? Array.from(new Set(rows.map((r) => String(r[categoryKey]))))
    return cats.map((cat) =>
      denseBarTrace({
        name: cat,
        rows: rows.filter((r) => String(r[categoryKey]) === cat),
        getTime: (r) => r.hour,
        getValue: (r) => r.count,
        intervalSeconds: 3600,
        color: colors[cat] ?? '#64748b',
        grid: xGrid,
      }),
    )
  }, [rows, categoryKey, colors, categoryOrder])

  return (
    <AnalyticsCard
      title={title}
      description={description}
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={isEmpty}
      error={error}
      className="min-h-[320px]"
      helpContent={helpContent}
      helpTitle={helpTitle}
    >
      <PlotlyChart
        data={traces}
        height="100%"
        layout={{
          barmode: 'stack',
          showlegend: true,
          margin: { l: 50, r: 20, t: 10, b: 40 },
          xaxis: { title: '', type: 'date' },
          yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' },
        }}
      />
    </AnalyticsCard>
  )
}

type CompRow = components['schemas']['ScoringComplianceRow']
const COMPLIANCE_COLORS: Record<string, string> = {
  ok: CHART_COLORS.success,
  missing: CHART_COLORS.neutral,
  tampered: '#e11d48',
  expired: CHART_COLORS.warning,
  unknown: '#7c3aed',
}

export function ComplianceChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(
    ['scoring-compliance', serviceId, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/compliance-breakdown', {
        params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      }),
  )
  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`}
      helpContent={<ComplianceHelp />}
      helpTitle="About Cookie Compliance"
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      isLoading={isLoading}
      isFetching={isFetching}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="compliance"
      colors={COMPLIANCE_COLORS}
    />
  )
}

type DistRow = components['schemas']['ScoringDistributionRow']
const BUCKETS = ['0-25', '25-50', '50-75', '75-100'] as const
const BUCKET_COLORS: Record<(typeof BUCKETS)[number], string> = {
  '0-25': CHART_COLORS.neutral,
  '25-50': '#facc15',
  '50-75': '#fb923c',
  '75-100': '#e11d48',
}

export function ScoreDistChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(
    ['scoring-score-dist', serviceId, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/score-distribution', {
        params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      }),
  )
  return (
    <StackedHourlyBarChart<DistRow>
      title={`Score distribution — last ${sinceHours}h`}
      helpContent={<ScoreDistHelp />}
      helpTitle="About Score Distribution"
      description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={(data?.rows?.length ?? 0) === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="bucket"
      colors={BUCKET_COLORS}
      categoryOrder={BUCKETS}
    />
  )
}

type Derived = ReturnType<typeof deriveScorerSeries>

export interface ScorerSeriesCardProps {
  serviceId: string
  sinceHours: number
  title: string
  description: (perMinute: boolean) => string
  help: ReactNode
  helpTitle: string
  a11yTitle: string
  toggle: { id: string; label: string }
  build: (
    series: Derived & { data: LatencyResponse | undefined },
    toggleOn: boolean,
  ) => { traces: PlotlyTrace[]; layout: PlotlyLayout }
  showToggle?: (series: Derived & { data: LatencyResponse | undefined }) => boolean
  placeholder?: (series: Derived & { data: LatencyResponse | undefined }) => ReactNode | null
}

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
            <Label
              htmlFor={toggle.id}
              className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground"
            >
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

const roundToTwo = (num: number): number => Math.round((num + Number.EPSILON) * 100) / 100

export function ScorerErrorsChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer errors — last ${sinceHours}h`}
      description={(perMinute) =>
        `Fail-open errors — requests that timed out or failed auth and were let through unscored.${perMinute ? ' Bucketed per minute.' : ''}`
      }
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

export function ScorerLatencyChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const hasLatency = ({ data, rows }: { data?: { has_latency?: boolean }; rows: LatencyRow[] }) =>
    (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer latency — last ${sinceHours}h`}
      description={(perMinute) =>
        `Edge round-trip (p50/p95/p99) vs Wasm exec (dashed) for the Compute scorer leg.${perMinute ? ' Bucketed per minute.' : ''}`
      }
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
              <code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>). These are added to
              the log format on enable and fill in once scored requests flow through and are
              ingested.
            </p>
          </div>
        ) : null
      }
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

export function ScoringOverviewTab({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <ComplianceChart serviceId={serviceId} sinceHours={sinceHours} />
      <ScoreDistChart serviceId={serviceId} sinceHours={sinceHours} />
      <ScorerLatencyChart serviceId={serviceId} sinceHours={sinceHours} />
      <ScorerErrorsChart serviceId={serviceId} sinceHours={sinceHours} />
    </div>
  )
}
