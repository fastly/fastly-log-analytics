'use client'

import * as React from 'react'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { ComplianceHelp, ScoreDistHelp } from '@/components/SessionScoring/help-content'
import { useScoringQuery } from './useScorerTimeseries'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { denseTimeGrid } from '@/lib/chart-helpers'
import { CHART_COLORS, denseBarTrace } from '@/lib/chart-builders'
import type { PlotlyTrace } from '@/types/plotly'

interface HourlyRow {
  hour: string
  count: number
}

interface Props<R extends HourlyRow> {
  title: string
  description: string
  isLoading?: boolean
  isFetching?: boolean
  isEmpty?: boolean
  error?: AnalyticsCardError | null
  rows: R[]
  /** Field on each row that names the stacked-bar category. */
  categoryKey: keyof R & string
  /** Hex color per category. Missing categories get a neutral grey. */
  colors: Record<string, string>
  /** Optional fixed ordering for category traces (preserves "0-25 → 75-100"
   *  ordering in the score-distribution chart). If omitted, derive ordering
   *  from the order categories appear in ``rows``. */
  categoryOrder?: readonly string[]
  helpContent?: React.ReactNode
  helpTitle?: string
}

/**
 * Shared "stacked-hourly-bar" rendering used by ScoreDistChart and
 * ComplianceChart. Both charts pivot a flat ``{hour, category, count}[]``
 * stream into one Plotly trace per category, sharing identical layout +
 * color-handling. Differences across the two callers (fixed bucket order
 * vs derived compliance order) are expressed via ``categoryOrder``.
 */
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
}: Props<R>) {
  const traces = React.useMemo((): PlotlyTrace[] => {
    const presentHours = Array.from(new Set(rows.map((r) => r.hour))).sort()
    // Rows are always hour-bucketed (backend `date_trunc('hour')`). Re-index
    // onto a contiguous hourly grid so a low-traffic window renders even-width
    // bars and an empty hour reads as an honest 0 instead of an ambiguous gap.
    // Falls back to the present hours when a grid can't be built (see
    // denseTimeGrid). Lookups key by epoch ms so the grid's `Z` suffix matches
    // the rows' `+00:00` suffix.
    const xGrid = denseTimeGrid(presentHours, 3600) ?? presentHours
    const cats =
      categoryOrder ??
      Array.from(new Set(rows.map((r) => String(r[categoryKey]))))
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
        layout={{
          barmode: 'stack',
          showlegend: true,
          margin: { l: 50, r: 20, t: 10, b: 40 },
          xaxis: { title: '', type: 'date' },
          yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' },
        }}
        height="100%"
      />
    </AnalyticsCard>
  )
}

interface ComplianceChartProps {
  serviceId: string
  sinceHours?: number
}

type CompRow = components['schemas']['ScoringComplianceRow']

const COMPLIANCE_COLORS: Record<string, string> = {
  ok: CHART_COLORS.success,
  missing: CHART_COLORS.neutral,
  tampered: '#e11d48',
  expired: CHART_COLORS.warning,
  unknown: '#7c3aed',
}

export function ComplianceChart({ serviceId, sinceHours = 24 }: ComplianceChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(['scoring-compliance', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/compliance-breakdown', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
    }),
  )

  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`}
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      helpContent={<ComplianceHelp />}
      helpTitle="About Cookie Compliance"
      isLoading={isLoading}
      isFetching={isFetching}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="compliance"
      colors={COMPLIANCE_COLORS}
    />
  )
}

interface ScoreDistChartProps {
  serviceId: string
  sinceHours?: number
}

type DistRow = components['schemas']['ScoringDistributionRow']

const BUCKETS = ['0-25', '25-50', '50-75', '75-100'] as const
const BUCKET_COLORS: Record<(typeof BUCKETS)[number], string> = {
  '0-25': CHART_COLORS.neutral,
  '25-50': '#facc15',
  '50-75': '#fb923c',
  '75-100': '#e11d48',
}

export function ScoreDistChart({ serviceId, sinceHours = 24 }: ScoreDistChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(['scoring-score-dist', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/score-distribution', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
    }),
  )

  return (
    <StackedHourlyBarChart<DistRow>
      title={`Score distribution — last ${sinceHours}h`}
      description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
      helpContent={<ScoreDistHelp />}
      helpTitle="About Score Distribution"
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
