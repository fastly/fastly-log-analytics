'use client'

import * as React from 'react'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { denseTimeGrid } from '@/lib/chart-helpers'

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
  const traces = React.useMemo(() => {
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
    return cats.map((cat) => {
      const byMs = new Map<number, number>()
      for (const r of rows) {
        if (String(r[categoryKey]) === cat) byMs.set(Date.parse(r.hour), r.count)
      }
      return {
        type: 'bar' as const,
        name: cat,
        x: xGrid,
        y: xGrid.map((h) => byMs.get(Date.parse(h)) ?? 0),
        marker: { color: colors[cat] ?? '#64748b' },
      }
    })
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
        data={traces as any[]}
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
