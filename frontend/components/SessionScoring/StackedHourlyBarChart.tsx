'use client'

import * as React from 'react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'

interface HourlyRow {
  hour: string
  count: number
}

interface Props<R extends HourlyRow> {
  title: string
  description: string
  isLoading?: boolean
  isFetching?: boolean
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
  rows,
  categoryKey,
  colors,
  categoryOrder,
  helpContent,
  helpTitle,
}: Props<R>) {
  const traces = React.useMemo(() => {
    const hours = Array.from(new Set(rows.map((r) => r.hour))).sort()
    const cats =
      categoryOrder ??
      Array.from(new Set(rows.map((r) => String(r[categoryKey]))))
    return cats.map((cat) => {
      const byHour = new Map<string, number>()
      for (const r of rows) {
        if (String(r[categoryKey]) === cat) byHour.set(r.hour, r.count)
      }
      return {
        type: 'bar' as const,
        name: cat,
        x: hours,
        y: hours.map((h) => byHour.get(h) ?? 0),
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
