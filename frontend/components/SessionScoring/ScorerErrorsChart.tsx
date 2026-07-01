'use client'

import { useState } from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { denseTimeGrid } from '@/lib/chart-helpers'
import { ScorerErrorsHelp } from '@/components/SessionScoring/help-content'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import {
  deriveScorerSeries,
  scorerHourlyLayout,
  useScorerTimeseries,
} from '@/components/SessionScoring/useScorerTimeseries'

interface ScorerErrorsChartProps {
  serviceId: string
  sinceHours?: number
}

/**
 * Hourly fail-open count for the scorer Compute leg: requests that timed out
 * or failed auth and were let through unscored. Companion to
 * ScorerLatencyChart — read the two together, since fail-opens tend to follow
 * latency. Unlike the latency lines, this series renders on any enabled
 * service (no ``has_latency`` gate).
 */
export function ScorerErrorsChart({ serviceId, sinceHours = 24 }: ScorerErrorsChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)
  const [showRate, setShowRate] = useState(true)

  const { rows, hours, perMinute } = deriveScorerSeries(data)

  // Fill empty buckets with 0 so a sparse fail-open series renders even-width
  // bars instead of Plotly hairlines, and a quiet bucket reads as "0 fail-opens"
  // rather than a gap. Grid keys by epoch ms (Z vs +00:00 safe); falls back to
  // the present buckets when a grid can't be built. The rate line stays sparse —
  // a missing bucket has no defined rate.
  const cleanHours = hours.filter((h): h is string => h != null)
  const barGrid = denseTimeGrid(cleanHours, perMinute ? 60 : 3600) ?? cleanHours
  const failByMs = new Map<number, number>()
  for (const r of rows) {
    if (r.hour != null) failByMs.set(Date.parse(r.hour), r.fail_open_count ?? 0)
  }
  const errorTrace = {
    type: 'bar' as const,
    name: 'Fail-opens',
    x: barGrid,
    y: barGrid.map((h) => failByMs.get(Date.parse(h)) ?? 0),
    marker: { color: 'rgba(225,29,72,0.45)' },
  }

  const rateTrace = {
    type: 'scatter' as const,
    mode: 'lines' as const,
    name: 'Error rate (%)',
    x: hours,
    y: rows.map((r) => {
      const total = r.total_count ?? 0
      return total > 0 ? roundToTwo(((r.fail_open_count ?? 0) / total) * 100) : 0
    }),
    yaxis: 'y2' as const,
    line: { color: '#e11d48', width: 2 },
  }

  const traces = [
    errorTrace,
    ...(showRate ? [rateTrace] : []),
  ]

  const baseLayout = scorerHourlyLayout(perMinute, 'Fail-opens', showRate)
  const layout = {
    ...baseLayout,
    ...(showRate
      ? {
          yaxis2: {
            title: 'Error rate (%)',
            overlaying: 'y' as const,
            side: 'right' as const,
            showgrid: false,
            rangemode: 'tozero' as const,
            ticksuffix: '%',
          },
          margin: { ...baseLayout.margin, r: 50 },
        }
      : {}),
  }

  return (
    <AnalyticsCard
      title={`Scorer errors — last ${sinceHours}h`}
      description={`Fail-open errors — requests that timed out or failed auth and were let through unscored.${perMinute ? ' Bucketed per minute.' : ''}`}
      helpContent={<ScorerErrorsHelp />}
      helpTitle="About Scorer Errors"
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      className="min-h-[320px]"
      headerAction={
        rows.length > 0 ? (
          <div className="flex items-center gap-2 mr-1">
            <Switch
              id="show-rate"
              checked={showRate}
              onCheckedChange={setShowRate}
            />
            <Label htmlFor="show-rate" className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground">
              Show rate (%)
            </Label>
          </div>
        ) : undefined
      }
    >
      <PlotlyChart
        data={traces}
        layout={layout}
        height="100%"
        a11yTitle={`Scorer fail-open errors over the last ${sinceHours} hours`}
      />
    </AnalyticsCard>
  )
}

function roundToTwo(num: number): number {
  return Math.round((num + Number.EPSILON) * 100) / 100
}
