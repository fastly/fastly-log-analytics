'use client'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { ScorerErrorsHelp } from '@/components/SessionScoring/help-content'
import { useScorerTimeseries } from '@/components/SessionScoring/useScorerTimeseries'

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

  const rows = data?.rows ?? []
  const hours = rows.map((r) => r.hour)
  const perMinute = data?.granularity === 'minute'

  const errorTrace = {
    type: 'bar' as const,
    name: 'fail-opens',
    x: hours,
    y: rows.map((r) => r.fail_open_count),
    marker: { color: 'rgba(225,29,72,0.45)' },
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
    >
      <PlotlyChart
        data={[errorTrace]}
        layout={{
          showlegend: false,
          margin: { l: 50, r: 20, t: 10, b: 40 },
          xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
          yaxis: {
            title: 'Fail-opens',
            rangemode: 'tozero',
          },
        }}
        height="100%"
        a11yTitle={`Scorer fail-open errors over the last ${sinceHours} hours`}
      />
    </AnalyticsCard>
  )
}
