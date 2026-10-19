'use client'

import { ScorerErrorsHelp } from '@/components/SessionScoring/help-content'
import { ScorerSeriesCard } from '@/components/SessionScoring/ScorerSeriesCard'
import { scorerHourlyLayout } from '@/components/SessionScoring/useScorerTimeseries'
import { denseBarTrace, lineTrace, withSecondaryAxis } from '@/lib/chart-builders'

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
