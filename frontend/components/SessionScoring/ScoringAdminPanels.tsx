'use client'

import { client } from '@/lib/api'
import type { components } from '@/types/api'
import { useScoringQuery } from './scoringHooks'

export { useScoringQuery } from './scoringHooks'
export * from './scoringQueryOptions'

export function useScoringStatus(serviceId: string) {
  return useScoringQuery(
    ['scoring-status', serviceId],
    (signal) =>
      client.GET('/api/services/{service_id}/scoring/status', {
        params: { path: { service_id: serviceId } },
        signal,
      }),
    { enabled: Boolean(serviceId) },
  )
}

export type LatencyRow = components['schemas']['ScoringLatencyRow']
export type LatencyResponse = components['schemas']['ScoringLatencyTimeseriesResponse']

export const usToMs = (us: number | null | undefined) => (us == null ? null : us / 1000)

export function deriveScorerSeries(data: LatencyResponse | undefined) {
  const rows = data?.rows ?? []
  return { rows, hours: rows.map((r) => r.hour), perMinute: data?.granularity === 'minute' }
}

export function scorerHourlyLayout(perMinute: boolean, yTitle: string, showlegend: boolean) {
  return {
    showlegend,
    margin: { l: 50, r: 20, t: showlegend ? 35 : 10, b: 40 },
    xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
    yaxis: { title: yTitle, rangemode: 'tozero' },
    legend: {
      orientation: 'h' as const,
      y: 1.05,
      x: 0.5,
      xanchor: 'center' as const,
      yanchor: 'bottom' as const,
      font: { size: 9.5 },
      bgcolor: 'transparent',
    },
  }
}

export function useScorerTimeseries(serviceId: string, sinceHours: number) {
  return useScoringQuery(['scoring-latency-timeseries', serviceId, sinceHours], (signal) =>
    client.GET('/api/services/{service_id}/scoring/latency-timeseries', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      signal,
    }),
  )
}

export * from './help-content'
export * from './LabelsTab'
export * from './ThresholdSlider'
export * from './MatrixVersionsCard'
export * from './ExcludeRegexCard'
export * from './TopFlaggedTable'
export * from './StoragePanels'
export * from './StatusPanel'
