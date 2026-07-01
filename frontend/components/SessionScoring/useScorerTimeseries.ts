'use client'

import { useQuery } from '@tanstack/react-query'

import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

// Derived from the generated OpenAPI schema now that
// /scoring/latency-timeseries has a real response_model — no more
// `as unknown as` laundering of an untyped dict response. The percentile
// fields are nullable because they exist only on services re-provisioned
// with the scorer-latency columns.
export type LatencyRow = components['schemas']['ScoringLatencyRow']
export type LatencyResponse = components['schemas']['ScoringLatencyTimeseriesResponse']

export const usToMs = (us: number | null | undefined) => (us == null ? null : us / 1000)

/** Shared derivation for the two scorer charts: the row list, the hour axis,
 *  and the per-minute granularity flag (both charts read these identically). */
export function deriveScorerSeries(data: LatencyResponse | undefined) {
  const rows = data?.rows ?? []
  return {
    rows,
    hours: rows.map((r) => r.hour),
    perMinute: data?.granularity === 'minute',
  }
}

/** Shared Plotly layout for the scorer charts — hour-bucketed time axis +
 *  zero-based y. Only the y-axis title and legend visibility differ between
 *  the latency lines and the fail-open bars. */
export function scorerHourlyLayout(perMinute: boolean, yTitle: string, showlegend: boolean) {
  return {
    showlegend,
    margin: { l: 50, r: 20, t: 10, b: 40 },
    xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
    yaxis: { title: yTitle, rangemode: 'tozero' },
  }
}

/**
 * Shared loader for the scorer latency time-series endpoint. Powers both
 * ScorerLatencyChart (the percentile lines) and ScorerErrorsChart (the
 * fail-open bars) — they used to be one dual-axis chart. The query key is
 * stable across both callers, so React Query dedupes them into a single
 * network request and a single cache entry.
 */
export function useScorerTimeseries(serviceId: string, sinceHours: number) {
  return useQuery({
    queryKey: ['scoring-latency-timeseries', serviceId, sinceHours],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/latency-timeseries',
        {
          params: {
            path: { service_id: serviceId },
            query: { since_hours: sinceHours },
          },
        },
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as LatencyResponse
    },
  })
}
