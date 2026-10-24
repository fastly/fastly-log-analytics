import { TIME_HOVER_LAYOUT, makeTimeXAxis } from '@/lib/chart-helpers'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'
import type { DashboardAggregates, DashboardCatalog } from './types'

export interface BuildChartLayoutParams {
  trafficData: PlotlyTrace[]
  aggregates: Pick<DashboardAggregates, 'metric'> | null | undefined
  metric: string
  startTime: string | null
  endTime: string | null
  timezone: string
  catalog: DashboardCatalog | null | undefined
}

/**
 * Build the Plotly layout object for the traffic chart. Pure function.
 */
export function buildChartLayout({
  trafficData,
  aggregates,
  metric,
  startTime,
  endTime,
  timezone,
  catalog,
}: BuildChartLayoutParams): PlotlyLayout {
  const actualMetric = aggregates?.metric || metric
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)

  return {
    ...TIME_HOVER_LAYOUT,
    barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
    bargap: 0.1,
    bargroupgap: 0.05,
    showlegend: trafficData.some(t => t.showlegend !== false),
    yaxis: {
      title: metricField?.unit || (actualMetric === 'requests' ? 'reqs' : ''),
      ticksuffix: metricField?.unit || '',
      separatethousands: true,
      exponentformat: 'none'
    },
    xaxis: makeTimeXAxis(startTime, endTime, timezone),
  }
}
