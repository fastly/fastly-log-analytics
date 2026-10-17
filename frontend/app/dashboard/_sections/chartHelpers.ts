import { TIME_HOVER_LAYOUT, makeTimeXAxis } from '@/lib/chart-helpers'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'
import type { DashboardAggregates, DashboardCatalog } from './types'

// densifyBarSeries now lives in lib/chart-helpers (shared with the scoring
// charts). Re-exported so existing importers (and chartHelpers.test.ts) keep
// resolving it from here. The traffic-trace builder lives in lib/traffic-chart
// (it also runs in a Web Worker, which must not depend on app/).
export { densifyBarSeries } from '@/lib/chart-helpers'
export { buildTrafficData, getIntervalSeconds, type BuildTrafficDataParams } from '@/lib/traffic-chart'

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
