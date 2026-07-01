import { formatDate } from '@/lib/date'
import { INTERVAL_SECONDS } from '@/lib/constants'
import { makeTimeXAxis, TIME_HOVER_LAYOUT, densifyBarSeries, type BarSeriesPoint } from '@/lib/chart-helpers'

// densifyBarSeries now lives in lib/chart-helpers (shared with the scoring
// charts). Re-exported so existing importers (and chartHelpers.test.ts) keep
// resolving it from here.
export { densifyBarSeries } from '@/lib/chart-helpers'

export interface BuildTrafficDataParams {
  aggregates: any
  compareAggregates: any
  compareMode: boolean
  compareStartTime: string | null | undefined
  startTime: string | null
  trend: string
  timezone: string
  metric: string
  effectiveInterval: string
  hiddenCategories: Set<string>
  catalog: any
}

/**
 * Build the Plotly traces for the traffic chart. Pure function — given the
 * same inputs returns the same output array. Memoize on the call-site.
 */
export function buildTrafficData({
  aggregates,
  compareAggregates,
  compareMode,
  compareStartTime,
  startTime,
  trend,
  timezone,
  metric,
  effectiveInterval,
  hiddenCategories,
  catalog,
}: BuildTrafficDataParams): any[] {
  const time_series = aggregates?.time_series
  if (!time_series?.length) return []

  const actualMetric = aggregates?.metric || metric
  const isBar = actualMetric === 'requests' || actualMetric === '5xx' || actualMetric === '4xx'

  // Find metric metadata from catalog
  const metricField = catalog?.fields?.find((f: any) => f.id === actualMetric)
  const unit = metricField?.unit || ''
  const precision = metricField?.precision ?? (actualMetric === 'requests' ? 0 : 1)

  const getHoverTemplate = (_m: string, label?: string) => {
    const pre = label ? `${label}: ` : ''
    const format = precision > 0 ? `.${precision}f` : ','
    return `${pre}%{y:${format}}${unit}<extra></extra>`
  }

  // If we have categories (e.g. 5xx/4xx breakdown), group by category.
  // Pydantic serializes optional fields as null, so null and undefined both mean "no category".
  const hasCategories = time_series.some((d: any) => d.category != null)

  // For bar metrics, zero-fill empty buckets so a sparse (filtered) series
  // renders full-width bars instead of Plotly hairlines. Scatter series
  // (latency/throughput/hit_rate trend) are left untouched — a missing bucket
  // there is undefined, not zero. barSeries === time_series when !isBar.
  const actualInterval = aggregates?.interval || effectiveInterval
  const intervalSeconds = INTERVAL_SECONDS[actualInterval as keyof typeof INTERVAL_SECONDS]
  const barSeries: BarSeriesPoint[] = isBar
    ? densifyBarSeries(time_series, intervalSeconds, hasCategories)
    : time_series

  let traces: any[] = []

  if (hasCategories) {
    const catMap: Record<string, { x: string[], y: number[] }> = {}
    barSeries.forEach((d) => {
      const cat = d.category || 'Other'
      if (!catMap[cat]) catMap[cat] = { x: [], y: [] }
      // Use a standard format that Plotly recognizes as a date but is in the target timezone
      catMap[cat].x.push(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
      catMap[cat].y.push(d.value)
    })

    // Standardize colors for common error statuses to keep them consistent
    const colorMap: Record<string, string> = {
      '400': '#fbbf24', '401': '#f59e0b', '403': '#d97706', '404': '#b45309',
      '500': '#ef4444', '502': '#dc2626', '503': '#b91c1c', '504': '#991b1b'
    }

    traces = Object.entries(catMap).map(([cat, data], i) => ({
      x: data.x,
      y: data.y,
      type: 'bar',
      name: cat,
      showlegend: false, // Custom legend will handle these
      visible: hiddenCategories.has(cat) ? 'legendonly' : true,
      hovertemplate: `Status ${cat}: %{y:,}<extra></extra>`,
      marker: { color: colorMap[cat] || `hsl(${(i * 50) % 360}, 70%, 50%)` }
    }))
  } else {
    const xValues = barSeries.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    const yValues = barSeries.map((d) => d.value)

    traces = [{
      x: xValues,
      y: yValues,
      type: isBar ? 'bar' : 'scatter',
      mode: isBar ? undefined : 'lines+markers',
      name: compareMode ? 'Primary Range' : (metricField?.label || actualMetric),
      showlegend: compareMode,
      hovertemplate: getHoverTemplate(actualMetric, compareMode ? 'Primary' : undefined),
      marker: { color: '#3b82f6' }
    }]
  }

  if (compareMode && compareAggregates?.time_series?.length && !hasCategories && startTime && compareStartTime) {
    const currentStart = new Date(startTime).getTime()
    const compareStart = new Date(compareStartTime).getTime()
    const shift = currentStart - compareStart

    const compX = compareAggregates.time_series.map((d: any) => {
      const t = new Date(d.time).getTime() + shift
      return formatDate(new Date(t).toISOString(), timezone, "yyyy-MM-dd HH:mm:ss")
    })
    const compY = compareAggregates.time_series.map((d: any) => d.value)

    traces.push({
      x: compX,
      y: compY,
      type: 'scatter',
      mode: 'lines',
      name: 'Comparison Range',
      line: { color: '#f97316', dash: 'dash', width: 2 },
      hovertemplate: getHoverTemplate(actualMetric, 'Comparison')
    })
  }

  if (!hasCategories && time_series.some((d: any) => d.baseline != null)) {
    traces.push({
      x: time_series.map((d: any) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")),
      y: time_series.map((d: any) => d.baseline),
      type: 'scatter', mode: 'lines',
      name: 'Baseline (7d prior)',
      hovertemplate: getHoverTemplate(actualMetric, 'Baseline'),
      line: { color: '#a1a1aa', dash: 'dot', width: 2 }
    })
  }

  if (!hasCategories && trend !== 'off') {
    const xValues = time_series.map((d: any) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    const yValues = time_series.map((d: any) => d.value)
    const n = yValues.length
    let windowSize = 0
    if (trend === 'auto') {
      if (n > 1000) windowSize = Math.floor(n / 20)
      else if (n > 100) windowSize = Math.floor(n / 10)
      else windowSize = Math.floor(n / 5)
    } else {
      const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
      const actualInterval = aggregates?.interval || effectiveInterval
      windowSize = Math.floor((trendMap[trend] ?? 0) / (INTERVAL_SECONDS[actualInterval as keyof typeof INTERVAL_SECONDS] ?? 60))
    }
    if (windowSize > 1) {
      const trendY = new Array(n).fill(null)
      for (let i = windowSize - 1; i < n; i++) {
        let sum = 0, count = 0
        for (let j = 0; j < windowSize; j++) {
          const v = yValues[i - j]
          if (v != null) { sum += v; count++ }
        }
        trendY[i] = count > 0 ? sum / count : null
      }
      traces.push({
        x: xValues, y: trendY,
        type: 'scatter', mode: 'lines',
        name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
        hovertemplate: getHoverTemplate(actualMetric),
        line: { color: '#f97316', width: 3 }
      })
    }
  }
  return traces
}

export interface BuildChartLayoutParams {
  trafficData: any[]
  aggregates: any
  metric: string
  startTime: string | null
  endTime: string | null
  timezone: string
  catalog: any
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
}: BuildChartLayoutParams): any {
  const actualMetric = aggregates?.metric || metric
  const metricField = catalog?.fields?.find((f: any) => f.id === actualMetric)

  return {
    ...TIME_HOVER_LAYOUT,
    barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
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
