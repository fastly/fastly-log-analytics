import { useMemo } from 'react'
import { formatDate } from '@/lib/date'

export interface TimeseriesDataPoint {
  time: string
  [key: string]: any
}

export interface TraceConfig {
  key: string
  name: string
  color: string
  stackgroup?: string
  fill?: 'none' | 'tozeroy' | 'tonexty' | 'toself'
  type?: 'scatter' | 'bar'
}

/**
 * Transforms flat backend timeseries data into Plotly trace arrays.
 *
 * Given an array of objects like:
 * [{ time: '2023-01-01', http2: 100, http3: 50 }, ...]
 *
 * Returns Plotly traces based on the provided configuration.
 */
export function useTimeseriesToTraces(
  data: TimeseriesDataPoint[] | undefined,
  configs: TraceConfig[],
  timezone?: string
) {
  return useMemo(() => {
    if (!data || data.length === 0) return []

    // Ensure times are sorted for Plotly (though the backend should do this)
    const sortedData = [...data]
      .filter(d => d && (d.time || d.ts))
      .sort((a, b) => {
        const ta = a.time || a.ts || ''
        const tb = b.time || b.ts || ''
        return ta.localeCompare(tb)
      })

    const xValues = sortedData.map(d => {
      const t = d.time || d.ts
      return timezone ? formatDate(t, timezone, "yyyy-MM-dd HH:mm:ss") : t
    })

    return configs.map(config => {
      const yValues = sortedData.map(d => Number(d[config.key]) || 0)
      const isBar = config.type === 'bar'

      // Build trace WITHOUT explicit `undefined` properties — Plotly's
      // schema validator silently rejects the whole trace if it sees
      // `stackgroup: undefined` or `marker: undefined` for a non-bar
      // chart, leaving the canvas empty with no console error. This is
      // why the Origin Latency chart "stopped working" even though the
      // data was fully present.
      const trace: Record<string, any> = {
        x: xValues,
        y: yValues,
        name: config.name,
        type: config.type || 'scatter',
        fill: config.fill || 'tozeroy',
      }
      if (isBar) {
        trace.marker = { color: config.color }
      } else {
        trace.mode = 'lines'
        trace.line = { width: 1.5, color: config.color }
      }
      if (config.stackgroup) {
        trace.stackgroup = config.stackgroup
      }
      return trace
    })
  }, [data, configs, timezone])
}
