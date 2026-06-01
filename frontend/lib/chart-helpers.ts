import { useMemo } from 'react'
import { formatDate } from '@/lib/date'
import { CHART_LAYOUT_DEFAULTS } from '@/lib/constants'

/**
 * Returns the standard Plotly xaxis config for a time-series chart bounded
 * by the current filter range.
 *
 * Replaces the repeated inline xaxis block in page components.
 */
export function makeTimeXAxis(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string
) {
  return {
    range: [
      startTime ? formatDate(startTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
      endTime ? formatDate(endTime, timezone, 'yyyy-MM-dd HH:mm:ss.SSS') : '',
    ],
    nticks: 8,
    tickangle: -45,
    automargin: true,
    type: 'date' as const,
    tickformatstops: CHART_LAYOUT_DEFAULTS.tickformatstops,
  }
}

/** Standard hover + legend layout for time-series charts. */
export const TIME_HOVER_LAYOUT = {
  hovermode: 'x unified' as const,
  legend: {
    orientation: 'h' as const,
    y: 1.15,
    x: 1,
    xanchor: 'right' as const,
    yanchor: 'bottom' as const,
  },
}

/** Memoised combination of TIME_HOVER_LAYOUT + makeTimeXAxis. */
export function useTimeLayout(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
  timezone: string
) {
  return useMemo(
    () => ({ ...TIME_HOVER_LAYOUT, xaxis: makeTimeXAxis(startTime, endTime, timezone) }),
    [startTime, endTime, timezone]
  )
}
