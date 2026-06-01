'use client'

import React, { useMemo } from 'react'
import { PlotlyChart } from '@/components/PlotlyChart/PlotlyChart'
import { ErrorBoundary } from '@/components/ErrorBoundary'
import { makeTimeXAxis, TIME_HOVER_LAYOUT } from '@/lib/chart-helpers'

interface TimeSeriesChartProps {
  data: any[]
  startTime?: string | null
  endTime?: string | null
  timezone: string
  layout?: Record<string, any>
  className?: string
  height?: number | string
  onRelayout?: (event: any) => void
}

/**
 * PlotlyChart pre-configured with standard time-series layout:
 * unified hover, legend above chart, and x-axis bounded to the filter range.
 */
function TimeSeriesChartImpl({
  data,
  startTime,
  endTime,
  timezone,
  layout,
  className,
  height,
  onRelayout,
}: TimeSeriesChartProps) {
  const mergedLayout = useMemo(
    () => ({
      ...TIME_HOVER_LAYOUT,
      xaxis: makeTimeXAxis(startTime, endTime, timezone),
      ...layout,
    }),
    [startTime, endTime, timezone, layout],
  )
  return (
    <ErrorBoundary>
      <PlotlyChart
        data={data}
        layout={mergedLayout}
        className={className}
        height={height}
        onRelayout={onRelayout}
      />
    </ErrorBoundary>
  )
}

export const TimeSeriesChart = React.memo(TimeSeriesChartImpl)
