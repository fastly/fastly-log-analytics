'use client'

import React, { useState, useEffect, useMemo } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { type ChartInterval, INTERVAL_SECONDS, INTERVALS } from '@/lib/constants'

export type { ChartInterval }

export interface ReportConfigOptions {
  defaultMetric?: string
  defaultInterval?: ChartInterval
  defaultTrend?: string
}

export interface ReportConfiguration {
  spanHours: number
  validIntervals: Set<ChartInterval>
  validTrends: Set<string>
  effectiveInterval: ChartInterval
}

export function useReportConfig(options: ReportConfigOptions = {}) {
  const { startTime, endTime, hasSyncedExtents } = useFilterStore(useShallow(state => ({
    startTime: state.startTime,
    endTime: state.endTime,
    hasSyncedExtents: state.hasSyncedExtents
  })))

  const [metric, setMetric] = useState(options.defaultMetric || 'requests')
  const [chartInterval, setChartInterval] = useState<ChartInterval>(options.defaultInterval ?? '1 minute')
  const [manualInterval, setManualInterval] = useState<string | null>(null)
  const [trend, setTrend] = useState(options.defaultTrend || 'off')

  const config = useMemo((): ReportConfiguration => {
    const spanSecs = (!startTime || !endTime) ? 0 : (new Date(endTime).getTime() - new Date(startTime).getTime()) / 1000
    const spanHours = spanSecs / 3600
    
    const intervals = new Set(INTERVALS.map(i => i.value))
    
    // Performance limits: prevent massive bucket counts
    if (spanHours > 6) intervals.delete('1 second')
    if (spanHours > 168) intervals.delete('1 minute')
    if (spanHours > 720) intervals.delete('1 hour')

    // Logical limits: prevent selecting a bucket size equal to or larger than the entire time range
    if (spanSecs <= 86400) intervals.delete('1 day')
    if (spanSecs <= 3600) intervals.delete('1 hour')
    if (spanSecs <= 60) intervals.delete('1 minute')

    let effectiveInt: ChartInterval = chartInterval
    // Re-evaluate if manual mode isn't locked in, or if the current interval just became invalid
    if (!manualInterval || !intervals.has(effectiveInt)) {
      // Find the most appropriate interval for the given span
      if (spanHours >= 168 && intervals.has('1 day')) effectiveInt = '1 day'
      else if (spanHours >= 24 && intervals.has('1 hour')) effectiveInt = '1 hour'
      else if (spanSecs >= 300 && intervals.has('1 minute')) effectiveInt = '1 minute'
      else if (intervals.has('1 second')) effectiveInt = '1 second'
      else {
        // Fallback to the largest available valid interval if the target isn't valid
        const ordered: ChartInterval[] = ['1 day', '1 hour', '1 minute', '1 second']
        effectiveInt = ordered.find(i => intervals.has(i)) ?? '1 minute'
      }
    }

    const trends = new Set(['off', 'auto'])
    const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
    const curInt = INTERVAL_SECONDS[effectiveInt] || 60
    for (const [t, secs] of Object.entries(trendMap)) {
      if (secs > curInt) trends.add(t)
    }

    return { 
      spanHours: spanHours, 
      validIntervals: intervals, 
      validTrends: trends, 
      effectiveInterval: effectiveInt 
    }
  }, [startTime, endTime, chartInterval, manualInterval])

  // Sync effective interval and validate trend
  useEffect(() => {
    if (config.effectiveInterval !== chartInterval) {
      setChartInterval(config.effectiveInterval)
    }

    if (!config.validTrends.has(trend)) {
      setTrend('off')
    }
  }, [config, chartInterval, trend])

  // When the user clicks Reset, filterStore.clearFilters() flips
  // hasSyncedExtents back to false. Clear the manualInterval lock so
  // auto-detection resumes from the freshly-reset time range. During
  // normal use (manual interval pick) the lock stays in place.
  useEffect(() => {
    if (!hasSyncedExtents && manualInterval !== null) {
      setManualInterval(null)
    }
  }, [hasSyncedExtents, manualInterval])

  return {
    metric,
    setMetric,
    chartInterval,
    setChartInterval: (val: ChartInterval) => {
      setChartInterval(val)
      setManualInterval(val)
    },
    trend,
    setTrend,
    config
  }
}
