'use client'

import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { type ChartInterval, INTERVAL_SECONDS, INTERVALS } from '@/lib/constants'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { historyHoursFromExtents, pickInsightsDefault } from '@/lib/insights-defaults'

interface ReportConfigOptions {
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
  const { startTime, endTime } = useFilterStore(useShallow(state => ({
    startTime: state.startTime,
    endTime: state.endTime,
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
      // Find the most appropriate interval for the given span.
      // 720h (30d) is the upper bound at which 1-hour granularity stays
      // valid (see the perf-limit `if (spanHours > 720)` above), so it's
      // the natural ceiling for defaulting to 1h — anything in the 7d–30d
      // band still feels granular at hourly buckets without exploding the
      // bar count. ≥30d defaults to 1d as before.
      if (spanHours >= 720 && intervals.has('1 day')) effectiveInt = '1 day'
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

  const effectiveInterval = config.effectiveInterval
  const effectiveTrend = config.validTrends.has(trend) ? trend : 'off'

  return {
    metric,
    setMetric,
    chartInterval: effectiveInterval,
    setChartInterval: (val: ChartInterval) => {
      setChartInterval(val)
      setManualInterval(val)
    },
    trend: effectiveTrend,
    setTrend,
    config
  }
}

export interface InsightsDefaults {
  windowHours: string
  baselineHours: string
  setWindowHours: (value: string) => void
  setBaselineHours: (value: string) => void
}

// An explicit user pick, scoped to the service it was made on. Keying the
// override by service id lets a manual selection stick for the current
// service while being dropped automatically when the active service changes —
// computed during render, so no effect/reset is needed (and no setState-in-
// effect, which this repo's eslint forbids).
interface Override {
  sid: string | null | undefined
  window?: string
  baseline?: string
}

const NO_OVERRIDE: Override = { sid: undefined }

// Picks the Insights window/baseline default from how much history the active
// service has (via the shared ['log-extents', sid] query that useBootstrap
// seeds), while letting an explicit user selection win. Returns the same
// { windowHours, baselineHours, setWindowHours, setBaselineHours } shape the
// page already consumed from useState, so wiring is a drop-in.
export function useInsightsDefaults(
  activeServiceId: string | null | undefined,
): InsightsDefaults {
  const { data } = useQuery({
    queryKey: queryKeys.logExtents(activeServiceId),
    queryFn: async () => {
      return await unwrapClient(client.GET('/api/log-extents'))
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
  })

  const adaptive = useMemo(
    () => pickInsightsDefault(historyHoursFromExtents(data?.earliest_log_at)),
    [data?.earliest_log_at],
  )

  const [override, setOverride] = useState<Override>({ sid: activeServiceId })
  // Drop a stale override the moment the service changes (derived, not reset).
  const eff = override.sid === activeServiceId ? override : NO_OVERRIDE

  const windowHours = eff.window ?? adaptive.window
  const baselineHours = eff.baseline ?? adaptive.baseline

  const setWindowHours = useCallback(
    (value: string) =>
      setOverride((prev) => ({
        sid: activeServiceId,
        baseline: prev.sid === activeServiceId ? prev.baseline : undefined,
        window: value,
      })),
    [activeServiceId],
  )
  const setBaselineHours = useCallback(
    (value: string) =>
      setOverride((prev) => ({
        sid: activeServiceId,
        window: prev.sid === activeServiceId ? prev.window : undefined,
        baseline: value,
      })),
    [activeServiceId],
  )

  return { windowHours, baselineHours, setWindowHours, setBaselineHours }
}
