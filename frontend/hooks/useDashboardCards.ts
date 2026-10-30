'use client'

import { useState, useCallback, useEffect, useMemo } from 'react'
import { usePathname } from 'next/navigation'
import { useQuery, type UseQueryOptions, type QueryFunctionContext, type QueryKey } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import {
  useDebounce,
  useDebouncedFilterPayload,
  useFilterPayload,
  useIsDataReady,
  useMounted,
} from '@/hooks/useBootstrap'
import { adminJsonResult, client, unwrapClient, queryKeys } from "@/lib/api";
import { INTERVAL_SECONDS, INTERVALS, historyHoursFromExtents, pickInsightsDefault, quantizeAnchor, resolveRangeWire, _resetUrlHydrationFlag, hydrateFilterStoreFromUrl, rangeLabelToHours, type ChartInterval, type RangeWire, type RangeWireArgs } from "@/lib/utils";
import { useFilterStore } from '@/stores/filterStore'
import type { Simplify, WithRequired, components } from '@/types/api'

export { useDebounce, useFilterPayload, useDebouncedFilterPayload }
export * from './useDashboardCardDefinitions'

/**
 * Bidirectional sync between the global filterStore and the page URL.
 */
export function useFilterUrlWriteback(): void {
  const pathname = usePathname()
  const { startTime, endTime, isAutoRange, relativeRange } = useFilterStore(
    useShallow(state => ({
      startTime: state.startTime,
      endTime: state.endTime,
      isAutoRange: state.isAutoRange,
      relativeRange: state.relativeRange,
    })),
  )
  const filterPayload = useFilterPayload()

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    const hasUrlRange = Boolean(
      (params.get('range') && rangeLabelToHours(params.get('range')!) !== null) ||
        (params.get('start_time') && params.get('end_time')),
    )

    _resetUrlHydrationFlag()
    hydrateFilterStoreFromUrl()

    if (!hasUrlRange) {
      const store = useFilterStore.getState()
      const now = new Date()
      const activeRange = store.relativeRange

      if (activeRange) {
        const hours = rangeLabelToHours(activeRange)
        if (hours !== null) {
          const start = new Date(now.getTime() - hours * 3600 * 1000).toISOString()
          store.setRelativeRange(activeRange, start, now.toISOString())
        }
      } else if (store.isAutoRange && (!store.startTime || !store.endTime)) {
        useFilterStore.setState({
          startTime: new Date(now.getTime() - 24 * 3600 * 1000).toISOString(),
          endTime: now.toISOString(),
        })
      }
    }
  }, [pathname])

  useEffect(() => {
    if (typeof window === 'undefined') return

    const url = new URL(window.location.href)
    if (Object.keys(filterPayload).length > 0) {
      url.searchParams.set('filters', JSON.stringify(filterPayload))
    } else {
      url.searchParams.delete('filters')
    }
    if (relativeRange) {
      url.searchParams.set('range', relativeRange)
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
    } else if (!isAutoRange && startTime && endTime) {
      url.searchParams.set('start_time', startTime)
      url.searchParams.set('end_time', endTime)
      url.searchParams.delete('range')
    } else {
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
      url.searchParams.delete('range')
    }
    window.history.replaceState({}, '', url.toString())
  }, [filterPayload, startTime, endTime, isAutoRange, relativeRange, pathname])
}

/**
 * Active time-range selection (primary + compare).
 */
export function useTimeRange() {
  return useFilterStore(
    useShallow(s => ({
      startTime: s.startTime,
      endTime: s.endTime,
      compareMode: s.compareMode,
      compareStartTime: s.compareStartTime,
      compareEndTime: s.compareEndTime,
    }))
  )
}

/**
 * Shared hook that resolves the `(relativeRange, isAutoRange, anchor)` tuple
 * for analytics report pages.
 */
export function useReportRangeAnchor(nowServerStr?: string) {
  const mounted = useMounted()
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = useMemo(() => {
    const baseVal = !mounted && nowServerStr ? nowServerStr : storeEndTime
    return quantizeAnchor(baseVal || undefined)
  }, [storeEndTime, nowServerStr, mounted])

  return { relativeRange, isAutoRange, anchor }
}

/**
 * `placeholderData` that holds the previous result across key changes within
 * one service (filter/range/page changes) but never across a service switch.
 */
export function keepPreviousWithinService(currentKey: QueryKey, serviceIdIndex = 2) {
  return <T>(previousData: T | undefined, previousQuery: { queryKey: QueryKey } | undefined): T | undefined => {
    if (!previousQuery) return undefined
    const prevKey = previousQuery.queryKey
    if (
      Array.isArray(currentKey) &&
      Array.isArray(prevKey) &&
      currentKey.length > serviceIdIndex &&
      prevKey.length > serviceIdIndex &&
      currentKey[serviceIdIndex] !== prevKey[serviceIdIndex]
    ) {
      return undefined
    }
    return previousData
  }
}

/**
 * Wraps useQuery with the standard enabled/placeholderData defaults every
 * data page uses: enabled when the service is ready, hold prior data while
 * the new fetch is in-flight.
 */
export function useServiceQuery<T>(
  queryKey: unknown[],
  queryFn: (context: QueryFunctionContext) => Promise<T>,
  options?: Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, 'queryKey' | 'queryFn' | 'placeholderData'>
) {
  const isReady = useIsDataReady()
  return useQuery<T, Error, T>({
    ...options,
    queryKey,
    queryFn,
    enabled: isReady && (options?.enabled ?? true),
    placeholderData: keepPreviousWithinService(queryKey),
  })
}

type RumStatusWire = WithRequired<components['schemas']['RumStatusResponse'], 'enabled'>
type RumStatus = Simplify<RumStatusWire & { unauthorized?: boolean }>

interface UseRumStatusOptions {
  enabled?: boolean
  refetchInterval?: number
}

/**
 * Single owner of the `['rum-status', serviceId]` cache entry.
 */
export function useRumStatus(
  serviceId: string | null | undefined,
  { enabled = true, refetchInterval }: UseRumStatusOptions = {},
) {
  return useQuery<RumStatus | null>({
    queryKey: queryKeys.rumStatus(serviceId),
    queryFn: async () => {
      if (!serviceId) return null
      const res = await adminJsonResult<'/api/services/{service_id}/rum/status', RumStatusWire>(
        '/api/services/{service_id}/rum/status',
        `/api/services/${serviceId}/rum/status`,
      )
      if (res.status === 403) return { enabled: false, unauthorized: true }
      return res.ok ? res.data : null
    },
    enabled: !!serviceId && enabled,
    refetchInterval,
  })
}

interface AnalyticsReportQueryContext {
  rangeBody: RangeWire['rangeBody']
  rangeToken: RangeWire['rangeToken']
  signal: AbortSignal | undefined
}

interface AnalyticsReportQueryArgs<TData> {
  namespace: readonly unknown[]
  serviceId: string | null
  filterPayload: unknown
  range: RangeWireArgs
  keyParts?: readonly unknown[]
  queryFn: (ctx: AnalyticsReportQueryContext) => Promise<TData>
  queryOptions?: Omit<
    UseQueryOptions<TData, Error, TData, readonly unknown[]>,
    'queryKey' | 'queryFn' | 'placeholderData'
  >
}

/**
 * Centralizes the query shape shared by the SSR-seedable analytics pages.
 */
export function useAnalyticsReportQuery<TData>({
  namespace,
  serviceId,
  filterPayload,
  range,
  keyParts,
  queryFn,
  queryOptions,
}: AnalyticsReportQueryArgs<TData>) {
  const { rangeKey, rangeBody, rangeToken } = resolveRangeWire(range)
  const queryKey: unknown[] = [
    ...namespace,
    serviceId,
    rangeKey,
    range.anchor,
    filterPayload,
    ...(keyParts ?? []),
  ]
  return useServiceQuery<TData>(
    queryKey,
    ({ signal }) => queryFn({ rangeBody, rangeToken, signal }),
    queryOptions,
  )
}

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

    if (spanHours > 6) intervals.delete('1 second')
    if (spanHours > 168) intervals.delete('1 minute')
    if (spanHours > 720) intervals.delete('1 hour')

    if (spanSecs <= 86400) intervals.delete('1 day')
    if (spanSecs <= 3600) intervals.delete('1 hour')
    if (spanSecs <= 60) intervals.delete('1 minute')

    let effectiveInt: ChartInterval = chartInterval
    if (!manualInterval || !intervals.has(effectiveInt)) {
      if (spanHours >= 720 && intervals.has('1 day')) effectiveInt = '1 day'
      else if (spanHours >= 24 && intervals.has('1 hour')) effectiveInt = '1 hour'
      else if (spanSecs >= 300 && intervals.has('1 minute')) effectiveInt = '1 minute'
      else if (intervals.has('1 second')) effectiveInt = '1 second'
      else {
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

interface Override {
  sid: string | null | undefined
  window?: string
  baseline?: string
}

const NO_OVERRIDE: Override = { sid: undefined }

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
