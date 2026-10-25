'use client'

import { useState, useCallback, useEffect, useMemo } from 'react'
import { usePathname } from 'next/navigation'
import { useQuery, type UseQueryOptions, type QueryFunctionContext, type QueryKey } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import {
  useBootstrap,
  useDebounce,
  useDebouncedFilterPayload,
  useFilterPayload,
  useIsDataReady,
  useLogFieldsCatalog,
  useMounted,
} from '@/hooks/useBootstrap'
import { adminJsonResult, client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { type ChartInterval, INTERVAL_SECONDS, INTERVALS } from '@/lib/utils'
import { parseFromInput } from '@/lib/format'
import { historyHoursFromExtents, pickInsightsDefault } from '@/lib/insights-defaults'
import { queryKeys } from '@/lib/query-keys'
import { quantizeAnchor, resolveRangeWire, type RangeWire, type RangeWireArgs } from '@/lib/utils'
import { _resetUrlHydrationFlag, hydrateFilterStoreFromUrl, rangeLabelToHours } from '@/lib/utils'
import { useFilterStore } from '@/stores/filterStore'
import type { Simplify, WithRequired, components } from '@/types/api'
import type { PlotlyRelayoutEvent } from '@/types/api'

export { useDebounce, useFilterPayload, useDebouncedFilterPayload }

export type CardTint = {
  bg: string
  border: string
  dot: string
}

export type CardCategory = {
  id: string
  label: string
  cardIds: string[]
  tint: CardTint
}

export const CARD_CATEGORIES: CardCategory[] = [
  {
    id: 'request',
    label: 'Request',
    cardIds: ['ip', 'asn', 'host', 'url', 'method', 'status', 'cache', 'proto', 'ua', 'referer', 'cookie_session', 'resp_header_content_encoding'],
    tint: { bg: 'bg-blue-50/60 dark:bg-blue-950/40', border: 'border-blue-200/70 dark:border-blue-900/60', dot: 'bg-blue-500' },
  },
  {
    id: 'cache',
    label: 'Cache',
    cardIds: ['ttl', 'age', 'hits', 'digest'],
    tint: { bg: 'bg-amber-50/60 dark:bg-amber-950/40', border: 'border-amber-200/70 dark:border-amber-900/60', dot: 'bg-amber-500' },
  },
  {
    id: 'geo',
    label: 'Geography',
    cardIds: ['city', 'region', 'country', 'metro'],
    tint: { bg: 'bg-emerald-50/60 dark:bg-emerald-950/40', border: 'border-emerald-200/70 dark:border-emerald-900/60', dot: 'bg-emerald-500' },
  },
  {
    id: 'network',
    label: 'Network & Connection',
    cardIds: [
      'tcp_rtt', 'transport', 'ploss', 'rtt_min', 'rtt_var', 'retrans',
      'c_speed', 'c_type', 'delivery_rate', 'data_segs_out',
    ],
    tint: { bg: 'bg-cyan-50/60 dark:bg-cyan-950/40', border: 'border-cyan-200/70 dark:border-cyan-900/60', dot: 'bg-cyan-500' },
  },
  {
    id: 'edge',
    label: 'Edge Infrastructure',
    cardIds: ['pop', 'backend', 'edge', 'server_region', 'tls', 'is_ipv6', 'conn_requests'],
    tint: { bg: 'bg-violet-50/60 dark:bg-violet-950/40', border: 'border-violet-200/70 dark:border-violet-900/60', dot: 'bg-violet-500' },
  },
  {
    id: 'security',
    label: 'Security',
    cardIds: [
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind', 'edge_score_reason_ind',
      'waf', 'waf_resp', 'waf_ms',
      'p_type', 'p_desc',
      'ja3', 'ja4', 'tls_ciphers_sha',
    ],
    tint: { bg: 'bg-rose-50/60 dark:bg-rose-950/40', border: 'border-rose-200/70 dark:border-rose-900/60', dot: 'bg-rose-500' },
  },
  {
    id: 'origin',
    label: 'Origin',
    cardIds: ['ottfb', 'ottlb', 'ost', 'obytes', 'oip', 'oretries'],
    tint: { bg: 'bg-yellow-50/60 dark:bg-yellow-950/40', border: 'border-yellow-200/70 dark:border-yellow-900/60', dot: 'bg-yellow-500' },
  },
  {
    id: 'quic',
    label: 'QUIC / HTTP3',
    cardIds: ['bw', 'q_rtt', 'q_rtt_var', 'q_lost', 'q_cwnd'],
    tint: { bg: 'bg-indigo-50/60 dark:bg-indigo-950/40', border: 'border-indigo-200/70 dark:border-indigo-900/60', dot: 'bg-indigo-500' },
  },
  {
    id: 'io',
    label: 'Image Optimization',
    cardIds: ['io_input_format', 'io_output_format'],
    tint: { bg: 'bg-fuchsia-50/60 dark:bg-fuchsia-950/40', border: 'border-fuchsia-200/70 dark:border-fuchsia-900/60', dot: 'bg-fuchsia-500' },
  },
]

export const CUSTOM_TINT: CardTint = {
  bg: 'bg-slate-50/60 dark:bg-slate-900/30',
  border: 'border-slate-200/60 dark:border-slate-800/50',
  dot: 'bg-slate-400',
}

export const CATEGORIZED_CARD_IDS = new Set(CARD_CATEGORIES.flatMap(c => c.cardIds))

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
    _resetUrlHydrationFlag()
    hydrateFilterStoreFromUrl()

    if (typeof window !== 'undefined') {
      const params = new URLSearchParams(window.location.search)
      const qsStart = params.get('start_time')
      const qsEnd = params.get('end_time')
      const qsRange = params.get('range')

      if (!qsStart && !qsEnd) {
        const store = useFilterStore.getState()
        const now = new Date()
        const activeRange = qsRange || store.relativeRange

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

export interface DashboardCard {
  id: string
  label: string
  inActiveFormat: boolean
}

const NGWAF_CARD_IDS = new Set(['_ngwaf_bot_name', 'waf_sig_ind'])

const NON_TOP_N_FIELDS = new Set([
  'timestamp',
  'elapsed',
  'ttfb',
  'req_bytes',
  'req_header_bytes',
  'resp_bytes',
  'lat',
  'lon',
  'rid',
  'prid',
  'waf_req_id',
  'waf_sig',
  '_source_file',
  'tcp_rtt',
  'ploss',
  'rtt_min',
  'rtt_var',
  'retrans',
  'bw',
  'delivery_rate',
  'data_segs_out',
  'q_rtt',
  'q_rtt_var',
  'q_lost',
  'q_cwnd',
  'ottfb',
  'ottlb',
  'oconnect_ms',
  'ost',
  'obytes',
  'oip',
  'oretries',
  'io_input_bytes',
  'io_output_bytes',
  'edge_score',
  'edge_score_l1',
  'edge_score_l2',
  'edge_score_rtt_us',
  'edge_score_exec_us',
])

export function useDashboardCards(): DashboardCard[] {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    if (!catalog?.fields || !bootstrap) return []

    const excludedGroups = ['METRICS', 'INTERNAL']
    const excludedIds = ['timestamp', 'elapsed', 'req_bytes', 'resp_bytes', 'req_header_bytes', 'ttfb', 'lat', 'lon']

    const FORCE_VISIBLE = new Set(['_bot_name', ...NGWAF_CARD_IDS])
    const FORCE_HIDDEN = new Set(['rid', 'prid', 'waf_sig', 'waf_req_id'])

    const activeIds = new Set<string>(bootstrap.active_log_field_ids ?? [])

    const standardCards: DashboardCard[] = catalog.fields
      .filter(f => {
        if ((f.group != null && excludedGroups.includes(f.group)) || excludedIds.includes(f.id)) {
          return false
        }

        const isCustom = f.is_custom || f.group === 'custom'
        if (isCustom) {
          const isSystemField =
            f.id.startsWith('cmcd_') ||
            f.id.startsWith('edge_') ||
            f.id.startsWith('rum_') ||
            f.id === 'fastly_req_id'

          if (isSystemField) {
            return false
          }

          return !!f.show_in_dashboard
        } else {
          return CATEGORIZED_CARD_IDS.has(f.id) && !NON_TOP_N_FIELDS.has(f.id)
        }
      })
      .map(f => {
        let inActiveFormat: boolean
        if (FORCE_HIDDEN.has(f.id)) inActiveFormat = false
        else if (FORCE_VISIBLE.has(f.id)) inActiveFormat = true
        else inActiveFormat = activeIds.size === 0 ? true : activeIds.has(f.id)
        return {
          id: f.id,
          label: f.label || f.id,
          inActiveFormat,
        }
      })

    const customCards: DashboardCard[] = (bootstrap.custom_dashboard_cards ?? [])
      .filter(c => {
        const isSystemField =
          c.id.startsWith('cmcd_') ||
          c.id.startsWith('edge_') ||
          c.id.startsWith('rum_') ||
          c.id === 'fastly_req_id'
        return !isSystemField
      })
      .map(c => ({
        id: c.id,
        label: c.label,
        inActiveFormat: activeIds.size === 0 ? true : activeIds.has(c.id),
      }))

    const seen = new Set<string>()
    return [...standardCards, ...customCards].filter(c => {
      if (NGWAF_CARD_IDS.has(c.id) && bootstrap.ngwaf_configured !== true) return false
      if (seen.has(c.id)) return false
      seen.add(c.id)
      return true
    })
  }, [catalog, bootstrap])
}

export function usePlotlyRangeZoom(
  timezone: string,
  setRange: (start: string, end: string) => void,
) {
  return useCallback(
    (event: PlotlyRelayoutEvent) => {
      if (event['xaxis.autorange'] === true || event['xaxis.showspikes'] !== undefined) return

      const nested = Array.isArray(event['xaxis.range']) ? event['xaxis.range'] : []
      const bound = (flat: unknown, fallback: unknown): string | number | undefined => {
        const v = flat ?? fallback
        return typeof v === 'string' || typeof v === 'number' ? v : undefined
      }
      const x0 = bound(event['xaxis.range[0]'], nested[0])
      const x1 = bound(event['xaxis.range[1]'], nested[1])

      if (x0 === undefined || x1 === undefined) return

      try {
        const toLocalStr = (val: string | number) => {
          if (typeof val === 'number') {
            const d = new Date(val)
            const pad = (n: number) => n.toString().padStart(2, '0')
            return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
          }
          return val.replace(' ', 'T')
        }
        const parsedStart = parseFromInput(toLocalStr(x0), timezone)
        const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
        if (parsedStart && parsedEnd) {
          setRange(parsedStart, parsedEnd)
        }
      } catch {
        // Ignore unparseable zoom events
      }
    },
    [setRange, timezone],
  )
}

interface CardVisibilityMigration {
  version: number
  removeIds?: string[]
  addIds?: string[]
}

export function useCardVisibility(
  storageKey: string,
  allIds: string[],
  defaultIds?: string[],
  migration?: CardVisibilityMigration,
) {
  const allIdsStr = JSON.stringify(allIds)
  const defaultIdsStr = JSON.stringify(defaultIds ?? allIds)
  const allVisible = useMemo(() => new Set<string>(JSON.parse(allIdsStr)), [allIdsStr])
  const defaultVisible = useMemo(() => new Set<string>(JSON.parse(defaultIdsStr)), [defaultIdsStr])

  const migrationKey = `${storageKey}_mv`
  const migrationVersion = migration?.version
  const migrationRemoveStr = JSON.stringify(migration?.removeIds ?? [])
  const migrationAddStr = JSON.stringify(migration?.addIds ?? [])

  const load = useCallback((): Set<string> => {
    if (typeof window === 'undefined') return defaultVisible
    try {
      const stored = localStorage.getItem(storageKey)
      if (!stored) return defaultVisible

      const set = new Set<string>(JSON.parse(stored))

      if (migrationVersion !== undefined) {
        const storedVersion = Number(localStorage.getItem(migrationKey) ?? 0)
        if (storedVersion < migrationVersion) {
          const removeIds: string[] = JSON.parse(migrationRemoveStr)
          const addIds: string[] = JSON.parse(migrationAddStr)
          for (const id of removeIds) set.delete(id)
          for (const id of addIds) set.add(id)
          try {
            localStorage.setItem(storageKey, JSON.stringify([...set]))
            localStorage.setItem(migrationKey, String(migrationVersion))
          } catch {}
        }
      }

      return set
    } catch {}
    return defaultVisible
  }, [storageKey, defaultVisible, migrationKey, migrationVersion, migrationRemoveStr, migrationAddStr])

  const mounted = useMounted()
  const [edit, setEdit] = useState<{ source: typeof load; set: Set<string> } | null>(null)
  const stored = useMemo(() => (mounted ? load() : defaultVisible), [mounted, load, defaultVisible])
  const visibleCards = edit && edit.source === load ? edit.set : stored

  const toggleCard = useCallback((id: string) => {
    setEdit(prev => {
      const next = new Set(prev && prev.source === load ? prev.set : stored)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try { localStorage.setItem(storageKey, JSON.stringify([...next])) } catch {}
      return { source: load, set: next }
    })
  }, [storageKey, load, stored])

  const showAll = useCallback(() => {
    setEdit({ source: load, set: allVisible })
    try { localStorage.setItem(storageKey, JSON.stringify([...allVisible])) } catch {}
  }, [storageKey, allVisible, load])

  const reset = useCallback(() => {
    setEdit({ source: load, set: defaultVisible })
    try { localStorage.setItem(storageKey, JSON.stringify([...defaultVisible])) } catch {}
  }, [storageKey, defaultVisible, load])

  return { visibleCards, toggleCard, showAll, reset, defaultVisible }
}
