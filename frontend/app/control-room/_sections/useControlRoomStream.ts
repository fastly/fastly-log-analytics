'use client'

import { useEffect, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { adminJsonResult, getPageLoadId } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'

export interface MetricsData {
  requests_per_second: number
  error_rate: number
  cache_hit_ratio: number
  bandwidth_mbps: number
  total_requests?: number
  total_hits?: number
  total_miss?: number
  total_pass?: number
  total_errors?: number
  status_breakdown?: Record<string, number>
  estimated_cost_usd?: number
  origin_requests_per_second?: number
  origin_bandwidth_mbps?: number
  origin_offload?: number
  shield_requests?: number
  shield_hit_ratio?: number
  pass_requests?: number
  synth_requests?: number
  waf_blocked?: number
  waf_logged?: number
  waf_passed?: number
  pop_count?: number
  degraded_pops?: string[]
  hit_latency_ms?: number
  miss_latency_ms?: number
  pass_latency_ms?: number
  http2?: number
  http3?: number
  ipv6?: number
  tls_v12?: number
  tls_v13?: number
  h2_pct?: number
  h3_pct?: number
  ipv6_pct?: number
  tls12_pct?: number
  tls13_pct?: number
  ddos_action_blackhole?: number
  ddos_action_tarpit?: number
  ddos_action_close?: number
  ddos_action_downgrade?: number
  ddos_detect?: number
  ddos_mitigate?: number
  status_detail?: Record<string, number>
  object_size_distribution?: Record<string, number>
  miss_histogram?: Record<string, number>
  origin_fetches?: number
  origin_revalidations?: number
  origin_cache_fetches?: number
  shield_hit_requests?: number
  shield_miss_requests?: number
  shield_revalidations?: number
  shield_fetch_body_bytes?: number
  request_collapse_usable?: number
  request_collapse_unusable?: number
  segblock_origin_fetches?: number
  segblock_shield_fetches?: number
  bot_challenge_starts?: number
  bot_challenges_issued?: number
  bot_challenges_succeeded?: number
  bot_challenges_failed?: number
  bot_detected?: number
  bot_verified?: number
  bot_ai_crawlers?: number
  compute_exec_time_ms?: number
  compute_req_time_ms?: number
  compute_ram_used?: number
  compute_bereq_errors?: number
  compute_guest_errors?: number
  compute_resource_exceeded?: number
  restarts?: number
  vcl_recv_count?: number
  vcl_recv_time_ms?: number
  vcl_fetch_count?: number
  vcl_fetch_time_ms?: number
  vcl_deliver_count?: number
  vcl_deliver_time_ms?: number
  vcl_error_count?: number
  vcl_error_time_ms?: number
  top_pops?: Array<{
    name: string
    requests: number
    errors: number
    hits: number
    miss: number
    hit_ratio: number
    error_rate: number
  }>
  all_pops?: Record<string, { r: number; e: number }>
}

export interface MetricsTick {
  event: string
  event_schema_version: number
  timestamp: string
  status: 'ok' | 'rt_down'
  data: MetricsData
  aggregate_delay?: number
}

interface TickEntry {
  timestamp: number
  data: MetricsData
}

const MAX_ENTRIES = 60
const EMPTY_DATA: MetricsData = {
  requests_per_second: 0,
  error_rate: 0,
  cache_hit_ratio: 0,
  bandwidth_mbps: 0,
}

export function useTickHistory(allTicks: MetricsTick[]) {
  const history = useMemo<TickEntry[]>(() => {
    const entries = (allTicks.length > MAX_ENTRIES ? allTicks.slice(-MAX_ENTRIES) : allTicks).map((t) => ({
      timestamp: new Date(t.timestamp).getTime(),
      data: t.data,
    }))
    return entries.length < MAX_ENTRIES
      ? [...Array.from({ length: MAX_ENTRIES - entries.length }, () => ({ timestamp: 0, data: EMPTY_DATA })), ...entries]
      : entries
  }, [allTicks])

  const prevTick = allTicks.length >= 2 ? allTicks[allTicks.length - 2] : null
  const series = useCallback((extractor: (d: MetricsData) => number): number[] => history.map((e) => extractor(e.data)), [history])
  const rollingAvg = useCallback(
    (extractor: (d: MetricsData) => number, windowSize: number): number => {
      const window = history.filter((e) => e.timestamp > 0).slice(-windowSize)
      return window.length === 0 ? 0 : window.reduce((sum, e) => sum + extractor(e.data), 0) / window.length
    },
    [history],
  )
  return { history, prevTick, series, rollingAvg }
}

export interface RealtimeStreamState {
  connected: boolean
  lastTickTime: Date | null
  latestTick: MetricsTick | null
  allTicks: MetricsTick[]
  rtDown: boolean
}

export interface ErrorEvent {
  event_id: string
  timestamp: string
  status: number
  url: string
  pop: string
  client_ip: string
}

function parseErrorEvent(line: SSELine): ErrorEvent | null {
  if (line.event !== 'error_event' && line.type !== 'error_event') return null
  const v = line.data as Partial<ErrorEvent> | undefined
  if (!v || typeof v !== 'object' || typeof v.event_id !== 'string' || typeof v.timestamp !== 'string' || typeof v.status !== 'number') {
    return null
  }
  return {
    event_id: v.event_id,
    timestamp: v.timestamp,
    status: v.status,
    url: typeof v.url === 'string' ? v.url : '',
    pop: typeof v.pop === 'string' ? v.pop : '',
    client_ip: typeof v.client_ip === 'string' ? v.client_ip : '',
  }
}

export function useErrorStream(): { connected: boolean; events: ErrorEvent[] } {
  const { lines, status, start, reset } = useSSE<ErrorEvent>({
    maxLines: 500,
    transform: parseErrorEvent,
  })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  useEffect(() => {
    if (!activeServiceId) return
    const pageLoadId = getPageLoadId()
    start(
      `/api/services/${activeServiceId}/control-room/error-stream`,
      undefined,
      pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined,
    )
    return () => reset()
  }, [activeServiceId, start, reset])

  const events = useMemo(() => {
    const seen = new Set<string>()
    return lines
      .filter((e) => !seen.has(e.event_id) && !!seen.add(e.event_id))
      .slice(-20)
  }, [lines])

  return { connected: status === 'streaming', events }
}

function parseMetricsTick(line: SSELine): MetricsTick | null {
  if (line.event !== 'metrics_tick' && line.type !== 'metrics_tick') return null
  const metricsData = line.data as MetricsData | undefined
  if (!metricsData || typeof metricsData !== 'object') return null
  return {
    event: 'metrics_tick',
    event_schema_version: (line.event_schema_version as number) ?? 1,
    timestamp: (line.timestamp as string) ?? new Date().toISOString(),
    status: (line.status as 'ok' | 'rt_down') ?? 'ok',
    data: metricsData,
    aggregate_delay: line.aggregate_delay as number | undefined,
  }
}

function isTick(t: unknown): t is MetricsTick {
  return (
    !!t &&
    typeof t === 'object' &&
    (t as Record<string, unknown>).event === 'metrics_tick' &&
    !!(t as Record<string, unknown>).data
  )
}

export function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, reset } = useSSE<MetricsTick>({
    maxLines: 600,
    transform: parseMetricsTick,
  })
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  const { data: seed } = useQuery({
    queryKey: ['realtime-seed', activeServiceId],
    queryFn: async ({ signal }) => {
      let seedTicks: MetricsTick[] = []
      try {
        const res = await adminJsonResult(
          '/api/services/{service_id}/realtime-seed',
          `/api/services/${activeServiceId}/realtime-seed`,
          { signal },
        )
        if (res.ok) seedTicks = ((res.data.ticks ?? []) as unknown[]).filter(isTick)
      } catch {
        /* Seed is best-effort */
      }
      return {
        ticks: seedTicks,
        timestamps: new Set(seedTicks.map((t) => t.timestamp)),
      }
    },
    enabled: !!activeServiceId,
    staleTime: Infinity,
  })

  useEffect(() => {
    if (!activeServiceId) return
    const pageLoadId = getPageLoadId()
    start(
      `/api/services/${activeServiceId}/realtime-stream`,
      undefined,
      pageLoadId ? { 'X-Page-Load-ID': pageLoadId } : undefined,
    )
    return () => reset()
  }, [activeServiceId, start, reset])

  const allTicks = useMemo(() => {
    const seedTicks = seed?.ticks ?? []
    const seedTimestamps = seed?.timestamps
    if (!seedTimestamps || seedTimestamps.size === 0) {
      return seedTicks.length === 0 ? lines : [...seedTicks, ...lines]
    }
    return [...seedTicks, ...lines.filter((t) => !seedTimestamps.has(t.timestamp))]
  }, [lines, seed])
  const latestTick = allTicks.length > 0 ? allTicks[allTicks.length - 1] : null
  return {
    connected: status === 'streaming',
    lastTickTime: latestTick ? new Date(latestTick.timestamp) : null,
    latestTick,
    allTicks,
    rtDown: latestTick?.status === 'rt_down',
  }
}

export type RollingAvgFn = (extractor: (d: MetricsData) => number, windowSize: number) => number
export type SeriesFn = (extractor: (d: MetricsData) => number) => number[]
export interface TabCommonProps {
  tick: MetricsTick | null
  rtDown: boolean
  historicalHref: string
  series: SeriesFn
  rollingAvg: RollingAvgFn
}

export interface MetricCardSpec {
  title: string
  value?: number
  decimals?: number
  suffix?: string
  rateFn?: (d: MetricsData) => number
  rates?: Array<{ label: string; value: number }>
  thresholds?: { warn: number; critical: number; direction?: 'above' | 'below' | string }
  helpText?: string
}

export function makeRates(rollingAvg: RollingAvgFn, fn: (d: MetricsData) => number) {
  return [
    { label: '5s', value: rollingAvg(fn, 5) },
    { label: '30s', value: rollingAvg(fn, 30) },
    { label: '60s', value: rollingAvg(fn, 60) },
  ]
}

export function seriesTraces(
  series: SeriesFn,
  specs: ReadonlyArray<readonly [name: string, color: string, fn: (d: MetricsData) => number, yaxis?: 'y' | 'y2']>,
) {
  return specs.map(([name, color, fn, yaxis]) => ({ y: series(fn), name, color, ...(yaxis ? { yaxis } : {}) }))
}
