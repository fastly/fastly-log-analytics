import { useMemo, useCallback } from 'react'

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
  shield_requests?: number
  shield_hit_ratio?: number
  pass_requests?: number
  synth_requests?: number
  waf_blocked?: number
  waf_logged?: number
  waf_passed?: number
  pop_count?: number
  degraded_pops?: string[]

  origin_offload?: number

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

  miss_histogram?: Record<string, number>

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

export interface TickEntry {
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
    const window = allTicks.length > MAX_ENTRIES
      ? allTicks.slice(allTicks.length - MAX_ENTRIES)
      : allTicks
    const entries = window.map((t) => ({
      timestamp: new Date(t.timestamp).getTime(),
      data: t.data,
    }))
    if (entries.length < MAX_ENTRIES) {
      const pad = Array.from({ length: MAX_ENTRIES - entries.length }, () => ({
        timestamp: 0,
        data: EMPTY_DATA,
      }))
      return [...pad, ...entries]
    }
    return entries
  }, [allTicks])

  const prevTick = allTicks.length >= 2 ? allTicks[allTicks.length - 2] : null

  const series = useCallback(
    (extractor: (d: MetricsData) => number): number[] => {
      return history.map((e) => extractor(e.data))
    },
    [history],
  )

  const rollingAvg = useCallback(
    (extractor: (d: MetricsData) => number, windowSize: number): number => {
      const realEntries = history.filter((e) => e.timestamp > 0)
      if (realEntries.length === 0) return 0
      const window = realEntries.slice(-windowSize)
      return window.reduce((sum, e) => sum + extractor(e.data), 0) / window.length
    },
    [history],
  )

  return { history, prevTick, series, rollingAvg }
}
