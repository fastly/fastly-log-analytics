import { describe, it, expect } from 'vitest'
import type { SSELine } from '@/hooks/useSSE'

/**
 * Mirror of parseMetricsTick from ControlRoomClient.tsx — kept in sync
 * so we can unit-test the parsing without exporting a component-private
 * function. If the real implementation changes, update this copy.
 */
interface MetricsData {
  requests_per_second: number
  error_rate: number
  cache_hit_ratio: number
  bandwidth_mbps: number
  [key: string]: unknown
}

interface MetricsTick {
  event: string
  event_schema_version: number
  timestamp: string
  status: 'ok' | 'rt_down'
  data: MetricsData
  aggregate_delay?: number
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

const REALISTIC_SSE_LINE: SSELine = {
  event: 'metrics_tick',
  event_schema_version: 2,
  timestamp: '2026-07-08T15:19:38.466551+00:00',
  status: 'ok',
  data: {
    requests_per_second: 42.5,
    error_rate: 0.01,
    cache_hit_ratio: 0.95,
    bandwidth_mbps: 1.5,
    total_requests: 42,
    total_hits: 40,
    total_miss: 2,
    total_pass: 0,
    total_errors: 0,
    status_breakdown: { status_2xx: 100 },
    estimated_cost_usd: 0.001,
    origin_requests_per_second: 5.0,
    origin_bandwidth_mbps: 0.2,
    shield_requests: 10,
    shield_hit_ratio: 0.05,
    pass_requests: 0,
    synth_requests: 0,
    waf_blocked: 0,
    waf_logged: 0,
    waf_passed: 42,
    pop_count: 2,
    degraded_pops: [],
    origin_offload: 0.85,
    hit_latency_ms: 1.0,
    miss_latency_ms: 150.0,
    pass_latency_ms: 80.0,
    h2_pct: 60.0,
    h3_pct: 30.0,
    ddos_detect: 0,
    ddos_mitigate: 0,
    status_detail: { '200': 40, '304': 2 },
    object_size_distribution: { '1k': 10, '10k': 20, '100k': 12 },
    origin_fetches: 3,
    origin_revalidations: 1,
    shield_hit_requests: 8,
    shield_miss_requests: 2,
    request_collapse_usable: 5,
    request_collapse_unusable: 1,
  },
  aggregate_delay: 9,
}

describe('parseMetricsTick', () => {
  it('parses a realistic SSE line with non-zero values', () => {
    const tick = parseMetricsTick(REALISTIC_SSE_LINE)
    expect(tick).not.toBeNull()
    expect(tick!.data.requests_per_second).toBe(42.5)
    expect(tick!.data.error_rate).toBe(0.01)
    expect(tick!.data.cache_hit_ratio).toBe(0.95)
    expect(tick!.data.bandwidth_mbps).toBe(1.5)
    expect(tick!.event_schema_version).toBe(2)
    expect(tick!.status).toBe('ok')
    expect(tick!.aggregate_delay).toBe(9)
  })

  it('rejects lines that are not metrics_tick', () => {
    expect(parseMetricsTick({ type: 'done' })).toBeNull()
    expect(parseMetricsTick({ event: 'heartbeat' })).toBeNull()
    expect(parseMetricsTick({})).toBeNull()
  })

  it('rejects metrics_tick lines with no data payload', () => {
    expect(parseMetricsTick({ event: 'metrics_tick' })).toBeNull()
    expect(parseMetricsTick({ event: 'metrics_tick', data: null as unknown })).toBeNull()
  })

  it('handles rt_down status', () => {
    const line: SSELine = {
      event: 'metrics_tick',
      status: 'rt_down',
      data: { requests_per_second: 0, error_rate: 0, cache_hit_ratio: 0, bandwidth_mbps: 0 },
    }
    const tick = parseMetricsTick(line)
    expect(tick).not.toBeNull()
    expect(tick!.status).toBe('rt_down')
    expect(tick!.data.requests_per_second).toBe(0)
  })

  it('preserves new Wave 1 fields through parsing', () => {
    const tick = parseMetricsTick(REALISTIC_SSE_LINE)
    expect(tick!.data.origin_offload).toBe(0.85)
    expect(tick!.data.hit_latency_ms).toBe(1.0)
    expect(tick!.data.status_detail).toEqual({ '200': 40, '304': 2 })
  })

  it('defaults event_schema_version to 1 when missing', () => {
    const line: SSELine = {
      event: 'metrics_tick',
      data: { requests_per_second: 1, error_rate: 0, cache_hit_ratio: 0, bandwidth_mbps: 0 },
    }
    const tick = parseMetricsTick(line)
    expect(tick!.event_schema_version).toBe(1)
  })
})
