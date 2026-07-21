import { describe, it, expect } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useTickHistory, type MetricsTick } from '@/app/control-room/_sections/useTickHistory'

function makeTick(rps: number): MetricsTick {
  return {
    event: 'metrics_tick',
    event_schema_version: 2,
    timestamp: new Date().toISOString(),
    status: 'ok',
    data: {
      requests_per_second: rps,
      error_rate: 0.01,
      cache_hit_ratio: 0.95,
      bandwidth_mbps: 1.5,
    },
  }
}

describe('useTickHistory', () => {
  it('starts with 60 zero-padded entries and null prevTick', () => {
    const { result } = renderHook(() => useTickHistory([]))
    expect(result.current.history).toHaveLength(60)
    expect(result.current.history.every((e) => e.data.requests_per_second === 0)).toBe(true)
    expect(result.current.prevTick).toBeNull()
  })

  it('pads history to 60 entries with zeros on the left', () => {
    const ticks = [makeTick(10), makeTick(20)]
    const { result } = renderHook(() => useTickHistory(ticks))
    expect(result.current.history).toHaveLength(60)
    expect(result.current.history[57].data.requests_per_second).toBe(0)
    expect(result.current.history[58].data.requests_per_second).toBe(10)
    expect(result.current.history[59].data.requests_per_second).toBe(20)
  })

  it('tracks prevTick as the tick before the current one', () => {
    const tick1 = makeTick(10)
    const tick2 = makeTick(20)
    const { result } = renderHook(() => useTickHistory([tick1, tick2]))
    expect(result.current.prevTick).toBe(tick1)
  })

  it('caps history at 60 entries with no padding', () => {
    const ticks = Array.from({ length: 65 }, (_, i) => makeTick(i))
    const { result } = renderHook(() => useTickHistory(ticks))

    expect(result.current.history).toHaveLength(60)
    expect(result.current.history[0].data.requests_per_second).toBe(5)
    expect(result.current.history[59].data.requests_per_second).toBe(64)
  })

  it('series() extracts values from history including padding', () => {
    const ticks = [makeTick(10), makeTick(20), makeTick(30)]
    const { result } = renderHook(() => useTickHistory(ticks))

    const rps = result.current.series((d) => d.requests_per_second)
    expect(rps).toHaveLength(60)
    expect(rps.slice(0, 57).every((v) => v === 0)).toBe(true)
    expect(rps.slice(57)).toEqual([10, 20, 30])
  })

  it('handles replay burst — all items appear right-aligned', () => {
    const ticks = Array.from({ length: 45 }, (_, i) => makeTick(i))
    const { result } = renderHook(() => useTickHistory(ticks))
    expect(result.current.history).toHaveLength(60)
    const rps = result.current.series((d) => d.requests_per_second)
    expect(rps.slice(0, 15).every((v) => v === 0)).toBe(true)
    expect(rps.slice(15)).toEqual(Array.from({ length: 45 }, (_, i) => i))
  })
})
