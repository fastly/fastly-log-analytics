import { describe, it, expect } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useStreamAggregates } from '@/app/sessions/stream/_sections/useStreamAggregates'

describe('useStreamAggregates', () => {
  it('returns null when rows are undefined or empty', () => {
    const { result: r1 } = renderHook(() => useStreamAggregates(undefined))
    expect(r1.current).toBeNull()

    const { result: r2 } = renderHook(() => useStreamAggregates([]))
    expect(r2.current).toBeNull()
  })

  it('aggregates pure video rows (cmcd_ot=v)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'v', cmcd_br: 4000, cmcd_bl: 3000, cmcd_tb: 6000 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'v', cmcd_br: 6000, cmcd_bl: 4000, cmcd_tb: 6000 },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.videoRequests).toBe(2)
    expect(agg.summary.totalRequests).toBe(2)
    expect(agg.summary.avgBitrate).toBe(5000)
    expect(agg.summary.topBitrate).toBe(6000)
    expect(agg.summary.utilization).toBeCloseTo(5000 / 6000)
    expect(agg.timeline).toHaveLength(2)
  })

  it('includes muxed audio/video rows (cmcd_ot=av)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'av', cmcd_br: 3500, cmcd_bl: 2500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'av', cmcd_br: 4500, cmcd_bl: 3500 },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.videoRequests).toBe(2)
    expect(agg.summary.avgBitrate).toBe(4000)
    expect(agg.summary.topBitrate).toBe(4500) // falls back to max bitrate when tb is null
    expect(agg.summary.utilization).toBeCloseTo(4000 / 4500)
  })

  it('falls back to rows with bitrate/buffer when no explicit v or av ot tag exists', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'm', cmcd_br: 2500, cmcd_bl: 4500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: null, cmcd_br: null },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.totalRequests).toBe(2)
    expect(agg.summary.videoRequests).toBe(1)
    expect(agg.summary.avgBitrate).toBe(2500)
    expect(agg.summary.topBitrate).toBe(2500)
    expect(agg.summary.bufferHealthPct).toBe(100)
    expect(agg.timeline).toHaveLength(1)
  })
})
