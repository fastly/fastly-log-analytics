import { describe, expect, it } from 'vitest'

import { buildTrafficData, densifyBarSeries } from '@/app/dashboard/_sections/chartHelpers'

const baseParams = {
  compareAggregates: null,
  compareMode: false,
  compareStartTime: null,
  startTime: null,
  trend: 'off',
  timezone: 'UTC',
  metric: 'requests',
  effectiveInterval: '1 hour',
  hiddenCategories: new Set<string>(),
  catalog: { fields: [] },
}

describe('densifyBarSeries', () => {
  it('fills gaps between the first and last bucket with zeros (1h grid)', () => {
    // 09:00 and 12:00 present, 10:00 + 11:00 missing → 4 contiguous buckets.
    const sparse = [
      { time: '2026-06-30T09:00:00Z', value: 5 },
      { time: '2026-06-30T12:00:00Z', value: 8 },
    ]
    const dense = densifyBarSeries(sparse, 3600, false)
    expect(dense).toHaveLength(4)
    expect(dense.map((d) => d.value)).toEqual([5, 0, 0, 8])
    // Synthesized buckets land exactly on the interval grid.
    expect(dense.map((d) => Date.parse(d.time))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
      Date.parse('2026-06-30T12:00:00Z'),
    ])
  })

  it('no-ops when the interval is unknown', () => {
    const sparse = [
      { time: '2026-06-30T09:00:00Z', value: 5 },
      { time: '2026-06-30T12:00:00Z', value: 8 },
    ]
    expect(densifyBarSeries(sparse, undefined, false)).toBe(sparse)
  })

  it('no-ops when the series is already contiguous', () => {
    const dense = [
      { time: '2026-06-30T09:00:00Z', value: 1 },
      { time: '2026-06-30T10:00:00Z', value: 2 },
      { time: '2026-06-30T11:00:00Z', value: 3 },
    ]
    expect(densifyBarSeries(dense, 3600, false)).toBe(dense)
  })

  it('no-ops when buckets are off the interval grid (mixed grain)', () => {
    const misaligned = [
      { time: '2026-06-30T09:00:00Z', value: 5 },
      { time: '2026-06-30T09:30:00Z', value: 7 }, // 30 min, not on the 1h grid
      { time: '2026-06-30T12:00:00Z', value: 8 },
    ]
    expect(densifyBarSeries(misaligned, 3600, false)).toBe(misaligned)
  })

  it('no-ops when the dense grid would exceed the safety cap', () => {
    // 1-second grid across ~2h = 7201 buckets > MAX_DENSE_BUCKETS (5000).
    const sparse = [
      { time: '2026-06-30T09:00:00Z', value: 1 },
      { time: '2026-06-30T11:00:01Z', value: 1 },
    ]
    expect(densifyBarSeries(sparse, 1, false)).toBe(sparse)
  })

  it('fills per-category on a shared grid for stacked bars', () => {
    const sparse = [
      { time: '2026-06-30T09:00:00Z', value: 2, category: '500' },
      { time: '2026-06-30T11:00:00Z', value: 3, category: '502' },
    ]
    const dense = densifyBarSeries(sparse, 3600, true)
    // 3 buckets (09,10,11) × 2 categories = 6 rows.
    expect(dense).toHaveLength(6)
    const at10 = dense.filter((d) => d.time === '2026-06-30T10:00:00.000Z')
    expect(at10).toHaveLength(2)
    expect(at10.every((d) => d.value === 0)).toBe(true)
    // Every category spans every bucket so Plotly stacks cleanly.
    expect(dense.filter((d) => d.category === '500')).toHaveLength(3)
    expect(dense.filter((d) => d.category === '502')).toHaveLength(3)
  })
})

describe('buildTrafficData gap-fill integration', () => {
  it('zero-fills a sparse requests bar series so all bars share interval width', () => {
    const aggregates = {
      metric: 'requests',
      interval: '1 hour',
      time_series: [
        { time: '2026-06-30T09:00:00Z', value: 5 },
        { time: '2026-06-30T12:00:00Z', value: 8 },
      ],
    }
    const traces = buildTrafficData({ ...baseParams, aggregates })
    expect(traces[0].type).toBe('bar')
    expect(traces[0].y).toEqual([5, 0, 0, 8])
  })

  it('does NOT zero-fill a scatter (latency) series', () => {
    const aggregates = {
      metric: 'p95_latency',
      interval: '1 hour',
      time_series: [
        { time: '2026-06-30T09:00:00Z', value: 120 },
        { time: '2026-06-30T12:00:00Z', value: 140 },
      ],
    }
    const traces = buildTrafficData({ ...baseParams, metric: 'p95_latency', aggregates })
    expect(traces[0].type).toBe('scatter')
    // Untouched: a missing latency bucket must not become a false 0 dip.
    expect(traces[0].y).toEqual([120, 140])
  })
})
