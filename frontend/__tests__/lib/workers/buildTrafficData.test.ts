import { describe, expect, it } from 'vitest'

import { buildTrafficDataAsync } from '@/lib/workers/buildTrafficData'

// vitest runs in jsdom with NODE_ENV=test, so buildTrafficDataAsync
// short-circuits the Worker path and resolves with the sync impl.
// That's the right behavior under test — we exercise the result
// parity here. The actual worker pathway is exercised on the
// browser side (verified manually via DevTools Performance tab).

function makeAggregates(rowCount: number) {
  const time_series = []
  for (let i = 0; i < rowCount; i++) {
    time_series.push({ time: `2026-06-01T${String(i % 24).padStart(2, '0')}:00:00Z`, value: i })
  }
  return { time_series, metric: 'requests', interval: 'hour' }
}

const defaultParams = {
  compareAggregates: null,
  compareMode: false,
  compareStartTime: null,
  startTime: null,
  trend: 'off',
  timezone: 'UTC',
  metric: 'requests',
  effectiveInterval: 'hour',
  hiddenCategories: new Set<string>(),
  catalog: { fields: [] },
}

describe('buildTrafficDataAsync (sync test path)', () => {
  it('resolves with the same trace shape the sync version produces for a small dataset', async () => {
    const aggregates = makeAggregates(50)
    const traces = await buildTrafficDataAsync({ ...defaultParams, aggregates })
    expect(Array.isArray(traces)).toBe(true)
    expect(traces.length).toBeGreaterThan(0)
    expect(traces[0]).toMatchObject({ type: 'bar', name: 'requests' })
    expect(traces[0].x).toHaveLength(50)
    expect(traces[0].y).toHaveLength(50)
  })

  it('returns an empty array when there is no time_series data', async () => {
    const traces = await buildTrafficDataAsync({ ...defaultParams, aggregates: { time_series: [], metric: 'requests' } })
    expect(traces).toEqual([])
  })

  it('handles a large dataset (above WORKER_THRESHOLD) via the sync fallback in test env', async () => {
    // Even at the "would-go-to-worker" size, NODE_ENV=test forces the
    // sync path. Asserts the threshold check doesn't break the
    // synchronous-resolution contract.
    const aggregates = makeAggregates(2500)
    const traces = await buildTrafficDataAsync({ ...defaultParams, aggregates })
    expect(traces[0].x).toHaveLength(2500)
  })

  it('propagates the trend overlay when trend != "off"', async () => {
    const aggregates = makeAggregates(100)
    const traces = await buildTrafficDataAsync({ ...defaultParams, aggregates, trend: 'auto' })
    // Original requests trace + auto-trend overlay = 2 traces.
    expect(traces).toHaveLength(2)
    expect(traces[1].name).toContain('Trend')
  })
})
