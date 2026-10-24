import { describe, it, expect } from 'vitest'
import { TIME_HOVER_LAYOUT, makeTimeXAxis, denseTimeGrid } from '@/lib/chart-helpers'
import { denseBarTrace, lineTrace, secondaryYAxis, withSecondaryAxis } from '@/lib/chart-builders'
import { buildTrafficData, type BuildTrafficDataParams } from '@/lib/traffic-chart'
import { buildTrafficDataAsync } from '@/lib/workers/buildTrafficData'

describe('TIME_HOVER_LAYOUT', () => {
  it('exposes the hover + legend keys callers spread into Plotly layouts', () => {
    expect(TIME_HOVER_LAYOUT).toMatchObject({
      hovermode: 'x unified',
      legend: expect.objectContaining({
        orientation: 'h',
        xanchor: 'center',
        yanchor: 'bottom',
      }),
    })
  })
})

describe('makeTimeXAxis', () => {
  it('returns a Plotly xaxis spec with type=date and a 2-element range', () => {
    const axis = makeTimeXAxis('2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z', 'UTC')
    expect(axis.type).toBe('date')
    expect(Array.isArray(axis.range)).toBe(true)
    expect(axis.range).toHaveLength(2)
    expect(axis.range[0]).not.toBe('')
    expect(axis.range[1]).not.toBe('')
    expect(axis.tickformatstops).toBeDefined()
  })

  it('formats range bounds in the supplied timezone', () => {
    const utc = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'UTC')
    const ny = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'America/New_York')
    expect(utc.range[0]).not.toBe(ny.range[0])
    expect(utc.range[0]).toContain('12:00:00')
    expect(ny.range[0]).toContain('07:00:00')
  })

  it('returns empty-string bounds for null/undefined start/end without throwing', () => {
    const both = makeTimeXAxis(null, null, 'UTC')
    expect(both.range).toEqual(['', ''])
    const undef = makeTimeXAxis(undefined, undefined, 'UTC')
    expect(undef.range).toEqual(['', ''])
    const mixed = makeTimeXAxis(null, '2026-01-01T00:00:00Z', 'UTC')
    expect(mixed.range[0]).toBe('')
    expect(mixed.range[1]).not.toBe('')
  })
})

describe('denseTimeGrid', () => {
  it('fills the gaps between the first and last bucket on the hourly grid', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid).toHaveLength(4)
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
      Date.parse('2026-06-30T12:00:00Z'),
    ])
  })

  it('treats a `+00:00` suffix (scoring `hour` format) the same as `Z`', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00+00:00', '2026-06-30T11:00:00+00:00'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00+00:00'),
      Date.parse('2026-06-30T10:00:00+00:00'),
      Date.parse('2026-06-30T11:00:00+00:00'),
    ])
  })

  it('treats space-separated naive timestamps (e.g. ClickHouse JSON) as UTC', () => {
    const grid = denseTimeGrid(
      ['2026-06-30 09:00:00', '2026-06-30 11:00:00'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
    ])
  })

  it('fills a per-minute grid for short scoring windows', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T09:03:00Z'],
      60,
    )
    expect(grid).toHaveLength(4)
  })

  it('no-ops (returns null) for unknown/zero interval', () => {
    const times = ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z']
    expect(denseTimeGrid(times, undefined)).toBeNull()
    expect(denseTimeGrid(times, 0)).toBeNull()
  })

  it('no-ops when there are fewer than 2 distinct buckets', () => {
    expect(denseTimeGrid(['2026-06-30T09:00:00Z'], 3600)).toBeNull()
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T09:00:00Z'], 3600),
    ).toBeNull()
  })

  it('no-ops when the series is already contiguous (no gaps)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T10:00:00Z', '2026-06-30T11:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when buckets are off the interval grid (mixed grain)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T09:30:00Z', '2026-06-30T12:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when the dense grid would exceed the safety cap', () => {
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T11:00:01Z'], 1),
    ).toBeNull()
  })
})

type Row = { t: string | null; v: number | null }

describe('denseBarTrace', () => {
  const rows: Row[] = [
    { t: '2026-10-01T00:00:00+00:00', v: 4 },
    { t: '2026-10-01T03:00:00+00:00', v: 9 },
    { t: null, v: 7 },
  ]
  const base = { rows, getTime: (r: Row) => r.t, getValue: (r: Row) => r.v, intervalSeconds: 3600, color: '#111' }

  it('zero-fills gaps and matches +00:00 rows to the Z grid by epoch ms', () => {
    const t = denseBarTrace({ ...base, name: 'n' })
    expect(t.x).toHaveLength(4)
    expect(t.y).toEqual([4, 0, 0, 9])
    expect(t).toMatchObject({ type: 'bar', name: 'n', marker: { color: '#111' } })
    expect('yaxis' in t).toBe(false)
  })

  it('uses a caller-supplied grid and passes yaxis/showlegend through', () => {
    const grid = ['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z']
    const t = denseBarTrace({ ...base, name: 'n', grid, yaxis: 'y2', showlegend: true })
    expect(t.x).toEqual(grid)
    expect(t.y).toEqual([4, 0])
    expect(t).toMatchObject({ yaxis: 'y2', showlegend: true })
  })

  it('falls back to present buckets when no grid can be built', () => {
    const t = denseBarTrace({ ...base, name: 'n', intervalSeconds: 0 })
    expect(t.x).toEqual(['2026-10-01T00:00:00+00:00', '2026-10-01T03:00:00+00:00'])
    expect(t.y).toEqual([4, 9])
  })
})

describe('lineTrace', () => {
  it('defaults to width 2 and omits optional keys', () => {
    const t = lineTrace({ name: 'l', x: ['a'], y: [1], color: '#222' })
    expect(t).toMatchObject({ type: 'scatter', mode: 'lines', line: { color: '#222', width: 2 } })
    expect('yaxis' in t).toBe(false)
    expect('connectgaps' in t).toBe(false)
  })
})

describe('secondary axis', () => {
  it('builds a right-hand overlaying axis', () => {
    expect(secondaryYAxis('Rate', { ticksuffix: '%' })).toEqual({
      title: 'Rate',
      overlaying: 'y',
      side: 'right',
      showgrid: false,
      rangemode: 'tozero',
      ticksuffix: '%',
    })
  })

  it('widens the right margin while keeping the others', () => {
    const l = withSecondaryAxis({ margin: { l: 50, r: 20 } as { r?: number } }, 'Rate')
    expect(l.margin).toEqual({ l: 50, r: 50 })
    expect(l.yaxis2.title).toBe('Rate')
  })
})

function makeBaseParams(overrides: Partial<BuildTrafficDataParams> = {}): BuildTrafficDataParams {
  return {
    aggregates: {
      metric: 'requests',
      interval: '1 minute',
      time_series: [
        { time: '2026-10-20T10:00:00Z', value: 100 },
        { time: '2026-10-20T10:01:00Z', value: 200 },
        { time: '2026-10-20T10:02:00Z', value: 150 },
        { time: '2026-10-20T10:03:00Z', value: 250 },
        { time: '2026-10-20T10:04:00Z', value: 300 },
        { time: '2026-10-20T10:05:00Z', value: 400 },
      ],
    },
    compareAggregates: null,
    compareMode: false,
    compareStartTime: null,
    startTime: '2026-10-20T10:00:00Z',
    endTime: '2026-10-20T10:05:00Z',
    trend: 'off',
    timezone: 'UTC',
    metric: 'requests',
    effectiveInterval: '1 minute',
    hiddenCategories: new Set(),
    catalog: null,
    ...overrides,
  }
}

describe('buildTrafficData golden characterization', () => {
  it('builds primary bar trace, baseline trace, and sliding-window trend line accurately', () => {
    const params = makeBaseParams({
      trend: '5m',
      compareMode: true,
      compareStartTime: '2026-10-19T10:00:00Z',
      compareAggregates: {
        metric: 'requests',
        interval: '1 minute',
        time_series: [
          { time: '2026-10-19T10:00:00Z', value: 90 },
          { time: '2026-10-19T10:01:00Z', value: 180 },
          { time: '2026-10-19T10:02:00Z', value: 140 },
          { time: '2026-10-19T10:03:00Z', value: 220 },
          { time: '2026-10-19T10:04:00Z', value: 280 },
          { time: '2026-10-19T10:05:00Z', value: 350 },
        ],
      },
    })

    const traces = buildTrafficData(params)
    expect(traces).toHaveLength(3)

    const [primary, baseline, trend] = traces
    expect(primary.type).toBe('bar')
    expect(primary.y).toEqual([100, 200, 150, 250, 300, 400])

    expect(baseline.name).toBe('Comparison Range')
    expect(baseline.x).toEqual(primary.x)
    expect(baseline.y).toEqual([90, 180, 140, 220, 280, 350])

    expect(trend.name).toBe('Trend')
    expect(trend.x).toEqual(primary.x)
    expect(trend.y).toEqual([null, null, null, null, 200, 260])
  })

  it('handles categorized 5xx status breakdown and hiddenCategories', () => {
    const params = makeBaseParams({
      metric: '5xx',
      aggregates: {
        metric: '5xx',
        interval: '1 minute',
        time_series: [
          { time: '2026-10-20T10:00:00Z', category: '500', value: 12 },
          { time: '2026-10-20T10:00:00Z', category: '503', value: 4 },
          { time: '2026-10-20T10:01:00Z', category: '500', value: 15 },
          { time: '2026-10-20T10:01:00Z', category: '503', value: 2 },
        ],
      },
      startTime: '2026-10-20T10:00:00Z',
      endTime: '2026-10-20T10:01:00Z',
      hiddenCategories: new Set(['503']),
    })

    const traces = buildTrafficData(params)
    expect(traces).toHaveLength(2)
    expect(traces[0].name).toBe('500')
    expect(traces[0].visible).toBe(true)
    expect(traces[1].name).toBe('503')
    expect(traces[1].visible).toBe('legendonly')
  })
})

function makeAggregates(rowCount: number) {
  const time_series = []
  for (let i = 0; i < rowCount; i++) {
    time_series.push({ time: `2026-06-01T${String(i % 24).padStart(2, '0')}:00:00Z`, value: i })
  }
  return { time_series, metric: 'requests', interval: 'hour' }
}

const defaultAsyncParams = {
  compareAggregates: null,
  compareMode: false,
  compareStartTime: null,
  startTime: null,
  endTime: null,
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
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates })
    expect(Array.isArray(traces)).toBe(true)
    expect(traces.length).toBeGreaterThan(0)
    expect(traces[0]).toMatchObject({ type: 'bar', name: 'requests' })
    expect(traces[0].x).toHaveLength(50)
    expect(traces[0].y).toHaveLength(50)
  })

  it('returns an empty array when there is no time_series data', async () => {
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: { time_series: [], metric: 'requests' } })
    expect(traces).toEqual([])
  })

  it('handles a large dataset (above WORKER_THRESHOLD) via the sync fallback in test env', async () => {
    const aggregates = makeAggregates(2500)
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates })
    expect(traces[0].x).toHaveLength(2500)
  })

  it('propagates the trend overlay when trend != "off"', async () => {
    const aggregates = makeAggregates(100)
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates, trend: 'auto' })
    expect(traces).toHaveLength(2)
    expect(traces[1].name).toContain('Trend')
  })
})
