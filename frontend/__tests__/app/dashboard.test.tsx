import { withNuqsTestingAdapter } from 'nuqs/adapters/testing'
import { screen, waitFor } from '@testing-library/react'
import { describe, expect, it, test, vi } from 'vitest'
// app/dashboard/page.tsx is now an async RSC shell that SSR-prefetches the
// bundle and dehydrates into <DashboardClient />; RTL can't render an async
// component, so the smoke test targets the client component that owns the
// title + chart/cards.
import DashboardClient, { buildChartLayout } from '@/app/dashboard/_sections/DashboardClient'
import { densifyBarSeries } from '@/lib/chart-helpers'
import { buildTrafficData, getIntervalSeconds } from '@/lib/traffic-chart'
import { createTestQueryClient, renderWithQuery } from '../helpers/page-smoke'
import * as apiLib from '@/lib/api'
import { getOk, postOk } from '@/__tests__/helpers/page-smoke'
import React from 'react'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/AnalyticsCard', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/AnalyticsCard')>()),
  TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div>,
}))
vi.mock('@/components/Map/ChoroplethMap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Map/ChoroplethMap')>()),
  ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div>,
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

test('renders dashboard and fetches data', async () => {
  vi.mocked(apiLib.client.GET).mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({ fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} })
    }
    return getOk({})
  })

  vi.mocked(apiLib.client.POST).mockResolvedValue(postOk({
      summary: { total: 1234 },
      time_series: [],
    }))

  const NuqsWrapper = withNuqsTestingAdapter()
  renderWithQuery(
    <NuqsWrapper>
      <DashboardClient />
    </NuqsWrapper>,
    { queryClient },
  )

  expect(screen.getByText('Dashboard')).toBeInTheDocument()

  // Dashboard mounts the cards grid (Traffic over Time + the rest of
  // the standard layout). Asserting on the first card title proves the
  // bundle fetch resolved without blowing up the render.
  await waitFor(() => {
    expect(screen.getByText('Traffic over Time')).toBeInTheDocument()
  })
})

const baseParams = {
  compareAggregates: null,
  compareMode: false,
  compareStartTime: null,
  startTime: null,
  endTime: null,
  trend: 'off',
  timezone: 'UTC',
  metric: 'requests',
  effectiveInterval: '1 hour',
  hiddenCategories: new Set<string>(),
  catalog: { fields: [] },
}

describe('densifyBarSeries', () => {
  it('fills gaps between the first and last bucket with zeros (1h grid)', () => {
    const sparse = [
      { time: '2026-06-30T09:00:00Z', value: 5 },
      { time: '2026-06-30T12:00:00Z', value: 8 },
    ]
    const dense = densifyBarSeries(sparse, 3600, false)
    expect(dense).toHaveLength(4)
    expect(dense.map((d) => d.value)).toEqual([5, 0, 0, 8])
    expect(dense.map((d) => Date.parse(d.time))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
      Date.parse('2026-06-30T12:00:00Z'),
    ])
  })

  it('densifies space-separated naive timestamps (e.g. ClickHouse JSON) correctly', () => {
    const sparse = [
      { time: '2026-06-30 09:00:00', value: 5 },
      { time: '2026-06-30 12:00:00', value: 8 },
    ]
    const dense = densifyBarSeries(sparse, 3600, false)
    expect(dense).toHaveLength(4)
    expect(dense.map((d) => d.value)).toEqual([5, 0, 0, 8])
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
      { time: '2026-06-30T09:30:00Z', value: 7 },
      { time: '2026-06-30T12:00:00Z', value: 8 },
    ]
    expect(densifyBarSeries(misaligned, 3600, false)).toBe(misaligned)
  })

  it('no-ops when the dense grid would exceed the safety cap', () => {
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
    expect(dense).toHaveLength(6)
    const at10 = dense.filter((d) => d.time === '2026-06-30T10:00:00.000Z')
    expect(at10).toHaveLength(2)
    expect(at10.every((d) => d.value === 0)).toBe(true)
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
    expect(traces[0].y).toEqual([120, 140])
  })

  it('densifies correctly when given shorthand or plural interval formats (e.g. 5m, 1 hours)', () => {
    const aggregates = {
      metric: 'requests',
      interval: '5 minutes',
      time_series: [
        { time: '2026-06-30T09:00:00Z', value: 10 },
        { time: '2026-06-30T09:15:00Z', value: 20 },
      ],
    }
    const traces = buildTrafficData({ ...baseParams, aggregates })
    expect(traces[0].type).toBe('bar')
    expect(traces[0].y).toEqual([10, 0, 0, 20])
    expect(traces[0].width).toBe(270000)
  })

  it('densifies correctly with fractional sub-second timestamps', () => {
    const aggregates = {
      metric: 'requests',
      interval: '1 minute',
      time_series: [
        { time: '2026-06-30 09:00:00.000', value: 3 },
        { time: '2026-06-30 09:02:00.000', value: 7 },
      ],
    }
    const traces = buildTrafficData({ ...baseParams, aggregates })
    expect(traces[0].type).toBe('bar')
    expect(traces[0].y).toEqual([3, 0, 7])
    expect(traces[0].width).toBe(54000)
  })
})

describe('getIntervalSeconds', () => {
  it('resolves canonical interval strings', () => {
    expect(getIntervalSeconds('1 second')).toBe(1)
    expect(getIntervalSeconds('1 minute')).toBe(60)
    expect(getIntervalSeconds('5 minutes')).toBe(300)
    expect(getIntervalSeconds('1 hour')).toBe(3600)
    expect(getIntervalSeconds('1 day')).toBe(86400)
  })

  it('resolves shorthand and plural variants', () => {
    expect(getIntervalSeconds('1s')).toBe(1)
    expect(getIntervalSeconds('1 seconds')).toBe(1)
    expect(getIntervalSeconds('1m')).toBe(60)
    expect(getIntervalSeconds('1 minutes')).toBe(60)
    expect(getIntervalSeconds('5m')).toBe(300)
    expect(getIntervalSeconds('1h')).toBe(3600)
    expect(getIntervalSeconds('1 hours')).toBe(3600)
    expect(getIntervalSeconds('1d')).toBe(86400)
    expect(getIntervalSeconds('1 days')).toBe(86400)
    expect(getIntervalSeconds('10m')).toBe(600)
    expect(getIntervalSeconds('2 hours')).toBe(7200)
  })

  it('returns undefined for invalid or empty intervals', () => {
    expect(getIntervalSeconds(undefined)).toBeUndefined()
    expect(getIntervalSeconds(null)).toBeUndefined()
    expect(getIntervalSeconds('')).toBeUndefined()
    expect(getIntervalSeconds('unknown')).toBeUndefined()
  })
})

describe('buildChartLayout', () => {
  it('includes explicit bargap and bargroupgap for visual spacing parity', () => {
    const layout = buildChartLayout({
      trafficData: [{ type: 'bar' }],
      aggregates: { metric: 'requests' },
      metric: 'requests',
      startTime: '2026-06-30T09:00:00Z',
      endTime: '2026-06-30T10:00:00Z',
      timezone: 'UTC',
      catalog: { fields: [] },
    })
    expect(layout.bargap).toBe(0.1)
    expect(layout.bargroupgap).toBe(0.05)
  })
})
