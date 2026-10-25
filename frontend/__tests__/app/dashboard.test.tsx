import { withNuqsTestingAdapter } from 'nuqs/adapters/testing'
import { screen, waitFor, within } from '@testing-library/react'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
import DashboardClient, { buildChartLayout } from '@/app/dashboard/_sections/DashboardClient'
import AssetsClient, { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import NetworkPage from '@/app/network/page'
import { densifyBarSeries } from '@/lib/chart-helpers'
import { buildTrafficData, getIntervalSeconds } from '@/lib/utils'
import {
  createTestQueryClient,
  renderWithQuery,
  getOk,
  postOk,
  spyOnConsoleError,
  type ReportLayoutChildArgs,
} from '../helpers/page-smoke'
import * as apiLib from '@/lib/api'
import React, { type ReactNode } from 'react'

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

const { mockGet, mockPost } = vi.hoisted(() => ({
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockGet: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockPost: vi.fn<(...args: any[]) => any>(),
}))

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    GET: mockGet,
    POST: mockPost,
    use: vi.fn(),
  },
  extractApiError: vi.fn((e: unknown) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

let reportLayoutChildArgs: ReportLayoutChildArgs | null = null
let useMockDataTable = false
let useMockAnalyticsCard = false

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ReportLayout: (props: any) => {
      if (!reportLayoutChildArgs) return <actual.ReportLayout {...props} />
      return (
        <div>
          <h1>{props.title}</h1>
          <div data-testid="header-actions">{props.headerActions}</div>
          {props.children(reportLayoutChildArgs)}
        </div>
      )
    },
  }
})

vi.mock('@/components/DataTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/DataTable')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    DataTable: (props: any) => (useMockDataTable ? <div data-testid="data-table" /> : <actual.DataTable {...props} />),
  }
})

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/AnalyticsCard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/AnalyticsCard')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    AnalyticsCard: (props: any) => {
      if (!useMockAnalyticsCard) return <actual.AnalyticsCard {...props} />
      return (
        <div data-testid="analytics-card">
          <h3>{props.title}</h3>
          {props.error ? <div role="alert">card error</div> : null}
          {props.children}
        </div>
      )
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    LazyMount: (props: any) => (useMockAnalyticsCard ? <>{props.children}</> : <actual.LazyMount {...props} />),
    TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div>,
  }
})
vi.mock('@/components/Map/ChoroplethMap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Map/ChoroplethMap')>()),
  ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div>,
}))
vi.mock('@/components/Map/NetworkMap', () => ({
  NetworkMap: () => <div data-testid="network-map" />,
}))
vi.mock('@/components/Map/ShieldingMap', () => ({
  ShieldingMap: () => <div data-testid="shielding-map" />,
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

describe('Dashboard page & chart helpers', () => {
  beforeEach(() => {
    queryClient.clear()
    reportLayoutChildArgs = null
    useMockDataTable = false
    useMockAnalyticsCard = false
    mockGet.mockReset()
    mockPost.mockReset()
  })

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
})

describe('Assets & Shield page', () => {
  beforeEach(() => {
    queryClient.clear()
    errorSpy = spyOnConsoleError()
    mockPost.mockReset()
    mockGet.mockReset()
    useMockDataTable = false
    useMockAnalyticsCard = false
    reportLayoutChildArgs = {
      startTime: '2026-08-19T12:00:00Z',
      endTime: '2026-08-19T13:00:00Z',
      activeServiceId: 'test-service-id',
      filterPayload: {},
      intervalButtons: null,
      bucketSeconds: 3600,
    }
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

const MOCK_ASSETS_DATA = {
  has_data: true,
  missing_fields: [],
  asset_type_breakdown: [
    {
      asset_type: 'Images',
      requests: 50000,
      egress_bytes: 250000000,
      cache_hit_ratio: 0.945,
      compression_rate: 0.85,
    },
    {
      asset_type: 'JavaScript/CSS',
      requests: 30000,
      egress_bytes: 60000000,
      cache_hit_ratio: 0.982,
      compression_rate: 0.92,
    },
  ],
  cache_performance: [
    {
      asset_type: 'Images',
      cache_status: 'HIT',
      requests: 47250,
      bytes: 236250000,
    },
    {
      asset_type: 'Images',
      cache_status: 'MISS',
      requests: 2750,
      bytes: 13750000,
    },
  ],
  compression_performance: [
    {
      asset_type: 'JavaScript/CSS',
      content_encoding: 'gzip',
      requests: 20000,
      bytes: 40000000,
    },
    {
      asset_type: 'JavaScript/CSS',
      content_encoding: 'br',
      requests: 7600,
      bytes: 15200000,
    },
  ],
  large_uncompressed_assets: [
    {
      url: '/static/bundle-huge.js',
      requests: 1200,
      avg_bytes: 3750.0,
      total_bytes: 4500000,
      status: 200,
    },
  ],
  low_ttl_assets: [
    {
      url: '/feed.json',
      requests: 3500,
      avg_ttl: 60.0,
      asset_type: 'API/Dynamic',
    },
  ],
}

test('assets-shield page mounts and renders title and layout shell', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  expect(screen.getByText('Assets & Shield')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders zero-CLS layout reservations and contextual skeletons during loading', async () => {
  mockPost.mockReturnValue(new Promise(() => {})) // Never resolves to keep loading state

  renderWithQuery(<AssetsClient />, { queryClient })

  // Verify zero-CLS card containers exist
  const overviewGrid = screen.getByTestId('assets-overview-grid')
  expect(overviewGrid).toBeInTheDocument()
  expect(overviewGrid.className).toContain('[contain-intrinsic-size:160px]')
  expect(overviewGrid.className).toContain('[content-visibility:auto]')

  const cacheCard = screen.getByTestId('assets-cache-efficiency-card')
  expect(cacheCard).toBeInTheDocument()
  expect(cacheCard.className).toContain('[contain-intrinsic-size:420px]')
  expect(cacheCard.className).toContain('[content-visibility:auto]')

  const compressionCard = screen.getByTestId('assets-compression-card')
  expect(compressionCard).toBeInTheDocument()
  expect(compressionCard.className).toContain('[contain-intrinsic-size:420px]')
  expect(compressionCard.className).toContain('[content-visibility:auto]')

  // Verify contextual in-place loading messages
  expect(screen.getByText('Evaluating cache absorption efficiency...')).toBeInTheDocument()
  expect(screen.getByText('Evaluating payload compression efficiency...')).toBeInTheDocument()
})

test('assets-shield renders full data: breakdown, cache performance, compression, and hotspots', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
  expect(screen.getByText('/feed.json')).toBeInTheDocument()
  expect(screen.getByTestId('assets-cache-efficiency-card')).toBeInTheDocument()
  expect(screen.getByTestId('assets-compression-card')).toBeInTheDocument()
  expect(screen.getByTestId('assets-uncompressed-card')).toBeInTheDocument()
  const overviewGrid = screen.getByTestId('assets-overview-grid')
  expect(within(overviewGrid).getByText('Images')).toBeInTheDocument()
  expect(within(overviewGrid).getByText('JavaScript/CSS')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders missing fields banner when optional fields are missing', async () => {
  mockPost.mockResolvedValue({
    data: {
      ...MOCK_ASSETS_DATA,
      missing_fields: ['resp_header_content_encoding', 'ttl'],
    },
  })

  renderWithQuery(<AssetsClient />, { queryClient })

  const banner = await screen.findByTestId('missing-asset-fields-banner')
  expect(banner).toBeInTheDocument()
  expect(within(banner).getByText(/Optional Log Fields Missing/i)).toBeInTheDocument()
  expect(within(banner).getByText(/resp_header_content_encoding, ttl/i)).toBeInTheDocument()

  const configLink = within(banner).getByRole('link', { name: /Configure Service Log Fields/i })
  expect(configLink).toBeInTheDocument()
  expect(configLink.getAttribute('href')).toContain('/service-config')
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders empty state when asset breakdown has no data', async () => {
  mockPost.mockResolvedValue({
    data: {
      has_data: false,
      missing_fields: [],
      asset_type_breakdown: [],
      cache_performance: [],
      compression_performance: [],
      large_uncompressed_assets: [],
      low_ttl_assets: [],
    },
  })

  renderWithQuery(<AssetsClient />, { queryClient })

  await waitFor(() => {
    expect(screen.getByText('No Asset Metrics Yet')).toBeInTheDocument()
  })
  expect(
    screen.getByText(/No static assets or dynamic pages were requested in this time window/i),
  ).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield refetches when only the custom range start changes', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })
  const base = {
    endTime: '2026-08-19T13:00:00Z',
    timezone: 'UTC',
    activeServiceId: 'test-service-id',
    filterPayload: {},
    config: {},
    relativeRange: null,
    isAutoRange: false,
    anchor: '2026-08-19T13:00:00Z',
  }
  const ui = (startTime: string) => <AssetsReportContent {...base} startTime={startTime} />

  const { rerender } = renderWithQuery(ui('2026-08-19T12:00:00Z'), { queryClient })
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(1))

  rerender(ui('2026-08-19T11:00:00Z'))
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(2))
  expect(mockPost.mock.calls[1][1].body.start_time).toBe('2026-08-19T11:00:00Z')
})

test('assets-shield first paint hits the SSR-seeded cache entry (no refetch)', async () => {
  const { resolveRangeWire, quantizeAnchor } = await import('@/lib/utils')
  const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
  const { rangeKey } = resolveRangeWire({
    relativeRange: null,
    isAutoRange: false,
    startTime: '2026-08-19T12:00:00Z',
    endTime: '2026-08-19T13:00:00Z',
    anchor,
  })
  const seedClient = createTestQueryClient({ queries: { staleTime: 60_000 } })
  seedClient.setQueryData(['assets', 'aggregates', 'test-service-id', rangeKey, anchor, {}], MOCK_ASSETS_DATA)
  mockPost.mockReturnValue(new Promise(() => {}))

  renderWithQuery(
    <AssetsReportContent
      startTime="2026-08-19T12:00:00Z"
      endTime="2026-08-19T13:00:00Z"
      activeServiceId="test-service-id"
      filterPayload={{}}
      relativeRange={null}
      isAutoRange={false}
      anchor={anchor}
    />,
    { queryClient: seedClient },
  )

  expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
  expect(mockPost).not.toHaveBeenCalled()
})

test('assets-shield renders the uncompressed and low-TTL tables with encoded drill-down links', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  const link = await screen.findByRole('link', { name: '/static/bundle-huge.js' })
  expect(link.getAttribute('href')).toContain('filter_url=%2Fstatic%2Fbundle-huge.js')
  for (const name of ['Static Asset URL', 'Requests', 'Avg Size', 'Bandwidth', 'Type', 'Cache Misses', 'Avg TTL']) {
    expect(screen.getAllByRole('columnheader', { name }).length).toBeGreaterThan(0)
  }
  expect(screen.getByText('60.0s')).toBeInTheDocument()
})
})

describe('Network & ASN Health page', () => {
  beforeEach(() => {
    queryClient.clear()
    errorSpy = spyOnConsoleError()
    mockPost.mockReset()
    mockGet.mockReset()
    useMockDataTable = true
    useMockAnalyticsCard = true
    reportLayoutChildArgs = {
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-01T01:00:00Z',
      activeServiceId: 'test-svc',
      filterPayload: {},
      intervalButtons: null,
      bucketSeconds: 3600,
    }
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

test('network page mounts and renders unavailable state when fields not active', async () => {
  mockPost.mockResolvedValue({ data: { available: false, reason: 'Network fields not enabled' } })
  mockGet.mockResolvedValue({ data: { data: [] } })

  renderWithQuery(<NetworkPage />, { queryClient })
  expect(screen.getByText('Network & ASN Health')).toBeInTheDocument()
  expect(await screen.findByText('Network Metrics Unavailable')).toBeInTheDocument()
  expect(screen.getByText('Network fields not enabled')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('network page renders full dashboard with transport metrics and leaderboards', async () => {
  mockPost.mockImplementation(async (url: string) => {
    if (url.includes('/api/network/bundle')) {
      return {
        data: {
          transport: {
            tcp_rtt_p50_ms: 24.5,
            tcp_rtt_p90_ms: 45.2,
            tcp_rtt_p95_ms: 62.1,
            tcp_rtt_p99_ms: 98.4,
            retrans_rate_pct: 0.15,
            packet_loss_pct: 0.05,
            delivery_rate_mbps: 42.8,
            protocol_distribution: [
              { protocol: 'HTTP/3', share_pct: 65.4, requests: 6540 },
              { protocol: 'HTTP/2', share_pct: 30.1, requests: 3010 },
              { protocol: 'HTTP/1.1', share_pct: 4.5, requests: 450 },
            ],
            missing_fields: [],
          },
        },
      }
    }
    if (url.includes('/api/network-health')) {
      return {
        data: {
          available: true,
          summary: {
            global_health_score: 94.2,
            avg_rtt_ms: 28.4,
            worst_asn: { label: 'Comcast (7922)', score: 78.5 },
            worst_country: { label: 'Australia (AU)', score: 81.2 },
          },
          leaderboard: [
            {
              asn: 7922,
              label: 'Comcast (7922)',
              total_reqs: 50000,
              health_score_now: 88.5,
              health_score_1h_ago: 87.0,
              p95_rtt_us: 45000,
              p99_rtt_us: 75000,
              trend: 'improving',
            },
          ],
          has_metro: true,
          metro_leaderboard: [
            {
              city: 'San Jose',
              raw_city: 'San Jose',
              region: 'CA',
              country: 'US',
              total_reqs: 25000,
              health_score: 95.0,
            },
          ],
        },
      }
    }
    if (url.includes('/api/network-quality')) {
      return {
        data: {
          available: true,
          by_country: [{ label: 'United States', rtt_ms: 22.1, reqs: 10000 }],
          by_asn: [{ label: 'Comcast (7922)', rtt_ms: 24.5, reqs: 5000 }],
          by_region: [{ label: 'California', rtt_ms: 18.2, reqs: 4000 }],
          by_pop: [{ label: 'SJC', rtt_ms: 15.1, reqs: 8000 }],
          countries: ['US'],
          scatter: [{ rtt_ms: 20, ttfb_ms: 45 }],
        },
      }
    }
    return { data: {} }
  })

  mockGet.mockResolvedValue({
    data: {
      data: [
        {
          pop: 'SJC',
          requests: 50000,
          errors: 10,
          error_rate: 0.02,
          p50_rtt_us: 15000,
          p95_ttfb_ms: 45.0,
          cache_hit_rate: 92.5,
          bandwidth_bytes: 104857600,
        },
      ],
    },
  })

  renderWithQuery(<NetworkPage />, { queryClient })

  // Verify shell and summary cards
  expect(screen.getByText('Network & ASN Health')).toBeInTheDocument()
  expect(await screen.findByText('94.2/100')).toBeInTheDocument()
  expect(screen.getByText('28.4ms')).toBeInTheDocument()
  expect(screen.getAllByText('Comcast (7922)').length).toBeGreaterThan(0)

  // Verify Transport Section
  expect(await screen.findByText('Edge Transport & TCP Performance')).toBeInTheDocument()
  expect(await screen.findByText('24.5')).toBeInTheDocument()
  expect(screen.getByText('/ 62.1ms')).toBeInTheDocument()
  expect(screen.getByText('42.8')).toBeInTheDocument()
  expect(screen.getByText('HTTP/3')).toBeInTheDocument()
  expect(screen.getByText('65.4%')).toBeInTheDocument()

  // Verify ASN Leaderboard
  expect(screen.getByText('ASN Performance Leaderboard')).toBeInTheDocument()
  expect(screen.getByText('IMPROVING')).toBeInTheDocument()
  for (const name of ['ASN', 'P95 RTT', 'P99 RTT', '1h Change', 'Trend', 'Metro / City']) {
    expect(screen.getAllByRole('columnheader', { name }).length).toBeGreaterThan(0)
  }
  expect(screen.getByText('45.0ms')).toBeInTheDocument()
  expect(screen.getByText('+1.5')).toBeInTheDocument()
  expect(screen.getByText('San Jose')).toBeInTheDocument()

  // Verify PoP Heatmap
  expect(await screen.findByText('Fastly Edge PoP Health Heatmap')).toBeInTheDocument()
  expect(screen.getByText('1 Active PoPs')).toBeInTheDocument()

  expect(errorSpy).not.toHaveBeenCalled()
})

test('transport section displays missing fields banner when optional fields not logged', async () => {
  mockPost.mockImplementation(async (url: string) => {
    if (url.includes('/api/network/bundle')) {
      return {
        data: {
          transport: {
            tcp_rtt_p50_ms: null,
            tcp_rtt_p90_ms: null,
            tcp_rtt_p95_ms: null,
            tcp_rtt_p99_ms: null,
            retrans_rate_pct: null,
            packet_loss_pct: null,
            delivery_rate_mbps: null,
            protocol_distribution: [],
            missing_fields: ['tcp_rtt', 'retrans', 'ploss'],
          },
        },
      }
    }
    return {
      data: {
        available: true,
        summary: { global_health_score: 90 },
      },
    }
  })
  mockGet.mockResolvedValue({ data: { data: [] } })

  renderWithQuery(<NetworkPage />, { queryClient })

  expect(await screen.findByTestId('missing-transport-fields-banner')).toBeInTheDocument()
  expect(screen.getByText('tcp_rtt, retrans, ploss')).toBeInTheDocument()
  expect(screen.getByText('Service Configuration')).toHaveAttribute('href', '/service-config')
})

test('network health queries keep the range-wire key shape and request body after the hook migration', async () => {
  mockPost.mockResolvedValue({ data: { available: false, reason: 'Network fields not enabled' } })
  mockGet.mockResolvedValue({ data: { data: [] } })
  const { resolveRangeWire, quantizeAnchor } = await import('@/lib/utils')

  renderWithQuery(<NetworkPage />, { queryClient })
  await screen.findByText('Network Metrics Unavailable')

  const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
  const { rangeKey } = resolveRangeWire({
    relativeRange: null,
    isAutoRange: false,
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    anchor,
  })
  const keys = queryClient.getQueryCache().getAll().map((q) => q.queryKey)
  const prefix = ['test-svc', rangeKey, anchor, {}]
  expect(keys).toContainEqual(['network', 'health', 'core', ...prefix, 3600, 'health_score'])
  expect(keys).toContainEqual(['network', 'health', 'map', ...prefix, 3600, 'all', 'health_score'])
  expect(keys).toContainEqual(['network', 'health', 'shielding', ...prefix])

  const coreCall = mockPost.mock.calls.find(
    ([url, opts]) =>
      url === '/api/network-health' &&
      (opts as { body: { sections: string[] } }).body.sections.length > 0,
  )
  expect(coreCall?.[1]).toMatchObject({
    body: { start_time: '2026-01-01T00:00:00Z', end_time: '2026-01-01T01:00:00Z', range_token: null, anchor },
  })
})

test('UX-4: Network Quality section stays mounted with an error surface on a /network-quality 5xx', async () => {
  mockGet.mockResolvedValue({ data: { data: [] } })
  mockPost.mockImplementation(async (path: string) => {
    if (path === '/api/network-quality') throw new Error('quality boom')
    return { data: { available: true } }
  })

  renderWithQuery(<NetworkPage />, { queryClient })
  expect(await screen.findByText('Network Quality')).toBeInTheDocument()
  expect((await screen.findAllByRole('alert')).length).toBeGreaterThan(0)
})
})
