import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'
import { act, render, renderHook, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { axe } from 'vitest-axe'
import { useQuery } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import childProcess, { ChildProcess, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import { syncBuiltinESMExports } from 'node:module'
import { EventEmitter } from 'node:events'
import { join } from 'node:path'
import { Activity, Star } from 'lucide-react'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
import DashboardClient, { buildChartLayout } from '@/app/dashboard/_sections/DashboardClient'
import AssetsClient, { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import NetworkPage from '@/app/network/page'
import QueryPage from '@/app/query/page'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
  SectionHeader,
  StatCard,
} from '@/components/ui/confirm-dialog'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { densifyBarSeries } from '@/lib/chart-helpers'
import {
  buildTrafficData,
  getIntervalSeconds,
  _resetUrlHydrationFlag,
  hydrateFilterStoreFromUrl,
} from '@/lib/utils'
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import { server } from '@/__tests__/helpers/msw/handlers'
import {
  createTestQueryClient,
  makeQueryWrapper,
  renderWithQuery,
  getOk,
  postOk,
  spyOnConsoleError,
  type ReportLayoutChildArgs,
} from '../helpers/page-smoke'
import * as apiLib from '@/lib/api'
import React from 'react'

vi.mock('../../playwright.config', () => ({
  E2E_BACKEND_PORT: 18004,
  E2E_FRONTEND_PORT: 13004,
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

let mockPathname = '/dashboard'
vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: vi.fn(),
    replace: vi.fn(),
    prefetch: vi.fn(),
    back: vi.fn(),
  }),
  useSearchParams: () => new URLSearchParams(window.location.search),
  usePathname: () => mockPathname,
}))

vi.mock('@uiw/react-codemirror', async () => {
  const React = await import('react')
  return {
    default: React.forwardRef(function MockCodeMirror(
      { value, onChange }: { value: string; onChange: (v: string) => void },
      ref: React.Ref<HTMLTextAreaElement>,
    ) {
      return <textarea ref={ref} data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
    }),
  }
})

let useRealApiClient = false
const { mockGet, mockPost } = vi.hoisted(() => ({
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockGet: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockPost: vi.fn<(...args: any[]) => any>(),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    client: {
      GET: vi.fn((...args: unknown[]) =>
        useRealApiClient ? (actual.client.GET as (...a: unknown[]) => unknown)(...args) : mockGet(...args),
      ),
      POST: vi.fn((...args: unknown[]) =>
        useRealApiClient ? (actual.client.POST as (...a: unknown[]) => unknown)(...args) : mockPost(...args),
      ),
      use: actual.client.use,
    },
    extractApiError: vi.fn((...args: Parameters<typeof actual.extractApiError>) =>
      useRealApiClient ? actual.extractApiError(...args) : String(args[0]),
    ),
    getApiBase: vi.fn((...args: Parameters<typeof actual.getApiBase>) =>
      useRealApiClient ? actual.getApiBase(...args) : 'http://test',
    ),
  }
})

let reportLayoutChildArgs: ReportLayoutChildArgs | null = null
let useMockDataTable = false
let useMockAnalyticsCard = false

function resetDefaultStores() {
  useRealApiClient = false
  mockPathname = '/dashboard'
  useServiceStore.setState({
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [{ id: 'test-svc', name: 'Test' }],
  })
  useFilterStore.getState().resetAll()
  useFilterStore.setState({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    filters: [],
    isAutoRange: false,
    compareMode: false,
  })
}

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
    resetDefaultStores()
    queryClient.clear()
    reportLayoutChildArgs = null
    useMockDataTable = false
    useMockAnalyticsCard = false
    mockGet.mockReset()
    mockPost.mockReset()
  })

test('renders dashboard and fetches data', async () => {
  mockGet.mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({ fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} })
    }
    return getOk({})
  })

  mockPost.mockResolvedValue(postOk({
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
    resetDefaultStores()
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
    resetDefaultStores()
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

describe('Query Explorer page', () => {
  const renderWithNuqs = (ui: React.ReactElement, onUrlUpdate?: OnUrlUpdateFunction) =>
    renderWithQuery(ui, {
      queryClient,
      wrapper: withNuqsTestingAdapter({ searchParams: window.location.search, hasMemory: true, onUrlUpdate }),
    })

  beforeEach(() => {
    resetDefaultStores()
    mockPathname = '/query'
    reportLayoutChildArgs = null
    useMockDataTable = false
    useMockAnalyticsCard = false
    mockGet.mockReset()
    mockPost.mockReset()
    useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
    useFilterStore.getState().resetAll()
    _resetUrlHydrationFlag()
    window.history.replaceState({}, '', '/query')
    queryClient.clear()
  })

  afterEach(() => {
    window.history.replaceState({}, '', '/')
  })

  test('renders query page and executes a query', async () => {
    const user = userEvent.setup()
    mockGet.mockImplementation(async (url: string) => {
      if (url.includes('/api/log-fields/catalog')) return { data: { fields: [] } }
      if (url.includes('/api/schema')) return { data: { schema: [] } }
      if (url.includes('/api/presets')) return { data: [] }
      return { data: {} }
    })

    mockPost.mockResolvedValue({
      data: {
        columns: ['timestamp', 'client_ip'],
        data: [{ timestamp: '2026-01-01T00:00:00Z', client_ip: '1.1.1.1' }],
        total_rows: 1,
        elapsed_ms: 10,
      },
    })

    renderWithNuqs(<QueryPage />)
    expect(screen.getByText('Query Explorer')).toBeInTheDocument()
    await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))
    const editor = screen.getByTestId('mock-editor')
    await user.clear(editor)
    await user.type(editor, 'SELECT * FROM logs')
    await user.click(screen.getByRole('button', { name: /run query/i }))

    await waitFor(() => {
      expect(screen.getByText('1.1.1.1')).toBeInTheDocument()
    })
  })

  test('/query keeps ?filters= in the URL after a filter mutation', async () => {
    mockGet.mockResolvedValue({ data: { fields: [] } })
    mockPost.mockResolvedValue({
      data: { columns: [], data: [], total_rows: 0, elapsed_ms: 1 },
    })

    const inbound = { status: { values: ['200'], mode: 'include' } }
    window.history.replaceState(
      {},
      '',
      `/query?filters=${encodeURIComponent(JSON.stringify(inbound))}`,
    )

    _resetUrlHydrationFlag()
    hydrateFilterStoreFromUrl()

    expect(useFilterStore.getState().filters.map((f) => [f.column, f.value, f.mode]))
      .toEqual([['status', '200', 'include']])

    renderWithNuqs(<QueryPage />)

    await waitFor(() => {
      const params = new URLSearchParams(window.location.search)
      expect(params.get('filters')).not.toBeNull()
      const parsed = JSON.parse(params.get('filters')!)
      expect(parsed).toMatchObject({ status: { values: ['200'], mode: 'include' } })
    })

    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })

    await waitFor(() => {
      const params = new URLSearchParams(window.location.search)
      const parsed = JSON.parse(params.get('filters')!)
      expect(parsed).toMatchObject({
        status: { values: ['200'], mode: 'include' },
        country: { values: ['US'], mode: 'include' },
      })
    })
  })

  test('renders engine initializing skeleton when service is not yet initialized', () => {
    useServiceStore.setState({ activeServiceId: null, isInitialized: false })
    mockGet.mockImplementation(() => new Promise(() => {}))

    renderWithNuqs(<QueryPage />)

    expect(screen.getByTestId('query-engine-initializing')).toBeInTheDocument()
    expect(screen.getByText(/Initializing DuckDB analytical engine/i)).toBeInTheDocument()
  })

  test('enforces zero-CLS layout reservations and renders schema sidebar in Raw SQL mode', async () => {
    const user = userEvent.setup()
    mockGet.mockImplementation(async (url: string) => {
      if (url.includes('/api/schema')) {
        return {
          data: {
            schema: [
              { name: 'timestamp', type: 'TIMESTAMP' },
              { name: 'client_ip', type: 'VARCHAR' },
            ],
          },
        }
      }
      return { data: { fields: [] } }
    })

    renderWithNuqs(<QueryPage />)

    const pageRoot = screen.getByTestId('query-page-root')
    expect(pageRoot.className).toContain('contain-intrinsic-size')

    const editorContainer = screen.getByTestId('query-editor-container')
    expect(editorContainer.className).toContain('contain-intrinsic-size')

    await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))

    const sidebar = await screen.findByTestId('query-schema-sidebar')
    expect(sidebar).toBeInTheDocument()
    expect(screen.getByText('timestamp')).toBeInTheDocument()
    expect(screen.getByText('client_ip')).toBeInTheDocument()
  })

  test('displays user-friendly error alert when mutating SQL is rejected by AST validator', async () => {
    const user = userEvent.setup()
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    mockPost.mockResolvedValue({
      error: JSON.stringify({
        detail: {
          error: 'sql_not_permitted',
          message: 'only SELECT / WITH / UNION statements allowed (got DROP)',
        },
      }),
    })

    renderWithNuqs(<QueryPage />)

    await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))
    const editor = screen.getByTestId('mock-editor')
    await user.clear(editor)
    await user.type(editor, 'DROP TABLE logs')
    await user.click(screen.getByRole('button', { name: /run query/i }))

    const alert = await screen.findByTestId('query-error-alert')
    expect(alert).toBeInTheDocument()
    expect(alert.textContent).toMatch(/only SELECT \/ WITH \/ UNION statements allowed/i)
  })

  test('renders CSV export button and virtualized results table after query execution', async () => {
    const user = userEvent.setup()
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    mockPost.mockResolvedValue({
      data: {
        columns: ['url', 'requests'],
        data: [
          { url: '/index.html', requests: 42 },
          { url: '/api/v1/items', requests: 19 },
        ],
        total_rows: 2,
        elapsed_ms: 8,
      },
    })

    renderWithNuqs(<QueryPage />)

    await user.click(screen.getByRole('button', { name: /run query/i }))

    await waitFor(() => {
      expect(screen.getByTestId('query-results-table')).toBeInTheDocument()
    })
    expect(screen.getByTestId('query-csv-export-btn')).toBeInTheDocument()
    expect(screen.getByText('/index.html')).toBeInTheDocument()
  })

  test('hydrates Raw SQL mode when ?q= URL parameter is provided', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    window.history.replaceState({}, '', '/query?q=' + encodeURIComponent('SELECT count(*) FROM logs'))

    renderWithNuqs(<QueryPage />)

    const editor = await screen.findByTestId('mock-editor')
    expect((editor as HTMLTextAreaElement).value).toBe('SELECT count(*) FROM logs')
  })

  test('deep link ?mode=raw opens the Raw SQL editor', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    window.history.replaceState({}, '', '/query?mode=raw')

    renderWithNuqs(<QueryPage />)

    expect(await screen.findByTestId('mock-editor')).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /edit raw sql/i })).toHaveAttribute('aria-selected', 'true')
  })

  test('an unrecognised ?mode= value stays in structured mode', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    window.history.replaceState({}, '', '/query?mode=bogus')

    renderWithNuqs(<QueryPage />)

    expect(await screen.findByRole('tab', { name: /edit raw sql/i })).toHaveAttribute('aria-selected', 'false')
    expect(screen.queryByTestId('mock-editor')).not.toBeInTheDocument()
  })

  test('switching modes writes and clears ?mode=raw while preserving other params', async () => {
    const user = userEvent.setup()
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    window.history.replaceState({}, '', '/query?start_time=2026-01-01T00:00:00Z&end_time=2026-01-01T01:00:00Z')
    const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()

    renderWithNuqs(<QueryPage />, onUrlUpdate)

    await user.click(await screen.findByRole('tab', { name: /edit raw sql/i }))
    await waitFor(() => {
      const last = onUrlUpdate.mock.calls.at(-1)?.[0]
      expect(last?.searchParams.get('mode')).toBe('raw')
      expect(last?.searchParams.get('start_time')).toBe('2026-01-01T00:00:00Z')
      expect(last?.options.history).toBe('replace')
    })

    await user.click(screen.getByRole('tab', { name: /structured/i }))
    await waitFor(() => {
      const last = onUrlUpdate.mock.calls.at(-1)?.[0]
      expect(last?.searchParams.has('mode')).toBe(false)
      expect(last?.searchParams.get('end_time')).toBe('2026-01-01T01:00:00Z')
    })
  })

  test('dashboard "See Raw Logs" deep link (?start_time&end_time&filters) hydrates the filter store', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    const filters = { status_2: { values: ['500', '503'], mode: 'exclude' } }
    window.history.replaceState(
      {},
      '',
      `/query?start_time=2026-02-01T00:00:00Z&end_time=2026-02-01T06:00:00Z&filters=${encodeURIComponent(JSON.stringify(filters))}`,
    )

    renderWithNuqs(<QueryPage />)

    await screen.findByText('Query Explorer')
    const state = useFilterStore.getState()
    expect(state.startTime).toBe('2026-02-01T00:00:00Z')
    expect(state.endTime).toBe('2026-02-01T06:00:00Z')
    expect(state.filters.map((f) => [f.column, f.value, f.mode])).toEqual([
      ['status', '500', 'exclude'],
      ['status', '503', 'exclude'],
    ])
  })
})

describe('UI primitives & a11y', () => {
  beforeEach(() => {
    resetDefaultStores()
    useMockAnalyticsCard = false
  })

  describe('ReviewCard', () => {
    it('renders children inside the card container', () => {
      render(
        <ReviewCard>
          <span data-testid="child">hello</span>
        </ReviewCard>,
      )
      expect(screen.getByTestId('child')).toBeInTheDocument()
    })

    it('renders header with optional icon and content text', () => {
      const { container } = render(
        <ReviewCard>
          <ReviewHeader icon={Star}>Summary</ReviewHeader>
          <ReviewContent>body text</ReviewContent>
        </ReviewCard>,
      )
      expect(screen.getByText('Summary')).toBeInTheDocument()
      expect(screen.getByText('body text')).toBeInTheDocument()
      expect(container.querySelector('svg')).toBeInTheDocument()
    })

    it('renders a default ReviewItem with label and value stacked', () => {
      render(<ReviewItem label="Service" value="cdn-prod" />)
      expect(screen.getByText('Service')).toBeInTheDocument()
      expect(screen.getByText('cdn-prod')).toBeInTheDocument()
    })

    it('renders the between variant with label and value side by side', () => {
      const { container } = render(
        <ReviewItem label="Status" value="active" variant="between" />,
      )
      expect(container.firstChild).toHaveClass('justify-between')
      expect(screen.getByText('active')).toBeInTheDocument()
    })
  })

  describe('SectionHeader', () => {
    it('renders the title text', () => {
      render(<SectionHeader title="Overview" />)
      expect(screen.getByRole('heading', { name: /overview/i })).toBeInTheDocument()
    })

    it('renders the icon when provided', () => {
      const { container } = render(<SectionHeader title="With Icon" icon={Star} />)
      expect(container.querySelector('svg')).toBeInTheDocument()
    })

    it('renders inline children alongside the title', () => {
      render(
        <SectionHeader title="Details">
          <span data-testid="action-slot"> · extra</span>
        </SectionHeader>,
      )
      expect(screen.getByTestId('action-slot')).toBeInTheDocument()
    })

    it('applies the optional className to the root element', () => {
      const { container } = render(
        <SectionHeader title="Styled" className="custom-class" />,
      )
      expect(container.firstChild).toHaveClass('custom-class')
    })
  })

  describe('SkeletonGrid', () => {
    it('renders the configured number of skeleton blocks', () => {
      const { container } = render(<SkeletonGrid count={4} />)
      expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(4)
    })

    it('renders a custom count when provided', () => {
      const { container } = render(<SkeletonGrid count={7} />)
      expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(7)
    })

    it('renders nothing when count is 0', () => {
      const { container } = render(<SkeletonGrid count={0} />)
      expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(0)
    })

    it('applies the default height inline style', () => {
      const { container } = render(<SkeletonGrid count={1} />)
      const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
      expect(block.style.height).toBe('120px')
    })

    it('honors a custom height prop', () => {
      const { container } = render(<SkeletonGrid count={1} height="200px" />)
      const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
      expect(block.style.height).toBe('200px')
    })
  })

  describe('StatCard', () => {
    it('renders title, value, and sub', () => {
      render(<StatCard title="Requests" value="1,234" sub="last 24h" />)
      expect(screen.getByText('Requests')).toBeInTheDocument()
      expect(screen.getByText('1,234')).toBeInTheDocument()
      expect(screen.getByText('last 24h')).toBeInTheDocument()
    })

    it('renders the icon when provided', () => {
      const { container } = render(
        <StatCard title="Hits" value={42} sub="ok" icon={Activity} />,
      )
      expect(container.querySelector('svg')).toBeInTheDocument()
    })

    it('shows a skeleton instead of the value when loading', () => {
      const { container } = render(
        <StatCard title="Loading" value="hidden-value" sub="..." loading />,
      )
      expect(screen.queryByText('hidden-value')).not.toBeInTheDocument()
      expect(container.querySelector('[data-slot="skeleton"]')).toBeInTheDocument()
    })

    it('renders the tooltip trigger when tooltip prop is provided', () => {
      const { container } = render(
        <StatCard title="Tipped" value="10" sub="info" tooltip="more info" />,
      )
      expect(container.querySelector('svg.lucide-circle-help, svg')).toBeInTheDocument()
    })
  })

  describe('Switch', () => {
    test('takes its accessible name from an associated <label htmlFor>', () => {
      render(
        <>
          <label htmlFor="edge-only">Edge-only logging</label>
          <Switch id="edge-only" />
        </>,
      )
      expect(screen.getByRole('switch', { name: 'Edge-only logging' })).toBeInTheDocument()
    })

    test('an explicit aria-label wins', () => {
      render(<Switch aria-label="Background Sync" />)
      expect(screen.getByRole('switch', { name: 'Background Sync' })).toBeInTheDocument()
    })

    test('does not invent a generic name when the call site gives none', () => {
      render(<Switch />)
      expect(screen.queryByRole('switch', { name: 'Toggle' })).not.toBeInTheDocument()
    })
  })

  describe('AnalyticsCard', () => {
    test('renders AnalyticsCard with children', () => {
      render(
        <AnalyticsCard title="Test Title">
          <div data-testid="test-child">Child Content</div>
        </AnalyticsCard>,
      )
      expect(screen.getByText('Test Title')).toBeInTheDocument()
      expect(screen.getByTestId('test-child')).toBeInTheDocument()
      expect(screen.getByText('Child Content')).toBeInTheDocument()
    })

    test('isLoading=true shows Loader2 spinner overlay', () => {
      const { container } = render(
        <AnalyticsCard title="Loading Card" isLoading>
          <div>placeholder</div>
        </AnalyticsCard>,
      )
      expect(screen.getByText(/loading data/i)).toBeInTheDocument()
      const spinner = container.querySelector('svg.animate-spin')
      expect(spinner).toBeInTheDocument()
    })

    test('isFetching && !isLoading applies opacity-40 to child container', () => {
      const { container } = render(
        <AnalyticsCard title="Refetching Card" isFetching>
          <div data-testid="child">stale data</div>
        </AnalyticsCard>,
      )
      expect(screen.queryByText(/loading data/i)).not.toBeInTheDocument()
      const dimmed = container.querySelector('.opacity-40')
      expect(dimmed).toBeInTheDocument()
      expect(dimmed).toContainElement(screen.getByTestId('child'))
    })

    test('icon prop renders icon node in the header', () => {
      render(
        <AnalyticsCard title="Iconed" icon={<span data-testid="card-icon">ICON</span>}>
          <div>body</div>
        </AnalyticsCard>,
      )
      expect(screen.getByTestId('card-icon')).toBeInTheDocument()
    })

    test('headerAction prop renders action node in the header', () => {
      render(
        <AnalyticsCard
          title="With Action"
          headerAction={<button data-testid="header-action">Do Thing</button>}
        >
          <div>body</div>
        </AnalyticsCard>,
      )
      expect(screen.getByTestId('header-action')).toBeInTheDocument()
    })

    test('helpContent shows HelpCircle button that opens HelpDialog on click', async () => {
      const user = userEvent.setup()
      render(
        <AnalyticsCard
          title="Helpful"
          helpContent={<p data-testid="help-body">Here is help</p>}
        >
          <div>body</div>
        </AnalyticsCard>,
      )
      const helpButton = screen.getByRole('button', { name: /about this chart/i })
      expect(helpButton).toBeInTheDocument()
      expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()

      await user.click(helpButton)

      const dialog = await screen.findByRole('dialog')
      expect(dialog).toBeInTheDocument()
      expect(screen.getByTestId('help-body')).toBeInTheDocument()

      await user.keyboard('{Escape}')
      expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()
    })

    test('footer prop renders footer node beneath the content', () => {
      render(
        <AnalyticsCard title="With Footer" footer={<div data-testid="footer-node">Footer Here</div>}>
          <div>body</div>
        </AnalyticsCard>,
      )
      expect(screen.getByTestId('footer-node')).toBeInTheDocument()
      expect(screen.getByText('Footer Here')).toBeInTheDocument()
    })

    test('description prop renders description text under the title', () => {
      render(
        <AnalyticsCard title="With Desc" description="A helpful description">
          <div>body</div>
        </AnalyticsCard>,
      )
      expect(screen.getByText('A helpful description')).toBeInTheDocument()
    })

    test('default props render no spinner, no opacity dim, no help button', () => {
      const { container } = render(
        <AnalyticsCard title="Bare">
          <div>body</div>
        </AnalyticsCard>,
      )
      expect(screen.queryByText(/loading data/i)).not.toBeInTheDocument()
      expect(container.querySelector('svg.animate-spin')).not.toBeInTheDocument()
      expect(container.querySelector('.opacity-40')).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /about this chart/i })).not.toBeInTheDocument()
    })
  })

  describe('a11y: AnalyticsCard', () => {
    it('has no detectable WCAG violations in its happy-path render', async () => {
      const { container } = render(
        <AnalyticsCard title="Total requests" helpContent={<p>How we count.</p>}>
          <div>
            <p>1,234,567</p>
            <p>over the last hour</p>
          </div>
        </AnalyticsCard>,
      )
      const results = await axe(container)
      expect(results).toHaveNoViolations()
    })

    it('has no violations while showing the loading overlay', async () => {
      const { container } = render(
        <AnalyticsCard title="Slow Card" isLoading isFetching>
          <p>placeholder</p>
        </AnalyticsCard>,
      )
      const results = await axe(container)
      expect(results).toHaveNoViolations()
    })
  })

  describe('a11y: basic form patterns', () => {
    it('a labelled input has no violations', async () => {
      const { container } = render(
        <div>
          <label htmlFor="search">Search</label>
          <input id="search" type="search" placeholder="Type to filter…" />
        </div>,
      )
      const results = await axe(container)
      expect(results).toHaveNoViolations()
    })
  })
})

const API_BASE = 'http://127.0.0.1:8000'

function errorPathsWrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  return makeQueryWrapper(qc)
}

describe('extractApiError & openapi-fetch error paths', () => {
  beforeEach(() => {
    resetDefaultStores()
    useRealApiClient = true
    vi.clearAllMocks()
  })

  afterEach(() => {
    useRealApiClient = false
  })

  describe('extractApiError — unit coverage of error shapes', () => {
    it('returns the string for {detail: "..."}', () => {
      expect(apiLib.extractApiError({ detail: 'bucket not found' })).toBe('bucket not found')
    })

    it('joins FastAPI 422 array shape {detail: [{loc, msg}]}', () => {
      const result = apiLib.extractApiError({
        detail: [
          { loc: ['body', 'name'], msg: 'String must not be empty' },
          { loc: ['body', 'region'], msg: 'String must match pattern' },
        ],
      })
      expect(result).toBe('name: String must not be empty, region: String must match pattern')
    })

    it('joins {detail: {errors: [...]}}', () => {
      expect(apiLib.extractApiError({ detail: { errors: ['e1', 'e2'] } })).toBe('e1, e2')
    })

    it('returns {error: "..."} from nested detail', () => {
      expect(apiLib.extractApiError({ detail: { error: 'Token expired' } })).toBe('Token expired')
    })

    it('handles the no-detail fallback {error: "..."}', () => {
      expect(apiLib.extractApiError({ error: 'Direct error message' })).toBe('Direct error message')
    })

    it('returns "Unknown error" for falsy input', () => {
      expect(apiLib.extractApiError(null)).toBe('Unknown error')
      expect(apiLib.extractApiError(undefined)).toBe('Unknown error')
    })

    it('passes through a raw string', () => {
      expect(apiLib.extractApiError('plain message')).toBe('plain message')
    })
  })

  describe('openapi-fetch onResponse middleware — error path coverage', () => {
    it('throws extracted message on 500 with {detail: "..."}', async () => {
      server.use(
        http.get(`${API_BASE}/api/admin/system-jobs`, () =>
          HttpResponse.json({ detail: 'Background scheduler offline' }, { status: 500 }),
        ),
      )

      await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow(
        'Background scheduler offline',
      )
    })

    it('throws joined message on 422 with FastAPI array shape', async () => {
      server.use(
        http.get(`${API_BASE}/api/admin/system-jobs`, () =>
          HttpResponse.json(
            {
              detail: [{ loc: ['query', 'service_id'], msg: 'field required' }],
            },
            { status: 422 },
          ),
        ),
      )

      await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow(
        /service_id: field required/,
      )
    })

    it('throws a generic message when the error body is not JSON', async () => {
      server.use(
        http.get(`${API_BASE}/api/admin/system-jobs`, () =>
          new HttpResponse('not json — gateway timeout', { status: 504 }),
        ),
      )

      await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow()
    })
  })

  describe('useQuery isError flag — react-query error propagation', () => {
    it('isError === true when handler returns 401', async () => {
      server.use(
        http.get(`${API_BASE}/api/admin/system-jobs`, () =>
          HttpResponse.json({ detail: 'Unauthorized' }, { status: 401 }),
        ),
      )

      const { result } = renderHook(
        () =>
          useQuery({
            queryKey: ['system-jobs-401'],
            queryFn: async () => {
              const { data } = await apiLib.client.GET('/api/admin/system-jobs')
              return data
            },
          }),
        { wrapper: errorPathsWrapper() },
      )

      await waitFor(() => expect(result.current.isError).toBe(true))
      expect(result.current.error).toBeInstanceOf(Error)
      expect((result.current.error as Error).message).toMatch(/Unauthorized/i)
    })

    it('isError === false and data populated on 2xx success', async () => {
      server.use(
        http.get(`${API_BASE}/api/admin/system-jobs`, () =>
          HttpResponse.json({ jobs: [] }),
        ),
      )

      const { result } = renderHook(
        () =>
          useQuery({
            queryKey: ['system-jobs-ok'],
            queryFn: async () => {
              const { data } = await apiLib.client.GET('/api/admin/system-jobs')
              return data
            },
          }),
        { wrapper: errorPathsWrapper() },
      )

      await waitFor(() => expect(result.current.isSuccess).toBe(true))
      expect(result.current.isError).toBe(false)
      expect(result.current.data).toEqual({ jobs: [] })
    })
  })
})

const rejected = {
  METADATA_DSN: 'postgresql://example.invalid/operator',
  DUCKLAKE_CATALOG: 'postgres:example.invalid/operator',
  DUCKLAKE_DATA_PATH: 's3://operator-placeholder/',
  CELERY_BROKER_URL: 'redis://example.invalid/0',
  CELERY_RESULT_BACKEND: 'redis://example.invalid/1',
  CLICKHOUSE_PASSWORD: 'operator-placeholder',
  HOT_S3_SECRET: 'operator-placeholder',
  FASTLY_API_KEY: 'operator-placeholder',
  AWS_SECRET_ACCESS_KEY: 'operator-placeholder',
  REMOTE_SHARE_DB_DIR: '/operator-placeholder',
  OAUTH_PROVIDERS_CONFIG_PATH: '/operator-placeholder/oauth.json',
  OAUTH_FLOW_STATE_SECRET: 'operator-placeholder',
  OAUTH_GOOGLE_CLIENT_SECRET: 'operator-placeholder',
  UV_ENV_FILE: '/operator-placeholder/.env',
  UV_CONFIG_FILE: '/operator-placeholder/uv.toml',
  UV_PROJECT_ENVIRONMENT: '/operator-placeholder/venv',
  PYTHONPATH: '/operator-placeholder',
  PYTHONSTARTUP: '/operator-placeholder/start.py',
  NODE_OPTIONS: '--env-file=/operator-placeholder/.env',
  ENV: '/operator-placeholder/.env',
  BASH_ENV: '/operator-placeholder/.env',
  DOTENV_CONFIG_PATH: '/operator-placeholder/.env',
  HTTP_PROXY: 'http://example.invalid',
  UNKNOWN_DEPLOYMENT_SECRET: 'operator-placeholder',
}

describe('backend harness child isolation', () => {
  let spawnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    vi.resetModules()
    vi.clearAllMocks()
    for (const [key, value] of Object.entries(rejected)) vi.stubEnv(key, value)
    for (const key of ['FLA_DEV_NO_CRONS', 'FASTLY_MOCK_MODE']) vi.stubEnv(key, '0')
    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    vi.stubEnv('SSE_BACKPLANE', 'valkey')
    vi.stubEnv('OTEL_EXPORTER', 'otlp')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true }))
    vi.spyOn(console, 'log').mockImplementation(() => {})
    const child = Object.assign(new ChildProcess(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      kill: vi.fn(() => {
        queueMicrotask(() => child.emit('exit', 0))
        return true
      }),
    })
    spawnSpy = vi.spyOn(childProcess, 'spawn').mockReturnValue(child)
    vi.spyOn(childProcess, 'execFileSync').mockReturnValue(Buffer.from(''))
    vi.spyOn(fs, 'mkdtempSync').mockImplementation(((prefix: string) => `${prefix}sandbox`) as typeof fs.mkdtempSync)
    vi.spyOn(fs, 'mkdirSync').mockImplementation((() => undefined) as typeof fs.mkdirSync)
    vi.spyOn(fs, 'writeFileSync').mockImplementation((() => undefined) as typeof fs.writeFileSync)
    vi.spyOn(fs, 'rmSync').mockImplementation((() => undefined) as typeof fs.rmSync)
    syncBuiltinESMExports()
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
    syncBuiltinESMExports()
    Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_PROC')
    Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_SANDBOX')
  })

  it.each(['contract', 'e2e'])('%s rejects deployment env before spawning uv', async (harness) => {
    const contract = await import('../helpers/msw/handlers')
    try {
      if (harness === 'contract') {
        await contract.startBackend()
      } else {
        const { default: globalSetup } = await import('../../e2e/global-setup')
        await globalSetup()
      }
      expect(spawnSpy).toHaveBeenCalledOnce()
      const [command, args, options] = spawnSpy.mock.calls[0]
      expect(command).toBe('uv')
      expect(args).toContain('scripts/run_contract_backend.py')
      const env = options!.env!
      for (const [key, value] of Object.entries(rejected)) {
        expect(env[key], key).not.toBe(value)
      }
      expect(env).toMatchObject({
        PATH: process.env.PATH,
        DEBUG_RESPONSES: 'true',
        FASTLY_MOCK_MODE: '1',
        FLA_DEV_NO_CRONS: '1',
        DEPLOYMENT_MODE: 'standard',
        SSE_BACKPLANE: 'local',
        OTEL_EXPORTER: 'none',
        UV_NO_ENV_FILE: '1',
        UV_NO_CONFIG: '1',
        PYTHON_DOTENV_DISABLED: '1',
      })
      expect(env.CONTRACT_CONFIGS_DIR).toBe(join(env.CONTRACT_DATA_DIR!, '..', 'configs'))
      if (harness === 'e2e') {
        expect(env).toMatchObject({
          OAUTH_MOCK_IDP: '1',
          OAUTH_MOCK_IDP_ISSUER: 'http://127.0.0.1:18004/mock-idp',
          OAUTH_REDIRECT_BASE: 'http://127.0.0.1:13004',
          OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
          OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
        })
      } else {
        expect(Object.keys(env).filter((key) => key.startsWith('OAUTH_'))).toEqual([])
      }
      const child = spawnSync(process.execPath, ['-e', 'console.log(JSON.stringify(process.env))'], {
        env,
        encoding: 'utf8',
      })
      expect(child.status, child.stderr).toBe(0)
      expect(JSON.parse(child.stdout)).toMatchObject(env)
    } finally {
      await contract.stopBackend()
    }
  })
})
