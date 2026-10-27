import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'
import { act, fireEvent, render, renderHook, screen, waitFor, within } from '@testing-library/react'
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
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import DashboardClient, { buildChartLayout } from '@/app/dashboard/_sections/DashboardClient'
import AssetsClient, { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import NetworkPage from '@/app/network/page'
import QueryPage from '@/app/query/page'
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import OriginClient from '@/app/origin/_sections/OriginClient'
import FastlyValueClient, { IOValueTab } from '@/app/fastly-value/page'
import type { components } from '@/types/api.generated'
import { UsageLogClient } from '@/app/admin/usage-log/page'
import Loading from '@/app/admin/usage-log/loading'
import UsagePage from '@/app/usage/page'
import SessionsPage from '@/app/sessions/page'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { DEFAULTS, reducer, calculate, type PrefillResponse } from '@/components/CostCalculator/CostCalculator'
import HighScaleRequestFactsPage from '@/app/high-scale/request-facts/page'
import HighScaleRequestFactsLoading from '@/app/high-scale/request-facts/loading'
import { highScaleRequestFactsHooks } from '@/components/high-scale/HighScaleRequestFactsPanel'
import { CorrelatorLeaderboard, useTickHistory, type MetricsTick as HistoryMetricsTick } from '@/app/control-room/_sections/ControlRoomClient'
import type { SSELine } from '@/hooks/useSSE'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ReviewCard, ReviewHeader, ReviewContent, ReviewItem, SectionHeader, StatCard } from '@/components/ui/confirm-dialog'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { densifyBarSeries, buildTrafficData, getIntervalSeconds, _resetUrlHydrationFlag, hydrateFilterStoreFromUrl } from '@/lib/utils'
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import { server, CMCD_FIXTURE } from '@/__tests__/helpers/msw/handlers'
import {
  createTestQueryClient, makeQueryWrapper, renderWithQuery, getOk, postOk,
  spyOnConsoleError, stubResizeObserver, stubPointerCaptureAndScroll, stubGetAnimations,
  createUseSSEMock, partial, type ReportLayoutChildArgs, type InteractiveDataTableMockProps,
} from '../helpers/page-smoke'
import * as apiLib from '@/lib/api'
import { getApiBase, queryKeys } from '@/lib/api'
import RumPageClient, { RumFaroVersionCard, RumStatusPanel } from '@/app/rum/_sections/RumClient'
import { RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'
import SecurityClient, {
  BotsSection,
  HeaderAnomaliesSection,
  NetworkSection,
  ProxyWatchdogSection,
} from '@/app/security/_sections/SecurityClient'
import type { PlotlyChart } from '@/components/PlotlyChart'
import React, { type ComponentProps } from 'react'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

const { state: sseState, start: mockStart, resetState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
}))

vi.mock('../../playwright.config', () => ({ E2E_BACKEND_PORT: 18004, E2E_FRONTEND_PORT: 13004 }))
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('next-themes', () => ({ useTheme: vi.fn(() => ({ theme: 'light' })) }))

let mockPathname = '/dashboard'
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), prefetch: vi.fn(), back: vi.fn() }),
  useSearchParams: () => new URLSearchParams(window.location.search),
  usePathname: () => mockPathname,
}))

vi.mock('@uiw/react-codemirror', async () => {
  const R = await import('react')
  return {
    default: R.forwardRef(function MockCodeMirror(
      { value, onChange }: { value: string; onChange: (v: string) => void },
      ref: React.Ref<HTMLTextAreaElement>,
    ) {
      return <textarea ref={ref} data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
    }),
  }
})

let useRealApiClient = false
let useMockAdminFetch = false
const {
  mockGet, mockPost, mockDelete, useFactsMock,
  mockAdminFetch, realAdminFetchRef, plotlyMock, dataTableMock, securityActiveFields,
} = vi.hoisted(() => ({
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockGet: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockPost: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockDelete: vi.fn<(...args: any[]) => any>(),
  useFactsMock: vi.fn(),
  mockAdminFetch: vi.fn(),
  realAdminFetchRef: { current: null as ((...args: unknown[]) => Promise<Response>) | null },
  plotlyMock: vi.fn<(props: ComponentProps<typeof PlotlyChart>) => void>(),
  dataTableMock: vi.fn<(props: InteractiveDataTableMockProps) => void>(),
  securityActiveFields: new Set<string>(),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  realAdminFetchRef.current = actual.adminFetch as (...args: unknown[]) => Promise<Response>
  const wrap = (fn: unknown, mock: (...a: unknown[]) => unknown) =>
    vi.fn((...args: unknown[]) => (useRealApiClient ? (fn as (...a: unknown[]) => unknown)(...args) : mock(...args)))
  return {
    ...actual,
    adminFetch: (...args: unknown[]) => (useMockAdminFetch ? mockAdminFetch(...args) : realAdminFetchRef.current!(...args)),
    client: {
      GET: wrap(actual.client.GET, mockGet),
      POST: wrap(actual.client.POST, mockPost),
      DELETE: wrap(actual.client.DELETE, mockDelete),
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
let useInteractiveDataTableMock = false
let useMockAnalyticsCard = false
let useMockActiveLogFields = false
let useSecurityActiveFields = false
let mockIsDataReady: boolean | null = null
let useMockHighScaleBootstrap = false
let mockIsAnalyst: boolean | null = null

afterEach(() => { mockIsAnalyst = null })

vi.mock('@/hooks/useBootstrap', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useBootstrap')>()
  return {
    ...actual,
    useActiveLogFields: (...args: Parameters<typeof actual.useActiveLogFields>) =>
      useSecurityActiveFields
        ? {
            ready: true,
            isFieldActive: (id: string) => securityActiveFields.has(id),
            isGroupActive: () => false,
            activeFields: securityActiveFields,
          }
        : useMockActiveLogFields
          ? { ready: true, isFieldActive: vi.fn(() => true), isGroupActive: () => true, activeFields: new Set(['ottfb', 'pop', 'oip']) }
          : actual.useActiveLogFields(...args),
    useIsDataReady: (...args: Parameters<typeof actual.useIsDataReady>) =>
      mockIsDataReady !== null ? mockIsDataReady : actual.useIsDataReady(...args),
    useIsAnalyst: () => (mockIsAnalyst !== null ? mockIsAnalyst : actual.useIsAnalyst()),
    useEffectiveServiceId: (...args: Parameters<typeof actual.useEffectiveServiceId>) =>
      useMockHighScaleBootstrap ? 'service-1' : actual.useEffectiveServiceId(...args),
    useBootstrapResolved: (...args: Parameters<typeof actual.useBootstrapResolved>) =>
      useMockHighScaleBootstrap ? true : actual.useBootstrapResolved(...args),
  }
})

vi.mock('@/hooks/useDashboardCards', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useDashboardCards')>()
  return {
    ...actual,
    useTimeRange: (...args: Parameters<typeof actual.useTimeRange>) =>
      useMockHighScaleBootstrap
        ? { startTime: '2026-09-11T00:00:00Z', endTime: '2026-09-11T01:00:00Z' }
        : actual.useTimeRange(...args),
  }
})

vi.mock('@/components/CostCalculator/CostCalculator', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/CostCalculator/CostCalculator')>()),
  CostCalculator: () => <div data-testid="cost-calculator" />,
}))

function resetDefaultStores() {
  useRealApiClient = false
  useMockAdminFetch = false
  useInteractiveDataTableMock = false
  useMockActiveLogFields = false
  useSecurityActiveFields = false
  mockIsDataReady = null
  useMockHighScaleBootstrap = false
  mockPathname = '/dashboard'
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true, services: [{ id: 'test-svc', name: 'Test' }] })
  useFilterStore.getState().resetAll()
  useFilterStore.setState({
    startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z',
    filters: [], isAutoRange: false, compareMode: false,
  })
}

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const WrappedReportLayout = (props: any) =>
    !reportLayoutChildArgs ? (
      <actual.ReportLayout {...props} />
    ) : (
      <div>
        <h1>{props.title}</h1>
        <div data-testid="header-actions">{props.headerActions}</div>
        {props.children(reportLayoutChildArgs)}
      </div>
    )
  return { ...actual, default: WrappedReportLayout, ReportLayout: WrappedReportLayout }
})

vi.mock('@/components/DataTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/DataTable')>()
  const interactive = await (await import('../helpers/page-smoke')).interactiveDataTableModuleMock(dataTableMock)
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    DataTable: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.DataTable(props)
        : useMockDataTable
          ? <div data-testid="data-table" />
          : <actual.DataTable {...props} />,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    FilterValueCell: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.FilterValueCell(props)
        : useMockDataTable
          ? <span>{props.display ?? props.filters?.[0]?.value}</span>
          : <actual.FilterValueCell {...props} />,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ColumnVisibilityDropdown: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.ColumnVisibilityDropdown(props)
        : useMockDataTable
          ? null
          : <actual.ColumnVisibilityDropdown {...props} />,
  }
})

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock(plotlyMock))
vi.mock('@/components/AnalyticsCard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/AnalyticsCard')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    AnalyticsCard: (props: any) =>
      !useMockAnalyticsCard ? (
        <actual.AnalyticsCard {...props} />
      ) : (
        <div data-testid="analytics-card">
          <h3>{props.title}</h3>
          {props.error ? <div role="alert">card error</div> : null}
          {props.children}
        </div>
      ),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    LazyMount: (props: any) => (useMockAnalyticsCard ? <>{props.children}</> : <actual.LazyMount {...props} />),
    TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div>,
  }
})
vi.mock('@/components/Map/ChoroplethMap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Map/ChoroplethMap')>()),
  ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div>,
}))
vi.mock('@/components/Map/NetworkMap', () => ({ NetworkMap: () => <div data-testid="network-map" /> }))
vi.mock('@/components/Map/ShieldingMap', () => ({ ShieldingMap: () => <div data-testid="shielding-map" /> }))

const queryClient = createTestQueryClient({ queries: { retry: false, staleTime: 0 } })
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
    mockGet.mockImplementation(async (url: string) =>
      url.includes('/api/log-fields/catalog')
        ? getOk({ fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} })
        : getOk({}),
    )
    mockPost.mockResolvedValue(postOk({ summary: { total: 1234 }, time_series: [] }))
    const NuqsWrapper = withNuqsTestingAdapter()
    renderWithQuery(<NuqsWrapper><DashboardClient /></NuqsWrapper>, { queryClient })
    expect(screen.getByText('Dashboard')).toBeInTheDocument()
    await waitFor(() => expect(screen.getByText('Traffic over Time')).toBeInTheDocument())
  })

  const baseParams = {
    compareAggregates: null, compareMode: false, compareStartTime: null,
    startTime: null, endTime: null, trend: 'off', timezone: 'UTC',
    metric: 'requests', effectiveInterval: '1 hour',
    hiddenCategories: new Set<string>(), catalog: { fields: [] },
  }

  describe('densifyBarSeries & buildTrafficData gap-fill', () => {
    it('fills gaps on 1h grid (ISO and naive timestamps), stacked bars, and no-ops when invalid/contiguous', () => {
      for (const sparse of [
        [{ time: '2026-06-30T09:00:00Z', value: 5 }, { time: '2026-06-30T12:00:00Z', value: 8 }],
        [{ time: '2026-06-30 09:00:00', value: 5 }, { time: '2026-06-30 12:00:00', value: 8 }],
      ]) {
        expect(densifyBarSeries(sparse, 3600, false).map((d) => d.value)).toEqual([5, 0, 0, 8])
      }
      const sparse = [{ time: '2026-06-30T09:00:00Z', value: 5 }, { time: '2026-06-30T12:00:00Z', value: 8 }]
      expect(densifyBarSeries(sparse, undefined, false)).toBe(sparse)

      const contig = [{ time: '2026-06-30T09:00:00Z', value: 1 }, { time: '2026-06-30T10:00:00Z', value: 2 }, { time: '2026-06-30T11:00:00Z', value: 3 }]
      expect(densifyBarSeries(contig, 3600, false)).toBe(contig)

      const misaligned = [{ time: '2026-06-30T09:00:00Z', value: 5 }, { time: '2026-06-30T09:30:00Z', value: 7 }, { time: '2026-06-30T12:00:00Z', value: 8 }]
      expect(densifyBarSeries(misaligned, 3600, false)).toBe(misaligned)

      const cap = [{ time: '2026-06-30T09:00:00Z', value: 1 }, { time: '2026-06-30T11:00:01Z', value: 1 }]
      expect(densifyBarSeries(cap, 1, false)).toBe(cap)

      const stacked = densifyBarSeries([{ time: '2026-06-30T09:00:00Z', value: 2, category: '500' }, { time: '2026-06-30T11:00:00Z', value: 3, category: '502' }], 3600, true)
      expect(stacked).toHaveLength(6)
      expect(stacked.filter((d) => d.time === '2026-06-30T10:00:00.000Z').every((d) => d.value === 0)).toBe(true)
    })

    it('zero-fills bar series in buildTrafficData while leaving scatter series untouched', () => {
      const barTraces = buildTrafficData({
        ...baseParams,
        aggregates: { metric: 'requests', interval: '1 hour', time_series: [{ time: '2026-06-30T09:00:00Z', value: 5 }, { time: '2026-06-30T12:00:00Z', value: 8 }] },
      })
      expect(barTraces[0]).toMatchObject({ type: 'bar', y: [5, 0, 0, 8] })

      const scatterTraces = buildTrafficData({
        ...baseParams, metric: 'p95_latency',
        aggregates: { metric: 'p95_latency', interval: '1 hour', time_series: [{ time: '2026-06-30T09:00:00Z', value: 120 }, { time: '2026-06-30T12:00:00Z', value: 140 }] },
      })
      expect(scatterTraces[0]).toMatchObject({ type: 'scatter', y: [120, 140] })

      const fiveMin = buildTrafficData({
        ...baseParams,
        aggregates: { metric: 'requests', interval: '5 minutes', time_series: [{ time: '2026-06-30T09:00:00Z', value: 10 }, { time: '2026-06-30T09:15:00Z', value: 20 }] },
      })
      expect(fiveMin[0]).toMatchObject({ type: 'bar', y: [10, 0, 0, 20], width: 270000 })

      const subSec = buildTrafficData({
        ...baseParams,
        aggregates: { metric: 'requests', interval: '1 minute', time_series: [{ time: '2026-06-30 09:00:00.000', value: 3 }, { time: '2026-06-30 09:02:00.000', value: 7 }] },
      })
      expect(subSec[0]).toMatchObject({ type: 'bar', y: [3, 0, 7], width: 54000 })
    })
  })

  describe('getIntervalSeconds & buildChartLayout', () => {
    it.each([
      ['1 second', 1], ['1s', 1], ['1 seconds', 1],
      ['1 minute', 60], ['1m', 60], ['1 minutes', 60],
      ['5 minutes', 300], ['5m', 300], ['10m', 600],
      ['1 hour', 3600], ['1h', 3600], ['1 hours', 3600], ['2 hours', 7200],
      ['1 day', 86400], ['1d', 86400], ['1 days', 86400],
      [undefined, undefined], [null, undefined], ['', undefined], ['unknown', undefined],
    ] as const)('resolves %s -> %s', (input, want) => {
      expect(getIntervalSeconds(input)).toBe(want)
    })

    it('includes explicit bargap and bargroupgap in buildChartLayout', () => {
      const layout = buildChartLayout({
        trafficData: [{ type: 'bar' }], aggregates: { metric: 'requests' }, metric: 'requests',
        startTime: '2026-06-30T09:00:00Z', endTime: '2026-06-30T10:00:00Z', timezone: 'UTC', catalog: { fields: [] },
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
      startTime: '2026-08-19T12:00:00Z', endTime: '2026-08-19T13:00:00Z',
      activeServiceId: 'test-service-id', filterPayload: {}, intervalButtons: null, bucketSeconds: 3600,
    }
  })

  afterEach(() => { errorSpy.mockRestore() })

  const MOCK_ASSETS_DATA = {
    has_data: true,
    missing_fields: [],
    asset_type_breakdown: [
      { asset_type: 'Images', requests: 50000, egress_bytes: 250000000, cache_hit_ratio: 0.945, compression_rate: 0.85 },
      { asset_type: 'JavaScript/CSS', requests: 30000, egress_bytes: 60000000, cache_hit_ratio: 0.982, compression_rate: 0.92 },
    ],
    cache_performance: [
      { asset_type: 'Images', cache_status: 'HIT', requests: 47250, bytes: 236250000 },
      { asset_type: 'Images', cache_status: 'MISS', requests: 2750, bytes: 13750000 },
    ],
    compression_performance: [
      { asset_type: 'JavaScript/CSS', content_encoding: 'gzip', requests: 20000, bytes: 40000000 },
      { asset_type: 'JavaScript/CSS', content_encoding: 'br', requests: 7600, bytes: 15200000 },
    ],
    large_uncompressed_assets: [{ url: '/static/bundle-huge.js', requests: 1200, avg_bytes: 3750.0, total_bytes: 4500000, status: 200 }],
    low_ttl_assets: [{ url: '/feed.json', requests: 3500, avg_ttl: 60.0, asset_type: 'API/Dynamic' }],
  }

  test('renders zero-CLS loading skeletons, full breakdown data, drill-down links, missing fields banner, and empty state', async () => {
    mockPost.mockReturnValue(new Promise(() => {}))
    const { unmount } = renderWithQuery(<AssetsClient />, { queryClient })
    expect(screen.getByText('Assets & Shield')).toBeInTheDocument()
    expect(screen.getByTestId('assets-overview-grid').className).toContain('[contain-intrinsic-size:160px]')
    expect(screen.getByTestId('assets-cache-efficiency-card').className).toContain('[contain-intrinsic-size:420px]')
    expect(screen.getByTestId('assets-compression-card').className).toContain('[contain-intrinsic-size:420px]')
    expect(screen.getByText('Evaluating cache absorption efficiency...')).toBeInTheDocument()
    expect(screen.getByText('Evaluating payload compression efficiency...')).toBeInTheDocument()
    unmount()

    queryClient.clear()
    mockPost.mockResolvedValue({ data: { ...MOCK_ASSETS_DATA, missing_fields: ['resp_header_content_encoding', 'ttl'] } })
    const { unmount: unmount2 } = renderWithQuery(<AssetsClient />, { queryClient })
    expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
    expect(screen.getByText('/feed.json')).toBeInTheDocument()
    expect(within(screen.getByTestId('assets-overview-grid')).getByText('Images')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: '/static/bundle-huge.js' }).getAttribute('href')).toContain('filter_url=%2Fstatic%2Fbundle-huge.js')
    for (const name of ['Static Asset URL', 'Requests', 'Avg Size', 'Bandwidth', 'Type', 'Cache Misses', 'Avg TTL']) {
      expect(screen.getAllByRole('columnheader', { name }).length).toBeGreaterThan(0)
    }
    expect(screen.getByText('60.0s')).toBeInTheDocument()
    const banner = await screen.findByTestId('missing-asset-fields-banner')
    expect(within(banner).getByText(/resp_header_content_encoding, ttl/i)).toBeInTheDocument()
    expect(within(banner).getByRole('link', { name: /Configure Service Log Fields/i }).getAttribute('href')).toContain('/service-config')
    unmount2()

    queryClient.clear()
    mockPost.mockResolvedValue({
      data: {
        has_data: false, missing_fields: [], asset_type_breakdown: [],
        cache_performance: [], compression_performance: [], large_uncompressed_assets: [], low_ttl_assets: [],
      },
    })
    renderWithQuery(<AssetsClient />, { queryClient })
    await waitFor(() => expect(screen.getByText('No Asset Metrics Yet')).toBeInTheDocument())
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('refetches when custom range start changes and hits SSR-seeded cache entry on first paint', async () => {
    mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })
    const base = {
      endTime: '2026-08-19T13:00:00Z', timezone: 'UTC', activeServiceId: 'test-service-id',
      filterPayload: {}, config: {}, relativeRange: null, isAutoRange: false, anchor: '2026-08-19T13:00:00Z',
    }
    const { rerender, unmount } = renderWithQuery(<AssetsReportContent {...base} startTime="2026-08-19T12:00:00Z" />, { queryClient })
    await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(1))
    rerender(<AssetsReportContent {...base} startTime="2026-08-19T11:00:00Z" />)
    await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(2))
    expect(mockPost.mock.calls[1][1].body.start_time).toBe('2026-08-19T11:00:00Z')
    unmount()

    const { resolveRangeWire, quantizeAnchor } = await import('@/lib/utils')
    const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
    const { rangeKey } = resolveRangeWire({
      relativeRange: null, isAutoRange: false, startTime: '2026-08-19T12:00:00Z', endTime: '2026-08-19T13:00:00Z', anchor,
    })
    const seedClient = createTestQueryClient({ queries: { staleTime: 60_000 } })
    seedClient.setQueryData(['assets', 'aggregates', 'test-service-id', rangeKey, anchor, {}], MOCK_ASSETS_DATA)
    mockPost.mockReset()
    mockPost.mockReturnValue(new Promise(() => {}))

    renderWithQuery(
      <AssetsReportContent
        startTime="2026-08-19T12:00:00Z" endTime="2026-08-19T13:00:00Z" activeServiceId="test-service-id"
        filterPayload={{}} relativeRange={null} isAutoRange={false} anchor={anchor}
      />,
      { queryClient: seedClient },
    )
    expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
    expect(mockPost).not.toHaveBeenCalled()
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
      startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z',
      activeServiceId: 'test-svc', filterPayload: {}, intervalButtons: null, bucketSeconds: 3600,
    }
  })

  afterEach(() => { errorSpy.mockRestore() })

  test('renders unavailable state and preserves range-wire key shape + request body', async () => {
    mockPost.mockResolvedValue({ data: { available: false, reason: 'Network fields not enabled' } })
    mockGet.mockResolvedValue({ data: { data: [] } })
    const { resolveRangeWire, quantizeAnchor } = await import('@/lib/utils')

    renderWithQuery(<NetworkPage />, { queryClient })
    expect(screen.getByText('Network & ASN Health')).toBeInTheDocument()
    expect(await screen.findByText('Network Metrics Unavailable')).toBeInTheDocument()
    expect(screen.getByText('Network fields not enabled')).toBeInTheDocument()

    const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
    const { rangeKey } = resolveRangeWire({
      relativeRange: null, isAutoRange: false, startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', anchor,
    })
    const keys = queryClient.getQueryCache().getAll().map((q) => q.queryKey)
    const prefix = ['test-svc', rangeKey, anchor, {}]
    expect(keys).toContainEqual(['network', 'health', 'core', ...prefix, 3600, 'health_score'])
    expect(keys).toContainEqual(['network', 'health', 'map', ...prefix, 3600, 'all', 'health_score'])
    expect(keys).toContainEqual(['network', 'health', 'shielding', ...prefix])

    const coreCall = mockPost.mock.calls.find(
      ([url, opts]) => url === '/api/network-health' && (opts as { body: { sections: string[] } }).body.sections.length > 0,
    )
    expect(coreCall?.[1]).toMatchObject({
      body: { start_time: '2026-01-01T00:00:00Z', end_time: '2026-01-01T01:00:00Z', range_token: null, anchor },
    })
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('renders full dashboard with transport metrics, leaderboards, PoP heatmap, missing fields banner, and error surface', async () => {
    mockPost.mockImplementation(async (url: string) => {
      if (url.includes('/api/network/bundle')) {
        return {
          data: {
            transport: {
              tcp_rtt_p50_ms: 24.5, tcp_rtt_p90_ms: 45.2, tcp_rtt_p95_ms: 62.1, tcp_rtt_p99_ms: 98.4,
              retrans_rate_pct: 0.15, packet_loss_pct: 0.05, delivery_rate_mbps: 42.8,
              protocol_distribution: [
                { protocol: 'HTTP/3', share_pct: 65.4, requests: 6540 },
                { protocol: 'HTTP/2', share_pct: 30.1, requests: 3010 },
                { protocol: 'HTTP/1.1', share_pct: 4.5, requests: 450 },
              ],
              missing_fields: ['tcp_rtt', 'retrans', 'ploss'],
            },
          },
        }
      }
      if (url.includes('/api/network-health')) {
        return {
          data: {
            available: true,
            summary: {
              global_health_score: 94.2, avg_rtt_ms: 28.4,
              worst_asn: { label: 'Comcast (7922)', score: 78.5 },
              worst_country: { label: 'Australia (AU)', score: 81.2 },
            },
            leaderboard: [{
              asn: 7922, label: 'Comcast (7922)', total_reqs: 50000,
              health_score_now: 88.5, health_score_1h_ago: 87.0, p95_rtt_us: 45000, p99_rtt_us: 75000, trend: 'improving',
            }],
            has_metro: true,
            metro_leaderboard: [{ city: 'San Jose', raw_city: 'San Jose', region: 'CA', country: 'US', total_reqs: 25000, health_score: 95.0 }],
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
            countries: ['US'], scatter: [{ rtt_ms: 20, ttfb_ms: 45 }],
          },
        }
      }
      return { data: {} }
    })
    mockGet.mockResolvedValue({
      data: { data: [{ pop: 'SJC', requests: 50000, errors: 10, error_rate: 0.02, p50_rtt_us: 15000, p95_ttfb_ms: 45.0, cache_hit_rate: 92.5, bandwidth_bytes: 104857600 }] },
    })

    const { unmount } = renderWithQuery(<NetworkPage />, { queryClient })
    for (const txt of ['94.2/100', '28.4ms', 'Edge Transport & TCP Performance', '24.5', 'HTTP/3', 'ASN Performance Leaderboard', 'IMPROVING', 'San Jose', 'Fastly Edge PoP Health Heatmap', '1 Active PoPs', 'tcp_rtt, retrans, ploss']) {
      expect(await screen.findByText(txt)).toBeInTheDocument()
    }
    expect(await screen.findByTestId('missing-transport-fields-banner')).toBeInTheDocument()
    unmount()
  })

  test('UX-4: Network Quality section stays mounted with an error surface on a /network-quality 5xx', async () => {
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
    renderWithQuery(ui, { queryClient, wrapper: withNuqsTestingAdapter({ searchParams: window.location.search, hasMemory: true, onUrlUpdate }) })

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

  afterEach(() => { window.history.replaceState({}, '', '/') })

  test('renders engine initializing skeleton, schema sidebar, query execution, CSV export, and AST error alert', async () => {
    useServiceStore.setState({ activeServiceId: null, isInitialized: false })
    mockGet.mockImplementation(() => new Promise(() => {}))
    const { unmount } = renderWithNuqs(<QueryPage />)
    expect(screen.getByTestId('query-engine-initializing')).toBeInTheDocument()
    unmount()

    useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
    const user = userEvent.setup()
    mockGet.mockImplementation(async (url: string) =>
      url.includes('/api/schema') ? { data: { schema: [{ name: 'timestamp', type: 'TIMESTAMP' }, { name: 'client_ip', type: 'VARCHAR' }] } } : { data: { fields: [] } },
    )
    mockPost.mockResolvedValue({
      data: { columns: ['url', 'requests'], data: [{ url: '/index.html', requests: 42 }, { url: '/api/v1/items', requests: 19 }], total_rows: 2, elapsed_ms: 8 },
    })

    renderWithNuqs(<QueryPage />)
    expect(screen.getByTestId('query-page-root').className).toContain('contain-intrinsic-size')
    expect(screen.getByTestId('query-editor-container').className).toContain('contain-intrinsic-size')

    await user.click(screen.getByRole('button', { name: /run query/i }))
    await waitFor(() => expect(screen.getByTestId('query-results-table')).toBeInTheDocument())
    expect(screen.getByTestId('query-csv-export-btn')).toBeInTheDocument()
    expect(screen.getByText('/index.html')).toBeInTheDocument()

    await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))
    expect(await screen.findByTestId('query-schema-sidebar')).toBeInTheDocument()
    expect(screen.getByText('timestamp')).toBeInTheDocument()

    mockPost.mockResolvedValueOnce({
      error: JSON.stringify({ detail: { error: 'sql_not_permitted', message: 'only SELECT / WITH / UNION statements allowed (got DROP)' } }),
    })
    const editor = screen.getByTestId('mock-editor')
    await user.clear(editor)
    await user.type(editor, 'DROP TABLE logs')
    await user.click(screen.getByRole('button', { name: /run query/i }))
    expect((await screen.findByTestId('query-error-alert')).textContent).toMatch(/only SELECT \/ WITH \/ UNION statements allowed/i)
  })

  test('hydrates URL deep links (?q=, ?mode=raw, ?mode=bogus, ?filters=) and syncs filter store mutations', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    mockPost.mockResolvedValue({ data: { columns: [], data: [], total_rows: 0, elapsed_ms: 1 } })

    window.history.replaceState({}, '', '/query?q=' + encodeURIComponent('SELECT count(*) FROM logs'))
    const { unmount: u1 } = renderWithNuqs(<QueryPage />)
    expect(((await screen.findByTestId('mock-editor')) as HTMLTextAreaElement).value).toBe('SELECT count(*) FROM logs')
    u1()

    window.history.replaceState({}, '', '/query?mode=bogus')
    const { unmount: u2 } = renderWithNuqs(<QueryPage />)
    expect(await screen.findByRole('tab', { name: /edit raw sql/i })).toHaveAttribute('aria-selected', 'false')
    expect(screen.queryByTestId('mock-editor')).not.toBeInTheDocument()
    u2()

    const user = userEvent.setup()
    window.history.replaceState({}, '', '/query?start_time=2026-01-01T00:00:00Z&end_time=2026-01-01T01:00:00Z')
    const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
    const { unmount: u3 } = renderWithNuqs(<QueryPage />, onUrlUpdate)
    await user.click(await screen.findByRole('tab', { name: /edit raw sql/i }))
    await waitFor(() => {
      const last = onUrlUpdate.mock.calls.at(-1)?.[0]
      expect(last?.searchParams.get('mode')).toBe('raw')
      expect(last?.searchParams.get('start_time')).toBe('2026-01-01T00:00:00Z')
    })
    await user.click(screen.getByRole('tab', { name: /structured/i }))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)?.[0]?.searchParams.has('mode')).toBe(false))
    u3()

    const inbound = { status: { values: ['200'], mode: 'include' } }
    window.history.replaceState({}, '', `/query?filters=${encodeURIComponent(JSON.stringify(inbound))}`)
    _resetUrlHydrationFlag()
    hydrateFilterStoreFromUrl()
    renderWithNuqs(<QueryPage />)
    await waitFor(() => {
      expect(JSON.parse(new URLSearchParams(window.location.search).get('filters')!)).toMatchObject({ status: { values: ['200'], mode: 'include' } })
    })
    act(() => useFilterStore.getState().addFilter('country', 'US', 'include'))
    await waitFor(() => {
      expect(JSON.parse(new URLSearchParams(window.location.search).get('filters')!)).toMatchObject({
        status: { values: ['200'], mode: 'include' }, country: { values: ['US'], mode: 'include' },
      })
    })
  })

  test('dashboard "See Raw Logs" deep link (?start_time&end_time&filters) hydrates the filter store', async () => {
    mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
    const filters = { status_2: { values: ['500', '503'], mode: 'exclude' } }
    window.history.replaceState({}, '', `/query?start_time=2026-02-01T00:00:00Z&end_time=2026-02-01T06:00:00Z&filters=${encodeURIComponent(JSON.stringify(filters))}`)
    renderWithNuqs(<QueryPage />)
    await screen.findByText('Query Explorer')
    const state = useFilterStore.getState()
    expect(state.startTime).toBe('2026-02-01T00:00:00Z')
    expect(state.endTime).toBe('2026-02-01T06:00:00Z')
    expect(state.filters.map((f) => [f.column, f.value, f.mode])).toEqual([['status', '500', 'exclude'], ['status', '503', 'exclude']])
  })
})

describe('UI primitives & a11y', () => {
  beforeEach(() => { resetDefaultStores(); useMockAnalyticsCard = false })

  it('renders ReviewCard, SectionHeader, SkeletonGrid, StatCard, and Switch variants cleanly', () => {
    const { container: rc } = render(
      <ReviewCard>
        <ReviewHeader icon={Star}>Summary</ReviewHeader>
        <ReviewContent><span data-testid="child">body text</span></ReviewContent>
        <ReviewItem label="Service" value="cdn-prod" />
        <ReviewItem label="Status" value="active" variant="between" />
      </ReviewCard>,
    )
    expect(screen.getByText('Summary')).toBeInTheDocument()
    expect(screen.getByTestId('child')).toBeInTheDocument()
    expect(screen.getByText('cdn-prod')).toBeInTheDocument()
    expect(rc.querySelector('svg')).toBeInTheDocument()

    const { container: sh } = render(
      <SectionHeader title="Overview" icon={Star} className="custom-class"><span data-testid="action-slot"> · extra</span></SectionHeader>,
    )
    expect(screen.getByRole('heading', { name: /overview/i })).toBeInTheDocument()
    expect(screen.getByTestId('action-slot')).toBeInTheDocument()
    expect(sh.firstChild).toHaveClass('custom-class')

    expect(render(<SkeletonGrid count={4} />).container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(4)
    expect(render(<SkeletonGrid count={0} />).container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(0)
    expect((render(<SkeletonGrid count={1} height="200px" />).container.querySelector('[data-slot="skeleton"]') as HTMLElement).style.height).toBe('200px')

    const { container: sc } = render(<StatCard title="Requests" value="1,234" sub="last 24h" icon={Activity} tooltip="more info" />)
    expect(screen.getByText('1,234')).toBeInTheDocument()
    expect(sc.querySelector('svg')).toBeInTheDocument()
    const { container: scl } = render(<StatCard title="Loading" value="hidden-value" sub="..." loading />)
    expect(screen.queryByText('hidden-value')).not.toBeInTheDocument()
    expect(scl.querySelector('[data-slot="skeleton"]')).toBeInTheDocument()

    render(<><label htmlFor="edge-only">Edge-only logging</label><Switch id="edge-only" /><Switch aria-label="Background Sync" /><Switch /></>)
    expect(screen.getByRole('switch', { name: 'Edge-only logging' })).toBeInTheDocument()
    expect(screen.getByRole('switch', { name: 'Background Sync' })).toBeInTheDocument()
    expect(screen.queryByRole('switch', { name: 'Toggle' })).not.toBeInTheDocument()
  })

  test('AnalyticsCard renders states, help dialog, and passes axe a11y checks', async () => {
    const user = userEvent.setup()
    const { container, rerender } = render(
      <AnalyticsCard
        title="Helpful" description="A helpful description" icon={<span data-testid="card-icon">ICON</span>}
        headerAction={<button data-testid="header-action">Do Thing</button>}
        footer={<div data-testid="footer-node">Footer Here</div>} helpContent={<p data-testid="help-body">Here is help</p>}
      >
        <div data-testid="test-child">Child Content</div>
      </AnalyticsCard>,
    )
    expect(screen.getByText('Helpful')).toBeInTheDocument()
    expect(screen.getByText('A helpful description')).toBeInTheDocument()
    expect(screen.getByTestId('card-icon')).toBeInTheDocument()
    expect(screen.getByTestId('header-action')).toBeInTheDocument()
    expect(screen.getByTestId('footer-node')).toBeInTheDocument()
    expect(await axe(container)).toHaveNoViolations()

    await user.click(screen.getByRole('button', { name: /about this chart/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()
    expect(screen.getByTestId('help-body')).toBeInTheDocument()
    await user.keyboard('{Escape}')
    expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()

    rerender(<AnalyticsCard title="Loading Card" isLoading isFetching><p>placeholder</p></AnalyticsCard>)
    expect(screen.getByText(/loading data/i)).toBeInTheDocument()
    expect(container.querySelector('svg.animate-spin')).toBeInTheDocument()
    expect(await axe(container)).toHaveNoViolations()

    rerender(<AnalyticsCard title="Refetching Card" isFetching><div data-testid="child">stale data</div></AnalyticsCard>)
    expect(container.querySelector('.opacity-40')).toContainElement(screen.getByTestId('child'))

    const { container: formContainer } = render(<div><label htmlFor="search">Search</label><input id="search" type="search" placeholder="Type to filter…" /></div>)
    expect(await axe(formContainer)).toHaveNoViolations()
  })
})

const API_BASE = 'http://127.0.0.1:8000'

describe('extractApiError & openapi-fetch error paths', () => {
  beforeEach(() => { resetDefaultStores(); useRealApiClient = true; vi.clearAllMocks() })
  afterEach(() => { useRealApiClient = false })

  it('extracts error strings across all FastAPI / custom shapes', () => {
    expect(apiLib.extractApiError({ detail: 'bucket not found' })).toBe('bucket not found')
    expect(apiLib.extractApiError({ detail: [{ loc: ['body', 'name'], msg: 'String must not be empty' }, { loc: ['body', 'region'], msg: 'String must match pattern' }] })).toBe('name: String must not be empty, region: String must match pattern')
    expect(apiLib.extractApiError({ detail: { errors: ['e1', 'e2'] } })).toBe('e1, e2')
    expect(apiLib.extractApiError({ detail: { error: 'Token expired' } })).toBe('Token expired')
    expect(apiLib.extractApiError({ error: 'Direct error message' })).toBe('Direct error message')
    expect(apiLib.extractApiError(null)).toBe('Unknown error')
    expect(apiLib.extractApiError(undefined)).toBe('Unknown error')
    expect(apiLib.extractApiError('plain message')).toBe('plain message')
  })

  it('propagates 500, 422, 504, 401, and 200 responses through openapi-fetch and useQuery', async () => {
    server.use(http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ detail: 'Background scheduler offline' }, { status: 500 })))
    await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow('Background scheduler offline')

    server.use(http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ detail: [{ loc: ['query', 'service_id'], msg: 'field required' }] }, { status: 422 })))
    await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow(/service_id: field required/)

    server.use(http.get(`${API_BASE}/api/admin/system-jobs`, () => new HttpResponse('not json — gateway timeout', { status: 504 })))
    await expect(apiLib.client.GET('/api/admin/system-jobs')).rejects.toThrow()

    const wrapper = makeQueryWrapper(createTestQueryClient({ queries: { gcTime: 0 } }))
    server.use(http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ detail: 'Unauthorized' }, { status: 401 })))
    const { result: errRes } = renderHook(() => useQuery({ queryKey: ['system-jobs-401'], queryFn: async () => (await apiLib.client.GET('/api/admin/system-jobs')).data }), { wrapper })
    await waitFor(() => expect(errRes.current.isError).toBe(true))
    expect((errRes.current.error as Error).message).toMatch(/Unauthorized/i)

    server.use(http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ jobs: [] })))
    const { result: okRes } = renderHook(() => useQuery({ queryKey: ['system-jobs-ok'], queryFn: async () => (await apiLib.client.GET('/api/admin/system-jobs')).data }), { wrapper })
    await waitFor(() => expect(okRes.current.isSuccess).toBe(true))
    expect(okRes.current.data).toEqual({ jobs: [] })
  })
})

const rejected: Record<string, string> = {
  METADATA_DSN: 'postgresql://example.invalid/operator', DUCKLAKE_CATALOG: 'postgres:example.invalid/operator',
  DUCKLAKE_DATA_PATH: 's3://operator-placeholder/', CELERY_BROKER_URL: 'redis://example.invalid/0',
  CELERY_RESULT_BACKEND: 'redis://example.invalid/1', CLICKHOUSE_PASSWORD: 'operator-placeholder',
  HOT_S3_SECRET: 'operator-placeholder', FASTLY_API_KEY: 'operator-placeholder',
  AWS_SECRET_ACCESS_KEY: 'operator-placeholder', REMOTE_SHARE_DB_DIR: '/operator-placeholder',
  OAUTH_PROVIDERS_CONFIG_PATH: '/operator-placeholder/oauth.json', OAUTH_FLOW_STATE_SECRET: 'operator-placeholder',
  OAUTH_GOOGLE_CLIENT_SECRET: 'operator-placeholder', UV_ENV_FILE: '/operator-placeholder/.env',
  UV_CONFIG_FILE: '/operator-placeholder/uv.toml', UV_PROJECT_ENVIRONMENT: '/operator-placeholder/venv',
  PYTHONPATH: '/operator-placeholder', PYTHONSTARTUP: '/operator-placeholder/start.py',
  NODE_OPTIONS: '--env-file=/operator-placeholder/.env', ENV: '/operator-placeholder/.env',
  BASH_ENV: '/operator-placeholder/.env', DOTENV_CONFIG_PATH: '/operator-placeholder/.env',
  HTTP_PROXY: 'http://example.invalid', UNKNOWN_DEPLOYMENT_SECRET: 'operator-placeholder',
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
      stdout: new EventEmitter(), stderr: new EventEmitter(),
      kill: vi.fn(() => { queueMicrotask(() => child.emit('exit', 0)); return true }),
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
      if (harness === 'contract') await contract.startBackend()
      else await (await import('../../e2e/global-setup')).default()
      expect(spawnSpy).toHaveBeenCalledOnce()
      const [command, args, options] = spawnSpy.mock.calls[0]
      expect(command).toBe('uv')
      expect(args).toContain('scripts/run_contract_backend.py')
      const env = options!.env!
      for (const [key, value] of Object.entries(rejected)) expect(env[key], key).not.toBe(value)
      expect(env).toMatchObject({
        PATH: process.env.PATH, DEBUG_RESPONSES: 'true', FASTLY_MOCK_MODE: '1', FLA_DEV_NO_CRONS: '1',
        DEPLOYMENT_MODE: 'standard', SSE_BACKPLANE: 'local', OTEL_EXPORTER: 'none',
        UV_NO_ENV_FILE: '1', UV_NO_CONFIG: '1', PYTHON_DOTENV_DISABLED: '1',
      })
      expect(env.CONTRACT_CONFIGS_DIR).toBe(join(env.CONTRACT_DATA_DIR!, '..', 'configs'))
      if (harness === 'e2e') {
        expect(env).toMatchObject({
          OAUTH_MOCK_IDP: '1', OAUTH_MOCK_IDP_ISSUER: 'http://127.0.0.1:18004/mock-idp',
          OAUTH_REDIRECT_BASE: 'http://127.0.0.1:13004', OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
          OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
        })
      } else {
        expect(Object.keys(env).filter((key) => key.startsWith('OAUTH_'))).toEqual([])
      }
      const child = spawnSync(process.execPath, ['-e', 'console.log(JSON.stringify(process.env))'], { env, encoding: 'utf8' })
      expect(child.status, child.stderr).toBe(0)
      expect(JSON.parse(child.stdout)).toMatchObject(env)
    } finally {
      await contract.stopBackend()
    }
  })
})

describe('Performance, Fastly-Value & Origin smoke tests', () => {
  beforeEach(() => {
    resetDefaultStores()
    useMockAnalyticsCard = false
    useMockDataTable = true
    useMockActiveLogFields = true
    mockPathname = '/performance'
    reportLayoutChildArgs = {
      startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', activeServiceId: 'test-svc',
      filterPayload: {}, intervalButtons: null, bucketSeconds: 3600,
      config: { effectiveInterval: '1 day', interval: '1 day', validTrends: new Set(['off', 'auto']) },
      setChartInterval: vi.fn(), timezone: 'UTC',
    }
    queryClient.clear()
    mockPost.mockReset()
    mockPost.mockResolvedValue({ data: {} })
    errorSpy = spyOnConsoleError()
  })

  afterEach(() => {
    reportLayoutChildArgs = null
    useMockDataTable = false
    useMockActiveLogFields = false
    errorSpy.mockRestore()
  })

  test('performance page mounts and renders title and all 5 card titles with loaded data', async () => {
    mockPost.mockResolvedValue({
      data: {
        waterfall: { avg: { edge_processing: 10.5, origin_wait: 45.2, origin_download: 15.0, client_download: 8.3 } },
        top_urls: [{ url: '/api/v1/test', requests: 120, avg: 55.4, p50: 42.1, p95: 98.0, p99: 145.2 }],
        top_asns: [{ asn: 15169, label: 'AS15169 Google', requests: 350, avg: 38.2, p50: 32.0, p95: 65.4, p99: 89.1 }],
        ttl_dist: [{ bucket: '<1m', count: 45 }, { bucket: '<1h', count: 180 }],
        scatter: [{ origin: 45.2, edge: 12.1, cache: 'HIT' }],
        _approx: false,
      },
    })
    renderWithQuery(<PerformanceClient />, { queryClient })
    expect(screen.getByText('Performance')).toBeInTheDocument()
    expect(await screen.findByText('End-to-End Latency Waterfall (Average)')).toBeInTheDocument()
    for (const title of ['Slowest URLs', 'Slowest Networks', 'Cache TTL Distribution', 'Origin vs Edge Processing (ms)']) {
      expect(screen.getByText(title)).toBeInTheDocument()
    }
    expect(screen.queryByText('Approximate')).not.toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('performance page displays contextual loading messages while query is pending', () => {
    mockPost.mockReturnValue(new Promise(() => {}))
    renderWithQuery(<PerformanceClient />, { queryClient })
    for (const msg of [
      'Calculating latency percentiles...', 'Profiling endpoint latency...', 'Profiling network latency...',
      'Profiling cache TTL distribution...', 'Profiling origin vs edge latency...',
    ]) {
      expect(screen.getByText(msg)).toBeInTheDocument()
    }
  })

  test('performance page displays empty states and approximate badge appropriately', async () => {
    mockPost.mockResolvedValue({ data: { waterfall: {}, top_urls: [], top_asns: [], ttl_dist: [], scatter: [], _approx: true } })
    renderWithQuery(<PerformanceClient />, { queryClient })
    expect((await screen.findAllByText('No data for this filter')).length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('Approximate')).toBeInTheDocument()
  })

  class Boundary extends React.Component<{ children: React.ReactNode }, { error: Error | null }> {
    state = { error: null as Error | null }
    static getDerivedStateFromError(error: Error) { return { error } }
    render() {
      return this.state.error ? <div data-testid="route-crashed">{this.state.error.message}</div> : this.props.children
    }
  }

  test('UX-2: /performance survives a dirty scatter row with cache=null', async () => {
    mockPost.mockImplementation(async (_path: string, opts?: unknown) => {
      const sections: string[] = (opts as { body?: { sections?: string[] } })?.body?.sections ?? []
      return sections.includes('scatter')
        ? { data: { available: true, scatter: [{ cache: null, origin: 10, edge: 5 }], waterfall: { avg: { edge_processing: 1, origin_wait: 2 } }, top_urls: [], top_asns: [] } }
        : { data: { available: true, ttl_dist: [] } }
    })
    renderWithQuery(<Boundary><PerformanceClient /></Boundary>, { queryClient })
    await screen.findByText('End-to-End Latency Waterfall (Average)')
    expect(screen.queryByTestId('route-crashed')).toBeNull()
  })

  test('fastly-value page mounts and renders title and tab navigation', () => {
    renderWithQuery(<FastlyValueClient />, { queryClient })
    expect(screen.getByText('Service Summary')).toBeInTheDocument()
    for (const re of [/summary/i, /cdn & caching/i, /assets & shield/i, /security/i, /bot management/i, /performance/i, /network/i, /image optimizer/i]) {
      expect(screen.getByRole('tab', { name: re })).toBeInTheDocument()
    }
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('fastly-value summary query key keeps its pre-migration shape', async () => {
    const { resolveRangeWire, quantizeAnchor } = await import('@/lib/utils')
    renderWithQuery(<FastlyValueClient />, { queryClient })
    const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
    const { rangeKey } = resolveRangeWire({
      relativeRange: null, isAutoRange: false, startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', anchor,
    })
    const keys = queryClient.getQueryCache().getAll().map((q) => q.queryKey)
    expect(keys).toContainEqual(['value', 'summary', 'test-svc', rangeKey, anchor, {}, '1 day', 'summary'])
  })

  type IOData = components['schemas']['IOMetrics']
  const baseIOData = {
    io_time_series: [], format_distribution: [], io_bandwidth_time_series: [], io_compression_time_series: [],
    optimization_opportunities: [
      { url: '/img/hero.png', request_count: 1500, avg_kb: 812.34, total_bytes: 2_000_000 },
      { url: '/img/nokb.png', request_count: 3, avg_kb: null, total_bytes: 100 },
    ],
    io_format_conversion_pairs: [],
  } as unknown as IOData

  describe('IOValueTab tables', () => {
    it('upsell view lists the top image URLs', () => {
      render(<IOValueTab data={baseIOData} />)
      for (const name of ['URL', 'Requests', 'Avg KB', 'Total Bandwidth']) {
        expect(screen.getByRole('columnheader', { name })).toBeTruthy()
      }
      expect(screen.getByText('/img/hero.png')).toBeTruthy()
      expect(screen.getByText('812.3')).toBeTruthy()
      expect(screen.getAllByText('—').length).toBeGreaterThan(0)
    })

    it('enabled view lists format conversions', () => {
      const data = {
        ...baseIOData, io_transforms: 10,
        io_format_conversion_pairs: [{ input_format: 'png', output_format: 'webp', count: 2500, avg_ratio: 3.2 }],
      } as unknown as IOData
      render(<IOValueTab data={data} />)
      for (const name of ['From', 'To', 'Count', 'Avg Ratio']) {
        expect(screen.getByRole('columnheader', { name })).toBeTruthy()
      }
      for (const txt of ['png', 'webp', '3.2x']) expect(screen.getByText(txt)).toBeTruthy()
    })
  })

  const MOCK_ORIGIN_DATA = {
    has_data: true,
    summary: {
      total_misses: 450, total_passes: 50, ottfb_p50_ms: 45.2, ottfb_p75_ms: 60.1, ottfb_p95_ms: 120.5, ottfb_p99_ms: 250.0,
      ottlb_p50_ms: 65.0, ottlb_p95_ms: 160.0, cdn_overhead_p50_ms: 5.2, origin_error_rate: 0.005, obytes_p50: 1024, _approx: false,
    },
    timeseries: { has_data: true, series: [{ time: '2026-01-01T00:00:00Z', value: 45.2 }, { time: '2026-01-01T00:05:00Z', value: 48.1 }], _approx: false },
    slow_urls: { has_data: true, rows: [{ url: '/api/v1/slow', requests: 120, p50_ms: 150.0, p95_ms: 320.0, p99_ms: 500.0 }], _approx: false },
    status_codes: { has_data: true, rows: [{ status: 200, count: 480 }, { status: 500, count: 20 }] },
    path_breakdown: { has_data: true, shielding_detected: true, rows: [] },
    pop_latency: { has_data: true, rows: [{ pop: 'SJC', requests: 300, p50_ms: 40.0, p95_ms: 110.0, elevated: false }], _approx: false },
    ip_health: { has_data: true, rows: [{ oip: '203.0.113.1', requests: 500, p50_ms: 45.0, p95_ms: 120.0, error_pct: 0.5 }], _approx: false },
  }

  test('origin page mounts and renders title and all origin card titles with loaded data', async () => {
    mockPost.mockResolvedValue({ data: MOCK_ORIGIN_DATA })
    renderWithQuery(<OriginClient />, { queryClient })
    expect(screen.getByText('Origin Performance')).toBeInTheDocument()
    expect(await screen.findByText('Origin TTFB (P50)')).toBeInTheDocument()
    for (const title of [
      'Origin TTFB (P95)', 'Origin Error Rate', 'Fetch Volume', 'Origin Latency', 'Status Code Distribution',
      'Slowest URLs at Origin', 'Origin Performance by POP', 'Origin IP Health', 'Origin Payload Size',
    ]) {
      expect(screen.getByText(title)).toBeInTheDocument()
    }
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('origin page displays contextual loading messages while query is pending', () => {
    mockPost.mockReturnValue(new Promise(() => {}))
    renderWithQuery(<OriginClient />, { queryClient })
    for (const msg of [
      'Loading Origin TTFB (P50)...', 'Loading Origin TTFB (P95)...', 'Loading Origin Error Rate...',
      'Loading Fetch Volume...', 'Loading origin latency timeseries...', 'Loading origin status code breakdown...',
      'Loading slowest URLs at origin...', 'Loading origin performance by POP...', 'Loading origin IP health...',
      'Loading origin payload size...',
    ]) {
      expect(screen.getByText(msg)).toBeInTheDocument()
    }
  })

  test('origin page displays empty states when has_data is false', async () => {
    mockPost.mockResolvedValue({ data: { has_data: false, summary: { has_data: false } } })
    renderWithQuery(<OriginClient />, { queryClient })
    expect(await screen.findByText('No Origin Metrics Yet')).toBeInTheDocument()
  })
})

// ── Usage, Sessions, Streaming & CostCalculator tests ────────────────────────

function makePrefill(overrides: Partial<PrefillResponse>): PrefillResponse {
  return {
    _is_cached: false, data_days: 7, commit_interval_mins: 5, sample_rate: 100, edge_only: false,
    compaction_enabled: true, delete_after: false, log_retention_days: 90, billing_zone: null, ...overrides,
  }
}

const SAMPLE_USAGE_LOG = {
  service_id: 'svc-alpha', total: 2,
  aggregate: {
    total_class_a: 1200, total_class_b: 45000, total_cdn_downloads: 18, total_cdn_bytes: 1073741824,
    total_fos_bytes: 5242880, estimated_cost_class_a: 0.006, estimated_cost_class_b: 0.045,
    estimated_cost_cdn: 0.12, estimated_cost_total: 0.171,
    class_a_breakdown: { PutObject: 800, ListObjectsV2: 400 }, class_b_breakdown: { GetObject: 45000 },
  },
  entries: [
    { id: 1, timestamp: '2026-06-01T12:00:00Z', operation_class: 'A', operation_type: 'PutObject', url: 's3://fos-bucket/lake/part-001.parquet', bytes: 2048, duration_ms: 14.2, function_name: 'commit_files', process_context: 'cron:commit:svc-alpha', status: 'OK', estimated_cost: 0.000005, count: 1 },
    { id: 2, timestamp: '2026-06-01T12:01:00Z', operation_class: 'B', operation_type: 'GetObject', url: 's3://fos-bucket/raw/request/batch.gz', bytes: 4096, duration_ms: 9.1, function_name: 'download_batch', process_context: 'cron:sync:svc-alpha', status: 'OK', estimated_cost: 0.000001, count: 1 },
  ],
}

const SAMPLE_ACCOUNTING = {
  by: 'hour', from_ts: '2026-05-31T12:00:00Z', to_ts: '2026-06-01T12:00:00Z',
  buckets: [{ ts: '2026-06-01T11:00:00Z', fastly_requests: 5000, our_rows: 5000, file_count: 12, gap: 0, gap_pct: 0 }],
  totals: { fastly_requests: 5000, our_rows: 5000, gap: 0, gap_pct: 0, worst_bucket_ts: null, worst_bucket_gap_pct: null },
  sustained_loss: null,
  catchup: { latest_ingest_ts: '2026-06-01T12:00:00Z', lag_seconds: 12, status: 'caught_up' },
}

describe('Usage, Sessions, Streaming & CostCalculator', () => {
  beforeEach(() => {
    resetDefaultStores()
    useServiceStore.setState({
      activeServiceId: 'test-svc', isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test' }],
    })
    useMockAnalyticsCard = false
    useMockDataTable = true
    mockIsDataReady = true
    mockPathname = '/usage'
    reportLayoutChildArgs = {
      startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', activeServiceId: 'test-svc',
      filterPayload: {}, intervalButtons: null, bucketSeconds: 300,
      config: { effectiveInterval: '5 minutes', interval: '5 minutes' },
      setChartInterval: vi.fn(), timezone: 'UTC', data: { rows: [], total: 0 }, isLoading: false, isFetching: false,
    }
    queryClient.clear()
    mockGet.mockReset()
    mockPost.mockReset()
    mockDelete.mockReset()
    mockPost.mockResolvedValue({ data: { rows: [] } })
    errorSpy = spyOnConsoleError()
  })

  afterEach(() => {
    reportLayoutChildArgs = null
    useMockDataTable = false
    mockIsDataReady = null
    errorSpy.mockRestore()
  })

  test('usage page mounts and renders title', () => {
    renderWithQuery(<UsagePage />, { queryClient })
    expect(screen.getByText('System Usage')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('UX-6: storage card shows an error (not "0.00 GB-hrs") and the cost is flagged partial on a storage 5xx', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') throw new Error('storage boom')
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    await waitFor(() => expect(screen.getByText('Failed to load storage usage.')).toBeInTheDocument())
    expect(screen.queryByText(/GB-hrs/)).toBeNull()
    expect(screen.getByText(/Partial estimate — storage cost excluded\./)).toBeInTheDocument()
  })

  test('sessions page mounts and renders title', () => {
    renderWithQuery(<SessionsPage />, { queryClient })
    expect(screen.getByText('User Sessions')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('requests /api/cmcd/aggregates with the page window and renders the headline cards', async () => {
    mockPost.mockResolvedValue({ data: CMCD_FIXTURE } as never)
    renderWithQuery(<StreamingClient />, { queryClient })
    expect(await screen.findByText('Total Sessions')).toBeInTheDocument()
    expect(screen.getByText('Peak Viewers')).toBeInTheDocument()
    expect(screen.getByText('Rebuffer Rate')).toBeInTheDocument()
    await waitFor(() => expect(screen.getByText('1,234')).toBeInTheDocument())
    expect(screen.getByText('4.2%')).toBeInTheDocument()
    expect(mockPost).toHaveBeenCalledWith(
      '/api/cmcd/aggregates',
      expect.objectContaining({ body: expect.objectContaining({ bucket_seconds: 300, top_n: 30, filters: {} }) }),
    )
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('shows the CMCD-not-enabled state when the service has no CMCD data', async () => {
    mockPost.mockResolvedValue({ data: { available: false, has_data: false } } as never)
    renderWithQuery(<StreamingClient />, { queryClient })
    expect(await screen.findByText('CMCD data not available')).toBeInTheDocument()
    expect(screen.getByText(/CMCD collection is not enabled for this service/)).toBeInTheDocument()
    expect(screen.queryByText('Total Sessions')).not.toBeInTheDocument()
  })

  test('a failed query leaves the page up with em-dash values instead of crashing', async () => {
    mockPost.mockResolvedValue({ error: { detail: 'boom' } } as never)
    renderWithQuery(<StreamingClient />, { queryClient })
    expect(await screen.findByText('Total Sessions')).toBeInTheDocument()
    await waitFor(() => expect(mockPost).toHaveBeenCalled())
    expect(screen.getAllByText('—').length).toBeGreaterThan(0)
  })

  describe('CostCalculator calc logic with RUM', () => {
    test('returns correct calculations for DEFAULTS', () => {
      const results = calculate(DEFAULTS)
      expect(results.totalCost).toBeGreaterThan(0)
      expect(results.rumBeaconsMonth).toBe(0)
      expect(results.rumParquetGBMonths).toBe(0)
      expect(results.costRum).toBe(0)
    })

    test('correctly calculates RUM beacon costs and storage when rumBeaconsDay is set', () => {
      const results = calculate({ ...DEFAULTS, rumBeaconsDay: 10000 })
      expect(results.rumBeaconsMonth).toBe(300000)
      expect(results.rumParquetGBMonths).toBeCloseTo(0.071525, 4)
      expect(results.costRum).toBeCloseTo(1.50143, 4)
      const rumTier = results.storageTiers.find(t => t.label === 'RUM data')
      expect(rumTier).toBeDefined()
      expect(rumTier?.gbMonths).toBeCloseTo(0.071525, 4)
    })

    test('High-Scale adds the archive Parquet storage tier and its write and verification requests', () => {
      const standard = calculate(DEFAULTS)
      const highScale = calculate({ ...DEFAULTS, deploymentMode: 'high_scale' })
      expect(standard.storageTiers.find(t => t.label === 'High-Scale archive')).toBeUndefined()
      const archive = highScale.storageTiers.find(t => t.label === 'High-Scale archive')
      expect(archive?.gbMonths).toBeGreaterThan(0)
      expect(highScale.totalGBStored).toBeCloseTo(standard.totalGBStored + (archive?.gbMonths ?? 0), 6)
      const pagesPerMonth = Math.ceil(((86400 / DEFAULTS.logPeriod) * DEFAULTS.logNodes) / DEFAULTS.archiveBatchObjects) * 30
      expect(highScale.classAPerMonth - standard.classAPerMonth).toBeCloseTo(pagesPerMonth * 3, 6)
      expect(highScale.classBPerMonth - standard.classBPerMonth).toBeCloseTo(pagesPerMonth * 3, 6)
      expect(calculate({ ...DEFAULTS, deploymentMode: 'high_scale', archiveBatchObjects: 500 }).classAPerMonth)
        .toBeLessThan(highScale.classAPerMonth)
    })

    test('reducer PREFILL handles rum_beacons_per_day correctly', () => {
      const prefill = makePrefill({
        rum_beacons_per_day: 5000, requests_per_day: 1000000, edge_requests_per_day: 800000,
        log_period_seconds: 60, commit_interval_mins: 5, sample_rate: 100, edge_only: true,
        compaction_enabled: true, delete_after: true, log_retention_days: 90,
        class_a_rate_per_1k: 0.005, class_b_rate_per_10k: 0.01, cdn_egress_rate_per_gb: 0.12,
        storage_rate_per_gb_month: 0.02, min_billed_days: 30, avg_nodes_per_flush: 1,
      })
      expect(reducer(DEFAULTS, { type: 'PREFILL', prefill }).rumBeaconsDay).toBe(5000)
    })

    test('Billing Zones (MBZ100, MBZ200, Global) configure correct rates and calculate egress costs', () => {
      expect(DEFAULTS.billingZone).toBe('MBZ200')
      expect(DEFAULTS.rateEgress).toBe(0.12)
      const mbz100State = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'MBZ100' })
      expect(mbz100State.billingZone).toBe('MBZ100')
      expect(mbz100State.rateEgress).toBe(0.08)
      const globalState = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'Global' })
      expect(globalState.billingZone).toBe('Global')
      expect(globalState.rateEgress).toBe(0.18)

      const [resMBZ100, resMBZ200, resGlobal] = [calculate(mbz100State), calculate(DEFAULTS), calculate(globalState)]
      expect(resMBZ100.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
      expect(resGlobal.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
      expect(resMBZ100.costEgress).toBeCloseTo(resMBZ100.cdnEgressGB * 0.08, 4)
      expect(resMBZ200.costEgress).toBeCloseTo(resMBZ200.cdnEgressGB * 0.12, 4)
      expect(resGlobal.costEgress).toBeCloseTo(resGlobal.cdnEgressGB * 0.18, 4)
      expect(resMBZ100.costEgress).toBeLessThan(resMBZ200.costEgress)
      expect(resMBZ200.costEgress).toBeLessThan(resGlobal.costEgress)
    })

    test('PREFILL respects billing_zone and maps corresponding rate', () => {
      const state = reducer(DEFAULTS, { type: 'PREFILL', prefill: makePrefill({ billing_zone: 'MBZ100', cdn_egress_rate_per_gb: 0.08 }) })
      expect(state.billingZone).toBe('MBZ100')
      expect(state.rateEgress).toBe(0.08)
    })
  })

  describe('/admin/usage-log Page & Client', () => {
    beforeEach(() => {
      useMockDataTable = false
      useServiceStore.setState({ activeServiceId: null })
      vi.clearAllMocks()
    })

    it('renders loading.tsx with geometry-matched zero-CLS reservation containers', () => {
      render(<Loading />)
      expect(screen.getByTestId('admin-usage-log-loading')).toBeInTheDocument()
      for (const [tid, size] of [
        ['admin-usage-log-toolbar', 'auto_36px'],
        ['admin-usage-log-kpi-reservation', 'auto_164px'],
        ['admin-usage-log-chart-reservation', 'auto_380px'],
        ['admin-usage-log-table-reservation', 'auto_520px'],
      ]) {
        const el = screen.getByTestId(tid)
        expect(el.className).toContain(`[contain-intrinsic-size:${size}]`)
        expect(el.className).toContain('[content-visibility:auto]')
      }
      expect(screen.getByText('Loading FOS usage ledger...')).toBeInTheDocument()
    })

    it('renders pending skeleton reservations before usage-log data resolves', () => {
      mockGet.mockImplementation(() => new Promise(() => {}))
      renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)
      expect(screen.getByTestId('admin-usage-log-root')).toBeInTheDocument()
      expect(screen.getByTestId('admin-usage-log-kpi-reservation').className).toContain('[contain-intrinsic-size:auto_164px]')
      expect(screen.getByTestId('admin-usage-log-chart-reservation').className).toContain('[contain-intrinsic-size:auto_380px]')
      expect(screen.getByTestId('admin-usage-log-table-reservation').className).toContain('[contain-intrinsic-size:auto_520px]')
      expect(screen.getByText('Loading log accounting…')).toBeInTheDocument()
    })

    it('renders aggregate KPIs, ingest accounting, and usage log rows', async () => {
      mockGet.mockImplementation((url: string) =>
        Promise.resolve({
          data: url === '/api/admin/usage-log' ? SAMPLE_USAGE_LOG : url === '/api/admin/log-accounting' ? SAMPLE_ACCOUNTING : {},
          response: new Response(null, { status: 200 }),
        }),
      )
      renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)
      await waitFor(() => expect(screen.getByText('1.2K')).toBeInTheDocument())
      expect(screen.getByText('45.0K')).toBeInTheDocument()
      expect(screen.getByText('$0.17')).toBeInTheDocument()
      expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument()
      expect(screen.getByText('cron:commit:svc-alpha')).toBeInTheDocument()
      expect(screen.getByText('Caught up')).toBeInTheDocument()
    })

    it('switches time-window presets, filters by process/operation, refreshes, and purges logs', async () => {
      mockGet.mockImplementation((url: string) =>
        Promise.resolve({
          data: url === '/api/admin/usage-log' ? SAMPLE_USAGE_LOG : SAMPLE_ACCOUNTING,
          response: new Response(null, { status: 200 }),
        }),
      )
      mockDelete.mockResolvedValue({ data: { ok: true }, response: new Response(null, { status: 200 }) })
      renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)
      await waitFor(() => expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument())

      expect(screen.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute('aria-pressed', 'true')
      fireEvent.click(screen.getByTestId('admin-usage-log-preset-1h'))
      expect(screen.getByTestId('admin-usage-log-preset-1h')).toHaveAttribute('aria-pressed', 'true')

      fireEvent.change(screen.getByTestId('admin-usage-log-process-input'), { target: { value: 'cron:commit' } })
      await waitFor(() =>
        expect(mockGet).toHaveBeenCalledWith(
          '/api/admin/usage-log',
          expect.objectContaining({ params: { query: expect.objectContaining({ process_context: 'cron:commit' }) } }),
        ),
      )

      fireEvent.change(screen.getByTestId('admin-usage-log-operation-input'), { target: { value: 'PutObject' } })
      await waitFor(() =>
        expect(mockGet).toHaveBeenCalledWith(
          '/api/admin/usage-log',
          expect.objectContaining({ params: { query: expect.objectContaining({ operation_type: 'PutObject' }) } }),
        ),
      )

      const callsBeforeRefresh = mockGet.mock.calls.length
      fireEvent.click(screen.getByTestId('admin-usage-log-refresh-btn'))
      await waitFor(() => expect(mockGet.mock.calls.length).toBeGreaterThan(callsBeforeRefresh))

      const exportBtn = screen.getByTestId('admin-usage-log-export-btn')
      expect(exportBtn.getAttribute('href')).toContain('/api/admin/usage-log/export?')
      expect(exportBtn.getAttribute('href')).toContain('process_context=cron%3Acommit')

      fireEvent.click(screen.getByTestId('admin-usage-log-purge-btn'))
      await waitFor(() => expect(screen.getByText('Purge all usage logs?')).toBeInTheDocument())
      fireEvent.click(screen.getByRole('button', { name: /Purge all logs/i }))
      await waitFor(() =>
        expect(mockDelete).toHaveBeenCalledWith(
          '/api/admin/usage-log',
          expect.objectContaining({ params: { query: { service_id: 'svc-alpha' } } }),
        ),
      )
    })
  })
})

// ── HighScaleRequestFactsPage & ControlRoomClient tests ─────────────────────

const baseMetadata = {
  status: 'complete' as const, exact: true, coverage: 1, freshness_lag_seconds: 18, mode: 'clickhouse' as const,
  watermark: {
    service_id: 'service-1', domain: 'request', owner_epoch: 1,
    coverage_start: '2026-09-10T00:00:00Z', coverage_end: '2026-09-11T01:00:00Z',
    last_accepted_cursor: null, last_archived_event_id: null, last_visible_event_id: null, exact: true,
  },
  approximation_error: null, error: null,
}

const mockFactsState = (overrides: Record<string, unknown> = {}) => ({
  isLoading: false, isError: false, data: undefined, error: null, isFetching: false,
  pageIndex: 0, canNextPage: false, canPreviousPage: false, nextPage: vi.fn(), previousPage: vi.fn(),
  ...overrides,
})

describe('HighScaleRequestFactsPage (/high-scale/request-facts)', () => {
  beforeEach(() => {
    resetDefaultStores()
    useMockHighScaleBootstrap = true
    useFactsMock.mockReset()
    vi.spyOn(highScaleRequestFactsHooks, 'useHighScaleRequestFacts').mockImplementation(useFactsMock)
  })

  afterEach(() => {
    useMockHighScaleBootstrap = false
    vi.restoreAllMocks()
  })

  it('renders route loading skeleton with zero-CLS layout reservations', () => {
    const { container } = render(<HighScaleRequestFactsLoading />)
    expect(screen.getByText('High-scale request facts')).toBeInTheDocument()
    expect(container.querySelector('[class*="contain-intrinsic-size"]')).toBeTruthy()
  })

  it('renders contextual distributed cluster shard skeleton while loading', () => {
    useFactsMock.mockReturnValue(mockFactsState({ isLoading: true, isFetching: true }))
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByText('Loading request facts…')).toBeInTheDocument()
    expect(screen.getByText('Querying distributed cluster shards...')).toBeInTheDocument()
    expect(screen.getByTestId('high-scale-loading-card').className).toContain('contain-intrinsic-size')
  })

  it('renders DuckDB Standard Mode Emulation badge and supports filter controls', () => {
    useFactsMock.mockReturnValue(
      mockFactsState({
        data: {
          rows: [{
            event_id: 'event-emul-1', timestamp: '2026-09-11T00:00:01Z', service_id: 'service-1',
            status: 200, host: 'www.example.com', pop: 'IAD', country: 'US', client_ip: '203.0.113.xxx', url: '/healthz',
          }],
          metadata: { ...baseMetadata, mode: 'duckdb_emulation' },
          next_cursor: null, _is_cached: false,
        },
      }),
    )
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByTestId('high-scale-mode-badge')).toHaveTextContent('DuckDB Standard Mode Emulation')
    expect(screen.getByText('/healthz')).toBeInTheDocument()
    expect(screen.getByText('IAD')).toBeInTheDocument()
    expect(screen.getByText('203.0.113.xxx')).toBeInTheDocument()

    for (const [tid, value] of [
      ['filter-status', '503'], ['filter-domain', 'api.example.com'], ['filter-pop', 'LHR'],
      ['filter-country', 'GB'], ['filter-batch-id', 'batch-9'],
    ]) {
      fireEvent.change(screen.getByTestId(tid), { target: { value } })
    }
    expect(useFactsMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        serviceId: 'service-1', status: 503, domain: 'api.example.com',
        pop: 'LHR', country: 'GB', batchId: 'batch-9', emulateStandard: true,
      }),
    )

    fireEvent.click(screen.getByTestId('filter-clear'))
    expect(useFactsMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ status: null, domain: null, pop: null, country: null, batchId: null }),
    )
  })

  it('applies dimmed opacity-40 transition during background refetch and renders empty state', () => {
    useFactsMock.mockReturnValue(
      mockFactsState({
        data: { rows: [], metadata: baseMetadata, next_cursor: null, _is_cached: false },
        isFetching: true,
      }),
    )
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByText('No request facts in this time range.')).toBeInTheDocument()
    expect(screen.getByTestId('high-scale-table-wrapper').className).toContain('opacity-40')
    expect(screen.getByTestId('high-scale-table-wrapper').className).toContain('pointer-events-none')
  })

  it('renders error alert and server metadata with column headers', () => {
    useFactsMock.mockReturnValue(mockFactsState({ isError: true, error: new Error('high-scale unavailable') }))
    const errView = render(<HighScaleRequestFactsPage />)
    expect(screen.getByRole('alert')).toHaveTextContent('high-scale unavailable')
    errView.unmount()

    useFactsMock.mockReturnValue(
      mockFactsState({
        data: {
          rows: [{ event_id: 'event-1', timestamp: '2026-09-11T00:00:01Z', service_id: 'service-1', country: 'US', client_ip: '1.2.3.xxx', url: '/first' }],
          metadata: { ...baseMetadata, freshness_lag_seconds: 42 },
          next_cursor: 'cursor-1', _is_cached: false,
        },
        canNextPage: true,
      }),
    )
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('complete')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('42s lag')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('100%')
    expect(screen.getByText('1.2.3.xxx')).toBeInTheDocument()
    expect(screen.queryByText('1.2.3.4')).not.toBeInTheDocument()
    for (const name of ['Timestamp', 'Status', 'Host', 'POP', 'Country', 'Client IP', 'URL']) {
      expect(screen.getByRole('columnheader', { name })).toBeInTheDocument()
    }
  })
})

describe('CorrelatorLeaderboard', () => {
  beforeEach(() => {
    resetDefaultStores()
    useRealApiClient = true
  })

  afterEach(() => {
    useRealApiClient = false
  })

  it('posts the dimension/limit and renders the leaderboard', async () => {
    let body: unknown = null
    server.use(
      http.post('*/api/services/svc-1/control-room/correlate', async ({ request }) => {
        body = await request.json()
        return HttpResponse.json({
          dimension: 'waf_sig', top: [{ value: 'sqli', count: 7 }], freshness: { latest_log_at: null, lag_seconds: 0 },
        })
      }),
    )
    render(<CorrelatorLeaderboard dimension="waf_sig" serviceId="svc-1" limit={5} />, {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })
    expect(await screen.findByText('sqli')).toBeInTheDocument()
    expect(body).toEqual({ dimension: 'waf_sig', window_minutes: 60, limit: 5 })
  })

  it('does not fetch without a service id', async () => {
    let calls = 0
    server.use(
      http.post('*/control-room/correlate', () => {
        calls++
        return HttpResponse.json({ dimension: 'waf_sig', top: [], freshness: { latest_log_at: null, lag_seconds: 0 } })
      }),
    )
    render(<CorrelatorLeaderboard dimension="waf_sig" serviceId={null} />, {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })
    await waitFor(() => expect(calls).toBe(0))
  })
})

function makeTick(rps: number): HistoryMetricsTick {
  return {
    event: 'metrics_tick', event_schema_version: 2, timestamp: new Date().toISOString(), status: 'ok',
    data: { requests_per_second: rps, error_rate: 0.01, cache_hit_ratio: 0.95, bandwidth_mbps: 1.5 },
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
    const { result } = renderHook(() => useTickHistory([makeTick(10), makeTick(20)]))
    expect(result.current.history).toHaveLength(60)
    expect(result.current.history[57].data.requests_per_second).toBe(0)
    expect(result.current.history[58].data.requests_per_second).toBe(10)
    expect(result.current.history[59].data.requests_per_second).toBe(20)
  })

  it('tracks prevTick as the tick before the current one', () => {
    const [tick1, tick2] = [makeTick(10), makeTick(20)]
    const { result } = renderHook(() => useTickHistory([tick1, tick2]))
    expect(result.current.prevTick).toBe(tick1)
  })

  it('caps history at 60 entries with no padding', () => {
    const { result } = renderHook(() => useTickHistory(Array.from({ length: 65 }, (_, i) => makeTick(i))))
    expect(result.current.history).toHaveLength(60)
    expect(result.current.history[0].data.requests_per_second).toBe(5)
    expect(result.current.history[59].data.requests_per_second).toBe(64)
  })

  it('series() extracts values from history including padding', () => {
    const { result } = renderHook(() => useTickHistory([makeTick(10), makeTick(20), makeTick(30)]))
    const rps = result.current.series((d) => d.requests_per_second)
    expect(rps).toHaveLength(60)
    expect(rps.slice(0, 57).every((v) => v === 0)).toBe(true)
    expect(rps.slice(57)).toEqual([10, 20, 30])
  })

  it('handles replay burst — all items appear right-aligned', () => {
    const { result } = renderHook(() => useTickHistory(Array.from({ length: 45 }, (_, i) => makeTick(i))))
    expect(result.current.history).toHaveLength(60)
    const rps = result.current.series((d) => d.requests_per_second)
    expect(rps.slice(0, 15).every((v) => v === 0)).toBe(true)
    expect(rps.slice(15)).toEqual(Array.from({ length: 45 }, (_, i) => i))
  })
})

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
  event: 'metrics_tick', event_schema_version: 2, timestamp: '2026-07-08T15:19:38.466551+00:00', status: 'ok',
  data: {
    requests_per_second: 42.5, error_rate: 0.01, cache_hit_ratio: 0.95, bandwidth_mbps: 1.5,
    total_requests: 42, total_hits: 40, total_miss: 2, total_pass: 0, total_errors: 0,
    status_breakdown: { status_2xx: 100 }, estimated_cost_usd: 0.001,
    origin_requests_per_second: 5.0, origin_bandwidth_mbps: 0.2, shield_requests: 10, shield_hit_ratio: 0.05,
    pass_requests: 0, synth_requests: 0, waf_blocked: 0, waf_logged: 0, waf_passed: 42,
    pop_count: 2, degraded_pops: [], origin_offload: 0.85, hit_latency_ms: 1.0, miss_latency_ms: 150.0, pass_latency_ms: 80.0,
    h2_pct: 60.0, h3_pct: 30.0, ddos_detect: 0, ddos_mitigate: 0,
    status_detail: { '200': 40, '304': 2 }, object_size_distribution: { '1k': 10, '10k': 20, '100k': 12 },
    origin_fetches: 3, origin_revalidations: 1, shield_hit_requests: 8, shield_miss_requests: 2,
    request_collapse_usable: 5, request_collapse_unusable: 1,
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
    const tick = parseMetricsTick({
      event: 'metrics_tick', status: 'rt_down',
      data: { requests_per_second: 0, error_rate: 0, cache_hit_ratio: 0, bandwidth_mbps: 0 },
    })
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
    const tick = parseMetricsTick({
      event: 'metrics_tick',
      data: { requests_per_second: 1, error_rate: 0, cache_hit_ratio: 0, bandwidth_mbps: 0 },
    })
    expect(tick!.event_schema_version).toBe(1)
  })
})

describe('RumClient, RumFaroVersionCard, RumSettingsDialog & RumStatusPanel', () => {
  beforeAll(() => {
    stubGetAnimations()
  })
  const SVC = 'svc-rum-client'
  const SVC_FARO = 'svc-faro-card'
  const SVC_PANEL = 'svc-rum-panel'
  const DEFAULT_DIALOG_PROPS = {
    serviceId: 'svc-1',
    open: true,
    availableVersions: ['1.9.0', '1.8.0', '1.7.0'],
    currentVersion: '1.8.0',
    latestVersion: '1.9.0',
  }

  beforeEach(() => {
    resetDefaultStores()
    useRealApiClient = true
    useMockAdminFetch = false
    useMockDataTable = false
    useMockAnalyticsCard = false
    reportLayoutChildArgs = null
  })

  afterEach(() => {
    server.resetHandlers()
  })

  function analytics(overrides: Record<string, unknown> = {}) {
    return {
      no_data: false,
      is_mock: false,
      beacon_count: 42,
      pageview_count: 30,
      interaction_count: 10,
      error_count: 2,
      vitals: {
        lcp: { p75: 2.1, distribution: { good: 80, needs_improvement: 15, poor: 5 } },
        cls: { p75: 0.05, distribution: { good: 90, needs_improvement: 8, poor: 2 } },
        inp: { p75: 150, distribution: { good: 85, needs_improvement: 10, poor: 5 } },
      },
      trends: { timestamps: [], lcp: [], cls: [], error_rate: [], pageviews: [], interactions: [], errors: [] },
      worst_pages: [],
      errors: [],
      environments: { browsers: {}, os: {}, devices: {} },
      ...overrides,
    }
  }

  function versions(overrides: Record<string, unknown> = {}) {
    return {
      available: ['1.9.0', '1.8.0'],
      current: '1.8.0',
      latest: '1.9.0',
      update_available: true,
      ...overrides,
    }
  }

  function mockRumEndpoints({
    versionsHandler,
    analyticsOverrides,
  }: { versionsHandler?: () => Response | Promise<Response>; analyticsOverrides?: Record<string, unknown> } = {}) {
    const apiBase = getApiBase()
    server.use(
      http.get('/api/services/:service_id/rum/status', () =>
        HttpResponse.json({ enabled: true, enabled_at: '2026-01-01T00:00:00Z' }),
      ),
      http.get('/api/services/:service_id/rum/analytics', () => HttpResponse.json(analytics(analyticsOverrides))),
      http.get('/api/services/:service_id/rum/live-events', () => HttpResponse.json([])),
      http.get(`${apiBase}/api/services/:service_id/rum/versions`, () =>
        versionsHandler ? versionsHandler() : HttpResponse.json(versions()),
      ),
    )
  }

  function renderClient() {
    const qc = createTestQueryClient()
    return render(
      <RumPageClient initialServiceId={SVC} />,
      { wrapper: makeQueryWrapper(qc) },
    )
  }

  describe('RumClient — Faro version card placement', () => {
    it('admin: renders the Faro version card alongside the vitals dashboard when RUM is enabled', async () => {
      useServiceStore.setState({
        activeServiceId: SVC,
        services: [{ id: SVC, name: 'Test Service' }],
        isInitialized: true,
      })
      mockRumEndpoints()

      renderClient()

      expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
      expect(await screen.findByText(/faro sdk:/i)).toBeInTheDocument()
      expect(await screen.findByText('1.8.0')).toBeInTheDocument()
      expect(screen.getByText('RUM Settings')).toBeInTheDocument()
      expect(screen.getByText('Installation Instructions')).toBeInTheDocument()
    })

    it('analyst: does not render the version card, and never calls /rum/versions', async () => {
      mockIsAnalyst = true
      useServiceStore.setState({
        activeServiceId: SVC,
        services: [{ id: SVC, name: 'Test Service' }],
        isInitialized: true,
      })
      let versionsCalled = false
      mockRumEndpoints({
        versionsHandler: () => {
          versionsCalled = true
          return HttpResponse.json(versions())
        },
      })

      renderClient()

      expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
      expect(screen.queryByText(/faro sdk:/i)).not.toBeInTheDocument()
      expect(screen.queryByText('Installation Instructions')).not.toBeInTheDocument()

      await new Promise((r) => setTimeout(r, 50))
      expect(versionsCalled).toBe(false)
    })

    it('a 503 from /rum/versions leaves the dashboard usable', async () => {
      useServiceStore.setState({
        activeServiceId: SVC,
        services: [{ id: SVC, name: 'Test Service' }],
        isInitialized: true,
      })
      mockRumEndpoints({
        versionsHandler: () => HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 }),
      })

      renderClient()

      expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
      await waitFor(() => {
        expect(screen.getByText(/versions offline/i)).toBeInTheDocument()
      })
      expect(screen.getByText('Cumulative Layout Shift (CLS)')).toBeInTheDocument()
      expect(screen.getByText('Interaction to Next Paint (INP)')).toBeInTheDocument()
    })
  })

  describe('RumClient — worst pages table', () => {
    it('renders worst pages through the shared table with formatted vitals', async () => {
      useServiceStore.setState({
        activeServiceId: SVC,
        services: [{ id: SVC, name: 'Test Service' }],
        isInitialized: true,
      })
      mockRumEndpoints({
        analyticsOverrides: {
          worst_pages: [
            { path: '/checkout', views: 12, avg_load_time: 3.456, lcp_p75: 4.2, cls_p75: 0.123 },
            { path: '/home', views: 3, avg_load_time: null, lcp_p75: null, cls_p75: null },
          ],
        },
      })

      renderClient()

      expect(await screen.findByText('/checkout')).toBeInTheDocument()
      for (const name of ['Path', 'Pageviews', 'Avg Load (s)', 'LCP p75', 'CLS p75']) {
        expect(screen.getByRole('columnheader', { name })).toBeInTheDocument()
      }
      expect(screen.getByText('3.5s')).toBeInTheDocument()
      expect(screen.getByText('4.20s')).toBeInTheDocument()
      expect(screen.getByText('0.12')).toBeInTheDocument()
      expect(screen.getByText('/home')).toBeInTheDocument()
    })
  })

  describe('RumClient — range wire', () => {
    it('sends range_token+anchor on the default window and keys the query [rum-analytics, svc, token, anchor, filters]', async () => {
      useServiceStore.setState({
        activeServiceId: SVC,
        services: [{ id: SVC, name: 'Test Service' }],
        isInitialized: true,
      })
      useFilterStore.getState().resetAll()
      mockRumEndpoints()
      let seen: URLSearchParams | null = null
      server.use(
        http.get('/api/services/:service_id/rum/analytics', ({ request }) => {
          seen = new URL(request.url).searchParams
          return HttpResponse.json(analytics())
        }),
      )

      const qc = createTestQueryClient()
      render(<RumPageClient initialServiceId={SVC} />, { wrapper: makeQueryWrapper(qc) })
      expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()

      const params = seen as URLSearchParams | null
      expect(params?.get('range_token')).toBe('24h')
      expect(params?.get('anchor')).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$/)

      const key = qc
        .getQueryCache()
        .getAll()
        .map((q) => q.queryKey as unknown[])
        .find((k) => k[0] === 'rum-analytics')
      expect(key).toEqual(['rum-analytics', SVC, '24h', params?.get('anchor'), {}])
    })
  })

  function renderCard(rumEnabled = true) {
    const qc = createTestQueryClient()
    server.use(
      http.get('/api/services/:service_id/rum/status', () =>
        HttpResponse.json({ enabled: rumEnabled }),
      ),
    )
    return render(<RumFaroVersionCard serviceId={SVC_FARO} />, {
      wrapper: makeQueryWrapper(qc),
    })
  }

  function faroVersions(overrides: Record<string, unknown> = {}) {
    return {
      available: ['1.9.0', '1.8.0', '1.7.0'],
      current: '1.8.0',
      latest: '1.9.0',
      update_available: true,
      ...overrides,
    }
  }

  describe('RumFaroVersionCard', () => {
    beforeEach(() => {
      sseState.status = 'idle'
      mockStart.mockClear()
      server.use(
        http.post('/api/services/:service_id/rum/settings', () =>
          HttpResponse.json({ ok: true }),
        ),
      )
    })

    it('renders the pinned and latest version', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json(faroVersions()),
        ),
      )
      renderCard()
      expect(await screen.findByText('1.8.0')).toBeInTheDocument()
    })

    it('surfaces the update-available affordance and enables Upgrade', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json(faroVersions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
        ),
      )
      renderCard()
      expect(await screen.findByText(/update available/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /rum settings/i })).toBeEnabled()
    })

    it('allows changing version/re-deploying when already current', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json(faroVersions({ current: '1.9.0', latest: '1.9.0', update_available: false })),
        ),
      )
      renderCard()
      expect(await screen.findByRole('button', { name: /rum settings/i })).toBeEnabled()
    })

    it('degrades gracefully on a registry 503 without breaking the page', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 }),
        ),
      )
      renderCard()
      expect(await screen.findByText(/versions offline/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /retry/i })).toBeEnabled()
    })

    it('shows an empty-list state distinct from an error', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json({ available: [], current: null, latest: null, update_available: false }),
        ),
      )
      renderCard()
      expect(await screen.findByText(/not pinned/i)).toBeInTheDocument()
      expect(screen.queryByText(/versions offline/i)).not.toBeInTheDocument()
    })

    it('skips the fetch entirely and does not invite an upgrade when RUM is not enabled', async () => {
      let called = false
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () => {
          called = true
          return HttpResponse.json(faroVersions())
        }),
      )
      renderCard(false)
      expect(screen.queryByText(/faro sdk/i)).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /upgrade/i })).not.toBeInTheDocument()
      expect(called).toBe(false)
    })

    it('opens the upgrade dialog defaulting the target to latest (Confirm enabled, not a no-op)', async () => {
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () =>
          HttpResponse.json(faroVersions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
        ),
      )
      const user = userEvent.setup()
      renderCard()
      await user.click(await screen.findByRole('button', { name: /rum settings/i }))
      expect(await screen.findByText(/rum tracking settings/i)).toBeInTheDocument()
      expect(await screen.findByText('Core Web Vitals')).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /apply settings/i })).toBeEnabled()
    })

    it('invalidates the versions query once the upgrade stream completes, without a manual reload', async () => {
      let versionCalls = 0
      server.use(
        http.get(`${getApiBase()}/api/services/:service_id/rum/versions`, () => {
          versionCalls += 1
          return HttpResponse.json(faroVersions({ current: null, latest: '1.9.0', update_available: true }))
        }),
      )
      const user = userEvent.setup()
      const { rerender } = renderCard()

      await waitFor(() => expect(versionCalls).toBe(1))
      await user.click(await screen.findByRole('button', { name: /rum settings/i }))
      await screen.findByText('Core Web Vitals')
      await user.click(screen.getByRole('button', { name: /apply settings/i }))

      await waitFor(() => {
        expect(mockStart).toHaveBeenCalledWith(`/api/services/${SVC_FARO}/rum/upgrade`, {
          version: '1.9.0',
          activate: true,
        })
      })

      sseState.status = 'done'
      rerender(<RumFaroVersionCard serviceId={SVC_FARO} />)

      await waitFor(() => expect(versionCalls).toBe(2))
    })
  })

  function renderDialog(props: Partial<React.ComponentProps<typeof RumSettingsDialog>> = {}) {
    const onOpenChange = vi.fn()
    const onComplete = vi.fn()
    const qc = createTestQueryClient({ queries: { staleTime: Infinity } })
    const svcId = props.serviceId !== undefined ? props.serviceId : DEFAULT_DIALOG_PROPS.serviceId
    if (svcId) {
      qc.setQueryData(queryKeys.rumStatus(svcId), {
        enabled: true,
        capture_vitals: true,
        capture_performance: true,
        capture_errors: true,
        capture_events: true,
      })
    }
    const utils = render(
      <RumSettingsDialog
        {...DEFAULT_DIALOG_PROPS}
        onOpenChange={onOpenChange}
        onComplete={onComplete}
        {...props}
      />,
      { wrapper: makeQueryWrapper(qc) },
    )
    return { ...utils, onOpenChange, onComplete, qc }
  }

  describe('RumSettingsDialog', () => {
    beforeEach(() => {
      resetState()
      useMockAdminFetch = true
      mockAdminFetch.mockReset()
      mockAdminFetch.mockResolvedValue({
        ok: true,
        status: 200,
        json: async () => ({ ok: true, message: 'Settings saved' }),
      })
    })

    it('renders all capture toggle switches and defaults targeted version', () => {
      renderDialog()
      expect(screen.getByText('Core Web Vitals')).toBeInTheDocument()
      expect(screen.getByText('Performance Timings')).toBeInTheDocument()
      expect(screen.getByText('JavaScript Errors')).toBeInTheDocument()
      expect(screen.getByText('Custom Events')).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /apply settings/i })).toBeEnabled()
    })

    it('submits toggles via POST settings endpoint on Confirm when version is unchanged', async () => {
      const user = userEvent.setup()
      const { onComplete } = renderDialog({ currentVersion: '1.9.0', latestVersion: '1.9.0' })

      await user.click(screen.getByRole('button', { name: /apply settings/i }))

      expect(mockAdminFetch).toHaveBeenCalledWith('/api/services/svc-1/rum/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          capture_vitals: true,
          capture_performance: true,
          capture_errors: true,
          capture_events: true,
          custom_condition: '',
        }),
      })

      await waitFor(() => expect(onComplete).toHaveBeenCalledTimes(1))
    })

    it('triggers both settings POST and streamed version upgrade when target version changes', async () => {
      const user = userEvent.setup()
      renderDialog({ currentVersion: null })

      await user.click(screen.getByRole('button', { name: /apply settings/i }))

      expect(mockAdminFetch).toHaveBeenCalledWith('/api/services/svc-1/rum/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          capture_vitals: true,
          capture_performance: true,
          capture_errors: true,
          capture_events: true,
          custom_condition: '',
        }),
      })

      await waitFor(() => {
        expect(mockStart).toHaveBeenCalledWith('/api/services/svc-1/rum/upgrade', {
          version: '1.9.0',
          activate: true,
        })
      })
    })

    it('shows execution screen during progress', async () => {
      const user = userEvent.setup()
      const { rerender } = renderDialog({ currentVersion: null })

      await user.click(screen.getByRole('button', { name: /apply settings/i }))

      sseState.status = 'streaming'
      rerender(<RumSettingsDialog {...DEFAULT_DIALOG_PROPS} currentVersion={null} onOpenChange={vi.fn()} />)

      expect(screen.getByText(/applying rum settings/i)).toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /cancel/i })).not.toBeInTheDocument()
      expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
    })

    it('calculates and displays estimated uncompressed log line size based on active toggles', async () => {
      const user = userEvent.setup()
      renderDialog()

      expect(screen.getByText(/2\.84 KB/i)).toBeInTheDocument()

      await user.click(screen.getByRole('switch', { name: /javascript errors/i }))
      await user.click(screen.getByRole('switch', { name: /custom events/i }))
      await user.click(screen.getByRole('switch', { name: /performance timings/i }))

      expect(screen.getByText(/423 B/i)).toBeInTheDocument()
    })

    it('returns null without a serviceId', () => {
      const { container } = renderDialog({ serviceId: null })
      expect(container).toBeEmptyDOMElement()
    })
  })

  function renderPanel() {
    const qc = createTestQueryClient()
    return render(<RumStatusPanel />, { wrapper: makeQueryWrapper(qc) })
  }

  function mockRumStatus(enabled: boolean) {
    server.use(
      http.get('/api/services/:service_id/rum/status', () =>
        HttpResponse.json({ enabled, enabled_at: enabled ? '2026-01-01T00:00:00Z' : null }),
      ),
      http.get('/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC_PANEL, name: 'Test Service' }],
        }),
      ),
    )
  }

  describe('RumStatusPanel', () => {
    beforeEach(() => {
      useMockAdminFetch = false
      useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false })
    })

    it('analyst + RUM not enabled: shows the empty state, not Enable/Disable controls', async () => {
      mockIsAnalyst = true
      useServiceStore.setState({
        activeServiceId: SVC_PANEL,
        services: [{ id: SVC_PANEL, name: 'Test Service' }],
        isInitialized: true,
      })
      mockRumStatus(false)

      renderPanel()

      expect(await screen.findByText(/rum not enabled/i)).toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /enable rum/i })).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /disable rum/i })).not.toBeInTheDocument()
    })

    it('admin + RUM not enabled: still shows Enable/Disable controls', async () => {
      useServiceStore.setState({
        activeServiceId: SVC_PANEL,
        services: [{ id: SVC_PANEL, name: 'Test Service' }],
        isInitialized: true,
      })
      mockRumStatus(false)

      renderPanel()

      expect(await screen.findByRole('button', { name: /enable rum/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /disable rum/i })).toBeInTheDocument()
      expect(screen.queryByText(/rum not enabled/i)).not.toBeInTheDocument()
    })
  })
})

type SecurityData = components['schemas']['SecurityAggregatesResponse']
type BotsData = NonNullable<React.ComponentProps<typeof BotsSection>['data']>
type HeaderData = NonNullable<React.ComponentProps<typeof HeaderAnomaliesSection>['data']>

describe('SecurityClient, BotsSection, HeaderAnomaliesSection, NetworkSection & ProxyWatchdogSection', () => {
  beforeEach(() => {
    resetDefaultStores()
    mockPathname = '/security'
    useInteractiveDataTableMock = true
    useSecurityActiveFields = true
    useMockAnalyticsCard = false
    reportLayoutChildArgs = {
      data: { security_signals: [], top_bots: [], ngwaf_bots: [] },
      isLoading: false,
      isFetching: false,
      intervalButtons: null,
      bucketSeconds: 3600,
    } as unknown as ReportLayoutChildArgs
    queryClient.clear()
    plotlyMock.mockClear()
    dataTableMock.mockClear()
    securityActiveFields.clear()
    errorSpy = spyOnConsoleError()
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

  test('security page mounts and renders title and tab navigation', async () => {
    renderWithQuery(<SecurityClient />, { queryClient })

    expect(screen.getByText('Security')).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /waf activity/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /threat intelligence/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /vpn & proxy watchdog/i })).toBeInTheDocument()

    await waitFor(() => {
      expect(
        screen.getByText('Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)'),
      ).toBeInTheDocument()
    })

    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('BotsSection renders Bot & Crawler Value vs. Cost table with bandwidth and referral metrics', async () => {
    renderWithQuery(
      <BotsSection
        data={{
          _is_cached: false,
          tls_fingerprints: [],
          fingerprint_coverage: {},
          req_size_dist: [],
          top_ips_header: [],
          ipv6_adoption: [],
          proxy_dist: [],
          conn_reuse_dist: [],
          verified_bots_ts: [],
          ngwaf_configured: false,
          ngwaf_verified_bots: [],
          ngwaf_verified_bots_ts: [],
          wellknown_bots: [],
          bot_efficiency: [
            {
              bot_name: 'Anthropic Crawler Search',
              category: 'AI Crawler',
              request_count: 1500,
              resp_bytes: 52_428_800,
              bandwidth_pct: 32.5,
              referrals: 0,
              requests_per_referral: 1500,
              bytes_per_referral: 52_428_800,
            },
            {
              bot_name: 'Google Crawler',
              category: 'Search Engine Crawler',
              request_count: 800,
              resp_bytes: 10_485_760,
              bandwidth_pct: 6.5,
              referrals: 400,
              requests_per_referral: 2.0,
              bytes_per_referral: 26_214.4,
            },
          ],
          bots: [],
          ngwaf_bots: [],
          active_proxies_count: 0,
          tunnel_requests_count: 0,
          distance_mismatches_count: 0,
          traffic_quality: [],
          suspicious_isps: [],
          active_clients: [],
        }}
        bucketSeconds={3600}
        isLoading={false}
        isFetching={false}
        error={null}
        intervalButtons={null}
        timezone="UTC"
        commonTimeLayout={{}}
        getFieldLabel={(id: string) => id}
        ngwafBotVisibility={{}}
        setNgwafBotVisibility={vi.fn()}
        onNgwafBotVisChange={vi.fn()}
        botVisibility={{}}
        setBotVisibility={vi.fn()}
        onBotVisChange={vi.fn()}
        botEfficiencyVisibility={{}}
        setBotEfficiencyVisibility={vi.fn()}
        onBotEfficiencyVisChange={vi.fn()}
        fingerprintVisibility={{}}
        setFingerprintVisibility={vi.fn()}
        onFingerprintVisChange={vi.fn()}
      />,
      { queryClient },
    )

    expect(
      screen.getByText('Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)'),
    ).toBeInTheDocument()
    expect(screen.getByText('Anthropic Crawler Search')).toBeInTheDocument()
    expect(screen.getByText('Google Crawler')).toBeInTheDocument()
    const efficiencyRows = dataTableMock.mock.calls.at(-1)?.[0].data as Array<{ category?: string }> | undefined
    expect(efficiencyRows?.map((r) => r.category)).toEqual(['AI Crawler', 'Search Engine Crawler'])
  })

  function baseProps(overrides: Partial<React.ComponentProps<typeof BotsSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      intervalButtons: <div data-testid="interval-buttons" />,
      bucketSeconds: 3600,
      timezone: 'UTC',
      commonTimeLayout: {},
      getFieldLabel: (id: string) => id,
      ngwafBotVisibility: {},
      setNgwafBotVisibility: vi.fn(),
      onNgwafBotVisChange: vi.fn(),
      botVisibility: {},
      setBotVisibility: vi.fn(),
      onBotVisChange: vi.fn(),
      fingerprintVisibility: {},
      setFingerprintVisibility: vi.fn(),
      onFingerprintVisChange: vi.fn(),
      ...overrides,
    }
  }

  describe('BotsSection', () => {
    it('shows loading skeleton (AnalyticsCard overlay) when data is undefined and isLoading=true', () => {
      render(<BotsSection {...baseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading NGWAF verified bots...')).toBeInTheDocument()
      expect(screen.getByText('Loading NGWAF bot names...')).toBeInTheDocument()
      expect(screen.getByText('Loading well-known bots...')).toBeInTheDocument()
      expect(screen.getByText('Loading TLS fingerprints...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<BotsSection {...baseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\], .\\[contain-intrinsic-size\\:300px\\]')
      expect(containedElements.length).toBeGreaterThanOrEqual(4)
    })

    it('renders empty-state messages when arrays are empty', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
          })}
        />,
      )
      expect(screen.getAllByText(/no ngwaf bot detections/i).length).toBe(2)
      expect(screen.getAllByTestId('data-table-0').length).toBeGreaterThanOrEqual(3)
    })

    it('hides NGWAF cards entirely when ngwaf_configured=false', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: false,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
          })}
        />,
      )
      expect(screen.queryByText(/Verified Bots \(NGWAF\)/i)).not.toBeInTheDocument()
      expect(screen.queryByText(/Verified Bot Names \(NGWAF\)/i)).not.toBeInTheDocument()
    })

    it('routes the TLS-fingerprint empty copy on field-active state', () => {
      const emptyData = partial<BotsData>({
        ngwaf_configured: true,
        ngwaf_verified_bots_ts: [],
        ngwaf_verified_bots: [],
        wellknown_bots: [],
        tls_fingerprints: [],
      })

      const { unmount } = render(<BotsSection {...baseProps({ data: emptyData })} />)
      expect(
        screen.getByText(
          'Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.',
        ),
      ).toBeInTheDocument()
      unmount()
      dataTableMock.mockClear()

      securityActiveFields.add('ja3')
      render(<BotsSection {...baseProps({ data: emptyData })} />)
      expect(screen.getByText('No TLS fingerprints in this time range.')).toBeInTheDocument()
      expect(
        screen.queryByText(/Requires Security: TLS Fingerprinting/),
      ).not.toBeInTheDocument()
    })

    it('passes populated rows through to DataTable for each section', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [
                { time: '2026-01-01T00:00:00Z', bot_name: 'OpenAI', count: 12 },
                { time: '2026-01-01T01:00:00Z', bot_name: 'OpenAI', count: 8 },
                { time: '2026-01-01T01:00:00Z', bot_name: 'Anthropic', count: 3 },
              ],
              ngwaf_verified_bots: [
                { bot_name: 'OpenAI', category: 'search-engine', request_count: 20 },
              ],
              wellknown_bots: [
                { id: 'gb', name: 'Googlebot', category: 'search-engine', request_count: 100, verified_count: 90, impersonator_count: 5, unverified_count: 3, pending_count: 2 },
              ],
              tls_fingerprints: [{ fingerprint: 'sha-abc', ip_count: 4, request_count: 50 }],
              bot_efficiency: [
                {
                  bot_name: 'Commoncrawl Crawler',
                  category: 'ai-scraper',
                  request_count: 40,
                  resp_bytes: 80000,
                  bandwidth_pct: 12.5,
                  referrals: 0,
                  requests_per_referral: 40,
                  bytes_per_referral: 80000,
                },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalled()
      const traces = plotlyMock.mock.calls[0]![0].data
      expect(Array.isArray(traces)).toBe(true)
      expect(traces.length).toBe(2)
      const names = traces.map((t) => t.name).sort()
      expect(names).toEqual(['Anthropic', 'OpenAI'])

      const tableProps = dataTableMock.mock.calls.map((c) => c[0])
      expect(tableProps).toHaveLength(4)
      expect(tableProps[0]?.data).toHaveLength(1)
      expect(tableProps[1]?.data).toHaveLength(1)
      expect(tableProps[2]?.data).toHaveLength(1)
      expect(tableProps[3]?.data).toHaveLength(1)

      expect(screen.getByText('Googlebot')).toBeInTheDocument()
      expect(screen.getByText('OpenAI')).toBeInTheDocument()
    })

    it('renders low-coverage hint only when fingerprint_coverage < 1% for that field', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
              fingerprint_coverage: { tls_ciphers_sha: 0.005 },
            }),
          })}
        />,
      )
      expect(screen.getByText(/low coverage/i)).toBeInTheDocument()
      expect(screen.getByText(/TLS fingerprints are only captured/i)).toBeInTheDocument()
    })

    it('invokes the column-visibility change callback when the user toggles a column off', async () => {
      const onNgwafBotVisChange = vi.fn()
      const user = userEvent.setup()
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
            onNgwafBotVisChange,
          })}
        />,
      )

      const buttons = screen.getAllByTestId('col-vis')
      expect(buttons.length).toBeGreaterThanOrEqual(3)
      await user.click(buttons[0]!)
      expect(onNgwafBotVisChange).toHaveBeenCalledTimes(1)
      expect(onNgwafBotVisChange).toHaveBeenCalledWith('bot_name', false)
    })
  })

  function headerBaseProps(overrides: Partial<React.ComponentProps<typeof HeaderAnomaliesSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      getFieldLabel: (id: string) => id,
      topIpVisibility: {},
      setTopIpVisibility: vi.fn(),
      onTopIpVisChange: vi.fn(),
      ...overrides,
    }
  }

  describe('HeaderAnomaliesSection', () => {
    it('shows loading skeleton overlay across both cards when isLoading', () => {
      render(<HeaderAnomaliesSection {...headerBaseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading request header size distribution...')).toBeInTheDocument()
      expect(screen.getByText('Loading oversized request headers...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<HeaderAnomaliesSection {...headerBaseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\], .\\[contain-intrinsic-size\\:300px\\]')
      expect(containedElements.length).toBe(2)
    })

    it('renders the not-enabled "Requires Group A" copy when req_header_bytes is inactive', () => {
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
          })}
        />,
      )
      expect(screen.getByText(/no data available/i)).toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
      expect(screen.getByTestId('empty')).toBeInTheDocument()
      expect(
        screen.getByText(
          'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.',
        ),
      ).toBeInTheDocument()
    })

    it('renders the neutral "no data yet" copy when req_header_bytes IS active', () => {
      securityActiveFields.add('req_header_bytes')
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
          })}
        />,
      )
      expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(2)
      expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
    })

    it('passes populated rows to Plotly and DataTable', () => {
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({
              req_size_dist: [
                { bucket: '0-512', count: 100 },
                { bucket: '512-1k', count: 80 },
                { bucket: '8k+', count: 5 },
              ],
              top_ips_header: [
                { ip: '203.0.113.5', max_header: 16384 },
                { ip: '198.51.100.7', max_header: 12000 },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalledTimes(1)
      const trace = plotlyMock.mock.calls[0]![0].data[0]
      expect(trace?.type).toBe('bar')
      expect(trace?.x).toEqual(['0-512', '512-1k', '8k+'])
      expect(trace?.y).toEqual([100, 80, 5])

      expect(dataTableMock).toHaveBeenCalledTimes(1)
      expect(dataTableMock.mock.calls[0]![0].data).toHaveLength(2)
      expect(screen.getByText('203.0.113.5')).toBeInTheDocument()
      expect(screen.getByText('198.51.100.7')).toBeInTheDocument()
    })

    it('forwards column-visibility toggles to the parent callback', async () => {
      const onTopIpVisChange = vi.fn()
      const user = userEvent.setup()
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
            onTopIpVisChange,
          })}
        />,
      )
      const btn = screen.getByTestId('col-vis')
      await user.click(btn)
      expect(onTopIpVisChange).toHaveBeenCalledWith('ip', false)
    })
  })

  function securityData(overrides: Partial<SecurityData> = {}): SecurityData {
    return {
      _is_cached: false,
      tls_fingerprints: [],
      fingerprint_coverage: {},
      req_size_dist: [],
      top_ips_header: [],
      ipv6_adoption: [],
      proxy_dist: [],
      conn_reuse_dist: [],
      verified_bots_ts: [],
      ngwaf_configured: false,
      ngwaf_verified_bots: [],
      ngwaf_verified_bots_ts: [],
      wellknown_bots: [],
      bot_efficiency: [],
      bots: [],
      ngwaf_bots: [],
      active_proxies_count: 10,
      tunnel_requests_count: 500,
      distance_mismatches_count: 2,
      traffic_quality: [],
      suspicious_isps: [],
      active_clients: [],
      ...overrides,
    }
  }

  function networkBaseProps(overrides: Partial<React.ComponentProps<typeof NetworkSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      timezone: 'UTC',
      commonTimeLayout: {},
      ...overrides,
    }
  }

  describe('NetworkSection', () => {
    it('shows loading skeleton overlay across all three cards when isLoading', () => {
      render(<NetworkSection {...networkBaseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading IPv6 adoption data...')).toBeInTheDocument()
      expect(screen.getByText('Loading proxy/anonymizer breakdown...')).toBeInTheDocument()
      expect(screen.getByText('Loading connection reuse distribution...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<NetworkSection {...networkBaseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\]')
      expect(containedElements.length).toBe(3)
    })

    it('renders "No data available" empty card for each chart when arrays are empty', () => {
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      expect(screen.getAllByText(/no data available/i).length).toBe(3)
      expect(screen.getAllByText(/Infrastructure \(Group C\) fields/i).length).toBe(2)
      expect(screen.getByText(/Security: Proxy & Anonymization \(Group I\)/i)).toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
    })

    it('shows a neutral "no data yet" state (no Requires hint) when the field group IS enabled', () => {
      securityActiveFields.add('is_ipv6')
      securityActiveFields.add('p_type')
      securityActiveFields.add('conn_requests')
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(3)
      expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
    })

    it('forwards populated data into Plotly traces (IPv6 line, proxy pie, conn-reuse bar)', () => {
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({
              ipv6_adoption: [
                { time: '2026-01-01T00:00:00Z', pct: 42 },
                { time: '2026-01-01T01:00:00Z', pct: 51 },
              ],
              proxy_dist: [
                { type: 'residential', count: 800 },
                { type: 'hosting', count: 150 },
                { type: 'tor', count: 5 },
              ],
              conn_reuse_dist: [
                { bucket: '1', count: 300 },
                { bucket: '2-5', count: 1200 },
                { bucket: '6-10', count: 900 },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalledTimes(3)
      const [ipv6Call, proxyCall, reuseCall] = plotlyMock.mock.calls.map((c) => c[0])

      expect(ipv6Call.data[0]?.name).toBe('IPv6 %')
      expect(ipv6Call.data[0]?.y).toEqual([42, 51])

      expect(proxyCall.data[0]?.type).toBe('pie')
      expect(proxyCall.data[0]?.labels).toEqual(['residential', 'hosting', 'tor'])
      expect(proxyCall.data[0]?.values).toEqual([800, 150, 5])

      expect(reuseCall.data[0]?.type).toBe('bar')
      expect(reuseCall.data[0]?.x).toEqual(['1', '2-5', '6-10'])
      expect(reuseCall.data[0]?.y).toEqual([300, 1200, 900])
    })

    it('opens the help dialog when the user clicks an AnalyticsCard help button', async () => {
      const user = userEvent.setup()
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      const helpButtons = screen.getAllByRole('button', { name: /about this chart/i })
      expect(helpButtons.length).toBe(3)
      await user.click(helpButtons[0]!)

      const dialog = await screen.findByRole('dialog')
      expect(dialog).toBeInTheDocument()
      expect(within(dialog).getByText(/Tracks the percentage of requests/i)).toBeInTheDocument()
    })
  })

  describe('ProxyWatchdogSection', () => {
    it('renders stats and all card headers correctly', () => {
      const mockData = securityData()
      render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
      expect(screen.getByText('Active VPN & Proxy Users')).toBeInTheDocument()
      expect(screen.getByText('10')).toBeInTheDocument()
      expect(screen.getByText('Tunnel Requests')).toBeInTheDocument()
      expect(screen.getByText('500')).toBeInTheDocument()
      expect(screen.getByText('Impossible Distance Mismatches')).toBeInTheDocument()
      expect(screen.getByText('2')).toBeInTheDocument()

      expect(screen.getByText('Traffic Quality Breakdown')).toBeInTheDocument()
      expect(screen.getByText('Suspicious ISPs & Providers')).toBeInTheDocument()
      expect(screen.getByText('VPN/Proxy Export Center')).toBeInTheDocument()
      expect(screen.getByText('Active Suspicious Clients')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on stat cards and card containers', () => {
      const mockData = securityData()
      const { container } = render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:120px\\]').length).toBeGreaterThanOrEqual(3)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:320px\\]').length).toBeGreaterThanOrEqual(2)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:140px\\]').length).toBeGreaterThanOrEqual(1)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:600px\\]').length).toBeGreaterThanOrEqual(1)
    })
  })
})
