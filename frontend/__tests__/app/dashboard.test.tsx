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
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import DashboardClient, { buildChartLayout } from '@/app/dashboard/_sections/DashboardClient'
import AssetsClient, { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import NetworkPage from '@/app/network/page'
import QueryPage from '@/app/query/page'
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import OriginClient from '@/app/origin/_sections/OriginClient'
import FastlyValueClient, { IOValueTab } from '@/app/fastly-value/page'
import type { components } from '@/types/api.generated'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ReviewCard, ReviewHeader, ReviewContent, ReviewItem, SectionHeader, StatCard } from '@/components/ui/confirm-dialog'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { densifyBarSeries, buildTrafficData, getIntervalSeconds, _resetUrlHydrationFlag, hydrateFilterStoreFromUrl } from '@/lib/utils'
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import { server } from '@/__tests__/helpers/msw/handlers'
import {
  createTestQueryClient, makeQueryWrapper, renderWithQuery, getOk, postOk,
  spyOnConsoleError, stubResizeObserver, stubPointerCaptureAndScroll,
  createUseSSEMock, type ReportLayoutChildArgs, type InteractiveDataTableMockProps,
} from '../helpers/page-smoke'
import * as apiLib from '@/lib/api'
import type { PlotlyChart } from '@/components/PlotlyChart'
import React, { type ComponentProps } from 'react'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

const { state: sseState } = createUseSSEMock()
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
  mockGet, mockPost, mockDelete,
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
