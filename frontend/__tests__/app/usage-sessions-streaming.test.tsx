import { fireEvent, render, renderHook, screen, waitFor, within } from '@testing-library/react'
import { http, HttpResponse } from 'msw/http'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { UsageLogClient } from '@/app/admin/usage-log/page'
import Loading from '@/app/admin/usage-log/loading'
import UsagePage from '@/app/usage/page'
import SessionsPage from '@/app/sessions/page'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { DEFAULTS, RAW_LOG_COMPRESSION_RATIO, RUM_COMPRESSED_BYTES_PER_BEACON, reducer, calculate, type PrefillResponse } from '@/components/CostCalculator/CostCalculator'
import HighScaleRequestFactsPage from '@/app/high-scale/request-facts/page'
import HighScaleRequestFactsLoading from '@/app/high-scale/request-facts/loading'
import { highScaleRequestFactsHooks } from '@/components/high-scale/HighScaleRequestFactsPanel'
import { CorrelatorLeaderboard, useTickHistory, type MetricsTick as HistoryMetricsTick } from '@/app/control-room/_sections/ControlRoomClient'
import type { SSELine } from '@/hooks/useSSE'
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import { server, CMCD_FIXTURE } from '@/__tests__/helpers/msw/handlers'
import {
  createTestQueryClient, makeQueryWrapper, renderWithQuery,
  spyOnConsoleError, stubResizeObserver, stubPointerCaptureAndScroll,
  createUseSSEMock, type ReportLayoutChildArgs, type InteractiveDataTableMockProps,
} from '../helpers/page-smoke'
import type { PlotlyChart } from '@/components/PlotlyChart'
import React, { type ComponentProps } from 'react'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { DEFAULT_INPUTS, DEFAULT_UNITS, LIST_PRICE_RATES } from '@/lib/cloud-engine/usage/estimate'
import { useServiceQuery } from '@/hooks/useDashboardCards'

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
  CostCalculator: ({ prefillNote, localStorageGbHours }: { prefillNote?: string; localStorageGbHours?: number | null }) => (
    <div data-testid="cost-calculator" data-local-storage-gb-hours={localStorageGbHours ?? ''}>{prefillNote}</div>
  ),
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


// ── Usage, Sessions, Streaming & CostCalculator tests ────────────────────────

function makePrefill(overrides: Partial<PrefillResponse>): PrefillResponse {
  return {
    _is_cached: false, data_days: 7, sample_rate: 100, edge_only: false,
    delete_after: false, snapshot_interval_minutes: 5, log_retention_days: 90, rum_retention_days: 90, billing_zone: null, ...overrides,
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
    setCloudModeRuntime(false)
    reportLayoutChildArgs = null
    useMockDataTable = false
    mockIsDataReady = null
    errorSpy.mockRestore()
  })

  test('treats a service query waiting for app data as initially loading', () => {
    mockIsDataReady = false
    const queryFn = vi.fn(async () => ({ rows: [] }))

    const { result } = renderHook(
      () => useServiceQuery(['loading-contract', 'test-svc'], queryFn),
      { wrapper: makeQueryWrapper(queryClient) },
    )

    expect(result.current.isLoading).toBe(true)
    expect(queryFn).not.toHaveBeenCalled()
  })

  test('does not mark an explicitly disabled service query as loading', () => {
    mockIsDataReady = false
    const queryFn = vi.fn(async () => ({ rows: [] }))

    const { result } = renderHook(
      () => useServiceQuery(['loading-contract-disabled', 'test-svc'], queryFn, { enabled: false }),
      { wrapper: makeQueryWrapper(queryClient) },
    )

    expect(result.current.isLoading).toBe(false)
    expect(queryFn).not.toHaveBeenCalled()
  })

  test('does not mark a callback-disabled service query as loading', () => {
    mockIsDataReady = false
    const queryFn = vi.fn(async () => ({ rows: [] }))

    const { result } = renderHook(
      () => useServiceQuery(['loading-contract-disabled-callback', 'test-svc'], queryFn, { enabled: () => false }),
      { wrapper: makeQueryWrapper(queryClient) },
    )

    expect(result.current.isLoading).toBe(false)
    expect(queryFn).not.toHaveBeenCalled()
  })

  test('usage page mounts and renders title', () => {
    renderWithQuery(<UsagePage />, { queryClient })
    expect(screen.getByText('System Usage')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('usage initial shell renders substantive above-the-fold summary and keeps partial storage notice compact', () => {
    reportLayoutChildArgs = null
    mockIsDataReady = false
    const { unmount } = renderWithQuery(<UsagePage />, { queryClient })

    expect(screen.getByRole('heading', { name: 'System Usage' })).toBeInTheDocument()
    const summary = screen.getByText(/Measure Fastly Object Storage impact/i)
    expect(summary).toBeInTheDocument()
    expect(summary.textContent?.length ?? 0).toBeGreaterThan(120)
    unmount()

    reportLayoutChildArgs = {
      startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', activeServiceId: 'test-svc',
      filterPayload: {}, intervalButtons: null, bucketSeconds: 300,
      config: { effectiveInterval: '5 minutes', interval: '5 minutes' },
      setChartInterval: vi.fn(), timezone: 'UTC', data: { rows: [], total: 0 }, isLoading: false, isFetching: false,
    }
    mockIsDataReady = true
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            live_bytes: 0, live_files: 0, quarantine_bytes: 0,
            total_billed_bytes: 0, total_billed_gb_hours: 720,
            storage_billing_complete: false,
            storage_billing_provenance: ['high-scale:incomplete_lifecycle'],
            total_files: 0, total_bytes: 0, deleted_bytes: 0,
            start: '2026-01-01T00:00:00Z', end: '2026-01-01T01:00:00Z',
          },
        } as never
      }
      return undefined
    })

    renderWithQuery(<UsagePage />, { queryClient })
    return screen.findByText(/Partial storage cost/).then((notice) => {
      expect(notice.className).toContain('truncate')
      expect(notice.className).toContain('max-w-[240px]')
      expect(notice.getAttribute('title')).toContain('high-scale:incomplete_lifecycle')
    })
  })

  test('RUM failure keeps an unavailable section visible when RUM is enabled', async () => {
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test', rum_enabled: true }],
    })
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/rum-breakdown') throw new Error('rum unavailable')
      return { data: {} }
    })

    renderWithQuery(<UsagePage />, { queryClient })

    expect(await screen.findByText('Real User Monitoring (RUM) Operations')).toBeInTheDocument()
    expect(await screen.findByText('Something went wrong')).toBeInTheDocument()
    expect(screen.getByText('rum unavailable')).toBeInTheDocument()
  })

  test('Cloud usage renders Compute, KV, secrets and CDN requests from measured prefill', async () => {
    setCloudModeRuntime(true)
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/prefill/rates') return { data: LIST_PRICE_RATES }
      if (path === '/api/usage/prefill') return {
        data: {
          inputs: { ...DEFAULT_INPUTS, requestsPerDay: 12345, edgeRequestsPerDay: 10000 },
          units: { ...DEFAULT_UNITS, secrets: 3 },
          measuredInputs: { requestsPerDay: true, edgeRequestsPerDay: true },
          measuredUnits: { secrets: true },
          rates: { ...LIST_PRICE_RATES, secret_rate_per_month: 7 },
          window: { days: 7, files: 10, lines: 100 },
        },
      }
      return undefined
    })

    renderWithQuery(<UsagePage />, { queryClient })
    expect(await screen.findByText('Compute (app + worker)')).toBeInTheDocument()
    expect(screen.getByText('KV Store')).toBeInTheDocument()
    expect(screen.getByText('Secret Store')).toBeInTheDocument()
    expect(screen.getByText('1 secrets')).toBeInTheDocument()
    expect(screen.getByText('$0.00')).toBeInTheDocument()
    expect(screen.getByText(/front reqs/)).toBeInTheDocument()
    expect(screen.getByRole('spinbutton', { name: 'Total requests per day' })).toHaveValue(12345)
    expect(screen.queryByTestId('cost-calculator')).not.toBeInTheDocument()
  })

  test('Cloud cost card and Compute charts use one Platform Metrics read for the range', async () => {
    setCloudModeRuntime(true)
    useMockAdminFetch = true
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') return { data: { compute_requests: 7, compute_vcpu_ms: 3, kv_ops: 2 } as never }
      return undefined
    })
    mockAdminFetch.mockImplementation(async () => Response.json({
      source: 'fastly', requests: 1_500, vcpu_ms: 2_500, billed_ms: 60_000,
      app_requests: 1_000, worker_requests: 500, app_vcpu_ms: 2_000, worker_vcpu_ms: 500,
      covered_from: '2026-01-01T00:00:00.000Z', covered_to: '2026-01-01T01:00:00.000Z',
      data: [{ time: '2026-01-01T00:00:00.000Z', app_requests: 1_000, worker_requests: 500, app_vcpu_ms: 2_000, worker_vcpu_ms: 500, billed_ms: 60_000 }],
    }))

    renderWithQuery(<UsagePage />, { queryClient })

    expect(await screen.findByText('Fastly stats · KV logged')).toBeInTheDocument()
    expect(screen.getByText('1.5K reqs')).toBeInTheDocument()
    expect(screen.queryByText('7 reqs')).not.toBeInTheDocument()
    expect(screen.getByText('Compute Requests')).toBeInTheDocument()
    expect(screen.getByText('Compute vCPU')).toBeInTheDocument()
    expect(mockAdminFetch).toHaveBeenCalledTimes(1)
    expect(mockAdminFetch.mock.calls[0][0]).toBe(
      '/api/usage/compute?start_time=2026-01-01T00%3A00%3A00Z&end_time=2026-01-01T01%3A00%3A00Z',
    )
    const vcpuTraces = plotlyMock.mock.calls.map(([props]) => props.data as Array<{ name?: string; y?: number[]; hovertemplate?: string }>)
      .findLast((traces) => traces.some((t) => t.hovertemplate?.includes(' s<extra>')))
    expect(vcpuTraces?.map((t) => [t.name, t.y])).toEqual([['App service', [2]], ['Worker service', [0.5]]])
  })

  test('Cloud cost card prices every product the install uses, not only storage, operations and egress', async () => {
    setCloudModeRuntime(true)
    useMockAdminFetch = true
    const line = (id: string, label: string, usd: number | null, detail: string, source = 'fastly') => ({ id, label, usd, detail, source })
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            total_billed_gb_hours: 43.2, storage_billing_complete: true, compute_requests: 7, compute_vcpu_ms: 3, kv_ops: 2,
            incurred: {
              total: 10.4212,
              telemetryUsd: 0,
              lines: [
                line('storage', 'Object Storage', 0.0012, '43.20 GB-hrs', 'logged'),
                line('ops', 'Storage operations', 0.08, 'A 15,000 · B 97,800', 'logged'),
                { ...line('delivery', 'CDN delivery', 9.2, '0.75 GB · 9,100,000 front reqs'), parts: [{ label: 'Egress', usd: 0.09 }, { label: 'Requests', usd: 9.11 }] },
                line('compute', 'Compute (app + worker)', 0.31, '141,195 reqs · 5,160,000 vCPU ms'),
                line('kv', 'KV Store operations', null, 'Admin only (Fastly stats)', 'none'),
                line('secrets', 'Secret Store', 0.83, '5 secrets × 1.0 days'),
                line('config', 'Config Store', 0, 'No usage charge'),
              ],
            },
          } as never,
        }
      }
      if (path === '/api/usage/operations') {
        return { data: { source: 'local', complete: true, provenance: ['fos_usage'], total_class_a: 15_000, total_class_b: 97_800, data: [] } as never }
      }
      return undefined
    })
    mockAdminFetch.mockImplementation(async () => Response.json({ source: 'unavailable', reason: 'stats_unavailable' }))

    renderWithQuery(<UsagePage />, { queryClient })

    const card = (await screen.findByText('Compute (app + worker):')).closest('[data-testid="stat-card"]') ?? document.body
    expect(within(card as HTMLElement).getByText('$10.42')).toBeInTheDocument()
    for (const [label, usd] of [['CDN delivery:', '$9.20'], ['Secret Store:', '$0.83'], ['Compute (app + worker):', '$0.31']]) {
      const row = within(card as HTMLElement).getByText(label).closest('tr') as HTMLElement
      expect(within(row).getByText(usd)).toBeInTheDocument()
    }
    const deliveryRow = within(card as HTMLElement).getByText('CDN delivery:').closest('tr') as HTMLElement
    expect(within(deliveryRow).getByText('Egress $0.09')).toBeInTheDocument()
    expect(within(deliveryRow).getByText('Requests $9.11')).toBeInTheDocument()
    const kvRow = within(card as HTMLElement).getByText('KV Store operations:').closest('tr') as HTMLElement
    expect(within(kvRow).getByText('n/a')).toBeInTheDocument()
    expect(within(card as HTMLElement).getByText(/KV Store operations cost excluded/)).toBeInTheDocument()
  })

  test('Cloud cost card falls back to logged Compute figures when Platform Metrics is unavailable', async () => {
    setCloudModeRuntime(true)
    useMockAdminFetch = true
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') return { data: { compute_requests: 7, compute_vcpu_ms: 3, kv_ops: 2 } as never }
      return undefined
    })
    mockAdminFetch.mockImplementation(async () => Response.json({ source: 'unavailable', reason: 'stats_unavailable' }))

    renderWithQuery(<UsagePage />, { queryClient })

    expect(await screen.findByText('logged')).toBeInTheDocument()
    expect(screen.getByText('7 reqs')).toBeInTheDocument()
  })

  test('High-Scale missing traffic is labelled reference rather than measured service traffic', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/prefill/rates' || path === '/api/usage/prefill')
        return { data: makePrefill({ requests_per_day: null, edge_requests_per_day: null, edge_only: true }) }
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    expect(await screen.findByText(/Traffic measurements are unavailable/)).toBeInTheDocument()
    expect(screen.queryByText('Compute (app + worker)')).not.toBeInTheDocument()
    expect(screen.queryByText('Compute Requests')).not.toBeInTheDocument()
  })

  test('Cloud usage defers below-the-fold estimator prefill, log-fields, and rum-breakdown queries until entering view', async () => {
    setCloudModeRuntime(true)
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test', rum_enabled: true }],
    })
    vi.stubGlobal('IntersectionObserver', class {
      observe() {}
      disconnect() {}
    })
    try {
      mockGet.mockImplementation(async (path: string) => {
        if (path === '/api/usage/prefill/rates') return { data: LIST_PRICE_RATES }
        return undefined
      })
      renderWithQuery(<UsagePage />, { queryClient })
      expect(await screen.findByText('Compute (app + worker)')).toBeInTheDocument()
      expect(mockGet).not.toHaveBeenCalledWith('/api/usage/prefill', expect.anything())
      expect(mockGet).not.toHaveBeenCalledWith('/api/services/{service_id}/log-fields', expect.anything())
      expect(mockGet).not.toHaveBeenCalledWith('/api/log-fields/catalog', expect.anything())
      expect(mockGet).not.toHaveBeenCalledWith('/api/usage/rum-breakdown', expect.anything())
    } finally {
      vi.unstubAllGlobals()
    }
  })

  test('High-Scale does not call traffic unavailable before the below-the-fold estimator loads its measurements', async () => {
    const observedOptions: Array<IntersectionObserverInit | undefined> = []
    vi.stubGlobal('IntersectionObserver', class {
      constructor(_cb: IntersectionObserverCallback, options?: IntersectionObserverInit) {
        observedOptions.push(options)
      }
      observe() {}
      disconnect() {}
    })
    try {
      mockGet.mockImplementation(async (path: string) => {
        if (path === '/api/usage/prefill/rates') return { data: makePrefill({ requests_per_day: null }) }
        return undefined
      })
      renderWithQuery(<UsagePage />, { queryClient })
      await screen.findByTestId('cost-calculator')
      expect(mockGet).not.toHaveBeenCalledWith('/api/usage/prefill', expect.anything())
      expect(screen.queryByText(/Traffic measurements are unavailable/)).not.toBeInTheDocument()
      expect(observedOptions).toContainEqual(expect.objectContaining({ rootMargin: '100px' }))
    } finally {
      vi.unstubAllGlobals()
    }
  })

  test('UX-6: storage card shows an error (not "0.00 GB-hrs") and the cost is flagged partial on a storage 5xx', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') throw new Error('storage boom')
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    await waitFor(() => expect(screen.getByText('Failed to load storage usage.')).toBeInTheDocument())
    expect(screen.queryByText(/GB-hrs/)).toBeNull()
    expect(await screen.findByText(/Partial estimate — storage.*cost excluded\./)).toBeInTheDocument()
  })

  test('storage breakdown keeps processed data under Live Storage and raw logs under Deleted (Still Billed)', async () => {
    const GB = 1024 ** 3
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            live_bytes: 10 * GB, live_files: 0, quarantine_bytes: 0, total_billed_bytes: 0, total_billed_gb_hours: 0,
            total_files: 0, total_bytes: 0, start: '', end: '',
            processed_request_bytes: 7 * GB, processed_rum_bytes: 2 * GB, processed_objects: 30,
            raw_logs_deleted: true, regular_log_bytes: 5 * GB, rum_bytes: 3 * GB,
            deleted_bytes: 20 * GB, cloud_copy_deleted_bytes: 4 * GB, archive_expired_bytes: 6 * GB,
          },
        } as never
      }
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    const label = async (text: string) => (await screen.findByText(text)).closest('tr') as HTMLElement
    const order = ['Live Storage:', '↳ Request data:', '↳ RUM data:', '↳ Other:', 'Deleted (Still Billed):',
      '↳ Raw request logs:', '↳ Raw RUM logs:', '↳ Compaction:', '↳ Expired by retention:']
    const rows = await Promise.all(order.map(label))
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i - 1].compareDocumentPosition(rows[i]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    }
    expect(rows[1]).toHaveTextContent('7 GB')
    expect(rows[1]).toHaveTextContent('30 objects')
    expect(rows[3]).toHaveTextContent('1 GB')
    expect(rows[5]).toHaveTextContent('5 GB')
    expect(screen.queryByText('↳ Cloud copy & compaction:')).toBeNull()
  })

  test('storage breakdown lists kept raw logs under Live Storage when raw deletion is off', async () => {
    const GB = 1024 ** 3
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            live_bytes: 9 * GB, live_files: 0, quarantine_bytes: 0, total_billed_bytes: 0, total_billed_gb_hours: 0,
            total_files: 0, total_bytes: 0, start: '', end: '',
            processed_request_bytes: 4 * GB, processed_rum_bytes: 0, processed_objects: 3,
            raw_logs_deleted: false, regular_log_bytes: 5 * GB, rum_bytes: 0, deleted_bytes: 0,
          },
        } as never
      }
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    const kept = (await screen.findByText('↳ Raw request logs (kept):')).closest('tr') as HTMLElement
    const deleted = screen.getByText('Deleted (Still Billed):').closest('tr') as HTMLElement
    expect(kept.compareDocumentPosition(deleted) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(screen.queryByText('↳ Other:')).toBeNull()
  })

  test('UsagePage keeps local selected-period storage authoritative when Platform storage is present and flags incomplete operations', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            live_bytes: 1024 ** 3, live_files: 1, quarantine_bytes: 0,
            total_billed_bytes: 1024 ** 3, total_billed_gb_hours: 720,
            storage_billing_complete: true, storage_billing_provenance: ['high-scale:source_objects'],
            total_files: 1, total_bytes: 1024 ** 3, deleted_bytes: 0, deleted_bytes_complete: true,
            start: '2026-01-01T00:00:00Z', end: '2026-01-01T01:00:00Z',
          },
        } as never
      }
      if (path === '/api/usage/platform') {
        return {
          data: {
            _is_cached: false,
            source: 'fastly',
            storage: { status: 'ok', scope: 'account', bytes: 2 * 1024 ** 3, buckets: 3, gb_hours: 48 },
            compute: { status: 'not_configured', scope: null, requests: 0, billed_time_ms: 0, backend_errors: 0, cache_operations: 0, sandboxes: 0, resource_errors: 0 },
            ngwaf: { status: 'not_configured', scope: null, total: 0, allowed: 0, blocked: 0, challenged: 0, logged: 0, timeout: 0, unknown: 0 },
          },
        } as never
      }
      if (path === '/api/usage/operations') {
        return {
          data: {
            data: [], total_class_a: 0, total_class_b: 0, granularity: 'hour', note: '', fos_fields_found: [],
            source: 'local', complete: false, provenance: [],
          },
        } as never
      }
      if (path === '/api/usage/prefill/rates' || path === '/api/usage/prefill') {
        return { data: makePrefill({}) } as never
      }
      return undefined
    })

    renderWithQuery(<UsagePage />, { queryClient })

    expect(await screen.findByText('720.00 GB-hrs')).toBeInTheDocument()
    expect(screen.queryByText('48.00 GB-hrs')).not.toBeInTheDocument()
    expect(screen.queryByText('48.00 GB-hours · 3 buckets')).not.toBeInTheDocument()
    expect(screen.queryByText('NGWAF Outcomes')).not.toBeInTheDocument()
    expect(screen.queryByText('Traffic Geography')).not.toBeInTheDocument()
    expect(screen.getAllByText('Measured · this install').length).toBeGreaterThan(0)
    expect(screen.queryByText('Measured · Fastly Platform (account-wide)')).not.toBeInTheDocument()
    expect(screen.getByTestId('cost-calculator')).toHaveAttribute('data-local-storage-gb-hours', '720')
    expect(screen.getAllByText('Unavailable').length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText(/Partial estimate.*operations.*excluded/)).toBeInTheDocument()
    expect(mockGet).not.toHaveBeenCalledWith('/api/usage/platform', expect.anything())
    expect(mockGet).not.toHaveBeenCalledWith('/api/usage/geography', expect.anything())
    expect(mockGet).toHaveBeenCalledWith(
      '/api/usage/current-storage',
      expect.objectContaining({
        params: { query: { start: '2026-01-01T00:00:00Z', end: '2026-01-01T01:00:00Z' } },
      }),
    )
  })

  test('sessions page mounts and renders title', () => {
    renderWithQuery(<SessionsPage />, { queryClient })
    expect(screen.getByText('User Sessions')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('shows a contextual loading state while Streaming prerequisites are pending', () => {
    mockIsDataReady = false

    renderWithQuery(<StreamingClient />, { queryClient })

    expect(screen.getByText('Loading Active Viewers...')).toBeInTheDocument()
    expect(screen.getByText('Loading Buffer Health...')).toBeInTheDocument()
    expect(mockPost).not.toHaveBeenCalled()
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

  test('formats Top Content metrics cleanly without raw floats, shows em-dash for null rebuffer_rate, and clamps Starvation Rate y-axis non-negative', async () => {
    useMockDataTable = false
    const { BUFFER_LAYOUT } = await import('@/app/streaming/_sections/StreamingConstants')
    expect(BUFFER_LAYOUT.yaxis2?.rangemode).toBe('nonnegative')

    mockPost.mockResolvedValue({
      data: {
        ...CMCD_FIXTURE,
        top_content: [
          {
            content_id: 'live-stream-1',
            session_count: 12,
            rebuffer_rate: null,
            avg_bitrate: 2395.9837186584173,
            avg_buffer_length: 3958.4825789645065,
          },
        ],
        bitrate_ladder: [
          { tier: '2.5–5 Mbps (1080p)', session_count: 10, request_count: 42, avg_bitrate: 3800, rebuffer_rate: 1.5 },
        ],
        stream_type_dist: [
          { stream_type: 'l', session_count: 8, request_count: 35 },
          { stream_type: 'v', session_count: 4, request_count: 12 },
        ],
      },
    } as never)
    renderWithQuery(<StreamingClient />, { queryClient })
    expect(await screen.findByText('live-stream-1')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Inspect Streaming Sessions/i })).toHaveAttribute(
      'href',
      expect.stringContaining('/sessions?streaming=true'),
    )
    expect(screen.getByText('ABR Bitrate Ladder')).toBeInTheDocument()
    expect(screen.getByText('2.5–5 Mbps (1080p)')).toBeInTheDocument()
    expect(screen.getByText('Stream Type (Live vs VOD)')).toBeInTheDocument()
    expect(screen.getByText('Live')).toBeInTheDocument()
    expect(screen.getByText('VOD')).toBeInTheDocument()
    const row = screen.getByText('live-stream-1').closest('tr')
    expect(row).not.toBeNull()
    expect(within(row!).getByText('2,396')).toBeInTheDocument()
    expect(within(row!).getByText('3,958')).toBeInTheDocument()
    expect(within(row!).getByText('—')).toBeInTheDocument()
    expect(screen.queryByText(/2395\.9837/)).not.toBeInTheDocument()
    expect(screen.queryByText(/3958\.4825/)).not.toBeInTheDocument()
  })

  test('Cloud streaming defers below-the-fold bitrate_ladder and stream_type_dist query until entering view', async () => {
    useMockDataTable = false
    setCloudModeRuntime(true)
    let triggerIntersect: (() => void) | null = null
    vi.stubGlobal('IntersectionObserver', class {
      private cb: IntersectionObserverCallback
      constructor(cb: IntersectionObserverCallback) {
        this.cb = cb
      }
      observe(target: Element) {
        triggerIntersect = () => {
          this.cb([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver)
        }
      }
      disconnect() {}
    })
    try {
      mockPost.mockImplementation(async (_path: string, opts?: { body?: { sections?: string[] } }) => {
        if (opts?.body?.sections?.includes('bitrate_ladder')) {
          return {
            data: {
              ...CMCD_FIXTURE,
              bitrate_ladder: [
                { tier: '2.5–5 Mbps (1080p)', session_count: 10, request_count: 42, avg_bitrate: 3800, rebuffer_rate: 1.5 },
              ],
              stream_type_dist: [
                { stream_type: 'l', session_count: 8, request_count: 35 },
              ],
            },
          } as never
        }
        return {
          data: {
            ...CMCD_FIXTURE,
            bitrate_ladder: [],
            stream_type_dist: [],
          },
        } as never
      })

      renderWithQuery(<StreamingClient />, { queryClient })
      expect(await screen.findByText('1,234')).toBeInTheDocument()
      // Only the primary summary-backed query ran; the raw extended query stays deferred below the fold.
      expect(mockPost).toHaveBeenCalledTimes(1)
      expect(triggerIntersect).not.toBeNull()

      // Scrolling the bottom grid into view triggers the extended query and renders its tables.
      triggerIntersect!()
      await waitFor(() => {
        expect(mockPost).toHaveBeenCalledWith(
          '/api/cmcd/aggregates',
          expect.objectContaining({
            body: expect.objectContaining({ sections: ['bitrate_ladder', 'stream_type_dist'] }),
          }),
        )
      })
      expect(await screen.findByText('2.5–5 Mbps (1080p)')).toBeInTheDocument()
      expect(screen.getByText('Live')).toBeInTheDocument()
    } finally {
      vi.unstubAllGlobals()
    }
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
      const results = calculate({ ...DEFAULTS, rumBeaconsDay: 10000, rumRetentionDays: 90 })
      expect(results.rumBeaconsMonth).toBe(300000)
      const expected30dRumGbMonths = (10000 * 30 * RUM_COMPRESSED_BYTES_PER_BEACON) / 1e9
      // RUM storage follows the RUM retention: 90 days is three times the 30-day monthly volume.
      expect(results.rumParquetGBMonths).toBeCloseTo(expected30dRumGbMonths * 3, 5)
      const rumTier = results.storageTiers.find(t => t.label === 'RUM data')
      const rumArchiveTier = results.storageTiers.find(t => t.label === 'RUM archive')
      expect(rumTier).toBeDefined()
      expect(rumArchiveTier).toBeDefined()
      expect(rumTier?.gbMonths).toBeCloseTo(expected30dRumGbMonths * 3, 5)
      expect(results.costRum).toBeCloseTo(0.75 + (expected30dRumGbMonths * 3 + (rumArchiveTier?.gbMonths ?? 0)) * 0.02, 5)
      // A shorter RUM retention shrinks RUM storage but not the request archive.
      const short = calculate({ ...DEFAULTS, rumBeaconsDay: 10000, rumRetentionDays: 30 })
      expect(short.rumParquetGBMonths).toBeCloseTo(expected30dRumGbMonths, 5)
      expect(short.storageTiers.find(t => t.label === 'Archive Parquet')?.gbMonths)
        .toBeCloseTo(results.storageTiers.find(t => t.label === 'Archive Parquet')?.gbMonths ?? 0, 6)
    })

    test('models ZSTD raw log compression (5x) for smaller compressed volume, raw storage, and CDN egress', () => {
      expect(RAW_LOG_COMPRESSION_RATIO).toBe(5)
      const results = calculate(DEFAULTS)
      expect(results.totalGzBytesPerMonth).toBeCloseTo(results.totalBytesPerMonth / RAW_LOG_COMPRESSION_RATIO, 2)
      expect(results.cdnEgressGB).toBeCloseTo(results.totalGzBytesPerMonth / 1e9, 6)
      // PREFILL keeps canonical DEFAULTS.bytesPerLine (1320) unless estimated_bytes_per_line is provided
      const prefilledDefault = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ avg_log_file_size_kb: 50, requests_per_day: 1_000_000, log_period_seconds: 60 }),
      })
      expect(prefilledDefault.bytesPerLine).toBe(1320)
      const prefilledMeasured = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ estimated_bytes_per_line: 2450, avg_log_file_size_kb: 50 }),
      })
      expect(prefilledMeasured.bytesPerLine).toBe(2450)
    })

    test('sizes the Archive Parquet tier from snapshot_interval_minutes and batches busy services', () => {
      const defaults = calculate(DEFAULTS)
      const archive = defaults.storageTiers.find(t => t.label === 'Archive Parquet')
      expect(archive?.gbMonths).toBeGreaterThan(0)
      // Defaults: a 10 s period gives 8640 log files/day; 5m (300 s) snapshot window batches 30 objects/page
      // (288 pages/day), each costing 3 reads on top of one read per raw file.
      const snapshotSeconds = DEFAULTS.snapshotIntervalMinutes * 60
      const filesPerDay = (86400 / DEFAULTS.logPeriod) * DEFAULTS.logNodes
      const pagesPerDay = Math.ceil(filesPerDay / Math.min(DEFAULTS.archiveBatchObjects, (filesPerDay * snapshotSeconds) / 86400))
      expect(pagesPerDay).toBe(288)
      expect(defaults.classBPerMonth).toBeCloseTo((filesPerDay + pagesPerDay * 5) * 30, 6)
      const busy = calculate({ ...DEFAULTS, logNodes: 60 })
      const busyPeriods = 86400 / DEFAULTS.logPeriod
      const busyReqEffective = (DEFAULTS.edgeOnly ? DEFAULTS.edgeReqDay : DEFAULTS.reqDay) * (DEFAULTS.sampleRate / 100)
      const busyFiles = busyPeriods * 60 * -Math.expm1(-busyReqEffective / busyPeriods / 60)
      const busyPages = Math.ceil(busyFiles / Math.min(DEFAULTS.archiveBatchObjects, (busyFiles * snapshotSeconds) / 86400))
      expect(busy.parquetFilesPerMonth).toBeCloseTo(busyPages * 30, 6)
      expect(busyPages).toBeLessThan(busyFiles)
    })

    test('PREFILL applies snapshot_interval_minutes and longer intervals reduce archive/DR ops while lengthening raw-log steady-state window', () => {
      const prefilled = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ snapshot_interval_minutes: 60 }),
      })
      expect(prefilled.snapshotIntervalMinutes).toBe(60)

      const at5m = calculate({ ...DEFAULTS, snapshotIntervalMinutes: 5, deleteLogs: true })
      const at60m = calculate({ ...DEFAULTS, snapshotIntervalMinutes: 60, deleteLogs: true })
      const at24h = calculate({ ...DEFAULTS, snapshotIntervalMinutes: 1440, deleteLogs: true })

      expect(at5m.drStateSnapshotsPerMonth).toBe(8640)
      expect(at60m.drStateSnapshotsPerMonth).toBe(720)
      expect(at24h.drStateSnapshotsPerMonth).toBe(30)

      expect(at60m.parquetFilesPerMonth).toBeLessThan(at5m.parquetFilesPerMonth)
      expect(at60m.classAPerMonth).toBeLessThan(at5m.classAPerMonth)
      expect(at60m.classBPerMonth).toBeLessThan(at5m.classBPerMonth)

      // Raw logs stay in FOS for snapshotIntervalMinutes + 15m grace when deleteLogs is true
      // At 24h (1440m + 15m = 24.25h) with minDays=0, raw-log steady-state GB-months is > at 5m (20m = 1/3h)
      const noMin5m = calculate({ ...DEFAULTS, snapshotIntervalMinutes: 5, deleteLogs: true, minDays: 0 })
      const noMin24h = calculate({ ...DEFAULTS, snapshotIntervalMinutes: 1440, deleteLogs: true, minDays: 0 })
      const raw5m = noMin5m.storageTiers.find((t) => t.label === 'Raw logs')!.gbMonths
      const raw24h = noMin24h.storageTiers.find((t) => t.label === 'Raw logs')!.gbMonths
      expect(raw24h / raw5m).toBeCloseTo((1440 + 15) / (5 + 15), 5)
    })

    test('PREFILL takes the service retention settings; 0 means keep forever', () => {
      const prefilled = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ log_retention_days: 14, rum_retention_days: 7 }),
      })
      expect([prefilled.retentionDays, prefilled.rumRetentionDays]).toEqual([14, 7])
      const forever = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ log_retention_days: 0, rum_retention_days: 0 }),
      })
      expect([forever.retentionDays, forever.rumRetentionDays]).toEqual([365, 365])
    })

    test('the FOS archive is billed for the retention window plus the one-day margin, per domain', () => {
      const base = { ...DEFAULTS, rumBeaconsDay: 10000, minDays: 1 }
      const archiveGB = (state: typeof base, label: string) =>
        calculate(state).storageTiers.find(t => t.label === label)?.gbMonths ?? 0
      // Request archive scales with the request retention (+1 day) and ignores RUM retention.
      const req30 = archiveGB({ ...base, retentionDays: 30 }, 'Archive Parquet')
      const req7 = archiveGB({ ...base, retentionDays: 7 }, 'Archive Parquet')
      expect(req30 / req7).toBeCloseTo(31 / 8, 6)
      expect(archiveGB({ ...base, retentionDays: 30, rumRetentionDays: 3 }, 'Archive Parquet')).toBeCloseTo(req30, 9)
      // RUM archive scales with the RUM retention (+1 day) and ignores the request retention.
      const rum30 = archiveGB({ ...base, rumRetentionDays: 30 }, 'RUM archive')
      const rum7 = archiveGB({ ...base, rumRetentionDays: 7 }, 'RUM archive')
      expect(rum30).toBeGreaterThan(0)
      expect(rum30 / rum7).toBeCloseTo(31 / 8, 6)
      expect(archiveGB({ ...base, retentionDays: 5, rumRetentionDays: 30 }, 'RUM archive')).toBeCloseTo(rum30, 9)
      // The FOS 30-day minimum floors a short window.
      const floored = calculate({ ...base, minDays: 30, retentionDays: 3 })
      expect(floored.storageTiers.find(t => t.label === 'Archive Parquet')?.flagged).toBe(true)
    })

    test('reducer PREFILL handles rum_beacons_per_day correctly', () => {
      const prefill = makePrefill({
        rum_beacons_per_day: 5000, requests_per_day: 1000000, edge_requests_per_day: 800000,
        log_period_seconds: 60, sample_rate: 100, edge_only: true,
        delete_after: true, log_retention_days: 90,
        class_a_rate_per_1k: 0.005, class_b_rate_per_10k: 0.01, cdn_egress_rate_per_gb: 0.12,
        storage_rate_per_gb_month: 0.02, min_billed_days: 30, avg_nodes_per_flush: 1,
      })
      expect(reducer(DEFAULTS, { type: 'PREFILL', prefill }).rumBeaconsDay).toBe(5000)
    })

    test('Billing Zones (MBZ100, MBZ200, Global) configure correct rates and calculate egress costs', () => {
      const highTraffic = { ...DEFAULTS, reqDay: 20_000_000, edgeReqDay: 20_000_000 }
      expect(highTraffic.billingZone).toBe('MBZ200')
      expect(highTraffic.rateEgress).toBe(0.12)
      const mbz100State = reducer(highTraffic, { type: 'SET_ZONE', zone: 'MBZ100' })
      expect(mbz100State.billingZone).toBe('MBZ100')
      expect(mbz100State.rateEgress).toBe(0.08)
      const globalState = reducer(highTraffic, { type: 'SET_ZONE', zone: 'Global' })
      expect(globalState.billingZone).toBe('Global')
      expect(globalState.rateEgress).toBe(0.18)

      const [resMBZ100, resMBZ200, resGlobal] = [calculate(mbz100State), calculate(highTraffic), calculate(globalState)]
      expect(resMBZ200.cdnEgressGB).toBeGreaterThan(100)
      expect(resMBZ100.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
      expect(resGlobal.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
      expect(resMBZ100.costEgress).toBeCloseTo((resMBZ100.cdnEgressGB - 100) * 0.08, 4)
      expect(resMBZ200.costEgress).toBeCloseTo((resMBZ200.cdnEgressGB - 100) * 0.12, 4)
      expect(resGlobal.costEgress).toBeCloseTo((resGlobal.cdnEgressGB - 100) * 0.18, 4)
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

    it('issues one usage-log read in Cloud mode so readiness never sees a dependent follow-up query', async () => {
      setCloudModeRuntime(true)
      try {
        mockGet.mockImplementation((url: string) =>
          Promise.resolve({
            data: url === '/api/admin/usage-log' ? SAMPLE_USAGE_LOG : SAMPLE_ACCOUNTING,
            response: new Response(null, { status: 200 }),
          }),
        )
        renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)
        await waitFor(() => expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument())
        await new Promise((resolve) => setTimeout(resolve, 20))
        const usageCalls = mockGet.mock.calls.filter(([url]) => url === '/api/admin/usage-log')
        expect(usageCalls).toHaveLength(1)
        expect(usageCalls[0][1]).toEqual(expect.objectContaining({ params: { query: expect.objectContaining({ page_size: 500 }) } }))
      } finally {
        setCloudModeRuntime(false)
      }
    })

    it('shows a failed usage-log read as an error instead of an empty ledger', async () => {
      mockGet.mockImplementation((url: string) =>
        url === '/api/admin/usage-log'
          ? Promise.reject(new Error('Cloud usage history is incomplete'))
          : Promise.resolve({ data: SAMPLE_ACCOUNTING, response: new Response(null, { status: 200 }) }),
      )
      renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)
      const alert = await screen.findByTestId('admin-usage-log-error')
      expect(alert).toHaveAttribute('role', 'alert')
      expect(alert).toHaveTextContent('Cloud usage history is incomplete')
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

  it('renders ClickHouse Distributed Cluster badge and supports filter controls', () => {
    useFactsMock.mockReturnValue(
      mockFactsState({
        data: {
          rows: [{
            event_id: 'event-emul-1', timestamp: '2026-09-11T00:00:01Z', service_id: 'service-1',
            status: 200, host: 'www.example.com', pop: 'IAD', country: 'US', client_ip: '203.0.113.xxx', url: '/healthz',
          }],
          metadata: baseMetadata,
          next_cursor: null, _is_cached: false,
        },
      }),
    )
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByTestId('high-scale-mode-badge')).toHaveTextContent('ClickHouse Distributed Cluster')
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
        pop: 'LHR', country: 'GB', batchId: 'batch-9',
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
