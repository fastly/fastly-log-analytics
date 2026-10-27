import { fireEvent, render, renderHook, screen, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
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
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import { server, CMCD_FIXTURE } from '@/__tests__/helpers/msw/handlers'
import {
  createTestQueryClient, makeQueryWrapper, renderWithQuery,
  spyOnConsoleError, stubResizeObserver, stubPointerCaptureAndScroll,
  createUseSSEMock, type ReportLayoutChildArgs, type InteractiveDataTableMockProps,
} from '../helpers/page-smoke'
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


// ── Usage, Sessions, Streaming & CostCalculator tests ────────────────────────

function makePrefill(overrides: Partial<PrefillResponse>): PrefillResponse {
  return {
    _is_cached: false, data_days: 7, commit_interval_mins: 5, sample_rate: 100, edge_only: false,
    compaction_enabled: true, delete_after: false, log_retention_days: 90, rum_retention_days: 90, deployment_mode: 'standard', billing_zone: null, ...overrides,
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

  test('storage breakdown itemizes cloud-copy deletions and expired archive inside Deleted (Still Billed)', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/usage/current-storage') {
        return {
          data: {
            live_bytes: 0, live_files: 0, quarantine_bytes: 0, total_billed_bytes: 0, total_billed_gb_hours: 0,
            total_files: 0, total_bytes: 0, start: '', end: '',
            deleted_bytes: 3 * 1024 ** 3, cloud_copy_deleted_bytes: 2 * 1024 ** 3, archive_expired_bytes: 1024 ** 3,
          },
        } as never
      }
      return undefined
    })
    renderWithQuery(<UsagePage />, { queryClient })
    expect(await screen.findByText('of which cloud-copy and compaction deletions:')).toBeInTheDocument()
    expect(screen.getByText('of which expired High-Scale archive:')).toBeInTheDocument()
    expect(screen.getByText('2 GB')).toBeInTheDocument()
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
      // RUM storage follows the RUM retention: 90 days is three times the 30-day monthly volume.
      expect(results.rumParquetGBMonths).toBeCloseTo(0.071525 * 3, 4)
      expect(results.costRum).toBeCloseTo(1.5 + 0.071525 * 3 * 0.02, 4)
      const rumTier = results.storageTiers.find(t => t.label === 'RUM data')
      expect(rumTier).toBeDefined()
      expect(rumTier?.gbMonths).toBeCloseTo(0.071525 * 3, 4)
      // A shorter RUM retention shrinks RUM storage but not the request data.
      const short = calculate({ ...DEFAULTS, rumBeaconsDay: 10000, rumRetentionDays: 30 })
      expect(short.rumParquetGBMonths).toBeCloseTo(0.071525, 4)
      expect(short.storageTiers.find(t => t.label === 'DuckLake data')?.gbMonths)
        .toBeCloseTo(results.storageTiers.find(t => t.label === 'DuckLake data')?.gbMonths ?? 0, 6)
    })

    test('High-Scale adds the archive Parquet tier sized from the measured per-artifact and per-row bytes', () => {
      const standard = calculate(DEFAULTS)
      const highScale = calculate({ ...DEFAULTS, deploymentMode: 'high_scale' })
      expect(standard.storageTiers.find(t => t.label === 'High-Scale archive')).toBeUndefined()
      const archive = highScale.storageTiers.find(t => t.label === 'High-Scale archive')
      expect(archive?.gbMonths).toBeGreaterThan(0)
      expect(highScale.totalGBStored).toBeCloseTo(standard.totalGBStored + (archive?.gbMonths ?? 0), 6)
      // Defaults: 1440 log files/day at ~0.33 objects per tick means one object per page, so 3 writes and 3 reads per file.
      const filesPerMonth = (86400 / DEFAULTS.logPeriod) * DEFAULTS.logNodes * 30
      expect(highScale.classAPerMonth - standard.classAPerMonth).toBeCloseTo(filesPerMonth * 3, 6)
      expect(highScale.classBPerMonth - standard.classBPerMonth).toBeCloseTo(filesPerMonth * 3, 6)
      const busy = calculate({ ...DEFAULTS, deploymentMode: 'high_scale', logNodes: 60 })
      const busyFiles = (86400 / DEFAULTS.logPeriod) * 60
      const busyPages = Math.ceil(busyFiles / Math.min(DEFAULTS.archiveBatchObjects, (busyFiles * 20) / 86400))
      expect(busy.classAPerMonth - calculate({ ...DEFAULTS, logNodes: 60 }).classAPerMonth).toBeCloseTo(busyPages * 30 * 3, 6)
      expect(busyPages).toBeLessThan(busyFiles)
    })

    test('PREFILL takes the service retention settings and deployment mode; 0 means keep forever', () => {
      const prefilled = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ log_retention_days: 14, rum_retention_days: 7, deployment_mode: 'high_scale' }),
      })
      expect([prefilled.retentionDays, prefilled.rumRetentionDays, prefilled.deploymentMode]).toEqual([14, 7, 'high_scale'])
      const forever = reducer(DEFAULTS, {
        type: 'PREFILL',
        prefill: makePrefill({ log_retention_days: 0, rum_retention_days: 0, deployment_mode: 'standard' }),
      })
      expect([forever.retentionDays, forever.rumRetentionDays, forever.deploymentMode]).toEqual([365, 365, 'standard'])
    })

    test('the High-Scale archive is billed for the retention window plus the one-day margin, per domain', () => {
      const base = { ...DEFAULTS, deploymentMode: 'high_scale' as const, rumBeaconsDay: 10000, minDays: 1 }
      const archiveGB = (state: typeof base, label: string) =>
        calculate(state).storageTiers.find(t => t.label === label)?.gbMonths ?? 0
      // Request archive scales with the request retention (+1 day) and ignores RUM retention.
      const req30 = archiveGB({ ...base, retentionDays: 30 }, 'High-Scale archive')
      const req7 = archiveGB({ ...base, retentionDays: 7 }, 'High-Scale archive')
      expect(req30 / req7).toBeCloseTo(31 / 8, 6)
      expect(archiveGB({ ...base, retentionDays: 30, rumRetentionDays: 3 }, 'High-Scale archive')).toBeCloseTo(req30, 9)
      // RUM archive scales with the RUM retention (+1 day) and ignores the request retention.
      const rum30 = archiveGB({ ...base, rumRetentionDays: 30 }, 'High-Scale RUM archive')
      const rum7 = archiveGB({ ...base, rumRetentionDays: 7 }, 'High-Scale RUM archive')
      expect(rum30).toBeGreaterThan(0)
      expect(rum30 / rum7).toBeCloseTo(31 / 8, 6)
      expect(archiveGB({ ...base, retentionDays: 5, rumRetentionDays: 30 }, 'High-Scale RUM archive')).toBeCloseTo(rum30, 9)
      // The FOS 30-day minimum floors a short window.
      const floored = calculate({ ...base, minDays: 30, retentionDays: 3 })
      expect(floored.storageTiers.find(t => t.label === 'High-Scale archive')?.flagged).toBe(true)
      // Standard keeps no archive and no RUM archive.
      const standard = calculate({ ...DEFAULTS, rumBeaconsDay: 10000 })
      expect(standard.storageTiers.some(t => t.label.includes('archive'))).toBe(false)
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
