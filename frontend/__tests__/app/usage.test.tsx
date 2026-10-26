import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
import UsageLogClient from '@/app/admin/usage-log/_sections/UsageLogClient'
import Loading from '@/app/admin/usage-log/loading'
import UsagePage from '@/app/usage/page'
import SessionsPage from '@/app/sessions/page'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { DEFAULTS, reducer, calculate, type PrefillResponse } from '@/components/CostCalculator/CostCalculator'
import { createTestQueryClient, renderWithQuery } from '../helpers/page-smoke'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'
import { CMCD_FIXTURE } from '../helpers/msw/handlers'
import { useServiceStore } from '@/stores/filterStore'

// Consolidated render-smoke + contract tests for the Usage, Sessions, and Streaming pages.

vi.mock('@/stores/filterStore', async () =>
  (await import('../helpers/page-smoke')).serviceStoreModuleMock({ accessLevel: 'read_write' }),
)

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/usage'))

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsDataReady: vi.fn(() => true),
}))

const mockGet = vi.fn()
const mockPost = vi.fn()
const mockDelete = vi.fn()

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    GET: (...args: unknown[]) => mockGet(...args),
    POST: (...args: unknown[]) => mockPost(...args),
    DELETE: (...args: unknown[]) => mockDelete(...args),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/CostCalculator/CostCalculator', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/CostCalculator/CostCalculator')>()),
  CostCalculator: () => <div data-testid="cost-calculator" />,
}))
let useMockDataTable = true
vi.mock('@/components/DataTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/DataTable')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    DataTable: (props: any) => (useMockDataTable ? <div data-testid="sessions-table" /> : <actual.DataTable {...props} />),
  }
})

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 300,
    config: { effectiveInterval: '5 minutes', interval: '5 minutes' },
    setChartInterval: vi.fn(),
    timezone: 'UTC',
    data: { rows: [], total: 0 },
    isLoading: false,
    isFetching: false,
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  useMockDataTable = true
  ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = 'test-svc'
  queryClient.clear()
  mockGet.mockReset()
  mockPost.mockReset()
  mockDelete.mockReset()
  mockPost.mockResolvedValue({ data: { rows: [] } })
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
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
  await waitFor(() => {
    expect(screen.getByText('Failed to load storage usage.')).toBeInTheDocument()
  })
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
    expect.objectContaining({
      body: expect.objectContaining({ bucket_seconds: 300, top_n: 30, filters: {} }),
    }),
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

function makePrefill(overrides: Partial<PrefillResponse>): PrefillResponse {
  return {
    _is_cached: false,
    data_days: 7,
    commit_interval_mins: 5,
    sample_rate: 100,
    edge_only: false,
    compaction_enabled: true,
    delete_after: false,
    log_retention_days: 90,
    billing_zone: null,
    ...overrides,
  }
}

describe('CostCalculator calc logic with RUM', () => {
  test('returns correct calculations for DEFAULTS', () => {
    const results = calculate(DEFAULTS)
    expect(results.totalCost).toBeGreaterThan(0)
    expect(results.rumBeaconsMonth).toBe(0)
    expect(results.rumParquetGBMonths).toBe(0)
    expect(results.costRum).toBe(0)
  })

  test('correctly calculates RUM beacon costs and storage when rumBeaconsDay is set', () => {
    const state = {
      ...DEFAULTS,
      rumBeaconsDay: 10000 // 10k beacons per day
    }
    const results = calculate(state)

    // 10k * 30 = 300,000 beacons per month
    expect(results.rumBeaconsMonth).toBe(300000)

    // RUM storage in GB-months:
    // 300,000 beacons * 1024 bytes (1 KB) = 307,200,000 bytes uncompressed
    // Compressed 4:1 to parquet = 76,800,000 bytes
    // 76,800,000 / (1024 * 1024 * 1024) = 0.071525 GB-months
    expect(results.rumParquetGBMonths).toBeCloseTo(0.071525, 4)

    // RUM Cost portion:
    // Class A cost: (300,000 / 1000) * 0.005 = $1.50
    // Storage cost: 0.0715255 * 0.02 = $0.00143
    // Total RUM portion: $1.50143
    expect(results.costRum).toBeCloseTo(1.50143, 4)

    // Verify RUM data storage is in storageTiers
    const rumTier = results.storageTiers.find(t => t.label === 'RUM data')
    expect(rumTier).toBeDefined()
    expect(rumTier?.gbMonths).toBeCloseTo(0.071525, 4)
  })

  test('reducer PREFILL handles rum_beacons_per_day correctly', () => {
    const prefill = makePrefill({
      rum_beacons_per_day: 5000,
      requests_per_day: 1000000,
      edge_requests_per_day: 800000,
      log_period_seconds: 60,
      commit_interval_mins: 5,
      sample_rate: 100,
      edge_only: true,
      compaction_enabled: true,
      delete_after: true,
      log_retention_days: 90,
      class_a_rate_per_1k: 0.005,
      class_b_rate_per_10k: 0.01,
      cdn_egress_rate_per_gb: 0.12,
      storage_rate_per_gb_month: 0.02,
      min_billed_days: 30,
      avg_nodes_per_flush: 1,
    })
    const nextState = reducer(DEFAULTS, { type: 'PREFILL', prefill })
    expect(nextState.rumBeaconsDay).toBe(5000)
  })

  test('Billing Zones (MBZ100, MBZ200, Global) configure correct rates and calculate egress costs', () => {
    // 1. Default is MBZ200 ($0.12/GB)
    expect(DEFAULTS.billingZone).toBe('MBZ200')
    expect(DEFAULTS.rateEgress).toBe(0.12)

    // 2. Switch to MBZ100 ($0.08/GB)
    const mbz100State = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'MBZ100' })
    expect(mbz100State.billingZone).toBe('MBZ100')
    expect(mbz100State.rateEgress).toBe(0.08)

    // 3. Switch to Global ($0.18/GB)
    const globalState = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'Global' })
    expect(globalState.billingZone).toBe('Global')
    expect(globalState.rateEgress).toBe(0.18)

    // 4. Verify calculation differences across zones for identical traffic
    const resMBZ100 = calculate(mbz100State)
    const resMBZ200 = calculate(DEFAULTS)
    const resGlobal = calculate(globalState)

    expect(resMBZ100.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
    expect(resGlobal.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)

    expect(resMBZ100.costEgress).toBeCloseTo(resMBZ100.cdnEgressGB * 0.08, 4)
    expect(resMBZ200.costEgress).toBeCloseTo(resMBZ200.cdnEgressGB * 0.12, 4)
    expect(resGlobal.costEgress).toBeCloseTo(resGlobal.cdnEgressGB * 0.18, 4)

    expect(resMBZ100.costEgress).toBeLessThan(resMBZ200.costEgress)
    expect(resMBZ200.costEgress).toBeLessThan(resGlobal.costEgress)
  })

  test('PREFILL respects billing_zone and maps corresponding rate', () => {
    const prefill = makePrefill({
      billing_zone: 'MBZ100',
      cdn_egress_rate_per_gb: 0.08,
    })
    const state = reducer(DEFAULTS, { type: 'PREFILL', prefill })
    expect(state.billingZone).toBe('MBZ100')
    expect(state.rateEgress).toBe(0.08)
  })
})

const SAMPLE_USAGE_LOG = {
  service_id: 'svc-alpha',
  total: 2,
  aggregate: {
    total_class_a: 1200,
    total_class_b: 45000,
    total_cdn_downloads: 18,
    total_cdn_bytes: 1073741824,
    total_fos_bytes: 5242880,
    estimated_cost_class_a: 0.006,
    estimated_cost_class_b: 0.045,
    estimated_cost_cdn: 0.12,
    estimated_cost_total: 0.171,
    class_a_breakdown: { PutObject: 800, ListObjectsV2: 400 },
    class_b_breakdown: { GetObject: 45000 },
  },
  entries: [
    {
      id: 1,
      timestamp: '2026-06-01T12:00:00Z',
      operation_class: 'A',
      operation_type: 'PutObject',
      url: 's3://fos-bucket/lake/part-001.parquet',
      bytes: 2048,
      duration_ms: 14.2,
      function_name: 'commit_files',
      process_context: 'cron:commit:svc-alpha',
      status: 'OK',
      estimated_cost: 0.000005,
      count: 1,
    },
    {
      id: 2,
      timestamp: '2026-06-01T12:01:00Z',
      operation_class: 'B',
      operation_type: 'GetObject',
      url: 's3://fos-bucket/raw/request/batch.gz',
      bytes: 4096,
      duration_ms: 9.1,
      function_name: 'download_batch',
      process_context: 'cron:sync:svc-alpha',
      status: 'OK',
      estimated_cost: 0.000001,
      count: 1,
    },
  ],
}

const SAMPLE_ACCOUNTING = {
  by: 'hour',
  from_ts: '2026-05-31T12:00:00Z',
  to_ts: '2026-06-01T12:00:00Z',
  buckets: [
    {
      ts: '2026-06-01T11:00:00Z',
      fastly_requests: 5000,
      our_rows: 5000,
      file_count: 12,
      gap: 0,
      gap_pct: 0,
    },
  ],
  totals: {
    fastly_requests: 5000,
    our_rows: 5000,
    gap: 0,
    gap_pct: 0,
    worst_bucket_ts: null,
    worst_bucket_gap_pct: null,
  },
  sustained_loss: null,
  catchup: {
    latest_ingest_ts: '2026-06-01T12:00:00Z',
    lag_seconds: 12,
    status: 'caught_up',
  },
}

describe('/admin/usage-log Page & Client', () => {
  beforeEach(() => {
    useMockDataTable = false
    ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = null
    vi.clearAllMocks()
  })

  it('renders loading.tsx with geometry-matched zero-CLS reservation containers', () => {
    render(<Loading />)

    expect(screen.getByTestId('admin-usage-log-loading')).toBeInTheDocument()
    const toolbar = screen.getByTestId('admin-usage-log-toolbar')
    expect(toolbar.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(toolbar.className).toContain('[content-visibility:auto]')

    const kpi = screen.getByTestId('admin-usage-log-kpi-reservation')
    expect(kpi.className).toContain('[contain-intrinsic-size:auto_164px]')
    expect(kpi.className).toContain('[content-visibility:auto]')

    const chart = screen.getByTestId('admin-usage-log-chart-reservation')
    expect(chart.className).toContain('[contain-intrinsic-size:auto_380px]')
    expect(chart.className).toContain('[content-visibility:auto]')

    const table = screen.getByTestId('admin-usage-log-table-reservation')
    expect(table.className).toContain('[contain-intrinsic-size:auto_520px]')
    expect(table.className).toContain('[content-visibility:auto]')
    expect(screen.getByText('Loading FOS usage ledger...')).toBeInTheDocument()
  })

  it('renders pending skeleton reservations before usage-log data resolves', () => {
    mockGet.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    expect(screen.getByTestId('admin-usage-log-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-usage-log-kpi-reservation').className).toContain(
      '[contain-intrinsic-size:auto_164px]',
    )
    expect(screen.getByTestId('admin-usage-log-chart-reservation').className).toContain(
      '[contain-intrinsic-size:auto_380px]',
    )
    expect(screen.getByTestId('admin-usage-log-table-reservation').className).toContain(
      '[contain-intrinsic-size:auto_520px]',
    )
    expect(screen.getByText('Loading log accounting…')).toBeInTheDocument()
  })

  it('renders aggregate KPIs, ingest accounting, and usage log rows', async () => {
    mockGet.mockImplementation((url: string) => {
      if (url === '/api/admin/usage-log') {
        return Promise.resolve({
          data: SAMPLE_USAGE_LOG,
          response: new Response(null, { status: 200 }),
        })
      }
      if (url === '/api/admin/log-accounting') {
        return Promise.resolve({
          data: SAMPLE_ACCOUNTING,
          response: new Response(null, { status: 200 }),
        })
      }
      return Promise.resolve({ data: {}, response: new Response(null, { status: 200 }) })
    })

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    await waitFor(() => {
      expect(screen.getByText('1.2K')).toBeInTheDocument()
    })

    expect(screen.getByText('45.0K')).toBeInTheDocument()
    expect(screen.getByText('$0.17')).toBeInTheDocument()
    expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument()
    expect(screen.getByText('cron:commit:svc-alpha')).toBeInTheDocument()
    expect(screen.getByText('Caught up')).toBeInTheDocument()
  })

  it('switches time-window presets, filters by process/operation, refreshes, and purges logs', async () => {
    mockGet.mockImplementation((url: string) => {
      if (url === '/api/admin/usage-log') {
        return Promise.resolve({
          data: SAMPLE_USAGE_LOG,
          response: new Response(null, { status: 200 }),
        })
      }
      return Promise.resolve({
        data: SAMPLE_ACCOUNTING,
        response: new Response(null, { status: 200 }),
      })
    })
    mockDelete.mockResolvedValue({
      data: { ok: true },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    await waitFor(() => {
      expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument()
    })

    // Preset toggle (24h -> 1h -> 168h)
    expect(screen.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByTestId('admin-usage-log-preset-1h'))
    expect(screen.getByTestId('admin-usage-log-preset-1h')).toHaveAttribute('aria-pressed', 'true')

    // Filter by process_context
    const procInput = screen.getByTestId('admin-usage-log-process-input')
    fireEvent.change(procInput, { target: { value: 'cron:commit' } })

    await waitFor(() => {
      expect(mockGet).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: {
            query: expect.objectContaining({
              process_context: 'cron:commit',
            }),
          },
        }),
      )
    })

    // Filter by operation_type
    const opInput = screen.getByTestId('admin-usage-log-operation-input')
    fireEvent.change(opInput, { target: { value: 'PutObject' } })

    await waitFor(() => {
      expect(mockGet).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: {
            query: expect.objectContaining({
              operation_type: 'PutObject',
            }),
          },
        }),
      )
    })

    // Manual Refresh
    const callsBeforeRefresh = mockGet.mock.calls.length
    const refreshBtn = screen.getByTestId('admin-usage-log-refresh-btn')
    fireEvent.click(refreshBtn)
    await waitFor(() => {
      expect(mockGet.mock.calls.length).toBeGreaterThan(callsBeforeRefresh)
    })

    // Export CSV href is populated after mount
    const exportBtn = screen.getByTestId('admin-usage-log-export-btn')
    expect(exportBtn.getAttribute('href')).toContain('/api/admin/usage-log/export?')
    expect(exportBtn.getAttribute('href')).toContain('process_context=cron%3Acommit')

    // Purge Logs dialog workflow
    fireEvent.click(screen.getByTestId('admin-usage-log-purge-btn'))
    await waitFor(() => {
      expect(screen.getByText('Purge all usage logs?')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('button', { name: /Purge all logs/i }))

    await waitFor(() => {
      expect(mockDelete).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: { query: { service_id: 'svc-alpha' } },
        }),
      )
    })
  })
})
