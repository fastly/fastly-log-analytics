import { screen, waitFor } from '@testing-library/react'
import { describe, expect, test, vi, beforeEach, afterEach } from 'vitest'
import UsagePage from '@/app/usage/page'
import SessionsPage from '@/app/sessions/page'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { DEFAULTS, reducer, calculate, type PrefillResponse } from '@/components/CostCalculator/calc'
import { createTestQueryClient, renderWithQuery } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'
import { CMCD_FIXTURE } from '../helpers/msw/handlers'

// Consolidated render-smoke + contract tests for the Usage, Sessions, and Streaming pages.

vi.mock('@/stores/serviceStore', async () =>
  (await import('../helpers/page-smoke')).serviceStoreModuleMock({ accessLevel: 'read_write' }),
)

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/usage'))

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('@/hooks/useIsDataReady', () => ({
  useIsDataReady: vi.fn(() => true),
}))

const mockGet = vi.fn()
const mockPost = vi.fn()

vi.mock('@/lib/api', () => ({
  client: {
    GET: (...args: unknown[]) => mockGet(...args),
    POST: (...args: unknown[]) => mockPost(...args),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/CostCalculator/CostCalculator', () => ({
  CostCalculator: () => <div data-testid="cost-calculator" />,
}))
vi.mock('@/app/sessions/_sections/ScoringControls', () => ({
  ScoringControls: () => <div data-testid="scoring-controls" />,
}))
vi.mock('@/app/sessions/_sections/SessionsTable', () => ({
  SessionsTable: () => <div data-testid="sessions-table" />,
}))

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
  queryClient.clear()
  mockGet.mockReset()
  mockPost.mockReset()
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
