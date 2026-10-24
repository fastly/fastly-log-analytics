import { screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import UsagePage from '@/app/usage/page'
import SessionsPage from '@/app/sessions/page'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
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
vi.mock('@/components/PlotlyChart/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
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
