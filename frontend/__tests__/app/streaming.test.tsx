import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'
import { CMCD_FIXTURE } from '../helpers/msw/handlers'
import { client } from '@/lib/api'

// Render-smoke + contract test for the CMCD streaming-analytics page.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/streaming'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn(), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: () => <div data-testid="plotly-chart" />,
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
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  vi.mocked(client.POST).mockReset()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

function renderPage() {
  return render(
    <QueryClientProvider client={queryClient}>
      <StreamingClient />
    </QueryClientProvider>,
  )
}

test('requests /api/cmcd/aggregates with the page window and renders the headline cards', async () => {
  vi.mocked(client.POST).mockResolvedValue({ data: CMCD_FIXTURE } as never)
  renderPage()

  expect(await screen.findByText('Total Sessions')).toBeInTheDocument()
  expect(screen.getByText('Peak Viewers')).toBeInTheDocument()
  expect(screen.getByText('Rebuffer Rate')).toBeInTheDocument()
  await waitFor(() => expect(screen.getByText('1,234')).toBeInTheDocument())
  expect(screen.getByText('4.2%')).toBeInTheDocument()

  expect(client.POST).toHaveBeenCalledWith(
    '/api/cmcd/aggregates',
    expect.objectContaining({
      body: expect.objectContaining({ bucket_seconds: 300, top_n: 30, filters: {} }),
    }),
  )
  expect(errorSpy).not.toHaveBeenCalled()
})

test('shows the CMCD-not-enabled state when the service has no CMCD data', async () => {
  vi.mocked(client.POST).mockResolvedValue({ data: { available: false, has_data: false } } as never)
  renderPage()

  expect(await screen.findByText('CMCD data not available')).toBeInTheDocument()
  expect(screen.getByText(/CMCD collection is not enabled for this service/)).toBeInTheDocument()
  expect(screen.queryByText('Total Sessions')).not.toBeInTheDocument()
})

test('a failed query leaves the page up with em-dash values instead of crashing', async () => {
  vi.mocked(client.POST).mockResolvedValue({ error: { detail: 'boom' } } as never)
  renderPage()

  expect(await screen.findByText('Total Sessions')).toBeInTheDocument()
  await waitFor(() => expect(client.POST).toHaveBeenCalled())
  expect(screen.getAllByText('—').length).toBeGreaterThan(0)
})
