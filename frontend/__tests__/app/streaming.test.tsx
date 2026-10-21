import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'
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

const CMCD_PAYLOAD = {
  available: true,
  has_data: true,
  overview: {
    active_sessions: 1234,
    peak_viewers: 321,
    avg_session_duration: 95,
    rebuffer_rate: 4.2,
    avg_bitrate: 3200,
    avg_buffer_length: 14500,
    median_throughput: 28000,
  },
  sessions_ts: [{ bucket: '2026-01-01T00:00:00Z', concurrent_sessions: 40, rebuffer_session_pct: 3.1, new_sessions: 12 }],
  buffer_health_ts: [{ bucket: '2026-01-01T00:00:00Z', p50_buffer: 12000, p95_buffer: 26000, starvation_rate: 2.5 }],
  bitrate_ts: [{ bucket: '2026-01-01T00:00:00Z', avg_bitrate: 3000, utilization_ratio: 0.5 }],
  throughput_ts: [{ bucket: '2026-01-01T00:00:00Z', p50: 28000, p95: 45000, p99: 49000 }],
  startup_ts: [{ bucket: '2026-01-01T00:00:00Z', startup_ratio: 8.5 }],
  top_content: [{ content_id: 'movie-001', session_count: 77, rebuffer_rate: 5, avg_bitrate: 3100, avg_buffer_length: 13000 }],
  rebuffer_by_country: [{ country: 'US', rebuffer_rate: 4, session_count: 600, median_throughput: 30000 }],
  rebuffer_by_asn: [{ asn: 7922, label: 'AS7922 Comcast', rebuffer_rate: 6, session_count: 200, avg_bitrate: 2900, avg_buffer_length: 12000 }],
  object_type_dist: [{ object_type: 'v', request_count: 900 }],
  streaming_format_dist: [{ streaming_format: 'h', session_count: 700 }],
  session_duration_dist: [{ duration_bucket: '1-5 min', session_count: 300 }],
}

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
  vi.mocked(client.POST).mockResolvedValue({ data: CMCD_PAYLOAD } as never)
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
