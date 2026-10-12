import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// app/origin/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <OriginClient />; RTL can't render an async component, so
// the smoke test targets the client component that owns the title + sections.
import OriginClient from '@/app/origin/_sections/OriginClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/origin'))

const mockPost = vi.fn()

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: (...args: any[]) => mockPost(...args), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
vi.mock('@/hooks/useActiveLogFields', () => ({
  useActiveLogFields: () => ({
    isFieldActive: vi.fn(() => true),
    activeFields: new Set(['ottfb', 'pop', 'oip']),
  }),
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 3600,
    config: { effectiveInterval: '5 minutes', interval: '5 minutes', validTrends: new Set(['off', 'auto']) },
    setChartInterval: vi.fn(),
    timezone: 'UTC',
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  mockPost.mockReset()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

const MOCK_ORIGIN_DATA = {
  has_data: true,
  summary: {
    total_misses: 450,
    total_passes: 50,
    ottfb_p50_ms: 45.2,
    ottfb_p75_ms: 60.1,
    ottfb_p95_ms: 120.5,
    ottfb_p99_ms: 250.0,
    ottlb_p50_ms: 65.0,
    ottlb_p95_ms: 160.0,
    cdn_overhead_p50_ms: 5.2,
    origin_error_rate: 0.005,
    obytes_p50: 1024,
    _approx: false,
  },
  timeseries: {
    has_data: true,
    series: [
      { time: '2026-01-01T00:00:00Z', value: 45.2 },
      { time: '2026-01-01T00:05:00Z', value: 48.1 },
    ],
    _approx: false,
  },
  slow_urls: {
    has_data: true,
    rows: [
      { url: '/api/v1/slow', requests: 120, p50_ms: 150.0, p95_ms: 320.0, p99_ms: 500.0 },
    ],
    _approx: false,
  },
  status_codes: {
    has_data: true,
    rows: [
      { status: 200, count: 480 },
      { status: 500, count: 20 },
    ],
  },
  path_breakdown: {
    has_data: true,
    shielding_detected: true,
    rows: [],
  },
  pop_latency: {
    has_data: true,
    rows: [
      { pop: 'SJC', requests: 300, p50_ms: 40.0, p95_ms: 110.0, elevated: false },
    ],
    _approx: false,
  },
  ip_health: {
    has_data: true,
    rows: [
      { oip: '203.0.113.1', requests: 500, p50_ms: 45.0, p95_ms: 120.0, error_pct: 0.5 },
    ],
    _approx: false,
  },
}

test('origin page mounts and renders title and all origin card titles with loaded data', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ORIGIN_DATA })

  render(
    <QueryClientProvider client={queryClient}>
      <OriginClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Origin Performance')).toBeInTheDocument()
  expect(await screen.findByText('Origin TTFB (P50)')).toBeInTheDocument()
  expect(screen.getByText('Origin TTFB (P95)')).toBeInTheDocument()
  expect(screen.getByText('Origin Error Rate')).toBeInTheDocument()
  expect(screen.getByText('Fetch Volume')).toBeInTheDocument()
  expect(screen.getByText('Origin Latency')).toBeInTheDocument()
  expect(screen.getByText('Status Code Distribution')).toBeInTheDocument()
  expect(screen.getByText('Slowest URLs at Origin')).toBeInTheDocument()
  expect(screen.getByText('Origin Performance by POP')).toBeInTheDocument()
  expect(screen.getByText('Origin IP Health')).toBeInTheDocument()
  expect(screen.getByText('Origin Payload Size')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('origin page displays contextual loading messages while query is pending', () => {
  // Never-resolving promise simulates active loading state
  mockPost.mockReturnValue(new Promise(() => {}))

  render(
    <QueryClientProvider client={queryClient}>
      <OriginClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Loading Origin TTFB (P50)...')).toBeInTheDocument()
  expect(screen.getByText('Loading Origin TTFB (P95)...')).toBeInTheDocument()
  expect(screen.getByText('Loading Origin Error Rate...')).toBeInTheDocument()
  expect(screen.getByText('Loading Fetch Volume...')).toBeInTheDocument()
  expect(screen.getByText('Loading origin latency timeseries...')).toBeInTheDocument()
  expect(screen.getByText('Loading origin status code breakdown...')).toBeInTheDocument()
  expect(screen.getByText('Loading slowest URLs at origin...')).toBeInTheDocument()
  expect(screen.getByText('Loading origin performance by POP...')).toBeInTheDocument()
  expect(screen.getByText('Loading origin IP health...')).toBeInTheDocument()
  expect(screen.getByText('Loading origin payload size...')).toBeInTheDocument()
})

test('origin page displays empty states when has_data is false', async () => {
  mockPost.mockResolvedValue({
    data: {
      has_data: false,
      summary: { has_data: false },
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <OriginClient />
    </QueryClientProvider>,
  )

  expect(await screen.findByText('No Origin Metrics Yet')).toBeInTheDocument()
})
