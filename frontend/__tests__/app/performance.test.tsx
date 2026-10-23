import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// app/performance/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <PerformanceClient />; RTL can't render an async component, so
// the smoke test targets the client component that owns the title + sections.
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6 (testing_suite_audit_2026-06-14.md): smoke & contract tests — verify
// the page mounts, renders all 5 cards, contextual loading messages, empty states,
// and the approximate badge.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/performance'))

type ApiCall = (url: string, opts?: unknown) => Promise<{ data: unknown }>
const mockPost = vi.fn<ApiCall>()

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: (url: string, opts?: unknown) => mockPost(url, opts), use: vi.fn() },
  extractApiError: vi.fn((e: unknown) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 3600,
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

test('performance page mounts and renders title and all 5 card titles with loaded data', async () => {
  mockPost.mockResolvedValue({
    data: {
      waterfall: {
        avg: {
          edge_processing: 10.5,
          origin_wait: 45.2,
          origin_download: 15.0,
          client_download: 8.3,
        },
      },
      top_urls: [
        { url: '/api/v1/test', requests: 120, avg: 55.4, p50: 42.1, p95: 98.0, p99: 145.2 },
      ],
      top_asns: [
        { asn: 15169, label: 'AS15169 Google', requests: 350, avg: 38.2, p50: 32.0, p95: 65.4, p99: 89.1 },
      ],
      ttl_dist: [
        { bucket: '<1m', count: 45 },
        { bucket: '<1h', count: 180 },
      ],
      scatter: [
        { origin: 45.2, edge: 12.1, cache: 'HIT' },
      ],
      _approx: false,
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <PerformanceClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Performance')).toBeInTheDocument()
  expect(await screen.findByText('End-to-End Latency Waterfall (Average)')).toBeInTheDocument()
  expect(screen.getByText('Slowest URLs')).toBeInTheDocument()
  expect(screen.getByText('Slowest Networks')).toBeInTheDocument()
  expect(screen.getByText('Cache TTL Distribution')).toBeInTheDocument()
  expect(screen.getByText('Origin vs Edge Processing (ms)')).toBeInTheDocument()
  expect(screen.queryByText('Approximate')).not.toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('performance page displays contextual loading messages while query is pending', () => {
  // Never-resolving promise simulates active loading state
  mockPost.mockReturnValue(new Promise(() => {}))

  render(
    <QueryClientProvider client={queryClient}>
      <PerformanceClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Calculating latency percentiles...')).toBeInTheDocument()
  expect(screen.getByText('Profiling endpoint latency...')).toBeInTheDocument()
  expect(screen.getByText('Profiling network latency...')).toBeInTheDocument()
  expect(screen.getByText('Profiling cache TTL distribution...')).toBeInTheDocument()
  expect(screen.getByText('Profiling origin vs edge latency...')).toBeInTheDocument()
})

test('performance page displays empty states and approximate badge appropriately', async () => {
  mockPost.mockResolvedValue({
    data: {
      waterfall: {},
      top_urls: [],
      top_asns: [],
      ttl_dist: [],
      scatter: [],
      _approx: true,
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <PerformanceClient />
    </QueryClientProvider>,
  )

  // Empty state across cards
  const emptyBadges = await screen.findAllByText('No data for this filter')
  expect(emptyBadges.length).toBeGreaterThanOrEqual(1)

  // Approximate badge is visible when _approx: true
  expect(screen.getByText('Approximate')).toBeInTheDocument()
})

class Boundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null }
  static getDerivedStateFromError(error: Error) {
    return { error }
  }
  render() {
    if (this.state.error) {
      return <div data-testid="route-crashed">{this.state.error.message}</div>
    }
    return this.props.children
  }
}

test('UX-2: /performance survives a dirty scatter row with cache=null', async () => {
  mockPost.mockImplementation(async (_path: string, opts?: unknown) => {
    const sections: string[] = (opts as { body?: { sections?: string[] } })?.body?.sections ?? []
    if (sections.includes('scatter')) {
      return {
        data: {
          available: true,
          scatter: [{ cache: null, origin: 10, edge: 5 }],
          waterfall: { avg: { edge_processing: 1, origin_wait: 2 } },
          top_urls: [],
          top_asns: [],
        },
      }
    }
    return { data: { available: true, ttl_dist: [] } }
  })

  render(
    <QueryClientProvider client={queryClient}>
      <Boundary>
        <PerformanceClient />
      </Boundary>
    </QueryClientProvider>,
  )
  await screen.findByText('End-to-End Latency Waterfall (Average)')
  expect(screen.queryByTestId('route-crashed')).toBeNull()
})
