import { render, waitFor } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
// Covers the fix for the SSR/client anchor-mismatch flash on /performance —
// see dashboard-anchor-snap.test.tsx for the full rationale. Renders the REAL
// ReportLayout (unlike performance.test.tsx's shallow smoke test) so both
// first-paint queries (core + distributions) fire and their request bodies
// can be inspected.
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { quantizeAnchor } from '@/lib/time-window'
import * as apiLib from '@/lib/api'
import React from 'react'

vi.mock('@/stores/serviceStore', () => ({
  useServiceStore: vi.fn((selector) => {
    const state = {
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test Service' }],
      setServices: vi.fn(),
      setInitialized: vi.fn(),
      setActiveServiceId: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
}))

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector) => {
    const state = {
      startTime: '2026-06-28T12:00:00Z',
      endTime: '2026-06-29T12:00:00Z',
      filters: [],
      edgeOnly: false,
      isAutoRange: true,
      hasSyncedExtents: false,
      relativeRange: null,
      compareMode: false,
      autoSetRange: vi.fn(),
      setRange: vi.fn(),
      clearFilters: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
}))

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/performance'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plotly-chart" /> }))
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
vi.mock('@/components/AnalyticsCard', () => ({
  AnalyticsCard: ({ title, children }: any) => (
    <div data-testid="analytics-card">
      <h3>{title}</h3>
      {children}
    </div>
  ),
}))

test('first-paint performance query anchors on the snapped (stale) log extents, not "now"', async () => {
  vi.mocked(apiLib.client.GET).mockResolvedValue({
    data: { fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} },
  } as any)
  vi.mocked(apiLib.client.POST).mockResolvedValue({ data: {} } as any)

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  queryClient.setQueryData(['log-extents', 'test-svc'], {
    earliest_log_at: '2026-06-01T00:00:00Z',
    latest_log_at: '2026-06-29T11:00:00Z',
  })

  render(
    <QueryClientProvider client={queryClient}>
      <PerformanceClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    const calls = vi
      .mocked(apiLib.client.POST)
      .mock.calls.filter(([url]: any[]) => String(url).includes('/api/performance/aggregates'))
    expect(calls.length).toBe(1)
  })

  const calls = vi
    .mocked(apiLib.client.POST)
    .mock.calls.filter(([url]: any[]) => String(url).includes('/api/performance/aggregates'))
  const expectedAnchor = quantizeAnchor('2026-06-29T11:00:00Z')
  for (const call of calls) {
    expect((call as any)[1].body.anchor).toBe(expectedAnchor)
  }
})
