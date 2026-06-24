import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import DashboardPage from '@/app/dashboard/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
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
      setActiveServiceId: vi.fn()
    }
    return selector ? selector(state) : state
  })
}))

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector) => {
    const state = {
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-01T01:00:00Z',
      filters: [],
      isAutoRange: false,
      hasSyncedExtents: true,
      compareMode: false,
      autoSetRange: vi.fn(),
      setRange: vi.fn(),
      clearFilters: vi.fn()
    }
    return selector ? selector(state) : state
  })
}))

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/dashboard'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plotly-chart">PlotlyChart</div> }))
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div> }))
vi.mock('@/components/Map/ChoroplethMap', () => ({ ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div> }))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

test('renders dashboard and fetches data', async () => {
  vi.mocked(apiLib.client.GET).mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return { data: { fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} } } as any
    }
    return { data: {} } as any
  })

  vi.mocked(apiLib.client.POST).mockResolvedValue({
    data: {
      summary: { total: 1234 },
      time_series: []
    }
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <DashboardPage />
    </QueryClientProvider>
  )

  expect(screen.getByText('Dashboard')).toBeInTheDocument()

  // Dashboard mounts the cards grid (Traffic over Time + the rest of
  // the standard layout). Asserting on the first card title proves the
  // bundle fetch resolved without blowing up the render.
  await waitFor(() => {
    expect(screen.getByText('Traffic over Time')).toBeInTheDocument()
  })
})
