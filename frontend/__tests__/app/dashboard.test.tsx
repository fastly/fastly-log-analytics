import { withNuqsTestingAdapter } from 'nuqs/adapters/testing'
import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
// app/dashboard/page.tsx is now an async RSC shell that SSR-prefetches the
// bundle and dehydrates into <DashboardClient />; RTL can't render an async
// component, so the smoke test targets the client component that owns the
// title + chart/cards.
import DashboardClient from '@/app/dashboard/_sections/DashboardClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import * as apiLib from '@/lib/api'
import { getOk, postOk } from '@/__tests__/helpers/api-result'
import React from 'react'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div> }))
vi.mock('@/components/Map/ChoroplethMap', () => ({ ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div> }))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

test('renders dashboard and fetches data', async () => {
  vi.mocked(apiLib.client.GET).mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({ fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} })
    }
    return getOk({})
  })

  vi.mocked(apiLib.client.POST).mockResolvedValue(postOk({
      summary: { total: 1234 },
      time_series: [],
    }))

  render(
    <QueryClientProvider client={queryClient}>
      <DashboardClient />
    </QueryClientProvider>,
    { wrapper: withNuqsTestingAdapter() },
  )

  expect(screen.getByText('Dashboard')).toBeInTheDocument()

  // Dashboard mounts the cards grid (Traffic over Time + the rest of
  // the standard layout). Asserting on the first card title proves the
  // bundle fetch resolved without blowing up the render.
  await waitFor(() => {
    expect(screen.getByText('Traffic over Time')).toBeInTheDocument()
  })
})
