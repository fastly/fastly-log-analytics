import { render, waitFor } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
// Covers the fix for the SSR/client anchor-mismatch flash: before FilterBar's
// extents-sync effect has run (isAutoRange && !hasSyncedExtents), the anchor
// DashboardClient computes for its first-paint bundle query should already
// reflect the service's real (stale) log extents — pre-warmed in the query
// cache by root layout — instead of the naive "now" default. See
// lib/log-extents-snap.ts and DashboardClient.tsx's `anchor` useMemo.
import DashboardClient from '@/app/dashboard/_sections/DashboardClient'
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

// Cold-load, unsynced state: isAutoRange true, hasSyncedExtents false — the
// window before FilterBar's own extents check has landed.
vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector) => {
    const state = {
      startTime: '2026-06-28T12:00:00Z',
      endTime: '2026-06-29T12:00:00Z',
      filters: [],
      isAutoRange: true,
      hasSyncedExtents: false,
      compareMode: false,
      autoSetRange: vi.fn(),
      setRange: vi.fn(),
      clearFilters: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
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

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/dashboard'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plotly-chart">PlotlyChart</div> }))
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div> }))
vi.mock('@/components/Map/ChoroplethMap', () => ({ ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div> }))

test('first-paint bundle query anchors on the snapped (stale) log extents, not "now"', async () => {
  vi.mocked(apiLib.client.GET).mockResolvedValue({
    data: { fields: [{ id: 'status', label: 'HTTP Status' }], groups: [], presets: {} },
  } as any)
  vi.mocked(apiLib.client.POST).mockResolvedValue({
    data: { aggregates: { total_rows: 1 }, top_bots: { rows: [] } },
  } as any)

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  // Root layout would have already seeded this from bootstrap.log_extents —
  // simulate a service whose latest log is well over 15 minutes stale.
  queryClient.setQueryData(['log-extents', 'test-svc'], {
    earliest_log_at: '2026-06-01T00:00:00Z',
    latest_log_at: '2026-06-29T11:00:00Z',
  })

  render(
    <QueryClientProvider client={queryClient}>
      <DashboardClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(apiLib.client.POST).toHaveBeenCalled()
  })

  const bundleCall = vi
    .mocked(apiLib.client.POST)
    .mock.calls.find(([url]: any[]) => String(url).includes('/api/dashboard/bundle'))
  expect(bundleCall).toBeDefined()
  const body = (bundleCall as any)[1].body
  expect(body.anchor).toBe(quantizeAnchor('2026-06-29T11:00:00Z'))
})
