import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import NetworkPage from '@/app/network/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'

// UX-4: the Network Quality section gate was `(isQualityLoadingInitial ||
// qualityData?.available)`. On a /api/network-quality 5xx both go falsy, so the
// heading + the 4 RTT cards (which already carry error={qualityQuery.error})
// silently unmount. The fix adds `|| qualityQuery.error` so the section stays
// mounted and the cards render their error surface.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/network'))

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(async (path: string) => {
      // network-health (core/map/shielding) succeeds; network-quality 5xxs.
      if (path === '/api/network-quality') throw new Error('quality boom')
      return { data: { available: true } }
    }),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/Map/NetworkMap', () => ({ NetworkMap: () => <div data-testid="network-map" /> }))
vi.mock('@/components/Map/ShieldingMap', () => ({ ShieldingMap: () => <div data-testid="shielding-map" /> }))
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
// AnalyticsCard mock surfaces the `error` prop so we can assert the cards
// actually render an error surface (the real one is covered by its own spec).
vi.mock('@/components/AnalyticsCard', () => ({
  AnalyticsCard: ({ title, error, children }: any) => (
    <div data-testid="analytics-card">
      <h3>{title}</h3>
      {error ? <div role="alert">card error</div> : null}
      {children}
    </div>
  ),
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
beforeEach(() => queryClient.clear())

test('UX-4: Network Quality section stays mounted with an error surface on a /network-quality 5xx', async () => {
  render(
    <QueryClientProvider client={queryClient}>
      <NetworkPage />
    </QueryClientProvider>,
  )
  // Heading present AND an error surface rendered → the section did not unmount
  // after the quality query settled to an error.
  await waitFor(() => {
    expect(screen.getByText('Network Quality')).toBeInTheDocument()
    expect(screen.getAllByRole('alert').length).toBeGreaterThan(0)
  })
})
