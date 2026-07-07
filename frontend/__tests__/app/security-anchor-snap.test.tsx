import { render, waitFor } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
// Covers the fix for the SSR/client anchor-mismatch flash on /security — see
// dashboard-anchor-snap.test.tsx for the full rationale. Renders the REAL
// ReportLayout (unlike security.test.tsx's shallow smoke test) so the actual
// first-paint query fires and its request body can be inspected.
import SecurityClient from '@/app/security/_sections/SecurityClient'
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
  usePathname: vi.fn(() => '/security'),
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
vi.mock('@/app/security/_sections/BotsSection', () => ({ BotsSection: () => <div data-testid="bots-section" /> }))
vi.mock('@/app/security/_sections/HeaderAnomaliesSection', () => ({
  HeaderAnomaliesSection: () => <div data-testid="header-anomalies-section" />,
}))
vi.mock('@/app/security/_sections/NetworkSection', () => ({ NetworkSection: () => <div data-testid="network-section" /> }))

test('first-paint aggregates query anchors on the snapped (stale) log extents, not "now"', async () => {
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
      <SecurityClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(apiLib.client.POST).toHaveBeenCalled()
  })

  const call = vi.mocked(apiLib.client.POST).mock.calls.find(([url]: any[]) => String(url).includes('/api/security/aggregates'))
  expect(call).toBeDefined()
  const body = (call as any)[1].body
  expect(body.anchor).toBe(quantizeAnchor('2026-06-29T11:00:00Z'))
})
