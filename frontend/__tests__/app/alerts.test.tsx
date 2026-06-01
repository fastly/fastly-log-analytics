/**
 * MSW migration (TESTING_PLAN_3 item 9). This test used to
 * ``vi.mock('@/lib/api')`` and hand-stub `client.GET`. That bypassed
 * the openapi-fetch middleware (header injection, error-throwing
 * onResponse) so an error in either layer would slip through tests.
 *
 * Now we let the real openapi-fetch client run and intercept at the
 * fetch boundary with MSW. The store mocks remain — they are state,
 * not network.
 */
import { render, screen } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

import AlertsPage from '@/app/alerts/page'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { server } from '../../tests/msw/server'

// Mock the heavy plotly child — irrelevant to the assertion here.
vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: () => <div data-testid="plotly-chart">PlotlyChart</div>,
}))

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/alerts'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

const API_BASE = 'http://127.0.0.1:8000'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, staleTime: 0 } },
})

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [{ id: 'test-svc', name: 'Test Service', accessLevel: 'read_write' }],
  })
  useFilterStore.setState({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    isAutoRange: false,
    hasSyncedExtents: true,
  } as never)
  queryClient.clear()
})

test('renders alerts page', async () => {
  server.use(
    http.get(`${API_BASE}/api/services/test-svc/logging-settings`, () =>
      HttpResponse.json({ period: 60 }),
    ),
    http.get(`${API_BASE}/api/alerts/test-svc`, () =>
      HttpResponse.json({ alerts: [], recent_triggers: [] }),
    ),
    http.get(`${API_BASE}/api/insight-availability`, () =>
      HttpResponse.json({ unavailable: [] }),
    ),
    http.get(`${API_BASE}/api/insight-availability/test-svc`, () =>
      HttpResponse.json({ unavailable: [] }),
    ),
  )

  render(
    <QueryClientProvider client={queryClient}>
      <AlertsPage />
    </QueryClientProvider>,
  )

  // findBy waits past react-query's async resolution.
  expect(await screen.findByText('Alerts')).toBeInTheDocument()
})
