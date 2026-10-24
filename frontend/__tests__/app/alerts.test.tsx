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
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

// The route's page.tsx is now an async RSC that pre-fetches alerts
// server-side and dehydrates the cache for the client island. The unit
// test exercises the client island directly so it stays compatible with
// vitest's synchronous render.
import AlertsClient from '@/app/alerts/_sections/AlertsClient'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { server } from '@/__tests__/helpers/msw/server'

// Mock the heavy plotly child — irrelevant to the assertion here.
vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())

const mockReplace = vi.fn()
const mockPush = vi.fn()
let mockSearchParams = new URLSearchParams()

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/alerts'),
  useSearchParams: vi.fn(() => mockSearchParams),
  useRouter: vi.fn(() => ({ replace: mockReplace, push: mockPush })),
}))

const API_BASE = 'http://127.0.0.1:8000'

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

const SAMPLE_ALERTS = [
  {
    id: 'alert-firing',
    service_id: 'test-svc',
    name: 'High 5xx Spike',
    category: 'reliability',
    metric: '5xx_rate',
    evaluation_type: 'absolute',
    evaluation_scope: 'all',
    operator: '>',
    threshold: 5,
    window_min: 5,
    enabled: true,
    last_triggered_at: new Date().toISOString(),
    channels: [{ type: 'slack', url: 'https://hooks.slack.com/services/T00/B00/X00' }],
  },
  {
    id: 'alert-healthy',
    service_id: 'test-svc',
    name: 'Origin TTFB Budget',
    category: 'performance',
    metric: 'ttfb',
    evaluation_type: 'absolute',
    evaluation_scope: 'origin',
    operator: '>',
    threshold: 250,
    window_min: 5,
    enabled: true,
    last_triggered_at: null,
    channels: [],
  },
  {
    id: 'alert-muted',
    service_id: 'test-svc',
    name: 'Muted Traffic Check',
    category: 'traffic',
    metric: 'requests',
    evaluation_type: 'absolute',
    evaluation_scope: 'edge',
    operator: '<',
    threshold: 10,
    window_min: 15,
    enabled: false,
    last_triggered_at: null,
    channels: [],
  },
]

function installBaseHandlers(alerts: readonly unknown[] = [], role: 'admin' | 'analyst' = 'admin') {
  server.use(
    http.get(`${API_BASE}/api/bootstrap`, () =>
      HttpResponse.json({
        services: [{ id: 'test-svc', name: 'Test Service', access_level: role === 'admin' ? 'read_write' : 'read_only' }],
        analyst_session:
          role === 'analyst'
            ? { session_id: 'sess-1', role: 'analyst', allowed_services: ['test-svc'] }
            : null,
      }),
    ),
    http.get(`${API_BASE}/api/services/test-svc/logging-settings`, () =>
      HttpResponse.json({ period: 60 }),
    ),
    http.get(`${API_BASE}/api/alerts/test-svc`, () =>
      HttpResponse.json({ data: alerts, evaluated_at: new Date().toISOString() }),
    ),
    http.get(`${API_BASE}/api/insight-availability`, () =>
      HttpResponse.json({ unavailable: [] }),
    ),
    http.get(`${API_BASE}/api/insight-availability/test-svc`, () =>
      HttpResponse.json({ unavailable: [] }),
    ),
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  mockSearchParams = new URLSearchParams()
  useServiceStore.setState({
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [{ id: 'test-svc', name: 'Test Service', accessLevel: 'read_write' }],
  })
  useFilterStore.setState({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    isAutoRange: false,
  })
  queryClient.clear()
})

test('renders alerts page empty state and zero-CLS containers', async () => {
  installBaseHandlers([])

  render(
    <QueryClientProvider client={queryClient}>
      <AlertsClient />
    </QueryClientProvider>,
  )

  expect(await screen.findByText('Alerts')).toBeInTheDocument()
  expect(await screen.findByTestId('alerts-empty-state')).toHaveTextContent('No alert rules configured')
  expect(screen.getByTestId('alerts-summary-grid').className).toContain('[contain-intrinsic-size:96px]')
  expect(screen.getByTestId('alerts-list-card').className).toContain('[contain-intrinsic-size:350px]')
  expect(screen.getByTestId('alerts-create-btn')).toBeInTheDocument()
})

test('renders configured rules, KPI summary cards, and status filter tabs', async () => {
  installBaseHandlers(SAMPLE_ALERTS)

  render(
    <QueryClientProvider client={queryClient}>
      <AlertsClient />
    </QueryClientProvider>,
  )

  expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()
  expect(screen.getByText('Origin TTFB Budget')).toBeInTheDocument()
  expect(screen.getByText('Muted Traffic Check')).toBeInTheDocument()
  expect(screen.getByTestId('alerts-count-total')).toHaveTextContent('3')
  expect(screen.getByTestId('alerts-count-alerting')).toHaveTextContent('1')
  expect(screen.getByTestId('alerts-count-ok')).toHaveTextContent('1')
  expect(screen.getByTestId('alerts-count-muted')).toHaveTextContent('1')

  // Verify status filter buttons exist and filter rows when clicked
  const mutedFilterBtn = screen.getByTestId('alerts-filter-muted')
  fireEvent.click(mutedFilterBtn)

  await waitFor(() => {
    expect(screen.getByText('Muted Traffic Check')).toBeInTheDocument()
    expect(screen.queryByText('High 5xx Spike')).not.toBeInTheDocument()
  })
})

test('dispatches PATCH on mute/enable switch toggle and POST on test notification', async () => {
  let patchedAlertId: string | null = null
  let patchedEnabled: boolean | null = null
  let testedAlertId: string | null = null

  installBaseHandlers(SAMPLE_ALERTS)
  server.use(
    http.patch(`${API_BASE}/api/alerts/:alertId/enabled`, async ({ params, request }) => {
      patchedAlertId = String(params.alertId)
      const body = (await request.json()) as { enabled: boolean }
      patchedEnabled = body.enabled
      return HttpResponse.json({ data: { id: patchedAlertId, status: 'success' } })
    }),
    http.post(`${API_BASE}/api/alerts/:alertId/test`, async ({ params }) => {
      testedAlertId = String(params.alertId)
      return HttpResponse.json({
        data: {
          alert_id: testedAlertId,
          service_id: 'test-svc',
          status: 'validated',
          targets_tested: 1,
          channel_type: 'slack',
        },
      })
    }),
  )

  render(
    <QueryClientProvider client={queryClient}>
      <AlertsClient />
    </QueryClientProvider>,
  )

  expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()

  const toggleSwitch = screen.getByTestId('toggle-alert-alert-firing')
  fireEvent.click(toggleSwitch)

  await waitFor(() => {
    expect(patchedAlertId).toBe('alert-firing')
    expect(patchedEnabled).toBe(false)
  })

  const testBtn = screen.getByTestId('test-alert-alert-firing')
  fireEvent.click(testBtn)

  await waitFor(() => {
    expect(testedAlertId).toBe('alert-firing')
  })
})

test('enforces read-only gating for Analyst Path B role', async () => {
  installBaseHandlers(SAMPLE_ALERTS, 'analyst')
  useServiceStore.setState({
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [{ id: 'test-svc', name: 'Test Service', accessLevel: 'read_only' }],
  })
  queryClient.setQueryData(['alerts', 'test-svc'], {
    data: SAMPLE_ALERTS,
    evaluated_at: new Date().toISOString(),
  })

  render(
    <QueryClientProvider client={queryClient}>
      <AlertsClient />
    </QueryClientProvider>,
  )

  expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.queryByTestId('alerts-create-btn')).not.toBeInTheDocument()
    expect(screen.queryByTestId('alerts-evaluate-btn')).not.toBeInTheDocument()
    expect(screen.queryByTestId('edit-alert-alert-firing')).not.toBeInTheDocument()
    expect(screen.queryByTestId('delete-alert-alert-firing')).not.toBeInTheDocument()
    expect(screen.queryByTestId('test-alert-alert-firing')).not.toBeInTheDocument()
    expect(screen.getByTestId('analyst-alert-enabled-alert-firing')).toHaveTextContent('On')
  })
})
