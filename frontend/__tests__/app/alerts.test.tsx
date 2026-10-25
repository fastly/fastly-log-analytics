import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { createTestQueryClient, renderWithQuery } from '../helpers/page-smoke'
import { describe, expect, test, vi, beforeEach } from 'vitest'
import React from 'react'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'

import AlertsClient from '@/app/alerts/_sections/AlertsClient'
import { AlertPreview } from '@/app/alerts/_sections/AlertEditor'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { server } from '@/__tests__/helpers/msw/handlers'

const plotlyChartCalls: Array<{ data: PlotlyTrace[]; layout: PlotlyLayout | undefined }> = []

vi.mock('@/components/PlotlyChart', async () =>
  (await import('../helpers/page-smoke')).plotlyChartModuleMock((props) => {
    plotlyChartCalls.push({ data: props.data, layout: props.layout })
  }),
)

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useLogFieldsCatalog: vi.fn(() => ({
    data: {
      fields: [
        { id: 'requests', unit: '', precision: 0 },
        { id: 'origin_latency_ms', unit: 'ms', precision: 1 },
      ],
    },
  })),
}))

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
  plotlyChartCalls.length = 0
  vi.clearAllMocks()
  mockSearchParams = new URLSearchParams()
  useTimezoneStore.setState({ timezone: 'UTC' })
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

  renderWithQuery(<AlertsClient />, { queryClient })

  expect(await screen.findByText('Alerts')).toBeInTheDocument()
  expect(await screen.findByTestId('alerts-empty-state')).toHaveTextContent('No alert rules configured')
  expect(screen.getByTestId('alerts-summary-grid').className).toContain('[contain-intrinsic-size:96px]')
  expect(screen.getByTestId('alerts-list-card').className).toContain('[contain-intrinsic-size:350px]')
  expect(screen.getByTestId('alerts-create-btn')).toBeInTheDocument()
})

test('renders configured rules, KPI summary cards, and status filter tabs', async () => {
  installBaseHandlers(SAMPLE_ALERTS)

  renderWithQuery(<AlertsClient />, { queryClient })

  expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()
  expect(screen.getByText('Origin TTFB Budget')).toBeInTheDocument()
  expect(screen.getByText('Muted Traffic Check')).toBeInTheDocument()
  expect(screen.getByTestId('alerts-count-total')).toHaveTextContent('3')
  expect(screen.getByTestId('alerts-count-alerting')).toHaveTextContent('1')
  expect(screen.getByTestId('alerts-count-ok')).toHaveTextContent('1')
  expect(screen.getByTestId('alerts-count-muted')).toHaveTextContent('1')

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

  renderWithQuery(<AlertsClient />, { queryClient })

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

  renderWithQuery(<AlertsClient />, { queryClient })

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

const basePreviewProps = {
  previewData: null,
  isPreviewLoading: false,
  lookbackHours: 6,
  setLookbackHours: vi.fn(),
  metric: 'requests',
  evalType: 'absolute',
  threshold: '',
}

describe('AlertPreview', () => {
  test('renders the empty placeholder when previewData has no points', () => {
    render(<AlertPreview {...basePreviewProps} previewData={{ times: [], values: [], type: 'absolute' }} />)
    expect(screen.getByText('No data available for preview.')).toBeInTheDocument()
    expect(plotlyChartCalls).toHaveLength(0)
  })

  test('shows the loading spinner when isPreviewLoading is true', () => {
    const { container } = render(
      <AlertPreview {...basePreviewProps} isPreviewLoading={true} previewData={null} />,
    )
    const spinner = container.querySelector('svg.animate-spin')
    expect(spinner).not.toBeNull()
  })

  test('highlights the active lookback button (default variant -> bg-primary)', async () => {
    const setLookbackHours = vi.fn()
    render(
      <AlertPreview
        {...basePreviewProps}
        lookbackHours={12}
        setLookbackHours={setLookbackHours}
        previewData={{ times: [], values: [], type: 'absolute' }}
      />,
    )

    const active = screen.getByRole('button', { name: '12h' })
    const inactive = screen.getByRole('button', { name: '1h' })

    expect(active.className).toContain('bg-primary')
    expect(inactive.className).not.toContain('bg-primary')

    await userEvent.click(inactive)
    expect(setLookbackHours).toHaveBeenCalledWith(1)
  })

  test('passes the current xy trace to PlotlyChart (timestamps converted to the selected tz)', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute' as const,
    }
    render(<AlertPreview {...basePreviewProps} previewData={previewData} threshold="" />)

    expect(plotlyChartCalls.length).toBeGreaterThan(0)
    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const current = lastCall.data[0]
    expect(current.name).toBe('Current')
    expect(current.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
    expect(current.y).toEqual(previewData.values)
    expect(current.type).toBe('bar')
  })

  test('omits the threshold overlay when threshold does not parse to a number', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute' as const,
    }
    render(
      <AlertPreview
        {...basePreviewProps}
        previewData={previewData}
        threshold=""
      />,
    )

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const thresholdTrace = lastCall.data.find((t) => t?.name === 'Threshold')
    expect(thresholdTrace).toBeUndefined()
  })

  test('emits a horizontal threshold trace when previewData.type=absolute and threshold parses', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute' as const,
    }
    render(<AlertPreview {...basePreviewProps} previewData={previewData} threshold="42" />)

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const thresholdTrace = lastCall.data.find((t) => t?.name === 'Threshold')
    expect(thresholdTrace).toBeDefined()
    expect(thresholdTrace?.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
    expect(thresholdTrace?.y).toEqual([42, 42])
    expect(thresholdTrace?.line?.dash).toBe('dash')
  })

  test('locks the x-axis (fixedrange) so the preview has no zoom/pan affordance', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute' as const,
    }
    render(<AlertPreview {...basePreviewProps} previewData={previewData} threshold="" />)

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    expect(lastCall.layout?.xaxis?.fixedrange).toBe(true)
  })

  test('emits both baseline and calculated-threshold traces for relative_increase evalType', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [110, 220],
      hist_values: [100, 200],
      type: 'relative' as const,
    }
    render(
      <AlertPreview
        {...basePreviewProps}
        previewData={previewData}
        evalType="relative_increase"
        threshold="10"
      />,
    )

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const baseline = lastCall.data.find((t) => t?.name === 'Baseline')
    const threshold = lastCall.data.find((t) => t?.name === 'Threshold')

    expect(baseline).toBeDefined()
    expect(baseline?.y).toEqual(previewData.hist_values)

    expect(threshold).toBeDefined()
    expect(threshold?.y).toHaveLength(2)
    expect(threshold?.y?.[0]).toBeCloseTo(110, 5)
    expect(threshold?.y?.[1]).toBeCloseTo(220, 5)
  })
})
