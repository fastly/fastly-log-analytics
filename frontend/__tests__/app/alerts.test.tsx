import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import {
  createTestQueryClient, renderWithQuery,
  stubMatchMedia, stubResizeObserver, stubGetAnimations, stubPointerCaptureAndScroll,

} from '../helpers/page-smoke'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import React from 'react'
import type { PlotlyLayout, PlotlyTrace } from '@/types/api'
import type { components } from '@/types/api.generated'
import {
  INSIGHT_SECTIONS, OTHER_SECTION, categoryToSection, sortInsights, groupInsightsBySection,
  summarizeSeverities, pickInsightsDefault, historyHoursFromExtents, WINDOW_OPTIONS, BASELINE_OPTIONS,
} from '@/lib/utils'
import AlertsClient, { AlertPreview, CreateAlertForm } from '@/app/alerts/_sections/AlertsClient'
import InsightsClient from '@/app/insights/_sections/InsightsClient'
import { useServiceStore, useFilterStore, useTimezoneStore, useSessionRoleStore } from '@/stores/filterStore'

let mockSearchParamsString = 'service=svc-test'
let mockScoringSubcomponents = false
let useMockInsightsApi = false
let useMockReportLayout = false
let useMockSessionApi = false
let useMockShareSearchParams = false
let mockPathname = '/admin'
let mockSearchParams = new URLSearchParams()

type ApiResult = { data: unknown }
type PostOpts = { body?: { window_size_hrs?: number; baseline_hours?: number; force_refresh?: boolean } }

const {
  adminFetchSpy, adminJsonSpy, adminJsonResultSpy, clientGetSpy, clientPostSpy,
  realApiRef, notFoundMock, apiGet, apiPost, postMock, pushSpy, replaceSpy,
} = vi.hoisted(() => ({
  adminFetchSpy: vi.fn(),
  adminJsonSpy: vi.fn(),
  adminJsonResultSpy: vi.fn(),
  clientGetSpy: vi.fn(),
  clientPostSpy: vi.fn(),
  apiGet: vi.fn<(url: string, opts?: unknown) => Promise<ApiResult>>(),
  apiPost: vi.fn<(url: string, opts?: PostOpts) => Promise<ApiResult>>(),
  postMock: vi.fn(),
  pushSpy: vi.fn(),
  replaceSpy: vi.fn(),
  locationAssignSpy: vi.fn(),
  notFoundMock: vi.fn(() => { throw new Error('NEXT_HTTP_ERROR_FALLBACK;404') }),
  realApiRef: {
    adminFetch: null as ((...args: unknown[]) => Promise<Response>) | null,
    adminJson: null as ((...args: unknown[]) => Promise<unknown>) | null,
    adminJsonResult: null as ((...args: unknown[]) => Promise<unknown>) | null,
    clientGet: null as ((...args: unknown[]) => Promise<unknown>) | null,
    clientPost: null as ((...args: unknown[]) => Promise<unknown>) | null,
  },
}))

vi.mock('@/components/Insights/InsightCard', () => ({
  InsightCard: ({ insight }: { insight: { name?: string; title?: string } }) => (
    <div data-testid="insight-card">{insight.name || insight.title}</div>
  ),
  SEVERITY_BADGE_CLASS: { clean: '', info: '', warning: '', critical: '', error: '' },
}))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const mockMod = await (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    activeServiceId: 'test-svc', startTime: null, endTime: null, filterPayload: {},
  })
  const Wrapped = (props: React.ComponentProps<typeof actual.ReportLayout>) =>
    useMockReportLayout ? mockMod.ReportLayout(props as never) : actual.ReportLayout(props)
  return { ...actual, default: Wrapped, ReportLayout: Wrapped }
})

const plotlyChartCalls: Array<{ data: PlotlyTrace[]; layout: PlotlyLayout | undefined }> = []

vi.mock('@/components/PlotlyChart', async () =>
  (await import('../helpers/page-smoke')).plotlyChartModuleMock((props) => {
    plotlyChartCalls.push({ data: props.data, layout: props.layout })
  }),
)

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
  useFieldLabel: () => (id: string) => id,
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  realApiRef.adminFetch = actual.adminFetch as (...args: unknown[]) => Promise<Response>
  realApiRef.clientGet = actual.client.GET as (...args: unknown[]) => Promise<unknown>
  realApiRef.clientPost = actual.client.POST as (...args: unknown[]) => Promise<unknown>
  realApiRef.adminJsonResult = actual.adminJsonResult as (...args: unknown[]) => Promise<unknown>
  const defaultAdminJson = async (...args: unknown[]): Promise<unknown> => {
    const [, url, init, errorMessage] = args as [unknown, unknown, unknown, ((status: number) => string) | undefined]
    const r = await adminFetchSpy(url, init)
    if (!r.ok) throw new Error((errorMessage ?? ((status: number) => `status ${status}`))(r.status))
    return r.json()
  }
  realApiRef.adminJson = defaultAdminJson
  adminFetchSpy.mockImplementation(realApiRef.adminFetch)
  adminJsonSpy.mockImplementation(defaultAdminJson)
  adminJsonResultSpy.mockImplementation(realApiRef.adminJsonResult)
  clientGetSpy.mockImplementation(realApiRef.clientGet)
  clientPostSpy.mockImplementation(realApiRef.clientPost)
  return {
    ...actual,
    adminFetch: (...args: unknown[]) => adminFetchSpy(...args),
    adminJson: (...args: unknown[]) => adminJsonSpy(...args),
    adminJsonResult: (...args: unknown[]) => adminJsonResultSpy(...args),
    client: new Proxy(actual.client, {
      get(target, prop, receiver) {
        if (prop === 'GET') {
          return (...args: unknown[]) => (useMockInsightsApi ? apiGet(...(args as [string, unknown])) : clientGetSpy(...args))
        }
        if (prop === 'POST') {
          return (...args: unknown[]) => {
            if (useMockInsightsApi) return apiPost(...(args as [string, PostOpts]))
            if (useMockSessionApi) return postMock(...args)
            return clientPostSpy(...args)
          }
        }
        return Reflect.get(target, prop, receiver)
      },
    }),
    extractApiError: (e: unknown) => (useMockInsightsApi || useMockSessionApi ? String(e) : actual.extractApiError(e)),
    getApiBase: () => (useMockInsightsApi ? 'http://test' : actual.getApiBase()),
  }
})

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useAdminEventStream: () => ({ state: 'open' as const }),
}))

vi.mock('@/components/SessionScoring/ScoringAdminPanels', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/ScoringAdminPanels')>()
  function wrap<T extends (...args: never[]) => unknown>(fn: T) {
    return (...args: Parameters<T>) => (mockScoringSubcomponents ? null : fn(...args))
  }
  return {
    ...actual,
    ScoringHealthCard: wrap(actual.ScoringHealthCard),
    ScorerFailOpenBreakdownCard: wrap(actual.ScorerFailOpenBreakdownCard),
    MatrixVersionsCard: wrap(actual.MatrixVersionsCard),
    AuditLogTab: wrap(actual.AuditLogTab),
    ExcludeRegexCard: wrap(actual.ExcludeRegexCard),
    RetrainButton: wrap(actual.RetrainButton),
    RotateKeyButton: wrap(actual.RotateKeyButton),
    ThresholdSlider: wrap(actual.ThresholdSlider),
  }
})

vi.mock('@/components/SessionScoring/StatusPanel', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/StatusPanel')>()
  function wrap<T extends (...args: never[]) => unknown>(fn: T) {
    return (...args: Parameters<T>) => (mockScoringSubcomponents ? null : fn(...args))
  }
  return {
    ...actual,
    StatusPanel: (props: React.ComponentProps<typeof actual.StatusPanel>) =>
      mockScoringSubcomponents ? <div data-testid="admin-session-scoring-status-card">Status for {props.serviceId}</div> : actual.StatusPanel(props),
    L2EnforcementCard: wrap(actual.L2EnforcementCard),
    RocPrCurves: wrap(actual.RocPrCurves),
    PerReasonAucCard: wrap(actual.PerReasonAucCard),
    ScoreDistChart: wrap(actual.ScoreDistChart),
    ComplianceChart: wrap(actual.ComplianceChart),
    StackedHourlyBarChart: wrap(actual.StackedHourlyBarChart),
    ScorerLatencyChart: wrap(actual.ScorerLatencyChart),
    ScorerErrorsChart: wrap(actual.ScorerErrorsChart),
    ScorerSeriesCard: wrap(actual.ScorerSeriesCard),
  }
})


vi.mock('@/components/SessionScoring/TopFlaggedTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/TopFlaggedTable')>()
  return {
    ...actual,
    TopFlaggedTable: (...args: Parameters<typeof actual.TopFlaggedTable>) => mockScoringSubcomponents ? null : actual.TopFlaggedTable(...args),
    FlagSessionPopover: (...args: Parameters<typeof actual.FlagSessionPopover>) =>
      useMockSessionApi ? <span data-testid="flag-popover" /> : actual.FlagSessionPopover(...args),
  }
})
import { getApiBase } from '@/lib/api'
import { server } from '@/__tests__/helpers/msw/handlers'
stubMatchMedia()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  stubGetAnimations()
})

const API_BASE = getApiBase()

vi.mock('next/navigation', async () => ({
  ...(await import('../helpers/page-smoke')).navigationModuleMock('/admin'),
  usePathname: vi.fn(() => mockPathname),
  useSearchParams: vi.fn(() => (useMockShareSearchParams ? mockSearchParams : new URLSearchParams(mockSearchParamsString))),
  useRouter: vi.fn(() => ({ push: pushSpy, replace: replaceSpy, back: vi.fn(), forward: vi.fn(), refresh: vi.fn(), prefetch: vi.fn() })),
  notFound: () => notFoundMock(),
}))
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

beforeEach(() => {
  vi.clearAllMocks()
  mockPathname = '/admin'
  mockSearchParamsString = 'service=svc-test'
  useMockShareSearchParams = false
  mockScoringSubcomponents = false
  useMockInsightsApi = false
  useMockReportLayout = false
  useMockSessionApi = false
  if (realApiRef.adminFetch) adminFetchSpy.mockImplementation(realApiRef.adminFetch)
  if (realApiRef.adminJson) adminJsonSpy.mockImplementation(realApiRef.adminJson)
  if (realApiRef.adminJsonResult) adminJsonResultSpy.mockImplementation(realApiRef.adminJsonResult)
  if (realApiRef.clientGet) clientGetSpy.mockImplementation(realApiRef.clientGet)
  if (realApiRef.clientPost) clientPostSpy.mockImplementation(realApiRef.clientPost)
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
})


// ============================================================================
// Merged from alerts.test.tsx
// ============================================================================
describe('Alerts & Insights Pages', () => {
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

  const SAMPLE_ALERTS = [
    {
      id: 'alert-firing', service_id: 'test-svc', name: 'High 5xx Spike', category: 'reliability',
      metric: '5xx_rate', evaluation_type: 'absolute', evaluation_scope: 'all', operator: '>',
      threshold: 5, window_min: 5, enabled: true, last_triggered_at: new Date().toISOString(),
      channels: [{ type: 'slack', url: 'https://hooks.slack.com/services/T00/B00/X00' }],
    },
    {
      id: 'alert-healthy', service_id: 'test-svc', name: 'Origin TTFB Budget', category: 'performance',
      metric: 'ttfb', evaluation_type: 'absolute', evaluation_scope: 'origin', operator: '>',
      threshold: 250, window_min: 5, enabled: true, last_triggered_at: null, channels: [],
    },
    {
      id: 'alert-muted', service_id: 'test-svc', name: 'Muted Traffic Check', category: 'traffic',
      metric: 'requests', evaluation_type: 'absolute', evaluation_scope: 'edge', operator: '<',
      threshold: 10, window_min: 15, enabled: false, last_triggered_at: null, channels: [],
    },
  ]

  function installBaseHandlers(alerts: readonly unknown[] = [], role: 'admin' | 'analyst' = 'admin') {
    server.use(
      http.get(`${API_BASE}/api/bootstrap`, () =>
        HttpResponse.json({
          services: [{ id: 'test-svc', name: 'Test Service' }],
          analyst_session: role === 'analyst' ? { session_id: 'sess-1', role: 'analyst', allowed_services: ['test-svc'] } : null,
        }),
      ),
      http.get(`${API_BASE}/api/services/test-svc/logging-settings`, () => HttpResponse.json({ period: 60 })),
      http.get(`${API_BASE}/api/alerts/test-svc`, () => HttpResponse.json({ data: alerts, evaluated_at: new Date().toISOString() })),
      http.get(`${API_BASE}/api/insight-availability`, () => HttpResponse.json({ unavailable: [] })),
      http.get(`${API_BASE}/api/insight-availability/test-svc`, () => HttpResponse.json({ unavailable: [] })),
    )
  }

  beforeEach(() => {
    plotlyChartCalls.length = 0
    vi.clearAllMocks()
    mockPathname = '/alerts'
    useMockShareSearchParams = true
    mockSearchParams = new URLSearchParams()
    useTimezoneStore.setState({ timezone: 'UTC' })
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test Service' }],
    })
    useFilterStore.setState({ startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z', isAutoRange: false })
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

    fireEvent.click(screen.getByTestId('alerts-filter-muted'))
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
        patchedEnabled = ((await request.json()) as { enabled: boolean }).enabled
        return HttpResponse.json({ data: { id: patchedAlertId, status: 'success' } })
      }),
      http.post(`${API_BASE}/api/alerts/:alertId/test`, async ({ params }) => {
        testedAlertId = String(params.alertId)
        return HttpResponse.json({
          data: { alert_id: testedAlertId, service_id: 'test-svc', status: 'validated', targets_tested: 1, channel_type: 'slack' },
        })
      }),
    )

    renderWithQuery(<AlertsClient />, { queryClient })
    expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()

    fireEvent.click(screen.getByTestId('toggle-alert-alert-firing'))
    await waitFor(() => {
      expect(patchedAlertId).toBe('alert-firing')
      expect(patchedEnabled).toBe(false)
    })

    fireEvent.click(screen.getByTestId('test-alert-alert-firing'))
    await waitFor(() => expect(testedAlertId).toBe('alert-firing'))
  })

  test('enforces read-only gating for Analyst Path B role', async () => {
    installBaseHandlers(SAMPLE_ALERTS, 'analyst')
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test Service' }],
    })
    queryClient.setQueryData(['bootstrap'], { settings: { is_remote_analyst: true } })
    queryClient.setQueryData(['alerts', 'test-svc'], { data: SAMPLE_ALERTS, evaluated_at: new Date().toISOString() })

    renderWithQuery(<AlertsClient />, { queryClient })
    expect(await screen.findByText('High 5xx Spike')).toBeInTheDocument()

    await waitFor(() => {
      for (const id of ['alerts-create-btn', 'alerts-evaluate-btn', 'edit-alert-alert-firing', 'delete-alert-alert-firing', 'test-alert-alert-firing']) {
        expect(screen.queryByTestId(id)).not.toBeInTheDocument()
      }
      expect(screen.getByTestId('analyst-alert-enabled-alert-firing')).toHaveTextContent('On')
    })
  })

  const basePreviewProps = {
    previewData: null, isPreviewLoading: false, lookbackHours: 6,
    setLookbackHours: vi.fn(), metric: 'requests', evalType: 'absolute', threshold: '',
  }
  const samplePreviewData = {
    times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
    values: [10, 20],
    type: 'absolute' as const,
  }

  describe('AlertPreview', () => {
    test('renders the empty placeholder when previewData has no points', () => {
      render(<AlertPreview {...basePreviewProps} previewData={{ times: [], values: [], type: 'absolute' }} />)
      expect(screen.getByText('No data available for preview.')).toBeInTheDocument()
      expect(plotlyChartCalls).toHaveLength(0)
    })

    test('shows the loading spinner when isPreviewLoading is true', () => {
      const { container } = render(<AlertPreview {...basePreviewProps} isPreviewLoading={true} previewData={null} />)
      expect(container.querySelector('svg.animate-spin')).not.toBeNull()
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
      render(<AlertPreview {...basePreviewProps} previewData={samplePreviewData} threshold="" />)
      expect(plotlyChartCalls.length).toBeGreaterThan(0)
      const current = plotlyChartCalls[plotlyChartCalls.length - 1].data[0]
      expect(current.name).toBe('Current')
      expect(current.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
      expect(current.y).toEqual(samplePreviewData.values)
      expect(current.type).toBe('bar')
    })

    test('omits the threshold overlay when threshold does not parse to a number', () => {
      render(<AlertPreview {...basePreviewProps} previewData={samplePreviewData} threshold="" />)
      const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
      expect(lastCall.data.find((t) => t?.name === 'Threshold')).toBeUndefined()
    })

    test('emits a horizontal threshold trace when previewData.type=absolute and threshold parses', () => {
      render(<AlertPreview {...basePreviewProps} previewData={samplePreviewData} threshold="42" />)
      const thresholdTrace = plotlyChartCalls[plotlyChartCalls.length - 1].data.find((t) => t?.name === 'Threshold')
      expect(thresholdTrace).toBeDefined()
      expect(thresholdTrace?.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
      expect(thresholdTrace?.y).toEqual([42, 42])
      expect(thresholdTrace?.line?.dash).toBe('dash')
    })

    test('locks the x-axis (fixedrange) so the preview has no zoom/pan affordance', () => {
      render(<AlertPreview {...basePreviewProps} previewData={samplePreviewData} threshold="" />)
      expect(plotlyChartCalls[plotlyChartCalls.length - 1].layout?.xaxis?.fixedrange).toBe(true)
    })

    test('emits both baseline and calculated-threshold traces for relative_increase evalType', () => {
      const previewData = {
        times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
        values: [110, 220],
        hist_values: [100, 200],
        type: 'relative' as const,
      }
      render(<AlertPreview {...basePreviewProps} previewData={previewData} evalType="relative_increase" threshold="10" />)

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

  type Alert = components['schemas']['Alert']
  const FORM_SVC = 'svc-alerts'

  describe('CreateAlertForm', () => {
    let posted: Record<string, unknown>[] = []
    let putCalls: { id: string; body: Record<string, unknown> }[] = []

    beforeEach(() => {
      posted = []
      putCalls = []
      useServiceStore.setState({
        activeServiceId: FORM_SVC,
        services: [{ id: FORM_SVC, name: 'Svc' }],
        isInitialized: true,
      })
      server.use(
        http.post(`${API_BASE}/api/alerts/preview`, () => HttpResponse.json({ data: null })),
        http.post(`${API_BASE}/api/alerts/`, async ({ request }) => {
          posted.push((await request.json()) as Record<string, unknown>)
          return HttpResponse.json({})
        }),
        http.put(`${API_BASE}/api/alerts/:id`, async ({ request, params }) => {
          putCalls.push({ id: String(params.id), body: (await request.json()) as Record<string, unknown> })
          return HttpResponse.json({})
        }),
      )
    })

    function renderForm(initialAlert?: Alert | null, onSuccess: () => void = () => {}) {
      return renderWithQuery(<CreateAlertForm initialAlert={initialAlert} onSuccess={onSuccess} />)
    }

    describe('CreateAlertForm validation', () => {
      it('disables Create until name and threshold are filled', async () => {
        const user = userEvent.setup()
        renderForm()
        const create = screen.getByRole('button', { name: 'Create Alert' }) as HTMLButtonElement
        expect(create.disabled).toBe(true)
        await user.type(screen.getByLabelText('Alert Name'), 'High 5xx')
        expect(create.disabled).toBe(true)
        await user.type(document.getElementById('threshold') as HTMLInputElement, '100')
        await waitFor(() => expect(create.disabled).toBe(false))
      })

      it('shows required errors only after blur, with the original copy', async () => {
        const user = userEvent.setup()
        renderForm()
        expect(screen.queryByText('Name is required.')).toBeNull()
        expect(screen.queryByText('Threshold is required.')).toBeNull()

        const name = screen.getByLabelText('Alert Name')
        await user.click(name)
        await user.tab()
        expect(await screen.findByText('Name is required.')).toBeTruthy()
        expect(name.getAttribute('aria-invalid')).toBe('true')

        const threshold = document.getElementById('threshold') as HTMLInputElement
        await user.click(threshold)
        await user.tab()
        expect(await screen.findByText('Threshold is required.')).toBeTruthy()

        await user.type(name, 'x')
        await waitFor(() => expect(screen.queryByText('Name is required.')).toBeNull())
      })
    })

    describe('CreateAlertForm submit', () => {
      it('POSTs a new alert with a numeric threshold and closes', async () => {
        const user = userEvent.setup()
        const onSuccess = vi.fn()
        renderForm(null, onSuccess)
        await user.type(screen.getByLabelText('Alert Name'), 'Traffic spike')
        await user.type(document.getElementById('threshold') as HTMLInputElement, '250.5')
        await user.click(screen.getByRole('button', { name: 'Create Alert' }))
        await waitFor(() => expect(posted.length).toBe(1))
        expect(posted[0]).toMatchObject({
          service_id: FORM_SVC, name: 'Traffic spike', category: 'traffic', metric: 'requests',
          evaluation_type: 'absolute', evaluation_scope: 'all', operator: '>', threshold: 250.5,
          window_min: 5, enabled: true,
        })
        expect(posted[0].webhook_url).toBeUndefined()
        expect(posted[0].zscore_threshold).toBeUndefined()
        await waitFor(() => expect(onSuccess).toHaveBeenCalled())
      })

      it('PUTs an edited alert by id and keeps its enabled flag', async () => {
        const user = userEvent.setup()
        const alert = {
          id: 'a-1', service_id: FORM_SVC, name: 'Existing', category: 'reliability',
          metric: '5xx', evaluation_type: 'absolute', evaluation_scope: 'edge', operator: '>=',
          threshold: 10, window_min: 15, enabled: false, channels: [], webhook_url: 'https://hooks.example.test/x',
        } as unknown as Alert
        renderForm(alert)
        const name = screen.getByLabelText('Alert Name') as HTMLInputElement
        expect(name.value).toBe('Existing')
        await user.type(name, ' v2')
        await user.click(screen.getByRole('button', { name: 'Save Changes' }))
        await waitFor(() => expect(putCalls.length).toBe(1))
        expect(putCalls[0].id).toBe('a-1')
        expect(putCalls[0].body).toMatchObject({
          name: 'Existing v2', operator: '>=', threshold: 10, window_min: 15,
          enabled: false, webhook_url: 'https://hooks.example.test/x',
        })
      })

      it('z-score alerts send zscore fields and a ">" operator', async () => {
        const user = userEvent.setup()
        const alert = {
          id: 'a-2', service_id: FORM_SVC, name: 'Anomaly', category: 'traffic', metric: 'requests',
          evaluation_type: 'anomaly_zscore', evaluation_scope: 'all', operator: '<', threshold: 3,
          window_min: 5, zscore_threshold: 4.5, baseline_period_days: 14, enabled: true, channels: [],
        } as unknown as Alert
        renderForm(alert)
        await user.click(screen.getByRole('button', { name: 'Save Changes' }))
        await waitFor(() => expect(putCalls.length).toBe(1))
        expect(putCalls[0].body).toMatchObject({
          operator: '>', threshold: 4.5, zscore_threshold: 4.5, baseline_period_days: 14,
        })
      })

      it('does not submit when required fields are empty', async () => {
        const user = userEvent.setup()
        renderForm()
        const form = screen.getByRole('button', { name: 'Create Alert' }).closest('form') as HTMLFormElement
        form.requestSubmit?.()
        await user.click(screen.getByLabelText('Alert Name'))
        await new Promise((r) => setTimeout(r, 50))
        expect(posted.length).toBe(0)
      })
    })
  })

  describe('InsightsClient', () => {
    beforeEach(() => {
      useMockInsightsApi = true
      useMockReportLayout = true
      useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
      useSessionRoleStore.setState({ isRemoteAnalyst: false })
      queryClient.clear()
    })

    afterEach(() => {
      useMockInsightsApi = false
      useMockReportLayout = false
    })

    test('renders insights page and displays insight cards', async () => {
      apiGet.mockResolvedValue({ data: { unavailable: [] } })
      apiPost.mockResolvedValue({
        data: {
          insights: [{
            id: 'insight-1', name: 'Global Latency Spike', title: 'Global Latency Spike',
            description: 'Latency is higher than usual', category: 'edge', severity: 'warning',
            current_value: 150, baseline_value: 100, delta_percent: 50, unit: 'ms',
          }],
          computed_at: new Date().toISOString(),
        },
      })

      renderWithQuery(<InsightsClient />, { queryClient })
      expect(screen.getByText('Anomaly Detection')).toBeInTheDocument()

      await waitFor(() => {
        expect(screen.getByTestId('insight-card')).toBeInTheDocument()
        expect(screen.getByText('Global Latency Spike')).toBeInTheDocument()
      })
    })

    test('adapts the default window/baseline to a service with ~2h of history', async () => {
      apiGet.mockImplementation((url) =>
        url === '/api/log-extents'
          ? Promise.resolve({
              data: {
                earliest_log_at: new Date(Date.now() - 2 * 3600_000).toISOString(),
                latest_log_at: new Date().toISOString(),
              },
            })
          : Promise.resolve({ data: { unavailable: [] } }),
      )
      apiPost.mockResolvedValue({ data: { insights: [], computed_at: new Date().toISOString() } })

      renderWithQuery(<InsightsClient />, { queryClient })
      await waitFor(
        () => {
          const sawAdaptive = apiPost.mock.calls.some(
            ([, opts]) => opts?.body?.window_size_hrs === 1 && opts?.body?.baseline_hours === 1,
          )
          expect(sawAdaptive).toBe(true)
        },
        { timeout: 3000 },
      )
    })

    test('uses the SSR service and adaptive seed before the service store hydrates', async () => {
      useServiceStore.setState({ activeServiceId: null, isInitialized: false })
      queryClient.setQueryData(['bootstrap'], { active_service_id: 'test-svc' })
      queryClient.setQueryData(['log-extents', 'test-svc'], {
        earliest_log_at: new Date(Date.now() - 36 * 3600_000).toISOString(),
        latest_log_at: new Date().toISOString(),
      })
      queryClient.setQueryData(['insights', 'test-svc', '4', '24'], {
        insights: [{ id: 'seeded-insight', title: 'Seeded anomaly', category: 'edge', severity: 'warning' }],
        computed_at: new Date().toISOString(),
      })
      apiGet.mockResolvedValue({ data: { unavailable: [] } })

      renderWithQuery(<InsightsClient />, { queryClient })
      expect(screen.getByText('Seeded anomaly')).toBeInTheDocument()
      expect(apiPost).not.toHaveBeenCalled()
    })

    test('renders all 6 category tabs and enforces zero-CLS layout reservation classes', async () => {
      apiGet.mockResolvedValue({ data: { unavailable: [] } })
      apiPost.mockResolvedValue({
        data: {
          insights: [{ id: 'insight-sec-1', name: 'SQL Injection Spike', title: 'SQL Injection Spike', category: 'security', severity: 'critical' }],
          computed_at: new Date().toISOString(),
        },
      })

      renderWithQuery(<InsightsClient />, { queryClient })
      await waitFor(() => {
        for (const tab of [/all categories/i, /security & threat detection/i, /origin health & stability/i, /edge & delivery performance/i, /network & routing path/i, /traffic & volumetrics/i]) {
          expect(screen.getByRole('tab', { name: tab })).toBeInTheDocument()
        }
        expect(screen.getByTestId('insights-grid')).toBeInTheDocument()
      })

      const grid = screen.getByTestId('insights-grid')
      expect(grid.className).toContain('min-h-[300px]')
      expect(grid.className).toContain('[contain-intrinsic-size:300px]')
      expect(grid.className).toContain('[content-visibility:auto]')
    })

    test('displays unavailable insights alert with link to admin log settings', async () => {
      apiGet.mockResolvedValue({
        data: {
          unavailable: [
            { id: 'ja3_anomaly', name: 'JA3 Fingerprint Anomaly', missing_fields: ['ja3_md5'] },
            { id: 'bot_spike', name: 'Bot Cluster Surge', missing_fields: ['bot_score'] },
          ],
        },
      })
      apiPost.mockResolvedValue({ data: { insights: [], computed_at: new Date().toISOString() } })

      renderWithQuery(<InsightsClient />, { queryClient })
      await waitFor(() => {
        expect(screen.getByText(/2 insights are unavailable/i)).toBeInTheDocument()
        expect(screen.getByText(/Configure Service Log Fields/i)).toBeInTheDocument()
      })
      expect(screen.getByRole('link', { name: /configure service log fields/i })).toHaveAttribute('href', '/admin?service=test-svc')
    })

    test('renders Re-scan button for Admin and handles force_refresh', async () => {
      useSessionRoleStore.setState({ isRemoteAnalyst: false })
      apiGet.mockResolvedValue({ data: { unavailable: [] } })
      apiPost.mockResolvedValue({ data: { insights: [], computed_at: new Date().toISOString() } })

      renderWithQuery(<InsightsClient />, { queryClient })
      const rescanBtn = await screen.findByRole('button', { name: /re-scan/i })
      fireEvent.click(rescanBtn)

      await waitFor(() => {
        expect(apiPost.mock.calls.some(([, opts]) => opts?.body?.force_refresh === true)).toBe(true)
      })
    })

    test('hides Re-scan button for Remote Analyst', async () => {
      useSessionRoleStore.setState({ isRemoteAnalyst: true })
      apiGet.mockResolvedValue({ data: { unavailable: [] } })
      apiPost.mockResolvedValue({ data: { insights: [], computed_at: new Date().toISOString() } })

      renderWithQuery(<InsightsClient />, { queryClient })
      await waitFor(() => expect(screen.getByText('Anomaly Detection')).toBeInTheDocument())
      expect(screen.queryByRole('button', { name: /re-scan/i })).not.toBeInTheDocument()
    })

    test('displays clean pass state when a category has no anomalies', async () => {
      apiGet.mockResolvedValue({ data: { unavailable: [] } })
      apiPost.mockResolvedValue({ data: { insights: [], computed_at: new Date().toISOString() } })

      renderWithQuery(<InsightsClient initialTab="security" />, { queryClient })
      await waitFor(() => {
        expect(screen.getByTestId('insights-empty-pass')).toBeInTheDocument()
        expect(screen.getByText(/no anomalies detected in security & threat detection/i)).toBeInTheDocument()
      })

      const emptyPass = screen.getByTestId('insights-empty-pass')
      expect(emptyPass.className).toContain('min-h-[300px]')
      expect(emptyPass.className).toContain('[contain-intrinsic-size:300px]')
      expect(emptyPass.className).toContain('[content-visibility:auto]')
    })
  })

  describe('categoryToSection', () => {
    it('maps each known category to its section', () => {
      for (const k of ['security', 'origin', 'edge', 'network', 'traffic'] as const) {
        expect(categoryToSection(k).key).toBe(k)
      }
    })

    it('falls back to Other for unknown / missing category', () => {
      for (const v of ['does_not_exist', '', null, undefined]) {
        expect(categoryToSection(v).key).toBe(OTHER_SECTION.key)
      }
    })
  })

  describe('sortInsights', () => {
    it('orders by severity (critical→error→warning→info→clean) then title', () => {
      const cards = [
        { title: 'Zeta', severity: 'clean' },
        { title: 'Alpha', severity: 'warning' },
        { title: 'Beta', severity: 'critical' },
        { title: 'Gamma', severity: 'warning' },
        { title: 'Delta', severity: 'error' },
        { title: 'Epsilon', severity: 'info' },
      ]
      expect(sortInsights(cards).map((c) => c.title)).toEqual(['Beta', 'Delta', 'Alpha', 'Gamma', 'Epsilon', 'Zeta'])
    })

    it('sorts unknown / missing severities last, then by title', () => {
      const cards = [{ title: 'B', severity: undefined }, { title: 'A', severity: 'critical' }, { title: 'C', severity: 'mystery' }]
      expect(sortInsights(cards).map((c) => c.title)).toEqual(['A', 'B', 'C'])
    })

    it('does not mutate its input', () => {
      const cards = [{ title: 'B', severity: 'clean' }, { title: 'A', severity: 'critical' }]
      const snapshot = [...cards]
      sortInsights(cards)
      expect(cards).toEqual(snapshot)
    })
  })

  describe('groupInsightsBySection', () => {
    it('returns sections in fixed order, dropping empty ones', () => {
      const cards = [
        { id: '1', title: 'T', severity: 'info', category: 'traffic' },
        { id: '2', title: 'S', severity: 'critical', category: 'security' },
      ]
      expect(groupInsightsBySection(cards).map((g) => g.section.key)).toEqual(['security', 'traffic'])
    })

    it('files each card into its section and sorts within the section', () => {
      const cards = [
        { id: '1', title: 'Warn', severity: 'warning', category: 'edge' },
        { id: '2', title: 'Crit', severity: 'critical', category: 'edge' },
        { id: '3', title: 'Sec', severity: 'info', category: 'security' },
      ]
      const groups = groupInsightsBySection(cards)
      expect(groups.find((g) => g.section.key === 'edge')!.cards.map((c) => c.id)).toEqual(['2', '1'])
      expect(groups.find((g) => g.section.key === 'security')!.cards.map((c) => c.id)).toEqual(['3'])
    })

    it('files network-category cards into the Network section, in fixed order', () => {
      const cards = [
        { id: '1', title: 'Traf', severity: 'info', category: 'traffic' },
        { id: '2', title: 'Net', severity: 'warning', category: 'network' },
        { id: '3', title: 'Edge', severity: 'info', category: 'edge' },
      ]
      const groups = groupInsightsBySection(cards)
      expect(groups.map((g) => g.section.key)).toEqual(['edge', 'network', 'traffic'])
      const net = groups.find((g) => g.section.key === 'network')!
      expect(net.cards.map((c) => c.id)).toEqual(['2'])
      expect(net.section.label).toBe('Network & Routing Path')
    })

    it('buckets unknown / missing categories into the Other section, ordered last', () => {
      const cards = [
        { id: '1', title: 'Known', severity: 'info', category: 'security' },
        { id: '2', title: 'Unknown', severity: 'info', category: 'does_not_exist' },
        { id: '3', title: 'Missing', severity: 'info', category: null },
      ]
      const groups = groupInsightsBySection(cards)
      expect(groups.map((g) => g.section.key)).toEqual(['security', OTHER_SECTION.key])
      expect(groups.find((g) => g.section.key === OTHER_SECTION.key)!.cards.map((c) => c.id).sort()).toEqual(['2', '3'])
    })

    it('groups skeleton-shaped entries (no severity) by title', () => {
      const skeletons = [{ id: 'b', title: 'Beta', category: 'origin' }, { id: 'a', title: 'Alpha', category: 'origin' }]
      const groups = groupInsightsBySection(skeletons)
      expect(groups).toHaveLength(1)
      expect(groups[0].section.key).toBe('origin')
      expect(groups[0].cards.map((c) => c.id)).toEqual(['a', 'b'])
    })

    it('every declared section key is a distinct backend category', () => {
      const keys = INSIGHT_SECTIONS.map((s) => s.key)
      expect(new Set(keys).size).toBe(keys.length)
      expect(keys).toEqual(['security', 'origin', 'edge', 'network', 'traffic'])
    })
  })

  describe('summarizeSeverities', () => {
    it('counts per severity, worst-first, omitting zero counts', () => {
      const cards = [
        { severity: 'warning' }, { severity: 'critical' }, { severity: 'warning' },
        { severity: 'warning' }, { severity: 'critical' }, { severity: 'clean' },
      ]
      expect(summarizeSeverities(cards)).toEqual([
        { severity: 'critical', count: 2 },
        { severity: 'warning', count: 3 },
        { severity: 'clean', count: 1 },
      ])
    })

    it('orders chips by the severity ramp regardless of input order', () => {
      const cards = [{ severity: 'clean' }, { severity: 'info' }, { severity: 'error' }, { severity: 'critical' }]
      expect(summarizeSeverities(cards).map((s) => s.severity)).toEqual(['critical', 'error', 'info', 'clean'])
    })

    it('ignores unknown and missing severities (skeleton entries → [])', () => {
      expect(summarizeSeverities([{ severity: 'mystery' }, { severity: null }, {}])).toEqual([])
      expect(summarizeSeverities([{ title: 'Alpha', category: 'origin' }])).toEqual([])
    })

    it('returns [] for an empty section', () => {
      expect(summarizeSeverities([])).toEqual([])
    })
  })

  const STATIC_DEFAULT = { window: '24', baseline: '168' } as const
  const WIN = new Set(WINDOW_OPTIONS.map((o) => o.value))
  const BASE = new Set(BASELINE_OPTIONS.map((o) => o.value))

  describe('pickInsightsDefault', () => {
    it('returns the static default when history is null or NaN', () => {
      expect(pickInsightsDefault(null)).toEqual(STATIC_DEFAULT)
      expect(pickInsightsDefault(NaN)).toEqual(STATIC_DEFAULT)
    })

    it.each([
      [0.5, '0.25', '1'], [2, '1', '1'], [12, '4', '1'], [36, '4', '24'],
      [96, '24', '24'], [200, '24', '168'], [800, '24', '168'],
      [0.999, '0.25', '1'], [1, '1', '1'], [3.999, '1', '1'], [4, '4', '1'],
      [23.999, '4', '1'], [24, '4', '24'], [47.999, '4', '24'], [48, '24', '24'],
      [167.999, '24', '24'], [168, '24', '168'], [719.999, '24', '168'], [720, '24', '168'],
    ])('history %sh -> window %s / baseline %s', (h, w, b) => {
      expect(pickInsightsDefault(h)).toEqual({ window: w, baseline: b })
    })

    it('only ever returns real dropdown option values', () => {
      for (const h of [null, 0, 0.5, 1, 2, 4, 12, 24, 36, 96, 168, 200, 720, 2000, NaN]) {
        const r = pickInsightsDefault(h as number | null)
        expect(WIN.has(r.window)).toBe(true)
        expect(BASE.has(r.baseline)).toBe(true)
      }
    })
  })

  describe('historyHoursFromExtents', () => {
    const NOW = Date.parse('2026-06-15T12:00:00.000Z')

    it('returns null for null/undefined/empty/unparseable earliest', () => {
      for (const v of [null, undefined, '', 'not-a-date'] as const) {
        expect(historyHoursFromExtents(v, NOW)).toBeNull()
      }
    })

    it('computes hours from ISO earliest, widens date-only to UTC start-of-day, and clamps future to 0', () => {
      expect(historyHoursFromExtents('2026-06-15T10:00:00.000Z', NOW)).toBeCloseTo(2, 6)
      expect(historyHoursFromExtents('2026-06-14', NOW)).toBeCloseTo(36, 6)
      expect(historyHoursFromExtents('2026-06-15T13:00:00.000Z', NOW)).toBe(0)
    })
  })
})
