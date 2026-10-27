import { act, cleanup, render, renderHook, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'
import {
  createTestQueryClient, renderWithQuery, renderWithQueryClient,
  stubMatchMedia, stubResizeObserver, stubGetAnimations, stubPointerCaptureAndScroll,
  stubWindowLocationAssign,
} from '../helpers/page-smoke'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import React from 'react'
import type { PlotlyLayout, PlotlyTrace, SessionsResponse } from '@/types/api'
import type { components } from '@/types/api.generated'
import {
  INSIGHT_SECTIONS, OTHER_SECTION, categoryToSection, sortInsights, groupInsightsBySection,
  summarizeSeverities, pickInsightsDefault, historyHoursFromExtents, WINDOW_OPTIONS, BASELINE_OPTIONS,
} from '@/lib/utils'
import AlertsClient, { AlertPreview, CreateAlertForm } from '@/app/alerts/_sections/AlertsClient'
import InsightsClient from '@/app/insights/_sections/InsightsClient'
import ShareLoginPage, { ShareLoginForm, safeReturnTarget, AcknowledgeButton, AcknowledgeFallback } from '@/app/share-login/page'
import ShareLoginLoading from '@/app/share-login/loading'
import { SessionDetail, SessionsTable, ScoringControls } from '@/app/sessions/page'
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
  realApiRef, notFoundMock, apiGet, apiPost, postMock, pushSpy, replaceSpy, locationAssignSpy,
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
  const wrap = <T extends (...args: never[]) => unknown>(fn: T) => (...args: Parameters<T>) => mockScoringSubcomponents ? null : fn(...args)
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
  const wrap = <T extends (...args: never[]) => unknown>(fn: T) => (...args: Parameters<T>) => mockScoringSubcomponents ? null : fn(...args)
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

import AdminPage from '@/app/admin/page'
import Loading from '@/app/admin/loading'
import AdminQueuePage from '@/app/admin/queue/page'
import QueueLoading from '@/app/admin/queue/loading'
import ShareDashboardPage from '@/app/admin/share/page'
import SessionScoringPage from '@/app/admin/session-scoring/page'
import RumAdminPage from '@/app/admin/rum/page'
import RumLoading from '@/app/admin/rum/loading'
import QueryMonitorPage from '@/app/admin/queries/page'
import QueriesLoading from '@/app/admin/queries/loading'
import ClickHouseLoading from '@/app/admin/clickhouse/loading'
import ClickHouseAdminPage from '@/app/admin/clickhouse/page'
import { ClickHouseAdminClient, ClickHouseStatusNotice } from '@/app/admin/clickhouse/page'
import { OperationsOverview, AdminTrendsClient, ServicesTable } from '@/app/admin/_sections/OperationsOverview'
import { QuarantineSection } from '@/app/control-room/_sections/ControlRoomClient'
import TrendsLoading from '@/app/admin/trends/loading'
import { getApiBase } from '@/lib/api'
import { server } from '@/__tests__/helpers/msw/handlers'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { CreateInviteDialog } from '@/components/share-dashboard/CreateInviteDialog'
import { useTableSort, type ShareStatus, type SortAccessors } from '@/components/share-dashboard/SharingControlPanel'
import { StatusPanel, L2EnforcementCard } from '@/components/SessionScoring/StatusPanel'
import { LabelsTab, AuditLogTab, MatrixVersionsCard } from '@/components/SessionScoring/ScoringAdminPanels'

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

test('Loading skeleton and AdminPage render zero-CLS contain-intrinsic-size reservations', () => {
  const { unmount } = render(<Loading />)
  expect(screen.getByTestId('admin-overview-loading')).toBeInTheDocument()
  expect(screen.getByText('Inspecting background scheduler state...')).toBeInTheDocument()
  expect(screen.getByText('Loading service configs...')).toBeInTheDocument()
  unmount()

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  const { container } = renderWithQuery(<AdminPage />, { queryClient })
  for (const id of [
    'admin-overview-root', 'admin-operations-reservation', 'admin-services-reservation',
    'admin-health-reservation', 'admin-quarantine-reservation',
    'admin-global-settings-reservation', 'admin-pricing-reservation',
  ]) {
    expect(screen.getByTestId(id)).toBeInTheDocument()
  }
  expect(container.querySelectorAll('[class*="contain-intrinsic-size"]').length).toBeGreaterThanOrEqual(6)
})

test('OperationsOverview renders live query, ingest gap, slow query, and worker status cards', async () => {
  server.use(
    http.get('/api/admin/queries/summary', () => HttpResponse.json({ active_total: 2, by_db_type: { duckdb: 2 }, recent_count: 15 })),
    http.get('/api/admin/log-accounting', () => HttpResponse.json({
      configured: true, service_id: 'test-svc', hours: 24, buckets: [],
      totals: { requests: 1000, log_lines: 1000, log_bytes: 50000, ingested_rows: 1000, gap: 0, gap_pct: 0 }, sustained_loss: null,
    })),
    http.get('/api/admin/slow-queries/count', () => HttpResponse.json({ count: 0, since_hours: 24, threshold_ms: 1000 })),
    http.get('/api/admin/celery/status', () => HttpResponse.json({
      workers: { count: 4 }, queues: { 'q.ingest': 0, 'q.control': 0 }, ledger: { dead_letter: 0, quarantined: 0 },
    })),
  )
  renderWithQuery(<OperationsOverview />, { queryClient: createTestQueryClient({ queries: { staleTime: 0 } }) })
  for (const label of ['Live Queries', 'Ingest Gap', 'Notable Slow Queries', 'Workers & Queues']) expect(screen.getByText(label)).toBeInTheDocument()
  await waitFor(() => {
    for (const txt of ['0%', 'healthy · 24h', '4 workers']) expect(screen.getByText(txt)).toBeInTheDocument()
  })
})

test('ServicesTable renders services, manual Refresh Status trigger, and Delete Data / Teardown menu order', async () => {
  let fetchCount = 0
  server.use(
    http.get(`${API_BASE}/api/services`, () => {
      fetchCount += 1
      return HttpResponse.json({ services: [{ service_id: 'test-svc', name: 'Production Edge CDN', fos_bucket: 'prod-logs-bucket' }] })
    }),
  )
  const user = userEvent.setup()
  renderWithQuery(<ServicesTable />, { queryClient: createTestQueryClient({ queries: { staleTime: 0 } }) })
  await waitFor(() => {
    for (const txt of ['Production Edge CDN', 'test-svc', 'prod-logs-bucket']) expect(screen.getByText(txt)).toBeInTheDocument()
  })
  await user.click(screen.getByTestId('admin-overview-refresh-btn'))
  await waitFor(() => expect(fetchCount).toBeGreaterThanOrEqual(2))

  await user.click(screen.getByRole('button', { name: /manage/i }))
  const deleteDataItem = await screen.findByText('Delete Data')
  expect(deleteDataItem.compareDocumentPosition(screen.getByText('Teardown Service')) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  await user.click(deleteDataItem)
  expect(await screen.findByText('Delete Data: Production Edge CDN')).toBeInTheDocument()
})

const evidence = {
  id: 17, source_type: 'request', original_key: 'raw/request/example.log.gz', line_ordinal: 9, byte_offset: 128,
  byte_length: 4, error_category: 'invalid_json', error_text: 'Unexpected end of JSON input', sha256: 'a'.repeat(64), quarantined_at: '2026-09-29T12:00:00Z',
}
const jsonResponse = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })

describe('QuarantineSection', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service', isInitialized: true })
    adminFetchSpy.mockImplementation(async (input: string) => {
      if (input.includes('/summary')) return jsonResponse({ total_items: 1, total_bytes: 4, request_items: 1, rum_items: 0, oldest_at: evidence.quarantined_at, newest_at: evidence.quarantined_at, category_counts: { invalid_json: 1 } })
      if (input.includes('/download/')) return new Response(new Uint8Array([0x00, 0xff, 0x7b, 0x7d]))
      if (input.includes('/purge')) return jsonResponse({ purged: 1 })
      return jsonResponse({ items: [evidence], total: 1 })
    })
  })
  afterEach(() => { if (realApiRef.adminFetch) adminFetchSpy.mockImplementation(realApiRef.adminFetch) })

  test('shows evidence metadata, downloads exact-byte item, purges individual item, and handles query errors', async () => {
    const user = userEvent.setup()
    const createObjectURL = vi.fn(() => 'blob:quarantine-test')
    vi.stubGlobal('URL', { ...URL, createObjectURL, revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    const { unmount } = renderWithQuery(<QuarantineSection />)

    await user.click(await screen.findByRole('button', { name: 'Details' }))
    expect(await screen.findByText('raw/request/example.log.gz')).toBeInTheDocument()
    expect(screen.getByText('Unexpected end of JSON input')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Download' }))
    await waitFor(() => expect(adminFetchSpy).toHaveBeenCalledWith('/api/admin/quarantine/download/17'))
    expect(createObjectURL).toHaveBeenCalledOnce()

    await user.click(await screen.findByRole('button', { name: 'Remove' }))
    await waitFor(() => expect(adminFetchSpy).toHaveBeenCalledWith('/api/admin/quarantine/purge?item_id=17', { method: 'POST' }))
    vi.unstubAllGlobals()
    unmount()

    adminFetchSpy.mockRejectedValueOnce(new Error('metadata unavailable'))
    renderWithQuery(<QuarantineSection />)
    expect(await screen.findByText('Quarantine unavailable')).toBeInTheDocument()
    expect(screen.getByText('metadata unavailable')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })
})

const SAMPLE_SERIES = {
  cpu_load_1m: [{ ts: '2026-04-19T12:00:00Z', value: 0.45 }, { ts: '2026-04-19T12:01:00Z', value: 0.82 }],
  mem_used_pct: [{ ts: '2026-04-19T12:00:00Z', value: 52.0 }, { ts: '2026-04-19T12:01:00Z', value: 54.3 }],
  disk_used_pct: [{ ts: '2026-04-19T12:01:00Z', value: 31.2 }],
  disk_used_pct_root: [{ ts: '2026-04-19T12:01:00Z', value: 44.8 }],
  active_query_count: [{ ts: '2026-04-19T12:01:00Z', value: 2.0 }],
  'pool_wait_p95_ms|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 4.2 }],
  'pool_wait_p95_ms|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 11.5 }],
  'ingest_lag_s|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 18.0 }],
  'ingest_lag_s|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 7.0 }],
}

describe('/admin/trends Page & Client', () => {
  beforeEach(() => { clientGetSpy.mockReset() })
  afterEach(() => { if (realApiRef.clientGet) clientGetSpy.mockImplementation(realApiRef.clientGet) })

  it('renders loading.tsx and pending skeleton reservations with zero-CLS geometry', () => {
    const { unmount } = render(<TrendsLoading />)
    expect(screen.getByTestId('admin-trends-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-trends-toolbar').className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(screen.getByTestId('admin-trends-grid-reservation').className).toContain('[contain-intrinsic-size:auto_680px]')
    unmount()

    clientGetSpy.mockImplementation(() => new Promise(() => {}))
    renderWithQuery(<AdminTrendsClient />)
    expect(screen.getByTestId('admin-trends-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-trends-grid-reservation').className).toContain('[contain-intrinsic-size:auto_680px]')
  })

  it('renders all 7 vital series cards, switches lookback windows, and triggers manual Refresh', async () => {
    clientGetSpy.mockResolvedValue({ data: { series: SAMPLE_SERIES }, response: new Response(null, { status: 200 }) })
    renderWithQuery(<AdminTrendsClient />)
    await waitFor(() => expect(screen.getByTestId('admin-trends-latest-cpu_load_1m')).toHaveTextContent('0.82'))
    for (const [k, v] of [['mem_used_pct', '54.3%'], ['disk_used_pct', '31.2%'], ['disk_used_pct_root', '44.8%'], ['active_query_count', '2.00'], ['pool_wait_p95_ms', '11.5ms'], ['ingest_lag_s', '18s']] as const) {
      expect(screen.getByTestId(`admin-trends-latest-${k}`)).toHaveTextContent(v)
    }

    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = screen.getByTestId(`admin-trends-window-${win}`)
      fireEvent.click(btn)
      await waitFor(() => expect(clientGetSpy).toHaveBeenCalledWith(
        '/api/admin/metric-history/batch',
        expect.objectContaining({ params: { query: { since: win } } }),
      ))
      expect(btn).toHaveAttribute('aria-pressed', 'true')
    }

    const callsBeforeRefresh = clientGetSpy.mock.calls.length
    fireEvent.click(screen.getByTestId('admin-trends-refresh-btn'))
    await waitFor(() => expect(clientGetSpy.mock.calls.length).toBeGreaterThan(callsBeforeRefresh))
  })
})

const SVC_RUM = 'svc-admin-rum-test'

describe('Admin RUM Configuration Page (/admin/rum)', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_RUM, services: [{ id: SVC_RUM, name: 'Test RUM Service' }], isInitialized: true })
  })

  it('renders route-level and in-place skeletons with zero-CLS reservations', () => {
    const { unmount } = render(<RumLoading />)
    expect(screen.getByTestId('admin-rum-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-toolbar-skeleton')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-skeleton-card')).toBeInTheDocument()
    unmount()

    server.use(
      http.get('*/api/services/:service_id/rum/status', async () => {
        await new Promise((r) => setTimeout(r, 200))
        return HttpResponse.json({ enabled: false })
      }),
      http.get('*/api/services', () => HttpResponse.json({ services: [{ service_id: SVC_RUM, name: 'Test RUM Service' }] })),
    )
    renderWithQuery(<RumAdminPage />)
    expect(screen.getByTestId('admin-rum-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-skeleton')).toBeInTheDocument()
  })

  it('renders enabled and disabled RUM states, collectors summary, Faro version card, and manual refresh', async () => {
    server.use(
      http.get('*/api/services/:service_id/rum/status', () => HttpResponse.json({
        enabled: true, enabled_at: '2026-10-18T10:00:00Z', deployed_vcl_sha: 'sha-123', current_vcl_sha: 'sha-123',
        vcl_drift: false, capture_vitals: true, capture_performance: true, capture_errors: true, capture_events: false,
        custom_condition: 'req.url !~ "^/internal"',
      })),
      http.get('*/api/services/:service_id/rum/versions', () => HttpResponse.json({
        available: ['2.3.1', '2.2.0'], current: '2.2.0', latest: '2.3.1', update_available: true,
      })),
      http.get('*/api/services', () => HttpResponse.json({ services: [{ service_id: SVC_RUM, name: 'Test RUM Service' }] })),
    )

    const { unmount } = renderWithQuery(<RumAdminPage />)
    await waitFor(() => expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('✅ Enabled'))
    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent('Beacon Pipeline Active')
    expect(screen.getByTestId('admin-rum-vcl-drift')).toHaveTextContent('✅ No')
    expect(screen.getByTestId('admin-rum-capture-summary')).toHaveTextContent('Vitals, Performance, Errors')
    expect(screen.getByTestId('admin-rum-enable-btn')).toBeDisabled()
    expect(screen.getByTestId('admin-rum-disable-btn')).not.toBeDisabled()
    await waitFor(() => expect(screen.getByTestId('admin-rum-faro-version')).toHaveTextContent('2.2.0'))
    unmount()

    let statusCalls = 0
    server.use(
      http.get('*/api/services/:service_id/rum/status', () => {
        statusCalls += 1
        return HttpResponse.json({ enabled: false, vcl_drift: false })
      }),
    )
    renderWithQuery(<RumAdminPage />)
    await waitFor(() => expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('❌ Disabled'))
    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent('Beacon Pipeline Inactive')
    const callsBeforeRefresh = statusCalls
    fireEvent.click(screen.getByTestId('admin-rum-refresh-btn'))
    await waitFor(() => expect(statusCalls).toBeGreaterThan(callsBeforeRefresh))
  })
})

describe('Admin Live Query Monitor (/admin/queries)', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders Loading skeleton and pending QueryMonitorPage with zero-CLS reservations', () => {
    const { unmount } = render(<QueriesLoading />)
    expect(screen.getByTestId('admin-queries-loading')).toBeDefined()
    expect(screen.getByTestId('admin-queries-summary-skeleton').className).toContain('[contain-intrinsic-size:auto_36px]')
    unmount()

    adminJsonSpy.mockImplementation((_spec: string, route: string) =>
      route === '/api/admin/app-config/query-monitor' ? Promise.resolve({ enabled: true }) : new Promise(() => {}),
    )
    renderWithQuery(<QueryMonitorPage />, { wrapper: withNuqsTestingAdapter() })
    for (const id of [
      'admin-queries-summary-reservation', 'admin-queries-active-reservation',
      'admin-queries-slow-reservation', 'admin-queries-completed-reservation',
    ]) {
      expect(screen.getByTestId(id).className).toContain('[contain-intrinsic-size:auto_')
    }
    expect(screen.getByTestId('admin-queries-active-skeleton')).toBeDefined()
  })

  it('renders populated queries, supports pause/refresh controls, and hydrates shared link filters', async () => {
    const nowSec = Math.floor(Date.now() / 1000)
    adminJsonSpy.mockImplementation(async (spec: string, route: string) => {
      if (spec === '/api/admin/app-config/query-monitor' || route === '/api/admin/app-config/query-monitor') return { enabled: true }
      if (spec === '/api/admin/queries' || route.startsWith('/api/admin/queries')) {
        return {
          last_seq: 12,
          active: [{
            query_id: 11, db_type: 'DuckDB', sql_preview: 'SELECT count(*) FROM logs WHERE status >= 500',
            sql: 'SELECT count(*) FROM logs WHERE status >= 500', sql_len: 46, service_id: 'svc_alpha',
            started_at_utc: nowSec - 2, duration_ms: 1450.0, cancellable: true, cancelled_at: null,
            attribution: {
              kind: 'admin', label: 'admin @ 127.0.0.1', principal_id: '127.0.0.1',
              caller_qualname: 'dashboard.aggregates', caller_file: 'backend/routers/dashboard.py:42',
              request_path: '/api/dashboard/aggregates', request_id: 'req-11', cron_job: null, cron_run_id: null, pool_slot: 'pool#0',
            },
          }],
          completed: [{
            query_id: 12, db_type: 'Postgres', sql_preview: 'SELECT * FROM slow_queries WHERE service_id = $1',
            sql: 'SELECT * FROM slow_queries WHERE service_id = $1', sql_len: 49, service_id: 'svc_alpha',
            started_at_utc: nowSec - 20, ended_at_utc: nowSec - 19, duration_ms: 820.0, outcome: 'ok',
            error_type: null, error_message: null, peak_memory_mb: 18.2,
            attribution: {
              kind: 'cron', label: 'cron:full_sync', principal_id: null,
              caller_qualname: 'slow_queries.list_slow_queries', caller_file: 'backend/core/metadata/slow_queries.py:187',
              request_path: null, request_id: null, cron_job: 'full_sync', cron_run_id: 42, pool_slot: null,
            },
          }],
        }
      }
      if (spec === '/api/admin/slow-queries' || route.startsWith('/api/admin/slow-queries')) {
        return { rows: [], since_hours: 24, threshold_ms: 500 }
      }
      return {}
    })

    const { unmount } = renderWithQuery(<QueryMonitorPage />, { wrapper: withNuqsTestingAdapter() })
    expect(await screen.findByText('1 active query')).toBeDefined()
    expect(screen.getByText(/longest:\s*1\.45 s/)).toBeDefined()
    fireEvent.click(screen.getByRole('button', { name: /Pause/i }))
    expect(await screen.findByRole('button', { name: /Resume/i })).toBeDefined()
    fireEvent.click(screen.getByTestId('admin-queries-refresh-btn'))
    unmount()

    adminJsonSpy.mockImplementation(async (_spec: string, route: string) =>
      route === '/api/admin/app-config/query-monitor' ? { enabled: true } : new Promise(() => {}),
    )
    renderWithQuery(<QueryMonitorPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: '?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1' }),
    })
    expect(await screen.findByDisplayValue('COMPACT')).toBeDefined()
    expect(screen.getByRole('tab', { name: /live only/i }).getAttribute('aria-selected')).toBe('true')
  })
})

describe('/admin/clickhouse page & ClickHouseAdminClient', () => {
  beforeEach(() => {
    notFoundMock.mockClear()
    vi.unstubAllEnvs()
    useServiceStore.setState({ activeServiceId: 'svc-test', services: [{ id: 'svc-test', name: 'Test Service' }] })
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('enforces Standard vs High-Scale deployment mode on loading shell, page, and status notices', async () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'standard')
    expect(() => render(<ClickHouseLoading />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')
    expect(() => renderWithQueryClient(<ClickHouseAdminPage />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')

    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    const { unmount: uLoad } = render(<ClickHouseLoading />)
    for (const id of [
      'admin-clickhouse-loading', 'admin-clickhouse-toolbar', 'admin-clickhouse-vitals-reservation',
      'admin-clickhouse-generation-reservation', 'admin-clickhouse-replay-reservation',
    ]) {
      expect(screen.getByTestId(id)).toBeInTheDocument()
    }
    uLoad()

    const { rerender, unmount: uNotice } = render(<ClickHouseStatusNotice />)
    const disabled = screen.getByTestId('admin-clickhouse-disabled-banner')
    const unavailable = screen.getByTestId('admin-clickhouse-unavailable-banner')
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    rerender(<ClickHouseStatusNotice enabled={false} health="disabled" />)
    expect(disabled).toHaveAttribute('aria-hidden', 'false')
    rerender(<ClickHouseStatusNotice enabled health="unavailable" />)
    expect(unavailable).toHaveAttribute('aria-hidden', 'false')
    rerender(<ClickHouseStatusNotice enabled health="ok" />)
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    uNotice()

    renderWithQueryClient(<ClickHouseAdminPage />)
    await waitFor(() => expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('disabled'))
    expect(screen.getByTestId('admin-clickhouse-replay-submit-btn')).toBeDisabled()
  })

  it('renders High-Scale ok state, dry-run replay workflow, and 503 unavailable state gracefully', async () => {
    server.use(
      http.get('*/api/admin/clickhouse/status', () => HttpResponse.json({
        service_id: 'svc-test', enabled: true, health: 'ok', schema_version: 2,
        publication_counts: { pending: 2, claimed: 1, failed: 0, published: 14 },
        oldest_pending_age_seconds: 18.4, last_published_at: '2026-10-20T12:00:00Z',
        active_generation: {
          generation: 'gen-active-01', dataset_id: 'ds-active-01',
          coverage_start: '2026-10-20T10:00:00Z', coverage_end: '2026-10-20T12:00:00Z',
          expires_at: '2026-10-21T12:00:00Z', expired: false, target_matches: true, coverage_age_seconds: 120,
        },
        expired_dataset_count: 3,
      })),
      http.post('*/api/admin/clickhouse/replay', async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({
          service_id: body.service_id, dry_run: body.dry_run, operation: body.dataset_id ? 'rebuild' : 'resume',
          dataset_id: (body.dataset_id as string) || 'ds-active-01', generation: (body.generation as string) || 'gen-active-01',
          artifact_count: 5, planned_artifacts: 5, attempted: body.dry_run ? 0 : 5, published: body.dry_run ? 0 : 5,
          activated: !body.dry_run, limit: body.limit ?? 100,
        })
      }),
    )

    const { unmount } = renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)
    await waitFor(() => expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('ok'))
    expect(screen.getByTestId('admin-clickhouse-vital-enabled')).toHaveTextContent('High-Scale (Enabled)')
    expect(screen.getByTestId('admin-clickhouse-vital-counts')).toHaveTextContent('14 / 2')
    expect(screen.getByTestId('admin-clickhouse-gen-id')).toHaveTextContent('gen-active-01')

    fireEvent.click(screen.getByTestId('admin-clickhouse-prefill-btn'))
    expect((screen.getByTestId('admin-clickhouse-reference-input') as HTMLInputElement).value).toBe('ds-active-01')
    fireEvent.click(screen.getByTestId('admin-clickhouse-replay-submit-btn'))
    await waitFor(() => expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toHaveTextContent('Dry-Run Replay Preview'))
    unmount()

    server.use(
      http.get('*/api/admin/clickhouse/status', () => HttpResponse.json({
        service_id: 'svc-test', enabled: true, health: 'unavailable', schema_version: 1,
        publication_counts: { pending: 0, claimed: 0, failed: 1, published: 4 },
        oldest_pending_age_seconds: null, last_published_at: null, active_generation: null, expired_dataset_count: 0,
      }, { status: 503 })),
    )
    renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)
    await waitFor(() => expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('unavailable'))
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toHaveAttribute('aria-hidden', 'false')
  })
})

const shareStatusFixture = {
  services: [{ service_id: 'svcA', name: 'Service A' }],
  invites: [
    { id: 'i1', name: 'Alice', email: 'alice@corp.com', service_ids: ['svcA'], expires_at: null, revoked: 0, auth_method: 'passcode', last_login_at: '2026-06-05T09:30:00Z' },
    { id: 'i2', name: 'Bob', email: 'bob@corp.com', service_ids: [], expires_at: null, revoked: 0, auth_method: 'passcode', last_login_at: null },
  ],
  sessions: [
    { session_id: 's1', invite_id: 'i1', email: 'alice@corp.com', ip_address: '1.2.3.4', last_active_time: '2026-06-05T10:00:00Z' },
  ],
} as ShareStatus

const bodyRows = () => screen.getAllByRole('row').slice(1)
const stubClipboard = (writeText: () => Promise<void>) =>
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
const hasCheck = (btn: HTMLElement) => btn.querySelector('svg.lucide-check') !== null

describe('InvitationsPanel, CreateInviteDialog & useTableSort', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  test('InvitationsPanel renders status, sorts columns, and handles clipboard copy success/failure', async () => {
    const user = userEvent.setup()
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
    expect(screen.getAllByLabelText('Online now')).toHaveLength(1)
    expect(screen.getByTitle('Service A (svcA)')).toBeInTheDocument()
    expect(bodyRows()[0]).toHaveTextContent('Alice')

    await user.click(screen.getByRole('button', { name: 'Name' }))
    expect(bodyRows()[0]).toHaveTextContent('Bob')
    await user.click(screen.getByRole('button', { name: 'Name' }))
    expect(bodyRows()[0]).toHaveTextContent('Alice')

    vi.useFakeTimers()
    const writeText = vi.fn().mockResolvedValue(undefined)
    stubClipboard(writeText)
    const [alice, bob] = screen.getAllByTitle('Copy share card')
    await act(async () => { fireEvent.click(alice) })
    expect(hasCheck(alice)).toBe(true)
    expect(hasCheck(bob)).toBe(false)
    await act(async () => { vi.advanceTimersByTime(1500) })
    expect(hasCheck(alice)).toBe(false)

    stubClipboard(vi.fn().mockRejectedValue(new Error('denied')))
    await act(async () => { fireEvent.click(alice) })
    expect(hasCheck(alice)).toBe(false)
  })

  test('CreateInviteDialog handles allow_concurrent_sessions and OAuth provider modes', async () => {
    useServiceStore.setState({ activeServiceId: 'svcA' })
    const shareServices: ShareStatus['services'] = [{ service_id: 'svcA', name: 'Service A' }]
    let posted: { allow_concurrent_sessions?: boolean } | undefined
    server.use(
      http.get('*/api/admin/share/oauth-providers', () => HttpResponse.json({ providers: [] })),
      http.post('*/api/admin/share/invites', async ({ request }) => {
        posted = (await request.json()) as { allow_concurrent_sessions?: boolean }
        return HttpResponse.json({ id: 'i1' })
      }),
    )

    const fillRequired = async (user: ReturnType<typeof userEvent.setup>) => {
      await user.type(screen.getByLabelText('Name'), 'Jane')
      await user.type(screen.getByLabelText('Email'), 'jane@example.com')
      await user.type(screen.getByLabelText('Passcode'), 'ocean-breeze-cabin-42')
      await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
    }

    const user = userEvent.setup()
    const { unmount: u1 } = render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)
    await fillRequired(user)
    await user.click(screen.getByRole('checkbox', { name: /multiple people to use this link/i }))
    await user.click(screen.getByRole('button', { name: /create invite/i }))
    await waitFor(() => expect(posted?.allow_concurrent_sessions).toBe(true))
    u1()

    posted = undefined
    const { unmount: u2 } = render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)
    await fillRequired(user)
    await user.click(screen.getByRole('button', { name: /create invite/i }))
    await waitFor(() => expect(posted?.allow_concurrent_sessions).toBe(false))
    u2()

    server.use(http.get('/api/admin/share/oauth-providers', () => HttpResponse.json({ providers: [{ id: 'google', display_name: 'Google Workspace', enabled: true }] })))
    const { unmount: u3 } = render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)
    await user.click(screen.getByRole('radio', { name: /SSO/i }))
    expect(screen.queryByLabelText('Passcode')).toBeNull()
    expect(await screen.findByLabelText('Identity provider')).toBeInTheDocument()
    u3()

    server.use(http.get('/api/admin/share/oauth-providers', () => HttpResponse.json({ providers: [] })))
    render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)
    await user.click(screen.getByRole('radio', { name: /SSO/i }))
    expect(await screen.findByText(/add one to the registry first/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create invite/i })).toBeDisabled()
  })

  test('useTableSort sorts ascending/descending, keeps nulls last, and resets direction on key change', () => {
    type SortRow = { name: string; n: number | null }
    const sortRows: SortRow[] = [{ name: 'bravo', n: 2 }, { name: 'alpha', n: null }, { name: 'charlie', n: 1 }]
    const sortAccessors: SortAccessors<SortRow> = { name: (r) => r.name, n: (r) => r.n }
    const names = (rs: SortRow[]) => rs.map((r) => r.name)

    const { result } = renderHook(() => useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }))
    expect(names(result.current.sorted)).toEqual(['alpha', 'bravo', 'charlie'])
    act(() => result.current.toggle('name'))
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
    act(() => result.current.toggle('n'))
    expect(result.current.sortDir).toBe('desc')
    expect(names(result.current.sorted)).toEqual(['bravo', 'charlie', 'alpha'])
    act(() => result.current.toggle('n'))
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })
})

describe('SessionScoring Admin Panels: StatusPanel, AuditLogTab, MatrixVersionsCard, LabelsTab, L2EnforcementCard', () => {
  const SVC = 'svc-scoring-test'

  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC, isInitialized: true })
  })

  it('StatusPanel gates on preflight_pragmas_ready without leaking internal pragma names, then opens SSEModal when ready', async () => {
    let statusCalls = 0
    let readyState = false
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () => {
        statusCalls += 1
        return HttpResponse.json({ enabled: false, preflight_pragmas_ready: readyState })
      }),
    )

    const user = userEvent.setup()
    const { queryClient, unmount } = renderWithQuery(<StatusPanel serviceId={SVC} />)
    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()
    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))

    const modal = await screen.findByTestId('admin-session-scoring-pragma-modal')
    expect(modal.textContent).toContain(SVC)
    expect(modal.textContent).not.toContain('fix_unsent_body_drain')
    expect(modal.textContent).not.toContain('no_body_if_bereq_is_get_or_head')
    expect(modal.textContent).not.toContain('hctl')

    readyState = true
    await user.click(screen.getByTestId('admin-session-scoring-pragma-recheck-btn'))
    await waitFor(() => expect(statusCalls).toBeGreaterThanOrEqual(2))
    await waitFor(() => expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument())

    act(() => { queryClient.setQueryData(['scoring-status', SVC], { enabled: false, preflight_pragmas_ready: false }) })
    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()
    expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument()
    unmount()

    renderWithQuery(<StatusPanel serviceId={SVC} />)
    const enableBtn = await screen.findByTestId('admin-session-scoring-enable-btn')
    await user.click(enableBtn)
    expect(await screen.findByRole('heading', { name: 'Enable Session Scoring' })).toBeInTheDocument()
  })

  it('renders AuditLogTab, MatrixVersionsCard, and LabelsTab with populated and empty states', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () => HttpResponse.json({
        audit: [
          { id: 1, timestamp: '2026-10-01T00:00:00Z', action: 'retrain', actor: 'admin', details: { a: 1 } },
          { id: 2, timestamp: '2026-10-01T01:00:00Z', action: 'enable', actor: null, details: null },
        ],
      })),
      http.get(`${API_BASE}/api/services/:service_id/scoring/matrix-versions`, () => HttpResponse.json({
        current_version: 'v2',
        versions: [
          { version: 'v2', key: 'k2', size_bytes: 2048, last_modified: '2026-10-02T00:00:00Z' },
          { version: 'v1', key: 'k1', size_bytes: 1024, last_modified: '2026-10-01T00:00:00Z' },
        ],
      })),
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () => HttpResponse.json({
        labels: [{ id: 'l1', sid: 'sid-abc', label: 'bad', notes: 'orig', sample_url: '/x', sample_ip: '1.2.3.4', updated_at: '2026-10-01T00:00:00Z' }],
        counts: { good: 0, neutral: 0, bad: 1 },
      })),
    )

    const { unmount: uAudit } = renderWithQuery(<AuditLogTab serviceId={SVC} />)
    expect(await screen.findByText('retrain')).toBeTruthy()
    expect(screen.getByText('{"a":1}')).toBeTruthy()
    uAudit()

    const { unmount: uMatrix } = renderWithQuery(<MatrixVersionsCard serviceId={SVC} />)
    expect(await screen.findByText('current')).toBeTruthy()
    const restoreBtns = screen.getAllByRole('button', { name: /Restore/ })
    expect((restoreBtns[0] as HTMLButtonElement).disabled).toBe(true)
    expect((restoreBtns[1] as HTMLButtonElement).disabled).toBe(false)
    uMatrix()

    const user = userEvent.setup()
    const { unmount: uLabels } = renderWithQuery(<LabelsTab serviceId={SVC} />)
    expect(await screen.findByText('sid-abc')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Edit label' }))
    const notes = await screen.findByPlaceholderText('Notes (optional)')
    await user.clear(notes)
    await user.type(notes, 'hello')
    await waitFor(() => expect((screen.getByPlaceholderText('Notes (optional)') as HTMLTextAreaElement).value).toBe('hello'))
    uLabels()

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () => HttpResponse.json({ audit: [] })),
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () => HttpResponse.json({ labels: [], counts: { good: 0, neutral: 0, bad: 0 } })),
    )
    renderWithQuery(<><AuditLogTab serviceId={SVC} /><LabelsTab serviceId={SVC} /></>)
    expect(await screen.findByText(/No audit events yet/)).toBeTruthy()
    expect(await screen.findByText(/No labels yet/)).toBeTruthy()
  })

  it('L2EnforcementCard handles warmup warning, ready state, confirm PUT flow, and mid-ramp progress', async () => {
    const l2State = (overrides: Record<string, unknown> = {}) => ({
      available: true, enabled: false, l2_enabled_at: null, days_since_optin: null,
      ramp_progress: 0, fully_ramped: false, warmup_days_remaining: null, scoring_enabled_at: null,
      deployment_age_days: 0, ready: false, ramp_days: 3, readiness_days: 7, ...overrides,
    })

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/health`, () =>
        HttpResponse.json({ matrix_staleness: { l2_high_pct: 8.5, l2_evaluated: 1000 } }),
      ),
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 2, ready: false, enabled: false })),
      ),
    )

    const { unmount: u1 } = renderWithQuery(<L2EnforcementCard serviceId={SVC} sinceHours={24} />)
    expect(await screen.findByText(/not yet recommended/i)).toBeInTheDocument()
    expect(screen.getByRole('switch')).toBeEnabled()
    u1()

    let captured: { body: unknown; confirm: string | null } | null = null
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 10, ready: true, enabled: false })),
      ),
      http.put(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, async ({ request }) => {
        captured = { body: await request.json(), confirm: new URL(request.url).searchParams.get('confirm') }
        return HttpResponse.json({ ok: true, enabled: true, l2_enabled_at: 1700000000 })
      }),
    )
    const user = userEvent.setup()
    const { unmount: u2 } = renderWithQuery(<L2EnforcementCard serviceId={SVC} sinceHours={24} />)
    expect(await screen.findByText(/ready to enable/i)).toBeInTheDocument()
    expect(screen.getByText(/8\.5%/)).toBeInTheDocument()
    await user.click(screen.getByRole('switch'))
    expect(await screen.findByText(/enable l2 enforcement \(live\)/i)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /^enable$/i }))
    await waitFor(() => expect(captured).toEqual({ body: { enabled: true }, confirm: 'true' }))
    u2()

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({
          enabled: true, ready: true, deployment_age_days: 12, days_since_optin: 1,
          ramp_progress: 0.33, fully_ramped: false, warmup_days_remaining: 2, l2_enabled_at: 1700000000,
        })),
      ),
    )
    renderWithQuery(<L2EnforcementCard serviceId={SVC} sinceHours={24} />)
    expect(await screen.findByText(/l2 contributes to enforcement/i)).toBeInTheDocument()
    expect(screen.getByText(/fading in/i)).toBeInTheDocument()
    expect(screen.getByText(/33%/)).toBeInTheDocument()
    expect(screen.getByRole('switch')).toBeChecked()
  })
})


describe('AdminQueuePage (/admin/queue)', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders route-level loading skeleton and pending in-place skeletons with zero-CLS reservations', () => {
    const { unmount } = render(<QueueLoading />)
    expect(screen.getByTestId('admin-queue-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-toolbar-skeleton')).toBeInTheDocument()
    expect(screen.getAllByText(/Inspecting Celery task broker/i).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Loading queue telemetry/i).length).toBeGreaterThan(0)
    unmount()

    clientGetSpy.mockImplementation(() => new Promise(() => {}))
    renderWithQuery(<AdminQueuePage />)
    for (const id of [
      'admin-queue-root', 'admin-queue-trends-reservation', 'admin-queue-depths-reservation',
      'admin-queue-workers-reservation', 'admin-queue-ledger-reservation', 'admin-queue-schedules-reservation',
    ]) {
      expect(screen.getByTestId(id)).toBeInTheDocument()
    }
    expect(screen.getByTestId('admin-queue-refresh-btn')).toBeDisabled()
  })

  it('renders High-Scale queue depths, active workers, ingest ledger cards, in-process schedules, and supports manual Refresh', async () => {
    let statusCalls = 0
    let batchCalls = 0
    const okRes = (data: unknown) => ({ data, response: { ok: true, status: 200 } })

    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        statusCalls += 1
        return okRes({
          broker_reachable: true,
          workers: {
            count: 1,
            active_tasks: { 'celery@worker-1': [{ id: 'task-uuid-101', name: 'backend.tasks.ingest.ingest_batch', args: ['svc-alpha', 25], time_start: 1700000000 }] },
            stats: { 'celery@worker-1': {} },
            registered: { 'celery@worker-1': ['backend.tasks.ingest.ingest_batch'] },
            scheduled: { 'celery@worker-1': [] },
          },
          queues: { celery: 0, 'q.control': 2, 'q.ingest': 9 },
          inprocess_schedules: [{ name: 'log_discovery_svc-alpha', trigger: 'interval[0:00:10]', next_run_at: 1700000010 }],
          ledger: { discovered: 5, claimed: 2, committed: 140, quarantined: 1, dead_letter: 0 },
        })
      }
      if (path === '/api/admin/metric-history/batch') {
        batchCalls += 1
        return okRes({
          series: {
            celery_queue_depth: [{ ts: 1700000000, value: 4 }, { ts: 1700000060, value: 11 }],
            celery_active_tasks: [{ ts: 1700000000, value: 1 }, { ts: 1700000060, value: 1 }],
          },
        })
      }
      if (path === '/api/admin/celery/ledger') {
        return okRes({
          items: [{
            service_id: 'svc-alpha', object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/2026-10-20T04_28_00.000-test.log.gz',
            source_type: 'rum', status: 'quarantined', attempts: 1, error_kind: 'corrupt_gzip', last_error: 'Not a gzipped file',
            originating_task: 'rum_discovery', originating_run_id: 42, discovered_at: 1700000000,
          }],
          total: 1,
        })
      }
      return okRes({})
    })

    renderWithQuery(<AdminQueuePage />)
    await waitFor(() => expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent('Broker Reachable'))

    expect(screen.getByTestId('admin-queue-depths-table')).toBeInTheDocument()
    expect(screen.getByText('q.ingest')).toBeInTheDocument()
    expect(screen.getByText('q.control')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-workers-list')).toBeInTheDocument()
    expect(screen.getByText('celery@worker-1')).toBeInTheDocument()
    expect(screen.getByText('task-uuid-101')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-ledger-cards')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-ledger-discovered')).toHaveTextContent('5')
    expect(screen.getByTestId('admin-queue-ledger-committed')).toHaveTextContent('140')
    expect(screen.getByTestId('admin-queue-inprocess-schedules-table')).toBeInTheDocument()
    expect(screen.getByText('log_discovery_svc-alpha')).toBeInTheDocument()

    const refreshBtn = screen.getByTestId('admin-queue-refresh-btn')
    expect(refreshBtn).not.toBeDisabled()
    fireEvent.click(refreshBtn)
    await waitFor(() => {
      expect(statusCalls).toBeGreaterThanOrEqual(2)
      expect(batchCalls).toBeGreaterThanOrEqual(2)
    })
  })

  it('renders Standard mode fallback and Standard Mode in-process scheduler telemetry', async () => {
    const okRes = (data: unknown) => ({ data, response: { ok: true, status: 200 } })
    const emptyWorkers = { count: 0, active_tasks: {}, stats: {}, registered: {}, scheduled: {}, error: 'Valkey unreachable' }

    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return okRes({ broker_reachable: false, workers: emptyWorkers, queues: {}, inprocess_schedules: [], ledger: {} })
      }
      if (path === '/api/admin/metric-history/batch') return okRes({ series: {} })
      return okRes({})
    })

    const { unmount } = renderWithQuery(<AdminQueuePage />)
    await waitFor(() => expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent('Broker Unreachable / Standard Mode'))
    expect(screen.getByText(/No queues found or Celery mode is inactive/i)).toBeInTheDocument()
    expect(screen.getByText(/No active workers found/i)).toBeInTheDocument()
    expect(screen.getByText(/Ledger is empty/i)).toBeInTheDocument()
    expect(screen.getByText(/No in-process schedules registered/i)).toBeInTheDocument()
    unmount()

    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return okRes({
          broker_reachable: false, deployment_mode: 'standard',
          workers: { ...emptyWorkers, error: 'Celery broker not configured in standard mode' },
          queues: {},
          inprocess_schedules: [
            { name: 'log_discovery_svc-alpha', trigger: 'interval[0:00:10]', next_run_at: 1700000010 },
            { name: 'commit_svc-alpha', trigger: 'interval[0:05:00]', next_run_at: 1700000300 },
          ],
          inprocess_active_runs: [{ run_id: 101, service_id: 'svc-alpha', task: 'sync', started_at: 1700000000 }],
          standard_ingest: { ingested_files: 1250, quarantined_items: 2 },
          ledger: { committed: 1250, quarantined: 2, dead_letter: 0 },
        })
      }
      if (path === '/api/admin/metric-history/batch') return okRes({ series: {} })
      return okRes({})
    })

    renderWithQuery(<AdminQueuePage />)
    await waitFor(() => expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent('Standard Mode · In-Process Scheduler'))
    expect(screen.queryByText(/^Queue Depths$/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Total Queue Depth \(1h\)/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Inspect warning:/i)).not.toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-depths-reservation')).toBeInTheDocument()
    expect(screen.getByText(/Quarantined Files & Corrupt Lines \(2\)/i)).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-standard-ingest-summary')).toBeInTheDocument()
    expect(screen.getByText('1,250')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-standard-scheduler-summary')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-inprocess-active-table')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-inprocess-schedules-table')).toBeInTheDocument()
    expect(screen.getByText('log_discovery_svc-alpha')).toBeInTheDocument()
    expect(screen.getByText('commit_svc-alpha')).toBeInTheDocument()
  })

  it('displays dead-letter entries in the ledger inspector and handles Retry and Dismiss actions', async () => {
    const okRes = (data: unknown) => ({ data, response: { ok: true, status: 200 } })
    const deadKey = 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz'
    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return okRes({
          broker_reachable: true,
          workers: { count: 1, active_tasks: {}, stats: {}, registered: {}, scheduled: {} },
          queues: { celery: 0, 'q.ingest': 0, 'q.control': 0 },
          inprocess_schedules: [],
          ledger: { discovered: 0, claimed: 0, committed: 120, quarantined: 0, dead_letter: 3 },
        })
      }
      if (path === '/api/admin/celery/ledger') {
        return okRes({
          items: [{
            service_id: 'svc-alpha', object_key: deadKey, source_type: 'rum', status: 'dead_letter', attempts: 1,
            error_kind: 'object_missing', last_error: 'object missing from FOS (already ingested+deleted, or expired)',
            originating_task: 'rum_discovery', originating_run_id: 24368, claimed_at: 1729400000,
          }],
          total: 1,
        })
      }
      if (path === '/api/admin/metric-history/batch') return okRes({ series: {} })
      return okRes({})
    })
    clientPostSpy.mockImplementation(async () => okRes({ ok: true, affected: 1, dispatched: 1 }))

    renderWithQuery(<AdminQueuePage />)
    await waitFor(() => expect(screen.getByTestId('admin-queue-ledger-entries')).toBeInTheDocument())
    expect(screen.getByText(deadKey)).toBeInTheDocument()
    expect(screen.getByText('object_missing')).toBeInTheDocument()
    expect(screen.getByText('object missing from FOS (already ingested+deleted, or expired)')).toBeInTheDocument()

    fireEvent.click(screen.getByTestId('admin-queue-ledger-retry-row'))
    await waitFor(() => expect(clientPostSpy).toHaveBeenCalledWith(
      '/api/admin/celery/ledger/retry',
      expect.objectContaining({ body: { items: [{ service_id: 'svc-alpha', object_key: deadKey }] } }),
    ))

    fireEvent.click(screen.getByTestId('admin-queue-ledger-purge-all'))
    await waitFor(() => expect(clientPostSpy).toHaveBeenCalledWith(
      '/api/admin/celery/ledger/purge',
      expect.objectContaining({ body: { status: 'problem' } }),
    ))
  })
})

function buildMockShareStatus(overrides: Record<string, unknown> = {}) {
  return {
    sharing_active: false, public_endpoint: null, public_url: null, forward_port: null, started_at: null,
    max_concurrent_sessions: 10, active_session_count: 0,
    rate_limits: { failures: [], lockouts: [] },
    telemetry: { current_uptime_s: null, heartbeat_unauth_count: 0, tunnel_uptime_history: [] },
    invites: [{
      id: 'inv-1', name: 'Security Analyst', email: 'analyst@example.com', created_at: 1710000000,
      expires_at: null, revoked: false, max_sessions: 1, services: ['svc-1'], active_sessions: 0,
    }],
    sessions: [],
    services: [{ service_id: 'svc-1', name: 'Primary Edge Service', remote_frontend_deployed: false, sharing_domain: null }],
    audit_logs: [],
    ...overrides,
  }
}

describe('admin/share page', () => {
  beforeEach(() => { mockSearchParamsString = '' })

  test('renders zero-CLS reservations, SharingControlPanel, InvitationsPanel, and manual Refresh', async () => {
    let statusCalls = 0
    let liveCalls = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => {
        statusCalls += 1
        return HttpResponse.json(buildMockShareStatus({
          sharing_active: statusCalls > 1,
          public_url: statusCalls > 1 ? 'https://share.example.com' : null,
        }))
      }),
      http.get(`${API_BASE}/api/admin/share/live`, () => {
        liveCalls += 1
        return HttpResponse.json({
          sharing_active: statusCalls > 1, public_url: statusCalls > 1 ? 'https://share.example.com' : null, active_session_count: 0,
          rate_limits: { failures: [], lockouts: [] },
          telemetry: { current_uptime_s: null, heartbeat_unauth_count: 0, tunnel_uptime_history: [] },
        })
      }),
    )

    renderWithQuery(<ShareDashboardPage />, { wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }) })
    await waitFor(() => { expect(statusCalls).toBe(1); expect(liveCalls).toBe(1) })

    const controlRes = screen.getByTestId('admin-share-control-reservation')
    const panelRes = screen.getByTestId('admin-share-panel-reservation')
    expect(screen.getByTestId('admin-share-root')).toBeTruthy()
    expect(controlRes.className).toContain('[contain-intrinsic-size:auto_220px]')
    expect(panelRes.className).toContain('[contain-intrinsic-size:auto_360px]')

    await waitFor(() => {
      expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy()
      expect(screen.getByTestId('admin-share-status-badge').textContent).toBe('OFF')
      expect(screen.getByTestId('admin-share-invites-panel')).toBeTruthy()
    })
    for (const id of ['admin-share-tab-invites', 'admin-share-tab-sessions', 'admin-share-tab-audit']) {
      expect(screen.getByTestId(id)).toBeTruthy()
    }
    expect(screen.getByText('analyst@example.com')).toBeTruthy()

    fireEvent.click(screen.getByTestId('admin-share-refresh-btn'))
    await waitFor(() => {
      expect(statusCalls).toBe(2)
      expect(screen.getByTestId('admin-share-status-badge').textContent).toBe('ACTIVE')
    })
  })

  test('initializes activeTab from URL ?tab=, falls back on unknown ?tab=, and displays 500 error', async () => {
    mockSearchParamsString = 'tab=sessions'
    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => HttpResponse.json(buildMockShareStatus())),
      http.get(`${API_BASE}/api/admin/share/live`, () => HttpResponse.json({})),
    )
    const { unmount: u1 } = renderWithQuery(<ShareDashboardPage />, { wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }) })
    await waitFor(() => expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy())
    expect(screen.getByTestId('admin-share-tab-sessions').getAttribute('aria-selected')).toBe('true')
    u1()

    mockSearchParamsString = 'tab=bogus'
    const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
    const { unmount: u2 } = renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString, hasMemory: true, onUrlUpdate, resetUrlUpdateQueueOnMount: false }),
    })
    await waitFor(() => expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy())
    expect(screen.getByTestId('admin-share-tab-invites').getAttribute('aria-selected')).toBe('true')
    fireEvent.click(screen.getByTestId('admin-share-tab-sessions'))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)?.[0].searchParams.get('tab')).toBe('sessions'))
    expect(onUrlUpdate.mock.calls.at(-1)?.[0].options.history).toBe('replace')
    fireEvent.click(screen.getByTestId('admin-share-tab-invites'))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)?.[0].searchParams.has('tab')).toBe(false))
    u2()

    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => HttpResponse.json({ detail: 'Share DB unavailable' }, { status: 500 })),
      http.get(`${API_BASE}/api/admin/share/live`, () => HttpResponse.json({})),
    )
    renderWithQuery(<ShareDashboardPage />, { wrapper: withNuqsTestingAdapter({ searchParams: '' }) })
    await waitFor(() => expect(screen.getByTestId('admin-share-status-error')).toBeTruthy())
  })
})

describe('admin/session-scoring page', () => {
  const analyticsStub = {
    health: { ok: true }, top_flagged: { rows: [] }, score_distribution: { buckets: [] },
    compliance_breakdown: { categories: [] }, evaluation_per_reason: { rows: [] }, evaluation: { mean_score: 0 },
  }

  beforeEach(() => {
    mockScoringSubcomponents = true
    useServiceStore.setState({ activeServiceId: 'svc-default', isInitialized: true })
  })

  test('renders zero-CLS reservations, composites, and manual refresh', async () => {
    const analyticsHits: string[] = []
    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, ({ params }) => {
        analyticsHits.push(String(params.service_id))
        return HttpResponse.json(analyticsStub)
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({
          status: { enabled: false }, threshold: { value: 50 }, exclude_regex: { pattern: '' },
          enforce_status_code: { code: 403 }, matrix_versions: { versions: [] },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)
    await waitFor(() => {
      expect(analyticsHits).toEqual(['svc-default'])
      expect(configHits).toEqual(['svc-default'])
    })

    expect(screen.getByTestId('admin-session-scoring-root')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-status-reservation').className).toContain('[contain-intrinsic-size:auto_220px]')
    expect(screen.getByTestId('admin-session-scoring-overview-reservation').className).toContain('[contain-intrinsic-size:auto_900px]')
    for (const id of ['admin-session-scoring-tab-overview', 'admin-session-scoring-tab-labels', 'admin-session-scoring-tab-matrix', 'admin-session-scoring-tab-audit']) {
      expect(screen.getByTestId(id)).toBeTruthy()
    }

    fireEvent.click(screen.getByTestId('admin-session-scoring-refresh-btn'))
    await waitFor(() => {
      expect(configHits.length).toBe(2)
      expect(analyticsHits.length).toBe(2)
    })
  })

  test('falls back to /api/services when activeServiceId is initially empty', async () => {
    useServiceStore.setState({ activeServiceId: null, isInitialized: false })
    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services`, () => HttpResponse.json({ services: [{ service_id: 'svc-fallback', name: 'Fallback Service' }] })),
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () => HttpResponse.json(analyticsStub)),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({ status: { enabled: false }, threshold: { threshold: null }, exclude_regex: { pattern: '' }, enforce_status_code: { effective: 429 } })
      }),
    )
    renderWithQuery(<SessionScoringPage />)
    await waitFor(() => expect(configHits).toContain('svc-fallback'))
  })
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

    test('displays unavailable insights alert with link to service-config', async () => {
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
      expect(screen.getByRole('link', { name: /configure service log fields/i })).toHaveAttribute('href', '/service-config?service=test-svc')
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

// ============================================================================
// Merged from share-login.test.tsx
// ============================================================================
describe('ShareLogin & Sessions Pages', () => {
  type SessionRow = components['schemas']['Session']
  const rtlRender = render
  const renderPage = renderWithQuery

  beforeEach(() => {
    useMockShareSearchParams = true
    pushSpy.mockReset()
    replaceSpy.mockReset()
    locationAssignSpy.mockReset()
    mockSearchParams = new URLSearchParams()
    stubWindowLocationAssign(locationAssignSpy, 'http://localhost/share-login')
  })

  afterEach(() => {
    useMockShareSearchParams = false
    vi.restoreAllMocks()
  })

  function useAuthConfig(passcode_enabled: boolean, providers: Array<{ id: string; display_name: string }>) {
    server.use(http.get('/api/share/auth-config', () => HttpResponse.json({ passcode_enabled, providers })))
  }

  async function submitLoginForm(user: ReturnType<typeof userEvent.setup>, email = 'jane@example.com', passcode = 'ocean-cabin-42') {
    await user.type(await screen.findByLabelText('Email'), email)
    await user.type(screen.getByLabelText('Passcode'), passcode)
    await user.click(screen.getByRole('button', { name: /sign in/i }))
  }

  const TOS_TEXT =
    'I acknowledge that I am viewing third-party operational log data, that my access is logged, and that I will not retain, redistribute, or use this data outside the scope of my engagement.'

  describe('ShareLoginPage — Zero-CLS Layout Reservations & Loading Skeletons', () => {
    it('renders pre-allocated zero-CLS card and content reservations on ShareLoginPage', async () => {
      renderPage(<ShareLoginPage />)
      const card = await screen.findByTestId('share-login-card')
      const content = screen.getByTestId('share-login-card-content')
      const formContainer = await screen.findByTestId('share-login-form-container')

      expect(card.className).toContain('[contain-intrinsic-size:auto_380px]')
      expect(card.className).toContain('[content-visibility:auto]')
      expect(content.className).toContain('[contain-intrinsic-size:auto_240px]')
      expect(content.className).toContain('[content-visibility:auto]')
      expect(formContainer.className).toContain('[contain-intrinsic-size:auto_240px]')
      expect(formContainer.className).toContain('[content-visibility:auto]')
    })

    it('renders ShareLoginLoading with zero-CLS reservations and contextual status copy', () => {
      renderPage(<ShareLoginLoading />)
      expect(screen.getByText(/verifying share session & security policies/i)).toBeInTheDocument()
    })

    it('shows contextual in-place loading skeleton while auth-config is pending', async () => {
      let resolveConfig!: () => void
      server.use(
        http.get('/api/share/auth-config', async () => {
          await new Promise<void>((r) => { resolveConfig = r })
          return HttpResponse.json({ passcode_enabled: true, oauth_enabled: false, oauth_provider_label: null })
        }),
      )

      renderPage(<ShareLoginPage />)
      expect(screen.getByTestId('share-login-loading-skeleton')).toBeInTheDocument()
      expect(screen.getByText(/verifying share session & security policies/i)).toBeInTheDocument()

      await waitFor(() => expect(resolveConfig).toBeTypeOf('function'))
      resolveConfig()
      await waitFor(() => expect(screen.queryByTestId('share-login-loading-skeleton')).not.toBeInTheDocument())
    })

    it('applies dimmed transition (opacity-40 pointer-events-none) while login submission is in-flight', async () => {
      let resolveLogin!: () => void
      server.use(
        http.post('/api/share/login', async () => {
          await new Promise<void>((r) => { resolveLogin = r })
          return HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' })
        }),
      )

      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
      await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')

      const fieldsContainer = screen.getByTestId('share-login-fields')
      expect(fieldsContainer.className).not.toContain('opacity-40')
      await user.click(screen.getByRole('button', { name: /sign in/i }))

      await waitFor(() => {
        expect(fieldsContainer.className).toContain('opacity-40')
        expect(fieldsContainer.className).toContain('pointer-events-none')
      })

      resolveLogin()
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    })
  })

  describe('ShareLoginPage — Invite Token Claim & Credential Validation', () => {
    it('claims a valid ?token= query parameter and pre-fills the analyst email', async () => {
      mockSearchParams = new URLSearchParams('token=valid-claim-tok')
      server.use(
        http.post('/api/share/claim/valid-claim-tok', () =>
          HttpResponse.json({
            name: 'Analyst Bob',
            email: 'bob@example.com',
            passcode: 'secret-words-99',
            share_url: 'https://share.example.com',
            expires_at: null,
          }),
        ),
      )

      renderPage(<ShareLoginPage />)
      expect(await screen.findByTestId('share-login-invite-banner')).toHaveTextContent(/invitation verified for analyst bob/i)
      expect((screen.getByLabelText('Email') as HTMLInputElement).value).toBe('bob@example.com')
    })

    it.each([404, 410])('shows expired invite error when ?token= returns %s', async (status) => {
      mockSearchParams = new URLSearchParams('token=expired-claim-tok')
      server.use(
        http.post('/api/share/claim/expired-claim-tok', () =>
          HttpResponse.json({ detail: { error: 'invite_gone' } }, { status }),
        ),
      )

      renderPage(<ShareLoginPage />)
      expect(await screen.findByText(/this invitation link has expired or is no longer valid/i)).toBeInTheDocument()
    })

    it('clears an expired invitation banner on submission and replaces it with the login error', async () => {
      mockSearchParams = new URLSearchParams('token=expired-claim-tok')
      let resolveLogin!: () => void
      server.use(
        http.post('/api/share/claim/expired-claim-tok', () =>
          HttpResponse.json({ detail: { error: 'invite_gone' } }, { status: 410 }),
        ),
        http.post('/api/share/login', async () => {
          await new Promise<void>((resolve) => { resolveLogin = resolve })
          return HttpResponse.json({ detail: { error: 'invalid_credentials' } }, { status: 401 })
        }),
      )

      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      const inviteError = await screen.findByText(/this invitation link has expired/i)
      await submitLoginForm(user, 'you@example.com', 'ocean-cabin-42')

      await waitFor(() => expect(resolveLogin).toBeTypeOf('function'))
      expect(inviteError).not.toBeInTheDocument()
      resolveLogin()
      const loginError = await screen.findByText('Invalid email or passcode.')
      expect(loginError.closest('[role="alert"]')).toHaveFocus()
      expect(screen.queryByText(/this invitation link has expired/i)).not.toBeInTheDocument()
    })

    it('displays error feedback for 503 capacity cap', async () => {
      const user = userEvent.setup()
      server.use(
        http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'capacity_exceeded' } }, { status: 503 })),
      )

      renderPage(<ShareLoginPage />)
      await submitLoginForm(user)
      expect(await screen.findByText(/the dashboard is at capacity/i)).toBeInTheDocument()
    })
  })

  describe('ShareLoginPage — TOS Gate & Safe Redirect Validation', () => {
    it('supports ?next= redirect parameter when routing through TOS acknowledgment', async () => {
      mockSearchParams = new URLSearchParams('next=/performance')
      server.use(
        http.post('/api/share/login', () =>
          HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: true, redirect: '/dashboard' }),
        ),
      )

      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user)
      await waitFor(() => expect(pushSpy).toHaveBeenCalledWith('/share-login/acknowledge?return=%2Fperformance'))
    })

    it('blocks open-redirect targets on ?next= and ?return= via safeReturnTarget', () => {
      for (const bad of ['//evil.example/phish', 'https://evil.example/phish', '/\t/evil.example', '/\\evil.example', '/share-login/acknowledge']) {
        expect(safeReturnTarget(bad)).toBeNull()
      }
      expect(safeReturnTarget('/query?service=svc-1')).toBe('/query?service=svc-1')
    })

    it('acknowledges TOS via AcknowledgeButton with custom safe redirect target and dimmed busy state', async () => {
      server.use(http.post('/api/share/acknowledge', () => HttpResponse.json({ ok: true })))
      const user = userEvent.setup()
      renderPage(<AcknowledgeButton version="v1" returnTo="//evil.example" />)
      await user.click(screen.getByTestId('share-acknowledge-submit'))
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    })

    it('renders zero-CLS container reservations in AcknowledgeFallback', async () => {
      server.use(http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: 'Terms of Service body text' })))
      renderPage(<AcknowledgeFallback />)
      const container = await screen.findByTestId('share-acknowledge-fallback')
      expect(container.className).toContain('[contain-intrinsic-size:auto_140px]')
      expect(container.className).toContain('[content-visibility:auto]')
    })
  })

  describe('ShareLoginPage', () => {
    it('hard-reloads to /dashboard on successful login when tos already accepted', async () => {
      server.use(
        http.post('/api/share/login', () =>
          HttpResponse.json({ ok: true, session_id: 'sid-1', name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' }),
        ),
      )

      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user)
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    })

    it('routes to acknowledge when tos_pending', async () => {
      server.use(
        http.post('/api/share/login', () =>
          HttpResponse.json({ ok: true, session_id: 'sid-1', name: 'Jane', email: 'jane@example.com', tos_pending: true, redirect: '/dashboard' }),
        ),
      )

      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user)
      await waitFor(() => expect(pushSpy).toHaveBeenCalledWith('/share-login/acknowledge'))
    })

    it('shows invalid-credentials on 401', async () => {
      server.use(http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'invalid' } }, { status: 401 })))
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user, 'wrong@example.com', 'nope')
      expect(await screen.findByText(/invalid email or passcode/i)).toBeInTheDocument()
      expect(pushSpy).not.toHaveBeenCalled()
    })

    it('shows lockout countdown on 429', async () => {
      server.use(
        http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'locked', retry_after_s: 30 } }, { status: 429 })),
      )
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user, 'jane@example.com', 'x')
      expect(await screen.findByText(/locked out/i)).toBeInTheDocument()
      expect(screen.getByText(/retry in 30s/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /sign in/i })).toBeDisabled()
    })

    it('shows ip whitelist message on 403', async () => {
      server.use(
        http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'ip_not_whitelisted' } }, { status: 403 })),
      )
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user, 'jane@example.com', 'x')
      expect(await screen.findByText(/ip address is not on the approved list/i)).toBeInTheDocument()
    })

    it('toggles passcode visibility', async () => {
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      const input = (await screen.findByLabelText('Passcode')) as HTMLInputElement
      expect(input.type).toBe('password')
      await user.click(screen.getByRole('button', { name: /reveal passcode/i }))
      expect(input.type).toBe('text')
    })
  })

  describe('ShareLoginForm submit', () => {
    it('posts exactly {email, passcode} as JSON', async () => {
      let body: unknown = null
      let contentType: string | null = null
      server.use(
        http.post('/api/share/login', async ({ request }) => {
          body = await request.json()
          contentType = request.headers.get('content-type')
          return HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' })
        }),
      )
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await submitLoginForm(user)
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
      expect(body).toEqual({ email: 'jane@example.com', passcode: 'ocean-cabin-42' })
      expect(contentType).toContain('application/json')
    })

    it('does not submit a blank form', async () => {
      let calls = 0
      server.use(http.post('/api/share/login', () => { calls += 1; return HttpResponse.json({}) }))
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await user.click(await screen.findByRole('button', { name: /sign in/i }))
      await new Promise((r) => setTimeout(r, 50))
      expect(calls).toBe(0)
    })

    it('focuses the email field once the passcode form shows', async () => {
      renderPage(<ShareLoginPage />)
      const email = await screen.findByLabelText('Email')
      await waitFor(() => expect(document.activeElement).toBe(email))
    })

    it('ignores a second submit while a login is in flight', async () => {
      let calls = 0
      let release!: () => void
      server.use(
        http.post('/api/share/login', async () => {
          calls += 1
          await new Promise<void>((r) => { release = r })
          return HttpResponse.json({ ok: true, name: 'J', email: 'j@example.com', tos_pending: false, redirect: '/dashboard' })
        }),
      )
      const user = userEvent.setup()
      renderPage(<ShareLoginPage />)
      await user.type(await screen.findByLabelText('Email'), 'j@example.com')
      await user.type(screen.getByLabelText('Passcode'), 'pw-1')
      const submit = screen.getByRole('button', { name: /sign in/i })
      await user.click(submit)
      await waitFor(() => expect(calls).toBe(1))
      await user.click(submit)
      release()
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalled())
      expect(calls).toBe(1)
    })
  })

  describe('ShareLoginForm OAuth', () => {
    it('renders SSO buttons + passcode form when both are enabled', async () => {
      useAuthConfig(true, [{ id: 'google', display_name: 'Google Workspace' }])
      renderPage(<ShareLoginForm />)
      expect(await screen.findByRole('button', { name: /sign in with google workspace/i })).toBeInTheDocument()
      expect(await screen.findByLabelText('Email')).toBeInTheDocument()
      expect(screen.getByText('OR')).toBeInTheDocument()
    })

    it('clicking an SSO button navigates to the authorize endpoint', async () => {
      useAuthConfig(false, [{ id: 'google', display_name: 'Google Workspace' }])
      const user = userEvent.setup()
      renderPage(<ShareLoginForm />)
      await user.click(await screen.findByRole('button', { name: /sign in with google workspace/i }))
      expect(locationAssignSpy).toHaveBeenCalledWith('/api/share/oauth/authorize?provider=google')
    })

    it('SSO-only config hides the passcode form', async () => {
      useAuthConfig(false, [{ id: 'google', display_name: 'Google Workspace' }])
      renderPage(<ShareLoginForm />)
      await screen.findByRole('button', { name: /sign in with google workspace/i })
      expect(screen.queryByLabelText('Email')).toBeNull()
      expect(screen.queryByText('OR')).toBeNull()
    })

    it('shows the misconfiguration lockout when nothing is enabled', async () => {
      useAuthConfig(false, [])
      renderPage(<ShareLoginForm />)
      expect(await screen.findByText(/sign-in is not configured/i)).toBeInTheDocument()
      expect(screen.queryByLabelText('Email')).toBeNull()
    })

    it('fails open to the passcode form when auth-config fetch fails', async () => {
      server.use(http.get('/api/share/auth-config', () => HttpResponse.json({ error: 'boom' }, { status: 500 })))
      renderPage(<ShareLoginForm />)
      expect(await screen.findByLabelText('Email')).toBeInTheDocument()
    })

    it('maps a known oauth_error code to friendly copy and never reflects the raw param', async () => {
      mockSearchParams = new URLSearchParams('oauth_error=not_invited')
      useAuthConfig(true, [])
      renderPage(<ShareLoginForm />)
      expect(await screen.findByText(/isn't invited to a dashboard/i)).toBeInTheDocument()
      expect(screen.queryByText(/not_invited/)).toBeNull()
    })

    it('maps an unknown oauth_error code to the generic fallback', async () => {
      mockSearchParams = new URLSearchParams('oauth_error=some_unknown_code_xyz')
      useAuthConfig(true, [])
      renderPage(<ShareLoginForm />)
      expect(await screen.findByText(/sign-in failed\. please try again\./i)).toBeInTheDocument()
      expect(screen.queryByText(/some_unknown_code_xyz/)).toBeNull()
    })
  })

  describe('AcknowledgeFallback (SSR-failure path)', () => {
    it('redirects to /share-login when tos fetch returns 401', async () => {
      server.use(http.get('/api/share/tos', () => HttpResponse.json({ detail: 'unauthenticated' }, { status: 401 })))
      rtlRender(<AcknowledgeFallback />)
      await waitFor(() => expect(replaceSpy).toHaveBeenCalledWith('/share-login'))
    })

    it('renders TOS text and acknowledges → hard-reload to /dashboard', async () => {
      const ackBody = vi.fn()
      server.use(
        http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: TOS_TEXT })),
        http.post('/api/share/acknowledge', async ({ request }) => {
          ackBody(await request.json())
          return HttpResponse.json({ ok: true })
        }),
      )

      const user = userEvent.setup()
      rtlRender(<AcknowledgeFallback />)
      expect(await screen.findByText(/i acknowledge that i am viewing/i)).toBeInTheDocument()
      await user.click(screen.getByRole('button', { name: /i acknowledge/i }))
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
      expect(ackBody).toHaveBeenCalledWith({ version: 'v1' })
    })

    it('shows server error if acknowledge fails', async () => {
      server.use(
        http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: TOS_TEXT })),
        http.post('/api/share/acknowledge', () => HttpResponse.json({ detail: { error: 'invalid_version' } }, { status: 400 })),
      )

      const user = userEvent.setup()
      rtlRender(<AcknowledgeFallback />)
      await screen.findByText(/i acknowledge that i am viewing/i)
      await user.click(screen.getByRole('button', { name: /i acknowledge/i }))
      expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
      expect(replaceSpy).not.toHaveBeenCalledWith('/dashboard')
    })
  })

  describe('AcknowledgeButton (SSR-happy-path island)', () => {
    it('acknowledges with the supplied version → hard-reload to /dashboard', async () => {
      const ackBody = vi.fn()
      server.use(
        http.post('/api/share/acknowledge', async ({ request }) => {
          ackBody(await request.json())
          return HttpResponse.json({ ok: true })
        }),
      )

      const user = userEvent.setup()
      rtlRender(<AcknowledgeButton version="v2" />)
      await user.click(screen.getByRole('button', { name: /i acknowledge/i }))
      await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
      expect(ackBody).toHaveBeenCalledWith({ version: 'v2' })
    })

    it('shows server error if acknowledge fails', async () => {
      server.use(
        http.post('/api/share/acknowledge', () => HttpResponse.json({ detail: { error: 'invalid_version' } }, { status: 400 })),
      )

      const user = userEvent.setup()
      rtlRender(<AcknowledgeButton version="v2" />)
      await user.click(screen.getByRole('button', { name: /i acknowledge/i }))
      expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
      expect(locationAssignSpy).not.toHaveBeenCalled()
    })
  })

  describe('safeReturnTarget (post-login open-redirect guard, finding 008)', () => {
    it('accepts in-app absolute paths', () => {
      expect(safeReturnTarget('/dashboard')).toBe('/dashboard')
      expect(safeReturnTarget('/sessions?x=1')).toBe('/sessions?x=1')
    })

    it('rejects protocol-relative, backslash, and absolute external targets', () => {
      for (const target of ['//evil.com', '/\\evil.com', 'https://evil.com', 'evil.com']) {
        expect(safeReturnTarget(target)).toBeNull()
      }
    })

    it('rejects whitespace-smuggled protocol-relative targets (the finding-008 bypass)', () => {
      for (const target of ['/\t/evil.com', '/\n/evil.com', '/ /evil.com', '/\t\\evil.com']) {
        expect(safeReturnTarget(target)).toBeNull()
      }
    })

    it('returns null for empty / nullish input', () => {
      expect(safeReturnTarget(null)).toBeNull()
      expect(safeReturnTarget(undefined)).toBeNull()
      expect(safeReturnTarget('')).toBeNull()
    })
  })

  function makeRow(overrides: Partial<SessionRow> = {}): SessionRow {
    return {
      ip: '10.0.0.1', country: 'US', asn: 7922,
      session_start: '2026-06-15T00:00:00Z', session_end: '2026-06-15T00:30:00Z',
      req_count: 100, edge_count: 80, shield_count: 20, unique_urls: 12,
      reqs_4xx: 5, reqs_5xx: 0, total_bytes: 50000, median_rtt_ms: 23.5,
      ja4: 't13d1715h2_5b57614c22b0_3d5424432f57', ua: 'curl/7.85.0',
      edge_sid: null, flagged: false, ...overrides,
    }
  }

  function makeResponse(
    rows: SessionRow[] = [],
    flags: Partial<Pick<SessionsResponse, 'has_rtt' | 'has_ja4' | 'has_edge' | 'has_edge_sid' | 'has_cmcd'>> = {},
  ): SessionsResponse {
    return {
      sessions: rows, asn_names: {}, total: rows.length, page: 1, limit: 100,
      has_rtt: false, has_ja4: false, has_edge: false, has_edge_sid: false, has_cmcd: false,
      min_reqs_flag: 0, min_4xx_pct_flag: 0, _is_cached: false, ...flags,
    }
  }

  function detailElement(overrides: Partial<React.ComponentProps<typeof SessionDetail>> = {}) {
    const props: React.ComponentProps<typeof SessionDetail> = {
      selectedSession: makeRow(),
      setSelectedSession: vi.fn(),
      activeServiceId: 'svc-1',
      data: makeResponse(),
      labels: [],
      labelBySid: new Map(),
      onFlagged: vi.fn(),
      ...overrides,
    }
    return <SessionDetail {...props} />
  }

  function renderSessionDetail(overrides: Partial<React.ComponentProps<typeof SessionDetail>> = {}) {
    return renderPage(detailElement(overrides), { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } })
  }

  describe('SessionDetail', () => {
    beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
    afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

    it('does not render the dialog when selectedSession is null', () => {
      renderSessionDetail({ selectedSession: null })
      expect(screen.queryByText(/^Session:/)).toBeNull()
      expect(postMock).not.toHaveBeenCalled()
    })

    it('renders the session header + metadata when a session is selected', () => {
      postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
      renderSessionDetail({ selectedSession: makeRow({ ip: '10.0.0.42', country: 'CA' }) })
      expect(screen.getByText(/Session:/)).toBeInTheDocument()
      for (const txt of ['10.0.0.42', 'CA', 'AS7922', '100']) expect(screen.getByText(txt)).toBeInTheDocument()
    })

    it('issues a POST to /api/sessions/detail with the session window', async () => {
      postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
      const selectedSession = makeRow({
        ip: '10.0.0.50', ja4: 'abc_def_ghi',
        session_start: '2026-06-15T00:00:00Z', session_end: '2026-06-15T01:00:00Z',
      })
      renderSessionDetail({ selectedSession })
      await waitFor(() => expect(postMock).toHaveBeenCalled())
      const [path, opts] = postMock.mock.calls[0]
      expect(path).toBe('/api/sessions/detail')
      expect(opts.body).toMatchObject({
        ip: '10.0.0.50', ja4: 'abc_def_ghi',
        start_time: '2026-06-15T00:00:00Z', end_time: '2026-06-15T01:00:00Z',
      })
    })

    it('skips the fetch when activeServiceId is null (useQuery enabled=false)', async () => {
      postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
      renderSessionDetail({ activeServiceId: null })
      await act(async () => { await new Promise((r) => setTimeout(r, 30)) })
      expect(postMock).not.toHaveBeenCalled()
    })

    it('renders the timeline columns + rows once the detail fetch resolves', async () => {
      postMock.mockResolvedValue({
        data: {
          _is_cached: false,
          columns: ['timestamp', 'host', 'url', 'method', 'status'],
          data: [
            { timestamp: '2026-06-15T00:00:00Z', host: 'example.com', url: '/a', method: 'GET', status: 200 },
            { timestamp: '2026-06-15T00:00:05Z', host: 'example.com', url: '/b', method: 'GET', status: 404 },
          ],
        },
      })
      renderSessionDetail()
      expect(screen.getByText(/Request Timeline/i)).toBeInTheDocument()
      await waitFor(() => expect(screen.getAllByText(/example\.com/).length).toBeGreaterThan(0))
      for (const txt of ['/a', '/b', '200', '404']) expect(screen.getByText(txt)).toBeInTheDocument()
    })

    it('shows the Edge-only switch only when data.has_edge=true', () => {
      postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
      const { rerender } = renderSessionDetail({ data: makeResponse([], { has_edge: false }) })
      expect(screen.queryByText(/Edge only/i)).toBeNull()

      rerender(detailElement({ data: makeResponse([], { has_edge: true }) }))
      expect(screen.getByText(/Edge only/i)).toBeInTheDocument()
    })

    it('renders the FlagSessionPopover in the header when edge_sid is present', () => {
      postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
      renderSessionDetail({ selectedSession: makeRow({ edge_sid: 'sid-abc' }) })
      expect(screen.getByTestId('flag-popover')).toBeInTheDocument()
    })

    it('UX-5: shows an inline error + Retry (not a bare empty table) when the detail query 5xxs', async () => {
      postMock.mockRejectedValueOnce(new Error('detail boom'))
      renderSessionDetail({ selectedSession: makeRow({ ip: '1.2.3.4' }), activeServiceId: 'test-svc', data: undefined })
      await waitFor(
        () => expect(screen.getByRole('alert')).toHaveTextContent(/failed to load session detail/i),
        { timeout: 10000 },
      )
      expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
    }, 15000)
  })

  function baseProps(overrides: Partial<React.ComponentProps<typeof SessionsTable>> = {}): React.ComponentProps<typeof SessionsTable> {
    return {
      data: makeResponse([makeRow()]), activeServiceId: 'svc-1', isLoadingInitial: false, isFetching: false,
      labels: [], labelBySid: new Map(), idBySid: new Map(), onFlagged: vi.fn(), onRowClick: vi.fn(),
      ...overrides,
    }
  }

  describe('SessionsTable', () => {
    beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
    afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

    it('renders the canonical column headers', () => {
      rtlRender(<SessionsTable {...baseProps()} />)
      for (const h of ['IP Address', 'Country', 'Requests', '4xx%', 'User-Agent']) {
        expect(screen.getByText(h)).toBeInTheDocument()
      }
    })

    it('renders one row per session in data.sessions', () => {
      const rows = [makeRow({ ip: '10.0.0.1' }), makeRow({ ip: '10.0.0.2', country: 'CA' }), makeRow({ ip: '10.0.0.3', country: 'GB' })]
      rtlRender(<SessionsTable {...baseProps({ data: makeResponse(rows) })} />)
      for (const ip of ['10.0.0.1', '10.0.0.2', '10.0.0.3']) expect(screen.getByText(ip)).toBeInTheDocument()
    })

    it('clicking the row-level Details button opens the detail panel via onRowClick', () => {
      const onRowClick = vi.fn()
      const row = makeRow({ ip: '10.0.0.42' })
      rtlRender(<SessionsTable {...baseProps({ data: makeResponse([row]), onRowClick })} />)
      fireEvent.click(screen.getByRole('button', { name: /details/i }))
      expect(onRowClick).toHaveBeenCalled()
      expect(onRowClick.mock.calls[0][0]).toMatchObject({ ip: '10.0.0.42' })
    })

    it('renders the flagged-icon when row.flagged=true', () => {
      const row = makeRow({ ip: '10.0.0.7', flagged: true })
      const { container } = rtlRender(<SessionsTable {...baseProps({ data: makeResponse([row]) })} />)
      expect(container.querySelector('[title="Flagged Session"]')).toBeInTheDocument()
    })

    it('omits RTT / JA4 / Edge columns when their has_* flags are false', () => {
      rtlRender(<SessionsTable {...baseProps()} />)
      for (const h of ['Med. RTT', 'JA4', 'Edge / Shield', 'Flag']) expect(screen.queryByText(h)).toBeNull()
    })

    it('adds RTT / JA4 / Edge columns when their has_* flags are true', () => {
      const row = makeRow({ edge_sid: 'sid-abc' })
      rtlRender(
        <SessionsTable
          {...baseProps({
            data: makeResponse([row], { has_rtt: true, has_ja4: true, has_edge: true, has_edge_sid: true }),
          })}
        />,
      )
      for (const h of ['Med. RTT', 'JA4', 'Edge / Shield', 'Flag']) expect(screen.getByText(h)).toBeInTheDocument()
      expect(screen.getAllByTestId('flag-popover').length).toBeGreaterThan(0)
    })

    it('renders the empty state when data has no sessions', () => {
      rtlRender(<SessionsTable {...baseProps({ data: makeResponse([]) })} />)
      expect(screen.getByText('IP Address')).toBeInTheDocument()
    })

    it('renders even when data is undefined (initial fetch)', () => {
      rtlRender(<SessionsTable {...baseProps({ data: undefined })} />)
      expect(screen.getByText('IP Address')).toBeInTheDocument()
    })
  })

  function controlsBaseProps(overrides: Partial<React.ComponentProps<typeof ScoringControls>> = {}) {
    return {
      cmcdEnabled: false, flaggedOnly: false, setFlaggedOnly: vi.fn(), streamingOnly: false, setStreamingOnly: vi.fn(),
      minReqs: '' as number | '', setMinReqs: vi.fn(), min4xxPct: '' as number | '', setMin4xxPct: vi.fn(),
      data: { min_reqs_flag: 1000, min_4xx_pct_flag: 20 }, isFetching: false, isLoadingInitial: false, refetch: vi.fn(),
      ...overrides,
    }
  }

  describe('ScoringControls', () => {
    beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
    afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

    it('renders the flagged-only switch label and the threshold inputs', () => {
      rtlRender(<ScoringControls {...controlsBaseProps()} />)
      expect(screen.getByText(/flagged only/i)).toBeInTheDocument()
      expect(screen.getByText(/min\. requests/i)).toBeInTheDocument()
      expect(screen.getByText(/min\. 4xx%/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument()
    })

    it('uses data.min_reqs_flag / data.min_4xx_pct_flag as placeholder hints', () => {
      rtlRender(<ScoringControls {...controlsBaseProps({ data: { min_reqs_flag: 5000, min_4xx_pct_flag: 35 } })} />)
      expect(screen.getByPlaceholderText(/≥\s*5000/)).toBeInTheDocument()
      expect(screen.getByPlaceholderText(/≥\s*35/)).toBeInTheDocument()
    })

    it('falls back to default placeholders when data is undefined', () => {
      rtlRender(<ScoringControls {...controlsBaseProps({ data: undefined })} />)
      expect(screen.getByPlaceholderText(/≥\s*1000/)).toBeInTheDocument()
      expect(screen.getByPlaceholderText(/≥\s*20/)).toBeInTheDocument()
    })

    it('fires setMinReqs with a number when the requests input changes', () => {
      const setMinReqs = vi.fn()
      rtlRender(<ScoringControls {...controlsBaseProps({ setMinReqs })} />)
      fireEvent.change(screen.getByPlaceholderText(/≥\s*1000/), { target: { value: '250' } })
      expect(setMinReqs).toHaveBeenCalledWith(250)
    })

    it('fires setMinReqs("") when the requests input is cleared', () => {
      const setMinReqs = vi.fn()
      rtlRender(<ScoringControls {...controlsBaseProps({ setMinReqs, minReqs: 250 })} />)
      fireEvent.change(screen.getByPlaceholderText(/≥\s*1000/), { target: { value: '' } })
      expect(setMinReqs).toHaveBeenCalledWith('')
    })

    it('fires setMin4xxPct with a number when the 4xx input changes', () => {
      const setMin4xxPct = vi.fn()
      rtlRender(<ScoringControls {...controlsBaseProps({ setMin4xxPct })} />)
      fireEvent.change(screen.getByPlaceholderText(/≥\s*20/), { target: { value: '40' } })
      expect(setMin4xxPct).toHaveBeenCalledWith(40)
    })

    it('fires setFlaggedOnly when the switch is toggled', async () => {
      const setFlaggedOnly = vi.fn()
      rtlRender(<ScoringControls {...controlsBaseProps({ setFlaggedOnly })} />)
      await userEvent.setup().click(screen.getByRole('switch'))
      expect(setFlaggedOnly).toHaveBeenCalledTimes(1)
      expect(setFlaggedOnly.mock.calls[0][0]).toBe(true)
    })

    it('does not show "Clear filters" when no filter is active', () => {
      rtlRender(<ScoringControls {...controlsBaseProps()} />)
      expect(screen.queryByRole('button', { name: /clear filters/i })).toBeNull()
    })

    it('shows "Clear filters" once any threshold is set and resets all three on click', () => {
      const setFlaggedOnly = vi.fn()
      const setMinReqs = vi.fn()
      const setMin4xxPct = vi.fn()
      rtlRender(
        <ScoringControls
          {...controlsBaseProps({
            flaggedOnly: true, minReqs: 250, min4xxPct: 40, setFlaggedOnly, setMinReqs, setMin4xxPct,
          })}
        />,
      )
      fireEvent.click(screen.getByRole('button', { name: /clear filters/i }))
      expect(setFlaggedOnly).toHaveBeenCalledWith(false)
      expect(setMinReqs).toHaveBeenCalledWith('')
      expect(setMin4xxPct).toHaveBeenCalledWith('')
    })

    it('calls refetch when the Refresh button is clicked', () => {
      const refetch = vi.fn()
      rtlRender(<ScoringControls {...controlsBaseProps({ refetch })} />)
      fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
      expect(refetch).toHaveBeenCalledTimes(1)
    })

    it('disables the Refresh button while isFetching=true', () => {
      rtlRender(<ScoringControls {...controlsBaseProps({ isFetching: true })} />)
      expect(screen.getByRole('button', { name: /refresh/i })).toBeDisabled()
    })

    it('dims the control bar while a background refetch is running', () => {
      const { container } = rtlRender(<ScoringControls {...controlsBaseProps({ isFetching: true, isLoadingInitial: false })} />)
      expect(container.querySelector('.opacity-40')).toBeInTheDocument()
    })

    it('renders the streaming sessions switch when cmcdEnabled is true', () => {
      rtlRender(<ScoringControls {...controlsBaseProps({ cmcdEnabled: true })} />)
      expect(screen.getByText(/streaming sessions/i)).toBeInTheDocument()
    })

    it('does not render the streaming sessions switch when cmcdEnabled is false', () => {
      rtlRender(<ScoringControls {...controlsBaseProps({ cmcdEnabled: false })} />)
      expect(screen.queryByText(/streaming sessions/i)).toBeNull()
    })
  })
})
