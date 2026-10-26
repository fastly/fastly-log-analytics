import { act, render, renderHook, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'
import {
  createTestQueryClient, makeQueryWrapper, renderWithQuery, renderWithQueryClient,
  stubMatchMedia, stubResizeObserver, stubGetAnimations, stubPointerCaptureAndScroll,
} from '../helpers/page-smoke'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import React from 'react'

let mockSearchParamsString = 'service=svc-test'
let mockScoringSubcomponents = false

const { adminFetchSpy, adminJsonSpy, adminJsonResultSpy, clientGetSpy, clientPostSpy, realApiRef, notFoundMock } = vi.hoisted(() => ({
  adminFetchSpy: vi.fn(),
  adminJsonSpy: vi.fn(),
  adminJsonResultSpy: vi.fn(),
  clientGetSpy: vi.fn(),
  clientPostSpy: vi.fn(),
  notFoundMock: vi.fn(() => { throw new Error('NEXT_HTTP_ERROR_FALLBACK;404') }),
  realApiRef: {
    adminFetch: null as ((...args: unknown[]) => Promise<Response>) | null,
    adminJson: null as ((...args: unknown[]) => Promise<unknown>) | null,
    adminJsonResult: null as ((...args: unknown[]) => Promise<unknown>) | null,
    clientGet: null as ((...args: unknown[]) => Promise<unknown>) | null,
    clientPost: null as ((...args: unknown[]) => Promise<unknown>) | null,
  },
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
    client: {
      ...actual.client,
      GET: (...args: unknown[]) => clientGetSpy(...args),
      POST: (...args: unknown[]) => clientPostSpy(...args),
    },
  }
})

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useAdminEventStream: () => ({ state: 'open' as const }),
}))

vi.mock('@/components/SessionScoring/ScoringAdminPanels', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/ScoringAdminPanels')>()
  return {
    ...actual,
    ScoringHealthCard: (...args: Parameters<typeof actual.ScoringHealthCard>) =>
      mockScoringSubcomponents ? null : actual.ScoringHealthCard(...args),
    ScorerFailOpenBreakdownCard: (...args: Parameters<typeof actual.ScorerFailOpenBreakdownCard>) =>
      mockScoringSubcomponents ? null : actual.ScorerFailOpenBreakdownCard(...args),
    MatrixVersionsCard: (...args: Parameters<typeof actual.MatrixVersionsCard>) =>
      mockScoringSubcomponents ? null : actual.MatrixVersionsCard(...args),
    AuditLogTab: (...args: Parameters<typeof actual.AuditLogTab>) =>
      mockScoringSubcomponents ? null : actual.AuditLogTab(...args),
    ExcludeRegexCard: (...args: Parameters<typeof actual.ExcludeRegexCard>) =>
      mockScoringSubcomponents ? null : actual.ExcludeRegexCard(...args),
    RetrainButton: (...args: Parameters<typeof actual.RetrainButton>) =>
      mockScoringSubcomponents ? null : actual.RetrainButton(...args),
    RotateKeyButton: (...args: Parameters<typeof actual.RotateKeyButton>) =>
      mockScoringSubcomponents ? null : actual.RotateKeyButton(...args),
  }
})

vi.mock('@/components/SessionScoring/StatusPanel', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/StatusPanel')>()
  return {
    ...actual,
    StatusPanel: (props: React.ComponentProps<typeof actual.StatusPanel>) =>
      mockScoringSubcomponents ? (
        <div data-testid="admin-session-scoring-status-card">Status for {props.serviceId}</div>
      ) : (
        actual.StatusPanel(props)
      ),
    L2EnforcementCard: (props: React.ComponentProps<typeof actual.L2EnforcementCard>) =>
      mockScoringSubcomponents ? null : actual.L2EnforcementCard(props),
    RocPrCurves: (...args: Parameters<typeof actual.RocPrCurves>) =>
      mockScoringSubcomponents ? null : actual.RocPrCurves(...args),
    PerReasonAucCard: (...args: Parameters<typeof actual.PerReasonAucCard>) =>
      mockScoringSubcomponents ? null : actual.PerReasonAucCard(...args),
    ScoreDistChart: (...args: Parameters<typeof actual.ScoreDistChart>) =>
      mockScoringSubcomponents ? null : actual.ScoreDistChart(...args),
    ComplianceChart: (...args: Parameters<typeof actual.ComplianceChart>) =>
      mockScoringSubcomponents ? null : actual.ComplianceChart(...args),
    StackedHourlyBarChart: (...args: Parameters<typeof actual.StackedHourlyBarChart>) =>
      mockScoringSubcomponents ? null : actual.StackedHourlyBarChart(...args),
    ScorerLatencyChart: (...args: Parameters<typeof actual.ScorerLatencyChart>) =>
      mockScoringSubcomponents ? null : actual.ScorerLatencyChart(...args),
    ScorerErrorsChart: (...args: Parameters<typeof actual.ScorerErrorsChart>) =>
      mockScoringSubcomponents ? null : actual.ScorerErrorsChart(...args),
    ScorerSeriesCard: (...args: Parameters<typeof actual.ScorerSeriesCard>) =>
      mockScoringSubcomponents ? null : actual.ScorerSeriesCard(...args),
  }
})

vi.mock('@/components/SessionScoring/ThresholdSlider', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/ThresholdSlider')>()
  return {
    ...actual,
    ThresholdSlider: (...args: Parameters<typeof actual.ThresholdSlider>) =>
      mockScoringSubcomponents ? null : actual.ThresholdSlider(...args),
  }
})

vi.mock('@/components/SessionScoring/TopFlaggedTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/SessionScoring/TopFlaggedTable')>()
  return {
    ...actual,
    TopFlaggedTable: (...args: Parameters<typeof actual.TopFlaggedTable>) =>
      mockScoringSubcomponents ? null : actual.TopFlaggedTable(...args),
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
import ClickHouseAdminClient, { ClickHouseStatusNotice } from '@/app/admin/clickhouse/_sections/ClickHouseAdminClient'
import { OperationsOverview, AdminTrendsClient, ServicesTable } from '@/app/admin/_sections/OperationsOverview'
import { QuarantineSection } from '@/app/control-room/_sections/ControlRoomClient'
import TrendsLoading from '@/app/admin/trends/loading'
import { useServiceStore } from '@/stores/filterStore'
import { getApiBase } from '@/lib/api'
import { server } from '@/__tests__/helpers/msw/handlers'
import { InviteAnalystDialog, InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { CreateInviteDialog } from '@/components/share-dashboard/CreateInviteDialog'
import { useTableSort, type ShareStatus, type SortAccessors } from '@/components/share-dashboard/SharingControlPanel'
import type { ServiceConfig } from '@/types/api'
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
  useSearchParams: vi.fn(() => new URLSearchParams(mockSearchParamsString)),
  notFound: () => notFoundMock(),
}))
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

beforeEach(() => {
  vi.clearAllMocks()
  mockSearchParamsString = 'service=svc-test'
  mockScoringSubcomponents = false
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
      totals: { requests: 1000, log_lines: 1000, log_bytes: 50000, ingested_rows: 1000, gap: 0, gap_pct: 0 },
      sustained_loss: null,
    })),
    http.get('/api/admin/slow-queries/count', () => HttpResponse.json({ count: 0, since_hours: 24, threshold_ms: 1000 })),
    http.get('/api/admin/celery/status', () => HttpResponse.json({
      workers: { count: 4 }, queues: { 'q.ingest': 0, 'q.control': 0 }, ledger: { dead_letter: 0, quarantined: 0 },
    })),
  )

  renderWithQuery(<OperationsOverview />, { queryClient: createTestQueryClient({ queries: { staleTime: 0 } }) })
  for (const label of ['Live Queries', 'Ingest Gap', 'Notable Slow Queries', 'Workers & Queues']) {
    expect(screen.getByText(label)).toBeInTheDocument()
  }
  await waitFor(() => {
    expect(screen.getByText('0%')).toBeInTheDocument()
    expect(screen.getByText('healthy · 24h')).toBeInTheDocument()
    expect(screen.getByText('4 workers')).toBeInTheDocument()
  })
})

test('ServicesTable renders services, manual Refresh Status trigger, and Delete Data / Teardown menu order', async () => {
  let fetchCount = 0
  server.use(
    http.get(`${API_BASE}/api/services`, () => {
      fetchCount += 1
      return HttpResponse.json({
        services: [{ service_id: 'test-svc', name: 'Production Edge CDN', fos_bucket: 'prod-logs-bucket', access_level: 'read_write' }],
      })
    }),
  )

  const user = userEvent.setup()
  renderWithQuery(<ServicesTable />, { queryClient: createTestQueryClient({ queries: { staleTime: 0 } }) })
  await waitFor(() => {
    expect(screen.getByText('Production Edge CDN')).toBeInTheDocument()
    expect(screen.getByText('test-svc')).toBeInTheDocument()
    expect(screen.getByText('prod-logs-bucket')).toBeInTheDocument()
  })

  await user.click(screen.getByTestId('admin-overview-refresh-btn'))
  await waitFor(() => expect(fetchCount).toBeGreaterThanOrEqual(2))

  await user.click(screen.getByRole('button', { name: /manage/i }))
  const deleteDataItem = await screen.findByText('Delete Data')
  const teardownItem = screen.getByText('Teardown Service')
  expect(deleteDataItem.compareDocumentPosition(teardownItem) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  await user.click(deleteDataItem)
  expect(await screen.findByText('Delete Data: Production Edge CDN')).toBeInTheDocument()
})

const evidence = {
  id: 17, source_type: 'request', original_key: 'raw/request/example.log.gz',
  line_ordinal: 9, byte_offset: 128, byte_length: 4, error_category: 'invalid_json',
  error_text: 'Unexpected end of JSON input', sha256: 'a'.repeat(64), quarantined_at: '2026-09-29T12:00:00Z',
}

const jsonResponse = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })

describe('QuarantineSection', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service', isInitialized: true })
    adminFetchSpy.mockImplementation(async (input: string) => {
      if (input.includes('/summary')) {
        return jsonResponse({
          total_items: 1, total_bytes: 4, request_items: 1, rum_items: 0,
          oldest_at: evidence.quarantined_at, newest_at: evidence.quarantined_at, category_counts: { invalid_json: 1 },
        })
      }
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
    expect(screen.getByTestId('admin-trends-latest-mem_used_pct')).toHaveTextContent('54.3%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct')).toHaveTextContent('31.2%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct_root')).toHaveTextContent('44.8%')
    expect(screen.getByTestId('admin-trends-latest-active_query_count')).toHaveTextContent('2.00')
    expect(screen.getByTestId('admin-trends-latest-pool_wait_p95_ms')).toHaveTextContent('11.5ms')
    expect(screen.getByTestId('admin-trends-latest-ingest_lag_s')).toHaveTextContent('18s')

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

describe('InvitationsPanel, CreateInviteDialog, useTableSort & InviteAnalystDialog', () => {
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

  test('InviteAnalystDialog handles Copy JSON success and clipboard rejection', async () => {
    useServiceStore.setState({ activeServiceId: 'svc-copy' })
    server.use(http.post('*/api/services/:service_id/generate-viewer-key', () => HttpResponse.json({
      name: 'analyst', service_id: 'svc-copy', fos_bucket: 'b', fos_region: 'r', fos_endpoint: 'e', fos_prefix: 'p',
      access_key_id: 'AKIAEXAMPLE', secret_key: 'not-a-real-secret',
    })))
    render(<InviteAnalystDialog service={{ service_id: 'svc-copy', name: 'Copy Svc' } as unknown as ServiceConfig} open onOpenChange={vi.fn()} />, {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: /generate invite/i })) })
    const btn = await screen.findByRole('button', { name: /copy json/i })

    const writeText = vi.fn().mockResolvedValue(undefined)
    stubClipboard(writeText)
    vi.useFakeTimers()
    await act(async () => { fireEvent.click(btn) })
    expect(JSON.parse(writeText.mock.calls[0][0]).service_id).toBe('svc-copy')
    expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument()
    await act(async () => { vi.advanceTimersByTime(2000) })
    expect(screen.getByRole('button', { name: /copy json/i })).toBeInTheDocument()

    stubClipboard(vi.fn().mockRejectedValue(new Error('denied')))
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: /copy json/i })) })
    expect(screen.queryByRole('button', { name: /copied!/i })).toBeNull()
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
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders route-level loading skeleton with zero-CLS reservation containers and contextual messages', () => {
    render(<QueueLoading />)
    expect(screen.getByTestId('admin-queue-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-toolbar-skeleton')).toBeInTheDocument()
    expect(screen.getAllByText(/Inspecting Celery task broker/i).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Loading queue telemetry/i).length).toBeGreaterThan(0)
  })

  it('renders pre-allocated zero-CLS reservation containers and contextual in-place skeletons while queries are pending', () => {
    clientGetSpy.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<AdminQueuePage />)
    expect(screen.getByTestId('admin-queue-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-trends-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-depths-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-workers-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-ledger-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-schedules-reservation')).toBeInTheDocument()
    expect(screen.getAllByText(/Inspecting Celery task broker/i).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Loading queue telemetry/i).length).toBeGreaterThan(0)
    expect(screen.getByTestId('admin-queue-refresh-btn')).toBeDisabled()
  })

  it('renders High-Scale queue depths, active workers, ingest ledger cards, in-process schedules, and supports manual Refresh', async () => {
    let statusCalls = 0
    let batchCalls = 0

    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        statusCalls += 1
        return {
          data: {
            broker_reachable: true,
            workers: {
              count: 1,
              active_tasks: {
                'celery@worker-1': [
                  {
                    id: 'task-uuid-101',
                    name: 'backend.tasks.ingest.ingest_batch',
                    args: ['svc-alpha', 25],
                    time_start: 1700000000,
                  },
                ],
              },
              stats: { 'celery@worker-1': {} },
              registered: { 'celery@worker-1': ['backend.tasks.ingest.ingest_batch'] },
              scheduled: { 'celery@worker-1': [] },
            },
            queues: {
              celery: 0,
              'q.control': 2,
              'q.ingest': 9,
            },
            inprocess_schedules: [
              {
                name: 'log_discovery_svc-alpha',
                trigger: 'interval[0:00:10]',
                next_run_at: 1700000010,
              },
            ],
            ledger: {
              discovered: 5,
              claimed: 2,
              committed: 140,
              quarantined: 1,
              dead_letter: 0,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        batchCalls += 1
        return {
          data: {
            series: {
              celery_queue_depth: [
                { ts: 1700000000, value: 4 },
                { ts: 1700000060, value: 11 },
              ],
              celery_active_tasks: [
                { ts: 1700000000, value: 1 },
                { ts: 1700000060, value: 1 },
              ],
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/celery/ledger') {
        return {
          data: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/2026-10-20T04_28_00.000-test.log.gz',
                source_type: 'rum',
                status: 'quarantined',
                attempts: 1,
                error_kind: 'corrupt_gzip',
                last_error: 'Not a gzipped file',
                originating_task: 'rum_discovery',
                originating_run_id: 42,
                discovered_at: 1700000000,
              },
            ],
            total: 1,
          },
          response: { ok: true, status: 200 },
        }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent('Broker Reachable')
    })

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

  it('renders Standard mode fallback cleanly when broker is unreachable or Celery queues are empty', async () => {
    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: false,
            workers: {
              count: 0,
              active_tasks: {},
              stats: {},
              registered: {},
              scheduled: {},
              error: 'Valkey unreachable',
            },
            queues: {},
            inprocess_schedules: [],
            ledger: {},
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return {
          data: { series: {} },
          response: { ok: true, status: 200 },
        }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent(
        'Broker Unreachable / Standard Mode',
      )
    })

    expect(screen.getByText(/No queues found or Celery mode is inactive/i)).toBeInTheDocument()
    expect(screen.getByText(/No active workers found/i)).toBeInTheDocument()
    expect(screen.getByText(/Ledger is empty/i)).toBeInTheDocument()
    expect(screen.getByText(/No in-process schedules registered/i)).toBeInTheDocument()
  })

  it('renders Standard Mode in-process scheduler & ingest telemetry and hides Celery sections when deployment_mode is standard', async () => {
    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: false,
            deployment_mode: 'standard',
            workers: {
              count: 0,
              active_tasks: {},
              stats: {},
              registered: {},
              scheduled: {},
              error: 'Celery broker not configured in standard mode',
            },
            queues: {},
            inprocess_schedules: [
              {
                name: 'log_discovery_svc-alpha',
                trigger: 'interval[0:00:10]',
                next_run_at: 1700000010,
              },
              {
                name: 'commit_svc-alpha',
                trigger: 'interval[0:05:00]',
                next_run_at: 1700000300,
              },
            ],
            inprocess_active_runs: [
              {
                run_id: 101,
                service_id: 'svc-alpha',
                task: 'sync',
                started_at: 1700000000,
              },
            ],
            standard_ingest: {
              ingested_files: 1250,
              quarantined_items: 2,
            },
            ledger: {
              committed: 1250,
              quarantined: 2,
              dead_letter: 0,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return { data: { series: {} }, response: { ok: true, status: 200 } }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent(
        'Standard Mode · In-Process Scheduler',
      )
    })

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
    clientGetSpy.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: true,
            workers: { count: 1, active_tasks: {}, stats: {}, registered: {}, scheduled: {} },
            queues: { celery: 0, 'q.ingest': 0, 'q.control': 0 },
            inprocess_schedules: [],
            ledger: {
              discovered: 0,
              claimed: 0,
              committed: 120,
              quarantined: 0,
              dead_letter: 3,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/celery/ledger') {
        return {
          data: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz',
                source_type: 'rum',
                status: 'dead_letter',
                attempts: 1,
                error_kind: 'object_missing',
                last_error: 'object missing from FOS (already ingested+deleted, or expired)',
                originating_task: 'rum_discovery',
                originating_run_id: 24368,
                claimed_at: 1729400000,
              },
            ],
            total: 1,
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return { data: { series: {} }, response: { ok: true, status: 200 } }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    clientPostSpy.mockImplementation(async () => ({
      data: { ok: true, affected: 1, dispatched: 1 },
      response: { ok: true, status: 200 },
    }))

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-ledger-entries')).toBeInTheDocument()
    })

    expect(
      screen.getByText('raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz'),
    ).toBeInTheDocument()
    expect(screen.getByText('object_missing')).toBeInTheDocument()
    expect(
      screen.getByText('object missing from FOS (already ingested+deleted, or expired)'),
    ).toBeInTheDocument()

    fireEvent.click(screen.getByTestId('admin-queue-ledger-retry-row'))
    await waitFor(() => {
      expect(clientPostSpy).toHaveBeenCalledWith(
        '/api/admin/celery/ledger/retry',
        expect.objectContaining({
          body: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz',
              },
            ],
          },
        }),
      )
    })

    fireEvent.click(screen.getByTestId('admin-queue-ledger-purge-all'))
    await waitFor(() => {
      expect(clientPostSpy).toHaveBeenCalledWith(
        '/api/admin/celery/ledger/purge',
        expect.objectContaining({
          body: { status: 'problem' },
        }),
      )
    })
  })
})

function buildMockShareStatus(overrides: Record<string, unknown> = {}) {
  return {
    sharing_active: false,
    public_endpoint: null,
    public_url: null,
    forward_port: null,
    started_at: null,
    max_concurrent_sessions: 10,
    active_session_count: 0,
    rate_limits: {
      failures: [],
      lockouts: [],
    },
    telemetry: {
      current_uptime_s: null,
      heartbeat_unauth_count: 0,
      tunnel_uptime_history: [],
    },
    invites: [
      {
        id: 'inv-1',
        name: 'Security Analyst',
        email: 'analyst@example.com',
        created_at: 1710000000,
        expires_at: null,
        revoked: false,
        max_sessions: 1,
        services: ['svc-1'],
        active_sessions: 0,
      },
    ],
    sessions: [],
    services: [
      {
        service_id: 'svc-1',
        name: 'Primary Edge Service',
        remote_frontend_deployed: false,
        sharing_domain: null,
      },
    ],
    audit_logs: [],
    ...overrides,
  }
}

describe('admin/share page', () => {
  beforeEach(() => {
    mockSearchParamsString = ''
  })

  test('renders zero-CLS reservation containers, SharingControlPanel, InvitationsPanel, and tabs', async () => {
    let statusCalls = 0
    let liveCalls = 0

    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => {
        statusCalls += 1
        return HttpResponse.json(buildMockShareStatus())
      }),
      http.get(`${API_BASE}/api/admin/share/live`, () => {
        liveCalls += 1
        return HttpResponse.json({
          sharing_active: false,
          public_url: null,
          active_session_count: 0,
          rate_limits: {
            failures: [],
            lockouts: [],
          },
          telemetry: {
            current_uptime_s: null,
            heartbeat_unauth_count: 0,
            tunnel_uptime_history: [],
          },
        })
      }),
    )

    renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }),
    })

    await waitFor(() => {
      expect(statusCalls).toBe(1)
      expect(liveCalls).toBe(1)
    })

    const root = screen.getByTestId('admin-share-root')
    const controlRes = screen.getByTestId('admin-share-control-reservation')
    const panelRes = screen.getByTestId('admin-share-panel-reservation')
    expect(root).toBeTruthy()
    expect(controlRes.className).toContain('[contain-intrinsic-size:auto_220px]')
    expect(controlRes.className).toContain('[content-visibility:auto]')
    expect(panelRes.className).toContain('[contain-intrinsic-size:auto_360px]')
    expect(panelRes.className).toContain('[content-visibility:auto]')

    await waitFor(() => {
      expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy()
      expect(screen.getByTestId('admin-share-status-badge').textContent).toBe('OFF')
      expect(screen.getByTestId('admin-share-invites-panel')).toBeTruthy()
    })

    expect(screen.getByTestId('admin-share-tab-invites')).toBeTruthy()
    expect(screen.getByTestId('admin-share-tab-sessions')).toBeTruthy()
    expect(screen.getByTestId('admin-share-tab-audit')).toBeTruthy()
    expect(screen.getByText('analyst@example.com')).toBeTruthy()
  })

  test('manual Refresh button refetches /api/admin/share/status', async () => {
    let statusCalls = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => {
        statusCalls += 1
        return HttpResponse.json(
          buildMockShareStatus({
            sharing_active: statusCalls > 1,
            public_url: statusCalls > 1 ? 'https://share.example.com' : null,
          }),
        )
      }),
      http.get(`${API_BASE}/api/admin/share/live`, () => {
        return HttpResponse.json({})
      }),
    )

    renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }),
    })

    await waitFor(() => {
      expect(statusCalls).toBe(1)
      expect(screen.getByTestId('admin-share-status-badge').textContent).toBe('OFF')
    })

    const refreshBtn = screen.getByTestId('admin-share-refresh-btn')
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(statusCalls).toBe(2)
      expect(screen.getByTestId('admin-share-status-badge').textContent).toBe('ACTIVE')
    })
  })

  test('initializes activeTab from URL ?tab= query parameter', async () => {
    mockSearchParamsString = 'tab=sessions'

    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => {
        return HttpResponse.json(buildMockShareStatus())
      }),
      http.get(`${API_BASE}/api/admin/share/live`, () => {
        return HttpResponse.json({})
      }),
    )

    renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }),
    })

    await waitFor(() => {
      expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy()
    })

    const sessionsTrigger = screen.getByTestId('admin-share-tab-sessions')
    expect(sessionsTrigger.getAttribute('aria-selected')).toBe('true')
  })

  test('displays destructive alert when /api/admin/share/status returns 500', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => {
        return HttpResponse.json({ detail: 'Share DB unavailable' }, { status: 500 })
      }),
      http.get(`${API_BASE}/api/admin/share/live`, () => {
        return HttpResponse.json({})
      }),
    )

    renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({ searchParams: mockSearchParamsString }),
    })

    await waitFor(() => {
      expect(screen.getByTestId('admin-share-status-error')).toBeTruthy()
    })
  })

  test('an unknown ?tab= falls back to invites; tab clicks write and clear ?tab= (replace)', async () => {
    mockSearchParamsString = 'tab=bogus'
    const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()

    server.use(
      http.get(`${API_BASE}/api/admin/share/status`, () => HttpResponse.json(buildMockShareStatus())),
      http.get(`${API_BASE}/api/admin/share/live`, () => HttpResponse.json({})),
    )

    renderWithQuery(<ShareDashboardPage />, {
      wrapper: withNuqsTestingAdapter({
        searchParams: mockSearchParamsString,
        hasMemory: true,
        onUrlUpdate,
        resetUrlUpdateQueueOnMount: false,
      }),
    })

    await waitFor(() => expect(screen.getByTestId('admin-share-control-panel')).toBeTruthy())
    expect(screen.getByTestId('admin-share-tab-invites').getAttribute('aria-selected')).toBe('true')

    fireEvent.click(screen.getByTestId('admin-share-tab-sessions'))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)?.[0].searchParams.get('tab')).toBe('sessions'))
    expect(onUrlUpdate.mock.calls.at(-1)?.[0].options.history).toBe('replace')

    fireEvent.click(screen.getByTestId('admin-share-tab-invites'))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)?.[0].searchParams.has('tab')).toBe(false))
  })
})

describe('admin/session-scoring page', () => {
  beforeEach(() => {
    mockScoringSubcomponents = true
    useServiceStore.setState({
      activeServiceId: 'svc-default',
      isInitialized: true,
    })
  })

  test('renders without unhandled requests (default MSW handlers cover the composites)', async () => {
    const analyticsHits: string[] = []
    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, ({ params }) => {
        analyticsHits.push(String(params.service_id))
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({
          status: { enabled: false },
          threshold: { value: 50 },
          exclude_regex: { pattern: '' },
          enforce_status_code: { code: 403 },
          matrix_versions: { versions: [] },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(analyticsHits, 'scoring/analytics composite must have been called').not.toEqual([])
      expect(configHits, 'scoring/config composite must have been called').not.toEqual([])
    })

    expect(analyticsHits[0]).toBe('svc-default')
    expect(configHits[0]).toBe('svc-default')

    const root = screen.getByTestId('admin-session-scoring-root')
    const statusRes = screen.getByTestId('admin-session-scoring-status-reservation')
    const overviewRes = screen.getByTestId('admin-session-scoring-overview-reservation')
    expect(root).toBeTruthy()
    expect(statusRes.className).toContain('[contain-intrinsic-size:auto_220px]')
    expect(statusRes.className).toContain('[content-visibility:auto]')
    expect(overviewRes.className).toContain('[contain-intrinsic-size:auto_900px]')
    expect(overviewRes.className).toContain('[content-visibility:auto]')

    expect(screen.getByTestId('admin-session-scoring-tab-overview')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-labels')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-matrix')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-audit')).toBeTruthy()
  })

  test('manual refresh button invalidates and refetches composites', async () => {
    let configCalls = 0
    let analyticsCalls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () => {
        analyticsCalls += 1
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, () => {
        configCalls += 1
        return HttpResponse.json({
          status: { enabled: true },
          threshold: { threshold: 75 },
          exclude_regex: { pattern: '' },
          enforce_status_code: { effective: 429 },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(configCalls).toBe(1)
      expect(analyticsCalls).toBe(1)
    })

    const refreshBtn = screen.getByTestId('admin-session-scoring-refresh-btn')
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(configCalls).toBe(2)
      expect(analyticsCalls).toBe(2)
    })
  })

  test('falls back to /api/services when activeServiceId is initially empty', async () => {
    useServiceStore.setState({
      activeServiceId: null,
      isInitialized: false,
    })

    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services`, () => {
        return HttpResponse.json({
          services: [{ service_id: 'svc-fallback', name: 'Fallback Service' }],
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () => {
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({
          status: { enabled: false },
          threshold: { threshold: null },
          exclude_regex: { pattern: '' },
          enforce_status_code: { effective: 429 },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(configHits).toContain('svc-fallback')
    })
  })
})
