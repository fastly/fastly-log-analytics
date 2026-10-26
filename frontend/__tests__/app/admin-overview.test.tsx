import { act, render, renderHook, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { withNuqsTestingAdapter } from 'nuqs/adapters/testing'
import {
  createTestQueryClient,
  makeQueryWrapper,
  renderWithQuery,
  renderWithQueryClient,
  stubMatchMedia,
  stubResizeObserver,
  stubGetAnimations,
  stubPointerCaptureAndScroll,
} from '../helpers/page-smoke'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import React from 'react'

const { adminFetchSpy, adminJsonSpy, adminJsonResultSpy, clientGetSpy, realApiRef, notFoundMock } = vi.hoisted(() => ({
  adminFetchSpy: vi.fn(),
  adminJsonSpy: vi.fn(),
  adminJsonResultSpy: vi.fn(),
  clientGetSpy: vi.fn(),
  notFoundMock: vi.fn(() => {
    throw new Error('NEXT_HTTP_ERROR_FALLBACK;404')
  }),
  realApiRef: {
    adminFetch: null as ((...args: unknown[]) => Promise<Response>) | null,
    adminJson: null as ((...args: unknown[]) => Promise<unknown>) | null,
    adminJsonResult: null as ((...args: unknown[]) => Promise<unknown>) | null,
    clientGet: null as ((...args: unknown[]) => Promise<unknown>) | null,
  },
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  realApiRef.adminFetch = actual.adminFetch as (...args: unknown[]) => Promise<Response>
  realApiRef.clientGet = actual.client.GET as (...args: unknown[]) => Promise<unknown>
  realApiRef.adminJsonResult = actual.adminJsonResult as (...args: unknown[]) => Promise<unknown>
  const defaultAdminJson = async (...args: unknown[]): Promise<unknown> => {
    const [, url, init, errorMessage] = args as [
      unknown,
      unknown,
      unknown,
      ((status: number) => string) | undefined,
    ]
    const r = await adminFetchSpy(url, init)
    if (!r.ok) throw new Error((errorMessage ?? ((status: number) => `status ${status}`))(r.status))
    return r.json()
  }
  realApiRef.adminJson = defaultAdminJson
  adminFetchSpy.mockImplementation(realApiRef.adminFetch)
  adminJsonSpy.mockImplementation(defaultAdminJson)
  adminJsonResultSpy.mockImplementation(realApiRef.adminJsonResult)
  clientGetSpy.mockImplementation(realApiRef.clientGet)
  return {
    ...actual,
    adminFetch: (...args: unknown[]) => adminFetchSpy(...args),
    adminJson: (...args: unknown[]) => adminJsonSpy(...args),
    adminJsonResult: (...args: unknown[]) => adminJsonResultSpy(...args),
    client: {
      ...actual.client,
      GET: (...args: unknown[]) => clientGetSpy(...args),
    },
  }
})

import AdminPage from '@/app/admin/page'
import Loading from '@/app/admin/loading'
import RumAdminPage from '@/app/admin/rum/page'
import RumLoading from '@/app/admin/rum/loading'
import QueryMonitorPage from '@/app/admin/queries/page'
import QueriesLoading from '@/app/admin/queries/loading'
import ClickHouseLoading from '@/app/admin/clickhouse/loading'
import ClickHouseAdminPage from '@/app/admin/clickhouse/page'
import ClickHouseAdminClient, { ClickHouseStatusNotice } from '@/app/admin/clickhouse/_sections/ClickHouseAdminClient'
import { OperationsOverview, AdminTrendsClient } from '@/app/admin/_sections/OperationsOverview'
import { ServicesTable } from '@/app/admin/_sections/ServicesTable'
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
  useSearchParams: vi.fn(() => new URLSearchParams('service=svc-test')),
  notFound: () => notFoundMock(),
}))
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

beforeEach(() => {
  vi.clearAllMocks()
  if (realApiRef.adminFetch) adminFetchSpy.mockImplementation(realApiRef.adminFetch)
  if (realApiRef.adminJson) adminJsonSpy.mockImplementation(realApiRef.adminJson)
  if (realApiRef.adminJsonResult) adminJsonResultSpy.mockImplementation(realApiRef.adminJsonResult)
  if (realApiRef.clientGet) clientGetSpy.mockImplementation(realApiRef.clientGet)
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
})

test('Loading skeleton renders zero-CLS reservation containers and contextual messages', () => {
  render(<Loading />)
  expect(screen.getByTestId('admin-overview-loading')).toBeInTheDocument()
  expect(screen.getByText('Inspecting background scheduler state...')).toBeInTheDocument()
  expect(screen.getByText('Loading service configs...')).toBeInTheDocument()
})

test('AdminPage wraps all child sections in zero-CLS contain-intrinsic-size reservations', () => {
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  const { container } = renderWithQuery(<AdminPage />, { queryClient })
  expect(screen.getByTestId('admin-overview-root')).toBeInTheDocument()
  expect(screen.getByTestId('admin-operations-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-services-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-health-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-quarantine-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-global-settings-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-pricing-reservation')).toBeInTheDocument()

  const reserved = container.querySelectorAll('[class*="contain-intrinsic-size"]')
  expect(reserved.length).toBeGreaterThanOrEqual(6)
})

test('OperationsOverview renders live query, ingest gap, slow query, and worker status cards', async () => {
  server.use(
    http.get('/api/admin/queries/summary', () =>
      HttpResponse.json({
        active_total: 2,
        by_db_type: { duckdb: 2 },
        recent_count: 15,
      }),
    ),
    http.get('/api/admin/log-accounting', () =>
      HttpResponse.json({
        configured: true,
        service_id: 'test-svc',
        hours: 24,
        buckets: [],
        totals: {
          requests: 1000,
          log_lines: 1000,
          log_bytes: 50000,
          ingested_rows: 1000,
          gap: 0,
          gap_pct: 0,
        },
        sustained_loss: null,
      }),
    ),
    http.get('/api/admin/slow-queries/count', () =>
      HttpResponse.json({ count: 0, since_hours: 24, threshold_ms: 1000 }),
    ),
    http.get('/api/admin/celery/status', () =>
      HttpResponse.json({
        workers: { count: 4 },
        queues: { 'q.ingest': 0, 'q.control': 0 },
        ledger: { dead_letter: 0, quarantined: 0 },
      }),
    ),
  )

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  renderWithQuery(<OperationsOverview />, { queryClient })

  expect(screen.getByText('Live Queries')).toBeInTheDocument()
  expect(screen.getByText('Ingest Gap')).toBeInTheDocument()
  expect(screen.getByText('Notable Slow Queries')).toBeInTheDocument()
  expect(screen.getByText('Workers & Queues')).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.getByText('0%')).toBeInTheDocument()
    expect(screen.getByText('healthy · 24h')).toBeInTheDocument()
    expect(screen.getByText('4 workers')).toBeInTheDocument()
  })
})

test('ServicesTable renders loading status, services list, and manual Refresh Status trigger', async () => {
  let fetchCount = 0
  server.use(
    http.get(`${API_BASE}/api/services`, () => {
      fetchCount += 1
      return HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Production Edge CDN',
            fos_bucket: 'prod-logs-bucket',
            access_level: 'read_write',
          },
        ],
      })
    }),
  )

  const user = userEvent.setup()
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  renderWithQuery(<ServicesTable />, { queryClient })

  await waitFor(() => {
    expect(screen.getByText('Production Edge CDN')).toBeInTheDocument()
    expect(screen.getByText('prod-logs-bucket')).toBeInTheDocument()
  })

  const refreshBtn = screen.getByTestId('admin-overview-refresh-btn')
  expect(refreshBtn).toBeInTheDocument()
  await user.click(refreshBtn)

  await waitFor(() => {
    expect(fetchCount).toBeGreaterThanOrEqual(2)
  })
})

const evidence = {
  id: 17,
  source_type: 'request',
  original_key: 'raw/request/example.log.gz',
  line_ordinal: 9,
  byte_offset: 128,
  byte_length: 4,
  error_category: 'invalid_json',
  error_text: 'Unexpected end of JSON input',
  sha256: 'a'.repeat(64),
  quarantined_at: '2026-09-29T12:00:00Z',
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
}

describe('QuarantineSection', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service', isInitialized: true })
    adminFetchSpy.mockImplementation(async (input: string) => {
      if (input.includes('/summary')) {
        return jsonResponse({
          total_items: 1,
          total_bytes: 4,
          request_items: 1,
          rum_items: 0,
          oldest_at: evidence.quarantined_at,
          newest_at: evidence.quarantined_at,
          category_counts: { invalid_json: 1 },
        })
      }
      if (input.includes('/download/')) return new Response(new Uint8Array([0x00, 0xff, 0x7b, 0x7d]))
      if (input.includes('/purge')) return jsonResponse({ purged: 1 })
      return jsonResponse({ items: [evidence], total: 1 })
    })
  })

  afterEach(() => {
    if (realApiRef.adminFetch) adminFetchSpy.mockImplementation(realApiRef.adminFetch)
  })

  test('shows evidence metadata and downloads the selected exact-byte item endpoint', async () => {
    const user = userEvent.setup()
    const createObjectURL = vi.fn(() => 'blob:quarantine-test')
    vi.stubGlobal('URL', { ...URL, createObjectURL, revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    renderWithQuery(<QuarantineSection />)

    await user.click(await screen.findByRole('button', { name: 'Details' }))
    expect(await screen.findByText('raw/request/example.log.gz')).toBeInTheDocument()
    expect(screen.getByText('Unexpected end of JSON input')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Download' }))
    await waitFor(() => expect(adminFetchSpy).toHaveBeenCalledWith('/api/admin/quarantine/download/17'))
    expect(createObjectURL).toHaveBeenCalledOnce()
    vi.unstubAllGlobals()
  })

  test('reports quarantine query failures with a retry action', async () => {
    adminFetchSpy.mockRejectedValueOnce(new Error('metadata unavailable'))
    renderWithQuery(<QuarantineSection />)

    expect(await screen.findByText('Quarantine unavailable')).toBeInTheDocument()
    expect(screen.getByText('metadata unavailable')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })

  test('purges an individual item through the item-scoped endpoint', async () => {
    const user = userEvent.setup()
    renderWithQuery(<QuarantineSection />)

    await user.click(await screen.findByRole('button', { name: 'Details' }))
    await user.click(await screen.findByRole('button', { name: 'Remove' }))

    await waitFor(() => {
      expect(adminFetchSpy).toHaveBeenCalledWith('/api/admin/quarantine/purge?item_id=17', { method: 'POST' })
    })
  })
})

const SAMPLE_SERIES = {
  cpu_load_1m: [
    { ts: '2026-04-19T12:00:00Z', value: 0.45 },
    { ts: '2026-04-19T12:01:00Z', value: 0.82 },
  ],
  mem_used_pct: [
    { ts: '2026-04-19T12:00:00Z', value: 52.0 },
    { ts: '2026-04-19T12:01:00Z', value: 54.3 },
  ],
  disk_used_pct: [{ ts: '2026-04-19T12:01:00Z', value: 31.2 }],
  disk_used_pct_root: [{ ts: '2026-04-19T12:01:00Z', value: 44.8 }],
  active_query_count: [{ ts: '2026-04-19T12:01:00Z', value: 2.0 }],
  'pool_wait_p95_ms|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 4.2 }],
  'pool_wait_p95_ms|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 11.5 }],
  'ingest_lag_s|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 18.0 }],
  'ingest_lag_s|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 7.0 }],
}

describe('/admin/trends Page & Client', () => {
  beforeEach(() => {
    clientGetSpy.mockReset()
  })

  afterEach(() => {
    if (realApiRef.clientGet) clientGetSpy.mockImplementation(realApiRef.clientGet)
  })

  it('renders loading.tsx with geometry-matched zero-CLS reservation containers', () => {
    render(<TrendsLoading />)

    expect(screen.getByTestId('admin-trends-loading')).toBeInTheDocument()
    const toolbar = screen.getByTestId('admin-trends-toolbar')
    expect(toolbar.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(toolbar.className).toContain('[content-visibility:auto]')

    const grid = screen.getByTestId('admin-trends-grid-reservation')
    expect(grid.className).toContain('[contain-intrinsic-size:auto_680px]')
    expect(grid.className).toContain('[content-visibility:auto]')
    expect(screen.getAllByText('Loading system vital trends...').length).toBeGreaterThanOrEqual(1)
  })

  it('renders pending skeleton reservations before batch data resolves', () => {
    clientGetSpy.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<AdminTrendsClient />)

    expect(screen.getByTestId('admin-trends-root')).toBeInTheDocument()
    const grid = screen.getByTestId('admin-trends-grid-reservation')
    expect(grid.className).toContain('[contain-intrinsic-size:auto_680px]')
    expect(grid.className).toContain('[content-visibility:auto]')
    expect(screen.getAllByText('Loading system vital trends...').length).toBeGreaterThanOrEqual(1)
  })

  it('renders all 7 vital series cards and computes max-per-ts across services', async () => {
    clientGetSpy.mockResolvedValue({
      data: { series: SAMPLE_SERIES },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<AdminTrendsClient />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-trends-latest-cpu_load_1m')).toHaveTextContent('0.82')
    })

    expect(screen.getByTestId('admin-trends-latest-mem_used_pct')).toHaveTextContent('54.3%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct')).toHaveTextContent('31.2%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct_root')).toHaveTextContent('44.8%')
    expect(screen.getByTestId('admin-trends-latest-active_query_count')).toHaveTextContent('2.00')
    expect(screen.getByTestId('admin-trends-latest-pool_wait_p95_ms')).toHaveTextContent('11.5ms')
    expect(screen.getByTestId('admin-trends-latest-ingest_lag_s')).toHaveTextContent('18s')
  })

  it('switches lookback windows (1h -> 6h -> 24h -> 7d) and triggers manual Refresh', async () => {
    clientGetSpy.mockResolvedValue({
      data: { series: SAMPLE_SERIES },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<AdminTrendsClient />)

    await waitFor(() => {
      expect(clientGetSpy).toHaveBeenCalledWith(
        '/api/admin/metric-history/batch',
        expect.objectContaining({
          params: { query: { since: '1h' } },
        }),
      )
    })

    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = screen.getByTestId(`admin-trends-window-${win}`)
      fireEvent.click(btn)

      await waitFor(() => {
        expect(clientGetSpy).toHaveBeenCalledWith(
          '/api/admin/metric-history/batch',
          expect.objectContaining({
            params: { query: { since: win } },
          }),
        )
      })
      expect(btn).toHaveAttribute('aria-pressed', 'true')
    }

    const callsBeforeRefresh = clientGetSpy.mock.calls.length
    const refreshBtn = screen.getByTestId('admin-trends-refresh-btn')
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(clientGetSpy.mock.calls.length).toBeGreaterThan(callsBeforeRefresh)
    })
  })
})

test('ServicesTable renders services flowed through MSW + openapi-fetch', async () => {
  server.use(
    http.get(`${API_BASE}/api/services`, () =>
      HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Test Service',
            fos_bucket: 'test-bucket',
            access_level: 'read_write',
          },
        ],
      }),
    ),
  )

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  renderWithQuery(<ServicesTable />, { queryClient })

  await waitFor(
    () => {
      expect(screen.getByText('Test Service')).toBeInTheDocument()
      expect(screen.getByText('test-svc')).toBeInTheDocument()
      expect(screen.getByText('test-bucket')).toBeInTheDocument()
    },
    { timeout: 3000 },
  )
})

test('the row Manage menu offers Delete Data (left of/above Teardown) and opens its confirmation dialog', async () => {
  server.use(
    http.get(`${API_BASE}/api/services`, () =>
      HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Test Service',
            fos_bucket: 'test-bucket',
            access_level: 'read_write',
          },
        ],
      }),
    ),
  )

  const user = userEvent.setup()
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  renderWithQuery(<ServicesTable />, { queryClient })

  await waitFor(() => expect(screen.getByText('Test Service')).toBeInTheDocument())

  await user.click(screen.getByRole('button', { name: /manage/i }))

  const deleteDataItem = await screen.findByText('Delete Data')
  const teardownItem = screen.getByText('Teardown Service')
  expect(
    deleteDataItem.compareDocumentPosition(teardownItem) & Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy()

  await user.click(deleteDataItem)
  expect(await screen.findByText('Delete Data: Test Service')).toBeInTheDocument()
})

const SVC_RUM = 'svc-admin-rum-test'

describe('Admin RUM Configuration Page (/admin/rum)', () => {
  beforeEach(() => {
    useServiceStore.setState({
      activeServiceId: SVC_RUM,
      services: [{ id: SVC_RUM, name: 'Test RUM Service' }],
      isInitialized: true,
    })
  })

  it('renders route-level loading skeleton with zero-CLS reservations', () => {
    render(<RumLoading />)
    expect(screen.getByTestId('admin-rum-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-toolbar-skeleton')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-skeleton-card')).toBeInTheDocument()
    expect(screen.getByText('Inspecting RUM beacon pipeline...')).toBeInTheDocument()
  })

  it('renders in-place skeleton while RUM status query is pending', async () => {
    server.use(
      http.get('*/api/services/:service_id/rum/status', async () => {
        await new Promise((r) => setTimeout(r, 200))
        return HttpResponse.json({ enabled: false })
      }),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC_RUM, name: 'Test RUM Service' }],
        }),
      ),
    )

    renderWithQuery(<RumAdminPage />)

    expect(screen.getByTestId('admin-rum-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-rum-status-skeleton')).toBeInTheDocument()
    expect(screen.getByText('Inspecting RUM beacon pipeline...')).toBeInTheDocument()
  })

  it('renders enabled RUM status, collectors summary, and Faro SDK version card', async () => {
    server.use(
      http.get('*/api/services/:service_id/rum/status', () =>
        HttpResponse.json({
          enabled: true,
          enabled_at: '2026-10-18T10:00:00Z',
          deployed_vcl_sha: 'sha-123',
          current_vcl_sha: 'sha-123',
          vcl_drift: false,
          capture_vitals: true,
          capture_performance: true,
          capture_errors: true,
          capture_events: false,
          custom_condition: 'req.url !~ "^/internal"',
        }),
      ),
      http.get('*/api/services/:service_id/rum/versions', () =>
        HttpResponse.json({
          available: ['2.3.1', '2.2.0'],
          current: '2.2.0',
          latest: '2.3.1',
          update_available: true,
        }),
      ),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC_RUM, name: 'Test RUM Service' }],
        }),
      ),
    )

    renderWithQuery(<RumAdminPage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('✅ Enabled')
    })

    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent(
      'Beacon Pipeline Active',
    )
    expect(screen.getByTestId('admin-rum-vcl-drift')).toHaveTextContent('✅ No')
    expect(screen.getByTestId('admin-rum-capture-summary')).toHaveTextContent(
      'Vitals, Performance, Errors',
    )
    expect(screen.getByTestId('admin-rum-enable-btn')).toBeDisabled()
    expect(screen.getByTestId('admin-rum-disable-btn')).not.toBeDisabled()

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-faro-version')).toHaveTextContent('2.2.0')
    })
    expect(screen.getByTestId('admin-rum-settings-btn')).toBeInTheDocument()
  })

  it('renders disabled RUM state and triggers manual refresh on click', async () => {
    let statusCalls = 0
    server.use(
      http.get('*/api/services/:service_id/rum/status', () => {
        statusCalls += 1
        return HttpResponse.json({
          enabled: false,
          vcl_drift: false,
        })
      }),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC_RUM, name: 'Test RUM Service' }],
        }),
      ),
    )

    renderWithQuery(<RumAdminPage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('❌ Disabled')
    })

    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent(
      'Beacon Pipeline Inactive',
    )
    expect(screen.getByTestId('admin-rum-enable-btn')).not.toBeDisabled()
    expect(screen.getByTestId('admin-rum-disable-btn')).toBeDisabled()

    const callsBeforeRefresh = statusCalls
    fireEvent.click(screen.getByTestId('admin-rum-refresh-btn'))

    await waitFor(() => {
      expect(statusCalls).toBeGreaterThan(callsBeforeRefresh)
    })
  })
})

describe('Admin Live Query Monitor (/admin/queries)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders Loading skeleton with zero-CLS reservations and contextual copy', () => {
    render(<QueriesLoading />)

    const loadingRoot = screen.getByTestId('admin-queries-loading')
    expect(loadingRoot).toBeDefined()
    expect(screen.getByText('Live Query Monitor')).toBeDefined()
    expect(screen.getAllByText('Inspecting DuckDB connection pool...').length).toBeGreaterThan(0)
    expect(screen.getAllByText('Loading query telemetry...').length).toBeGreaterThan(0)

    const summarySkel = screen.getByTestId('admin-queries-summary-skeleton')
    expect(summarySkel.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(summarySkel.className).toContain('[content-visibility:auto]')
  })

  it('renders QueryMonitorPage with zero-CLS reservation containers and contextual in-place skeletons while pending', () => {
    adminJsonSpy.mockImplementation((_spec: string, route: string) => {
      if (route === '/api/admin/app-config/query-monitor') {
        return Promise.resolve({ enabled: true })
      }
      return new Promise(() => {})
    })

    renderWithQuery(<QueryMonitorPage />, { wrapper: withNuqsTestingAdapter() })

    expect(screen.getByTestId('admin-queries-root')).toBeDefined()
    expect(screen.getByText('Live Query Monitor')).toBeDefined()

    const reservations = [
      'admin-queries-summary-reservation',
      'admin-queries-active-reservation',
      'admin-queries-slow-reservation',
      'admin-queries-completed-reservation',
    ]

    for (const id of reservations) {
      const el = screen.getByTestId(id)
      expect(el).toBeDefined()
      expect(el.className).toContain('[contain-intrinsic-size:auto_')
      expect(el.className).toContain('[content-visibility:auto]')
    }

    expect(screen.getByTestId('admin-queries-active-skeleton')).toBeDefined()
    expect(screen.getByTestId('admin-queries-slow-skeleton')).toBeDefined()
    expect(screen.getByText('Inspecting DuckDB connection pool...')).toBeDefined()
    expect(screen.getByText('Loading query telemetry...')).toBeDefined()
  })

  it('renders populated active, slow, and completed queries and supports refresh and pause controls', async () => {
    const nowSec = Math.floor(Date.now() / 1000)
    adminJsonSpy.mockImplementation(async (spec: string, route: string) => {
      if (spec === '/api/admin/app-config/query-monitor' || route === '/api/admin/app-config/query-monitor') {
        return { enabled: true }
      }
      if (spec === '/api/admin/queries' || route.startsWith('/api/admin/queries')) {
        return {
          last_seq: 12,
          active: [
            {
              query_id: 11,
              db_type: 'DuckDB',
              sql_preview: 'SELECT count(*) FROM logs WHERE status >= 500',
              sql: 'SELECT count(*) FROM logs WHERE status >= 500',
              sql_len: 46,
              service_id: 'svc_alpha',
              started_at_utc: nowSec - 2,
              duration_ms: 1450.0,
              cancellable: true,
              cancelled_at: null,
              attribution: {
                kind: 'admin',
                label: 'admin @ 127.0.0.1',
                principal_id: '127.0.0.1',
                caller_qualname: 'dashboard.aggregates',
                caller_file: 'backend/routers/dashboard.py:42',
                request_path: '/api/dashboard/aggregates',
                request_id: 'req-11',
                cron_job: null,
                cron_run_id: null,
                pool_slot: 'pool#0',
              },
            },
          ],
          completed: [
            {
              query_id: 12,
              db_type: 'Postgres',
              sql_preview: 'SELECT * FROM slow_queries WHERE service_id = $1',
              sql: 'SELECT * FROM slow_queries WHERE service_id = $1',
              sql_len: 49,
              service_id: 'svc_alpha',
              started_at_utc: nowSec - 20,
              ended_at_utc: nowSec - 19,
              duration_ms: 820.0,
              outcome: 'ok',
              error_type: null,
              error_message: null,
              peak_memory_mb: 18.2,
              attribution: {
                kind: 'cron',
                label: 'cron:full_sync',
                principal_id: null,
                caller_qualname: 'slow_queries.list_slow_queries',
                caller_file: 'backend/core/metadata/slow_queries.py:187',
                request_path: null,
                request_id: null,
                cron_job: 'full_sync',
                cron_run_id: 42,
                pool_slot: null,
              },
            },
          ],
        }
      }
      if (spec === '/api/admin/slow-queries' || route.startsWith('/api/admin/slow-queries')) {
        return {
          rows: [],
          since_hours: 24,
          threshold_ms: 500,
        }
      }
      return {}
    })

    renderWithQuery(<QueryMonitorPage />, { wrapper: withNuqsTestingAdapter() })

    const statusLive = await screen.findByText('1 active query')
    expect(statusLive).toBeDefined()
    expect(screen.getByText(/longest:\s*1\.45 s/)).toBeDefined()

    expect(screen.getByText(/Active & Just-Finished/)).toBeDefined()
    expect(screen.getByText('Notable Slow Queries')).toBeDefined()
    expect(screen.getByText('Recently Completed')).toBeDefined()

    const pauseBtn = screen.getByRole('button', { name: /Pause/i })
    fireEvent.click(pauseBtn)
    expect(await screen.findByRole('button', { name: /Resume/i })).toBeDefined()

    const refreshBtn = screen.getByTestId('admin-queries-refresh-btn')
    expect(refreshBtn).toBeDefined()
    fireEvent.click(refreshBtn)
  })

  it('hydrates filters from a pre-nuqs shared link (?q=&kind=&view=&slow=&db=&nogroup=)', async () => {
    adminJsonSpy.mockImplementation(async (_spec: string, route: string) =>
      route === '/api/admin/app-config/query-monitor' ? { enabled: true } : new Promise(() => {}),
    )

    renderWithQuery(<QueryMonitorPage />, {
      wrapper: withNuqsTestingAdapter({
        searchParams: '?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1',
      }),
    })

    expect(await screen.findByDisplayValue('COMPACT')).toBeDefined()
    expect(screen.getByRole('tab', { name: /live only/i }).getAttribute('aria-selected')).toBe('true')
  })
})

describe('/admin/clickhouse page & ClickHouseAdminClient', () => {
  beforeEach(() => {
    notFoundMock.mockClear()
    vi.unstubAllEnvs()
    useServiceStore.setState({
      activeServiceId: 'svc-test',
      services: [{ id: 'svc-test', name: 'Test Service' }],
    })
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('renders route-level loading skeleton with pre-allocated zero-CLS reservation containers', () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    render(<ClickHouseLoading />)
    expect(screen.getByTestId('admin-clickhouse-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-toolbar')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-vitals-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-generation-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-replay-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toHaveAttribute('aria-hidden', 'true')
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toHaveAttribute('aria-hidden', 'true')
  })

  it('triggers notFound() for the route-level loading shell in Standard deployment mode', () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'standard')
    expect(() => render(<ClickHouseLoading />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')
    expect(notFoundMock).toHaveBeenCalled()
  })

  it('keeps notice reservations mounted while status changes and exposes only the applicable notice', () => {
    const { rerender } = render(<ClickHouseStatusNotice />)
    const disabled = screen.getByTestId('admin-clickhouse-disabled-banner')
    const unavailable = screen.getByTestId('admin-clickhouse-unavailable-banner')
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    expect(unavailable).toHaveAttribute('aria-hidden', 'true')

    rerender(<ClickHouseStatusNotice enabled={false} health="disabled" />)
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toBe(disabled)
    expect(disabled).toHaveAttribute('aria-hidden', 'false')
    expect(disabled).not.toHaveClass('invisible')
    expect(unavailable).toHaveClass('invisible')

    rerender(<ClickHouseStatusNotice enabled health="unavailable" />)
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toBe(unavailable)
    expect(unavailable).toHaveAttribute('aria-hidden', 'false')
    expect(unavailable).not.toHaveClass('invisible')
    expect(disabled).toHaveClass('invisible')

    rerender(<ClickHouseStatusNotice enabled health="ok" />)
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    expect(unavailable).toHaveAttribute('aria-hidden', 'true')
    expect(disabled).toHaveClass('invisible')
    expect(unavailable).toHaveClass('invisible')
  })

  it('triggers notFound() on /admin/clickhouse in Standard deployment mode and renders disabled client banner when mounted in High-Scale mode', async () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'standard')
    expect(() => renderWithQueryClient(<ClickHouseAdminPage />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')
    expect(notFoundMock).toHaveBeenCalled()

    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    renderWithQueryClient(<ClickHouseAdminPage />)

    expect(screen.getByTestId('admin-clickhouse-root')).toBeInTheDocument()
    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('disabled')
    })
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toHaveAttribute('aria-hidden', 'false')
    expect(screen.getByTestId('admin-clickhouse-vital-enabled')).toHaveTextContent('Disabled')
    expect(screen.getByTestId('admin-clickhouse-replay-disabled-hint')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-replay-submit-btn')).toBeDisabled()
  })

  it('renders High-Scale mode ok state, active generation, pre-fill, and dry-run replay workflow', async () => {
    server.use(
      http.get('*/api/admin/clickhouse/status', () =>
        HttpResponse.json({
          service_id: 'svc-test',
          enabled: true,
          health: 'ok',
          schema_version: 2,
          publication_counts: { pending: 2, claimed: 1, failed: 0, published: 14 },
          oldest_pending_age_seconds: 18.4,
          last_published_at: '2026-10-20T12:00:00Z',
          active_generation: {
            generation: 'gen-active-01',
            dataset_id: 'ds-active-01',
            coverage_start: '2026-10-20T10:00:00Z',
            coverage_end: '2026-10-20T12:00:00Z',
            expires_at: '2026-10-21T12:00:00Z',
            expired: false,
            target_matches: true,
            coverage_age_seconds: 120,
          },
          expired_dataset_count: 3,
        }),
      ),
      http.post('*/api/admin/clickhouse/replay', async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({
          service_id: body.service_id,
          dry_run: body.dry_run,
          operation: body.dataset_id ? 'rebuild' : 'resume',
          dataset_id: (body.dataset_id as string) || 'ds-active-01',
          generation: (body.generation as string) || 'gen-active-01',
          artifact_count: 5,
          planned_artifacts: 5,
          attempted: body.dry_run ? 0 : 5,
          published: body.dry_run ? 0 : 5,
          activated: !body.dry_run,
          limit: body.limit ?? 100,
        })
      }),
    )

    renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('ok')
    })

    expect(screen.getByTestId('admin-clickhouse-vital-enabled')).toHaveTextContent('High-Scale (Enabled)')
    expect(screen.getByTestId('admin-clickhouse-vital-counts')).toHaveTextContent('14 / 2')
    expect(screen.getByTestId('admin-clickhouse-gen-id')).toHaveTextContent('gen-active-01')
    expect(screen.getByTestId('admin-clickhouse-dataset-id')).toHaveTextContent('ds-active-01')

    fireEvent.click(screen.getByTestId('admin-clickhouse-prefill-btn'))
    const refInput = screen.getByTestId('admin-clickhouse-reference-input') as HTMLInputElement
    expect(refInput.value).toBe('ds-active-01')

    fireEvent.click(screen.getByTestId('admin-clickhouse-replay-submit-btn'))

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toBeInTheDocument()
    })
    expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toHaveTextContent('Dry-Run Replay Preview')
    expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toHaveTextContent('operation: rebuild')
  })

  it('renders unavailable 503 status payload gracefully without crashing', async () => {
    server.use(
      http.get('*/api/admin/clickhouse/status', () =>
        HttpResponse.json(
          {
            service_id: 'svc-test',
            enabled: true,
            health: 'unavailable',
            schema_version: 1,
            publication_counts: { pending: 0, claimed: 0, failed: 1, published: 4 },
            oldest_pending_age_seconds: null,
            last_published_at: null,
            active_generation: null,
            expired_dataset_count: 0,
          },
          { status: 503 },
        ),
      ),
    )

    renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('unavailable')
    })
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toHaveAttribute('aria-hidden', 'false')
  })
})

// ---------------------------------------------------------------------------
// Folded from InvitationsPanel.test.tsx
// ---------------------------------------------------------------------------

const shareStatusFixture = {
  services: [{ service_id: 'svcA', name: 'Service A' }],
  invites: [
    {
      id: 'i1',
      name: 'Alice',
      email: 'alice@corp.com',
      service_ids: ['svcA'],
      expires_at: null,
      revoked: 0,
      auth_method: 'passcode',
      last_login_at: '2026-06-05T09:30:00Z',
    },
    {
      id: 'i2',
      name: 'Bob',
      email: 'bob@corp.com',
      service_ids: [],
      expires_at: null,
      revoked: 0,
      auth_method: 'passcode',
      last_login_at: null,
    },
  ],
  sessions: [
    {
      session_id: 's1',
      invite_id: 'i1',
      email: 'alice@corp.com',
      ip_address: '1.2.3.4',
      last_active_time: '2026-06-05T10:00:00Z',
    },
  ],
} as ShareStatus

const bodyRows = () => screen.getAllByRole('row').slice(1)
const stubClipboard = (writeText: () => Promise<void>) =>
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
const hasCheck = (btn: HTMLElement) => btn.querySelector('svg.lucide-check') !== null

describe('InvitationsPanel', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  test('renders last-login (with Never fallback), online dot, and service name + id', () => {
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)

    expect(screen.getByText('Never')).toBeInTheDocument()
    expect(screen.getAllByLabelText('Online now')).toHaveLength(1)
    expect(screen.getByTitle('Service A (svcA)')).toBeInTheDocument()
    expect(screen.getByText('(svcA)')).toBeInTheDocument()
  })

  test('defaults to last-login desc: the recently-active invite sorts above "Never"', () => {
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)
    expect(bodyRows()[0]).toHaveTextContent('Alice')
    expect(bodyRows()[1]).toHaveTextContent('Bob')
  })

  test('clicking a column header re-sorts the rows', async () => {
    const user = userEvent.setup()
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)

    await user.click(screen.getByRole('button', { name: 'Name' }))
    expect(bodyRows()[0]).toHaveTextContent('Bob')
    expect(bodyRows()[1]).toHaveTextContent('Alice')

    await user.click(screen.getByRole('button', { name: 'Name' }))
    expect(bodyRows()[0]).toHaveTextContent('Alice')
  })

  test('share-card copy flips the icon for the copied row only, then resets', async () => {
    vi.useFakeTimers()
    const writeText = vi.fn().mockResolvedValue(undefined)
    stubClipboard(writeText)
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)
    const [alice, bob] = screen.getAllByTitle('Copy share card')

    await act(async () => {
      fireEvent.click(alice)
    })
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(hasCheck(alice)).toBe(true)
    expect(hasCheck(bob)).toBe(false)

    await act(async () => {
      vi.advanceTimersByTime(1500)
    })
    expect(hasCheck(alice)).toBe(false)
  })

  test('share-card copy does not show success when the clipboard write is rejected', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    stubClipboard(writeText)
    render(<InvitationsPanel status={shareStatusFixture} onRefresh={vi.fn()} onError={vi.fn()} />)
    const [alice] = screen.getAllByTitle('Copy share card')
    await act(async () => {
      fireEvent.click(alice)
    })
    expect(hasCheck(alice)).toBe(false)
  })
})

const shareServices: ShareStatus['services'] = [{ service_id: 'svcA', name: 'Service A' }]

interface InviteBody {
  allow_concurrent_sessions?: boolean
}

function captureInviteBody(): { current: InviteBody | undefined } {
  const captured: { current: InviteBody | undefined } = { current: undefined }
  server.use(
    http.post('*/api/admin/share/invites', async ({ request }) => {
      captured.current = (await request.json()) as InviteBody
      return HttpResponse.json({ id: 'i1' })
    }),
  )
  return captured
}

async function fillRequiredFields(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Name'), 'Jane')
  await user.type(screen.getByLabelText('Email'), 'jane@example.com')
  await user.type(screen.getByLabelText('Passcode'), 'ocean-breeze-cabin-42')
  await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
}

describe('CreateInviteDialog', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'svcA' })
  })

  test('CreateInviteDialog sends allow_concurrent_sessions=true when the shared-logins box is checked', async () => {
    const user = userEvent.setup()
    const posted = captureInviteBody()

    render(
      <CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />,
    )

    await fillRequiredFields(user)
    await user.click(screen.getByRole('checkbox', { name: /multiple people to use this link/i }))
    await user.click(screen.getByRole('button', { name: /create invite/i }))

    await waitFor(() => expect(posted.current).toBeDefined())
    expect(posted.current?.allow_concurrent_sessions).toBe(true)
  })

  test('CreateInviteDialog defaults allow_concurrent_sessions to false', async () => {
    const user = userEvent.setup()
    const posted = captureInviteBody()

    render(
      <CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />,
    )

    await fillRequiredFields(user)
    await user.click(screen.getByRole('button', { name: /create invite/i }))

    await waitFor(() => expect(posted.current).toBeDefined())
    expect(posted.current?.allow_concurrent_sessions).toBe(false)
  })

  test('CreateInviteDialog OAuth mode hides the passcode field and shows the provider selector', async () => {
    server.use(
      http.get('/api/admin/share/oauth-providers', () =>
        HttpResponse.json({ providers: [{ id: 'google', display_name: 'Google Workspace', enabled: true }] }),
      ),
    )
    const user = userEvent.setup()
    render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)

    expect(screen.getByLabelText('Passcode')).toBeInTheDocument()

    await user.click(screen.getByRole('radio', { name: /SSO/i }))
    expect(screen.queryByLabelText('Passcode')).toBeNull()
    expect(await screen.findByLabelText('Identity provider')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Name'), 'Jane')
    await user.type(screen.getByLabelText('Email'), 'jane@example.com')
    await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
    expect(screen.getByRole('button', { name: /create invite/i })).toBeDisabled()
  })

  test('CreateInviteDialog OAuth mode with no configured providers stays unsubmittable', async () => {
    server.use(http.get('/api/admin/share/oauth-providers', () => HttpResponse.json({ providers: [] })))
    const user = userEvent.setup()
    render(<CreateInviteDialog open onOpenChange={vi.fn()} services={shareServices} onCreated={vi.fn()} />)

    await user.type(screen.getByLabelText('Name'), 'Jane')
    await user.type(screen.getByLabelText('Email'), 'jane@example.com')
    await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
    await user.click(screen.getByRole('radio', { name: /SSO/i }))

    expect(await screen.findByText(/add one to the registry first/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create invite/i })).toBeDisabled()
  })
})

type SortRow = { name: string; n: number | null }

const sortRows: SortRow[] = [
  { name: 'bravo', n: 2 },
  { name: 'alpha', n: null },
  { name: 'charlie', n: 1 },
]

const sortAccessors: SortAccessors<SortRow> = {
  name: (r) => r.name,
  n: (r) => r.n,
}

const names = (rs: SortRow[]) => rs.map((r) => r.name)

describe('useTableSort', () => {
  test('default sorts by the given key/direction', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    expect(names(result.current.sorted)).toEqual(['alpha', 'bravo', 'charlie'])
    expect(result.current.sortKey).toBe('name')
    expect(result.current.sortDir).toBe('asc')
  })

  test('toggling the active key flips asc <-> desc', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('name'))
    expect(result.current.sortDir).toBe('desc')
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('nulls always sort last, regardless of direction', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'n', defaultDir: 'desc' }),
    )
    expect(names(result.current.sorted)).toEqual(['bravo', 'charlie', 'alpha'])
    act(() => result.current.toggle('n'))
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('switching to a new key resets direction to desc', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('n'))
    expect(result.current.sortKey).toBe('n')
    expect(result.current.sortDir).toBe('desc')
  })
})

const analystService = { service_id: 'svc-copy', name: 'Copy Svc' } as unknown as ServiceConfig

const analystInvite = {
  name: 'analyst',
  service_id: 'svc-copy',
  fos_bucket: 'b',
  fos_region: 'r',
  fos_endpoint: 'e',
  fos_prefix: 'p',
  access_key_id: 'AKIAEXAMPLE',
  secret_key: 'not-a-real-secret',
}

const openAnalystResult = async () => {
  useServiceStore.setState({ activeServiceId: 'svc-copy' })
  server.use(
    http.post('*/api/services/:service_id/generate-viewer-key', () =>
      HttpResponse.json(analystInvite),
    ),
  )
  render(<InviteAnalystDialog service={analystService} open onOpenChange={vi.fn()} />, {
    wrapper: makeQueryWrapper(createTestQueryClient()),
  })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /generate invite/i }))
  })
  return screen.findByRole('button', { name: /copy json/i })
}

describe('InviteAnalystDialog', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  test('Copy JSON flips to Copied after a successful write and resets', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const btn = await openAnalystResult()
    vi.useFakeTimers()
    await act(async () => {
      fireEvent.click(btn)
    })
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(JSON.parse(writeText.mock.calls[0][0]).service_id).toBe('svc-copy')
    expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument()
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(screen.getByRole('button', { name: /copy json/i })).toBeInTheDocument()
  })

  test('Copy JSON does not claim success when the clipboard write is rejected', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockRejectedValue(new Error('denied')) },
      configurable: true,
    })
    const btn = await openAnalystResult()
    await act(async () => {
      fireEvent.click(btn)
    })
    expect(screen.queryByRole('button', { name: /copied!/i })).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// Folded from StatusPanel.test.tsx
// ---------------------------------------------------------------------------

const SVC_PRAGMA = 'svc-pragma-test'

function renderPanel() {
  return renderWithQuery(<StatusPanel serviceId={SVC_PRAGMA} />)
}

describe('StatusPanel preflight pragma gate', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_PRAGMA, isInitialized: true })
  })

  it('opens the prerequisite modal instead of SSEModal when preflight_pragmas_ready is false, without exposing internal pragma names', async () => {
    let statusCalls = 0
    let readyState = false

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () => {
        statusCalls += 1
        return HttpResponse.json({
          enabled: false,
          preflight_pragmas_ready: readyState,
        })
      }),
    )

    const user = userEvent.setup()
    renderPanel()

    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()

    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))

    const modal = await screen.findByTestId('admin-session-scoring-pragma-modal')
    expect(modal).toBeInTheDocument()
    expect(screen.getByText(/Preflight Request Body Preservation Required/i)).toBeInTheDocument()

    const msgEl = screen.getByTestId('admin-session-scoring-pragma-message')
    expect(msgEl.textContent).toContain(SVC_PRAGMA)
    expect(msgEl.textContent).toContain('Fastly team')
    expect(modal.textContent).not.toContain('fix_unsent_body_drain')
    expect(modal.textContent).not.toContain('no_body_if_bereq_is_get_or_head')
    expect(modal.textContent).not.toContain('hctl')

    readyState = true
    await user.click(screen.getByTestId('admin-session-scoring-pragma-recheck-btn'))

    await waitFor(() => expect(statusCalls).toBeGreaterThanOrEqual(2))
    await waitFor(() =>
      expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument(),
    )
  })

  it('keeps a resolved prerequisite dialog closed if readiness later becomes missing again', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () =>
        HttpResponse.json({ enabled: false, preflight_pragmas_ready: false }),
      ),
    )
    const user = userEvent.setup()
    const { queryClient } = renderPanel()
    await screen.findByTestId('admin-session-scoring-pragma-warning')
    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))
    expect(await screen.findByTestId('admin-session-scoring-pragma-modal')).toBeInTheDocument()

    act(() => {
      queryClient.setQueryData(['scoring-status', SVC_PRAGMA], { enabled: false, preflight_pragmas_ready: true })
    })
    await waitFor(() => expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument())
    act(() => {
      queryClient.setQueryData(['scoring-status', SVC_PRAGMA], { enabled: false, preflight_pragmas_ready: false })
    })
    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()
    expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))
    expect(await screen.findByTestId('admin-session-scoring-pragma-modal')).toBeInTheDocument()
  })

  it('opens the Enable Session Scoring SSEModal when preflight_pragmas_ready is true', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () =>
        HttpResponse.json({
          enabled: false,
          preflight_pragmas_ready: true,
        }),
      ),
    )

    const user = userEvent.setup()
    renderPanel()

    const enableBtn = await screen.findByTestId('admin-session-scoring-enable-btn')
    expect(screen.queryByTestId('admin-session-scoring-pragma-warning')).not.toBeInTheDocument()

    await user.click(enableBtn)
    expect(await screen.findByRole('heading', { name: 'Enable Session Scoring' })).toBeInTheDocument()
  })
})

const SVC_TABLES = 'svc-tables'

describe('AuditLogTab', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_TABLES, isInitialized: true })
  })

  it('renders column headers and rows', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () =>
        HttpResponse.json({
          audit: [
            { id: 1, timestamp: '2026-10-01T00:00:00Z', action: 'retrain', actor: 'admin', details: { a: 1 } },
            { id: 2, timestamp: '2026-10-01T01:00:00Z', action: 'enable', actor: null, details: null },
          ],
        }),
      ),
    )
    renderWithQuery(<AuditLogTab serviceId={SVC_TABLES} />)
    expect(await screen.findByText('retrain')).toBeTruthy()
    for (const name of ['Timestamp', 'Action', 'Actor', 'Details']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('{"a":1}')).toBeTruthy()
    expect(screen.getByText('enable')).toBeTruthy()
  })

  it('shows the empty message when there are no events', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () => HttpResponse.json({ audit: [] })),
    )
    renderWithQuery(<AuditLogTab serviceId={SVC_TABLES} />)
    expect(await screen.findByText(/No audit events yet/)).toBeTruthy()
  })
})

describe('MatrixVersionsCard', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_TABLES, isInitialized: true })
  })

  it('flags the current version and disables its Restore button', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/matrix-versions`, () =>
        HttpResponse.json({
          current_version: 'v2',
          versions: [
            { version: 'v2', key: 'k2', size_bytes: 2048, last_modified: '2026-10-02T00:00:00Z' },
            { version: 'v1', key: 'k1', size_bytes: 1024, last_modified: '2026-10-01T00:00:00Z' },
          ],
        }),
      ),
    )
    renderWithQuery(<MatrixVersionsCard serviceId={SVC_TABLES} />)
    expect(await screen.findByText('current')).toBeTruthy()
    const buttons = screen.getAllByRole('button', { name: /Restore/ })
    expect(buttons).toHaveLength(2)
    expect((buttons[0] as HTMLButtonElement).disabled).toBe(true)
    expect((buttons[1] as HTMLButtonElement).disabled).toBe(false)
  })
})

describe('LabelsTab', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_TABLES, isInitialized: true })
  })

  it('keeps the edit popover state while typing notes', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () =>
        HttpResponse.json({
          labels: [
            {
              id: 'l1',
              sid: 'sid-abc',
              label: 'bad',
              notes: 'orig',
              sample_url: '/x',
              sample_ip: '1.2.3.4',
              updated_at: '2026-10-01T00:00:00Z',
            },
          ],
          counts: { good: 0, neutral: 0, bad: 1 },
        }),
      ),
    )
    const user = userEvent.setup()
    renderWithQuery(<LabelsTab serviceId={SVC_TABLES} />)
    expect(await screen.findByText('sid-abc')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Edit label' }))
    const notes = await screen.findByPlaceholderText('Notes (optional)')
    await user.clear(notes)
    await user.type(notes, 'hello')
    await waitFor(() =>
      expect((screen.getByPlaceholderText('Notes (optional)') as HTMLTextAreaElement).value).toBe('hello'),
    )
  })

  it('shows the empty message with no labels', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () =>
        HttpResponse.json({ labels: [], counts: { good: 0, neutral: 0, bad: 0 } }),
      ),
    )
    renderWithQuery(<LabelsTab serviceId={SVC_TABLES} />)
    expect(await screen.findByText(/No labels yet/)).toBeTruthy()
  })
})

const SVC_L2 = 'svc-l2'

function renderCard() {
  return renderWithQuery(<L2EnforcementCard serviceId={SVC_L2} sinceHours={24} />)
}

function l2State(overrides: Record<string, unknown> = {}) {
  return {
    available: true,
    enabled: false,
    l2_enabled_at: null,
    days_since_optin: null,
    ramp_progress: 0,
    fully_ramped: false,
    warmup_days_remaining: null,
    scoring_enabled_at: null,
    deployment_age_days: 0,
    ready: false,
    ramp_days: 3,
    readiness_days: 7,
    ...overrides,
  }
}

describe('L2EnforcementCard', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: SVC_L2, isInitialized: true })
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/health`, () =>
        HttpResponse.json({ matrix_staleness: { l2_high_pct: 8.5, l2_evaluated: 1000 } }),
      ),
    )
  })

  it('warns (amber) when fewer than 7 days of observed data, but keeps the switch usable', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 2, ready: false, enabled: false })),
      ),
    )
    renderCard()

    expect(await screen.findByText(/not yet recommended/i)).toBeInTheDocument()
    expect(screen.getByText(/risky/i)).toBeInTheDocument()
    const sw = screen.getByRole('switch')
    expect(sw).toBeEnabled()
    expect(sw).not.toBeChecked()
  })

  it('shows "ready" when the deployment age meets the gauge', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 10, ready: true, enabled: false })),
      ),
    )
    renderCard()
    expect(await screen.findByText(/ready to enable/i)).toBeInTheDocument()
    expect(screen.getByText(/8\.5%/)).toBeInTheDocument()
  })

  it('enabling flows through a confirm dialog and PUTs {enabled:true} with confirm=true', async () => {
    let captured: { body: unknown; confirm: string | null } | null = null
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 10, ready: true, enabled: false })),
      ),
      http.put(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, async ({ request }) => {
        const url = new URL(request.url)
        captured = { body: await request.json(), confirm: url.searchParams.get('confirm') }
        return HttpResponse.json({ ok: true, enabled: true, l2_enabled_at: 1700000000 })
      }),
    )
    const user = userEvent.setup()
    renderCard()

    await screen.findByText(/ready to enable/i)
    await user.click(screen.getByRole('switch'))

    expect(await screen.findByText(/enable l2 enforcement \(live\)/i)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /^enable$/i }))

    await waitFor(() => expect(captured).not.toBeNull())
    expect(captured!.body).toEqual({ enabled: true })
    expect(captured!.confirm).toBe('true')
  })

  it('renders fade-in progress when L2 is enabled and mid-ramp', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(
          l2State({
            enabled: true,
            ready: true,
            deployment_age_days: 12,
            days_since_optin: 1,
            ramp_progress: 0.33,
            fully_ramped: false,
            warmup_days_remaining: 2,
            l2_enabled_at: 1700000000,
          }),
        ),
      ),
    )
    renderCard()
    expect(await screen.findByText(/l2 contributes to enforcement/i)).toBeInTheDocument()
    expect(screen.getByText(/fading in/i)).toBeInTheDocument()
    expect(screen.getByText(/33%/)).toBeInTheDocument()
    expect(screen.getByRole('switch')).toBeChecked()
  })
})
