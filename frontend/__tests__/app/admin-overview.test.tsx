import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { withNuqsTestingAdapter } from 'nuqs/adapters/testing'
import { createTestQueryClient, renderWithQuery, renderWithQueryClient } from '../helpers/page-smoke'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
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
import { QuarantineSection } from '@/app/admin/_sections/QuarantineSection'
import TrendsLoading from '@/app/admin/trends/loading'
import { useServiceStore } from '@/stores/serviceStore'
import { getApiBase } from '@/lib/api'
import { server } from '@/__tests__/helpers/msw/handlers'

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
