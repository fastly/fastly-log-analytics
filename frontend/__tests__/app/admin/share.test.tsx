/**
 * Unit & integration smoke suite for `/admin/share`.
 *
 * Validates:
 *   1. Page mounts without unhandled requests and fires `/api/admin/share/status`
 *      and `/api/admin/share/live`.
 *   2. Zero-CLS reservation containers (`admin-share-control-reservation`,
 *      `admin-share-panel-reservation`) carry `[contain-intrinsic-size:auto_*]`
 *      and `[content-visibility:auto]`.
 *   3. SharingControlPanel (`admin-share-control-panel`, `admin-share-status-badge`),
 *      InvitationsPanel (`admin-share-invites-panel`), tab triggers
 *      (`Invitations`, `Sessions`, `Audit`), and manual `Refresh`
 *      (`admin-share-refresh-btn`) are wired and interactive.
 *   4. URL `?tab=` query param synchronization (`invites`, `sessions`, `audit`)
 *      and status error banner (`admin-share-status-error`).
 */
import { describe, expect, test, vi, beforeEach } from 'vitest'
import { fireEvent, screen, waitFor } from '@testing-library/react'
import { renderWithQuery } from '../../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import React from 'react'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'

import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import ShareDashboardPage from '@/app/admin/share/page'
import SessionScoringPage from '@/app/admin/session-scoring/page'

let mockSearchParamsString = ''

vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: vi.fn(),
    replace: vi.fn(),
    prefetch: vi.fn(),
    back: vi.fn(),
  }),
  usePathname: () => '/admin/share',
  useSearchParams: () => new URLSearchParams(mockSearchParamsString),
  useParams: () => ({}),
}))

vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: () => () => null,
}))

vi.mock('@/components/SessionScoring/RocPrCurves', () => ({
  RocPrCurves: () => null,
  PerReasonAucCard: () => null,
}))
vi.mock('@/components/SessionScoring/ScorerSeriesCard', () => ({
  ScoreDistChart: () => null,
  ComplianceChart: () => null,
  StackedHourlyBarChart: () => null,
  ScorerLatencyChart: () => null,
  ScorerErrorsChart: () => null,
  ScorerSeriesCard: () => null,
}))
vi.mock('@/components/SessionScoring/ScoringHealthCard', () => ({
  ScoringHealthCard: () => null,
  ScorerFailOpenBreakdownCard: () => null,
}))
vi.mock('@/components/SessionScoring/StatusPanel', () => ({
  StatusPanel: ({ serviceId }: { serviceId: string }) => (
    <div data-testid="admin-session-scoring-status-card">Status for {serviceId}</div>
  ),
  L2EnforcementCard: () => null,
}))
vi.mock('@/components/SessionScoring/ThresholdSlider', () => ({ ThresholdSlider: () => null }))
vi.mock('@/components/SessionScoring/TopFlaggedTable', () => ({ TopFlaggedTable: () => null }))
vi.mock('@/components/SessionScoring/MatrixVersionsCard', () => ({
  MatrixVersionsCard: () => null,
  AuditLogTab: () => null,
}))
vi.mock('@/components/SessionScoring/ExcludeRegexCard', () => ({ ExcludeRegexCard: () => null }))
vi.mock('@/components/SessionScoring/RetrainButton', () => ({
  RetrainButton: () => null,
  RotateKeyButton: () => null,
}))

const API_BASE = getApiBase()

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

beforeEach(() => {
  mockSearchParamsString = ''
})

describe('admin/share page', () => {
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
