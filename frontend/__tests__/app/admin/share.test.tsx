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
import ShareDashboardPage from '@/app/admin/share/page'

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
