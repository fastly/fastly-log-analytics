/**
 * @vitest-environment jsdom
 *
 * RumClient — the operator asked "if there is a new version we should let
 * the user know on the RUM page and ask them if they want to upgrade", but
 * RumFaroVersionCard previously only mounted inside RumStatusPanel, which
 * RumClient only rendered for the *disabled* RUM case. An admin looking at
 * the normal (enabled) /rum vitals dashboard never saw it. This pins:
 *   - the version card now renders on /rum for an admin when RUM is enabled
 *   - it does NOT render for an analyst, and the /rum/versions query never
 *     fires for an analyst (would 403 — analyst-blocked in middleware)
 *   - the vitals dashboard still renders normally alongside the card
 *   - a 503 from /rum/versions degrades quietly, the dashboard stays usable
 */
import * as React from 'react'
import { describe, it, expect, afterEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient, makeQueryWrapper } from '../../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import RumPageClient from '@/app/rum/_sections/RumClient'

vi.mock('@/components/PlotlyChart', async () => (await import('../../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries-chart" /> }))

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/browser-stubs')).reactVirtualModuleMock())

const API_BASE = getApiBase()
const SVC = 'svc-rum-client'

function analytics(overrides: Record<string, unknown> = {}) {
  return {
    no_data: false,
    is_mock: false,
    beacon_count: 42,
    pageview_count: 30,
    interaction_count: 10,
    error_count: 2,
    vitals: {
      lcp: { p75: 2.1, distribution: { good: 80, needs_improvement: 15, poor: 5 } },
      cls: { p75: 0.05, distribution: { good: 90, needs_improvement: 8, poor: 2 } },
      inp: { p75: 150, distribution: { good: 85, needs_improvement: 10, poor: 5 } },
    },
    trends: { timestamps: [], lcp: [], cls: [], error_rate: [], pageviews: [], interactions: [], errors: [] },
    worst_pages: [],
    errors: [],
    environments: { browsers: {}, os: {}, devices: {} },
    ...overrides,
  }
}

function versions(overrides: Record<string, unknown> = {}) {
  return {
    available: ['1.9.0', '1.8.0'],
    current: '1.8.0',
    latest: '1.9.0',
    update_available: true,
    ...overrides,
  }
}

function mockRumEndpoints({
  versionsHandler,
  analyticsOverrides,
}: { versionsHandler?: () => Response | Promise<Response>; analyticsOverrides?: Record<string, unknown> } = {}) {
  server.use(
    // RumClient fetches status/analytics/live-events via `adminFetch` with a
    // plain relative path, so the request resolves against jsdom's own
    // origin (http://localhost:3000), NOT the `NEXT_PUBLIC_API_URL` the test
    // env pins for the openapi-fetch `client` (http://127.0.0.1:8000) —
    // these three must stay unprefixed or the request bypasses MSW as a
    // real (never-resolving) network call.
    http.get('/api/services/:service_id/rum/status', () =>
      HttpResponse.json({ enabled: true, enabled_at: '2026-01-01T00:00:00Z' }),
    ),
    http.get('/api/services/:service_id/rum/analytics', () => HttpResponse.json(analytics(analyticsOverrides))),
    http.get('/api/services/:service_id/rum/live-events', () => HttpResponse.json([])),
    // RumFaroVersionCard fetches via the openapi-fetch `client`, whose
    // baseUrl IS `NEXT_PUBLIC_API_URL` — this one needs the prefix.
    http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
      versionsHandler ? versionsHandler() : HttpResponse.json(versions()),
    ),
  )
}

function renderClient() {
  const qc = createTestQueryClient()
  return render(
    <RumPageClient initialServiceId={SVC} />,
    { wrapper: makeQueryWrapper(qc) },
  )
}

afterEach(() => {
  server.resetHandlers()
})

describe('RumClient — Faro version card placement', () => {
  it('admin: renders the Faro version card alongside the vitals dashboard when RUM is enabled', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumEndpoints()

    renderClient()

    // Vitals dashboard content renders.
    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
    // Version card renders too, with real data (not a stub).
    expect(await screen.findByText(/faro sdk:/i)).toBeInTheDocument()
    expect(await screen.findByText('1.8.0')).toBeInTheDocument()
    expect(screen.getByText('RUM Settings')).toBeInTheDocument()
    // Installation instructions button is rendered for the admin.
    expect(screen.getByText('Installation Instructions')).toBeInTheDocument()
  })

  it('analyst: does not render the version card, and never calls /rum/versions', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_only' }],
      isInitialized: true,
    })
    let versionsCalled = false
    mockRumEndpoints({
      versionsHandler: () => {
        versionsCalled = true
        return HttpResponse.json(versions())
      },
    })

    renderClient()

    // Dashboard still renders for the analyst.
    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
    expect(screen.queryByText(/faro sdk:/i)).not.toBeInTheDocument()
    // Installation instructions button is NOT rendered for the analyst.
    expect(screen.queryByText('Installation Instructions')).not.toBeInTheDocument()

    // Give any stray query a moment to fire, then assert it never did.
    await new Promise((r) => setTimeout(r, 50))
    expect(versionsCalled).toBe(false)
  })

  it('a 503 from /rum/versions leaves the dashboard usable', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumEndpoints({
      versionsHandler: () => HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 }),
    })

    renderClient()

    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
    await waitFor(() => {
      expect(screen.getByText(/versions offline/i)).toBeInTheDocument()
    })
    // The rest of the dashboard is unaffected by the version-card error.
    expect(screen.getByText('Cumulative Layout Shift (CLS)')).toBeInTheDocument()
    expect(screen.getByText('Interaction to Next Paint (INP)')).toBeInTheDocument()
  })
})

describe('RumClient — worst pages table', () => {
  it('renders worst pages through the shared table with formatted vitals', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumEndpoints({
      analyticsOverrides: {
        worst_pages: [
          { path: '/checkout', views: 12, avg_load_time: 3.456, lcp_p75: 4.2, cls_p75: 0.123 },
          { path: '/home', views: 3, avg_load_time: null, lcp_p75: null, cls_p75: null },
        ],
      },
    })

    renderClient()

    expect(await screen.findByText('/checkout')).toBeInTheDocument()
    for (const name of ['Path', 'Pageviews', 'Avg Load (s)', 'LCP p75', 'CLS p75']) {
      expect(screen.getByRole('columnheader', { name })).toBeInTheDocument()
    }
    expect(screen.getByText('3.5s')).toBeInTheDocument()
    expect(screen.getByText('4.20s')).toBeInTheDocument()
    expect(screen.getByText('0.12')).toBeInTheDocument()
    expect(screen.getByText('/home')).toBeInTheDocument()
  })
})

describe('RumClient — range wire', () => {
  it('sends range_token+anchor on the default window and keys the query [rum-analytics, svc, token, anchor, filters]', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumEndpoints()
    let seen: URLSearchParams | null = null
    server.use(
      http.get('/api/services/:service_id/rum/analytics', ({ request }) => {
        seen = new URL(request.url).searchParams
        return HttpResponse.json(analytics())
      }),
    )

    const qc = createTestQueryClient()
    render(<RumPageClient initialServiceId={SVC} />, { wrapper: makeQueryWrapper(qc) })
    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()

    const params = seen as URLSearchParams | null
    expect(params?.get('range_token')).toBe('24h')
    expect(params?.get('anchor')).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$/)

    const key = qc
      .getQueryCache()
      .getAll()
      .map((q) => q.queryKey as unknown[])
      .find((k) => k[0] === 'rum-analytics')
    expect(key).toEqual(['rum-analytics', SVC, '24h', params?.get('anchor'), {}])
  })
})
