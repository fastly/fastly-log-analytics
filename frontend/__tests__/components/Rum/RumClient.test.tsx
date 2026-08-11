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
import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { RumClient } from '@/app/rum/_sections/RumClient'

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plotly-chart" /> }))

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
    trends: { timestamps: [], lcp: [], cls: [], error_rate: [] },
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

function mockRumEndpoints({ versionsHandler }: { versionsHandler?: () => Response | Promise<Response> } = {}) {
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
    http.get('/api/services/:service_id/rum/analytics', () => HttpResponse.json(analytics())),
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
    <RumClient serviceId={SVC} startTime={null} endTime={null} filterPayload={{}} />,
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
    } as never)
    mockRumEndpoints()

    renderClient()

    // Vitals dashboard content renders.
    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
    // Version card renders too, with real data (not a stub).
    expect(await screen.findByText('Faro SDK Version')).toBeInTheDocument()
    expect(await screen.findByText('1.8.0')).toBeInTheDocument()
    expect(screen.getByText('1.9.0')).toBeInTheDocument()
  })

  it('analyst: does not render the version card, and never calls /rum/versions', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_only' }],
      isInitialized: true,
    } as never)
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
    expect(screen.queryByText('Faro SDK Version')).not.toBeInTheDocument()

    // Give any stray query a moment to fire, then assert it never did.
    await new Promise((r) => setTimeout(r, 50))
    expect(versionsCalled).toBe(false)
  })

  it('a 503 from /rum/versions leaves the dashboard usable', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    } as never)
    mockRumEndpoints({
      versionsHandler: () => HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 }),
    })

    renderClient()

    expect(await screen.findByText('Largest Contentful Paint (LCP)')).toBeInTheDocument()
    await waitFor(() => {
      expect(screen.getByText(/couldn.t reach the npm registry/i)).toBeInTheDocument()
    })
    // The rest of the dashboard is unaffected by the version-card error.
    expect(screen.getByText('Cumulative Layout Shift (CLS)')).toBeInTheDocument()
    expect(screen.getByText('Interaction to Next Paint (INP)')).toBeInTheDocument()
  })
})
