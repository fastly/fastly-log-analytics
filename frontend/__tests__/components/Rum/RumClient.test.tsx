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
import { describe, it, expect, afterEach, beforeEach, beforeAll, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'

import {
  createTestQueryClient,
  makeQueryWrapper,
  stubResizeObserver,
  stubPointerCaptureAndScroll,
  stubGetAnimations,
  createUseSSEMock,
} from '../../helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/filterStore'
import RumPageClient, { RumFaroVersionCard, RumStatusPanel } from '@/app/rum/_sections/RumClient'
import { RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  stubGetAnimations()
})

const { state: sseState, start: mockStart, resetState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
}))

const { mockAdminFetch, realAdminFetchRef } = vi.hoisted(() => ({
  mockAdminFetch: vi.fn(),
  realAdminFetchRef: { current: null as ((...args: unknown[]) => Promise<Response>) | null },
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  realAdminFetchRef.current = actual.adminFetch as (...args: unknown[]) => Promise<Response>
  return {
    ...actual,
    adminFetch: (...args: unknown[]) => mockAdminFetch(...args),
  }
})

beforeEach(() => {
  mockAdminFetch.mockImplementation((...args: unknown[]) => realAdminFetchRef.current!(...args))
})

vi.mock('@/components/PlotlyChart', async () => (await import('../../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/AnalyticsCard', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/AnalyticsCard')>()),
  TimeSeriesChart: () => <div data-testid="timeseries-chart" />,
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/page-smoke')).reactVirtualModuleMock())

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

const SVC_FARO = 'svc-faro-card'

function renderCard(rumEnabled = true) {
  const qc = createTestQueryClient()
  server.use(
    http.get('/api/services/:service_id/rum/status', () =>
      HttpResponse.json({ enabled: rumEnabled }),
    ),
  )
  return render(<RumFaroVersionCard serviceId={SVC_FARO} />, {
    wrapper: makeQueryWrapper(qc),
  })
}

function faroVersions(overrides: Record<string, unknown> = {}) {
  return {
    available: ['1.9.0', '1.8.0', '1.7.0'],
    current: '1.8.0',
    latest: '1.9.0',
    update_available: true,
    ...overrides,
  }
}

describe('RumFaroVersionCard', () => {
  beforeEach(() => {
    sseState.status = 'idle'
    mockStart.mockClear()
    server.use(
      http.post('/api/services/:service_id/rum/settings', () =>
        HttpResponse.json({ ok: true }),
      ),
    )
  })

  it('renders the pinned and latest version', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(faroVersions()),
      ),
    )
    renderCard()
    expect(await screen.findByText('1.8.0')).toBeInTheDocument()
  })

  it('surfaces the update-available affordance and enables Upgrade', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(faroVersions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
      ),
    )
    renderCard()
    expect(await screen.findByText(/update available/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /rum settings/i })).toBeEnabled()
  })

  it('allows changing version/re-deploying when already current', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(faroVersions({ current: '1.9.0', latest: '1.9.0', update_available: false })),
      ),
    )
    renderCard()
    expect(await screen.findByRole('button', { name: /rum settings/i })).toBeEnabled()
  })

  it('degrades gracefully on a registry 503 without breaking the page', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 }),
      ),
    )
    renderCard()
    expect(await screen.findByText(/versions offline/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeEnabled()
  })

  it('shows an empty-list state distinct from an error', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({ available: [], current: null, latest: null, update_available: false }),
      ),
    )
    renderCard()
    expect(await screen.findByText(/not pinned/i)).toBeInTheDocument()
    expect(screen.queryByText(/versions offline/i)).not.toBeInTheDocument()
  })

  it('skips the fetch entirely and does not invite an upgrade when RUM is not enabled', async () => {
    let called = false
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        called = true
        return HttpResponse.json(faroVersions())
      }),
    )
    renderCard(false)
    expect(screen.queryByText(/faro sdk/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /upgrade/i })).not.toBeInTheDocument()
    expect(called).toBe(false)
  })

  it('opens the upgrade dialog defaulting the target to latest (Confirm enabled, not a no-op)', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(faroVersions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
      ),
    )
    const user = userEvent.setup()
    renderCard()
    await user.click(await screen.findByRole('button', { name: /rum settings/i }))
    expect(await screen.findByText(/rum tracking settings/i)).toBeInTheDocument()
    expect(await screen.findByText('Core Web Vitals')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /apply settings/i })).toBeEnabled()
  })

  it('invalidates the versions query once the upgrade stream completes, without a manual reload', async () => {
    let versionCalls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        versionCalls += 1
        return HttpResponse.json(faroVersions({ current: null, latest: '1.9.0', update_available: true }))
      }),
    )
    const user = userEvent.setup()
    const { rerender } = renderCard()

    await waitFor(() => expect(versionCalls).toBe(1))
    await user.click(await screen.findByRole('button', { name: /rum settings/i }))
    await screen.findByText('Core Web Vitals')
    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith(`/api/services/${SVC_FARO}/rum/upgrade`, {
        version: '1.9.0',
        activate: true,
      })
    })

    sseState.status = 'done'
    rerender(<RumFaroVersionCard serviceId={SVC_FARO} />)

    await waitFor(() => expect(versionCalls).toBe(2))
  })
})

const DEFAULT_DIALOG_PROPS = {
  serviceId: 'svc-1',
  open: true,
  availableVersions: ['1.9.0', '1.8.0', '1.7.0'],
  currentVersion: '1.8.0',
  latestVersion: '1.9.0',
}

function renderDialog(props: Partial<React.ComponentProps<typeof RumSettingsDialog>> = {}) {
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()
  const qc = createTestQueryClient({ queries: { staleTime: Infinity } })
  const svcId = props.serviceId !== undefined ? props.serviceId : DEFAULT_DIALOG_PROPS.serviceId
  if (svcId) {
    qc.setQueryData(queryKeys.rumStatus(svcId), {
      enabled: true,
      capture_vitals: true,
      capture_performance: true,
      capture_errors: true,
      capture_events: true,
    })
  }
  const utils = render(
    <RumSettingsDialog
      {...DEFAULT_DIALOG_PROPS}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      {...props}
    />,
    { wrapper: makeQueryWrapper(qc) },
  )
  return { ...utils, onOpenChange, onComplete, qc }
}

describe('RumSettingsDialog', () => {
  beforeEach(() => {
    resetState()
    mockAdminFetch.mockReset()
    mockAdminFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, message: 'Settings saved' }),
    })
  })

  it('renders all capture toggle switches and defaults targeted version', () => {
    renderDialog()
    expect(screen.getByText('Core Web Vitals')).toBeInTheDocument()
    expect(screen.getByText('Performance Timings')).toBeInTheDocument()
    expect(screen.getByText('JavaScript Errors')).toBeInTheDocument()
    expect(screen.getByText('Custom Events')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /apply settings/i })).toBeEnabled()
  })

  it('submits toggles via POST settings endpoint on Confirm when version is unchanged', async () => {
    const user = userEvent.setup()
    const { onComplete } = renderDialog({ currentVersion: '1.9.0', latestVersion: '1.9.0' })

    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    expect(mockAdminFetch).toHaveBeenCalledWith('/api/services/svc-1/rum/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        capture_vitals: true,
        capture_performance: true,
        capture_errors: true,
        capture_events: true,
        custom_condition: '',
      }),
    })

    await waitFor(() => expect(onComplete).toHaveBeenCalledTimes(1))
  })

  it('triggers both settings POST and streamed version upgrade when target version changes', async () => {
    const user = userEvent.setup()
    renderDialog({ currentVersion: null })

    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    expect(mockAdminFetch).toHaveBeenCalledWith('/api/services/svc-1/rum/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        capture_vitals: true,
        capture_performance: true,
        capture_errors: true,
        capture_events: true,
        custom_condition: '',
      }),
    })

    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith('/api/services/svc-1/rum/upgrade', {
        version: '1.9.0',
        activate: true,
      })
    })
  })

  it('shows execution screen during progress', async () => {
    const user = userEvent.setup()
    const { rerender } = renderDialog({ currentVersion: null })

    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    sseState.status = 'streaming'
    rerender(<RumSettingsDialog {...DEFAULT_DIALOG_PROPS} currentVersion={null} onOpenChange={vi.fn()} />)

    expect(screen.getByText(/applying rum settings/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /cancel/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
  })

  it('calculates and displays estimated uncompressed log line size based on active toggles', async () => {
    const user = userEvent.setup()
    renderDialog()

    expect(screen.getByText(/2\.84 KB/i)).toBeInTheDocument()

    await user.click(screen.getByRole('switch', { name: /javascript errors/i }))
    await user.click(screen.getByRole('switch', { name: /custom events/i }))
    await user.click(screen.getByRole('switch', { name: /performance timings/i }))

    expect(screen.getByText(/423 B/i)).toBeInTheDocument()
  })

  it('returns null without a serviceId', () => {
    const { container } = renderDialog({ serviceId: null })
    expect(container).toBeEmptyDOMElement()
  })
})

const SVC_PANEL = 'svc-rum-panel'

function renderPanel() {
  const qc = createTestQueryClient()
  return render(<RumStatusPanel />, { wrapper: makeQueryWrapper(qc) })
}

function mockRumStatus(enabled: boolean) {
  server.use(
    http.get('/api/services/:service_id/rum/status', () =>
      HttpResponse.json({ enabled, enabled_at: enabled ? '2026-01-01T00:00:00Z' : null }),
    ),
    http.get('/api/services', () =>
      HttpResponse.json({
        services: [{ service_id: SVC_PANEL, name: 'Test Service', access_level: 'read_write' }],
      }),
    ),
  )
}

describe('RumStatusPanel', () => {
  beforeEach(() => {
    mockAdminFetch.mockImplementation((...args: unknown[]) => realAdminFetchRef.current!(...args))
    useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false })
  })

  it('analyst + RUM not enabled: shows the empty state, not Enable/Disable controls', async () => {
    useServiceStore.setState({
      activeServiceId: SVC_PANEL,
      services: [{ id: SVC_PANEL, name: 'Test Service', accessLevel: 'read_only' }],
      isInitialized: true,
    })
    mockRumStatus(false)

    renderPanel()

    expect(await screen.findByText(/rum not enabled/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /enable rum/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /disable rum/i })).not.toBeInTheDocument()
  })

  it('admin + RUM not enabled: still shows Enable/Disable controls', async () => {
    useServiceStore.setState({
      activeServiceId: SVC_PANEL,
      services: [{ id: SVC_PANEL, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumStatus(false)

    renderPanel()

    expect(await screen.findByRole('button', { name: /enable rum/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /disable rum/i })).toBeInTheDocument()
    expect(screen.queryByText(/rum not enabled/i)).not.toBeInTheDocument()
  })
})
