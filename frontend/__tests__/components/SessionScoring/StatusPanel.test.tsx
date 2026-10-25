/**
 * StatusPanel contract for Session Scoring prerequisite pragmas.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubGetAnimations, stubPointerCaptureAndScroll } from '@/__tests__/helpers/browser-stubs'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'

import { renderWithQuery } from '../../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { StatusPanel } from '@/components/SessionScoring/StatusPanel'
import { AuditLogTab } from '@/components/SessionScoring/AuditLogTab'
import { LabelsTab } from '@/components/SessionScoring/LabelsTab'
import { MatrixVersionsCard } from '@/components/SessionScoring/MatrixVersionsCard'
import { L2EnforcementCard } from '@/components/SessionScoring/L2EnforcementCard'

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/virtual-mock')).reactVirtualModuleMock())

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  stubGetAnimations()
})

const API_BASE = getApiBase()
const SVC = 'svc-pragma-test'

function renderPanel() {
  return renderWithQuery(<StatusPanel serviceId={SVC} />)
}

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: SVC, isInitialized: true })
})

describe('StatusPanel preflight pragma gate', () => {
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
    expect(msgEl.textContent).toContain(SVC)
    expect(msgEl.textContent).toContain('Fastly team')
    // Must never leak internal pragma names or hctl CLI syntax
    expect(modal.textContent).not.toContain('fix_unsent_body_drain')
    expect(modal.textContent).not.toContain('no_body_if_bereq_is_get_or_head')
    expect(modal.textContent).not.toContain('hctl')

    // Flip readyState to true and click "Check Again" -> modal closes automatically
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
      queryClient.setQueryData(['scoring-status', SVC], { enabled: false, preflight_pragmas_ready: true })
    })
    await waitFor(() => expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument())
    act(() => {
      queryClient.setQueryData(['scoring-status', SVC], { enabled: false, preflight_pragmas_ready: false })
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
const renderWithClient = renderWithQuery

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
    renderWithClient(<AuditLogTab serviceId={SVC_TABLES} />)
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
    renderWithClient(<AuditLogTab serviceId={SVC_TABLES} />)
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
    renderWithClient(<MatrixVersionsCard serviceId={SVC_TABLES} />)
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
    renderWithClient(<LabelsTab serviceId={SVC_TABLES} />)
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
    renderWithClient(<LabelsTab serviceId={SVC_TABLES} />)
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
