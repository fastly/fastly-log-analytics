/**
 * L2EnforcementCard contract.
 *
 * The card gates whether edge Layer-2 joins the *enforced* score. Coverage:
 *   1. Readiness gauge < 7 days → amber "not yet recommended / risky" warning,
 *      but the Switch is still usable (SOFT gate).
 *   2. Enabling flows through a ConfirmDialog and PUTs {enabled:true} with
 *      ?confirm=true.
 *   3. When already enabled + mid-ramp, the card shows fade-in progress.
 */
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient } from '../../helpers/query'
import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { L2EnforcementCard } from '@/components/SessionScoring/L2EnforcementCard'

// Radix AlertDialog (under ConfirmDialog) needs these jsdom shims.
beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
  window.HTMLElement.prototype.hasPointerCapture = vi.fn() as never
  window.HTMLElement.prototype.releasePointerCapture = vi.fn() as never
  window.HTMLElement.prototype.scrollIntoView = vi.fn() as never
})

const API_BASE = getApiBase()
const SVC = 'svc-l2'

function renderCard() {
  const qc = createTestQueryClient()
  return render(
    <QueryClientProvider client={qc}>
      <L2EnforcementCard serviceId={SVC} sinceHours={24} />
    </QueryClientProvider>,
  )
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

beforeEach(() => {
  // The api client middleware aborts requests when no active service is set.
  useServiceStore.setState({ activeServiceId: SVC, isInitialized: true } as never)
  // Health default (carries l2_high_pct the card displays).
  server.use(
    http.get(`${API_BASE}/api/services/:service_id/scoring/health`, () =>
      HttpResponse.json({ matrix_staleness: { l2_high_pct: 8.5, l2_evaluated: 1000 } }),
    ),
  )
})

describe('L2EnforcementCard', () => {
  it('warns (amber) when fewer than 7 days of observed data, but keeps the switch usable', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
        HttpResponse.json(l2State({ deployment_age_days: 2, ready: false, enabled: false })),
      ),
    )
    renderCard()

    expect(await screen.findByText(/not yet recommended/i)).toBeInTheDocument()
    expect(screen.getByText(/risky/i)).toBeInTheDocument()
    // SOFT gate: the switch is present + enabled (off, but operable).
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
    // l2_high_pct from the health query is surfaced.
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

    // Confirm dialog appears (LIVE).
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
    // Switch reflects the on-state.
    expect(screen.getByRole('switch')).toBeChecked()
  })
})
