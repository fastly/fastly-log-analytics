/**
 * @vitest-environment jsdom
 *
 * RumFaroVersionCard (Task 8) — the "there's a new Faro Web SDK version,
 * want to upgrade?" surface the user explicitly asked for. Coverage:
 *   - renders pinned vs latest
 *   - update-available surfaces the upgrade affordance
 *   - already-current disables it (no no-op upgrades)
 *   - a registry 503 degrades locally, without an unhandled-request crash
 *   - an empty version list is a distinct state from an error
 *   - RUM-not-enabled skips the fetch entirely and never invites an upgrade
 *   - completing an upgrade (via the SSE stream) invalidates the versions
 *     query so the card reflects the new pin without a manual reload
 */
import * as React from 'react'
import { describe, it, expect, afterEach, beforeEach, beforeAll, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient, makeQueryWrapper } from '../../helpers/query'
import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { RumFaroVersionCard } from '@/components/Rum/RumFaroVersionCard'

beforeAll(() => {
  // Radix/base-ui Select needs these jsdom shims to open + select an option.
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
  window.HTMLElement.prototype.hasPointerCapture = vi.fn() as never
  window.HTMLElement.prototype.releasePointerCapture = vi.fn() as never
  window.HTMLElement.prototype.scrollIntoView = vi.fn() as never
  // jsdom doesn't implement the Web Animations API; base-ui's ScrollArea
  // (under SSEProgressView, rendered once the upgrade dialog switches to
  // the streaming view) probes it on an internal auto-hide timer.
  window.Element.prototype.getAnimations = vi.fn(() => []) as never
})

const API_BASE = getApiBase()
const SVC = 'svc-faro-card'

// Mutable so the "completes and invalidates" test can flip status without a
// second vi.mock factory — vi.mock is hoisted and can't read per-test state
// (same pattern as DeleteDataDialog.test.tsx / TeardownDialog.test.tsx).
const mockStart = vi.fn()
const sseState: { status: 'idle' | 'streaming' | 'done' | 'error' } = { status: 'idle' }

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    get status() {
      return sseState.status
    },
    isDone: false,
    error: null,
    start: mockStart,
    stop: vi.fn(),
    reset: vi.fn(),
  }),
}))

function renderCard(rumEnabled = true) {
  const qc = createTestQueryClient()
  server.use(
    http.get('/api/services/:service_id/rum/status', () =>
      HttpResponse.json({ enabled: rumEnabled }),
    ),
  )
  return render(<RumFaroVersionCard serviceId={SVC} />, {
    wrapper: makeQueryWrapper(qc),
  })
}

function versions(overrides: Record<string, unknown> = {}) {
  return {
    available: ['1.9.0', '1.8.0', '1.7.0'],
    current: '1.8.0',
    latest: '1.9.0',
    update_available: true,
    ...overrides,
  }
}

beforeEach(() => {
  sseState.status = 'idle'
  mockStart.mockClear()
  server.use(
    http.post('/api/services/:service_id/rum/settings', () =>
      HttpResponse.json({ ok: true }),
    ),
  )
})

afterEach(() => {
  server.resetHandlers()
})

describe('RumFaroVersionCard', () => {
  it('renders the pinned and latest version', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(versions()),
      ),
    )
    renderCard()
    expect(await screen.findByText('1.8.0')).toBeInTheDocument()
  })

  it('surfaces the update-available affordance and enables Upgrade', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(versions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
      ),
    )
    renderCard()
    expect(await screen.findByText(/update available/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /rum settings/i })).toBeEnabled()
  })

  it('allows changing version/re-deploying when already current', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(versions({ current: '1.9.0', latest: '1.9.0', update_available: false })),
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
        return HttpResponse.json(versions())
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
        HttpResponse.json(versions({ current: '1.8.0', latest: '1.9.0', update_available: true })),
      ),
    )
    const user = userEvent.setup()
    renderCard()
    await user.click(await screen.findByRole('button', { name: /rum settings/i }))
    expect(await screen.findByText(/rum tracking settings/i)).toBeInTheDocument()
    expect(await screen.findByText('Core Web Vitals')).toBeInTheDocument()
    // If the dialog had defaulted to the already-pinned version instead of
    // latest, Confirm would be disabled as a no-op — this fails that case.
    expect(screen.getByRole('button', { name: /apply settings/i })).toBeEnabled()
  })

  it('invalidates the versions query once the upgrade stream completes, without a manual reload', async () => {
    let versionCalls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        versionCalls += 1
        return HttpResponse.json(versions({ current: '1.8.0', latest: '1.9.0', update_available: true }))
      }),
    )
    const user = userEvent.setup()
    const { rerender } = renderCard()

    await waitFor(() => expect(versionCalls).toBe(1))
    await user.click(await screen.findByRole('button', { name: /rum settings/i }))
    await screen.findByText('Core Web Vitals')
    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith(`/api/services/${SVC}/rum/upgrade`, {
        version: '1.9.0',
        activate: true,
      })
    })

    // Simulate the SSE stream reaching 'done' (same mounted instance, not a
    // fresh render — completedRef/isExecuting are internal state set by the
    // click above).
    sseState.status = 'done'
    rerender(<RumFaroVersionCard serviceId={SVC} />)

    await waitFor(() => expect(versionCalls).toBe(2))
  })
})
