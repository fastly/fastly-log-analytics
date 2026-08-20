/**
 * @vitest-environment jsdom
 *
 * RumStatusPanel — an analyst visiting /rum when RUM isn't enabled
 * previously saw the admin-oriented panel with "Enable RUM" / "Disable RUM"
 * buttons that 403 on click (backend blocks /rum/enable and /rum/disable
 * for the analyst role). This pins the fix: an analyst sees an
 * analyst-appropriate empty state instead, matching the "feature not
 * enabled for this service" pattern used elsewhere (StreamingClient.tsx),
 * while an admin still sees the full controls.
 */
import * as React from 'react'
import { describe, it, expect, afterEach, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient, makeQueryWrapper } from '../../helpers/query'
import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel'

const API_BASE = getApiBase()
const SVC = 'svc-rum-panel'

function renderPanel() {
  const qc = createTestQueryClient()
  return render(<RumStatusPanel />, { wrapper: makeQueryWrapper(qc) })
}

function mockRumStatus(enabled: boolean) {
  server.use(
    // RumStatusPanel fetches status/services via `adminFetch` with a plain
    // relative path, resolving against jsdom's own origin
    // (http://localhost:3000) — NOT the `NEXT_PUBLIC_API_URL` the test env
    // pins for the openapi-fetch `client` (http://127.0.0.1:8000, used by
    // RumFaroVersionCard's `/rum/versions` fetch below). Keep these two
    // unprefixed or the request bypasses MSW as a real network call.
    http.get('/api/services/:service_id/rum/status', () =>
      HttpResponse.json({ enabled, enabled_at: enabled ? '2026-01-01T00:00:00Z' : null }),
    ),
    http.get('/api/services', () =>
      HttpResponse.json({
        services: [{ service_id: SVC, name: 'Test Service', access_level: 'read_write' }],
      }),
    ),
  )
}

afterEach(() => {
  server.resetHandlers()
})

describe('RumStatusPanel', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false } as never)
  })

  it('analyst + RUM not enabled: shows the empty state, not Enable/Disable controls', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_only' }],
      isInitialized: true,
    } as never)
    mockRumStatus(false)

    renderPanel()

    expect(await screen.findByText(/rum not enabled/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /enable rum/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /disable rum/i })).not.toBeInTheDocument()
  })

  it('admin + RUM not enabled: still shows Enable/Disable controls', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    } as never)
    mockRumStatus(false)

    renderPanel()

    expect(await screen.findByRole('button', { name: /enable rum/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /disable rum/i })).toBeInTheDocument()
    expect(screen.queryByText(/rum not enabled/i)).not.toBeInTheDocument()
  })
})
