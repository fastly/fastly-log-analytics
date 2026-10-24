/**
 * @vitest-environment jsdom
 *
 * Consolidated RumSettingsDialog and RumStatusPanel tests.
 */
import {
  stubResizeObserver,
  stubPointerCaptureAndScroll,
  stubGetAnimations,
} from '@/__tests__/helpers/browser-stubs'
import { createUseSSEMock } from '../../helpers/sse-mock'
import * as React from 'react'
import { describe, it, expect, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { createTestQueryClient, makeQueryWrapper } from '../../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { useServiceStore } from '@/stores/serviceStore'
import { queryKeys } from '@/lib/query-keys'
import { RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  stubGetAnimations()
})

const { state: sseState, start: mockStart, resetState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', () => ({ useSSE: () => sseState }))

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

const DEFAULT_PROPS = {
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
  const svcId = props.serviceId !== undefined ? props.serviceId : DEFAULT_PROPS.serviceId
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
      {...DEFAULT_PROPS}
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
    rerender(<RumSettingsDialog {...DEFAULT_PROPS} currentVersion={null} onOpenChange={vi.fn()} />)

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

const SVC = 'svc-rum-panel'

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
        services: [{ service_id: SVC, name: 'Test Service', access_level: 'read_write' }],
      }),
    ),
  )
}

describe('RumStatusPanel', () => {
  beforeEach(() => {
    mockAdminFetch.mockImplementation((...args: unknown[]) => realAdminFetchRef.current!(...args))
    useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false })
  })

  afterEach(() => {
    server.resetHandlers()
  })

  it('analyst + RUM not enabled: shows the empty state, not Enable/Disable controls', async () => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_only' }],
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
      activeServiceId: SVC,
      services: [{ id: SVC, name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
    mockRumStatus(false)

    renderPanel()

    expect(await screen.findByRole('button', { name: /enable rum/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /disable rum/i })).toBeInTheDocument()
    expect(screen.queryByText(/rum not enabled/i)).not.toBeInTheDocument()
  })
})
