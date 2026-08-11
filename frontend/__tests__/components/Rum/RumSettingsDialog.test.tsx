/**
 * @vitest-environment jsdom
 *
 * RumSettingsDialog — version-selection + RUM capture toggles,
 * and optional SSE-streamed upgrade on version change.
 */
import * as React from 'react'
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
  window.HTMLElement.prototype.hasPointerCapture = vi.fn() as never
  window.HTMLElement.prototype.releasePointerCapture = vi.fn() as never
  window.HTMLElement.prototype.scrollIntoView = vi.fn() as never
  window.Element.prototype.getAnimations = vi.fn(() => []) as never
})

const mockStart = vi.fn()
const mockStop = vi.fn()
const sseState: { status: 'idle' | 'streaming' | 'done' | 'error'; error: string | null } = {
  status: 'idle',
  error: null,
}

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    get status() {
      return sseState.status
    },
    get error() {
      return sseState.error
    },
    isDone: false,
    start: mockStart,
    stop: mockStop,
    reset: vi.fn(),
  }),
}))

const mockAdminFetch = vi.fn()

vi.mock('@/lib/api', () => ({
  adminFetch: (...args: any[]) => mockAdminFetch(...args),
}))

vi.mock('@tanstack/react-query', () => ({
  useQuery: () => ({
    data: {
      capture_vitals: true,
      capture_performance: true,
      capture_errors: true,
      capture_events: true,
    },
    isLoading: false,
  }),
  useQueryClient: () => ({
    invalidateQueries: vi.fn(),
  }),
}))

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
  const utils = render(
    <RumSettingsDialog
      {...DEFAULT_PROPS}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      {...props}
    />,
  )
  return { ...utils, onOpenChange, onComplete }
}

beforeEach(() => {
  sseState.status = 'idle'
  sseState.error = null
  mockStart.mockClear()
  mockStop.mockClear()
  mockAdminFetch.mockClear()
  mockAdminFetch.mockResolvedValue({
    ok: true,
    json: async () => ({ ok: true, message: 'Settings saved' }),
  })
})

describe('RumSettingsDialog', () => {
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

    // Should complete immediately without SSE version upgrade
    await waitFor(() => expect(onComplete).toHaveBeenCalledTimes(1))
  })

  it('triggers both settings POST and streamed version upgrade when target version changes', async () => {
    const user = userEvent.setup()
    renderDialog() // Pinned: 1.8.0, Target defaults to 1.9.0

    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    // First, setting toggles are updated
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

    // Then, the SSE version upgrade starts
    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith('/api/services/svc-1/rum/upgrade', {
        version: '1.9.0',
        activate: true,
      })
    })
  })

  it('shows execution screen during progress', async () => {
    const user = userEvent.setup()
    const { rerender } = renderDialog()

    await user.click(screen.getByRole('button', { name: /apply settings/i }))

    // Simulate SSE streaming state
    sseState.status = 'streaming'
    rerender(<RumSettingsDialog {...DEFAULT_PROPS} onOpenChange={vi.fn()} />)

    expect(screen.getByText(/applying rum settings/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /cancel/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
  })

  it('calculates and displays estimated uncompressed log line size based on active toggles', async () => {
    const user = userEvent.setup()
    renderDialog()

    // All toggles (vitals, perf, errors, events) default to true from react-query mock
    // Total fields: 17
    // Total typical bytes: 2820
    // Structural: 2 + 17*5 = 87
    // Total: 2820 + 87 = 2907 bytes (2.84 KB)
    expect(screen.getByText(/2\.84 KB/i)).toBeInTheDocument()

    // Toggle JavaScript Errors off
    await user.click(screen.getByRole('switch', { name: /javascript errors/i }))
    // Toggle Custom Events off
    await user.click(screen.getByRole('switch', { name: /custom events/i }))
    // Toggle Performance Timings off
    await user.click(screen.getByRole('switch', { name: /performance timings/i }))

    // Only Core Web Vitals remains:
    // Active fields for Vitals: rum_cid (12), fastly_req_id (12), rum_pathname (256), rum_connection_speed (10), rum_trace_id (32), rum_span_id (16), rum_metric_name (12), rum_metric_value (8), rum_metric_rating (18).
    // Total fields: 9.
    // Field bytes: 12 + 12 + 256 + 10 + 32 + 16 + 12 + 8 + 18 = 376.
    // Structural: 2 + 9 * 5 = 47.
    // Total: 376 + 47 = 423 B.
    expect(screen.getByText(/423 B/i)).toBeInTheDocument()
  })

  it('returns null without a serviceId', () => {
    const { container } = renderDialog({ serviceId: null })
    expect(container).toBeEmptyDOMElement()
  })
})
