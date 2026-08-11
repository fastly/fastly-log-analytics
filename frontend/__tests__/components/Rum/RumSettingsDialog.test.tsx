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

  it('returns null without a serviceId', () => {
    const { container } = renderDialog({ serviceId: null })
    expect(container).toBeEmptyDOMElement()
  })
})
