/**
 * @vitest-environment jsdom
 *
 * UpgradeFaroDialog (Task 8) — version-selection + SSE-streamed upgrade,
 * consuming the same run_with_events machinery as EnableRumDialog /
 * DisableRumDialog. useSSE is mocked at the module boundary (matching
 * TeardownDialog.test.tsx / DeleteDataDialog.test.tsx) so status
 * transitions can be driven deterministically across rerenders.
 */
import * as React from 'react'
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { UpgradeFaroDialog } from '@/components/Rum/UpgradeFaroDialog'

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
  window.HTMLElement.prototype.hasPointerCapture = vi.fn() as never
  window.HTMLElement.prototype.releasePointerCapture = vi.fn() as never
  window.HTMLElement.prototype.scrollIntoView = vi.fn() as never
  // jsdom doesn't implement the Web Animations API; base-ui's ScrollArea
  // (under SSEProgressView, rendered once the dialog switches to the
  // streaming view) probes it on an internal auto-hide timer.
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

const DEFAULT_PROPS = {
  serviceId: 'svc-1',
  open: true,
  availableVersions: ['1.9.0', '1.8.0', '1.7.0'],
  currentVersion: '1.8.0',
  latestVersion: '1.9.0',
}

function renderDialog(props: Partial<React.ComponentProps<typeof UpgradeFaroDialog>> = {}) {
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()
  const utils = render(
    <UpgradeFaroDialog
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
})

describe('UpgradeFaroDialog', () => {
  it('defaults the target version to latest and enables Confirm', () => {
    renderDialog()
    expect(screen.getByRole('button', { name: /confirm & upgrade/i })).toBeEnabled()
  })

  it('shows the pinned version as the "from" side of the preview', () => {
    renderDialog()
    expect(screen.getByText('1.8.0')).toBeInTheDocument()
  })

  it('lets the operator choose a different target version', async () => {
    const user = userEvent.setup()
    renderDialog()
    await user.click(screen.getByRole('combobox', { name: /target faro web sdk version/i }))
    await user.click(await screen.findByRole('option', { name: /1\.7\.0/ }))
    await user.click(screen.getByRole('button', { name: /confirm & upgrade/i }))
    expect(mockStart).toHaveBeenCalledWith('/api/services/svc-1/rum/upgrade', {
      version: '1.7.0',
      activate: true,
    })
  })

  it('disables Confirm when the picked version is a no-op (matches the pinned version)', async () => {
    const user = userEvent.setup()
    renderDialog()
    await user.click(screen.getByRole('combobox', { name: /target faro web sdk version/i }))
    await user.click(await screen.findByRole('option', { name: /1\.8\.0/ }))
    expect(screen.getByRole('button', { name: /confirm & upgrade/i })).toBeDisabled()
  })

  it('starts the SSE stream with the default (latest) version on confirm', async () => {
    const user = userEvent.setup()
    renderDialog()
    await user.click(screen.getByRole('button', { name: /confirm & upgrade/i }))
    expect(mockStart).toHaveBeenCalledWith('/api/services/svc-1/rum/upgrade', {
      version: '1.9.0',
      activate: true,
    })
  })

  it('shows streaming progress and offers only Stop (no second launch while streaming)', async () => {
    const user = userEvent.setup()
    const { rerender } = renderDialog()
    await user.click(screen.getByRole('button', { name: /confirm & upgrade/i }))
    sseState.status = 'streaming'
    rerender(<UpgradeFaroDialog {...DEFAULT_PROPS} onOpenChange={vi.fn()} />)

    expect(screen.getByText(/processing/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /cancel/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /confirm & upgrade/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
  })

  it('blocks closing (Escape) while streaming', async () => {
    sseState.status = 'streaming'
    const user = userEvent.setup()
    const { onOpenChange } = renderDialog()
    await user.keyboard('{Escape}')
    expect(onOpenChange).not.toHaveBeenCalled()
  })

  it('surfaces the stream error and does not report success', async () => {
    const user = userEvent.setup()
    const { rerender, onComplete } = renderDialog()
    await user.click(screen.getByRole('button', { name: /confirm & upgrade/i }))
    sseState.status = 'error'
    sseState.error = 'unknown_faro_version: v9.9.9'
    rerender(<UpgradeFaroDialog {...DEFAULT_PROPS} onOpenChange={vi.fn()} onComplete={onComplete} />)

    expect(await screen.findByText(/unknown_faro_version/i)).toBeInTheDocument()
    // Both the footer "Close" button and the dialog's built-in X (sr-only
    // label "Close") match this name — the dialog isn't silently closing,
    // both affordances to dismiss it are present.
    expect(screen.getAllByRole('button', { name: /^close$/i }).length).toBeGreaterThan(0)
    expect(onComplete).not.toHaveBeenCalled()
  })

  it('invokes onComplete exactly once when the stream reports done', async () => {
    const user = userEvent.setup()
    const { rerender, onComplete } = renderDialog()
    await user.click(screen.getByRole('button', { name: /confirm & upgrade/i }))
    sseState.status = 'done'
    rerender(<UpgradeFaroDialog {...DEFAULT_PROPS} onOpenChange={vi.fn()} onComplete={onComplete} />)

    await waitFor(() => expect(onComplete).toHaveBeenCalledTimes(1))

    // A further re-render at the same 'done' status must not re-fire it.
    rerender(<UpgradeFaroDialog {...DEFAULT_PROPS} onOpenChange={vi.fn()} onComplete={onComplete} />)
    expect(onComplete).toHaveBeenCalledTimes(1)
  })

  it('returns null without a serviceId', () => {
    const { container } = renderDialog({ serviceId: null })
    expect(container).toBeEmptyDOMElement()
  })
})
