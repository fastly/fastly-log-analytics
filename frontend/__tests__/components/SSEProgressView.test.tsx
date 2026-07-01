/**
 * SSEProgressView renders the live SSE feed for SSEModal flows. This spec
 * covers the actionable-error-link path: when a terminal `{type:'error'}` event
 * carries a `link` (e.g. a manage.fastly.com deep link emitted when a required
 * Fastly product isn't enabled for session scoring), the error block renders a
 * clickable "Open Fastly to enable it" anchor pointing at that link. A plain
 * error (no link) renders the message with no anchor.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import React from 'react'

import { SSEProgressView } from '@/components/SSEModal/SSEProgressView'
import type { SSELine } from '@/hooks/useSSE'

// ScrollArea is a portal-y shadcn wrapper; a passthrough keeps jsdom simple.
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

describe('SSEProgressView error link', () => {
  it('renders a clickable Fastly link when the error event carries one', () => {
    const link = 'https://manage.fastly.com/products/compute'
    const lines: SSELine[] = [
      { type: 'status', message: 'Enabling…', _id: 1 },
      { type: 'error', message: 'Compute isn’t enabled', link, _id: 2 },
    ]
    render(
      <SSEProgressView lines={lines} status="error" error="Compute isn’t enabled" />,
    )

    const anchor = screen.getByRole('link', { name: /open fastly to enable it/i })
    expect(anchor).toHaveAttribute('href', link)
    expect(anchor).toHaveAttribute('target', '_blank')
    // The human-readable error message is still shown.
    expect(screen.getByText(/Compute isn’t enabled/)).toBeInTheDocument()
  })

  it('renders no link for a plain error event', () => {
    const lines: SSELine[] = [{ type: 'error', message: 'Something broke', _id: 1 }]
    render(<SSEProgressView lines={lines} status="error" error="Something broke" />)

    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByText(/Something broke/)).toBeInTheDocument()
  })
})
