/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import React from 'react'

vi.mock('@/hooks/useDateFormat', () => ({
  useDateFormat: () => ({
    full: (iso: string) => `full:${iso}`,
    abbr: () => 'UTC',
  }),
}))

// The trigger's relative text now comes from the shared live <TimeAgo>
// (subscribes to useNowMs + formatTimeAgo). Stub it to a deterministic
// string so assertions don't depend on wall-clock-relative output.
vi.mock('@/components/TimeAgo', () => ({
  TimeAgo: ({ timestamp }: { timestamp: string }) => `ago:${timestamp}`,
}))

import { DateTimeCell } from '@/components/DataTable/DateTimeCell'

afterEach(() => cleanup())

describe('DateTimeCell', () => {
  it('renders default em-dash fallback when iso is null', () => {
    const { container } = render(<DateTimeCell iso={null} />)
    const span = container.querySelector('span.text-muted-foreground\\/40')
    expect(span).not.toBeNull()
    expect(span?.textContent).toBe('—')
  })

  it('renders emptyFallback node when iso is null and fallback supplied', () => {
    render(<DateTimeCell iso={undefined} emptyFallback={<span>N/A</span>} />)
    expect(screen.getByText('N/A')).toBeInTheDocument()
    // Default em-dash should not render when a custom fallback is supplied
    expect(screen.queryByText('—')).toBeNull()
  })

  it('renders the live <TimeAgo> on the trigger when iso is provided', () => {
    render(<DateTimeCell iso="2026-06-15T12:00:00Z" />)
    // The trigger text comes from the stubbed shared <TimeAgo>
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
  })
})
