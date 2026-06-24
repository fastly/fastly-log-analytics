/**
 * TimeAgo — the shared live "X ago" text node. Subscribes to the global
 * 1 Hz useNowMs tick so it re-evaluates formatTimeAgo every second.
 *
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import React from 'react'

// Stub the shared tick (so no real 1s interval spins up) but keep it a spy
// so we can assert the component subscribes.
const { useNowMsMock } = vi.hoisted(() => ({ useNowMsMock: vi.fn(() => 0) }))
vi.mock('@/hooks/useNowSeconds', () => ({ useNowMs: useNowMsMock }))
vi.mock('@/lib/date', () => ({ formatTimeAgo: (ts: string) => `ago:${ts}` }))

import { TimeAgo } from '@/components/TimeAgo'

afterEach(() => {
  cleanup()
  useNowMsMock.mockClear()
})

describe('TimeAgo', () => {
  it('renders formatTimeAgo output for a timestamp', () => {
    render(<TimeAgo timestamp="2026-06-15T12:00:00Z" />)
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
  })

  it('subscribes to the shared 1 Hz tick', () => {
    render(<TimeAgo timestamp="2026-06-15T12:00:00Z" />)
    expect(useNowMsMock).toHaveBeenCalled()
  })

  it('renders nothing by default when timestamp is null', () => {
    const { container } = render(<TimeAgo timestamp={null} />)
    expect(container.textContent).toBe('')
  })

  it('renders the fallback when timestamp is missing', () => {
    render(<TimeAgo timestamp={undefined} fallback={<span>Never</span>} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
  })
})
