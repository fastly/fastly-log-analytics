/**
 * Consolidated widget tests: DeltaIndicator, ErrorBoundary, HelpDialog,
 * LazyMount, ServerFooter, Sparkline, and TimeAgo.
 *
 * @vitest-environment jsdom
 */
import { stubIntersectionObserver } from '@/__tests__/helpers/browser-stubs'
import { render, screen, cleanup, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'

let mockHeaders: Record<string, string> = {}
vi.mock('next/headers', () => ({
  headers: async () => ({ get: (k: string) => mockHeaders[k.toLowerCase()] ?? null }),
}))

const { useNowMsMock } = vi.hoisted(() => ({ useNowMsMock: vi.fn(() => 0) }))
vi.mock('@/hooks/useNowSeconds', () => ({ useNowMs: useNowMsMock }))
vi.mock('@/lib/date', () => ({ formatTimeAgo: (ts: string) => `ago:${ts}` }))

import { DeltaIndicator } from '@/components/Insights/shared'
import { ErrorBoundary } from '@/components/ErrorBoundary'
import { HelpDialog } from '@/components/ui/help-dialog'
import { LazyMount } from '@/components/LazyMount'
import { ServerFooter } from '@/components/ServerFooter'
import { Sparkline } from '@/components/Sparkline'
import { TimeAgo } from '@/components/TimeAgo'

afterEach(() => {
  cleanup()
  useNowMsMock.mockClear()
})

describe('DeltaIndicator', () => {
  it('renders nothing when baseline is null or zero', () => {
    const { container: c1 } = render(<DeltaIndicator current={100} baseline={null} />)
    expect(c1.firstChild).toBeNull()
    const { container: c2 } = render(<DeltaIndicator current={100} baseline={0} />)
    expect(c2.firstChild).toBeNull()
  })

  it('renders red TrendingUp when current > baseline, green TrendingDown when <, and neutral Minus when |pct| < 1', () => {
    const { container: up } = render(<DeltaIndicator current={200} baseline={100} />)
    expect(up.querySelector('span')?.className).toMatch(/text-red-600/)
    expect(up.querySelector('span')?.textContent).toMatch(/\+100%/)

    const { container: down } = render(<DeltaIndicator current={50} baseline={100} />)
    expect(down.querySelector('span')?.className).toMatch(/text-green-700/)
    expect(down.querySelector('span')?.textContent).toMatch(/-50%/)

    const { container: neutral } = render(<DeltaIndicator current={1000.5} baseline={1000} />)
    expect(neutral.querySelector('span')).toBeNull()
    expect(neutral.querySelector('svg')).not.toBeNull()

    const { container: diff } = render(<DeltaIndicator current={1234} baseline={1000} />)
    expect(diff.textContent).toMatch(/\+234/)
  })
})

function Boom(): React.ReactElement {
  throw new Error('kaboom')
}

describe('ErrorBoundary', () => {
  let errSpy: ReturnType<typeof vi.spyOn>
  beforeEach(() => {
    errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    errSpy.mockRestore()
  })

  it('renders default fallback when a child throws, custom fallback when provided, and children when ok', () => {
    const { unmount: u1 } = render(
      <ErrorBoundary>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('kaboom')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
    u1()

    const { unmount: u2 } = render(
      <ErrorBoundary>
        <div data-testid="ok">all good</div>
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('ok')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).toBeNull()
    u2()

    render(
      <ErrorBoundary fallback={(err) => <div data-testid="custom">caught: {err.message}</div>}>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('custom')).toHaveTextContent('caught: kaboom')
  })
})

describe('HelpDialog', () => {
  it('renders title, icon, and children when open, nothing when closed, and respects size prop', () => {
    const { unmount: u1 } = render(
      <HelpDialog
        open
        onOpenChange={vi.fn()}
        title="My Title"
        icon={<span data-testid="help-icon">★</span>}
      >
        <p>My content</p>
      </HelpDialog>,
    )
    expect(screen.getByText('My Title')).toBeInTheDocument()
    expect(screen.getByText('My content')).toBeInTheDocument()
    expect(screen.getByTestId('help-icon')).toBeInTheDocument()
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-lg'))).toBe(true)
    u1()

    const { unmount: u2 } = render(
      <HelpDialog open={false} onOpenChange={vi.fn()} title="Hidden">
        <p>Hidden body</p>
      </HelpDialog>,
    )
    expect(screen.queryByText('Hidden body')).not.toBeInTheDocument()
    u2()

    const { unmount: u3 } = render(
      <HelpDialog open onOpenChange={vi.fn()} title="Large-only" size="xl">
        body
      </HelpDialog>,
    )
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-xl'))).toBe(true)
    u3()
  })
})

describe('LazyMount', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('mounts children once IntersectionObserver fires isIntersecting or immediately when unavailable', () => {
    const io = stubIntersectionObserver()
    const { unmount } = render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    expect(screen.queryByTestId('child')).toBeNull()
    act(() => {
      io.fire(true)
    })
    expect(screen.getByTestId('child')).toBeInTheDocument()
    unmount()

    vi.stubGlobal('IntersectionObserver', undefined)
    render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })
})

describe('ServerFooter', () => {
  beforeEach(() => {
    mockHeaders = {}
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, text: async () => 'secret-instance' }))
    process.env.COMMIT_HASH = 'abc1234'
  })
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('renders infrastructure details for admin requests and nothing for Caddy-proxied requests', async () => {
    const el = await ServerFooter()
    expect(el).not.toBeNull()
    expect(JSON.stringify(el)).toContain('abc1234')

    const fetchSpy = vi.fn()
    vi.stubGlobal('fetch', fetchSpy)
    mockHeaders = { 'x-proxied-by-caddy': 'true' }
    const el2 = await ServerFooter()
    expect(el2).toBeNull()
    expect(fetchSpy).not.toHaveBeenCalled()
  })
})

const pts = (vals: number[]) =>
  vals.map((value, i) => ({ ts: new Date(2026, 0, 1, 0, i).toISOString(), value }))

describe('Sparkline', () => {
  it('renders nothing for < 2 points and an svg polyline with custom height for >= 2 points', () => {
    expect(render(<Sparkline points={[]} />).container.firstChild).toBeNull()
    expect(render(<Sparkline points={pts([1])} />).container.firstChild).toBeNull()

    const { container: c1 } = render(<Sparkline points={pts([0, 5, 10])} />)
    const poly1 = c1.querySelector('polyline')!
    expect(poly1.getAttribute('points')!.trim().split(/\s+/)).toHaveLength(3)

    const { container: c2 } = render(<Sparkline points={pts([10, 20, 30, 40])} height={64} />)
    const svg2 = c2.querySelector('svg')!
    expect(svg2.getAttribute('height')).toBe('64')
    expect(svg2.querySelector('polyline')!.getAttribute('points')).not.toEqual(poly1.getAttribute('points'))
  })
})

describe('TimeAgo', () => {
  it('renders formatTimeAgo output, subscribes to 1 Hz tick, and handles null/missing timestamp', () => {
    const { unmount } = render(<TimeAgo timestamp="2026-06-15T12:00:00Z" />)
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
    expect(useNowMsMock).toHaveBeenCalled()
    unmount()

    expect(render(<TimeAgo timestamp={null} />).container.textContent).toBe('')
    render(<TimeAgo timestamp={undefined} fallback={<span>Never</span>} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
  })
})
