/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup, act } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import { LazyMount } from '@/components/LazyMount'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe('LazyMount', () => {
  it('mounts children once IntersectionObserver fires isIntersecting', () => {
    type Cb = (entries: { isIntersecting: boolean }[]) => void
    let fire: Cb | null = null
    class FakeIO {
      constructor(cb: Cb) { fire = cb }
      observe() {}
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal('IntersectionObserver', FakeIO as unknown as typeof IntersectionObserver)

    render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    // Placeholder mounted; child should NOT yet be in DOM.
    expect(screen.queryByTestId('child')).toBeNull()

    act(() => {
      fire!([{ isIntersecting: true }])
    })
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })

  it('mounts immediately when IntersectionObserver is unavailable', () => {
    vi.stubGlobal('IntersectionObserver', undefined)
    render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })
})
