/**
 * @vitest-environment jsdom
 */
import { stubIntersectionObserver } from '@/__tests__/helpers/browser-stubs'
import { render, screen, cleanup, act } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import { LazyMount } from '@/components/LazyMount'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe('LazyMount', () => {
  it('mounts children once IntersectionObserver fires isIntersecting', () => {
    const io = stubIntersectionObserver()

    render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    // Placeholder mounted; child should NOT yet be in DOM.
    expect(screen.queryByTestId('child')).toBeNull()

    act(() => {
      io.fire(true)
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
