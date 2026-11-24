/**
 * @vitest-environment jsdom
 */
import React from 'react'
import { act, cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useInViewOnce } from '@/components/AnalyticsCard'

let observerCallback: ((entries: Array<{ isIntersecting: boolean }>) => void) | null = null
const observeSpy = vi.fn()
const disconnectSpy = vi.fn()

class FakeIntersectionObserver {
  constructor(cb: (entries: Array<{ isIntersecting: boolean }>) => void) {
    observerCallback = cb
  }
  observe = observeSpy
  disconnect = disconnectSpy
  unobserve = vi.fn()
}

function Probe({ enabled }: { enabled: boolean }) {
  const ref = React.useRef<HTMLDivElement>(null)
  const seen = useInViewOnce(ref, { rootMargin: '0px', enabled })
  return (
    <div ref={ref} data-testid="probe">
      {seen ? 'seen' : 'hidden'}
    </div>
  )
}

afterEach(() => {
  cleanup()
  observerCallback = null
  observeSpy.mockReset()
  disconnectSpy.mockReset()
  vi.unstubAllGlobals()
})

describe('useInViewOnce enabled option', () => {
  it('does not attach IntersectionObserver or latch while disabled during initial skeleton mount', () => {
    vi.stubGlobal('IntersectionObserver', FakeIntersectionObserver)
    const { rerender } = render(<Probe enabled={false} />)
    expect(screen.getByTestId('probe').textContent).toBe('hidden')
    expect(observeSpy).not.toHaveBeenCalled()

    rerender(<Probe enabled={true} />)
    expect(observeSpy).toHaveBeenCalledTimes(1)
    expect(screen.getByTestId('probe').textContent).toBe('hidden')

    act(() => {
      observerCallback?.([{ isIntersecting: true }])
    })
    expect(screen.getByTestId('probe').textContent).toBe('seen')
  })
})
