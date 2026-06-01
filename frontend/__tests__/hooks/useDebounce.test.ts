/**
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { useDebounce } from '@/hooks/useDebounce'

describe('useDebounce', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('returns the initial value immediately', () => {
    const { result } = renderHook(() => useDebounce('initial', 300))
    expect(result.current).toBe('initial')
  })

  it('does not update before the delay elapses', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'first' },
    })
    rerender({ val: 'second' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('first')
  })

  it('updates after the delay elapses', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'first' },
    })
    rerender({ val: 'second' })
    act(() => { vi.advanceTimersByTime(300) })
    expect(result.current).toBe('second')
  })

  it('resets the timer on rapid changes, only fires once', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'a' },
    })
    rerender({ val: 'b' })
    act(() => { vi.advanceTimersByTime(100) })
    rerender({ val: 'c' })
    act(() => { vi.advanceTimersByTime(100) })
    rerender({ val: 'd' })
    // Still hasn't fired — timer was reset each time
    expect(result.current).toBe('a')
    act(() => { vi.advanceTimersByTime(300) })
    expect(result.current).toBe('d')
  })

  it('uses 300ms default delay', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val), {
      initialProps: { val: 'x' },
    })
    rerender({ val: 'y' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('x')
    act(() => { vi.advanceTimersByTime(1) })
    expect(result.current).toBe('y')
  })
})
