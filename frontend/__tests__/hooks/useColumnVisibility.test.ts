import { renderHook, act } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'

describe('useColumnVisibility', () => {
  it('initializes to empty object by default', () => {
    const { result } = renderHook(() => useColumnVisibility())
    expect(result.current[0]).toEqual({})
  })

  it('accepts an initial visibility state', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: false, col2: true }))
    expect(result.current[0]).toEqual({ col1: false, col2: true })
  })

  it('handleChange sets a column to true', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => { result.current[2]('col1', true) })
    expect(result.current[0]).toEqual({ col1: true })
  })

  it('handleChange sets a column to false', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => { result.current[2]('col1', false) })
    expect(result.current[0]).toEqual({ col1: false })
  })

  it('handleChange accumulates changes without overwriting other columns', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: true }))
    act(() => { result.current[2]('col2', false) })
    expect(result.current[0]).toEqual({ col1: true, col2: false })
  })

  it('multiple handleChange calls within one act accumulate', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => {
      result.current[2]('col1', true)
      result.current[2]('col2', false)
    })
    expect(result.current[0]).toEqual({ col1: true, col2: false })
  })

  it('setVisibility replaces the full state', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: true, col2: true }))
    act(() => { result.current[1]({ col3: false }) })
    expect(result.current[0]).toEqual({ col3: false })
  })

  it('handleChange is stable across renders', () => {
    const { result, rerender } = renderHook(() => useColumnVisibility())
    const handleChangeBefore = result.current[2]
    rerender()
    expect(result.current[2]).toBe(handleChangeBefore)
  })
})
