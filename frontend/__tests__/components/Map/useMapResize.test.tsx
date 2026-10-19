import { renderHook } from '@testing-library/react'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { useMapResize } from '@/components/Map/useMapResize'

let trigger: () => void = () => {}
const observe = vi.fn()
const disconnect = vi.fn()

beforeEach(() => {
  observe.mockReset()
  disconnect.mockReset()
  vi.stubGlobal(
    'ResizeObserver',
    class {
      constructor(cb: () => void) {
        trigger = cb
      }
      observe = observe
      disconnect = disconnect
    },
  )
})

afterEach(() => vi.unstubAllGlobals())

test('calls map.resize() whenever the container resizes and disconnects on unmount', () => {
  const el = document.createElement('div')
  const resize = vi.fn()
  const mapRef = { current: { resize } }
  const containerRef = { current: el }
  const { unmount } = renderHook(() => useMapResize(mapRef, containerRef))

  expect(observe).toHaveBeenCalledWith(el)
  trigger()
  expect(resize).toHaveBeenCalledTimes(1)

  unmount()
  expect(disconnect).toHaveBeenCalledTimes(1)
})

test('tolerates a map that has not been constructed yet', () => {
  const mapRef = { current: null }
  renderHook(() => useMapResize(mapRef, { current: document.createElement('div') }))
  expect(() => trigger()).not.toThrow()
})

test('does nothing without a container', () => {
  renderHook(() => useMapResize({ current: null }, { current: null }))
  expect(observe).not.toHaveBeenCalled()
})
