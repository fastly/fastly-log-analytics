import { renderHook } from '@testing-library/react'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { useMapResize } from '@/components/Map/useMapResize'
import { stubResizeObserver } from '../../helpers/browser-stubs'

let ro: ReturnType<typeof stubResizeObserver>

beforeEach(() => {
  ro = stubResizeObserver()
})

afterEach(() => vi.unstubAllGlobals())

test('calls map.resize() whenever the container resizes and disconnects on unmount', () => {
  const el = document.createElement('div')
  const resize = vi.fn()
  const mapRef = { current: { resize } }
  const containerRef = { current: el }
  const { unmount } = renderHook(() => useMapResize(mapRef, containerRef))

  expect(ro.observe).toHaveBeenCalledWith(el)
  ro.fire()
  expect(resize).toHaveBeenCalledTimes(1)

  unmount()
  expect(ro.disconnect).toHaveBeenCalledTimes(1)
})

test('tolerates a map that has not been constructed yet', () => {
  const mapRef = { current: null }
  renderHook(() => useMapResize(mapRef, { current: document.createElement('div') }))
  expect(() => ro.fire()).not.toThrow()
})

test('does nothing without a container', () => {
  renderHook(() => useMapResize({ current: null }, { current: null }))
  expect(ro.observe).not.toHaveBeenCalled()
})
