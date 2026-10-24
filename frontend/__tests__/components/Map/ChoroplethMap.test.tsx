/**
 * Consolidated Map tests: ChoroplethMap, MapPrewarm, and useMapResize.
 *
 * @vitest-environment jsdom
 */
import { render, renderHook, waitFor } from '@testing-library/react'
import { describe, it, test, expect, vi, beforeEach } from 'vitest'

import {
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
} from '../../helpers/maplibre-mock'
import { stubResizeObserver } from '../../helpers/browser-stubs'
import { ChoroplethMap } from '@/components/Map/ChoroplethMap'
import { MapPrewarm } from '@/components/Map/MapPrewarm'
import { useMapResize } from '@/components/Map/shared'

const { setWorkerUrl, constructedWithWorker } = vi.hoisted(() => ({
  setWorkerUrl: vi.fn(),
  constructedWithWorker: vi.fn(),
}))

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('maplibre-gl', () => {
  const mapMock = maplibreMockFactory()
  class PrewarmMap extends mapMock.Map {
    constructor(options: ConstructorParameters<typeof mapMock.Map>[0]) {
      constructedWithWorker(setWorkerUrl.mock.calls.at(-1)?.[0])
      super(options)
    }
  }
  return { ...mapMock, Map: PrewarmMap, setWorkerUrl }
})
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))

installMaplibreSideEffects()

describe('ChoroplethMap', () => {
  beforeEach(() => {
    mapInstances.length = 0
  })

  it('mounts a map instance into the container ref', async () => {
    const { container } = render(
      <ChoroplethMap data={[{ country: 'US', count: 100 }]} />,
    )
    expect(container.querySelector('div')).toBeInTheDocument()
    await Promise.resolve()
    expect(mapInstances.length).toBe(1)
  })

  it('renders an empty container when given an empty data array and re-renders cleanly on data changes', () => {
    const { rerender, container } = render(<ChoroplethMap data={[]} />)
    expect(container.firstChild).not.toBeNull()
    rerender(
      <ChoroplethMap
        data={[
          { country: 'US', count: 50 },
          { country: 'GB', count: 5 },
        ]}
      />,
    )
    expect(container.firstChild).not.toBeNull()
  })

  it('accepts an onCountryClick handler without invoking it on mount', async () => {
    const handler = vi.fn()
    render(<ChoroplethMap data={[{ country: 'US', count: 1 }]} onCountryClick={handler} />)
    await Promise.resolve()
    expect(handler).not.toHaveBeenCalled()
  })
})

describe('MapPrewarm', () => {
  it('configures the worker before prewarming without another map component present', async () => {
    constructedWithWorker.mockClear()
    const { unmount } = render(<MapPrewarm />)
    await waitFor(() => expect(constructedWithWorker).toHaveBeenCalledTimes(1))
    expect(constructedWithWorker).toHaveBeenCalledWith('/maplibre-gl-worker.mjs')
    unmount()
  })
})

describe('useMapResize', () => {
  test('calls map.resize() whenever the container resizes and disconnects on unmount', () => {
    const ro = stubResizeObserver()
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

  test('tolerates a map that has not been constructed yet or missing container', () => {
    const ro = stubResizeObserver()
    const mapRef = { current: null }
    renderHook(() => useMapResize(mapRef, { current: document.createElement('div') }))
    expect(() => ro.fire()).not.toThrow()

    const ro2 = stubResizeObserver()
    renderHook(() => useMapResize({ current: null }, { current: null }))
    expect(ro2.observe).not.toHaveBeenCalled()
  })
})
