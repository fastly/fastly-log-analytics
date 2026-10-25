/**
 * Reusable maplibre-gl mock for jsdom-driven component / hook tests.
 *
 * Maplibre is a WebGL-backed library — jsdom can't render it, but most of
 * our wrapper components only need the constructor to fire, a `load` event
 * to bubble, and the side-effecting calls (`addSource`, `addLayer`,
 * `setPaintProperty`, …) to be no-ops we can assert against.
 *
 * Usage pattern. `vi.mock` is hoisted to the top of the file, so the mock
 * call must stay at module scope in the test file. This helper provides
 * the *factory body* and a shared `mapInstances` array — the test file
 * keeps the `vi.mock` call itself:
 *
 *     import { vi } from 'vitest'
 *     import { mapInstances, maplibreMockFactory, installMaplibreSideEffects }
 *       from '../helpers/maplibre-mock'
 *
 *     vi.mock('maplibre-gl', () => maplibreMockFactory())
 *     vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))
 *     installMaplibreSideEffects()
 *
 *     beforeEach(() => { mapInstances.length = 0 })
 *
 *     it('mounts a map', () => {
 *       render(<SomeMapComponent />)
 *       expect(mapInstances.length).toBe(1)
 *     })
 */

import type { MapOptions } from 'maplibre-gl'
import { StubResizeObserver } from './page-smoke'

export type MockMapEventPayload = Record<string, unknown>
export type MockMapListener = (e: MockMapEventPayload) => void
export type MockMapSource = Record<string, unknown>
export type MockMapLayer = { id?: string } & Record<string, unknown>
export type MockMapSourceHandle = MockMapSource & { setData: (data: unknown) => void }

export interface MockMapInstance {
  container: string | HTMLElement | undefined
  listeners: Record<string, MockMapListener[]>
  sources: Record<string, MockMapSource>
  layers: MockMapLayer[]
  options: Partial<MapOptions> | undefined
  on: (event: string, ...rest: unknown[]) => void
  off: () => void
  addSource: (id: string, src: MockMapSource) => void
  addLayer: (layer: MockMapLayer) => void
  removeLayer: () => void
  removeSource: () => void
  getSource: (id: string) => MockMapSourceHandle | undefined
  getLayer: (id: string) => MockMapLayer | undefined
  setFeatureState: () => void
  setPaintProperty: (...args: unknown[]) => void
  setLayoutProperty: () => void
  setFilter: () => void
  setStyle: () => void
  isStyleLoaded: () => boolean
  getStyle: () => { layers: MockMapLayer[] }
  queryRenderedFeatures: () => unknown[]
  getCanvas: () => { style: Record<string, string> }
  remove: () => void
  resize: () => void
  removed: boolean
  project: () => { x: number; y: number }
  fitBounds: () => void
  flyTo: () => void
  cameraForBounds: () => { center: [number, number]; zoom: number }
  setMaxBounds: () => void
  addControl: () => void
  emit: (event: string, payload?: MockMapEventPayload) => void
}

/**
 * Shared array of every MockMap instance created via the mocked
 * `maplibregl.Map` constructor. Test files should clear it in a
 * `beforeEach` so each case starts with a clean slate:
 *
 *     beforeEach(() => { mapInstances.length = 0 })
 */
export const mapInstances: MockMapInstance[] = []

/**
 * Build the module object that `import maplibregl from 'maplibre-gl'`
 * normally returns. Designed to be passed as the factory argument to
 * vi.mock at the top level of the test file:
 *
 *     vi.mock('maplibre-gl', () => maplibreMockFactory())
 *
 * Every constructed map gets pushed into the shared `mapInstances` array.
 */
export function maplibreMockFactory(opts?: { strictSources?: boolean }) {
  const strictSources = opts?.strictSources ?? false
  class MockMap implements MockMapInstance {
    container: string | HTMLElement | undefined
    listeners: Record<string, MockMapListener[]> = {}
    sources: Record<string, MockMapSource> = {}
    layers: MockMapLayer[] = []
    options: Partial<MapOptions> | undefined
    removed = false
    constructor(opts?: Partial<MapOptions>) {
      this.container = opts?.container
      this.options = opts
      mapInstances.push(this)
      // Fire `load` on the next microtask, matching real maplibre's async
      // bootstrap. Tests can `await Promise.resolve()` to flush.
      queueMicrotask(() => this.emit('load'))
    }
    on(event: string, ...rest: unknown[]) {
      // maplibre's `on` accepts either (event, handler) OR
      // (event, layerId, handler) — collapse both forms.
      const fn = (rest.length === 2 ? rest[1] : rest[0]) as MockMapListener
      this.listeners[event] = this.listeners[event] || []
      this.listeners[event].push(fn)
    }
    off() {}
    addSource(id: string, src: MockMapSource) {
      const handle: MockMapSourceHandle = {
        ...src,
        setData: (data: unknown) => {
          handle.data = data
        },
      }
      this.sources[id] = handle
    }
    addLayer(layer: MockMapLayer) {
      this.layers.push(layer)
    }
    removeLayer() {}
    removeSource() {}
    getSource(id: string): MockMapSourceHandle | undefined {
      const src = this.sources[id] as MockMapSourceHandle | undefined
      if (strictSources) return src
      return src ?? { setData: () => {} }
    }
    getLayer(id: string) {
      return this.layers.find((l) => l.id === id)
    }
    setFeatureState() {}
    setPaintProperty() {}
    setLayoutProperty() {}
    setFilter() {}
    setStyle() {}
    isStyleLoaded() {
      return true
    }
    getStyle() {
      return { layers: this.layers }
    }
    queryRenderedFeatures(): unknown[] {
      return []
    }
    getCanvas() {
      return { style: {} as Record<string, string> }
    }
    remove() {
      this.removed = true
    }
    resize() {}
    project() {
      return { x: 0, y: 0 }
    }
    fitBounds() {}
    flyTo() {}
    cameraForBounds(): { center: [number, number]; zoom: number } {
      return { center: [0, 0], zoom: 2 }
    }
    setMaxBounds() {}
    addControl() {}
    emit(event: string, payload: MockMapEventPayload = {}) {
      ;(this.listeners[event] || []).forEach((fn) => fn(payload))
    }
  }

  class MockLngLatBounds {
    extend() {
      return this
    }
  }

  // maplibre-gl ships both a default export (used in app code as
  // `import maplibregl from 'maplibre-gl'`) and named exports. Provide both.
  return {
    default: {
      Map: MockMap,
      LngLatBounds: MockLngLatBounds,
      NavigationControl: class {},
      GlobeControl: class {},
      setWorkerUrl: () => {},
    },
    Map: MockMap,
    LngLatBounds: MockLngLatBounds,
    NavigationControl: class {},
    GlobeControl: class {},
    setWorkerUrl: () => {},
  }
}

/**
 * Install browser-only globals that maplibre wrappers tend to use
 * (notably ResizeObserver, which jsdom does not implement). Safe to call
 * multiple times — only patches when the global is absent.
 */
export function installMaplibreSideEffects(): void {
  if (!('ResizeObserver' in globalThis)) {
    globalThis.ResizeObserver = StubResizeObserver
  }
}
