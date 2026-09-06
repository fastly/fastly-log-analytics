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

export interface MockMapInstance {
  container: HTMLElement | undefined
  listeners: Record<string, ((e: any) => void)[]>
  sources: Record<string, any>
  layers: any[]
  options: any
  on: (event: string, ...rest: any[]) => void
  off: () => void
  addSource: (id: string, src: any) => void
  addLayer: (layer: any) => void
  removeLayer: () => void
  removeSource: () => void
  getSource: (id: string) => any
  getLayer: (id: string) => any
  setFeatureState: () => void
  setPaintProperty: (...args: any[]) => void
  setLayoutProperty: () => void
  setFilter: () => void
  setStyle: () => void
  isStyleLoaded: () => boolean
  getStyle: () => { layers: any[] }
  queryRenderedFeatures: () => any[]
  getCanvas: () => { style: Record<string, string> }
  remove: () => void
  removed: boolean
  project: () => { x: number; y: number }
  fitBounds: () => void
  setMaxBounds: () => void
  addControl: () => void
  emit: (event: string, payload?: any) => void
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
export function maplibreMockFactory() {
  class MockMap implements MockMapInstance {
    container: HTMLElement | undefined
    listeners: Record<string, ((e: any) => void)[]> = {}
    sources: Record<string, any> = {}
    layers: any[] = []
    options: any
    removed = false
    constructor(opts: any) {
      this.container = opts?.container
      this.options = opts
      mapInstances.push(this)
      // Fire `load` on the next microtask, matching real maplibre's async
      // bootstrap. Tests can `await Promise.resolve()` to flush.
      queueMicrotask(() => this.emit('load'))
    }
    on(event: string, ...rest: any[]) {
      // maplibre's `on` accepts either (event, handler) OR
      // (event, layerId, handler) — collapse both forms.
      const fn = rest.length === 2 ? rest[1] : rest[0]
      this.listeners[event] = this.listeners[event] || []
      this.listeners[event].push(fn)
    }
    off() {}
    addSource(id: string, src: any) {
      this.sources[id] = src
    }
    addLayer(layer: any) {
      this.layers.push(layer)
    }
    removeLayer() {}
    removeSource() {}
    getSource(id: string) {
      const src = this.sources[id]
      if (src) {
        return {
          ...src,
          setData: (data: any) => {
            src.data = data
          }
        }
      }
      return { setData: () => {} }
    }
    getLayer(id: string) {
      return this.layers.find((l) => l === id || l?.id === id)
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
    queryRenderedFeatures() {
      return []
    }
    getCanvas() {
      return { style: {} as Record<string, string> }
    }
    remove() {
      this.removed = true
    }
    project() {
      return { x: 0, y: 0 }
    }
    fitBounds() {}
    setMaxBounds() {}
    addControl() {}
    emit(event: string, payload: any = {}) {
      ;(this.listeners[event] || []).forEach((fn) => fn(payload))
    }
  }

  // maplibre-gl ships both a default export (used in app code as
  // `import maplibregl from 'maplibre-gl'`) and named exports. Provide both.
  return {
    default: {
      Map: MockMap,
      LngLatBounds: class {},
      NavigationControl: class {},
      setWorkerUrl: () => {},
    },
    Map: MockMap,
    LngLatBounds: class {},
    NavigationControl: class {},
    setWorkerUrl: () => {},
  }
}

/**
 * Install browser-only globals that maplibre wrappers tend to use
 * (notably ResizeObserver, which jsdom does not implement). Safe to call
 * multiple times — only patches when the global is absent.
 */
export function installMaplibreSideEffects(): void {
  if (typeof globalThis !== 'undefined' && !(globalThis as any).ResizeObserver) {
    class MockResizeObserver {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
    ;(globalThis as any).ResizeObserver = MockResizeObserver
  }
}
