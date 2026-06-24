/**
 * Coverage uplift for `components/Map/ShieldingMap.tsx`. The file mixes pure
 * geometry/color helpers with a maplibre-gl wrapper. jsdom can't render
 * WebGL, so we unit-test the pure helpers directly and add a maplibre-mocked
 * smoke for the component (mount, sources added, cleanup).
 *
 * @vitest-environment jsdom
 */
import { render, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

const mapInstances: any[] = []
vi.mock('maplibre-gl', () => {
  class MockMap {
    container: HTMLElement | undefined
    listeners: Record<string, ((e: any) => void)[]> = {}
    sources: Record<string, any> = {}
    layers: any[] = []
    removed = false
    constructor(opts: any) {
      this.container = opts.container
      mapInstances.push(this)
      queueMicrotask(() => this.emit('load'))
    }
    on(event: string, ...rest: any[]) {
      const fn = rest.length === 2 ? rest[1] : rest[0]
      this.listeners[event] = this.listeners[event] || []
      this.listeners[event].push(fn)
    }
    off() {}
    addSource(id: string, src: any) {
      this.sources[id] = { ...src, setData: vi.fn() }
    }
    addLayer(layer: any) {
      this.layers.push(layer)
    }
    removeLayer() {}
    removeSource() {}
    getSource(id: string) {
      return this.sources[id]
    }
    getLayer() {
      return undefined
    }
    setFeatureState() {}
    setPaintProperty() {}
    setLayoutProperty() {}
    setFilter() {}
    setStyle() {}
    isStyleLoaded() {
      return true
    }
    queryRenderedFeatures() {
      return []
    }
    getCanvas() {
      return { style: {} }
    }
    remove() {
      this.removed = true
    }
    project() {
      return { x: 0, y: 0 }
    }
    fitBounds() {}
    flyTo() {}
    cameraForBounds() {
      return { center: [0, 0], zoom: 2 }
    }
    setMaxBounds() {}
    addControl() {}
    emit(event: string, payload: any = {}) {
      ;(this.listeners[event] || []).forEach((fn) => fn(payload))
    }
  }
  return {
    default: { Map: MockMap, LngLatBounds: class {}, NavigationControl: class {} },
    Map: MockMap,
    LngLatBounds: class {},
    NavigationControl: class {},
  }
})

vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))

class MockResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
;(globalThis as any).ResizeObserver = (globalThis as any).ResizeObserver || MockResizeObserver

// ── Helper unit tests ─────────────────────────────────────────────────────────

describe('ShieldingMap helpers', () => {
  describe('greatCirclePoints', () => {
    it('returns a 2-point degenerate when endpoints are identical', async () => {
      const { greatCirclePoints } = await import('@/components/Map/ShieldingMap')
      const pts = greatCirclePoints(40, -74, 40, -74)
      expect(pts).toHaveLength(2)
      expect(pts[0]).toEqual([-74, 40])
      expect(pts[1]).toEqual([-74, 40])
    })

    it('handles antimeridian wrap without >180 degree jumps in output', async () => {
      const { greatCirclePoints } = await import('@/components/Map/ShieldingMap')
      const pts = greatCirclePoints(0, -179, 0, 179)
      // Consecutive longitude deltas must not exceed 180 degrees — that's the
      // exact bug the wrap-handling block in greatCirclePoints prevents.
      for (let i = 1; i < pts.length; i++) {
        const delta = Math.abs(pts[i][0] - pts[i - 1][0])
        expect(delta).toBeLessThanOrEqual(180)
      }
      expect(pts.length).toBeGreaterThan(2)
    })
  })

  describe('efficiencyColor', () => {
    it('returns the neutral indigo when ratio is null', async () => {
      const { efficiencyColor } = await import('@/components/Map/ShieldingMap')
      expect(efficiencyColor(null)).toBe('#6366f1')
    })

    it('returns green when ratio < 1.5', async () => {
      const { efficiencyColor } = await import('@/components/Map/ShieldingMap')
      expect(efficiencyColor(1.0)).toBe('#22c55e')
      expect(efficiencyColor(1.49)).toBe('#22c55e')
    })

    it('returns yellow when ratio < 3', async () => {
      const { efficiencyColor } = await import('@/components/Map/ShieldingMap')
      expect(efficiencyColor(1.5)).toBe('#eab308')
      expect(efficiencyColor(2.9)).toBe('#eab308')
    })

    it('returns red when ratio >= 3', async () => {
      const { efficiencyColor } = await import('@/components/Map/ShieldingMap')
      expect(efficiencyColor(3.0)).toBe('#ef4444')
      expect(efficiencyColor(10)).toBe('#ef4444')
    })
  })

  describe('lineWidth', () => {
    it('clamps to 1.5 at low request counts', async () => {
      const { lineWidth } = await import('@/components/Map/ShieldingMap')
      expect(lineWidth(0)).toBe(1.5)
      expect(lineWidth(1)).toBe(1.5)
    })

    it('clamps to 6 at very high request counts', async () => {
      const { lineWidth } = await import('@/components/Map/ShieldingMap')
      expect(lineWidth(1_000_000_000)).toBe(6)
      expect(lineWidth(Number.MAX_SAFE_INTEGER)).toBe(6)
    })

    it('scales logarithmically between the bounds', async () => {
      const { lineWidth } = await import('@/components/Map/ShieldingMap')
      const w10 = lineWidth(10)
      const w100 = lineWidth(100)
      expect(w10).toBeGreaterThan(1.5)
      expect(w10).toBeLessThan(6)
      expect(w100).toBeGreaterThan(w10)
    })
  })

  describe('rafThrottle', () => {
    let rafCallbacks: FrameRequestCallback[]
    let originalRAF: typeof globalThis.requestAnimationFrame

    beforeEach(() => {
      rafCallbacks = []
      originalRAF = globalThis.requestAnimationFrame
      globalThis.requestAnimationFrame = ((cb: FrameRequestCallback) => {
        rafCallbacks.push(cb)
        return rafCallbacks.length as unknown as number
      }) as typeof globalThis.requestAnimationFrame
    })

    afterEach(() => {
      globalThis.requestAnimationFrame = originalRAF
    })

    it('coalesces multiple calls within a frame to a single invocation', async () => {
      const { rafThrottle } = await import('@/components/Map/ShieldingMap')
      const fn = vi.fn()
      const throttled = rafThrottle(fn)
      throttled(1)
      throttled(2)
      throttled(3)
      // Nothing fires until the frame runs.
      expect(fn).not.toHaveBeenCalled()
      expect(rafCallbacks).toHaveLength(1)
      rafCallbacks[0](performance.now())
      // Only the latest args land.
      expect(fn).toHaveBeenCalledTimes(1)
      expect(fn).toHaveBeenCalledWith(3)
    })

    it('re-queues on subsequent calls after the frame fires', async () => {
      const { rafThrottle } = await import('@/components/Map/ShieldingMap')
      const fn = vi.fn()
      const throttled = rafThrottle(fn)
      throttled('a')
      rafCallbacks[0](performance.now())
      throttled('b')
      expect(rafCallbacks).toHaveLength(2)
      rafCallbacks[1](performance.now())
      expect(fn).toHaveBeenCalledTimes(2)
      expect(fn).toHaveBeenNthCalledWith(1, 'a')
      expect(fn).toHaveBeenNthCalledWith(2, 'b')
    })
  })

  describe('buildArcFeatures', () => {
    it('returns a FeatureCollection with one feature per valid row', async () => {
      const { buildArcFeatures } = await import('@/components/Map/ShieldingMap')
      const rows = [
        {
          edge_lat: 40, edge_lon: -74,
          shield_lat: 51, shield_lon: 0,
          edge_pop: 'JFK', shield_pop: 'LON',
          requests: 100, p50_ms: 10, p95_ms: 50, p99_ms: 100,
          distance_km: 5000, light_speed_rtt_ms: 33, efficiency_ratio: 2.5,
          anomaly_static: false,
        },
        {
          edge_lat: 35, edge_lon: 139,
          shield_lat: 22, shield_lon: 114,
          edge_pop: 'NRT', shield_pop: 'HKG',
          requests: 50, p50_ms: 5, p95_ms: 20, p99_ms: 40,
          distance_km: 2900, light_speed_rtt_ms: 19, efficiency_ratio: 1.2,
          anomaly_static: false,
        },
      ]
      const fc = buildArcFeatures(rows)
      expect(fc.type).toBe('FeatureCollection')
      expect(fc.features).toHaveLength(2)
      expect(fc.features[0].geometry.type).toBe('LineString')
      expect(fc.features[0].properties?.edge_pop).toBe('JFK')
      expect(fc.features[0].properties?.color).toBeDefined()
      expect(fc.features[0].properties?.line_width).toBeGreaterThan(1.5)
    })

    it('skips rows with missing coords or zero-length arcs', async () => {
      const { buildArcFeatures } = await import('@/components/Map/ShieldingMap')
      const rows = [
        // missing shield coords
        { edge_lat: 40, edge_lon: -74, shield_lat: null, shield_lon: null, requests: 10 },
        // missing edge coords
        { edge_lat: null, edge_lon: null, shield_lat: 51, shield_lon: 0, requests: 10 },
        // zero-length (same POP)
        { edge_lat: 40, edge_lon: -74, shield_lat: 40, shield_lon: -74, requests: 10 },
      ]
      const fc = buildArcFeatures(rows)
      expect(fc.features).toHaveLength(0)
    })
  })

  describe('buildDotFeatures', () => {
    it('returns one feature per unique edge/shield POP', async () => {
      const { buildDotFeatures } = await import('@/components/Map/ShieldingMap')
      const rows = [
        { edge_lat: 40, edge_lon: -74, edge_pop: 'JFK', shield_lat: 51, shield_lon: 0, shield_pop: 'LON' },
        // duplicate edge — should dedupe
        { edge_lat: 40, edge_lon: -74, edge_pop: 'JFK', shield_lat: 35, shield_lon: 139, shield_pop: 'NRT' },
      ]
      const fc = buildDotFeatures(rows)
      expect(fc.type).toBe('FeatureCollection')
      // 1 unique edge (JFK) + 2 unique shields (LON, NRT)
      expect(fc.features).toHaveLength(3)
      const roles = fc.features.map((f) => f.properties?.role).sort()
      expect(roles).toEqual(['edge', 'shield', 'shield'])
    })

    it('omits the synthetic "Direct to Origin" shield placeholder', async () => {
      const { buildDotFeatures } = await import('@/components/Map/ShieldingMap')
      const rows = [
        {
          edge_lat: 40, edge_lon: -74, edge_pop: 'JFK',
          shield_lat: 0, shield_lon: 0, shield_pop: 'Direct to Origin',
        },
      ]
      const fc = buildDotFeatures(rows)
      const shieldPops = fc.features.filter((f) => f.properties?.role === 'shield')
      expect(shieldPops).toHaveLength(0)
    })

    it('returns an empty FeatureCollection for missing coords', async () => {
      const { buildDotFeatures } = await import('@/components/Map/ShieldingMap')
      const rows = [
        { edge_lat: null, edge_lon: null, shield_lat: null, shield_lon: null },
      ]
      const fc = buildDotFeatures(rows)
      expect(fc.features).toHaveLength(0)
    })
  })
})

// ── Component smoke ───────────────────────────────────────────────────────────

describe('ShieldingMap component', () => {
  beforeEach(() => {
    mapInstances.length = 0
    // The map's `load` handler runs addCountryBaseLayer → loadWorldGeoJson,
    // which fire-and-forget `fetch('/geo/world.topo.json')`. In jsdom that
    // relative URL resolves to http://localhost:3000 (no dev server) and the
    // un-awaited rejection surfaces as an unhandled ECONNREFUSED error. maplibre
    // itself is mocked above; stub the topojson fetch so the world-source decode
    // resolves cleanly with an empty geometry collection.
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        json: async () => ({
          type: 'Topology',
          objects: { world: { type: 'GeometryCollection', geometries: [] } },
          arcs: [],
        }),
      })),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('mounts a map instance and registers arcs + dots sources', async () => {
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const rows = [
      {
        edge_lat: 40, edge_lon: -74,
        shield_lat: 51, shield_lon: 0,
        edge_pop: 'JFK', shield_pop: 'LON',
        requests: 100, p50_ms: 10, p95_ms: 50, p99_ms: 100,
      },
    ]
    render(<ShieldingMap rows={rows} />)
    // Flush the queued 'load' microtask inside act() — it triggers
    // setMapReady(true), which React 19 flags if unwrapped.
    await act(async () => {
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(mapInstances.length).toBe(1)
    const instance = mapInstances[0]
    expect(Object.keys(instance.sources)).toEqual(
      expect.arrayContaining(['world', 'arcs', 'dots']),
    )
  })

  it('renders the edge-only fallback when edgeOnly is true', async () => {
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const { getByText } = render(<ShieldingMap rows={[]} edgeOnly />)
    expect(getByText(/edge-only logging detected/i)).toBeInTheDocument()
  })

  it('renders the empty-state when rows is empty and not loading', async () => {
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const { getByText } = render(<ShieldingMap rows={[]} isLoading={false} />)
    expect(getByText(/no shielding path data/i)).toBeInTheDocument()
  })

  it('does not crash transitioning from loading to the empty state (Rules-of-Hooks regression)', async () => {
    // Repro of the production crash on /network. The shielding card first
    // mounts in the loading state (no early return fires → all hooks run,
    // main render path), then resolves to empty (early return). When the
    // a11y useMemo hooks lived BELOW the early returns, the second render
    // executed fewer hooks and React threw "Rendered fewer hooks than
    // expected", crashing /network to its segment error boundary. The hooks
    // now sit above the returns, so the transition is safe.
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const { rerender, getByText } = render(<ShieldingMap rows={[]} isLoading />)
    await act(async () => {
      await Promise.resolve()
      await Promise.resolve()
    })
    // Resolving to the empty state must not throw.
    rerender(<ShieldingMap rows={[]} isLoading={false} />)
    expect(getByText(/no shielding path data/i)).toBeInTheDocument()
  })

  it('renders the POP-coordinates-unavailable fallback when rows have no coords', async () => {
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const rows = [
      { edge_pop: 'XYZ', shield_pop: 'ABC', edge_lat: null, edge_lon: null, shield_lat: null, shield_lon: null },
    ]
    const { getByText } = render(<ShieldingMap rows={rows} isLoading={false} />)
    expect(getByText(/POP coordinates unavailable/i)).toBeInTheDocument()
  })

  it('calls map.remove() on unmount to release WebGL resources', async () => {
    const { ShieldingMap } = await import('@/components/Map/ShieldingMap')
    const rows = [
      {
        edge_lat: 40, edge_lon: -74,
        shield_lat: 51, shield_lon: 0,
        edge_pop: 'JFK', shield_pop: 'LON',
        requests: 100, p50_ms: 10, p95_ms: 50, p99_ms: 100,
      },
    ]
    const { unmount } = render(<ShieldingMap rows={rows} />)
    await act(async () => {
      await Promise.resolve()
      await Promise.resolve()
    })
    const instance = mapInstances[0]
    expect(instance.removed).toBe(false)
    unmount()
    expect(instance.removed).toBe(true)
  })
})
