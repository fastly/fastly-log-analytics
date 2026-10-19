'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import type { Map as MapLibreMap } from 'maplibre-gl'

/**
 * Forces the ~1MB maplibre-gl chunk to download AND maplibre's
 * WebGL initialization to run during app mount, so the dashboard's
 * "Requests by Country" choropleth doesn't pay that cost when the
 * map_data prop arrives.
 *
 * Mirrors PlotlyPrewarm. The cold-init cost of MapLibre is real:
 *   - ~1MB JS chunk parse + compile (~300-800ms)
 *   - WebGL context creation + first paint (~200-400ms)
 *   - world.geojson fetch (~251KB) + parse + initial country fills
 *
 * The choropleth's ChoroplethMap component is dynamically imported
 * by the dashboard page — its loader only fires when the dashboard
 * route mounts. Without this prewarm, the loader runs concurrently
 * with the data-fetch and the user sees a multi-hundred-ms gap
 * between dashboard data arriving and the world map appearing.
 *
 * With the prewarm, the chunk is parsed + WebGL context is ready by
 * the time the dashboard route mounts. The dashboard's ChoroplethMap
 * mount re-uses the already-initialized maplibre module.
 *
 * Hidden via opacity:0 + 1px height (kept in layout flow).
 */
const PrewarmMap = dynamic(
  async () => {
    const maplibre = await import('maplibre-gl')
    // Prewarm can create the shared worker pool before any visible map loads.
    maplibre.setWorkerUrl('/maplibre-gl-worker.mjs')
    const MaplibreMap: typeof MapLibreMap | undefined =
      maplibre.Map || (maplibre as { default?: { Map?: typeof MapLibreMap } }).default?.Map

    function PrewarmInner() {
      const ref = React.useRef<HTMLDivElement>(null)
      React.useEffect(() => {
        if (!ref.current || !MaplibreMap) return
        let map: MapLibreMap | null = null
        try {
          map = new MaplibreMap({
            container: ref.current,
            style: { version: 8, sources: {}, layers: [] },
            interactive: false,
            attributionControl: false,
          })
        } catch {
          // WebGL unavailable (test env / headless / locked-down browser).
          // Real choropleth will hit the same failure and degrade gracefully.
        }
        return () => {
          try {
            map?.remove()
          } catch {}
        }
      }, [])
      return <div ref={ref} style={{ width: 1, height: 1 }} />
    }
    return PrewarmInner
  },
  { ssr: false },
)

function MapPrewarmImpl() {
  return (
    <div
      aria-hidden="true"
      style={{
        opacity: 0,
        height: '1px',
        width: '1px',
        overflow: 'hidden',
        pointerEvents: 'none',
      }}
    >
      <PrewarmMap />
    </div>
  )
}

export const MapPrewarm = React.memo(MapPrewarmImpl)
