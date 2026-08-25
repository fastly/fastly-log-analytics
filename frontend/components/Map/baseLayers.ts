/**
 * Shared MapLibre base-layer init (world source + countries fill layer).
 *
 * Four map components (ChoroplethMap, NetworkMap/MapLayer, ShieldingMap,
 * Insights/ImpossibleDistanceModal) each open their `map.on('load')`
 * handler with the same two calls:
 *   addSource('world', { type: 'geojson', data: <decoded geojson> })
 *   addLayer({ id: 'countries', type: 'fill', source: 'world', paint: {
 *     'fill-color': countryFill(isDark),
 *     'fill-outline-color': countryOutline(isDark),
 *     // optionally 'fill-opacity': 0.8
 *   }})
 * Two of those then re-paint the same layer when theme flips dark/light.
 *
 * Extracting both halves removes ~20 lines of duplication per consumer and
 * makes layer-id / source-id collisions impossible (everyone resolves
 * through the same constants).
 *
 * World geometry ships as TopoJSON (~108KB raw / ~39KB gzip) and is decoded
 * to GeoJSON at runtime by topojson-client. The decoded FeatureCollection is
 * memoized in a module-scope Promise so multiple maps mounting on the same
 * route share one fetch + one parse + one decode (previously each map paid
 * its own ~257KB JSON.parse on every mount).
 */

import type { FeatureCollection } from 'geojson'
import type { Map as MapLibreMap } from 'maplibre-gl'
import type { GeometryCollection, Topology } from 'topojson-specification'

import { countryFill, countryOutline } from './colors'

export const WORLD_SOURCE_ID = 'world'
export const COUNTRIES_LAYER_ID = 'countries'
export const WORLD_TOPOJSON_URL = '/geo/world.topo.json'

type FillPaintExtras = {
  'fill-opacity'?: number
}

let worldGeoPromise: Promise<FeatureCollection> | null = null

async function loadWorldGeoJson(): Promise<FeatureCollection> {
  if (!worldGeoPromise) {
    worldGeoPromise = (async () => {
      // Dynamic import so topojson-client only lands in chunks that mount a
      // map. Routes like /admin / /share-login never pay this parse cost.
      const [topojsonClient, res] = await Promise.all([
        import('topojson-client'),
        fetch(WORLD_TOPOJSON_URL),
      ])

      const featureFn = topojsonClient.feature || (topojsonClient as { default?: { feature?: typeof topojsonClient.feature } }).default?.feature;
      if (!featureFn) {
        throw new Error("feature function not found in topojson-client exports!");
      }

      const topo = (await res.json()) as Topology<{ world: GeometryCollection }>
      const fc = featureFn(topo, topo.objects.world) as FeatureCollection
      return fc
    })().catch((err) => {
      console.error("[MapBaseLayer] loadWorldGeoJson: caught error:", err);
      worldGeoPromise = null
      throw err
    })
  }
  return worldGeoPromise
}

export function addCountryBaseLayer(
  map: MapLibreMap,
  opts: { isDark: boolean; extraPaint?: FillPaintExtras; beforeId?: string },
): void {
  if (map.getSource(WORLD_SOURCE_ID)) {
    if (!map.getLayer(COUNTRIES_LAYER_ID)) {
      const layers = typeof map.getStyle === 'function' ? map.getStyle()?.layers : undefined
      const firstLayerAfterBg = layers?.find(l => l.id !== 'background')
      const beforeId = opts.beforeId || firstLayerAfterBg?.id
      map.addLayer(
        {
          id: COUNTRIES_LAYER_ID,
          type: 'fill',
          source: WORLD_SOURCE_ID,
          paint: {
            'fill-color': countryFill(opts.isDark),
            'fill-outline-color': countryOutline(opts.isDark),
            ...(opts.extraPaint ?? {}),
          },
        },
        beforeId,
      )
    }
    return
  }

  void loadWorldGeoJson().then((fc) => {
    if (!map.getSource(WORLD_SOURCE_ID)) {
      map.addSource(WORLD_SOURCE_ID, { type: 'geojson', data: fc })
    }
    if (!map.getLayer(COUNTRIES_LAYER_ID)) {
      const layers = typeof map.getStyle === 'function' ? map.getStyle()?.layers : undefined
      const firstLayerAfterBg = layers?.find(l => l.id !== 'background')
      const beforeId = opts.beforeId || firstLayerAfterBg?.id
      map.addLayer(
        {
          id: COUNTRIES_LAYER_ID,
          type: 'fill',
          source: WORLD_SOURCE_ID,
          paint: {
            'fill-color': countryFill(opts.isDark),
            'fill-outline-color': countryOutline(opts.isDark),
            ...(opts.extraPaint ?? {}),
          },
        },
        beforeId,
      )
    }
    if (typeof map.resize === 'function') {
      map.resize()
    }
  }).catch((err) => {
    console.error("[MapBaseLayer] Failed to load or set world geojson:", err);
  })
}

export function updateCountryBaseLayerTheme(map: MapLibreMap, isDark: boolean): void {
  if (!map.getLayer(COUNTRIES_LAYER_ID)) return
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-color', countryFill(isDark))
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-outline-color', countryOutline(isDark))
}
