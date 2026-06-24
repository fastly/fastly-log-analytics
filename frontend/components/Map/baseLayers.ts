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
import type { GeoJSONSource, Map as MapLibreMap } from 'maplibre-gl'
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
      const [{ feature }, res] = await Promise.all([
        import('topojson-client'),
        fetch(WORLD_TOPOJSON_URL),
      ])
      const topo = (await res.json()) as Topology<{ world: GeometryCollection }>
      return feature(topo, topo.objects.world) as FeatureCollection
    })().catch((err) => {
      // Reset the cache so a transient fetch failure doesn't permanently
      // wedge every map on the page.
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
  if (!map.getSource(WORLD_SOURCE_ID)) {
    // Seed the source with an empty FeatureCollection so addLayer below has
    // something to bind to synchronously; swap in the real data once the
    // topojson fetch + decode resolves. MapLibre repaints on setData.
    const emptyFc: FeatureCollection = { type: 'FeatureCollection', features: [] }
    map.addSource(WORLD_SOURCE_ID, { type: 'geojson', data: emptyFc })
    void loadWorldGeoJson().then((fc) => {
      // The map can be torn down between fetch start and resolve; bail if
      // the source is gone or no longer a GeoJSON source.
      const src = map.getSource(WORLD_SOURCE_ID) as GeoJSONSource | undefined
      if (src && typeof src.setData === 'function') {
        src.setData(fc)
      }
    })
  }
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
    opts.beforeId,
  )
}

export function updateCountryBaseLayerTheme(map: MapLibreMap, isDark: boolean): void {
  if (!map.getLayer(COUNTRIES_LAYER_ID)) return
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-color', countryFill(isDark))
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-outline-color', countryOutline(isDark))
}
