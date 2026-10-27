'use client'

import { useEffect, type RefObject } from 'react'
import * as maplibregl from 'maplibre-gl'
import { type Map as MapLibreMap, type MapOptions } from 'maplibre-gl'
import type { FeatureCollection } from 'geojson'
import type { GeometryCollection, Topology } from 'topojson-specification'
import type { components } from '@/types/api.generated'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
export { MapPrewarm } from '@/components/PlotlyChart'

export const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
}

export function rafThrottle<TArgs extends unknown[]>(fn: (...args: TArgs) => void) {
  let queued = false
  let lastArgs: TArgs | null = null
  return (...args: TArgs) => {
    lastArgs = args
    if (queued) return
    queued = true
    requestAnimationFrame(() => {
      queued = false
      if (lastArgs) fn(...lastArgs)
      lastArgs = null
    })
  }
}

const COUNTRY_FILL_DARK = '#27272a'
const COUNTRY_FILL_LIGHT = '#e4e4e7'
const COUNTRY_OUTLINE_DARK = '#3f3f46'
const COUNTRY_OUTLINE_LIGHT = '#d4d4d8'

export function countryFill(isDark: boolean): string {
  return isDark ? COUNTRY_FILL_DARK : COUNTRY_FILL_LIGHT
}

export function countryOutline(isDark: boolean): string {
  return isDark ? COUNTRY_OUTLINE_DARK : COUNTRY_OUTLINE_LIGHT
}

type Resizable = { resize: () => unknown }

export function observeMapResize(container: Element, getMap: () => Resizable | null | undefined): () => void {
  const ro = new ResizeObserver(() => {
    getMap()?.resize()
  })
  ro.observe(container)
  return () => ro.disconnect()
}

export function useMapResize(
  mapRef: RefObject<Resizable | null>,
  containerRef: RefObject<Element | null>,
): void {
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    return observeMapResize(el, () => mapRef.current)
  }, [mapRef, containerRef])
}

export type MapFillColor = maplibregl.DataDrivenPropertyValueSpecification<string> | undefined

export function createMapAvailabilityStore() {
  let unavailable = false
  const listeners = new Set<() => void>()
  return {
    subscribe(listener: () => void) {
      listeners.add(listener)
      return () => {
        listeners.delete(listener)
      }
    },
    getSnapshot: () => unavailable,
    getServerSnapshot: () => false,
    reportUnavailable() {
      if (unavailable) return
      unavailable = true
      listeners.forEach((listener) => listener())
    },
  }
}

const WORLD_SOURCE_ID = 'world'
const COUNTRIES_LAYER_ID = 'countries'
const WORLD_TOPOJSON_URL = '/geo/world.topo.json'

type FillPaintExtras = {
  'fill-opacity'?: number
}

let worldGeoPromise: Promise<FeatureCollection> | null = null

async function loadWorldGeoJson(): Promise<FeatureCollection> {
  if (!worldGeoPromise) {
    worldGeoPromise = (async () => {
      const [topojsonClient, res] = await Promise.all([
        import('topojson-client'),
        fetch(WORLD_TOPOJSON_URL),
      ])

      const featureFn =
        topojsonClient.feature ||
        (topojsonClient as { default?: { feature?: typeof topojsonClient.feature } }).default?.feature
      if (!featureFn) {
        throw new Error('feature function not found in topojson-client exports!')
      }

      const topo = (await res.json()) as Topology<{ world: GeometryCollection }>
      const fc = featureFn(topo, topo.objects.world) as FeatureCollection
      return fc
    })().catch((err) => {
      console.error('[MapBaseLayer] loadWorldGeoJson: caught error:', err)
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
      const firstLayerAfterBg = layers?.find((l) => l.id !== 'background')
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

  void loadWorldGeoJson()
    .then((fc) => {
      if (!map.getSource(WORLD_SOURCE_ID)) {
        map.addSource(WORLD_SOURCE_ID, { type: 'geojson', data: fc })
      }
      if (!map.getLayer(COUNTRIES_LAYER_ID)) {
        const layers = typeof map.getStyle === 'function' ? map.getStyle()?.layers : undefined
        const firstLayerAfterBg = layers?.find((l) => l.id !== 'background')
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
    })
    .catch((err) => {
      console.error('[MapBaseLayer] Failed to load or set world geojson:', err)
    })
}

export function updateCountryBaseLayerTheme(map: MapLibreMap, isDark: boolean): void {
  if (!map.getLayer(COUNTRIES_LAYER_ID)) return
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-color', countryFill(isDark))
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-outline-color', countryOutline(isDark))
}

export interface BaseMapOptions {
  renderWorldCopies: boolean
  zoom: number
  cooperativeGestures: boolean
  background?: string
  globe?: boolean
  lockRotation?: boolean
  showCompass?: boolean
}

export function createBaseMap(
  maplibre: typeof import('maplibre-gl'),
  container: HTMLElement,
  opts: BaseMapOptions,
): MapLibreMap {
  const options = {
    container,
    renderWorldCopies: opts.renderWorldCopies,
    preserveDrawingBuffer: true,
    style: {
      version: 8,
      ...(opts.globe ? { projection: { type: 'globe' as const } } : {}),
      sources: {},
      layers: [
        {
          id: 'background',
          type: 'background',
          paint: { 'background-color': opts.background ?? 'transparent' },
        },
      ],
    },
    center: [0, 20],
    zoom: opts.zoom,
    cooperativeGestures: opts.cooperativeGestures,
    ...(opts.lockRotation ? { dragRotate: false, touchZoomRotate: false } : {}),
  } as MapOptions
  const map = new maplibre.Map(options)
  map.addControl(new maplibre.NavigationControl({ showCompass: opts.showCompass ?? true }), 'top-right')
  return map
}

export function removeMapTabStops(container: HTMLElement | null): void {
  container
    ?.querySelectorAll<HTMLElement>('a, button, [tabindex], canvas')
    .forEach((el) => el.setAttribute('tabindex', '-1'))
}

type NetworkHealth = components['schemas']['NetworkHealthResponse']
/** The slice of the network-health response the map renders. */
export type NetworkMapData = Partial<Pick<NetworkHealth, 'buckets' | 'map_buckets' | 'cities'>>
export type MapCity = components['schemas']['NetworkMapCity']

/** Per-city values behind a tooltip: the map-bucket cell fields, or a GeoJSON feature's properties. */
export type CityTooltipData = Record<string, unknown>

export interface TooltipInfo {
  clientX: number
  clientY: number
  city: string
  country?: string
  cityData: CityTooltipData
}

export const METRIC_OPTIONS = [
  { value: 'health_score', label: 'Health Score' },
  { value: 'rtt_med_us', label: 'Median RTT' },
  { value: 'avg_ploss', label: 'Packet Loss' },
  { value: 'error_pct', label: 'Error Rate' },
  { value: 'throughput_bps', label: 'Throughput' },
]

export const SPEED_OPTIONS = [
  { value: 1000, label: '1×' },
  { value: 500, label: '2×' },
  { value: 200, label: '5×' },
  { value: 100, label: '10×' },
]

export const STEP_OPTIONS = [
  { value: 1, label: '1 sec' },
  { value: 5, label: '5 sec' },
  { value: 10, label: '10 sec' },
  { value: 30, label: '30 sec' },
  { value: 60, label: '1 min' },
  { value: 300, label: '5 min' },
  { value: 900, label: '15 min' },
  { value: 1800, label: '30 min' },
  { value: 3600, label: '1 hr' },
  { value: 7200, label: '2 hr' },
  { value: 14400, label: '4 hr' },
]
