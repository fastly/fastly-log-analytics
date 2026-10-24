'use client'

import { useEffect, useRef, MutableRefObject } from 'react'
import * as maplibregl from 'maplibre-gl'
import countryMapData from '@/lib/country-codes.json'
import type { components } from '@/types/api.generated'
import type { CityTooltipData, TooltipInfo } from './OverlayLayer'

type NetworkHealth = components['schemas']['NetworkHealthResponse']

/** The slice of the network-health response the map renders. */
export type NetworkMapData = Partial<Pick<NetworkHealth, 'buckets' | 'map_buckets' | 'cities'>>
type MapCity = components['schemas']['NetworkMapCity']

const A2_TO_A3: Record<string, string> = countryMapData

/** One city's value for a metric id (``health_score``, ``rtt_med_us`` …); ``null`` when absent. */
function cityMetric(c: MapCity, metric: string): number | null {
  const v = (c as Record<string, unknown>)[metric]
  return typeof v === 'number' ? v : null
}

const asCityString = (v: unknown): string => (typeof v === 'string' ? v : '')

export function getScoreColor(val: number | null, metric: string): string {
  if (val == null) return 'transparent'

  if (metric === 'health_score') {
    if (val >= 90) return '#22c55e'
    if (val >= 70) return '#eab308'
    if (val >= 50) return '#f97316'
    return CHART_COLORS.danger
  }

  if (metric === 'throughput_bps') {
    if (val >= 100_000_000) return '#22c55e'
    if (val >= 10_000_000) return '#eab308'
    if (val >= 1_000_000) return '#f97316'
    return CHART_COLORS.danger
  }

  if (metric === 'rtt_med_us') {
    if (val <= 50_000) return '#22c55e'
    if (val <= 150_000) return '#eab308'
    if (val <= 300_000) return '#f97316'
    return CHART_COLORS.danger
  }
  if (metric === 'avg_ploss') {
    if (val <= 0.01) return '#22c55e'
    if (val <= 0.05) return '#eab308'
    if (val <= 0.10) return '#f97316'
    return CHART_COLORS.danger
  }
  if (metric === 'error_pct') {
    if (val <= 1) return '#22c55e'
    if (val <= 5) return '#eab308'
    if (val <= 10) return '#f97316'
    return CHART_COLORS.danger
  }

  return CHART_COLORS.info
}

interface UseMapInitArgs {
  mapContainer: MutableRefObject<HTMLDivElement | null>
  isDark: boolean
  isDarkRef: MutableRefObject<boolean>
  dmaDataRef: MutableRefObject<Record<number, CityTooltipData>>
  setTooltip: (t: TooltipInfo | null) => void
  // Called when MapLibre's constructor throws (WebGL unavailable — headless /
  // locked-down browser). Lets the caller degrade to a placeholder instead of
  // letting the throw propagate into the route error boundary.
  onMapError?: () => void
}

import { rafThrottle, countryFill, observeMapResize } from '@/components/Map/shared'
import { addCountryBaseLayer, createBaseMap, updateCountryBaseLayerTheme, type MapFillColor } from '@/components/Map/baseLayers'
import { CHART_COLORS } from '@/lib/chart-builders'

/**
 * Initializes the MapLibre instance, adds sources/layers, and wires hover
 * handlers for the city-scatter and dma-fill layers. Cleans up on unmount or
 * when `isDark` changes (so the map style can be rebuilt for the new theme).
 */
export function useMapInit({
  mapContainer,
  isDark,
  isDarkRef,
  dmaDataRef,
  setTooltip,
  onMapError,
}: UseMapInitArgs): MutableRefObject<maplibregl.Map | null> {
  // The hook owns the map instance; consumers read it (useMapData) but never assign it.
  const map = useRef<maplibregl.Map | null>(null)
  useEffect(() => {
    if (!mapContainer.current) return
    let stopResizeObserver: (() => void) | null = null

    if (!map.current) {
      try {
      map.current = createBaseMap(maplibregl, mapContainer.current, {
        renderWorldCopies: false,
        zoom: 1,
        // Don't hijack page scroll: a plain mousewheel scrolls the PAGE; only
        // ⌘/Ctrl + wheel (or pinch / the +/- buttons) zooms the map. The map
        // sits mid-page, so bare-wheel zoom traps the scroll. (cooperative gestures)
        cooperativeGestures: true,
      })

      map.current.on('error', (e) => {
        console.error("[NetworkMap:map] MapLibre error event:", e)
      })

      stopResizeObserver = observeMapResize(mapContainer.current, () => map.current)

      map.current.on('load', () => {
        if (!map.current) return

        map.current.addSource('dma', { type: 'geojson', data: '/geo/dma.geojson' })
        map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        addCountryBaseLayer(map.current, {
          isDark: isDarkRef.current,
          extraPaint: { 'fill-opacity': 0.8 },
        })

        map.current.addLayer({
          id: 'dma-fill',
          type: 'fill',
          source: 'dma',
          paint: { 'fill-opacity': 0.7, 'fill-color': 'transparent' }
        })

        // City scatter — only for cities without DMA polygon coverage
        map.current.addLayer({
          id: 'city-scatter',
          type: 'circle',
          source: 'heatmap',
          paint: {
            'circle-radius': ['get', 'radius'],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.8,
            'circle-stroke-width': 1,
            'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff'
          }
        })

        // ── Hover: city scatter dots ─────────────────────────────────────────
        map.current.on('mouseenter', 'city-scatter', (e) => {
          if (!e.features?.length || !map.current) return
          map.current.getCanvas().style.cursor = 'pointer'
          const props: CityTooltipData = e.features[0].properties ?? {}
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: asCityString(props.city),
            country: asCityString(props.country) || undefined,
            cityData: props,
          })
        })

        map.current.on('mousemove', 'city-scatter', rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
          if (!e.features?.length) return
          const props: CityTooltipData = e.features[0].properties ?? {}
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: asCityString(props.city),
            country: asCityString(props.country) || undefined,
            cityData: props,
          })
        }))

        map.current.on('mouseleave', 'city-scatter', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })

        // ── Hover: DMA filled regions ────────────────────────────────────────
        map.current.on('mouseenter', 'dma-fill', (e) => {
          if (!e.features?.length || !map.current) return
          // dma_code may come through as a string — normalise to number to match dmaDataRef keys
          const code = Number(e.features[0].properties?.dma_code)
          const cityData = dmaDataRef.current[code]
          if (!cityData) return
          map.current.getCanvas().style.cursor = 'pointer'
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: asCityString(cityData.city),
            country: asCityString(cityData.country) || undefined,
            cityData,
          })
        })

        map.current.on('mousemove', 'dma-fill', rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
          if (!e.features?.length) return
          const code = Number(e.features[0].properties?.dma_code)
          const cityData = dmaDataRef.current[code]
          if (!cityData) return
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: asCityString(cityData.city),
            country: asCityString(cityData.country) || undefined,
            cityData,
          })
        }))

        map.current.on('mouseleave', 'dma-fill', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })
      })
      } catch {
        // WebGL unavailable (headless / locked-down browser). Surface to the
        // caller so it can render its sr-only data table + a placeholder
        // instead of throwing into the route error boundary.
        map.current = null
        onMapError?.()
      }
    }
    return () => {
      setTooltip(null)
      stopResizeObserver?.()
      map.current?.remove()
      map.current = null
    }
  }, [dmaDataRef, isDarkRef, mapContainer, onMapError, setTooltip])

  useEffect(() => {
    if (!map.current) return
    updateCountryBaseLayerTheme(map.current, isDark)
    if (map.current.getLayer('city-scatter')) {
      map.current.setPaintProperty('city-scatter', 'circle-stroke-color', isDark ? '#18181b' : '#ffffff')
    }
  }, [isDark])
  return map
}

interface UseMapDataArgs {
  map: MutableRefObject<maplibregl.Map | null>
  dmaDataRef: MutableRefObject<Record<number, CityTooltipData>>
  data: NetworkMapData | null | undefined
  bucketIdx: number
  metric: string
  isDark: boolean
  setTooltip: (t: TooltipInfo | null) => void
}

/**
 * Pushes the current bucket's city data into the map's heatmap source and
 * recomputes per-country and per-DMA fill colors. Runs whenever bucketIdx,
 * data, metric, or theme changes.
 */
export function useMapData({
  map,
  dmaDataRef,
  data,
  bucketIdx,
  metric,
  isDark,
  setTooltip,
}: UseMapDataArgs) {
  useEffect(() => {
    if (!map.current || !data?.map_buckets) return
    const mapBuckets = data.map_buckets
    const cityEntries = data.cities ?? []

    let pendingStyleDataHandler: (() => void) | null = null

    const updateMap = () => {
      if (!map.current) return

      if (!map.current.getLayer('countries') || !map.current.getSource('heatmap')) {
        if (pendingStyleDataHandler) {
          map.current.off('styledata', pendingStyleDataHandler)
        }
        const onStyleData = () => {
          if (map.current?.getLayer('countries') && map.current?.getSource('heatmap')) {
            map.current.off('styledata', onStyleData)
            pendingStyleDataHandler = null
            updateMap()
          }
        }
        pendingStyleDataHandler = onStyleData
        map.current.on('styledata', onStyleData)
        return
      }

      setTooltip(null)

      const bucketData = mapBuckets[bucketIdx]
      if (!bucketData || !bucketData.cities) return

      type CityRec = { name: string; lat: number | null; lon: number | null }
      const resolveCity = (c: MapCity): CityRec => {
        const entry = c.city_idx >= 0 && c.city_idx < cityEntries.length ? cityEntries[c.city_idx] : undefined
        return { name: entry?.name ?? '', lat: entry?.lat ?? null, lon: entry?.lon ?? null }
      }

      const features: GeoJSON.Feature[] = []
      const dmaColors: Record<number, string> = {}
      const countryScores: Record<string, { sum: number, count: number }> = {}
      const nextDmaData: Record<number, CityTooltipData> = {}

    bucketData.cities.forEach((c) => {
      const val = cityMetric(c, metric)
      const reqs = c.reqs
      const color = getScoreColor(val, metric)
      const city = resolveCity(c)

      if (c.country) {
        const a3 = A2_TO_A3[c.country]
        if (a3 && val != null) {
          if (!countryScores[a3]) countryScores[a3] = { sum: 0, count: 0 }
          countryScores[a3].sum += val * reqs
          countryScores[a3].count += reqs
        }
      }

      if (c.metro_code) {
        // US DMA city — show as filled polygon, not a dot (avoid double-rendering).
        // Stash the resolved name onto the dma data so downstream tooltip reads
        // see ``city`` even though the wire shape carries ``city_idx``.
        dmaColors[Number(c.metro_code)] = color
        nextDmaData[Number(c.metro_code)] = { ...c, city: city.name }
      } else if (city.lat != null && city.lon != null) {
        // Non-DMA city (international or small US town) — show as circle dot
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [city.lon, city.lat] },
          properties: {
            city: city.name,
            country: c.country || '',
            color,
            radius: Math.max(3, Math.min(15, Math.log10(reqs + 1) * 2.5)),
            reqs,
            health_score: c.health_score,
            rtt_med_us: c.rtt_med_us,
            avg_ploss: c.avg_ploss,
            error_pct: c.error_pct,
          }
        })
      }
    })

    dmaDataRef.current = nextDmaData

    const matchCountry: unknown[] = ['match', ['to-string', ['id']]]
    Object.entries(countryScores).forEach(([a3, stats]) => {
      matchCountry.push(a3)
      matchCountry.push(getScoreColor(stats.sum / stats.count, metric))
    })
    matchCountry.push(countryFill(isDark))

    map.current.setPaintProperty(
      'countries',
      'fill-color',
      (Object.keys(countryScores).length > 0 ? matchCountry : countryFill(isDark)) as MapFillColor
    )

    const source = map.current.getSource('heatmap') as maplibregl.GeoJSONSource
    source?.setData({ type: 'FeatureCollection', features })

      if (map.current.getLayer('dma-fill')) {
        const dmaEntries = Object.entries(dmaColors)
        if (dmaEntries.length > 0) {
          const matchDma: unknown[] = ['match', ['get', 'dma_code']]
          dmaEntries.forEach(([code, color]) => { matchDma.push(Number(code)); matchDma.push(color) })
          matchDma.push('transparent')
          map.current.setPaintProperty('dma-fill', 'fill-color', matchDma as MapFillColor)
        } else {
          map.current.setPaintProperty('dma-fill', 'fill-color', 'transparent')
        }
      }
      if (typeof map.current.resize === 'function') {
        map.current.resize()
      }
    }

    updateMap()

    const currentMap = map.current
    return () => {
      if (pendingStyleDataHandler) {
        currentMap?.off('styledata', pendingStyleDataHandler)
        pendingStyleDataHandler = null
      }
    }
  }, [bucketIdx, data, metric, isDark, dmaDataRef, map, setTooltip])
}
