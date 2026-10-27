'use client'

import { useEffect, useRef, type MutableRefObject } from 'react'
import * as maplibregl from 'maplibre-gl'
import countryMapData from '@/lib/country-codes.json'
import { CHART_COLORS } from '@/lib/utils'
import {
  rafThrottle,
  countryFill,
  observeMapResize,
  type MapFillColor,
  addCountryBaseLayer,
  updateCountryBaseLayerTheme,
  createBaseMap,
  type NetworkMapData,
  type MapCity,
  type CityTooltipData,
  type TooltipInfo,
} from './types'

const A2_TO_A3: Record<string, string> = countryMapData

function cityMetric(c: MapCity, metric: string): number | null {
  const v = (c as Record<string, unknown>)[metric]
  return typeof v === 'number' ? v : null
}

const asCityString = (v: unknown): string => (typeof v === 'string' ? v : '')

const SCORE_BANDS: Record<string, { highIsGood: boolean; t: [number, number, number] }> = {
  health_score: { highIsGood: true, t: [90, 70, 50] },
  throughput_bps: { highIsGood: true, t: [100_000_000, 10_000_000, 1_000_000] },
  rtt_med_us: { highIsGood: false, t: [50_000, 150_000, 300_000] },
  avg_ploss: { highIsGood: false, t: [0.01, 0.05, 0.10] },
  error_pct: { highIsGood: false, t: [1, 5, 10] },
}

export function getScoreColor(val: number | null, metric: string): string {
  if (val == null) return 'transparent'
  const band = SCORE_BANDS[metric]
  if (!band) return CHART_COLORS.info
  const [g, y, o] = band.t
  if (band.highIsGood) {
    if (val >= g) return '#22c55e'
    if (val >= y) return '#eab308'
    if (val >= o) return '#f97316'
    return CHART_COLORS.danger
  }
  if (val <= g) return '#22c55e'
  if (val <= y) return '#eab308'
  if (val <= o) return '#f97316'
  return CHART_COLORS.danger
}

export interface UseMapInitArgs {
  mapContainer: MutableRefObject<HTMLDivElement | null>
  isDark: boolean
  isDarkRef: MutableRefObject<boolean>
  dmaDataRef: MutableRefObject<Record<number, CityTooltipData>>
  setTooltip: (t: TooltipInfo | null) => void
  onMapError?: () => void
}

/**
 * Initializes the MapLibre instance, adds sources/layers, and wires hover
 * handlers for the city-scatter and dma-fill layers.
 */
export function useMapInit({
  mapContainer,
  isDark,
  isDarkRef,
  dmaDataRef,
  setTooltip,
  onMapError,
}: UseMapInitArgs): MutableRefObject<maplibregl.Map | null> {
  const map = useRef<maplibregl.Map | null>(null)
  useEffect(() => {
    if (!mapContainer.current) return
    let stopResizeObserver: (() => void) | null = null

    if (!map.current) {
      try {
        map.current = createBaseMap(maplibregl, mapContainer.current, {
          renderWorldCopies: false,
          zoom: 1,
          cooperativeGestures: true,
        })
        map.current.on('error', (e) => {
          console.error('[NetworkMap:map] MapLibre error event:', e)
        })
        stopResizeObserver = observeMapResize(mapContainer.current, () => map.current)

        map.current.on('load', () => {
          if (!map.current) return
          map.current.addSource('dma', { type: 'geojson', data: '/geo/dma.geojson' })
          map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
          addCountryBaseLayer(map.current, { isDark: isDarkRef.current, extraPaint: { 'fill-opacity': 0.8 } })
          map.current.addLayer({
            id: 'dma-fill',
            type: 'fill',
            source: 'dma',
            paint: { 'fill-opacity': 0.7, 'fill-color': 'transparent' },
          })
          map.current.addLayer({
            id: 'city-scatter',
            type: 'circle',
            source: 'heatmap',
            paint: {
              'circle-radius': ['get', 'radius'],
              'circle-color': ['get', 'color'],
              'circle-opacity': 0.8,
              'circle-stroke-width': 1,
              'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff',
            },
          })

          const pushTooltip = (e: maplibregl.MapLayerMouseEvent, cityData: CityTooltipData) => {
            setTooltip({
              clientX: e.originalEvent.clientX,
              clientY: e.originalEvent.clientY,
              city: asCityString(cityData.city),
              country: asCityString(cityData.country) || undefined,
              cityData,
            })
          }

          const bindHover = (
            layer: string,
            resolveData: (f: maplibregl.MapGeoJSONFeature) => CityTooltipData | undefined,
          ) => {
            if (!map.current) return
            map.current.on('mouseenter', layer, (e) => {
              if (!e.features?.length || !map.current) return
              const cityData = resolveData(e.features[0])
              if (!cityData) return
              map.current.getCanvas().style.cursor = 'pointer'
              pushTooltip(e, cityData)
            })
            map.current.on(
              'mousemove',
              layer,
              rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
                if (!e.features?.length) return
                const cityData = resolveData(e.features[0])
                if (cityData) pushTooltip(e, cityData)
              }),
            )
            map.current.on('mouseleave', layer, () => {
              if (map.current) map.current.getCanvas().style.cursor = ''
              setTooltip(null)
            })
          }

          bindHover('city-scatter', (f) => f.properties ?? {})
          bindHover('dma-fill', (f) => dmaDataRef.current[Number(f.properties?.dma_code)])
        })
      } catch {
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

export interface UseMapDataArgs {
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
 * recomputes per-country and per-DMA fill colors.
 */
export function useMapData({ map, dmaDataRef, data, bucketIdx, metric, isDark, setTooltip }: UseMapDataArgs) {
  useEffect(() => {
    if (!map.current || !data?.map_buckets) return
    const mapBuckets = data.map_buckets
    const cityEntries = data.cities ?? []
    let pendingStyleDataHandler: (() => void) | null = null

    const updateMap = () => {
      if (!map.current) return
      if (!map.current.getLayer('countries') || !map.current.getSource('heatmap')) {
        if (pendingStyleDataHandler) map.current.off('styledata', pendingStyleDataHandler)
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
      if (!bucketData?.cities) return

      const resolveCity = (c: MapCity) => {
        const entry = c.city_idx >= 0 && c.city_idx < cityEntries.length ? cityEntries[c.city_idx] : undefined
        return { name: entry?.name ?? '', lat: entry?.lat ?? null, lon: entry?.lon ?? null }
      }

      const features: GeoJSON.Feature[] = []
      const dmaColors: Record<number, string> = {}
      const countryScores: Record<string, { sum: number; count: number }> = {}
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
          dmaColors[Number(c.metro_code)] = color
          nextDmaData[Number(c.metro_code)] = { ...c, city: city.name }
        } else if (city.lat != null && city.lon != null) {
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
            },
          })
        }
      })

      dmaDataRef.current = nextDmaData

      const matchCountry: unknown[] = ['match', ['to-string', ['id']]]
      Object.entries(countryScores).forEach(([a3, stats]) => {
        matchCountry.push(a3, getScoreColor(stats.sum / stats.count, metric))
      })
      matchCountry.push(countryFill(isDark))

      map.current.setPaintProperty(
        'countries',
        'fill-color',
        (Object.keys(countryScores).length > 0 ? matchCountry : countryFill(isDark)) as MapFillColor,
      )

      const source = map.current.getSource('heatmap') as maplibregl.GeoJSONSource
      source?.setData({ type: 'FeatureCollection', features })

      if (map.current.getLayer('dma-fill')) {
        const dmaEntries = Object.entries(dmaColors)
        if (dmaEntries.length > 0) {
          const matchDma: unknown[] = ['match', ['get', 'dma_code']]
          dmaEntries.forEach(([code, color]) => {
            matchDma.push(Number(code), color)
          })
          matchDma.push('transparent')
          map.current.setPaintProperty('dma-fill', 'fill-color', matchDma as MapFillColor)
        } else {
          map.current.setPaintProperty('dma-fill', 'fill-color', 'transparent')
        }
      }
      if (typeof map.current.resize === 'function') map.current.resize()
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

export const MapLayer = { getScoreColor, useMapInit, useMapData }
