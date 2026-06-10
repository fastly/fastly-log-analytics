'use client'

import { useEffect, MutableRefObject } from 'react'
import maplibregl from 'maplibre-gl'
import countryMapData from '@/lib/country-codes.json'
import type { TooltipInfo } from './OverlayLayer'

const A2_TO_A3: Record<string, string> = countryMapData

export function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const utc = /[Z+\-]\d*$/.test(iso) ? iso : iso + 'Z'
  const d = new Date(utc)
  if (isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(d)
}

export function getScoreColor(val: number | null, metric: string): string {
  if (val == null) return 'transparent'

  if (metric === 'health_score') {
    if (val >= 90) return '#22c55e'
    if (val >= 70) return '#eab308'
    if (val >= 50) return '#f97316'
    return '#ef4444'
  }

  if (metric === 'throughput_bps') {
    if (val >= 100_000_000) return '#22c55e'
    if (val >= 10_000_000) return '#eab308'
    if (val >= 1_000_000) return '#f97316'
    return '#ef4444'
  }

  if (metric === 'rtt_med_us') {
    if (val <= 50_000) return '#22c55e'
    if (val <= 150_000) return '#eab308'
    if (val <= 300_000) return '#f97316'
    return '#ef4444'
  }
  if (metric === 'avg_ploss') {
    if (val <= 0.01) return '#22c55e'
    if (val <= 0.05) return '#eab308'
    if (val <= 0.10) return '#f97316'
    return '#ef4444'
  }
  if (metric === 'error_pct') {
    if (val <= 1) return '#22c55e'
    if (val <= 5) return '#eab308'
    if (val <= 10) return '#f97316'
    return '#ef4444'
  }

  return '#3b82f6'
}

interface UseMapInitArgs {
  mapContainer: MutableRefObject<HTMLDivElement | null>
  map: MutableRefObject<maplibregl.Map | null>
  isDark: boolean
  isDarkRef: MutableRefObject<boolean>
  dmaDataRef: MutableRefObject<Record<number, any>>
  setTooltip: (t: TooltipInfo | null) => void
}

/**
 * Initializes the MapLibre instance, adds sources/layers, and wires hover
 * handlers for the city-scatter and dma-fill layers. Cleans up on unmount or
 * when `isDark` changes (so the map style can be rebuilt for the new theme).
 */
export function useMapInit({
  mapContainer,
  map,
  isDark,
  isDarkRef,
  dmaDataRef,
  setTooltip,
}: UseMapInitArgs) {
  useEffect(() => {
    if (!mapContainer.current) return
    if (!map.current) {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        renderWorldCopies: false,
        style: {
          version: 8,
          sources: {},
          layers: [
            {
              id: 'background',
              type: 'background',
              paint: { 'background-color': 'transparent' }
            }
          ]
        },
        center: [0, 20],
        zoom: 1,
        interactive: true
      })
      map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

      map.current.on('load', () => {
        if (!map.current) return

        map.current.addSource('world', { type: 'geojson', data: '/geo/world.geojson' })
        map.current.addSource('dma', { type: 'geojson', data: '/geo/dma.geojson' })
        map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        map.current.addLayer({
          id: 'countries',
          type: 'fill',
          source: 'world',
          paint: {
            'fill-color': isDarkRef.current ? '#27272a' : '#e4e4e7',
            'fill-outline-color': isDarkRef.current ? '#3f3f46' : '#d4d4d8',
            'fill-opacity': 0.8
          }
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
          const props = e.features[0].properties as Record<string, any>
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: props.city || '',
            country: props.country || undefined,
            cityData: props,
          })
        })

        map.current.on('mousemove', 'city-scatter', (e) => {
          if (!e.features?.length) return
          const props = e.features[0].properties as Record<string, any>
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: props.city || '',
            country: props.country || undefined,
            cityData: props,
          })
        })

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
            city: cityData.city || '',
            country: cityData.country || undefined,
            cityData,
          })
        })

        map.current.on('mousemove', 'dma-fill', (e) => {
          if (!e.features?.length) return
          const code = Number(e.features[0].properties?.dma_code)
          const cityData = dmaDataRef.current[code]
          if (!cityData) return
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: cityData.city || '',
            country: cityData.country || undefined,
            cityData,
          })
        })

        map.current.on('mouseleave', 'dma-fill', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })
      })
    }
    return () => {
      setTooltip(null)
      map.current?.remove()
      map.current = null
    }
  }, [isDark])
}

interface UseMapDataArgs {
  map: MutableRefObject<maplibregl.Map | null>
  dmaDataRef: MutableRefObject<Record<number, any>>
  data: any
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
    if (!map.current || !map.current.isStyleLoaded() || !data?.map_buckets) return

    setTooltip(null)

    const bucketData = data.map_buckets[bucketIdx]
    if (!bucketData || !bucketData.cities) return

    const features: any[] = []
    const dmaColors: Record<number, string> = {}
    const countryScores: Record<string, { sum: number, count: number }> = {}
    const nextDmaData: Record<number, any> = {}

    bucketData.cities.forEach((c: any) => {
      const val = metric === 'health_score' ? c.health_score : c[metric]
      const reqs = c.reqs
      const color = getScoreColor(val, metric)

      if (c.country) {
        const a3 = A2_TO_A3[c.country]
        if (a3 && val != null) {
          if (!countryScores[a3]) countryScores[a3] = { sum: 0, count: 0 }
          countryScores[a3].sum += val * reqs
          countryScores[a3].count += reqs
        }
      }

      if (c.metro_code) {
        // US DMA city — show as filled polygon, not a dot (avoid double-rendering)
        dmaColors[Number(c.metro_code)] = color
        nextDmaData[Number(c.metro_code)] = c
      } else if (c.lat != null && c.lon != null) {
        // Non-DMA city (international or small US town) — show as circle dot
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [c.lon, c.lat] },
          properties: {
            city: c.city || '',
            country: c.country || '',
            color,
            radius: Math.max(3, Math.min(15, Math.log10(reqs + 1) * 2.5)),
            reqs,
            health_score: c.health_score,
            rtt_med_us: c.rtt_med_us,
            avg_ploss: c.avg_ploss,
            error_pct: c.error_pct,
            throughput_bps: c.throughput_bps,
          }
        })
      }
    })

    dmaDataRef.current = nextDmaData

    const matchCountry: any[] = ['match', ['id']]
    Object.entries(countryScores).forEach(([a3, stats]) => {
      matchCountry.push(a3)
      matchCountry.push(getScoreColor(stats.sum / stats.count, metric))
    })
    matchCountry.push(isDark ? '#27272a' : '#e4e4e7')

    map.current.setPaintProperty(
      'countries',
      'fill-color',
      Object.keys(countryScores).length > 0 ? matchCountry : (isDark ? '#27272a' : '#e4e4e7')
    )

    const source = map.current.getSource('heatmap') as maplibregl.GeoJSONSource
    source?.setData({ type: 'FeatureCollection', features })

    if (map.current.getLayer('dma-fill')) {
      const dmaEntries = Object.entries(dmaColors)
      if (dmaEntries.length > 0) {
        const matchDma: any[] = ['match', ['get', 'dma_code']]
        dmaEntries.forEach(([code, color]) => { matchDma.push(Number(code)); matchDma.push(color) })
        matchDma.push('transparent')
        map.current.setPaintProperty('dma-fill', 'fill-color', matchDma)
      } else {
        map.current.setPaintProperty('dma-fill', 'fill-color', 'transparent')
      }
    }

  }, [bucketIdx, data, metric, isDark])
}
