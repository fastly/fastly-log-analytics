'use client'

import React, { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { DashboardMapData } from '@/types/api'
import countryCodes from '@/lib/country-codes.json'

const alpha3ToAlpha2: Record<string, string> = {}
Object.entries(countryCodes as Record<string, string>).forEach(([a2, a3]) => {
  alpha3ToAlpha2[a3.toUpperCase()] = a2.toUpperCase()
})

interface ChoroplethMapProps {
  data: DashboardMapData[]
  className?: string
  onCountryClick?: (country: string) => void
}

const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'United Kingdom': 'United Kingdom of Great Britain and Northern Ireland',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
  // Fastly usually returns plain names like 'United States',
  // GeoJSON from johan/world.geo.json uses full names for some.
}

interface TooltipState {
  x: number
  y: number
  name: string
  count: number
}

// Instantiate Intl formatters ONCE outside the render loop
const regionNames = typeof Intl !== 'undefined' ? new Intl.DisplayNames(['en'], { type: 'region' }) : null;

/**
 * rAF-throttle a function so it fires at most once per animation frame.
 * MapLibre's per-layer mousemove fires on every native mousemove (60-120
 * Hz on a trackpad) and each call walks the feature index + triggers a
 * React render via setTooltip. Coalescing to one call per frame keeps
 * the latest position and discards intermediates.
 */
function rafThrottle<TArgs extends any[]>(fn: (...args: TArgs) => void) {
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

const getCountryName = (code: string) => {
  if (!code) return code
  try {
    if (regionNames && code.length === 2) {
      return regionNames.of(code.toUpperCase()) || code
    }
    return code
  } catch {
    return code
  }
}

export const ChoroplethMap = React.memo(function ChoroplethMap({ data, className = '', onCountryClick }: ChoroplethMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const { theme } = useTheme()

  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const dataMapRef = useRef<Map<string, number>>(new Map())
  // Reverse lookup: GeoJSON-feature name → alpha-2 country code. Built from
  // the data array (whose .country IS the alpha-2 code), so it stays in sync
  // with whatever the backend actually returns. Avoids depending on the
  // GeoJSON feature id (MapLibre can drop string ids in click events) or on
  // country-codes.json being complete (it has 168 codes vs 180 features).
  const nameToCodeRef = useRef<Map<string, string>>(new Map())
  const onCountryClickRef = useRef(onCountryClick)
  useEffect(() => { onCountryClickRef.current = onCountryClick }, [onCountryClick])

  // ... (map init useEffect stays the same) ...

  useEffect(() => {
    if (!mapContainer.current) return

    if (!map.current) {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
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
        zoom: 0.5,
        dragRotate: false,
        touchZoomRotate: false
      })

      map.current.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')

      map.current.on('load', () => {
        if (!map.current) return

        map.current.addSource('world', {
          type: 'geojson',
          data: '/geo/world.geojson'
        })

        map.current.addLayer({
          id: 'countries',
          type: 'fill',
          source: 'world',
          paint: {
            'fill-color': theme === 'dark' ? '#27272a' : '#e4e4e7',
            'fill-outline-color': theme === 'dark' ? '#3f3f46' : '#d4d4d8'
          }
        })

        // Hover events — rAF-throttled to one update per frame.
        map.current.on('mousemove', 'countries', rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
          if (e.features && e.features.length > 0) {
            const feature = e.features[0]
            const name = feature.properties?.name
            const count = dataMapRef.current.get(name) || 0

            setTooltip({
              x: e.point.x,
              y: e.point.y,
              name,
              count
            })
            if (map.current) map.current.getCanvas().style.cursor = 'pointer'
          }
        }))

        map.current.on('mouseleave', 'countries', () => {
          setTooltip(null)
          if (map.current) map.current.getCanvas().style.cursor = ''
        })

        map.current.on('click', 'countries', (e) => {
          if (e.features && e.features.length > 0) {
            const feature = e.features[0]
            const name = feature.properties.name as string
            // Prefer the data-derived name→code map (always alpha-2 matching
            // what the backend filter expects); fall back to the GeoJSON
            // feature id's alpha-3 lookup, then to the name as a last resort.
            const id = feature.id as string | undefined
            const code = nameToCodeRef.current.get(name)
              || (id ? alpha3ToAlpha2[id.toUpperCase()] : null)
            onCountryClickRef.current?.(code || name)
          }
        })
      })
    }

    return () => {
      map.current?.remove()
      map.current = null
    }
  }, [theme])

  useEffect(() => {
    if (!map.current || !data) return

    // Update data map for hover lookups + reverse map for click-to-code.
    const newDataMap = new Map<string, number>()
    const newNameToCode = new Map<string, string>()
    data.forEach(d => {
      const englishName = getCountryName(d.country)
      const countryName = NORMALIZE_COUNTRY[englishName] || englishName
      newDataMap.set(countryName, d.count)
      if (d.country) newNameToCode.set(countryName, d.country.toUpperCase())
    })
    dataMapRef.current = newDataMap
    nameToCodeRef.current = newNameToCode

    const updateData = () => {
      if (!map.current?.getLayer('countries')) {
        // Layer not added yet — the init effect's 'load' handler adds
        // it. Listen for 'styledata' (fires whenever layers/sources
        // change) and retry. Previously this used setTimeout(100ms)
        // polling, which added 100-300ms of artificial latency to the
        // first paint when data arrived before the map's 'load' event.
        // 'styledata' fires synchronously after addLayer() so this
        // path resolves with zero polling delay.
        const onStyleData = () => {
          if (map.current?.getLayer('countries')) {
            map.current.off('styledata', onStyleData)
            updateData()
          }
        }
        map.current?.on('styledata', onStyleData)
        return
      }

      if (!data.length) {
        map.current.setPaintProperty('countries', 'fill-color', theme === 'dark' ? '#27272a' : '#e4e4e7')
        return
      }

      const max = Math.max(...data.map(d => d.count))
      const matchExpression: any[] = ['match', ['get', 'name']]

      data.forEach(d => {
        const intensity = 0.2 + (d.count / max) * 0.8
        const englishName = getCountryName(d.country)
        const countryName = NORMALIZE_COUNTRY[englishName] || englishName
        matchExpression.push(countryName)
        matchExpression.push(`rgba(59, 130, 246, ${intensity})`)
      })

      matchExpression.push(theme === 'dark' ? '#27272a' : '#e4e4e7')
      map.current.setPaintProperty('countries', 'fill-color', matchExpression)
    }

    if (map.current.isStyleLoaded()) {
      updateData()
    } else {
      map.current.once('load', updateData)
    }

  }, [data, theme])

  const maxCount = data.length > 0 ? Math.max(...data.map(d => d.count)) : 0

  useEffect(() => {
    // MapLibre's containerSize is captured at construction time and isn't
    // re-measured automatically when the parent layout changes (a flex
    // child grown by sibling content, a card collapsing/expanding, etc.).
    // ResizeObserver wakes us up on any container size change and calls
    // map.resize() so the canvas re-sizes immediately instead of waiting
    // for a window resize or a forced re-mount.
    if (!mapContainer.current) return
    const el = mapContainer.current
    const ro = new ResizeObserver(() => map.current?.resize())
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  return (
    <div className={`relative min-h-[300px] w-full h-full rounded-lg overflow-hidden bg-background ${className}`}>
      <div ref={mapContainer} className="absolute inset-0 w-full h-full" />

      {/* Tooltip */}
      {tooltip && (
        <div
          className="absolute z-50 pointer-events-none bg-popover/95 backdrop-blur-sm border shadow-lg rounded-md px-3 py-2 text-sm transition-opacity"
          style={{
            left: Math.min(tooltip.x + 15, (mapContainer.current?.clientWidth || 500) - 160),
            top: Math.min(tooltip.y + 15, (mapContainer.current?.clientHeight || 300) - 80)
          }}
        >
          <div className="font-semibold text-foreground">{tooltip.name}</div>
          <div className="text-muted-foreground">{tooltip.count.toLocaleString()} requests</div>
        </div>
      )}

      {/* Legend */}
      {data.length > 0 && (
        <div className="absolute bottom-4 left-4 bg-background/95 backdrop-blur-sm border rounded-md p-3 shadow-md text-xs pointer-events-none z-10">
          <div className="font-medium text-foreground mb-2">Traffic Intensity</div>
          <div className="flex items-center gap-3">
            <span className="text-muted-foreground font-medium">0</span>
            <div className="w-32 h-2.5 rounded-full bg-gradient-to-r from-[rgba(59,130,246,0.2)] to-[rgba(59,130,246,1)] ring-1 ring-black/5 dark:ring-white/10" />
            <span className="text-muted-foreground font-medium">
              {maxCount >= 1000000 ? (maxCount/1000000).toFixed(1) + 'M' : maxCount >= 1000 ? (maxCount/1000).toFixed(1) + 'k' : maxCount}
            </span>
          </div>
        </div>
      )}
    </div>
  )
})
