'use client'

import React, { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { DashboardMapData } from '@/types/api'

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

        // Hover events
        map.current.on('mousemove', 'countries', (e) => {
          if (e.features && e.features.length > 0) {
            const feature = e.features[0]
            const name = feature.properties.name
            const count = dataMapRef.current.get(name) || 0
            
            setTooltip({
              x: e.point.x,
              y: e.point.y,
              name,
              count
            })
            if (map.current) map.current.getCanvas().style.cursor = 'pointer'
          }
        })

        map.current.on('mouseleave', 'countries', () => {
          setTooltip(null)
          if (map.current) map.current.getCanvas().style.cursor = ''
        })

        map.current.on('click', 'countries', (e) => {
          if (e.features && e.features.length > 0) {
            const name = e.features[0].properties.name as string
            onCountryClickRef.current?.(name)
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

    // Update data map for hover lookups
    const newDataMap = new Map<string, number>()
    data.forEach(d => {
      const englishName = getCountryName(d.country)
      const countryName = NORMALIZE_COUNTRY[englishName] || englishName
      newDataMap.set(countryName, d.count)
    })
    dataMapRef.current = newDataMap

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
    // A trick to ensure MapLibre accurately captures its container dimensions once mounted.
    const t = setTimeout(() => map.current?.resize(), 50)
    return () => clearTimeout(t)
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
