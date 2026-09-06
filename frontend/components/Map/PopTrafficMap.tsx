'use client'

import React, { useEffect, useRef, useState, useMemo } from 'react'
import * as maplibregl from 'maplibre-gl'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
import { useTheme } from 'next-themes'
import { usePopGeoStore } from '@/stores/popGeoStore'
import { formatPopGeo, type PopGeo } from '@/lib/pop'
import { formatCompactCount } from '@/lib/format'
import { countryFill } from '@/components/Map/colors'
import { addCountryBaseLayer } from '@/components/Map/baseLayers'

type CoordTree = number | CoordTree[]

function geoBbox(geometry: { coordinates: CoordTree[] }): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  const processCoord = ([x, y]: [number, number]) => {
    if (x < minX) minX = x
    if (y < minY) minY = y
    if (x > maxX) maxX = x
    if (y > maxY) maxY = y
  }
  const walk = (coords: CoordTree[]) => {
    if (typeof coords[0] === 'number') { processCoord(coords as unknown as [number, number]); return }
    for (const c of coords) walk(c as CoordTree[])
  }
  walk(geometry.coordinates)
  return [minX, minY, maxX, maxY]
}

const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
}

interface PopTrafficMapProps {
  allPops: Record<string, { r: number; e: number }>
  className?: string
}

interface TooltipState {
  x: number
  y: number
  code: string
  geo: PopGeo | undefined
  requests: number
  errorRate: number
}

export const PopTrafficMap = React.memo(function PopTrafficMap({ allPops, className = '' }: PopTrafficMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const { theme } = useTheme()
  const geoMap = usePopGeoStore((s) => s.map)
  const locations = usePopGeoStore((s) => s.locations)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [mapError, setMapError] = useState(false)
  const [zoomed, setZoomed] = useState(false)
  const geoMapRef = useRef(geoMap)
  useEffect(() => { geoMapRef.current = geoMap }, [geoMap])

  const popEntries = useMemo(() => {
    const entries: Array<{
      code: string
      lat: number
      lon: number
      requests: number
      errors: number
      errorRate: number
      country: string
    }> = []
    for (const [code, [lat, lon]] of Object.entries(locations)) {
      const stats = allPops[code] ?? allPops[code.toLowerCase()]
      const requests = stats?.r ?? 0
      const errors = stats?.e ?? 0
      const errorRate = requests > 0 ? errors / requests : 0
      const geo = geoMap[code]
      entries.push({
        code,
        lat,
        lon,
        requests,
        errors,
        errorRate,
        country: geo?.country?.toUpperCase() ?? '',
      })
    }
    return entries
  }, [allPops, locations, geoMap])

  const countryAgg = useMemo(() => {
    const agg: Record<string, number> = {}
    for (const e of popEntries) {
      if (e.country) {
        agg[e.country] = (agg[e.country] ?? 0) + e.requests
      }
    }
    return agg
  }, [popEntries])

  const popGeoJson = useMemo(() => {
    const maxReq = Math.max(1, ...popEntries.map((e) => e.requests))
    return {
      type: 'FeatureCollection' as const,
      features: popEntries.map((e) => {
        const active = e.requests > 0
        return {
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [e.lon, e.lat] },
          properties: {
            code: e.code,
            requests: e.requests,
            errors: e.errors,
            errorRate: e.errorRate,
            radius: active ? 4 + Math.log10(e.requests) * 3 : 3,
            color: active ? (e.errorRate > 0.05 ? '#ef4444' : '#3b82f6') : '#9ca3af',
            opacity: active ? 0.3 + (e.requests / maxReq) * 0.6 : 0.3,
          },
        }
      }),
    }
  }, [popEntries])

  useEffect(() => {
    if (!mapContainer.current) return

    if (!map.current) {
      try {
        map.current = new maplibregl.Map({
          container: mapContainer.current,
          renderWorldCopies: false,
          preserveDrawingBuffer: true,
          style: {
            version: 8,
            sources: {},
            layers: [
              {
                id: 'background',
                type: 'background',
                paint: { 'background-color': theme === 'dark' ? '#1a1b23' : '#f0f4fa' },
              },
            ],
          },
          center: [0, 20],
          zoom: 0.5,
          dragRotate: false,
          touchZoomRotate: false,
          cooperativeGestures: true,
        } as maplibregl.MapOptions)

        map.current.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')

        requestAnimationFrame(() => {
          map.current?.resize()
        })

        map.current.on('load', () => {
          if (!map.current) return

          if (mapContainer.current) {
            mapContainer.current
              .querySelectorAll<HTMLElement>('a, button, [tabindex], canvas')
              .forEach((el) => el.setAttribute('tabindex', '-1'))
          }

          addCountryBaseLayer(map.current, { isDark: theme === 'dark' })

          map.current.addSource('pop-markers', {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: [] },
          })

          map.current.addLayer({
            id: 'pop-circles',
            type: 'circle',
            source: 'pop-markers',
            paint: {
              'circle-radius': ['get', 'radius'],
              'circle-color': ['get', 'color'],
              'circle-opacity': ['get', 'opacity'],
              'circle-stroke-width': 1,
              'circle-stroke-color': 'rgba(255,255,255,0.4)',
            },
          })

          map.current.on('click', 'countries', (e) => {
            if (!map.current || !e.features?.length) return
            const feature = e.features[0]
            if (feature.geometry.type === 'Polygon' || feature.geometry.type === 'MultiPolygon') {
              const bounds = geoBbox(feature.geometry)
              map.current.fitBounds(bounds, { padding: 40, maxZoom: 6 })
              setZoomed(true)
            }
          })

          map.current.on('mousemove', 'pop-circles', (e: maplibregl.MapLayerMouseEvent) => {
            if (!e.features?.length || !map.current) return
            const props = e.features[0].properties
            setTooltip({
              x: e.point.x,
              y: e.point.y,
              code: props?.code ?? '',
              geo: geoMapRef.current[(props?.code ?? '').toUpperCase()],
              requests: props?.requests ?? 0,
              errorRate: props?.errorRate ?? 0,
            })
            map.current.getCanvas().style.cursor = 'pointer'
          })

          map.current.on('mouseleave', 'pop-circles', () => {
            setTooltip(null)
            if (map.current) map.current.getCanvas().style.cursor = ''
          })

          map.current.on('mousemove', 'countries', () => {
            if (map.current) map.current.getCanvas().style.cursor = 'pointer'
          })

          map.current.on('mouseleave', 'countries', () => {
            if (map.current) map.current.getCanvas().style.cursor = ''
          })
        })
      } catch {
        map.current = null
        setMapError(true)
      }
    }

    return () => {
      map.current?.remove()
      map.current = null
    }
  }, [theme])

  useEffect(() => {
    if (!map.current) return

    const updateLayers = () => {
      if (!map.current) return

      const src = map.current.getSource('pop-markers') as maplibregl.GeoJSONSource | undefined
      if (src) {
        src.setData(popGeoJson as GeoJSON.FeatureCollection)
      }

      if (!map.current.getLayer('countries')) {
        const onStyleData = () => {
          if (map.current?.getLayer('countries')) {
            map.current.off('styledata', onStyleData)
            updateLayers()
          }
        }
        map.current.on('styledata', onStyleData)
        return
      }

      const countryEntries = Object.entries(countryAgg)
      if (countryEntries.length === 0) {
        map.current.setPaintProperty('countries', 'fill-color', countryFill(theme === 'dark'))
        return
      }

      const max = Math.max(...countryEntries.map(([, v]) => v))
      const matchExpression: (string | string[])[] = ['match', ['get', 'name']]

      for (const [cc, count] of countryEntries) {
        const intensity = 0.15 + (count / max) * 0.55
        let countryName: string
        try {
          const regionNames = new Intl.DisplayNames(['en'], { type: 'region' })
          countryName = regionNames.of(cc) ?? cc
        } catch {
          countryName = cc
        }
        const normalized = NORMALIZE_COUNTRY[countryName] ?? countryName
        matchExpression.push(normalized)
        matchExpression.push(`rgba(59, 130, 246, ${intensity})`)
      }

      matchExpression.push(countryFill(theme === 'dark'))
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      map.current.setPaintProperty('countries', 'fill-color', matchExpression as any)
    }

    updateLayers()
  }, [popGeoJson, countryAgg, theme])

  useEffect(() => {
    if (!mapContainer.current) return
    const el = mapContainer.current
    const ro = new ResizeObserver(() => map.current?.resize())
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const handleResetView = () => {
    if (!map.current) return
    map.current.flyTo({ center: [0, 20], zoom: 0.5 })
    setZoomed(false)
  }

  const maxCount = Math.max(1, ...popEntries.map((e) => e.requests))

  const sortedForA11y = useMemo(
    () => [...popEntries].sort((a, b) => b.requests - a.requests).slice(0, 50),
    [popEntries],
  )

  const ariaLabel = useMemo(() => {
    if (sortedForA11y.length === 0) {
      return 'World map of traffic by Fastly PoP. No data available.'
    }
    const top = sortedForA11y.slice(0, 5)
    const topPart = top.map((e) => `${e.code} ${formatCompactCount(e.requests)}`).join(', ')
    return `World map of traffic by Fastly PoP, showing top PoPs: ${topPart}.`
  }, [sortedForA11y])

  return (
    <div
      className={`relative min-h-[300px] w-full rounded-lg overflow-hidden bg-background ${className}`}
      role="img"
      aria-label={ariaLabel}
    >
      {mapError ? (
        <div
          className="absolute inset-0 flex items-center justify-center px-4 text-center text-xs text-muted-foreground"
          aria-hidden="true"
        >
          Interactive map unavailable in this browser. See the PoP table below.
        </div>
      ) : (
        <div ref={mapContainer} className="absolute inset-0 w-full h-full min-h-[300px]" aria-hidden="true" />
      )}

      <table className="sr-only">
        <caption>
          {sortedForA11y.length > 0
            ? `PoP traffic data — ${sortedForA11y.length} PoPs shown.`
            : 'PoP traffic data — no PoPs available.'}
        </caption>
        <thead>
          <tr>
            <th scope="col">Rank</th>
            <th scope="col">PoP</th>
            <th scope="col">Location</th>
            <th scope="col">Requests</th>
            <th scope="col">Error Rate</th>
          </tr>
        </thead>
        <tbody>
          {sortedForA11y.map((e, i) => (
            <tr key={e.code}>
              <td>{i + 1}</td>
              <td>{e.code}</td>
              <td>{formatPopGeo(geoMap[e.code])}</td>
              <td>{e.requests.toLocaleString()}</td>
              <td>{(e.errorRate * 100).toFixed(2)}%</td>
            </tr>
          ))}
        </tbody>
      </table>

      {zoomed && (
        <button
          type="button"
          onClick={handleResetView}
          className="absolute top-3 left-3 z-20 bg-background/95 backdrop-blur-sm border rounded-md px-3 py-1.5 text-xs font-medium shadow-md hover:bg-accent focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
        >
          Back to world view
        </button>
      )}

      {tooltip && (
        <div
          className="absolute z-50 pointer-events-none bg-popover/95 backdrop-blur-sm border shadow-lg rounded-md px-3 py-2 text-sm transition-opacity"
          style={{
            left: Math.min(tooltip.x + 15, (mapContainer.current?.clientWidth ?? 500) - 180),
            top: Math.min(tooltip.y + 15, (mapContainer.current?.clientHeight ?? 300) - 100),
          }}
        >
          <div className="font-semibold text-foreground font-mono">{tooltip.code}</div>
          {tooltip.geo && (
            <div className="text-muted-foreground text-xs">{formatPopGeo(tooltip.geo)}</div>
          )}
          <div className="mt-1 text-xs space-y-0.5">
            <div>
              <span className="text-muted-foreground">Requests:</span>{' '}
              <span className="tabular-nums">{tooltip.requests.toLocaleString()}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Error rate:</span>{' '}
              <span
                className={`tabular-nums ${tooltip.errorRate > 0.05 ? 'text-red-600 dark:text-red-400 font-medium' : tooltip.errorRate > 0.01 ? 'text-amber-600 dark:text-amber-400' : ''}`}
              >
                {(tooltip.errorRate * 100).toFixed(2)}%
              </span>
            </div>
          </div>
        </div>
      )}

      {popEntries.length > 0 && (
        <div className="absolute bottom-4 left-4 bg-background/95 backdrop-blur-sm border rounded-md p-3 shadow-md text-xs pointer-events-none z-10">
          <div className="font-medium text-foreground mb-2">PoP Traffic</div>
          <div className="flex items-center gap-3">
            <span className="text-muted-foreground font-medium">0</span>
            <div className="w-32 h-2.5 rounded-full bg-gradient-to-r from-[rgba(59,130,246,0.2)] to-[rgba(59,130,246,1)] ring-1 ring-black/5 dark:ring-white/10" />
            <span className="text-muted-foreground font-medium">{formatCompactCount(maxCount)}</span>
          </div>
          <div className="mt-1.5 flex items-center gap-2">
            <span className="inline-block h-2 w-2 rounded-full bg-red-500" />
            <span className="text-muted-foreground">&gt;5% errors</span>
          </div>
        </div>
      )}
    </div>
  )
})
