'use client'

import React, { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter
} from '@/components/ui/dialog'
import { useTheme } from 'next-themes'
import { Info, Zap, Globe } from 'lucide-react'
import { cn } from '@/lib/utils'
import { ImpossibleDistanceData } from './types'
import { addCountryBaseLayer, updateCountryBaseLayerTheme } from '@/components/Map/baseLayers'

function PhysicsMap({ data, isDark }: { data: ImpossibleDistanceData; isDark: boolean }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [mapError, setMapError] = useState<string | null>(null)

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  useEffect(() => {
    if (!mapContainer.current || !data || !hasCoords) return

    if (!map.current) {
      try {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        style: {
          version: 8,
          sources: {},
          layers: [
            {
              id: 'background',
              type: 'background',
              paint: { 'background-color': isDark ? '#18181b' : '#f4f4f5' }
            }
          ]
        },
        center: [(data.client_lon + data.pop_lon) / 2, (data.client_lat + data.pop_lat) / 2],
        zoom: 1,
        renderWorldCopies: false,
        interactive: false
      })

      map.current.on('load', () => {
        if (!map.current || !data) return

        addCountryBaseLayer(map.current, { isDark })

        const features: GeoJSON.Feature[] = [
          {
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [data.client_lon, data.client_lat] },
            properties: { type: 'client', title: 'Client' }
          },
          {
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [data.pop_lon, data.pop_lat] },
            properties: { type: 'pop', title: `POP: ${data.pop}` }
          },
          {
            type: 'Feature',
            geometry: {
              type: 'LineString',
              coordinates: [
                [data.client_lon, data.client_lat],
                [data.pop_lon, data.pop_lat]
              ]
            },
            properties: { type: 'path' }
          }
        ]

        map.current.addSource('points', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features }
        })

        map.current.addLayer({
          id: 'path-layer',
          type: 'line',
          source: 'points',
          filter: ['==', ['get', 'type'], 'path'],
          paint: {
            'line-color': '#ef4444',
            'line-width': 2,
            'line-dasharray': [2, 2]
          }
        })

        map.current.addLayer({
          id: 'points-layer',
          type: 'circle',
          source: 'points',
          filter: ['!=', ['get', 'type'], 'path'],
          paint: {
            'circle-radius': 6,
            'circle-color': [
              'match',
              ['get', 'type'],
              'client', '#3b82f6',
              'pop', '#22c55e',
              '#000000'
            ],
            'circle-stroke-width': 2,
            'circle-stroke-color': '#ffffff'
          }
        })

        const bounds = new maplibregl.LngLatBounds()
        bounds.extend([data.client_lon, data.client_lat])
        bounds.extend([data.pop_lon, data.pop_lat])
        map.current.fitBounds(bounds, { padding: 50, maxZoom: 5, duration: 0 })
      })
      } catch (e) {
        map.current = null
        setMapError('Map failed to initialize.')
      }
    } else {
      if (map.current.isStyleLoaded()) {
        map.current.setPaintProperty('background', 'background-color', isDark ? '#18181b' : '#f4f4f5')

        updateCountryBaseLayerTheme(map.current, isDark)

        const source = map.current.getSource('points') as maplibregl.GeoJSONSource
        if (source) {
          source.setData({
            type: 'FeatureCollection',
            features: [
              {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [data.client_lon, data.client_lat] },
                properties: { type: 'client', title: 'Client' }
              },
              {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [data.pop_lon, data.pop_lat] },
                properties: { type: 'pop', title: `POP: ${data.pop}` }
              },
              {
                type: 'Feature',
                geometry: {
                  type: 'LineString',
                  coordinates: [
                    [data.client_lon, data.client_lat],
                    [data.pop_lon, data.pop_lat]
                  ]
                },
                properties: { type: 'path' }
              }
            ]
          })

          const bounds = new maplibregl.LngLatBounds()
          bounds.extend([data.client_lon, data.client_lat])
          bounds.extend([data.pop_lon, data.pop_lat])
          map.current.fitBounds(bounds, { padding: 50, maxZoom: 5, duration: 0 })
        }
      }
    }
  }, [data, isDark])

  // Automatically resize the map when the container dimensions change
  useEffect(() => {
    if (!mapContainer.current) return
    const resizeObserver = new ResizeObserver(() => {
      if (map.current) {
        map.current.resize()
      }
    })
    resizeObserver.observe(mapContainer.current)
    return () => resizeObserver.disconnect()
  }, [])

  // Extra hack for initial mount resize (common with Mapbox/MapLibre inside modals)
  useEffect(() => {
    const t = setTimeout(() => {
      if (map.current) {
        map.current.resize()
        if (data && map.current.isStyleLoaded()) {
          const bounds = new maplibregl.LngLatBounds()
          bounds.extend([data.client_lon, data.client_lat])
          bounds.extend([data.pop_lon, data.pop_lat])
          map.current.fitBounds(bounds, { padding: 50, maxZoom: 5, duration: 0 })
        }
      }
    }, 150)
    return () => clearTimeout(t)
  }, [data])

  useEffect(() => {
    return () => {
      if (map.current) {
        map.current.remove()
        map.current = null
      }
    }
  }, [])

  if (!hasCoords || mapError) {
    return (
      <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground">
        {mapError ?? 'Location data unavailable.'}
      </div>
    )
  }

  return <div ref={mapContainer} className="absolute inset-0 w-full h-full" />
}

interface ImpossibleDistanceModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ImpossibleDistanceData | null
}

export function ImpossibleDistanceModal({ isOpen, onOpenChange, data }: ImpossibleDistanceModalProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  if (!data) return null

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  const c = 299792.458
  const c_fibre = 200000
  const one_way_ms = data.tcp_rtt / 2 / 1000
  const required_speed = data.distance_km / (one_way_ms / 1000)

  const violation_ratio = required_speed / c_fibre
  const exceeds_vacuum = required_speed > c

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Zap className="h-5 w-5 text-yellow-600" />
            Physics Violation: {data.label}
          </DialogTitle>
          <DialogDescription>
            Geographic distance between client and POP is physically impossible given the TCP RTT.
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 py-4">
          <div className="md:col-span-2 relative border rounded-lg overflow-hidden bg-muted/20 h-[350px]">
            {isOpen && <PhysicsMap data={data} isDark={isDark} />}
            {hasCoords && (
              <div className="absolute top-2 left-2 flex flex-col gap-1 z-10 pointer-events-none">
                <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                  <div className="h-2 w-2 rounded-full bg-[#3b82f6]" />
                  <span>Client: {data.client_lat.toFixed(2)}, {data.client_lon.toFixed(2)}{data.city || data.country ? ` (${[data.city, data.country].filter(Boolean).join(', ')})` : ''}</span>
                </div>
                <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                  <div className="h-2 w-2 rounded-full bg-[#22c55e]" />
                  <span>POP ({data.pop}): {data.pop_lat.toFixed(2)}, {data.pop_lon.toFixed(2)}</span>
                </div>
              </div>
            )}
          </div>

          <div className="flex flex-col gap-4">
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-1">
                <Globe className="h-3 w-3" /> Measured Stats
              </h4>
              <div className="grid grid-cols-2 gap-2">
                <div className="p-2 rounded-md bg-muted/50 border">
                  <p className="text-[11px] sm:text-[10px] text-muted-foreground">TCP RTT</p>
                  <p className="font-mono text-sm">{(data.tcp_rtt / 1000).toFixed(1)} ms</p>
                </div>
                <div className="p-2 rounded-md bg-muted/50 border">
                  <p className="text-[11px] sm:text-[10px] text-muted-foreground">Geo Distance</p>
                  <p className="font-mono text-sm">{data.distance_km.toLocaleString()} km</p>
                </div>
              </div>
            </div>

            <div className={cn(
              "p-3 rounded-lg border space-y-2",
              exceeds_vacuum ? "bg-red-500/10 border-red-500/50" : "bg-yellow-500/10 border-yellow-500/50"
            )}>
              <h4 className="text-xs font-bold uppercase flex items-center gap-1">
                <Zap className="h-3 w-3" /> Violation Report
              </h4>
              <div className="space-y-1">
                <p className="text-xs leading-relaxed">
                  To cover {data.distance_km.toLocaleString()} km in {one_way_ms.toFixed(2)} ms (one-way),
                  the signal would need to travel at:
                </p>
                <p className="font-mono text-lg font-bold text-center py-1">
                  {required_speed.toLocaleString(undefined, { maximumFractionDigits: 0 })} km/s
                </p>
                <div className="space-y-1 text-xs sm:text-[11px]">
                  <div className="flex justify-between items-center">
                    <span>vs. Fibre Speed (~200k km/s)</span>
                    <span className="font-bold text-red-500">{(violation_ratio * 100).toFixed(0)}% of limit</span>
                  </div>
                  {exceeds_vacuum && (
                    <div className="flex justify-between items-center font-bold text-red-600 animate-pulse">
                      <span>FASTER THAN LIGHT (VACUUM)</span>
                      <span>YES</span>
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="p-2 rounded bg-blue-500/5 border border-blue-500/20 text-[11px] sm:text-[10px] leading-relaxed">
              <Info className="h-3 w-3 inline mr-1 mb-0.5 text-blue-600" />
              <strong>Conclusion:</strong> The user is likely using a VPN, proxy, or GPS spoofing to appear in a location physically distant from the Fastly POP they are actually hitting.
            </div>
          </div>
        </div>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}
