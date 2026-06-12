'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { Shield } from 'lucide-react'

interface ShieldingMapProps {
  rows: any[]
  isLoading?: boolean
  edgeOnly?: boolean
  className?: string
}

interface TooltipInfo {
  clientX: number
  clientY: number
  props: Record<string, any>
}

/**
 * rAF-throttle a function so it fires at most once per animation frame.
 * Wrapping MapLibre `mousemove` handlers caps the per-frame re-render
 * cost to display refresh rate while preserving the latest position.
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

// ── Geometry helpers ──────────────────────────────────────────────────────────

function greatCirclePoints(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
  n = 32
): [number, number][] {
  const toRad = (d: number) => (d * Math.PI) / 180
  const toDeg = (r: number) => (r * 180) / Math.PI
  const φ1 = toRad(lat1), λ1 = toRad(lon1)
  const φ2 = toRad(lat2), λ2 = toRad(lon2)

  // Clamp dot product to [-1, 1] to prevent Math.acos from returning NaN due to floating point inaccuracy
  const dotProduct = Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(λ2 - λ1)
  const clampedDot = Math.max(-1, Math.min(1, dotProduct))
  const d = Math.acos(clampedDot)

  if (isNaN(d) || d < 0.001) return [[lon1, lat1], [lon2, lat2]]

  const pts: [number, number][] = []
  let prevLon = lon1

  for (let i = 0; i <= n; i++) {
    const f = i / n
    const A = Math.sin((1 - f) * d) / Math.sin(d)
    const B = Math.sin(f * d) / Math.sin(d)
    const x = A * Math.cos(φ1) * Math.cos(λ1) + B * Math.cos(φ2) * Math.cos(λ2)
    const y = A * Math.cos(φ1) * Math.sin(λ1) + B * Math.cos(φ2) * Math.sin(λ2)
    const z = A * Math.sin(φ1) + B * Math.sin(φ2)

    const lat = toDeg(Math.atan2(z, Math.sqrt(x * x + y * y)))
    let lon = toDeg(Math.atan2(y, x))

    // Handle antimeridian crossing (prevents lines drawing across the entire map)
    if (i > 0 && Math.abs(lon - prevLon) > 180) {
      if (prevLon < 0) lon -= 360
      else lon += 360
    }

    pts.push([lon, lat])
    prevLon = lon
  }
  return pts
}

function efficiencyColor(ratio: number | null): string {
  if (ratio == null) return '#6366f1'
  if (ratio < 1.5) return '#22c55e'
  if (ratio < 3.0) return '#eab308'
  return '#ef4444'
}

function lineWidth(requests: number): number {
  return Math.max(1.5, Math.min(6, 1.5 + Math.log10(Math.max(1, requests)) * 1.2))
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function ShieldingTooltip({ info }: { info: TooltipInfo }) {
  const { props, clientX, clientY } = info
  const flipLeft = clientX > window.innerWidth * 0.7

  return (
    <div
      style={{
        position: 'fixed',
        top: clientY - 12,
        left: flipLeft ? clientX - 14 : clientX + 14,
        transform: flipLeft ? 'translate(-100%, -100%)' : 'translateY(-100%)',
        zIndex: 9999,
        pointerEvents: 'none',
      }}
      className="bg-popover text-popover-foreground border border-border rounded-lg shadow-xl px-3 py-2.5 font-sans min-w-[180px]"
    >
      <div className="font-semibold text-xs leading-tight">
        {props.edge_pop} <span className="text-muted-foreground">→</span>{' '}
        <span className="text-purple-500">{props.shield_pop}</span>
      </div>
      <div className="mt-2 space-y-1">
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.requests).toLocaleString()}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P50 RTT</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p50_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P95 RTT</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p95_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P99 RTT</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p99_ms).toFixed(1)}ms</span>
        </div>
        {props.distance_km != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Distance</span>
            <span className="text-[11px] font-semibold tabular-nums">{Number(props.distance_km).toLocaleString()}km</span>
          </div>
        )}
        {props.light_speed_rtt_ms != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Light floor</span>
            <span className="text-[11px] font-semibold tabular-nums">{Number(props.light_speed_rtt_ms).toFixed(1)}ms</span>
          </div>
        )}
        {props.efficiency_ratio != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Efficiency</span>
            <span
              className="text-[11px] font-semibold tabular-nums"
              style={{ color: efficiencyColor(Number(props.efficiency_ratio)) }}
            >
              {Number(props.efficiency_ratio).toFixed(1)}× light speed
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

// ── GeoJSON builders ──────────────────────────────────────────────────────────

function buildArcFeatures(rows: any[]): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = []
  for (const row of rows) {
    if (
      row.edge_lat == null || row.edge_lon == null ||
      row.shield_lat == null || row.shield_lon == null
    ) continue

    // Skip 0-length arcs (same POP or coordinates) to prevent MapLibre WebGL triangulation crashes
    if (Math.abs(row.edge_lat - row.shield_lat) < 0.001 && Math.abs(row.edge_lon - row.shield_lon) < 0.001) {
      continue
    }

    const coords = greatCirclePoints(row.edge_lat, row.edge_lon, row.shield_lat, row.shield_lon)
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: coords },
      properties: {
        edge_pop: row.edge_pop,
        shield_pop: row.shield_pop,
        requests: row.requests,
        p50_ms: row.p50_ms,
        p95_ms: row.p95_ms,
        p99_ms: row.p99_ms,
        distance_km: row.distance_km,
        light_speed_rtt_ms: row.light_speed_rtt_ms,
        efficiency_ratio: row.efficiency_ratio,
        anomaly_static: row.anomaly_static,
        color: efficiencyColor(row.efficiency_ratio),
        line_width: lineWidth(row.requests),
      },
    })
  }
  return { type: 'FeatureCollection', features }
}

function buildDotFeatures(rows: any[]): GeoJSON.FeatureCollection {
  const seen = new Set<string>()
  const features: GeoJSON.Feature[] = []
  for (const row of rows) {
    if (row.edge_lat != null && row.edge_lon != null) {
      const key = `edge:${row.edge_pop}`
      if (!seen.has(key)) {
        seen.add(key)
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [row.edge_lon, row.edge_lat] },
          properties: { pop: row.edge_pop, role: 'edge' },
        })
      }
    }
    if (row.shield_lat != null && row.shield_lon != null && row.shield_pop !== 'Direct to Origin') {
      const key = `shield:${row.shield_pop}`
      if (!seen.has(key)) {
        seen.add(key)
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [row.shield_lon, row.shield_lat] },
          properties: { pop: row.shield_pop, role: 'shield' },
        })
      }
    }
  }
  return { type: 'FeatureCollection', features }
}

// ── Component ─────────────────────────────────────────────────────────────────

const EMPTY_FC: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] }

export function ShieldingMap({ rows, isLoading, edgeOnly, className }: ShieldingMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const isDarkRef = useRef(false)
  // rowsRef always holds the latest rows so the load callback never reads stale data
  const rowsRef = useRef<any[]>(rows)
  rowsRef.current = rows

  const [mapReady, setMapReady] = useState(false)
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)

  const { theme } = useTheme()
  const isDark = theme === 'dark'

  useEffect(() => { isDarkRef.current = isDark }, [isDark])

  // Sync country fill color when theme changes
  useEffect(() => {
    if (!map.current || !mapReady) return
    map.current.setPaintProperty('countries', 'fill-color', isDark ? '#27272a' : '#e4e4e7')
    map.current.setPaintProperty('countries', 'fill-outline-color', isDark ? '#3f3f46' : '#d4d4d8')
  }, [isDark, mapReady])

  // Initialize map once
  useEffect(() => {
    if (!mapContainer.current) return

    if (!map.current) {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        renderWorldCopies: true,
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
        map.current.addSource('arcs', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
        map.current.addSource('dots', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        map.current.addLayer({
          id: 'countries',
          type: 'fill',
          source: 'world',
          paint: {
            'fill-color': isDarkRef.current ? '#27272a' : '#e4e4e7',
            'fill-outline-color': isDarkRef.current ? '#3f3f46' : '#d4d4d8',
            'fill-opacity': 0.8,
          }
        })

        // Arc glow (thicker, transparent)
        map.current.addLayer({
          id: 'arc-glow',
          type: 'line',
          source: 'arcs',
          paint: {
            'line-color': ['get', 'color'],
            'line-width': ['*', ['get', 'line_width'], 2.5],
            'line-opacity': 0.15,
          }
        })

        // Arc line
        map.current.addLayer({
          id: 'arc-lines',
          type: 'line',
          source: 'arcs',
          paint: {
            'line-color': ['get', 'color'],
            'line-width': ['get', 'line_width'],
            'line-opacity': 0.85,
          }
        })

        // POP dots — edge (blue)
        map.current.addLayer({
          id: 'dots-edge',
          type: 'circle',
          source: 'dots',
          filter: ['==', ['get', 'role'], 'edge'],
          paint: {
            'circle-radius': 5,
            'circle-color': '#3b82f6',
            'circle-stroke-width': 1.5,
            'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff',
          }
        })

        // POP dots — shield (purple)
        map.current.addLayer({
          id: 'dots-shield',
          type: 'circle',
          source: 'dots',
          filter: ['==', ['get', 'role'], 'shield'],
          paint: {
            'circle-radius': 6,
            'circle-color': '#a855f7',
            'circle-stroke-width': 1.5,
            'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff',
          }
        })

        // Arc hover
        map.current.on('mouseenter', 'arc-lines', (e) => {
          if (!e.features?.length || !map.current) return
          map.current.getCanvas().style.cursor = 'pointer'
          const props = e.features[0].properties as Record<string, any>
          setTooltip({ clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY, props })
        })
        map.current.on('mousemove', 'arc-lines', rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
          if (!e.features?.length) return
          const props = e.features[0].properties as Record<string, any>
          setTooltip({ clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY, props })
        }))
        map.current.on('mouseleave', 'arc-lines', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })

        setMapReady(true)
      })
    }

    const resizeObserver = new ResizeObserver(() => {
      map.current?.resize()
    })
    resizeObserver.observe(mapContainer.current)

    return () => {
      resizeObserver.disconnect()
      map.current?.remove()
      map.current = null
      setMapReady(false)
    }
  }, [])

  // Update sources when rows change or map becomes ready
  useEffect(() => {
    if (!map.current || !mapReady) return

    const updateData = () => {
      if (!map.current) return
      const arcSrc = map.current.getSource('arcs') as maplibregl.GeoJSONSource | undefined
      const dotSrc = map.current.getSource('dots') as maplibregl.GeoJSONSource | undefined
      if (!arcSrc || !dotSrc) return

      arcSrc.setData(buildArcFeatures(rows))
      dotSrc.setData(buildDotFeatures(rows))

      // Center map on shield POPs if available
      const shieldLons: number[] = []
      const shieldLats: number[] = []
      for (const row of rows) {
        if (row.shield_lon != null && row.shield_lat != null) {
          shieldLons.push(row.shield_lon)
          shieldLats.push(row.shield_lat)
        }
      }

      if (shieldLons.length > 0 && shieldLats.length > 0) {
        const minLon = Math.min(...shieldLons)
        const maxLon = Math.max(...shieldLons)
        const minLat = Math.min(...shieldLats)
        const maxLat = Math.max(...shieldLats)

        // If there's only one point or they are very close, fly to it instead of fitBounds to avoid zooming in too far
        if (Math.abs(maxLon - minLon) < 1 && Math.abs(maxLat - minLat) < 1) {
           map.current.flyTo({ center: [minLon, minLat], zoom: 1, duration: 1000 })
        } else {
           const camera = map.current.cameraForBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 50 });
           if (camera && camera.zoom !== undefined) {
             map.current.flyTo({
               ...camera,
               zoom: Math.max(0, camera.zoom - 2),
               duration: 1000
             });
           } else {
             map.current.fitBounds(
               [[minLon, minLat], [maxLon, maxLat]],
               { padding: 50, duration: 1000, maxZoom: 3 }
             )
           }
        }
      }
    }

    // Safety check that the style is fully loaded
    if (!map.current.isStyleLoaded()) {
      const timer = setTimeout(() => {
        if (!map.current || !map.current.isStyleLoaded()) return
        updateData()
      }, 100)
      return () => clearTimeout(timer)
    }

    updateData()
  }, [rows, mapReady])

  const arcCount = buildArcFeatures(rows).features.length

  if (edgeOnly) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed space-y-1">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
        <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Enable full logging (remove edge-only filtering) to see POP-to-POP transit arcs here.</p>
      </div>
    )
  }

  if (!isLoading && rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground italic">No shielding path data available for this time range.</p>
      </div>
    )
  }

  if (!isLoading && rows.length > 0 && arcCount === 0) {
    // Rows exist but no coordinates matched — POP codes from logs not in pop_locations.json
    const popCodes = [...new Set(rows.flatMap(r => [r.edge_pop, r.shield_pop]).filter(Boolean))].join(', ')
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed space-y-1">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground">POP coordinates unavailable for the observed paths.</p>
        <p className="text-xs text-muted-foreground italic">Observed POP codes: {popCodes}</p>
      </div>
    )
  }

  return (
    <div className={`relative flex flex-col border rounded-xl overflow-hidden bg-muted/10 ${className ?? ''} min-h-[420px]`}>
      <div ref={mapContainer} className="w-full h-[420px]" />

      {isLoading && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm">
          <div className="text-sm text-muted-foreground animate-pulse">Loading shielding paths…</div>
        </div>
      )}

      {tooltip && createPortal(<ShieldingTooltip info={tooltip} />, document.body)}
    </div>
  )
}
