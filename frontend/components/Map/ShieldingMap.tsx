'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { Shield, Maximize2 } from 'lucide-react'
import { addCountryBaseLayer, updateCountryBaseLayerTheme } from '@/components/Map/baseLayers'
import { PopLabel } from '@/components/PopLabel'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'

interface ShieldingMapProps {
  rows: any[]
  isLoading?: boolean
  edgeOnly?: boolean
  /** Backend computed the analysis but the handler errored (M2 sentinel). */
  errored?: boolean
  className?: string
  /** Show an "Expand" button that opens the map full-size in a modal. The
      modal renders a second ShieldingMap with `fillHeight` (and WITHOUT
      `expandable`) so there's no recursive expand button. */
  expandable?: boolean
  /** Fill the parent's height instead of the fixed 420px card height. Used by
      the fullscreen modal so the globe gets the whole dialog body. */
  fillHeight?: boolean
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
export function rafThrottle<TArgs extends any[]>(fn: (...args: TArgs) => void) {
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

export function greatCirclePoints(
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

export function efficiencyColor(ratio: number | null): string {
  // A negative ratio (transit delta < 0 — possible on sparse routes or with
  // clock skew) is "no meaningful efficiency", NOT "excellent": treat it like
  // null/indigo rather than letting `ratio < 1.5` paint it green. (L5)
  if (ratio == null || ratio < 0) return '#6366f1'
  if (ratio < 1.5) return '#22c55e'
  if (ratio < 3.0) return '#eab308'
  return '#ef4444'
}

export function lineWidth(requests: number): number {
  return Math.max(1.5, Math.min(6, 1.5 + Math.log10(Math.max(1, requests)) * 1.2))
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function ShieldingTooltip({ info }: { info: TooltipInfo }) {
  const { props, clientX, clientY } = info
  const flipLeft = clientX > window.innerWidth * 0.7
  // MapLibre can stringify boolean feature-properties, so accept both forms.
  const lowSample = props.low_sample === true || props.low_sample === 'true'

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
        <PopLabel code={props.edge_pop} /> <span className="text-muted-foreground">→</span>{' '}
        <PopLabel code={props.shield_pop} className="text-purple-500" />
      </div>
      <div className="mt-2 space-y-1">
        {/* These percentiles are the edge→shield transit delta (E→S), NOT a
            TCP round-trip time — label them the same way the data table does
            so the two surfaces don't disagree. (L4) */}
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.requests).toLocaleString()}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P50 (E→S)</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p50_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P95 (E→S)</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p95_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P99 (E→S)</span>
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
              // Low-sample routes don't get an efficiency colour — the ratio is
              // noise below the flag floor, so render it neutral. (low-sample gating)
              style={{ color: lowSample ? '#94a3b8' : efficiencyColor(Number(props.efficiency_ratio)) }}
            >
              {Number(props.efficiency_ratio).toFixed(1)}× light speed
            </span>
          </div>
        )}
        {/* anomaly_static is computed server-side (efficiency > 3× AND ≥20ms
            absolute overhead, AND enough requests to trust the median).
            Surface it on the map too — previously it only appeared as the red
            edge-POP cell in the table. (L7) */}
        {(props.anomaly_static === true || props.anomaly_static === 'true') && (
          <div className="mt-1 flex items-center gap-1 text-[11px] font-semibold text-destructive">
            <span aria-hidden="true">⚠</span>
            <span>Suboptimal peering</span>
          </div>
        )}
        {/* Below the anomaly-flag floor: too few requests for the percentiles
            to mean anything. Say so instead of leaving a scary-looking ratio
            unqualified. (low-sample gating) */}
        {lowSample && (
          <div className="mt-1 flex items-center gap-1 text-[11px] text-muted-foreground">
            <span aria-hidden="true">ⓘ</span>
            <span>Low sample (&lt;30 reqs) — not flagged</span>
          </div>
        )}
      </div>
    </div>
  )
}

// ── GeoJSON builders ──────────────────────────────────────────────────────────

export function buildArcFeatures(rows: any[]): GeoJSON.FeatureCollection {
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
        low_sample: row.low_sample,
        // Too few requests for the median to be trustworthy → paint a neutral
        // grey arc instead of an efficiency colour so a quiet route can't read
        // as a red "problem". The route stays on the map for visibility; it
        // just isn't colour-graded or anomaly-flagged. (low-sample gating)
        color: row.low_sample ? '#94a3b8' : efficiencyColor(row.efficiency_ratio),
        line_width: lineWidth(row.requests),
      },
    })
  }
  return { type: 'FeatureCollection', features }
}

export function buildDotFeatures(rows: any[]): GeoJSON.FeatureCollection {
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

export function ShieldingMap({ rows, isLoading, edgeOnly, errored, className, expandable, fillHeight }: ShieldingMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const isDarkRef = useRef(false)
  // rowsRef always holds the latest rows so the load callback never reads stale data
  const rowsRef = useRef<any[]>(rows)
  rowsRef.current = rows

  const [mapReady, setMapReady] = useState(false)
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)
  // Fullscreen "expand" dialog — a bigger interactive globe of the same arcs.
  const [isExpanded, setIsExpanded] = useState(false)
  // WebGL-unavailable fallback (headless / locked-down browser). MapLibre's
  // constructor throws `webglcontextcreationerror` with no GL context; left
  // unguarded that propagates out of the mount effect into the route error
  // boundary. The sr-only data table below still renders, so degrade to a
  // placeholder instead.
  const [mapError, setMapError] = useState(false)

  const { theme } = useTheme()
  const isDark = theme === 'dark'

  useEffect(() => { isDarkRef.current = isDark }, [isDark])

  // Sync country fill color when theme changes
  useEffect(() => {
    if (!map.current || !mapReady) return
    updateCountryBaseLayerTheme(map.current, isDark)
  }, [isDark, mapReady])

  // Initialize map once
  useEffect(() => {
    if (!mapContainer.current) return

    if (!map.current) {
      try {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        renderWorldCopies: true,
        style: {
          version: 8,
          // Default to a 3D globe: edge→shield arcs are great-circle paths
          // that wrap the planet, and on a flat Mercator map a long route
          // reads as a misleading straight slash across the world. On a globe
          // the same arc curves naturally over the surface. The GlobeControl
          // below lets the operator flip back to flat Mercator at will.
          projection: { type: 'globe' },
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
        interactive: true,
        // The inline card sits mid-page, so a bare mousewheel would zoom the
        // map and swallow the page scroll. Cooperative gestures make a plain
        // wheel scroll the PAGE (with a brief "use ⌘/Ctrl + scroll to zoom"
        // hint); zoom still works via modifier+wheel, pinch, or the +/-
        // buttons. In the fullscreen dialog there's nothing to scroll behind
        // it, so let a bare wheel zoom the globe directly.
        cooperativeGestures: !fillHeight,
      })

      map.current.addControl(new maplibregl.NavigationControl(), 'top-right')
      // Globe ⇄ Mercator toggle button (MapLibre v5 built-in), stacked under
      // the zoom/compass control in the same corner.
      map.current.addControl(new maplibregl.GlobeControl(), 'top-right')

      map.current.on('load', () => {
        if (!map.current) return

        map.current.addSource('arcs', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
        map.current.addSource('dots', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        addCountryBaseLayer(map.current, {
          isDark: isDarkRef.current,
          extraPaint: { 'fill-opacity': 0.8 },
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

        // Anomalous routes (anomaly_static) get a dashed red casing on top so
        // a flagged route reads as anomalous on the MAP, not just in the
        // table's red edge-POP cell. A route can be red-by-efficiency without
        // being anomaly_static (short hops where TCP overhead dominates), so
        // this is a distinct signal from the efficiency color. (L7)
        map.current.addLayer({
          id: 'arc-anomaly',
          type: 'line',
          source: 'arcs',
          filter: ['==', ['get', 'anomaly_static'], true],
          paint: {
            'line-color': '#ef4444',
            'line-width': ['+', ['get', 'line_width'], 1.5],
            'line-opacity': 0.95,
            'line-dasharray': [2, 2],
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
      } catch {
        // WebGL unavailable (headless / locked-down browser). The sr-only
        // table below still renders the data; show a placeholder instead of
        // throwing into the route error boundary.
        map.current = null
        // Error-recovery path only: fires at most once and never on a normal
        // render, so the "cascading renders" concern this rule guards against
        // doesn't apply (same try/catch shape as ImpossibleDistanceModal).
        // eslint-disable-next-line react-hooks/set-state-in-effect
        setMapError(true)
      }
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
    // fillHeight is fixed per instance (the inline card vs. the modal), so this
    // still constructs the map exactly once — it's only a dep because the
    // constructor reads it for cooperativeGestures.
  }, [fillHeight])

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

        // The fullscreen modal has far more room than the 420px inline card, so
        // open it one zoom level closer for a bigger, more readable globe.
        const zoomBias = fillHeight ? 1 : 0

        // If there's only one point or they are very close, fly to it instead of fitBounds to avoid zooming in too far
        if (Math.abs(maxLon - minLon) < 1 && Math.abs(maxLat - minLat) < 1) {
           map.current.flyTo({ center: [minLon, minLat], zoom: 1 + zoomBias, duration: 1000 })
        } else {
           const camera = map.current.cameraForBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 50 });
           if (camera && camera.zoom !== undefined) {
             map.current.flyTo({
               ...camera,
               zoom: Math.max(0, camera.zoom - 2 + zoomBias),
               duration: 1000
             });
           } else {
             map.current.fitBounds(
               [[minLon, minLat], [maxLon, maxLat]],
               { padding: 50, duration: 1000, maxZoom: 3 + zoomBias }
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
    // fillHeight is fixed per instance; it's only a dep because the modal opens
    // one zoom level closer (zoomBias above).
  }, [rows, mapReady, fillHeight])

  // a11y mirror of ChoroplethMap's pattern: <canvas> exposes nothing to
  // assistive tech, so we surface an aria-label summary + sr-only table of
  // the same rows the visual layer renders. Skipping the keyboard listbox
  // (the ChoroplethMap variant) because shielding edges are paired routes,
  // not single-axis values — the sr-only table is the right shape.
  // NB: these useMemo hooks MUST stay above the conditional early-returns
  // below (edgeOnly / empty / no-coords). When the card first mounts in the
  // loading state (all hooks run) and then resolves to one of those empty
  // states (early return → hooks skipped), React throws "Rendered fewer
  // hooks than expected" and crashes /network to its error boundary.
  const sortedRows = React.useMemo(
    () => [...(rows || [])]
      .filter(r => r && r.edge_pop && r.shield_pop)
      .sort((a, b) => (b.requests || 0) - (a.requests || 0)),
    [rows],
  )
  const a11yLabel = React.useMemo(() => {
    if (sortedRows.length === 0) {
      return 'Shielding map showing edge-POP to shield-POP paths. No paths available.'
    }
    const topThree = sortedRows.slice(0, 3)
      .map(r => `${r.edge_pop} to ${r.shield_pop} ${r.requests?.toLocaleString() ?? '0'} requests`)
      .join(', ')
    return `Shielding map of ${sortedRows.length} edge-to-shield paths. Top routes: ${topThree}.`
  }, [sortedRows])
  const a11yRows = React.useMemo(() => sortedRows.slice(0, 50), [sortedRows])

  const arcFeatures = React.useMemo(() => buildArcFeatures(rows), [rows])
  const arcCount = arcFeatures.features.length

  if (errored) {
    // M2: the backend handler failed (and logged a stack trace). Show an
    // explicit error state rather than the misleading "no data" empty state.
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed border-destructive/40 space-y-1">
        <Shield className="h-8 w-8 text-destructive mb-2 opacity-30" />
        <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
        <p className="text-xs text-muted-foreground max-w-sm">The edge-to-shield transit analysis failed to compute for this window. This is a server-side error, not an absence of data — try again or narrow the time range.</p>
      </div>
    )
  }

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
    <div
      className={`relative flex flex-col border rounded-xl overflow-hidden bg-muted/10 ${className ?? ''} ${fillHeight ? 'h-full' : 'min-h-[420px]'}`}
      // role="group" (not "img"): the wrapper holds the interactive MapLibre
      // controls + the sr-only data table below, so it can't be an "img"
      // (axe: nested-interactive). group legitimately groups interactive
      // content under the aria-label.
      role="group"
      aria-label={a11yLabel}
    >
      {mapError ? (
        <div className={`w-full flex items-center justify-center px-4 text-center text-xs text-muted-foreground ${fillHeight ? 'flex-1' : 'h-[420px]'}`} aria-hidden="true">
          Interactive map unavailable in this browser. See the shielding-paths table below.
        </div>
      ) : (
        <div ref={mapContainer} className={`w-full ${fillHeight ? 'flex-1 min-h-0' : 'h-[420px]'}`} aria-hidden="true" />
      )}

      {/* Expand to a full-size globe in a modal. Only on the inline card
          (not the modal's own map, and not the WebGL-unavailable placeholder). */}
      {expandable && !fillHeight && !mapError && (
        <button
          type="button"
          onClick={() => setIsExpanded(true)}
          className="absolute top-2 left-2 z-10 inline-flex items-center gap-1.5 rounded-md border bg-background/90 px-2 py-1 text-[11px] font-medium text-muted-foreground shadow-sm backdrop-blur-sm transition-colors hover:bg-background hover:text-foreground"
          aria-label="Expand map to fullscreen"
        >
          <Maximize2 className="h-3.5 w-3.5" aria-hidden="true" />
          <span className="hidden sm:inline">Expand</span>
        </button>
      )}

      <table className="sr-only">
        <caption>
          {a11yRows.length > 0
            ? `Shielding paths — ${a11yRows.length} routes shown${sortedRows.length > a11yRows.length ? ` of ${sortedRows.length} total` : ''}.`
            : 'Shielding paths — none available.'}
        </caption>
        <thead>
          {/* Column parity with the visual tooltip + the Shielding Analysis
              data table so assistive-tech users get the same numbers, not a
              subset. (L9) */}
          <tr>
            <th scope="col">Edge POP</th>
            <th scope="col">Shield POP</th>
            <th scope="col">Requests</th>
            <th scope="col">p50 transit (ms)</th>
            <th scope="col">p95 transit (ms)</th>
            <th scope="col">p99 transit (ms)</th>
            <th scope="col">Distance (km)</th>
            <th scope="col">Light-speed floor (ms)</th>
            <th scope="col">Efficiency ratio</th>
            <th scope="col">Anomalous</th>
          </tr>
        </thead>
        <tbody>
          {a11yRows.map((r, i) => (
            <tr key={`${r.edge_pop}-${r.shield_pop}-${i}`}>
              <td><PopLabel code={r.edge_pop} /></td>
              <td><PopLabel code={r.shield_pop} /></td>
              <td>{(r.requests ?? 0).toLocaleString()}{r.low_sample ? ' (low sample)' : ''}</td>
              <td>{r.p50_ms != null ? r.p50_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.p95_ms != null ? r.p95_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.p99_ms != null ? r.p99_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.distance_km != null ? r.distance_km.toLocaleString() : 'n/a'}</td>
              <td>{r.light_speed_rtt_ms != null ? r.light_speed_rtt_ms.toFixed(1) : 'n/a'}</td>
              <td>{r.efficiency_ratio != null ? r.efficiency_ratio.toFixed(2) : 'n/a'}</td>
              <td>{r.anomaly_static ? 'yes' : 'no'}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {isLoading && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm">
          <div className="text-sm text-muted-foreground animate-pulse">Loading shielding paths…</div>
        </div>
      )}

      {tooltip && createPortal(<ShieldingTooltip info={tooltip} />, document.body)}

      {expandable && (
        <Dialog open={isExpanded} onOpenChange={setIsExpanded}>
          <DialogContent className="max-w-5xl w-[calc(100%-2rem)] gap-3 p-4">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Shield className="h-4 w-4 text-purple-500" aria-hidden="true" />
                Edge → Shield Transit Map
              </DialogTitle>
              <DialogDescription>
                A larger, interactive globe of the same edge-POP → shield-POP transit
                paths. Drag to rotate, scroll to zoom; the globe ⇄ flat-map toggle sits
                top-right.
              </DialogDescription>
            </DialogHeader>
            {/* Mount the big map only while open so a second WebGL context isn't
                built in the background; fillHeight lets it take the whole body. */}
            <div className="h-[70vh] w-full">
              {isExpanded && <ShieldingMap rows={rows} fillHeight />}
            </div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  )
}
