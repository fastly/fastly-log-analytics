'use client'

import React, { useEffect, useRef, useState } from 'react'
import * as maplibregl from 'maplibre-gl'
import { useTheme } from 'next-themes'
import { createPortal } from 'react-dom'
import { Shield, Maximize2 } from 'lucide-react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import type { ShieldingRowInput } from '@/app/network/page'
import { CHART_COLORS, formatCountMaybe } from '@/lib/utils'
import {
  rafThrottle,
  observeMapResize,
  addCountryBaseLayer,
  updateCountryBaseLayerTheme,
  createBaseMap,
  removeMapTabStops,
} from './types'
import {
  type ShieldingTooltipInfo,
  greatCirclePoints,
  efficiencyColor,
  lineWidth,
  ShieldingTooltip,
  buildArcFeatures,
  buildDotFeatures,
  ShieldingEmptyState,
  ShieldingA11yTable,
} from './ShieldingMapHelpers'

export { greatCirclePoints, efficiencyColor, lineWidth, buildArcFeatures, buildDotFeatures }

export interface ShieldingMapProps {
  rows: ShieldingRowInput[]
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

// ── Component ─────────────────────────────────────────────────────────────────

export function ShieldingMap({
  rows,
  isLoading,
  edgeOnly,
  errored,
  className,
  expandable,
  fillHeight,
}: ShieldingMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const isDarkRef = useRef(false)
  const [mapReady, setMapReady] = useState(false)
  const [tooltip, setTooltip] = useState<ShieldingTooltipInfo | null>(null)
  // Fullscreen "expand" dialog — a bigger interactive globe of the same arcs.
  const [isExpanded, setIsExpanded] = useState(false)
  // WebGL-unavailable fallback (headless / locked-down browser).
  const [mapError, setMapError] = useState(false)

  const { theme } = useTheme()
  const isDark = theme === 'dark'

  useEffect(() => {
    isDarkRef.current = isDark
  }, [isDark])

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
        // Default to a 3D globe: edge→shield arcs are great-circle paths
        // that wrap the planet, and on a flat Mercator map a long route
        // reads as a misleading straight slash across the world. On a globe
        // the same arc curves naturally over the surface. The GlobeControl
        // below lets the operator flip back to flat Mercator at will.
        map.current = createBaseMap(maplibregl, mapContainer.current, {
          renderWorldCopies: true,
          zoom: 1,
          globe: true,
          cooperativeGestures: !fillHeight,
        })
        // Globe ⇄ Mercator toggle button (MapLibre v5 built-in), stacked under
        // the zoom/compass control in the same corner.
        map.current.addControl(new maplibregl.GlobeControl(), 'top-right')

        map.current.on('load', () => {
          if (!map.current) return

          removeMapTabStops(mapContainer.current)

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
            },
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
            },
          })

          // Anomalous routes (anomaly_static) get a dashed red casing on top so
          // a flagged route reads as anomalous on the MAP, not just in the
          // table's red edge-POP cell. (L7)
          map.current.addLayer({
            id: 'arc-anomaly',
            type: 'line',
            source: 'arcs',
            filter: ['==', ['get', 'anomaly_static'], true],
            paint: {
              'line-color': CHART_COLORS.danger,
              'line-width': ['+', ['get', 'line_width'], 1.5],
              'line-opacity': 0.95,
              'line-dasharray': [2, 2],
            },
          })

          // POP dots — edge (blue)
          map.current.addLayer({
            id: 'dots-edge',
            type: 'circle',
            source: 'dots',
            filter: ['==', ['get', 'role'], 'edge'],
            paint: {
              'circle-radius': 5,
              'circle-color': CHART_COLORS.info,
              'circle-stroke-width': 1.5,
              'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff',
            },
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
            },
          })

          // Arc hover
          map.current.on('mouseenter', 'arc-lines', (e) => {
            if (!e.features?.length || !map.current) return
            map.current.getCanvas().style.cursor = 'pointer'
            const props: Record<string, unknown> = e.features[0].properties ?? {}
            setTooltip({ clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY, props })
          })
          map.current.on(
            'mousemove',
            'arc-lines',
            rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
              if (!e.features?.length) return
              const props: Record<string, unknown> = e.features[0].properties ?? {}
              setTooltip({ clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY, props })
            }),
          )
          map.current.on('mouseleave', 'arc-lines', () => {
            if (map.current) map.current.getCanvas().style.cursor = ''
            setTooltip(null)
          })

          setMapReady(true)
        })
      } catch {
        map.current = null
        queueMicrotask(() => setMapError(true))
      }
    }

    const stopResizeObserver = observeMapResize(mapContainer.current, () => map.current)

    return () => {
      stopResizeObserver()
      map.current?.remove()
      map.current = null
      setMapReady(false)
    }
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

        const zoomBias = fillHeight ? 1 : 0

        if (Math.abs(maxLon - minLon) < 1 && Math.abs(maxLat - minLat) < 1) {
          map.current.flyTo({ center: [minLon, minLat], zoom: 1 + zoomBias, duration: 1000 })
        } else {
          const camera = map.current.cameraForBounds(
            [
              [minLon, minLat],
              [maxLon, maxLat],
            ],
            { padding: 50 },
          )
          if (camera && camera.zoom !== undefined) {
            map.current.flyTo({
              ...camera,
              zoom: Math.max(0, camera.zoom - 2 + zoomBias),
              duration: 1000,
            })
          } else {
            map.current.fitBounds(
              [
                [minLon, minLat],
                [maxLon, maxLat],
              ],
              { padding: 50, duration: 1000, maxZoom: 3 + zoomBias },
            )
          }
        }
      }
    }

    if (!map.current.isStyleLoaded()) {
      const timer = setTimeout(() => {
        if (!map.current || !map.current.isStyleLoaded()) return
        updateData()
      }, 100)
      return () => clearTimeout(timer)
    }

    updateData()
  }, [rows, mapReady, fillHeight])

  // NB: these useMemo hooks MUST stay above the conditional early-returns
  // below (edgeOnly / empty / no-coords).
  const sortedRows = React.useMemo(
    () =>
      [...(rows || [])]
        .filter((r) => r && r.edge_pop && r.shield_pop)
        .sort((a, b) => (b.requests || 0) - (a.requests || 0)),
    [rows],
  )
  const a11yLabel = React.useMemo(() => {
    if (sortedRows.length === 0) {
      return 'Shielding map showing edge-POP to shield-POP paths. No paths available.'
    }
    const topThree = sortedRows
      .slice(0, 3)
      .map((r) => `${r.edge_pop} to ${r.shield_pop} ${formatCountMaybe(r.requests) ?? '0'} requests`)
      .join(', ')
    return `Shielding map of ${sortedRows.length} edge-to-shield paths. Top routes: ${topThree}.`
  }, [sortedRows])
  const a11yRows = React.useMemo(() => sortedRows.slice(0, 50), [sortedRows])

  const arcFeatures = React.useMemo(() => buildArcFeatures(rows), [rows])
  const arcCount = arcFeatures.features.length

  const emptyState = ShieldingEmptyState({ errored, edgeOnly, isLoading, rows, arcCount })
  if (emptyState) return emptyState

  return (
    <div
      className={`relative flex flex-col border rounded-xl overflow-hidden bg-muted/10 ${className ?? ''} ${fillHeight ? 'h-full' : 'min-h-[420px]'}`}
      role="group"
      aria-label={a11yLabel}
    >
      {mapError ? (
        <div
          className={`w-full flex items-center justify-center px-4 text-center text-xs text-muted-foreground ${fillHeight ? 'flex-1' : 'h-[420px]'}`}
          aria-hidden="true"
        >
          Interactive map unavailable in this browser. See the shielding-paths table below.
        </div>
      ) : (
        <div ref={mapContainer} className={`w-full ${fillHeight ? 'flex-1 min-h-0' : 'h-[420px]'}`} aria-hidden="true" />
      )}

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

      <ShieldingA11yTable a11yRows={a11yRows} totalSortedCount={sortedRows.length} />

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
                A larger, interactive globe of the same edge-POP → shield-POP transit paths. Drag to rotate, scroll to
                zoom; the globe ⇄ flat-map toggle sits top-right.
              </DialogDescription>
            </DialogHeader>
            <div className="h-[70vh] w-full">{isExpanded && <ShieldingMap rows={rows} fillHeight />}</div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  )
}
