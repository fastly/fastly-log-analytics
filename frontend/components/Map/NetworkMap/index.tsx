'use client'

import React, { useEffect, useRef, useState, useSyncExternalStore, type RefObject, useMemo, MutableRefObject } from 'react'
import * as maplibregl from 'maplibre-gl'
import { type Map as MapLibreMap, type MapOptions } from 'maplibre-gl'
import type { FeatureCollection } from 'geojson'
import type { GeometryCollection, Topology } from 'topojson-specification'
import { useTheme } from 'next-themes'
import { DashboardMapData } from '@/types/api'
import countryMapData from '@/lib/country-codes.json'
import { formatCompactCount, resolveCountryName, formatCount, CHART_COLORS, formatCountMaybe, keyByOccurrence, formatBucket } from '@/lib/utils'
import { createPortal } from 'react-dom'
import { Shield, Maximize2, Play, Pause } from 'lucide-react'
import { PopLabel, formatPopGeo, type PopGeo } from '@/components/DataTable'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import type { ShieldingRowInput } from '@/app/network/page'
import { useTimezoneStore, usePopGeoStore } from '@/stores/filterStore'
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { components } from '@/types/api.generated'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
export { MapPrewarm } from '@/components/PlotlyChart'

export const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
}

export function rafThrottle<TArgs extends unknown[]>(fn: (...args: TArgs) => void) {
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

const COUNTRY_FILL_DARK = '#27272a'
const COUNTRY_FILL_LIGHT = '#e4e4e7'
const COUNTRY_OUTLINE_DARK = '#3f3f46'
const COUNTRY_OUTLINE_LIGHT = '#d4d4d8'

export function countryFill(isDark: boolean): string {
  return isDark ? COUNTRY_FILL_DARK : COUNTRY_FILL_LIGHT
}

export function countryOutline(isDark: boolean): string {
  return isDark ? COUNTRY_OUTLINE_DARK : COUNTRY_OUTLINE_LIGHT
}

type Resizable = { resize: () => unknown }

export function observeMapResize(container: Element, getMap: () => Resizable | null | undefined): () => void {
  const ro = new ResizeObserver(() => {
    getMap()?.resize()
  })
  ro.observe(container)
  return () => ro.disconnect()
}

export function useMapResize(
  mapRef: RefObject<Resizable | null>,
  containerRef: RefObject<Element | null>,
): void {
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    return observeMapResize(el, () => mapRef.current)
  }, [mapRef, containerRef])
}

declare const sampleMap: MapLibreMap

export type MapFillColor = Parameters<typeof sampleMap.setPaintProperty<'fill-color'>>[2]

const WORLD_SOURCE_ID = 'world'
const COUNTRIES_LAYER_ID = 'countries'
const WORLD_TOPOJSON_URL = '/geo/world.topo.json'

type FillPaintExtras = {
  'fill-opacity'?: number
}

let worldGeoPromise: Promise<FeatureCollection> | null = null

async function loadWorldGeoJson(): Promise<FeatureCollection> {
  if (!worldGeoPromise) {
    worldGeoPromise = (async () => {
      const [topojsonClient, res] = await Promise.all([
        import('topojson-client'),
        fetch(WORLD_TOPOJSON_URL),
      ])

      const featureFn = topojsonClient.feature || (topojsonClient as { default?: { feature?: typeof topojsonClient.feature } }).default?.feature
      if (!featureFn) {
        throw new Error("feature function not found in topojson-client exports!")
      }

      const topo = (await res.json()) as Topology<{ world: GeometryCollection }>
      const fc = featureFn(topo, topo.objects.world) as FeatureCollection
      return fc
    })().catch((err) => {
      console.error("[MapBaseLayer] loadWorldGeoJson: caught error:", err)
      worldGeoPromise = null
      throw err
    })
  }
  return worldGeoPromise
}

export function addCountryBaseLayer(
  map: MapLibreMap,
  opts: { isDark: boolean; extraPaint?: FillPaintExtras; beforeId?: string },
): void {
  if (map.getSource(WORLD_SOURCE_ID)) {
    if (!map.getLayer(COUNTRIES_LAYER_ID)) {
      const layers = typeof map.getStyle === 'function' ? map.getStyle()?.layers : undefined
      const firstLayerAfterBg = layers?.find(l => l.id !== 'background')
      const beforeId = opts.beforeId || firstLayerAfterBg?.id
      map.addLayer(
        {
          id: COUNTRIES_LAYER_ID,
          type: 'fill',
          source: WORLD_SOURCE_ID,
          paint: {
            'fill-color': countryFill(opts.isDark),
            'fill-outline-color': countryOutline(opts.isDark),
            ...(opts.extraPaint ?? {}),
          },
        },
        beforeId,
      )
    }
    return
  }

  void loadWorldGeoJson().then((fc) => {
    if (!map.getSource(WORLD_SOURCE_ID)) {
      map.addSource(WORLD_SOURCE_ID, { type: 'geojson', data: fc })
    }
    if (!map.getLayer(COUNTRIES_LAYER_ID)) {
      const layers = typeof map.getStyle === 'function' ? map.getStyle()?.layers : undefined
      const firstLayerAfterBg = layers?.find(l => l.id !== 'background')
      const beforeId = opts.beforeId || firstLayerAfterBg?.id
      map.addLayer(
        {
          id: COUNTRIES_LAYER_ID,
          type: 'fill',
          source: WORLD_SOURCE_ID,
          paint: {
            'fill-color': countryFill(opts.isDark),
            'fill-outline-color': countryOutline(opts.isDark),
            ...(opts.extraPaint ?? {}),
          },
        },
        beforeId,
      )
    }
    if (typeof map.resize === 'function') {
      map.resize()
    }
  }).catch((err) => {
    console.error("[MapBaseLayer] Failed to load or set world geojson:", err)
  })
}

export function updateCountryBaseLayerTheme(map: MapLibreMap, isDark: boolean): void {
  if (!map.getLayer(COUNTRIES_LAYER_ID)) return
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-color', countryFill(isDark))
  map.setPaintProperty(COUNTRIES_LAYER_ID, 'fill-outline-color', countryOutline(isDark))
}

interface BaseMapOptions {
  renderWorldCopies: boolean
  zoom: number
  cooperativeGestures: boolean
  background?: string
  globe?: boolean
  lockRotation?: boolean
  showCompass?: boolean
}

export function createBaseMap(
  maplibre: typeof import('maplibre-gl'),
  container: HTMLElement,
  opts: BaseMapOptions,
): MapLibreMap {
  const options = {
    container,
    renderWorldCopies: opts.renderWorldCopies,
    preserveDrawingBuffer: true,
    style: {
      version: 8,
      ...(opts.globe ? { projection: { type: 'globe' as const } } : {}),
      sources: {},
      layers: [
        {
          id: 'background',
          type: 'background',
          paint: { 'background-color': opts.background ?? 'transparent' },
        },
      ],
    },
    center: [0, 20],
    zoom: opts.zoom,
    cooperativeGestures: opts.cooperativeGestures,
    ...(opts.lockRotation ? { dragRotate: false, touchZoomRotate: false } : {}),
  } as MapOptions
  const map = new maplibre.Map(options)
  map.addControl(new maplibre.NavigationControl({ showCompass: opts.showCompass ?? true }), 'top-right')
  return map
}

export function removeMapTabStops(container: HTMLElement | null): void {
  container
    ?.querySelectorAll<HTMLElement>('a, button, [tabindex], canvas')
    .forEach((el) => el.setAttribute('tabindex', '-1'))
}
const alpha3ToAlpha2: Record<string, string> = {}
Object.entries(countryMapData as Record<string, string>).forEach(([a2, a3]) => {
  alpha3ToAlpha2[a3.toUpperCase()] = a2.toUpperCase()
})

interface ChoroplethMapProps {
  data: DashboardMapData[]
  className?: string
  onCountryClick?: (country: string) => void
}

interface TooltipState {
  x: number
  y: number
  name: string
  count: number
}

function createMapAvailabilityStore() {
  let unavailable = false
  const listeners = new Set<() => void>()
  return {
    subscribe(listener: () => void) {
      listeners.add(listener)
      return () => { listeners.delete(listener) }
    },
    getSnapshot: () => unavailable,
    getServerSnapshot: () => false,
    reportUnavailable() {
      if (unavailable) return
      unavailable = true
      listeners.forEach(listener => listener())
    },
  }
}

function focusCountryList(node: HTMLUListElement | null) {
  node?.focus()
}

function CountryList({ rows, onPick, onClose }: {
  rows: DashboardMapData[]
  onPick: (country: string) => void
  onClose: () => void
}) {
  const [activeIndex, setActiveIndex] = useState(0)
  const handleKeyDown = (e: React.KeyboardEvent<HTMLUListElement>) => {
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault()
        setActiveIndex(i => (i + 1) % rows.length)
        break
      case 'ArrowUp':
        e.preventDefault()
        setActiveIndex(i => (i - 1 + rows.length) % rows.length)
        break
      case 'Home':
        e.preventDefault()
        setActiveIndex(0)
        break
      case 'End':
        e.preventDefault()
        setActiveIndex(rows.length - 1)
        break
      case 'Escape':
        e.preventDefault()
        onClose()
        break
      case 'Enter':
      case ' ':
        e.preventDefault()
        onPick(rows[activeIndex].country)
        break
    }
  }

  return (
    <ul
      ref={focusCountryList}
      id="choropleth-country-listbox"
      role="listbox"
      tabIndex={0}
      aria-label="Countries by request count — use arrow keys to navigate, Enter to filter"
      aria-activedescendant={`choropleth-country-option-${activeIndex}`}
      onKeyDown={handleKeyDown}
      onClick={(e) => {
        const country = (e.target as HTMLElement).closest<HTMLElement>('[role="option"]')?.dataset.country
        if (country !== undefined) onPick(country)
      }}
      className="mt-1 max-h-72 w-64 overflow-y-auto bg-popover/95 backdrop-blur-sm border rounded-md shadow-lg text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      {rows.map((d, i) => (
        <li
          key={d.country}
          id={`choropleth-country-option-${i}`}
          role="option"
          aria-selected={i === activeIndex}
          data-country={d.country}
          onMouseEnter={() => setActiveIndex(i)}
          className={`flex items-center justify-between gap-3 px-3 py-1.5 cursor-pointer ${i === activeIndex ? 'bg-accent text-accent-foreground' : ''}`}
        >
          <span className="truncate">
            <span className="text-muted-foreground tabular-nums mr-2">{i + 1}.</span>
            {resolveCountryName(d.country)}
          </span>
          <span className="text-muted-foreground tabular-nums">{formatCount(d.count)}</span>
        </li>
      ))}
    </ul>
  )
}

export const ChoroplethMap = React.memo(function ChoroplethMap({ data, className = '', onCountryClick }: ChoroplethMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const { theme } = useTheme()
  const isDarkRef = useRef(theme === 'dark')
  useEffect(() => { isDarkRef.current = theme === 'dark' }, [theme])

  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  // WebGL-unavailable fallback (headless / locked-down browser). MapLibre's
  // constructor throws `webglcontextcreationerror` when no GL context can be
  // created; left unguarded that throw propagates out of the mount effect into
  // the route error boundary (app/error.tsx). The sr-only table + keyboard
  // picker below render the same data, so we degrade to a placeholder instead.
  // MapLibre is an external resource: subscribe to its initialization result
  // without synchronously setting React state in the mount effect. SSR always
  // sees the same initial snapshot; only the browser initializes WebGL.
  const [mapAvailability] = useState(createMapAvailabilityStore)
  const mapError = useSyncExternalStore(
    mapAvailability.subscribe, mapAvailability.getSnapshot, mapAvailability.getServerSnapshot,
  )
  // A-2 (a11y / WCAG 2.1.1 Keyboard): the MapLibre canvas exposes nothing
  // to keyboard users — mousemove tooltips and click-to-filter were
  // mouse-only. The sr-only table (A-1) gives screen-reader users the data;
  // this picker gives sighted keyboard users an equivalent on-demand
  // listbox. Toggle button lives top-left so it stays clear of the Legend
  // (bottom-left) and NavigationControl (top-right).
  const [pickerOpen, setPickerOpen] = useState(false)
  const pickerToggleRef = useRef<HTMLButtonElement>(null)
  const dataMapRef = useRef<Map<string, number>>(new Map())
  // Reverse lookup: GeoJSON-feature name → alpha-2 country code. Built from
  // the data array (whose .country IS the alpha-2 code), so it stays in sync
  // with whatever the backend actually returns. Avoids depending on the
  // GeoJSON feature id (MapLibre can drop string ids in click events) or on
  // country-codes.json being complete (it has 168 codes vs 180 features).
  const nameToCodeRef = useRef<Map<string, string>>(new Map())
  const onCountryClickRef = useRef(onCountryClick)
  useEffect(() => { onCountryClickRef.current = onCountryClick }, [onCountryClick])

  useEffect(() => {
    if (!mapContainer.current) return

    if (!map.current) {
      try {
      map.current = createBaseMap(maplibregl, mapContainer.current, {
        renderWorldCopies: false,
        zoom: 0.5,
        lockRotation: true,
        showCompass: false,
        // Don't hijack page scroll: a plain mousewheel scrolls the PAGE; only
        // ⌘/Ctrl + wheel (or the +/- buttons) zooms the map. (cooperative gestures)
        cooperativeGestures: true,
      })

      requestAnimationFrame(() => {
        map.current?.resize();
      });

      map.current.on('load', () => {
        if (!map.current) return

        removeMapTabStops(mapContainer.current)

        addCountryBaseLayer(map.current, { isDark: isDarkRef.current })

        // Hover events. Previously wrapped in a rAF-throttle helper, but
        // Turbopack's minifier was inlining the throttle's closures as
        // bare outer-scope assignments that collided with the click
        // handler's `e` parameter — the mousemove handler silently never
        // fired in prod while click on the same layer worked fine.
        // Re-throttle inline if the per-frame setState becomes a profile
        // hot spot; today's setTooltip is cheap enough to run unthrottled.
        map.current.on('mousemove', 'countries', (e: maplibregl.MapLayerMouseEvent) => {
          if (e.features && e.features.length > 0) {
            const feature = e.features[0]
            const name = feature.properties?.name
            const count = dataMapRef.current.get(name) || 0

            setTooltip({
              x: Math.min(e.point.x + 15, (mapContainer.current?.clientWidth || 500) - 160),
              y: Math.min(e.point.y + 15, (mapContainer.current?.clientHeight || 300) - 80),
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
      } catch {
        // WebGL unavailable (headless / locked-down browser). The sr-only
        // table + keyboard picker below still render the data; show a
        // placeholder instead of throwing into the route error boundary.
        map.current = null
        mapAvailability.reportUnavailable()
      }
    }

    return () => {
      map.current?.remove()
      map.current = null
    }
  }, [mapAvailability])

  useEffect(() => {
    if (!map.current) return
    updateCountryBaseLayerTheme(map.current, theme === 'dark')
    if (!data) return

    // Update data map for hover lookups + reverse map for click-to-code.
    const newDataMap = new Map<string, number>()
    const newNameToCode = new Map<string, string>()
    data.forEach(d => {
      const englishName = resolveCountryName(d.country)
      const countryName = NORMALIZE_COUNTRY[englishName] || englishName
      newDataMap.set(countryName, d.count)
      if (d.country) newNameToCode.set(countryName, d.country.toUpperCase())
    })
    dataMapRef.current = newDataMap
    nameToCodeRef.current = newNameToCode

    let pendingStyleDataHandler: (() => void) | null = null

    const updateData = () => {
      if (!map.current?.getLayer('countries')) {
        // Layer not added yet — the init effect's 'load' handler adds
        // it. Listen for 'styledata' (fires whenever layers/sources
        // change) and retry. Previously this used setTimeout(100ms)
        // polling, which added 100-300ms of artificial latency to the
        // first paint when data arrived before the map's 'load' event.
        // 'styledata' fires synchronously after addLayer() so this
        // path resolves with zero polling delay.
        if (pendingStyleDataHandler) {
          map.current?.off('styledata', pendingStyleDataHandler)
        }
        const onStyleData = () => {
          if (map.current?.getLayer('countries')) {
            map.current.off('styledata', onStyleData)
            pendingStyleDataHandler = null
            map.current.resize()
            updateData()
          }
        }
        pendingStyleDataHandler = onStyleData
        map.current?.on('styledata', onStyleData)
        return
      }

      if (!data.length) {
        map.current.setPaintProperty('countries', 'fill-color', countryFill(theme === 'dark'))
        map.current.resize()
        return
      }

      const max = Math.max(...data.map(d => d.count))
      if (!(max > 0)) {
        map.current.setPaintProperty('countries', 'fill-color', countryFill(theme === 'dark'))
        map.current.resize()
        return
      }
      const [first, ...others] = data.map((d): [string, string] => {
        const intensity = 0.2 + (d.count / max) * 0.8
        const englishName = resolveCountryName(d.country)
        return [NORMALIZE_COUNTRY[englishName] || englishName, `rgba(59, 130, 246, ${intensity})`]
      })

      const matchExpression: maplibregl.ExpressionSpecification = [
        'match', ['get', 'name'], first[0], first[1], ...others.flat(), countryFill(theme === 'dark'),
      ]
      map.current.setPaintProperty('countries', 'fill-color', matchExpression)
      map.current.resize()
    }

    updateData()

    return () => {
      if (pendingStyleDataHandler) {
        map.current?.off('styledata', pendingStyleDataHandler)
        pendingStyleDataHandler = null
      }
    }
  }, [data, theme])

  const maxCount = data.length > 0 ? Math.max(...data.map(d => d.count)) : 0

  // a11y: derive a screen-reader summary + full sorted table from the same data
  // the visual layer uses. The map is a <canvas> with no inherent semantics
  // (mirrors the PlotlyChart / ChartA11yTable pattern). Sort once here and
  // reuse for both the aria-label top-5 and the sr-only table.
  const sortedForA11y = React.useMemo(() => {
    if (!data || data.length === 0) return []
    return [...data]
      .filter(d => d && d.country)
      .sort((a, b) => b.count - a.count)
  }, [data])

  const ariaLabel = React.useMemo(() => {
    if (sortedForA11y.length === 0) {
      return 'World map of requests by country. No data available.'
    }
    const top = sortedForA11y.slice(0, 5)
    const topPart = top
      .map(d => `${resolveCountryName(d.country)} ${formatCompactCount(d.count)}`)
      .join(', ')
    return `World map of requests by country, showing top countries: ${topPart}.`
  }, [sortedForA11y])

  // Cap the sr-only table at 50 rows to avoid SR-user fatigue on long-tail
  // datasets. The aria-label still summarizes the top 5 above.
  // Shared with the A-2 keyboard picker so both surfaces stay in sync.
  const tableRows = React.useMemo(() => sortedForA11y.slice(0, 50), [sortedForA11y])

  const closePicker = () => {
    setPickerOpen(false)
    // requestAnimationFrame lets React's render flush before we move focus,
    // otherwise the toggle button may not be focusable yet on re-render.
    requestAnimationFrame(() => pickerToggleRef.current?.focus())
  }

  useMapResize(map, mapContainer)

  return (
    <div
      className={`relative min-h-[300px] w-full h-full rounded-lg overflow-hidden bg-background ${className}`}
      // role="group" (not "img"): the wrapper holds the interactive MapLibre
      // controls + the sr-only data table below, so it can't be an "img"
      // (axe: nested-interactive — an img must not contain focusable
      // descendants). group is a structural role that legitimately groups
      // interactive content under the aria-label.
      role="group"
      aria-label={ariaLabel}
    >
      {mapError ? (
        <div className="absolute inset-0 flex items-center justify-center px-4 text-center text-xs text-muted-foreground" aria-hidden="true">
          Interactive map unavailable in this browser. See the country list below.
        </div>
      ) : (
        <div ref={mapContainer} className="absolute inset-0 w-full h-full min-h-[300px]" aria-hidden="true" />
      )}

      {/* Screen-reader-only data table. The MapLibre canvas exposes nothing
          to assistive tech, so this sibling renders the same data as a real
          navigable table. Hidden visually via Tailwind's `sr-only`. */}
      <table className="sr-only">
        <caption>
          {tableRows.length > 0
            ? `Country traffic data — ${tableRows.length} countries shown${sortedForA11y.length > tableRows.length ? ` of ${sortedForA11y.length} total` : ''}.`
            : 'Country traffic data — no countries available.'}
        </caption>
        <thead>
          <tr>
            <th scope="col">Rank</th>
            <th scope="col">Country</th>
            <th scope="col">Requests</th>
          </tr>
        </thead>
        <tbody>
          {tableRows.map((d, i) => (
            <tr key={d.country}>
              <td>{i + 1}</td>
              <td>{resolveCountryName(d.country)}</td>
              <td>{formatCount(d.count)}</td>
            </tr>
          ))}
        </tbody>
      </table>


      {/* A-2 keyboard picker: on-demand visible listbox for sighted keyboard
          users. Mirrors the sr-only table (same tableRows source) so all
          three a11y surfaces — aria-label summary, sr-only table, this
          picker — stay in sync. Toggle is top-3 left-3 to avoid the legend
          (bottom-left) and NavigationControl (top-right). */}
      {tableRows.length > 0 && (
        <div className="absolute top-3 left-3 z-20">
          <button
            ref={pickerToggleRef}
            type="button"
            onClick={() => setPickerOpen(o => !o)}
            aria-expanded={pickerOpen}
            aria-haspopup="listbox"
            aria-controls="choropleth-country-listbox"
            className="bg-background/95 backdrop-blur-sm border rounded-md px-3 py-1.5 text-xs font-medium shadow-md hover:bg-accent focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
          >
            {pickerOpen ? 'Hide countries' : 'Browse countries'}
          </button>
          {pickerOpen && (
            <CountryList
              // A changed row count resets selection before the next paint,
              // without an effect that can briefly expose an invalid index.
              key={tableRows.length}
              rows={tableRows}
              onPick={(country) => {
                onCountryClickRef.current?.(country)
                closePicker()
              }}
              onClose={closePicker}
            />
          )}
        </div>
      )}

      {/* Tooltip */}
      {tooltip && (
        <div
          className="absolute z-50 pointer-events-none bg-popover/95 backdrop-blur-sm border shadow-lg rounded-md px-3 py-2 text-sm transition-opacity"
          style={{
            left: tooltip.x,
            top: tooltip.y
          }}
        >
          <div className="font-semibold text-foreground">{tooltip.name}</div>
          <div className="text-muted-foreground">{formatCount(tooltip.count)} requests</div>
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
              {formatCompactCount(maxCount)}
            </span>
          </div>
        </div>
      )}
    </div>
  )
})


interface ShieldingMapProps {
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

interface ShieldingTooltipInfo {
  clientX: number
  clientY: number
  props: Record<string, unknown>
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

export function efficiencyColor(ratio: number | null | undefined): string {
  // A negative ratio (transit delta < 0 — possible on sparse routes or with
  // clock skew) is "no meaningful efficiency", NOT "excellent": treat it like
  // null/indigo rather than letting `ratio < 1.5` paint it green. (L5)
  if (ratio == null || ratio < 0) return '#6366f1'
  if (ratio < 1.5) return '#22c55e'
  if (ratio < 3.0) return '#eab308'
  return CHART_COLORS.danger
}

export function lineWidth(requests: number): number {
  return Math.max(1.5, Math.min(6, 1.5 + Math.log10(Math.max(1, requests)) * 1.2))
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function ShieldingTooltip({ info }: { info: ShieldingTooltipInfo }) {
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
        <PopLabel code={String(props.edge_pop ?? '')} /> <span className="text-muted-foreground">→</span>{' '}
        <PopLabel code={String(props.shield_pop ?? '')} className="text-purple-500" />
      </div>
      <div className="mt-2 space-y-1">
        {/* These percentiles are the edge→shield transit delta (E→S), NOT a
            TCP round-trip time — label them the same way the data table does
            so the two surfaces don't disagree. (L4) */}
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{formatCount(Number(props.requests))}</span>
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
            <span className="text-[11px] font-semibold tabular-nums">{formatCount(Number(props.distance_km))}km</span>
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
              style={{ color: lowSample ? CHART_COLORS.neutral : efficiencyColor(Number(props.efficiency_ratio)) }}
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

export function buildArcFeatures(rows: readonly ShieldingRowInput[]): GeoJSON.FeatureCollection {
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
        color: row.low_sample ? CHART_COLORS.neutral : efficiencyColor(row.efficiency_ratio),
        line_width: lineWidth(row.requests ?? 0),
      },
    })
  }
  return { type: 'FeatureCollection', features }
}

export function buildDotFeatures(rows: readonly ShieldingRowInput[]): GeoJSON.FeatureCollection {
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

export function ShieldingMap({ rows, isLoading, edgeOnly, errored, className, expandable, fillHeight }: ShieldingMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const isDarkRef = useRef(false)
  const [mapReady, setMapReady] = useState(false)
  const [tooltip, setTooltip] = useState<ShieldingTooltipInfo | null>(null)
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
      // Default to a 3D globe: edge→shield arcs are great-circle paths
      // that wrap the planet, and on a flat Mercator map a long route
      // reads as a misleading straight slash across the world. On a globe
      // the same arc curves naturally over the surface. The GlobeControl
      // below lets the operator flip back to flat Mercator at will.
      map.current = createBaseMap(maplibregl, mapContainer.current, {
        renderWorldCopies: true,
        zoom: 1,
        globe: true,
        // The inline card sits mid-page, so a bare mousewheel would zoom the
        // map and swallow the page scroll. Cooperative gestures make a plain
        // wheel scroll the PAGE (with a brief "use ⌘/Ctrl + scroll to zoom"
        // hint); zoom still works via modifier+wheel, pinch, or the +/-
        // buttons. In the fullscreen dialog there's nothing to scroll behind
        // it, so let a bare wheel zoom the globe directly.
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
            'line-color': CHART_COLORS.danger,
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
            'circle-color': CHART_COLORS.info,
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
          const props: Record<string, unknown> = e.features[0].properties ?? {}
          setTooltip({ clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY, props })
        })
        map.current.on('mousemove', 'arc-lines', rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
          if (!e.features?.length) return
          const props: Record<string, unknown> = e.features[0].properties ?? {}
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
        // Reported from a microtask: MapLibre's failure is an external-system event, not render state.
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
      .map(r => `${r.edge_pop} to ${r.shield_pop} ${formatCountMaybe(r.requests) ?? '0'} requests`)
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
          {keyByOccurrence(a11yRows, (row) => `${row.edge_pop}-${row.shield_pop}`).map(({ key, item: r }) => (
            <tr key={key}>
              <td><PopLabel code={r.edge_pop} /></td>
              <td><PopLabel code={r.shield_pop} /></td>
              <td>{formatCount(r.requests ?? 0)}{r.low_sample ? ' (low sample)' : ''}</td>
              <td>{r.p50_ms != null ? r.p50_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.p95_ms != null ? r.p95_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.p99_ms != null ? r.p99_ms.toFixed(0) : 'n/a'}</td>
              <td>{r.distance_km != null ? formatCount(r.distance_km) : 'n/a'}</td>
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


type NetworkHealth = components['schemas']['NetworkHealthResponse']
/** The slice of the network-health response the map renders. */
export type NetworkMapData = Partial<Pick<NetworkHealth, 'buckets' | 'map_buckets' | 'cities'>>
type MapCity = components['schemas']['NetworkMapCity']

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
  if (band.highIsGood) return val >= g ? '#22c55e' : val >= y ? '#eab308' : val >= o ? '#f97316' : CHART_COLORS.danger
  return val <= g ? '#22c55e' : val <= y ? '#eab308' : val <= o ? '#f97316' : CHART_COLORS.danger
}

interface UseMapInitArgs {
  mapContainer: MutableRefObject<HTMLDivElement | null>
  isDark: boolean; isDarkRef: MutableRefObject<boolean>
  dmaDataRef: MutableRefObject<Record<number, CityTooltipData>>
  setTooltip: (t: TooltipInfo | null) => void; onMapError?: () => void
}

/**
 * Initializes the MapLibre instance, adds sources/layers, and wires hover
 * handlers for the city-scatter and dma-fill layers.
 */
export function useMapInit({ mapContainer, isDark, isDarkRef, dmaDataRef, setTooltip, onMapError }: UseMapInitArgs): MutableRefObject<maplibregl.Map | null> {
  const map = useRef<maplibregl.Map | null>(null)
  useEffect(() => {
    if (!mapContainer.current) return
    let stopResizeObserver: (() => void) | null = null

    if (!map.current) {
      try {
        map.current = createBaseMap(maplibregl, mapContainer.current, { renderWorldCopies: false, zoom: 1, cooperativeGestures: true })
        map.current.on('error', (e) => { console.error('[NetworkMap:map] MapLibre error event:', e) })
        stopResizeObserver = observeMapResize(mapContainer.current, () => map.current)

        map.current.on('load', () => {
          if (!map.current) return
          map.current.addSource('dma', { type: 'geojson', data: '/geo/dma.geojson' })
          map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
          addCountryBaseLayer(map.current, { isDark: isDarkRef.current, extraPaint: { 'fill-opacity': 0.8 } })
          map.current.addLayer({ id: 'dma-fill', type: 'fill', source: 'dma', paint: { 'fill-opacity': 0.7, 'fill-color': 'transparent' } })
          map.current.addLayer({
            id: 'city-scatter', type: 'circle', source: 'heatmap',
            paint: {
              'circle-radius': ['get', 'radius'], 'circle-color': ['get', 'color'], 'circle-opacity': 0.8,
              'circle-stroke-width': 1, 'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff',
            },
          })

          const pushTooltip = (e: maplibregl.MapLayerMouseEvent, cityData: CityTooltipData) => {
            setTooltip({
              clientX: e.originalEvent.clientX, clientY: e.originalEvent.clientY,
              city: asCityString(cityData.city), country: asCityString(cityData.country) || undefined, cityData,
            })
          }

          const bindHover = (layer: string, resolveData: (f: maplibregl.MapGeoJSONFeature) => CityTooltipData | undefined) => {
            if (!map.current) return
            map.current.on('mouseenter', layer, (e) => {
              if (!e.features?.length || !map.current) return
              const cityData = resolveData(e.features[0])
              if (!cityData) return
              map.current.getCanvas().style.cursor = 'pointer'
              pushTooltip(e, cityData)
            })
            map.current.on('mousemove', layer, rafThrottle((e: maplibregl.MapLayerMouseEvent) => {
              if (!e.features?.length) return
              const cityData = resolveData(e.features[0])
              if (cityData) pushTooltip(e, cityData)
            }))
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

interface UseMapDataArgs {
  map: MutableRefObject<maplibregl.Map | null>
  dmaDataRef: MutableRefObject<Record<number, CityTooltipData>>
  data: NetworkMapData | null | undefined
  bucketIdx: number; metric: string; isDark: boolean
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
              city: city.name, country: c.country || '', color,
              radius: Math.max(3, Math.min(15, Math.log10(reqs + 1) * 2.5)), reqs,
              health_score: c.health_score, rtt_med_us: c.rtt_med_us, avg_ploss: c.avg_ploss, error_pct: c.error_pct,
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
        'countries', 'fill-color',
        (Object.keys(countryScores).length > 0 ? matchCountry : countryFill(isDark)) as MapFillColor
      )

      const source = map.current.getSource('heatmap') as maplibregl.GeoJSONSource
      source?.setData({ type: 'FeatureCollection', features })

      if (map.current.getLayer('dma-fill')) {
        const dmaEntries = Object.entries(dmaColors)
        if (dmaEntries.length > 0) {
          const matchDma: unknown[] = ['match', ['get', 'dma_code']]
          dmaEntries.forEach(([code, color]) => { matchDma.push(Number(code), color) })
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

export const METRIC_OPTIONS = [
  { value: 'health_score', label: 'Health Score' },
  { value: 'rtt_med_us', label: 'Median RTT' },
  { value: 'avg_ploss', label: 'Packet Loss' },
  { value: 'error_pct', label: 'Error Rate' },
  { value: 'throughput_bps', label: 'Throughput' },
]

export const SPEED_OPTIONS = [
  { value: 1000, label: '1×' }, { value: 500, label: '2×' },
  { value: 200, label: '5×' }, { value: 100, label: '10×' },
]

export const STEP_OPTIONS = [
  { value: 1, label: '1 sec' }, { value: 5, label: '5 sec' }, { value: 10, label: '10 sec' },
  { value: 30, label: '30 sec' }, { value: 60, label: '1 min' }, { value: 300, label: '5 min' },
  { value: 900, label: '15 min' }, { value: 1800, label: '30 min' }, { value: 3600, label: '1 hr' },
  { value: 7200, label: '2 hr' }, { value: 14400, label: '4 hr' },
]

/** Per-city values behind a tooltip: the map-bucket cell fields, or a GeoJSON feature's properties. */
export type CityTooltipData = Record<string, unknown>

export interface TooltipInfo {
  clientX: number; clientY: number; city: string; country?: string; cityData: CityTooltipData
}

const numberOrNull = (v: unknown): number | null => (typeof v === 'number' ? v : null)

export function formatMetricValue(val: number | null | undefined, metric: string): string {
  if (val == null) return '—'
  if (metric === 'health_score') return `${val.toFixed(0)}/100`
  if (metric === 'rtt_med_us') return `${(val / 1000).toFixed(1)} ms`
  if (metric === 'avg_ploss') return `${(val * 100).toFixed(2)}%`
  if (metric === 'error_pct') return `${val.toFixed(2)}%`
  if (metric === 'throughput_bps') {
    if (val >= 1e9) return `${(val / 1e9).toFixed(1)} Gbps`
    if (val >= 1e6) return `${(val / 1e6).toFixed(1)} Mbps`
    if (val >= 1e3) return `${(val / 1e3).toFixed(1)} Kbps`
    return `${val.toFixed(0)} bps`
  }
  return String(val)
}

export function MapTooltip({ info, metric }: { info: TooltipInfo; metric: string }) {
  const metricLabel = METRIC_OPTIONS.find((m) => m.value === metric)?.label ?? metric
  const metricVal = numberOrNull(metric === 'health_score' ? info.cityData.health_score : info.cityData[metric])
  const reqs = numberOrNull(info.cityData.reqs) ?? 0
  const flipLeft = info.clientX > window.innerWidth * 0.7

  return (
    <div
      data-testid="map-tooltip"
      data-metric={metric}
      style={{
        position: 'fixed', top: info.clientY - 12,
        left: flipLeft ? info.clientX - 14 : info.clientX + 14,
        transform: flipLeft ? 'translate(-100%, -100%)' : 'translateY(-100%)',
        zIndex: 9999, pointerEvents: 'none',
      }}
      className="bg-popover text-popover-foreground border border-border rounded-lg shadow-xl px-3 py-2.5 font-sans min-w-[160px]"
    >
      <div className="font-semibold text-xs leading-tight">{info.city || 'Unknown'}</div>
      {info.country && <div className="text-[10px] text-muted-foreground mt-0.5">{info.country}</div>}
      <div className="mt-2 space-y-1">
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">{metricLabel}</span>
          <span className="text-[11px] font-semibold tabular-nums">{formatMetricValue(metricVal, metric)}</span>
        </div>
        {metric !== 'health_score' && numberOrNull(info.cityData.health_score) != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Health Score</span>
            <span className="text-[11px] font-semibold tabular-nums">{Number(info.cityData.health_score).toFixed(0)}/100</span>
          </div>
        )}
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{formatCount(reqs)}</span>
        </div>
      </div>
    </div>
  )
}

interface PlaybackControlsProps {
  playing: boolean; setPlaying: (p: boolean) => void
  bucketIdx: number; setBucketIdx: (i: number) => void; bucketsLength: number
  firstBucketLabel: string; currentBucketLabel: string; lastBucketLabel: string
  metric: string; onMetricChange: (m: string) => void
  bucketSeconds: number; onBucketChange: (b: number) => void
  playInterval: number; setPlayInterval: (n: number) => void
  mapAsn: string; onAsnChange: (a: string) => void
  asnOptions: Array<{ value: string; label: string }>
}

export function PlaybackControls({
  playing, setPlaying, bucketIdx, setBucketIdx, bucketsLength,
  firstBucketLabel, currentBucketLabel, lastBucketLabel,
  metric, onMetricChange, bucketSeconds, onBucketChange,
  playInterval, setPlayInterval, mapAsn, onAsnChange, asnOptions,
}: PlaybackControlsProps) {
  const selects = [
    { label: 'Metric', width: 'w-[150px]', value: metric, onChange: onMetricChange, options: METRIC_OPTIONS },
    { label: 'Time step', width: 'w-[90px]', value: String(bucketSeconds), onChange: (v: string) => onBucketChange(Number(v)), options: STEP_OPTIONS.map((o) => ({ value: String(o.value), label: o.label })) },
    { label: 'Playback speed', width: 'w-[68px]', value: String(playInterval), onChange: (v: string) => setPlayInterval(Number(v)), options: SPEED_OPTIONS.map((o) => ({ value: String(o.value), label: o.label })) },
    { label: 'ASN filter', width: 'w-[180px]', value: mapAsn, onChange: onAsnChange, options: [{ value: 'all', label: 'All ASNs' }, ...asnOptions] },
  ]
  return (
    <div
      data-testid="playback-controls"
      data-playing={String(playing)}
      data-bucket-idx={String(bucketIdx)}
      data-buckets-length={String(bucketsLength)}
      data-metric={String(metric)}
      data-current-label={String(currentBucketLabel)}
      className="absolute bottom-4 left-4 right-4 bg-background/90 backdrop-blur-sm p-3 rounded-lg border shadow-lg z-10 space-y-2"
    >
      <div className="flex items-center gap-3">
        <Button
          variant="outline" size="icon"
          aria-label={playing ? 'Pause map playback' : 'Play map playback'}
          className="shrink-0 h-8 w-8"
          onClick={() => setPlaying(!playing)}
        >
          {playing ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
        </Button>
        <div className="flex-1 min-w-0 flex flex-col gap-1">
          <div className="flex justify-between text-[10px] text-muted-foreground font-mono">
            <span>{firstBucketLabel}</span>
            <span className="font-semibold text-foreground">{currentBucketLabel}</span>
            <span>{lastBucketLabel}</span>
          </div>
          <Slider
            value={[bucketIdx]} min={0} max={bucketsLength - 1} step={1}
            aria-label="Time scrubber" aria-valuetext={currentBucketLabel}
            onValueChange={(val) => {
              if (Array.isArray(val) && val.length) setBucketIdx(val[0])
              setPlaying(false)
            }}
          />
        </div>
      </div>

      <div className="flex items-center gap-2 flex-wrap">
        {selects.map((sel) => (
          <Select key={sel.label} value={sel.value} onValueChange={(v) => v && sel.onChange(v)}>
            <SelectTrigger aria-label={sel.label} className={`h-7 text-xs ${sel.width}`}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {sel.options.map((o) => (
                <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        ))}
      </div>
    </div>
  )
}

interface NetworkMapProps {
  data: NetworkMapData | null | undefined; isLoading?: boolean; error?: unknown; className?: string
  metric: string; onMetricChange: (m: string) => void
  bucketSeconds: number; onBucketChange: (b: number) => void
  mapAsn: string; onAsnChange: (a: string) => void
  asnOptions: Array<{ value: string; label: string }>
}

export function NetworkMap({
  data, isLoading, error, className, metric, onMetricChange,
  bucketSeconds, onBucketChange, mapAsn, onAsnChange, asnOptions,
}: NetworkMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const metricRef = useRef(metric)
  const isDarkRef = useRef(false)
  const dmaDataRef = useRef<Record<number, CityTooltipData>>({})

  const { theme } = useTheme()
  const { timezone } = useTimezoneStore()
  const isDark = theme === 'dark'

  const [bucketIdx, setBucketIdx] = useState(0)
  const [playing, setPlaying] = useState(false)
  const [playInterval, setPlayInterval] = useState(100)
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)
  const [mapError, setMapError] = useState(false)

  useEffect(() => { metricRef.current = metric }, [metric])
  useEffect(() => { isDarkRef.current = isDark }, [isDark])

  useEffect(() => {
    const bucketCount = data?.buckets?.length ?? 0
    if (!playing || !bucketCount) return
    const id = setInterval(() => setBucketIdx((i) => (i + 1) % bucketCount), playInterval)
    return () => clearInterval(id)
  }, [playing, data?.buckets?.length, playInterval])

  const buckets = data?.buckets
  const [seenBuckets, setSeenBuckets] = useState<typeof buckets>(undefined)
  if (buckets !== seenBuckets) {
    setSeenBuckets(buckets)
    if (buckets) setBucketIdx(buckets.length > 0 ? buckets.length - 1 : 0)
    setPlaying(false)
  }

  const map = MapLayer.useMapInit({ mapContainer, isDark, isDarkRef, dmaDataRef, setTooltip, onMapError: () => setMapError(true) })
  MapLayer.useMapData({ map, dmaDataRef, data, bucketIdx, metric, isDark, setTooltip })

  const currentBucketLabel = formatBucket(data?.buckets?.[bucketIdx] || '', timezone)
  const firstBucketLabel = formatBucket(data?.buckets?.[0] || '', timezone)
  const lastBucketLabel = formatBucket(data?.buckets?.[(data?.buckets?.length ?? 0) - 1] || '', timezone)

  const currentBucketRows = React.useMemo(() => {
    const bucket = data?.map_buckets?.[bucketIdx]
    if (!bucket) return []
    const cityEntries = data?.cities ?? []
    return bucket.cities
      .filter((c) => c.metro_code != null)
      .map((c) => ({
        code: String(c.metro_code),
        city: cityEntries[c.city_idx]?.name ?? '',
        value: (c as Record<string, unknown>)[metric],
      }))
      .sort((a, b) => (Number(b.value) || 0) - (Number(a.value) || 0))
      .slice(0, 50)
  }, [data, bucketIdx, metric])
  const a11yLabel = currentBucketRows.length > 0
    ? `Network map showing ${metric} per metro for bucket ${currentBucketLabel}. ${currentBucketRows.length} metros active.`
    : `Network map. No metros active for the selected bucket / ASN filter.`

  return (
    <>
      <div className={`relative flex flex-col border rounded-lg overflow-hidden ${className} min-h-[400px]`} role="group" aria-label={a11yLabel}>
        {mapError ? (
          <div className="w-full h-[400px] flex items-center justify-center px-4 text-center text-xs text-muted-foreground" aria-hidden="true">
            Interactive map unavailable in this browser. See the metro table below.
          </div>
        ) : (
          <div ref={mapContainer} className="w-full h-[400px]" aria-hidden="true" />
        )}
        <table className="sr-only">
          <caption>Network map — top {currentBucketRows.length} metros by {metric} for bucket {currentBucketLabel}.</caption>
          <thead>
            <tr><th scope="col">Rank</th><th scope="col">Metro code</th><th scope="col">{metric}</th></tr>
          </thead>
          <tbody>
            {currentBucketRows.map((r, i) => (
              <tr key={r.code}>
                <td>{i + 1}</td>
                <td>{r.code}</td>
                <td>{r.value != null ? (typeof r.value === 'number' ? r.value.toFixed(2) : String(r.value)) : 'n/a'}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {!data?.buckets?.length ? (
          <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/50 backdrop-blur-sm p-4 text-center text-sm">
            {isLoading ? (
              'Loading map data...'
            ) : error != null ? (
              <div role="alert" className="max-w-sm text-destructive">
                <div className="font-medium">Failed to load map data.</div>
                <div className="mt-1 text-xs opacity-80">
                  {error instanceof Error ? error.message : 'The backend returned an error.'}
                </div>
              </div>
            ) : (
              'No map data available'
            )}
          </div>
        ) : (
          <PlaybackControls
            playing={playing} setPlaying={setPlaying} bucketIdx={bucketIdx} setBucketIdx={setBucketIdx}
            bucketsLength={data.buckets?.length ?? 0} firstBucketLabel={firstBucketLabel}
            currentBucketLabel={currentBucketLabel} lastBucketLabel={lastBucketLabel}
            metric={metric} onMetricChange={onMetricChange} bucketSeconds={bucketSeconds}
            onBucketChange={onBucketChange} playInterval={playInterval} setPlayInterval={setPlayInterval}
            mapAsn={mapAsn} onAsnChange={onAsnChange} asnOptions={asnOptions}
          />
        )}
      </div>

      {tooltip && typeof document !== 'undefined' && createPortal(<MapTooltip info={tooltip} metric={metric} />, document.body)}
    </>
  )
}

type CoordTree = number | CoordTree[]
const isPosition = (coords: CoordTree[]): coords is number[] => typeof coords[0] === 'number'

function geoBbox(geometry: { coordinates: CoordTree[] }): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  const walk = (coords: CoordTree[]) => {
    if (isPosition(coords)) {
      const [x, y] = coords
      if (x < minX) minX = x; if (y < minY) minY = y; if (x > maxX) maxX = x; if (y > maxY) maxY = y
      return
    }
    for (const c of coords) if (Array.isArray(c)) walk(c)
  }
  walk(geometry.coordinates)
  return [minX, minY, maxX, maxY]
}

interface PopTrafficMapProps { allPops: Record<string, { r: number; e: number }>; className?: string }
interface PopTooltipState { x: number; y: number; code: string; geo: PopGeo | undefined; requests: number; errorRate: number }

export const PopTrafficMap = React.memo(function PopTrafficMap({ allPops, className = '' }: PopTrafficMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const { theme } = useTheme()
  const isDarkRef = useRef(theme === 'dark')
  useEffect(() => { isDarkRef.current = theme === 'dark' }, [theme])
  const geoMap = usePopGeoStore((s) => s.map)
  const locations = usePopGeoStore((s) => s.locations)
  const [tooltip, setTooltip] = useState<PopTooltipState | null>(null)
  const [mapError, setMapError] = useState(false)
  const [zoomed, setZoomed] = useState(false)
  const geoMapRef = useRef(geoMap)
  useEffect(() => { geoMapRef.current = geoMap }, [geoMap])

  const popEntries = useMemo(() => Object.entries(locations).map(([code, [lat, lon]]) => {
    const stats = allPops[code] ?? allPops[code.toLowerCase()]
    const requests = stats?.r ?? 0
    const errors = stats?.e ?? 0
    const errorRate = requests > 0 ? errors / requests : 0
    const geo = geoMap[code]
    return { code, lat, lon, requests, errors, errorRate, country: geo?.country?.toUpperCase() ?? '' }
  }), [allPops, locations, geoMap])

  const countryAgg = useMemo(() => {
    const agg: Record<string, number> = {}
    for (const e of popEntries) if (e.country) agg[e.country] = (agg[e.country] ?? 0) + e.requests
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
            code: e.code, requests: e.requests, errors: e.errors, errorRate: e.errorRate,
            radius: active ? 4 + Math.log10(e.requests) * 3 : 3,
            color: active ? (e.errorRate > 0.05 ? CHART_COLORS.danger : CHART_COLORS.info) : '#9ca3af',
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
        map.current = createBaseMap(maplibregl, mapContainer.current, {
          renderWorldCopies: false, zoom: 0.5, background: isDarkRef.current ? '#1a1b23' : '#f0f4fa',
          lockRotation: true, showCompass: false, cooperativeGestures: true,
        })
        requestAnimationFrame(() => { map.current?.resize() })

        map.current.on('load', () => {
          if (!map.current) return
          removeMapTabStops(mapContainer.current)
          addCountryBaseLayer(map.current, { isDark: isDarkRef.current })
          map.current.addSource('pop-markers', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
          map.current.addLayer({
            id: 'pop-circles', type: 'circle', source: 'pop-markers',
            paint: {
              'circle-radius': ['get', 'radius'], 'circle-color': ['get', 'color'],
              'circle-opacity': ['get', 'opacity'], 'circle-stroke-width': 1, 'circle-stroke-color': 'rgba(255,255,255,0.4)',
            },
          })

          map.current.on('click', 'countries', (e) => {
            if (!map.current || !e.features?.length) return
            const feature = e.features[0]
            if (feature.geometry.type === 'Polygon' || feature.geometry.type === 'MultiPolygon') {
              map.current.fitBounds(geoBbox(feature.geometry), { padding: 40, maxZoom: 6 })
              setZoomed(true)
            }
          })

          map.current.on('mousemove', 'pop-circles', (e: maplibregl.MapLayerMouseEvent) => {
            if (!e.features?.length || !map.current) return
            const props = e.features[0].properties
            setTooltip({
              x: e.point.x, y: e.point.y, code: props?.code ?? '',
              geo: geoMapRef.current[(props?.code ?? '').toUpperCase()],
              requests: props?.requests ?? 0, errorRate: props?.errorRate ?? 0,
            })
            map.current.getCanvas().style.cursor = 'pointer'
          })

          map.current.on('mouseleave', 'pop-circles', () => {
            setTooltip(null)
            if (map.current) map.current.getCanvas().style.cursor = ''
          })
          map.current.on('mousemove', 'countries', () => { if (map.current) map.current.getCanvas().style.cursor = 'pointer' })
          map.current.on('mouseleave', 'countries', () => { if (map.current) map.current.getCanvas().style.cursor = '' })
        })
      } catch {
        map.current = null
        setMapError(true)
      }
    }
    return () => { map.current?.remove(); map.current = null }
  }, [])

  useEffect(() => {
    if (!map.current) return
    let pendingStyleDataHandler: (() => void) | null = null

    const updateLayers = () => {
      if (!map.current) return
      if (map.current.getLayer('background')) {
        map.current.setPaintProperty('background', 'background-color', theme === 'dark' ? '#1a1b23' : '#f0f4fa')
      }
      updateCountryBaseLayerTheme(map.current, theme === 'dark')

      const src = map.current.getSource('pop-markers') as maplibregl.GeoJSONSource | undefined
      if (src) src.setData(popGeoJson as GeoJSON.FeatureCollection)

      if (!map.current.getLayer('countries')) {
        if (pendingStyleDataHandler) map.current.off('styledata', pendingStyleDataHandler)
        const onStyleData = () => {
          if (map.current?.getLayer('countries')) {
            map.current.off('styledata', onStyleData)
            pendingStyleDataHandler = null
            updateLayers()
          }
        }
        pendingStyleDataHandler = onStyleData
        map.current.on('styledata', onStyleData)
        return
      }

      const countryEntries = Object.entries(countryAgg)
      const max = countryEntries.length > 0 ? Math.max(...countryEntries.map(([, v]) => v)) : 0
      if (countryEntries.length === 0 || !(max > 0)) {
        map.current.setPaintProperty('countries', 'fill-color', countryFill(theme === 'dark'))
        return
      }
      const matchExpression: (string | string[])[] = ['match', ['get', 'name']]
      for (const [cc, count] of countryEntries) {
        const intensity = 0.15 + (count / max) * 0.55
        const countryName = resolveCountryName(cc)
        matchExpression.push(NORMALIZE_COUNTRY[countryName] ?? countryName, `rgba(59, 130, 246, ${intensity})`)
      }
      matchExpression.push(countryFill(theme === 'dark'))
      map.current.setPaintProperty('countries', 'fill-color', matchExpression as MapFillColor)
    }

    updateLayers()
    return () => {
      if (pendingStyleDataHandler) {
        map.current?.off('styledata', pendingStyleDataHandler)
        pendingStyleDataHandler = null
      }
    }
  }, [popGeoJson, countryAgg, theme])

  useMapResize(map, mapContainer)

  const handleResetView = () => {
    if (!map.current) return
    map.current.flyTo({ center: [0, 20], zoom: 0.5 })
    setZoomed(false)
  }

  const maxCount = Math.max(1, ...popEntries.map((e) => e.requests))
  const sortedForA11y = useMemo(() => [...popEntries].sort((a, b) => b.requests - a.requests).slice(0, 50), [popEntries])

  const ariaLabel = useMemo(() => {
    if (sortedForA11y.length === 0) return 'World map of traffic by Fastly PoP. No data available.'
    const topPart = sortedForA11y.slice(0, 5).map((e) => `${e.code} ${formatCompactCount(e.requests)}`).join(', ')
    return `World map of traffic by Fastly PoP, showing top PoPs: ${topPart}.`
  }, [sortedForA11y])

  return (
    <div className={`relative min-h-[300px] w-full rounded-lg overflow-hidden bg-background ${className}`} role="group" aria-label={ariaLabel}>
      {mapError ? (
        <div className="absolute inset-0 flex items-center justify-center px-4 text-center text-xs text-muted-foreground" aria-hidden="true">
          Interactive map unavailable in this browser. See the PoP table below.
        </div>
      ) : (
        <div ref={mapContainer} className="absolute inset-0 w-full h-full min-h-[300px]" aria-hidden="true" />
      )}

      <table className="sr-only">
        <caption>{sortedForA11y.length > 0 ? `PoP traffic data — ${sortedForA11y.length} PoPs shown.` : 'PoP traffic data — no PoPs available.'}</caption>
        <thead>
          <tr><th scope="col">Rank</th><th scope="col">PoP</th><th scope="col">Location</th><th scope="col">Requests</th><th scope="col">Error Rate</th></tr>
        </thead>
        <tbody>
          {sortedForA11y.map((e, i) => (
            <tr key={e.code}>
              <td>{i + 1}</td><td>{e.code}</td><td>{formatPopGeo(geoMap[e.code])}</td>
              <td>{formatCount(e.requests)}</td><td>{(e.errorRate * 100).toFixed(2)}%</td>
            </tr>
          ))}
        </tbody>
      </table>

      {zoomed && (
        <button
          type="button" onClick={handleResetView}
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
          {tooltip.geo && <div className="text-muted-foreground text-xs">{formatPopGeo(tooltip.geo)}</div>}
          <div className="mt-1 text-xs space-y-0.5">
            <div><span className="text-muted-foreground">Requests:</span> <span className="tabular-nums">{formatCount(tooltip.requests)}</span></div>
            <div>
              <span className="text-muted-foreground">Error rate:</span>{' '}
              <span className={`tabular-nums ${tooltip.errorRate > 0.05 ? 'text-red-600 dark:text-red-400 font-medium' : tooltip.errorRate > 0.01 ? 'text-amber-600 dark:text-amber-400' : ''}`}>
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
