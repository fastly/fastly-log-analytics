'use client'

import React, { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { DashboardMapData } from '@/types/api'
import countryCodes from '@/lib/country-codes.json'
import { countryFill } from '@/components/Map/colors'
import { addCountryBaseLayer } from '@/components/Map/baseLayers'
import { formatCompactCount, resolveCountryName } from '@/lib/format'

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

export const ChoroplethMap = React.memo(function ChoroplethMap({ data, className = '', onCountryClick }: ChoroplethMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const { theme } = useTheme()

  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  // WebGL-unavailable fallback (headless / locked-down browser). MapLibre's
  // constructor throws `webglcontextcreationerror` when no GL context can be
  // created; left unguarded that throw propagates out of the mount effect into
  // the route error boundary (app/error.tsx). The sr-only table + keyboard
  // picker below render the same data, so we degrade to a placeholder instead.
  const [mapError, setMapError] = useState(false)
  // A-2 (a11y / WCAG 2.1.1 Keyboard): the MapLibre canvas exposes nothing
  // to keyboard users — mousemove tooltips and click-to-filter were
  // mouse-only. The sr-only table (A-1) gives screen-reader users the data;
  // this picker gives sighted keyboard users an equivalent on-demand
  // listbox. Toggle button lives top-left so it stays clear of the Legend
  // (bottom-left) and NavigationControl (top-right).
  const [pickerOpen, setPickerOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState(0)
  const pickerToggleRef = useRef<HTMLButtonElement>(null)
  const pickerListRef = useRef<HTMLUListElement>(null)
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
        zoom: 0.5,
        dragRotate: false,
        touchZoomRotate: false,
        // Don't hijack page scroll: a plain mousewheel scrolls the PAGE; only
        // ⌘/Ctrl + wheel (or the +/- buttons) zooms the map. (cooperative gestures)
        cooperativeGestures: true,
      })

      map.current.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')

      map.current.on('load', () => {
        if (!map.current) return

        addCountryBaseLayer(map.current, { isDark: theme === 'dark' })

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
        setMapError(true)
      }
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
      const englishName = resolveCountryName(d.country)
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
        map.current.setPaintProperty('countries', 'fill-color', countryFill(theme === 'dark'))
        return
      }

      const max = Math.max(...data.map(d => d.count))
      const matchExpression: any[] = ['match', ['get', 'name']]

      data.forEach(d => {
        const intensity = 0.2 + (d.count / max) * 0.8
        const englishName = resolveCountryName(d.country)
        const countryName = NORMALIZE_COUNTRY[englishName] || englishName
        matchExpression.push(countryName)
        matchExpression.push(`rgba(59, 130, 246, ${intensity})`)
      })

      matchExpression.push(countryFill(theme === 'dark'))
      map.current.setPaintProperty('countries', 'fill-color', matchExpression)
    }

    if (map.current.isStyleLoaded()) {
      updateData()
    } else {
      map.current.once('load', updateData)
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

  // Reset highlighted row whenever the underlying dataset changes so we
  // never point past the end of the list after a filter change.
  useEffect(() => {
    setActiveIndex(0)
  }, [tableRows.length])

  // Auto-focus the listbox when opened and restore focus to the toggle on
  // close (Escape, Enter/Space pick) so keyboard users don't lose context.
  useEffect(() => {
    if (pickerOpen) {
      pickerListRef.current?.focus()
    }
  }, [pickerOpen])

  const closePicker = () => {
    setPickerOpen(false)
    // requestAnimationFrame lets React's render flush before we move focus,
    // otherwise the toggle button may not be focusable yet on re-render.
    requestAnimationFrame(() => pickerToggleRef.current?.focus())
  }

  const handlePickerKeyDown = (e: React.KeyboardEvent<HTMLUListElement>) => {
    if (tableRows.length === 0) return
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault()
        setActiveIndex(i => (i + 1) % tableRows.length)
        break
      case 'ArrowUp':
        e.preventDefault()
        setActiveIndex(i => (i - 1 + tableRows.length) % tableRows.length)
        break
      case 'Home':
        e.preventDefault()
        setActiveIndex(0)
        break
      case 'End':
        e.preventDefault()
        setActiveIndex(tableRows.length - 1)
        break
      case 'Escape':
        e.preventDefault()
        closePicker()
        break
      case 'Enter':
      case ' ': {
        e.preventDefault()
        const picked = tableRows[activeIndex]
        if (picked) {
          // Same payload shape as the map click handler — alpha-2 code if
          // available, else the display name as a last-resort fallback.
          onCountryClickRef.current?.(picked.country)
          closePicker()
        }
        break
      }
    }
  }

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
    <div
      className={`relative min-h-[300px] w-full h-full rounded-lg overflow-hidden bg-background ${className}`}
      role="img"
      aria-label={ariaLabel}
    >
      {mapError ? (
        <div className="absolute inset-0 flex items-center justify-center px-4 text-center text-xs text-muted-foreground" aria-hidden="true">
          Interactive map unavailable in this browser. See the country list below.
        </div>
      ) : (
        <div ref={mapContainer} className="absolute inset-0 w-full h-full" aria-hidden="true" />
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
              <td>{d.count.toLocaleString()}</td>
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
            <ul
              ref={pickerListRef}
              id="choropleth-country-listbox"
              role="listbox"
              tabIndex={0}
              aria-label="Countries by request count — use arrow keys to navigate, Enter to filter"
              aria-activedescendant={`choropleth-country-option-${activeIndex}`}
              onKeyDown={handlePickerKeyDown}
              className="mt-1 max-h-72 w-64 overflow-y-auto bg-popover/95 backdrop-blur-sm border rounded-md shadow-lg text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              {tableRows.map((d, i) => {
                const isActive = i === activeIndex
                return (
                  <li
                    key={d.country}
                    id={`choropleth-country-option-${i}`}
                    role="option"
                    aria-selected={isActive}
                    onMouseEnter={() => setActiveIndex(i)}
                    onClick={() => {
                      onCountryClickRef.current?.(d.country)
                      closePicker()
                    }}
                    className={`flex items-center justify-between gap-3 px-3 py-1.5 cursor-pointer ${isActive ? 'bg-accent text-accent-foreground' : ''}`}
                  >
                    <span className="truncate">
                      <span className="text-muted-foreground tabular-nums mr-2">{i + 1}.</span>
                      {resolveCountryName(d.country)}
                    </span>
                    <span className="text-muted-foreground tabular-nums">
                      {d.count.toLocaleString()}
                    </span>
                  </li>
                )
              })}
            </ul>
          )}
        </div>
      )}

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
              {formatCompactCount(maxCount)}
            </span>
          </div>
        </div>
      )}
    </div>
  )
})
