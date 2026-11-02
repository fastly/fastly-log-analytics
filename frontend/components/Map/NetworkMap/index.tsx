'use client'

import React, { useEffect, useRef, useState, useMemo, useSyncExternalStore } from 'react'
import * as maplibregl from 'maplibre-gl'
import { useTheme } from 'next-themes'
import { createPortal } from 'react-dom'
import { formatCompactCount, resolveCountryName, formatCount, CHART_COLORS, formatBucket } from '@/lib/utils'
import { formatPopGeo, type PopGeo } from '@/components/DataTable'
import { useTimezoneStore, usePopGeoStore } from '@/stores/filterStore'
import {
  NORMALIZE_COUNTRY,
  countryFill,
  useMapResize,
  createMapAvailabilityStore,
  type MapFillColor,
  addCountryBaseLayer,
  updateCountryBaseLayerTheme,
  createBaseMap,
  removeMapTabStops,
  type NetworkMapData,
  type CityTooltipData,
  type TooltipInfo,
} from './types'
import { MapLayer } from './MapLayer'
import { MapTooltip, PlaybackControls } from './MapControls'

export * from './types'
export * from './ChoroplethMap'
export * from './ShieldingMap'
export * from './MapLayer'
export * from './MapControls'

export interface NetworkMapProps {
  data: NetworkMapData | null | undefined
  isLoading?: boolean
  error?: unknown
  className?: string
  metric: string
  onMetricChange: (m: string) => void
  bucketSeconds: number
  onBucketChange: (b: number) => void
  mapAsn: string
  onAsnChange: (a: string) => void
  asnOptions: Array<{ value: string; label: string }>
}

export function NetworkMap({
  data,
  isLoading,
  error,
  className,
  metric,
  onMetricChange,
  bucketSeconds,
  onBucketChange,
  mapAsn,
  onAsnChange,
  asnOptions,
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
  const [playInterval, setPlayInterval] = useState(500)
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)
  const [mapError, setMapError] = useState(false)

  useEffect(() => {
    metricRef.current = metric
  }, [metric])
  useEffect(() => {
    isDarkRef.current = isDark
  }, [isDark])

  useEffect(() => {
    const bucketCount = data?.buckets?.length ?? 0
    if (!playing || !bucketCount) return
    const id = setInterval(() => setBucketIdx((i) => (i + 1) % bucketCount), playInterval)
    return () => clearInterval(id)
  }, [playing, data?.buckets?.length, playInterval])

  const buckets = data?.buckets
  const mapBuckets = data?.map_buckets
  const [seenBuckets, setSeenBuckets] = useState<typeof buckets>(undefined)
  const [seenMapBuckets, setSeenMapBuckets] = useState<typeof mapBuckets>(undefined)
  if (buckets !== seenBuckets || mapBuckets !== seenMapBuckets) {
    setSeenBuckets(buckets)
    setSeenMapBuckets(mapBuckets)
    if (buckets && buckets.length > 0) {
      let initialIdx = buckets.length - 1
      if (mapBuckets && mapBuckets.length > 0) {
        for (let i = Math.min(buckets.length, mapBuckets.length) - 1; i >= 0; i--) {
          if ((mapBuckets[i]?.cities?.length ?? 0) > 0) {
            initialIdx = i
            break
          }
        }
      }
      setBucketIdx(initialIdx)
    } else {
      setBucketIdx(0)
    }
    setPlaying(false)
  }

  const safeBucketIdx = buckets && buckets.length > 0 ? Math.min(bucketIdx, buckets.length - 1) : 0

  const map = MapLayer.useMapInit({
    mapContainer,
    isDark,
    isDarkRef,
    dmaDataRef,
    setTooltip,
    onMapError: () => setMapError(true),
  })
  MapLayer.useMapData({ map, dmaDataRef, data, bucketIdx: safeBucketIdx, metric, isDark, setTooltip })

  const currentBucketLabel = formatBucket(data?.buckets?.[safeBucketIdx] || '', timezone)
  const firstBucketLabel = formatBucket(data?.buckets?.[0] || '', timezone)
  const lastBucketLabel = formatBucket(data?.buckets?.[(data?.buckets?.length ?? 0) - 1] || '', timezone)

  const currentBucketRows = React.useMemo(() => {
    const bucket = data?.map_buckets?.[safeBucketIdx]
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
  }, [data, safeBucketIdx, metric])

  const a11yLabel =
    currentBucketRows.length > 0
      ? `Network map showing ${metric} per metro for bucket ${currentBucketLabel}. ${currentBucketRows.length} metros active.`
      : `Network map. No metros active for the selected bucket / ASN filter.`

  return (
    <>
      <div
        className={`relative flex flex-col border rounded-lg overflow-hidden ${className} min-h-[400px]`}
        role="group"
        aria-label={a11yLabel}
      >
        {mapError ? (
          <div
            className="w-full h-[400px] flex items-center justify-center px-4 text-center text-xs text-muted-foreground"
            aria-hidden="true"
          >
            Interactive map unavailable in this browser. See the metro table below.
          </div>
        ) : (
          <div ref={mapContainer} className="w-full h-[400px]" aria-hidden="true" />
        )}
        <table className="sr-only">
          <caption>
            Network map — top {currentBucketRows.length} metros by {metric} for bucket {currentBucketLabel}.
          </caption>
          <thead>
            <tr>
              <th scope="col">Rank</th>
              <th scope="col">Metro code</th>
              <th scope="col">{metric}</th>
            </tr>
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
            playing={playing}
            setPlaying={setPlaying}
            bucketIdx={safeBucketIdx}
            setBucketIdx={setBucketIdx}
            bucketsLength={data.buckets?.length ?? 0}
            firstBucketLabel={firstBucketLabel}
            currentBucketLabel={currentBucketLabel}
            lastBucketLabel={lastBucketLabel}
            metric={metric}
            onMetricChange={onMetricChange}
            bucketSeconds={bucketSeconds}
            onBucketChange={onBucketChange}
            playInterval={playInterval}
            setPlayInterval={setPlayInterval}
            mapAsn={mapAsn}
            onAsnChange={onAsnChange}
            asnOptions={asnOptions}
          />
        )}
      </div>

      {tooltip &&
        typeof document !== 'undefined' &&
        createPortal(<MapTooltip info={tooltip} metric={metric} />, document.body)}
    </>
  )
}

type CoordTree = number | CoordTree[]
const isPosition = (coords: CoordTree[]): coords is number[] => typeof coords[0] === 'number'

function geoBbox(geometry: { coordinates: CoordTree[] }): [number, number, number, number] {
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity
  const walk = (coords: CoordTree[]) => {
    if (isPosition(coords)) {
      const [x, y] = coords
      if (x < minX) minX = x
      if (y < minY) minY = y
      if (x > maxX) maxX = x
      if (y > maxY) maxY = y
      return
    }
    for (const c of coords) {
      if (Array.isArray(c)) walk(c)
    }
  }
  walk(geometry.coordinates)
  return [minX, minY, maxX, maxY]
}

export interface PopTrafficMapProps {
  allPops: Record<string, { r: number; e: number }>
  className?: string
}

interface PopTooltipState {
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
  const isDarkRef = useRef(theme === 'dark')
  useEffect(() => {
    isDarkRef.current = theme === 'dark'
  }, [theme])
  const geoMap = usePopGeoStore((s) => s.map)
  const locations = usePopGeoStore((s) => s.locations)
  const [tooltip, setTooltip] = useState<PopTooltipState | null>(null)
  const [mapAvailability] = useState(createMapAvailabilityStore)
  const mapError = useSyncExternalStore(
    mapAvailability.subscribe,
    mapAvailability.getSnapshot,
    mapAvailability.getServerSnapshot,
  )
  const [zoomed, setZoomed] = useState(false)
  const geoMapRef = useRef(geoMap)
  useEffect(() => {
    geoMapRef.current = geoMap
  }, [geoMap])

  const popEntries = useMemo(
    () =>
      Object.entries(locations).map(([code, [lat, lon]]) => {
        const stats = allPops[code] ?? allPops[code.toLowerCase()]
        const requests = stats?.r ?? 0
        const errors = stats?.e ?? 0
        const errorRate = requests > 0 ? errors / requests : 0
        const geo = geoMap[code]
        return { code, lat, lon, requests, errors, errorRate, country: geo?.country?.toUpperCase() ?? '' }
      }),
    [allPops, locations, geoMap],
  )

  const countryAgg = useMemo(() => {
    const agg: Record<string, number> = {}
    for (const e of popEntries) {
      if (e.country) agg[e.country] = (agg[e.country] ?? 0) + e.requests
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
          renderWorldCopies: false,
          zoom: 0.5,
          background: isDarkRef.current ? '#1a1b23' : '#f0f4fa',
          lockRotation: true,
          showCompass: false,
          cooperativeGestures: true,
        })
        requestAnimationFrame(() => {
          map.current?.resize()
        })

        map.current.on('load', () => {
          if (!map.current) return
          removeMapTabStops(mapContainer.current)
          addCountryBaseLayer(map.current, { isDark: isDarkRef.current })
          map.current.addSource('pop-markers', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
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
              map.current.fitBounds(geoBbox(feature.geometry), { padding: 40, maxZoom: 6 })
              setZoomed(true)
            }
          })

          map.current.on('mousemove', 'pop-circles', (e: maplibregl.MapLayerMouseEvent) => {
            if (!e.features?.length || !map.current) return
            const props = e.features[0].properties
            setTooltip({
              x: Math.min(e.point.x + 15, (mapContainer.current?.clientWidth ?? 500) - 180),
              y: Math.min(e.point.y + 15, (mapContainer.current?.clientHeight ?? 300) - 100),
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
    const topPart = sortedForA11y
      .slice(0, 5)
      .map((e) => `${e.code} ${formatCompactCount(e.requests)}`)
      .join(', ')
    return `World map of traffic by Fastly PoP, showing top PoPs: ${topPart}.`
  }, [sortedForA11y])

  return (
    <div
      className={`relative min-h-[300px] w-full rounded-lg overflow-hidden bg-background ${className}`}
      role="group"
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
              <td>{formatCount(e.requests)}</td>
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
            left: tooltip.x,
            top: tooltip.y,
          }}
        >
          <div className="font-semibold text-foreground font-mono">{tooltip.code}</div>
          {tooltip.geo && <div className="text-muted-foreground text-xs">{formatPopGeo(tooltip.geo)}</div>}
          <div className="mt-1 text-xs space-y-0.5">
            <div>
              <span className="text-muted-foreground">Requests:</span>{' '}
              <span className="tabular-nums">{formatCount(tooltip.requests)}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Error rate:</span>{' '}
              <span
                className={`tabular-nums ${
                  tooltip.errorRate > 0.05
                    ? 'text-red-600 dark:text-red-400 font-medium'
                    : tooltip.errorRate > 0.01
                      ? 'text-amber-600 dark:text-amber-400'
                      : ''
                }`}
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
