'use client'

import React, { useEffect, useRef, useState, useMemo, MutableRefObject } from 'react'
import { createPortal } from 'react-dom'
import * as maplibregl from 'maplibre-gl'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
import { useTheme } from 'next-themes'
import { Play, Pause } from 'lucide-react'
import { useTimezoneStore, usePopGeoStore } from '@/stores/filterStore'
import { formatPopGeo, type PopGeo } from '@/components/DataTable'
import { formatBucket, formatCompactCount, formatCount, resolveCountryName, CHART_COLORS } from "@/lib/utils";
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import countryMapData from '@/lib/country-codes.json'
import type { components } from '@/types/api.generated'
import {
  rafThrottle, countryFill, observeMapResize, NORMALIZE_COUNTRY, useMapResize,
  addCountryBaseLayer, createBaseMap, removeMapTabStops, updateCountryBaseLayerTheme, type MapFillColor,
} from '@/components/Map/ChoroplethMap'

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
