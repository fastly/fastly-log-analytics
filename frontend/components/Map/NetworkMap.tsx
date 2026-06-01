'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { Slider } from '@/components/ui/slider'
import { Play, Pause } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import countryMapData from '@/lib/country-codes.json'
import { useTimezoneStore } from '@/stores/timezoneStore'

const A2_TO_A3: Record<string, string> = countryMapData

interface NetworkMapProps {
  data: any
  isLoading?: boolean
  className?: string
  metric: string
  onMetricChange: (m: string) => void
  bucketSeconds: number
  onBucketChange: (b: number) => void
  mapAsn: string
  onAsnChange: (a: string) => void
  asnOptions: Array<{ value: string; label: string }>
}

const METRIC_OPTIONS = [
  { value: 'health_score', label: 'Health Score' },
  { value: 'rtt_med_us', label: 'Median RTT' },
  { value: 'avg_ploss', label: 'Packet Loss' },
  { value: 'error_pct', label: 'Error Rate' },
  { value: 'throughput_bps', label: 'Throughput' },
]

const SPEED_OPTIONS = [
  { value: 1000, label: '1×' },
  { value: 500,  label: '2×' },
  { value: 200,  label: '5×' },
  { value: 100,  label: '10×' },
]

const STEP_OPTIONS = [
  { value: 1,     label: '1 sec' },
  { value: 5,     label: '5 sec' },
  { value: 10,    label: '10 sec' },
  { value: 30,    label: '30 sec' },
  { value: 60,    label: '1 min' },
  { value: 300,   label: '5 min' },
  { value: 900,   label: '15 min' },
  { value: 1800,  label: '30 min' },
  { value: 3600,  label: '1 hr' },
  { value: 7200,  label: '2 hr' },
  { value: 14400, label: '4 hr' },
]

// ── Tooltip ───────────────────────────────────────────────────────────────────

interface TooltipInfo {
  clientX: number
  clientY: number
  city: string
  country?: string
  cityData: Record<string, any>
}

function formatMetricValue(val: number | null | undefined, metric: string): string {
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

function MapTooltip({ info, metric }: { info: TooltipInfo; metric: string }) {
  const metricLabel = METRIC_OPTIONS.find(m => m.value === metric)?.label ?? metric
  const metricVal = metric === 'health_score' ? info.cityData.health_score : info.cityData[metric]
  const reqs: number = info.cityData.reqs ?? 0

  // Flip to left side when cursor is in the right 30% of the viewport
  const flipLeft = info.clientX > window.innerWidth * 0.7

  return (
    <div
      style={{
        position: 'fixed',
        top: info.clientY - 12,
        left: flipLeft ? info.clientX - 14 : info.clientX + 14,
        transform: flipLeft ? 'translate(-100%, -100%)' : 'translateY(-100%)',
        zIndex: 9999,
        pointerEvents: 'none',
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
        {metric !== 'health_score' && info.cityData.health_score != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Health Score</span>
            <span className="text-[11px] font-semibold tabular-nums">{Number(info.cityData.health_score).toFixed(0)}/100</span>
          </div>
        )}
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{reqs.toLocaleString()}</span>
        </div>
      </div>
    </div>
  )
}

// ── Map color helpers ─────────────────────────────────────────────────────────

function formatBucket(iso: string, tz: string): string {
  if (!iso) return ''
  const utc = /[Z+\-]\d*$/.test(iso) ? iso : iso + 'Z'
  const d = new Date(utc)
  if (isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(d)
}

function getScoreColor(val: number | null, metric: string): string {
  if (val == null) return 'transparent'

  if (metric === 'health_score') {
    if (val >= 90) return '#22c55e'
    if (val >= 70) return '#eab308'
    if (val >= 50) return '#f97316'
    return '#ef4444'
  }

  if (metric === 'throughput_bps') {
    if (val >= 100_000_000) return '#22c55e'
    if (val >= 10_000_000) return '#eab308'
    if (val >= 1_000_000) return '#f97316'
    return '#ef4444'
  }

  if (metric === 'rtt_med_us') {
    if (val <= 50_000) return '#22c55e'
    if (val <= 150_000) return '#eab308'
    if (val <= 300_000) return '#f97316'
    return '#ef4444'
  }
  if (metric === 'avg_ploss') {
    if (val <= 0.01) return '#22c55e'
    if (val <= 0.05) return '#eab308'
    if (val <= 0.10) return '#f97316'
    return '#ef4444'
  }
  if (metric === 'error_pct') {
    if (val <= 1) return '#22c55e'
    if (val <= 5) return '#eab308'
    if (val <= 10) return '#f97316'
    return '#ef4444'
  }

  return '#3b82f6'
}

// ── Component ─────────────────────────────────────────────────────────────────

export function NetworkMap({
  data,
  isLoading,
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
  const map = useRef<maplibregl.Map | null>(null)
  // Refs so stable map event handlers can read current React state without stale closures
  const metricRef = useRef(metric)
  const isDarkRef = useRef(false)
  // DMA city data for the current bucket, keyed by metro_code — used by the dma-fill hover handler
  const dmaDataRef = useRef<Record<number, any>>({})

  const { theme } = useTheme()
  const { timezone } = useTimezoneStore()
  const isDark = theme === 'dark'

  const [bucketIdx, setBucketIdx] = useState(0)
  const [playing, setPlaying] = useState(false)
  const [playInterval, setPlayInterval] = useState(100)
  // Portal-based tooltip — avoids overflow-hidden clipping from the map container
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)

  // Keep refs in sync
  useEffect(() => { metricRef.current = metric }, [metric])
  useEffect(() => { isDarkRef.current = isDark }, [isDark])

  // Auto-play animation
  useEffect(() => {
    if (!playing || !data?.buckets.length) return
    const id = setInterval(() => {
      setBucketIdx(i => (i + 1) % data.buckets.length)
    }, playInterval)
    return () => clearInterval(id)
  }, [playing, data?.buckets.length, playInterval])

  // Reset idx on new data
  useEffect(() => {
    if (data?.buckets) {
      setBucketIdx(data.buckets.length > 0 ? data.buckets.length - 1 : 0)
    }
    setPlaying(false)
  }, [data?.buckets])

  // Initialize Map
  useEffect(() => {
    if (!mapContainer.current) return
    if (!map.current) {
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
        zoom: 1,
        interactive: true
      })
      map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

      map.current.on('load', () => {
        if (!map.current) return

        map.current.addSource('world', { type: 'geojson', data: '/geo/world.geojson' })
        map.current.addSource('dma', { type: 'geojson', data: '/geo/dma.geojson' })
        map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        map.current.addLayer({
          id: 'countries',
          type: 'fill',
          source: 'world',
          paint: {
            'fill-color': isDarkRef.current ? '#27272a' : '#e4e4e7',
            'fill-outline-color': isDarkRef.current ? '#3f3f46' : '#d4d4d8',
            'fill-opacity': 0.8
          }
        })

        map.current.addLayer({
          id: 'dma-fill',
          type: 'fill',
          source: 'dma',
          paint: { 'fill-opacity': 0.7, 'fill-color': 'transparent' }
        })

        // City scatter — only for cities without DMA polygon coverage
        map.current.addLayer({
          id: 'city-scatter',
          type: 'circle',
          source: 'heatmap',
          paint: {
            'circle-radius': ['get', 'radius'],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.8,
            'circle-stroke-width': 1,
            'circle-stroke-color': isDarkRef.current ? '#18181b' : '#ffffff'
          }
        })

        // ── Hover: city scatter dots ─────────────────────────────────────────
        map.current.on('mouseenter', 'city-scatter', (e) => {
          if (!e.features?.length || !map.current) return
          map.current.getCanvas().style.cursor = 'pointer'
          const props = e.features[0].properties as Record<string, any>
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: props.city || '',
            country: props.country || undefined,
            cityData: props,
          })
        })

        map.current.on('mousemove', 'city-scatter', (e) => {
          if (!e.features?.length) return
          const props = e.features[0].properties as Record<string, any>
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: props.city || '',
            country: props.country || undefined,
            cityData: props,
          })
        })

        map.current.on('mouseleave', 'city-scatter', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })

        // ── Hover: DMA filled regions ────────────────────────────────────────
        map.current.on('mouseenter', 'dma-fill', (e) => {
          if (!e.features?.length || !map.current) return
          // dma_code may come through as a string — normalise to number to match dmaDataRef keys
          const code = Number(e.features[0].properties?.dma_code)
          const cityData = dmaDataRef.current[code]
          if (!cityData) return
          map.current.getCanvas().style.cursor = 'pointer'
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: cityData.city || '',
            country: cityData.country || undefined,
            cityData,
          })
        })

        map.current.on('mousemove', 'dma-fill', (e) => {
          if (!e.features?.length) return
          const code = Number(e.features[0].properties?.dma_code)
          const cityData = dmaDataRef.current[code]
          if (!cityData) return
          setTooltip({
            clientX: e.originalEvent.clientX,
            clientY: e.originalEvent.clientY,
            city: cityData.city || '',
            country: cityData.country || undefined,
            cityData,
          })
        })

        map.current.on('mouseleave', 'dma-fill', () => {
          if (map.current) map.current.getCanvas().style.cursor = ''
          setTooltip(null)
        })
      })
    }
    return () => {
      setTooltip(null)
      map.current?.remove()
      map.current = null
    }
  }, [isDark])

  // Update map data when bucketIdx or metric changes
  useEffect(() => {
    if (!map.current || !map.current.isStyleLoaded() || !data?.map_buckets) return

    setTooltip(null)

    const bucketData = data.map_buckets[bucketIdx]
    if (!bucketData || !bucketData.cities) return

    const features: any[] = []
    const dmaColors: Record<number, string> = {}
    const countryScores: Record<string, { sum: number, count: number }> = {}
    const nextDmaData: Record<number, any> = {}

    bucketData.cities.forEach((c: any) => {
      const val = metric === 'health_score' ? c.health_score : c[metric]
      const reqs = c.reqs
      const color = getScoreColor(val, metric)

      if (c.country) {
        const a3 = A2_TO_A3[c.country]
        if (a3 && val != null) {
          if (!countryScores[a3]) countryScores[a3] = { sum: 0, count: 0 }
          countryScores[a3].sum += val * reqs
          countryScores[a3].count += reqs
        }
      }

      if (c.metro_code) {
        // US DMA city — show as filled polygon, not a dot (avoid double-rendering)
        dmaColors[Number(c.metro_code)] = color
        nextDmaData[Number(c.metro_code)] = c
      } else if (c.lat != null && c.lon != null) {
        // Non-DMA city (international or small US town) — show as circle dot
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [c.lon, c.lat] },
          properties: {
            city: c.city || '',
            country: c.country || '',
            color,
            radius: Math.max(3, Math.min(15, Math.log10(reqs + 1) * 2.5)),
            reqs,
            health_score: c.health_score,
            rtt_med_us: c.rtt_med_us,
            avg_ploss: c.avg_ploss,
            error_pct: c.error_pct,
            throughput_bps: c.throughput_bps,
          }
        })
      }
    })

    dmaDataRef.current = nextDmaData

    const matchCountry: any[] = ['match', ['id']]
    Object.entries(countryScores).forEach(([a3, stats]) => {
      matchCountry.push(a3)
      matchCountry.push(getScoreColor(stats.sum / stats.count, metric))
    })
    matchCountry.push(isDark ? '#27272a' : '#e4e4e7')

    map.current.setPaintProperty(
      'countries',
      'fill-color',
      Object.keys(countryScores).length > 0 ? matchCountry : (isDark ? '#27272a' : '#e4e4e7')
    )

    const source = map.current.getSource('heatmap') as maplibregl.GeoJSONSource
    source?.setData({ type: 'FeatureCollection', features })

    if (map.current.getLayer('dma-fill')) {
      const dmaEntries = Object.entries(dmaColors)
      if (dmaEntries.length > 0) {
        const matchDma: any[] = ['match', ['get', 'dma_code']]
        dmaEntries.forEach(([code, color]) => { matchDma.push(Number(code)); matchDma.push(color) })
        matchDma.push('transparent')
        map.current.setPaintProperty('dma-fill', 'fill-color', matchDma)
      } else {
        map.current.setPaintProperty('dma-fill', 'fill-color', 'transparent')
      }
    }

  }, [bucketIdx, data, metric, isDark])

  const currentBucketLabel = formatBucket(data?.buckets?.[bucketIdx] || '', timezone)
  const firstBucketLabel = formatBucket(data?.buckets?.[0] || '', timezone)
  const lastBucketLabel = formatBucket(data?.buckets?.[data?.buckets.length - 1] || '', timezone)

  return (
    <>
      <div className={`relative flex flex-col border rounded-lg overflow-hidden ${className} min-h-[400px]`}>
        <div ref={mapContainer} className="w-full h-[400px]" />

        {!data?.buckets?.length ? (
          <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/50 backdrop-blur-sm">
            {isLoading ? 'Loading map data...' : 'No map data available'}
          </div>
        ) : (
          <div className="absolute bottom-4 left-4 right-4 bg-background/90 backdrop-blur-sm p-3 rounded-lg border shadow-lg z-10 space-y-2">
            {/* Playback row */}
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                size="icon"
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
                  value={[bucketIdx]}
                  min={0}
                  max={data.buckets.length - 1}
                  step={1}
                  onValueChange={(val) => {
                    if (Array.isArray(val) && val.length) setBucketIdx(val[0])
                    setPlaying(false)
                  }}
                />
              </div>
            </div>

            {/* Controls row */}
            <div className="flex items-center gap-2 flex-wrap">
              <Select value={metric} onValueChange={(val) => val && onMetricChange(val)}>
                <SelectTrigger className="h-7 text-xs w-[150px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {METRIC_OPTIONS.map(o => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={String(bucketSeconds)} onValueChange={(v) => v && onBucketChange(Number(v))}>
                <SelectTrigger className="h-7 text-xs w-[90px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STEP_OPTIONS.map(o => (
                    <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={String(playInterval)} onValueChange={(v) => v && setPlayInterval(Number(v))}>
                <SelectTrigger className="h-7 text-xs w-[68px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SPEED_OPTIONS.map(o => (
                    <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={mapAsn} onValueChange={(val) => val && onAsnChange(val)}>
                <SelectTrigger className="h-7 text-xs w-[180px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All ASNs</SelectItem>
                  {asnOptions.map(o => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}
      </div>

      {/* Tooltip rendered as a portal into document.body so overflow-hidden on the map
          container cannot clip it. Position is fixed to viewport coordinates. */}
      {tooltip && typeof document !== 'undefined' && createPortal(
        <MapTooltip info={tooltip} metric={metric} />,
        document.body
      )}
    </>
  )
}
