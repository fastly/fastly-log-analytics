'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import * as maplibregl from 'maplibre-gl'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
import { useTheme } from 'next-themes'
import { Play, Pause } from 'lucide-react'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { formatBucket } from '@/lib/date'
import { formatCount } from '@/lib/format'
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { useMapInit, useMapData, type NetworkMapData } from './MapLayer'

export const METRIC_OPTIONS = [
  { value: 'health_score', label: 'Health Score' },
  { value: 'rtt_med_us', label: 'Median RTT' },
  { value: 'avg_ploss', label: 'Packet Loss' },
  { value: 'error_pct', label: 'Error Rate' },
  { value: 'throughput_bps', label: 'Throughput' },
]

export const SPEED_OPTIONS = [
  { value: 1000, label: '1×' },
  { value: 500,  label: '2×' },
  { value: 200,  label: '5×' },
  { value: 100,  label: '10×' },
]

export const STEP_OPTIONS = [
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

/** Per-city values behind a tooltip: the map-bucket cell fields, or a GeoJSON feature's properties. */
export type CityTooltipData = Record<string, unknown>

export interface TooltipInfo {
  clientX: number
  clientY: number
  city: string
  country?: string
  cityData: CityTooltipData
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
  const metricLabel = METRIC_OPTIONS.find(m => m.value === metric)?.label ?? metric
  const metricVal = numberOrNull(metric === 'health_score' ? info.cityData.health_score : info.cityData[metric])
  const reqs = numberOrNull(info.cityData.reqs) ?? 0

  // Flip to left side when cursor is in the right 30% of the viewport
  const flipLeft = info.clientX > window.innerWidth * 0.7

  return (
    <div
      data-testid="map-tooltip"
      data-metric={metric}
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
  playing: boolean
  setPlaying: (p: boolean) => void
  bucketIdx: number
  setBucketIdx: (i: number) => void
  bucketsLength: number
  firstBucketLabel: string
  currentBucketLabel: string
  lastBucketLabel: string
  metric: string
  onMetricChange: (m: string) => void
  bucketSeconds: number
  onBucketChange: (b: number) => void
  playInterval: number
  setPlayInterval: (n: number) => void
  mapAsn: string
  onAsnChange: (a: string) => void
  asnOptions: Array<{ value: string; label: string }>
}

export function PlaybackControls({
  playing,
  setPlaying,
  bucketIdx,
  setBucketIdx,
  bucketsLength,
  firstBucketLabel,
  currentBucketLabel,
  lastBucketLabel,
  metric,
  onMetricChange,
  bucketSeconds,
  onBucketChange,
  playInterval,
  setPlayInterval,
  mapAsn,
  onAsnChange,
  asnOptions,
}: PlaybackControlsProps) {
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
      {/* Playback row */}
      <div className="flex items-center gap-3">
        <Button
          variant="outline"
          size="icon"
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
            value={[bucketIdx]}
            min={0}
            max={bucketsLength - 1}
            step={1}
            aria-label="Time scrubber"
            aria-valuetext={currentBucketLabel}
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
          <SelectTrigger aria-label="Metric" className="h-7 text-xs w-[150px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {METRIC_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={String(bucketSeconds)} onValueChange={(v) => v && onBucketChange(Number(v))}>
          <SelectTrigger aria-label="Time step" className="h-7 text-xs w-[90px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {STEP_OPTIONS.map(o => (
              <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={String(playInterval)} onValueChange={(v) => v && setPlayInterval(Number(v))}>
          <SelectTrigger aria-label="Playback speed" className="h-7 text-xs w-[68px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SPEED_OPTIONS.map(o => (
              <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={mapAsn} onValueChange={(val) => val && onAsnChange(val)}>
          <SelectTrigger aria-label="ASN filter" className="h-7 text-xs w-[180px]">
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
  )
}

interface NetworkMapProps {
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
  // Refs so stable map event handlers can read current React state without stale closures
  const metricRef = useRef(metric)
  const isDarkRef = useRef(false)
  // DMA city data for the current bucket, keyed by metro_code — used by the dma-fill hover handler
  const dmaDataRef = useRef<Record<number, CityTooltipData>>({})

  const { theme } = useTheme()
  const { timezone } = useTimezoneStore()
  const isDark = theme === 'dark'

  const [bucketIdx, setBucketIdx] = useState(0)
  const [playing, setPlaying] = useState(false)
  const [playInterval, setPlayInterval] = useState(100)
  // Portal-based tooltip — avoids overflow-hidden clipping from the map container
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)
  // WebGL-unavailable fallback (headless / locked-down browser). useMapInit
  // calls onMapError when MapLibre's constructor throws; the sr-only table
  // below still renders the data, so we degrade to a placeholder instead of
  // letting the throw reach the route error boundary.
  const [mapError, setMapError] = useState(false)

  // Keep refs in sync
  useEffect(() => { metricRef.current = metric }, [metric])
  useEffect(() => { isDarkRef.current = isDark }, [isDark])

  // Auto-play animation
  useEffect(() => {
    const bucketCount = data?.buckets?.length ?? 0
    if (!playing || !bucketCount) return
    const id = setInterval(() => {
      setBucketIdx(i => (i + 1) % bucketCount)
    }, playInterval)
    return () => clearInterval(id)
  }, [playing, data?.buckets?.length, playInterval])

  // Reset idx on new data (adjusted during render, not in an effect: the previous buckets array
  // is kept in state and compared by identity). Starting from `undefined` also makes the first
  // render with data land on the latest bucket.
  const buckets = data?.buckets
  const [seenBuckets, setSeenBuckets] = useState<typeof buckets>(undefined)
  if (buckets !== seenBuckets) {
    setSeenBuckets(buckets)
    if (buckets) setBucketIdx(buckets.length > 0 ? buckets.length - 1 : 0)
    setPlaying(false)
  }

  const map = useMapInit({ mapContainer, isDark, isDarkRef, dmaDataRef, setTooltip, onMapError: () => setMapError(true) })
  useMapData({ map, dmaDataRef, data, bucketIdx, metric, isDark, setTooltip })

  const currentBucketLabel = formatBucket(data?.buckets?.[bucketIdx] || '', timezone)
  const firstBucketLabel = formatBucket(data?.buckets?.[0] || '', timezone)
  const lastBucketLabel = formatBucket(data?.buckets?.[(data?.buckets?.length ?? 0) - 1] || '', timezone)

  // a11y mirror of the ChoroplethMap pattern: <canvas> is invisible to AT,
  // so surface a summary + sr-only table of the currently-shown bucket's
  // top metros. The visible Playback control already carries semantic
  // metric / time-range info, so the aria-label scopes to "what's painted".
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
      <div
        className={`relative flex flex-col border rounded-lg overflow-hidden ${className} min-h-[400px]`}
        // role="group" (not "img"): the wrapper holds the interactive MapLibre
        // controls + the sr-only data table below, so it can't be an "img"
        // (axe: nested-interactive — an img must not contain focusable
        // descendants). group is a structural role that legitimately groups
        // interactive content under the aria-label.
        role="group"
        aria-label={a11yLabel}
      >
        {mapError ? (
          <div className="w-full h-[400px] flex items-center justify-center px-4 text-center text-xs text-muted-foreground" aria-hidden="true">
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
            bucketIdx={bucketIdx}
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

      {/* Tooltip rendered as a portal into document.body so overflow-hidden on the map
          container cannot clip it. Position is fixed to viewport coordinates. */}
      {tooltip && typeof document !== 'undefined' && createPortal(
        <MapTooltip info={tooltip} metric={metric} />,
        document.body
      )}
    </>
  )
}
