'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import * as maplibregl from 'maplibre-gl'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')
import { useTheme } from 'next-themes'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { formatBucket } from '@/lib/date'
import { PlaybackControls } from './controls'
import { MapTooltip, type CityTooltipData, type TooltipInfo } from './OverlayLayer'
import { useMapInit, useMapData, type NetworkMapData } from './MapLayer'

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
