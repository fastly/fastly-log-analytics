'use client'

import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from 'next-themes'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { PlaybackControls } from './controls'
import { MapTooltip, type TooltipInfo } from './OverlayLayer'
import { formatBucket, useMapInit, useMapData } from './MapLayer'

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

  useMapInit({ mapContainer, map, isDark, isDarkRef, dmaDataRef, setTooltip })
  useMapData({ map, dmaDataRef, data, bucketIdx, metric, isDark, setTooltip })

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
          <PlaybackControls
            playing={playing}
            setPlaying={setPlaying}
            bucketIdx={bucketIdx}
            setBucketIdx={setBucketIdx}
            bucketsLength={data.buckets.length}
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
