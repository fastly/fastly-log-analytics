'use client'

import React from 'react'
import { Slider } from '@/components/ui/slider'
import { Play, Pause } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

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
    <div className="absolute bottom-4 left-4 right-4 bg-background/90 backdrop-blur-sm p-3 rounded-lg border shadow-lg z-10 space-y-2">
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
  )
}
