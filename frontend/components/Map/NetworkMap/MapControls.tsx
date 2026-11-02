'use client'

import React from 'react'
import { Play, Pause } from 'lucide-react'
import { formatCount } from '@/lib/utils'
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { METRIC_OPTIONS, SPEED_OPTIONS, STEP_OPTIONS, type TooltipInfo } from './types'

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
            <span className="text-[11px] font-semibold tabular-nums">
              {Number(info.cityData.health_score).toFixed(0)}/100
            </span>
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

export interface PlaybackControlsProps {
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
  const selects = [
    { label: 'Metric', width: 'w-[150px]', value: metric, onChange: onMetricChange, options: METRIC_OPTIONS },
    {
      label: 'Time step',
      width: 'w-[90px]',
      value: String(bucketSeconds),
      onChange: (v: string) => onBucketChange(Number(v)),
      options: STEP_OPTIONS.map((o) => ({ value: String(o.value), label: o.label })),
    },
    {
      label: 'Playback speed',
      width: 'w-[68px]',
      value: String(playInterval),
      onChange: (v: string) => setPlayInterval(Number(v)),
      options: SPEED_OPTIONS.map((o) => ({ value: String(o.value), label: o.label })),
    },
    {
      label: 'ASN filter',
      width: 'w-[180px]',
      value: mapAsn,
      onChange: onAsnChange,
      options: [{ value: 'all', label: 'All ASNs' }, ...asnOptions],
    },
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

      <div className="flex items-center gap-2 flex-wrap">
        {selects.map((sel) => (
          <Select key={sel.label} value={sel.value} onValueChange={(v) => v && sel.onChange(v)}>
            <SelectTrigger aria-label={sel.label} className={`h-7 text-xs ${sel.width}`}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {sel.options.map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        ))}
      </div>
    </div>
  )
}
