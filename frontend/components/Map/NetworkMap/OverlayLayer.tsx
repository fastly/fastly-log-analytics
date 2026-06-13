'use client'

import React from 'react'
import { METRIC_OPTIONS } from './controls'

export interface TooltipInfo {
  clientX: number
  clientY: number
  city: string
  country?: string
  cityData: Record<string, any>
}

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
