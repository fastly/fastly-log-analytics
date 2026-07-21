'use client'

import React from 'react'
import { PlotlyChart } from '@/components/PlotlyChart'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { Activity } from 'lucide-react'
import type { TimelinePoint } from './useStreamAggregates'

const TIMELINE_LAYOUT = {
  yaxis: { title: { text: 'kbps' } },
  yaxis2: {
    title: { text: 'Buffer (ms)' },
    overlaying: 'y',
    side: 'right' as const,
    rangemode: 'tozero',
  },
  legend: { orientation: 'h' as const, y: -0.15 },
}

interface StreamTimelineProps {
  timeline: TimelinePoint[]
  isLoading?: boolean
}

export function StreamTimeline({ timeline, isLoading }: StreamTimelineProps) {
  const { traces, shapes, annotations } = React.useMemo(() => {
    if (!timeline.length) return { traces: [], shapes: [], annotations: [] }

    const ts = timeline.map(p => p.timestamp)

    const traces: Record<string, unknown>[] = [
      {
        x: ts,
        y: timeline.map(p => p.bitrate),
        name: 'Bitrate',
        type: 'scatter' as const,
        line: { color: '#10b981', shape: 'hv' },
      },
      {
        x: ts,
        y: timeline.map(p => p.throughput),
        name: 'Throughput',
        type: 'scatter' as const,
        line: { color: '#f59e0b', dash: 'dot' },
      },
      {
        x: ts,
        y: timeline.map(p => p.buffer),
        name: 'Buffer',
        type: 'scatter' as const,
        yaxis: 'y2',
        fill: 'tozeroy' as const,
        line: { color: '#6366f1' },
        fillcolor: 'rgba(99, 102, 241, 0.1)',
      },
    ]

    const shapes = timeline
      .filter(p => p.isRebuffer)
      .map(p => ({
        type: 'line' as const,
        x0: p.timestamp,
        x1: p.timestamp,
        y0: 0,
        y1: 1,
        yref: 'paper' as const,
        line: { color: '#ef4444', width: 1.5, dash: 'dash' as const },
      }))

    const annotations = timeline
      .filter(p => p.isStartup)
      .map(p => ({
        x: p.timestamp,
        y: 1,
        yref: 'paper' as const,
        text: 'SU',
        showarrow: false,
        font: { size: 9, color: '#6366f1' },
        bgcolor: 'rgba(99, 102, 241, 0.1)',
        borderpad: 2,
      }))

    return { traces, shapes, annotations }
  }, [timeline])

  const layout = React.useMemo(() => ({
    ...TIMELINE_LAYOUT,
    shapes,
    annotations,
  }), [shapes, annotations])

  return (
    <AnalyticsCard
      title="Session Timeline"
      icon={<Activity className="h-4 w-4" />}
      isLoading={isLoading}
      isEmpty={!timeline.length}
      className="h-[400px] mb-6"
      contentClassName="p-2"
      description="Bitrate and throughput (left axis) with buffer depth (right axis). Red dashed lines mark rebuffer events."
    >
      <PlotlyChart data={traces} layout={layout} height="100%" />
    </AnalyticsCard>
  )
}
