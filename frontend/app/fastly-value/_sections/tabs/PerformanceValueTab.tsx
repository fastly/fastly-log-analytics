'use client'

import React from 'react'
import { Zap, Timer, Gauge, TrendingDown } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import type { components } from '@/types/api.generated'

type PerformanceData = components['schemas']['PerformanceMetrics']

const LATENCY_LAYOUT = {
  yaxis: { title: 'Latency (ms)', ticksuffix: 'ms' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

export default function PerformanceValueTab({ data }: { data?: PerformanceData | null }) {
  const latencyData = React.useMemo(() => {
    if (!data?.latency_time_series?.length) return []
    const grouped: Record<string, { x: string[]; y: number[] }> = {}
    for (const p of data.latency_time_series) {
      const cat = p.category ?? 'hit'
      if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
      grouped[cat].x.push(p.time ?? '')
      grouped[cat].y.push(p.value)
    }
    const colors: Record<string, string> = { hit: '#10b981', miss: '#ef4444' }
    const names: Record<string, string> = { hit: 'Cache HIT', miss: 'Cache MISS' }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'scatter',
      mode: 'lines',
      name: names[cat] ?? cat,
      line: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No performance data available for this service.</p>
  }

  const fmtMs = (v: number | null | undefined) => v != null ? `${v.toFixed(1)} ms` : '—'

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Cache Acceleration"
          icon={Zap}
          value={data.cache_accel_factor != null ? `${data.cache_accel_factor}x` : '—'}
          sub="faster than origin"
          tooltip="Ratio of miss latency to hit latency — how much faster cached responses are"
        />
        <StatCard
          title="Avg Hit Latency"
          icon={Timer}
          value={fmtMs(data.avg_hit_latency_ms)}
          sub="cached response time"
        />
        <StatCard
          title="Avg Miss Latency"
          icon={TrendingDown}
          value={fmtMs(data.avg_miss_latency_ms)}
          sub="origin fetch time"
        />
        <StatCard
          title="P99 Latency"
          icon={Gauge}
          value={fmtMs(data.p99_latency_ms)}
          sub="99th percentile"
        />
      </div>

      <div className="grid gap-4 grid-cols-1">
        <AnalyticsCard title="Hit vs Miss Latency" isEmpty={!latencyData.length}>
          <PlotlyChart data={latencyData} layout={LATENCY_LAYOUT} a11yTitle="Cache hit versus miss latency over time" />
        </AnalyticsCard>
      </div>
    </div>
  )
}
