'use client'

import React from 'react'
import { HardDrive, Shield, ArrowDownToLine, Layers } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatBytes, formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type CachingData = components['schemas']['CachingMetrics']

const OFFLOAD_LAYOUT = {
  yaxis: { title: 'Offload %', ticksuffix: '%', range: [0, 100] },
  xaxis: { type: 'date' as const },
  margin: { l: 50, r: 20, t: 10, b: 40 },
}

const CACHE_STATE_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

export default function CachingValueTab({ data }: { data?: CachingData | null }) {
  const offloadData = React.useMemo(() => {
    if (!data?.offload_time_series?.length) return []
    return [{
      x: data.offload_time_series.map(p => p.time),
      y: data.offload_time_series.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Offload %',
      line: { color: '#10b981' },
    }]
  }, [data])

  const cacheStateData = React.useMemo(() => {
    if (!data?.cache_state_time_series?.length) return []
    const grouped: Record<string, { x: string[]; y: number[] }> = {}
    for (const p of data.cache_state_time_series) {
      const cat = p.category ?? 'OTHER'
      if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
      grouped[cat].x.push(p.time ?? '')
      grouped[cat].y.push(p.value)
    }
    const colors: Record<string, string> = { HIT: '#10b981', MISS: '#ef4444', PASS: '#f59e0b', OTHER: '#6b7280' }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No caching data available for this service.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Origin Offload"
          icon={HardDrive}
          value={data.origin_offload_pct != null ? `${data.origin_offload_pct}%` : '—'}
          sub="requests served from cache"
        />
        <StatCard
          title="Bandwidth Saved"
          icon={ArrowDownToLine}
          value={data.bandwidth_saved_bytes != null ? formatBytes(data.bandwidth_saved_bytes) : '—'}
          sub="served from edge cache"
        />
        <StatCard
          title="Shield Effectiveness"
          icon={Shield}
          value={data.shield_effectiveness_pct != null ? `${data.shield_effectiveness_pct}%` : '—'}
          sub="shield hit ratio"
        />
        <StatCard
          title="Cache Breakdown"
          icon={Layers}
          value={data.hit_requests != null ? formatCompactCount(data.hit_requests) + ' hits' : '—'}
          sub={[
            data.miss_requests != null ? `${formatCompactCount(data.miss_requests)} miss` : null,
            data.pass_requests != null ? `${formatCompactCount(data.pass_requests)} pass` : null,
          ].filter(Boolean).join(' · ')}
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title="Origin Offload Trend" isEmpty={!offloadData.length}>
          <PlotlyChart data={offloadData} layout={OFFLOAD_LAYOUT} a11yTitle="Origin offload percentage over time" />
        </AnalyticsCard>
        <AnalyticsCard title="Cache State Breakdown" isEmpty={!cacheStateData.length}>
          <PlotlyChart data={cacheStateData} layout={CACHE_STATE_LAYOUT} a11yTitle="Cache state breakdown over time" />
        </AnalyticsCard>
      </div>
    </div>
  )
}
