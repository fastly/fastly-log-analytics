'use client'

import React from 'react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import type { components } from '@/types/api.generated'
import { CHART_COLORS } from '@/lib/chart-builders'

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

export function OffloadAndCacheCharts({ caching }: { caching?: CachingData | null }) {
  const offloadData = React.useMemo(() => {
    if (!caching?.offload_time_series?.length) return []
    return [{
      x: caching.offload_time_series.map(p => p.time),
      y: caching.offload_time_series.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Offload %',
      line: { color: CHART_COLORS.success },
    }]
  }, [caching])

  const cacheStateData = React.useMemo(() => {
    if (!caching?.cache_state_time_series?.length) return []
    const grouped: Record<string, { x: string[]; y: number[] }> = {}
    for (const p of caching.cache_state_time_series) {
      const cat = p.category ?? 'OTHER'
      if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
      grouped[cat].x.push(p.time ?? '')
      grouped[cat].y.push(p.value)
    }
    const colors: Record<string, string> = {
      HIT: CHART_COLORS.success,
      MISS: CHART_COLORS.danger,
      PASS: CHART_COLORS.warning,
      OTHER: '#6b7280',
    }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [caching])

  return (
    <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
      <AnalyticsCard title="Origin Offload Trend" isEmpty={!offloadData.length}>
        <PlotlyChart data={offloadData} layout={OFFLOAD_LAYOUT} a11yTitle="Origin offload percentage over time" />
      </AnalyticsCard>
      <AnalyticsCard title="Cache State Breakdown" isEmpty={!cacheStateData.length}>
        <PlotlyChart data={cacheStateData} layout={CACHE_STATE_LAYOUT} a11yTitle="Cache state breakdown over time" />
      </AnalyticsCard>
    </div>
  )
}
