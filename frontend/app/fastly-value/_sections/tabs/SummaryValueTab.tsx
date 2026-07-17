'use client'

import React from 'react'
import {
  Gauge,
  HardDrive,
  Zap,
  Shield,
  ArrowDownToLine,
  Wifi,
  Lock,
  Globe,
} from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatBytes, formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type ValueData = components['schemas']['ValueSummaryResponse']

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

export default function SummaryValueTab({ data, loading }: { data?: ValueData; loading?: boolean }) {
  const overview = data?.overview
  const caching = data?.caching
  const network = data?.network

  const offloadData = React.useMemo(() => {
    if (!caching?.offload_time_series?.length) return []
    return [{
      x: caching.offload_time_series.map(p => p.time),
      y: caching.offload_time_series.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Offload %',
      line: { color: '#10b981' },
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
    const colors: Record<string, string> = { HIT: '#10b981', MISS: '#ef4444', PASS: '#f59e0b', OTHER: '#6b7280' }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [caching])

  const fmtPct = (v: number | null | undefined) => v != null ? `${v}%` : '—'

  return (
    <div className="space-y-6 pt-4">
      {/* Hero KPIs */}
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Total Requests"
          icon={Gauge}
          loading={loading}
          value={overview?.total_requests != null ? formatCompactCount(overview.total_requests) : '—'}
          sub="processed at the edge"
          tooltip="Total requests processed by Fastly in the selected time range"
        />
        <StatCard
          title="Bandwidth Delivered"
          icon={HardDrive}
          loading={loading}
          value={overview?.total_bandwidth_bytes != null ? formatBytes(overview.total_bandwidth_bytes) : '—'}
          sub={overview?.origin_offload_pct != null ? `${overview.origin_offload_pct}% served from cache` : ''}
          tooltip="Total bandwidth delivered from edge and origin combined"
        />
        <StatCard
          title="Cache Acceleration"
          icon={Zap}
          loading={loading}
          value={overview?.cache_acceleration_factor != null ? `${overview.cache_acceleration_factor}x` : '—'}
          sub="faster than origin"
          tooltip="How much faster cached responses are compared to origin fetches"
        />
        <StatCard
          title="Threats Blocked"
          icon={Shield}
          loading={loading}
          value={overview?.threats_blocked != null ? formatCompactCount(overview.threats_blocked) : '—'}
          sub="attacks blocked by WAF"
          tooltip="Total requests blocked by Fastly's WAF and DDoS mitigation"
        />
      </div>

      {/* Expanded KPIs */}
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Origin Offload"
          icon={ArrowDownToLine}
          loading={loading}
          value={fmtPct(caching?.origin_offload_pct)}
          sub="requests served from cache"
        />
        <StatCard
          title="Bandwidth Saved"
          icon={HardDrive}
          loading={loading}
          value={caching?.bandwidth_saved_bytes != null ? formatBytes(caching.bandwidth_saved_bytes) : '—'}
          sub="served from edge cache"
        />
        {network?.http3_pct != null && (
          <StatCard
            title="HTTP/3 Adoption"
            icon={Wifi}
            loading={loading}
            value={fmtPct(network.http3_pct)}
            sub="of requests using QUIC"
          />
        )}
        {network?.tls_pct != null && (
          <StatCard
            title="TLS Encrypted"
            icon={Lock}
            loading={loading}
            value={fmtPct(network.tls_pct)}
            sub={network?.ipv6_pct != null ? `${network.ipv6_pct}% IPv6` : ''}
          />
        )}
      </div>

      {/* Charts */}
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
