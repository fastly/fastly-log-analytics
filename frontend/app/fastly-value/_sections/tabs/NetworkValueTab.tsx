'use client'

import React from 'react'
import { Network, Globe, Lock, Wifi } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import type { components } from '@/types/api.generated'

type NetworkData = components['schemas']['NetworkMetrics']

const PROTOCOL_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

export default function NetworkValueTab({ data }: { data?: NetworkData | null }) {
  const protocolData = React.useMemo(() => {
    if (!data?.protocol_time_series?.length) return []
    const grouped: Record<string, { x: string[]; y: number[] }> = {}
    for (const p of data.protocol_time_series) {
      const cat = p.category ?? 'Other'
      if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
      grouped[cat].x.push(p.time ?? '')
      grouped[cat].y.push(p.value)
    }
    const colors: Record<string, string> = {
      'HTTP/3': '#10b981',
      'HTTP/2': '#6366f1',
      'HTTP/1.1': '#f59e0b',
      'Other': '#6b7280',
    }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat,
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No network protocol data available for this service.</p>
  }

  const fmtPct = (v: number | null | undefined) => v != null ? `${v}%` : '—'

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="HTTP/3"
          icon={Wifi}
          value={fmtPct(data.http3_pct)}
          sub="of requests using QUIC"
        />
        <StatCard
          title="TLS"
          icon={Lock}
          value={fmtPct(data.tls_pct)}
          sub="encrypted connections"
        />
        <StatCard
          title="IPv6"
          icon={Globe}
          value={fmtPct(data.ipv6_pct)}
          sub="of requests over IPv6"
        />
        <StatCard
          title="HTTP/2"
          icon={Network}
          value={fmtPct(data.h2_pct)}
          sub="multiplexed connections"
        />
      </div>

      <div className="grid gap-4 grid-cols-1">
        <AnalyticsCard title="Protocol Adoption Trend" isEmpty={!protocolData.length}>
          <PlotlyChart data={protocolData} layout={PROTOCOL_LAYOUT} a11yTitle="HTTP protocol version distribution over time" />
        </AnalyticsCard>
      </div>
    </div>
  )
}
