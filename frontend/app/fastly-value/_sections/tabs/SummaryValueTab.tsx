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
import { formatBytes, formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'
import { OffloadAndCacheCharts } from './OffloadAndCacheCharts'

type ValueData = components['schemas']['ValueSummaryResponse']

export default function SummaryValueTab({ data, loading }: { data?: ValueData; loading?: boolean }) {
  const overview = data?.overview
  const caching = data?.caching
  const network = data?.network

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
      <OffloadAndCacheCharts caching={caching} />
    </div>
  )
}
