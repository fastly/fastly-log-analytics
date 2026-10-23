'use client'

import React from 'react'
import { HardDrive, Shield, ArrowDownToLine, Layers } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { formatBytes, formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'
import { OffloadAndCacheCharts } from './OffloadAndCacheCharts'

type CachingData = components['schemas']['CachingMetrics']

export default function CachingValueTab({ data }: { data?: CachingData | null }) {
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

      <OffloadAndCacheCharts caching={data} />
    </div>
  )
}
