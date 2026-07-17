'use client'

import React from 'react'
import { Shield, ShieldAlert, ShieldCheck, AlertTriangle } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityMetrics']

const THREAT_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'date' as const },
  barmode: 'stack' as const,
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const SIGNAL_LAYOUT = {
  yaxis: { title: 'Count' },
  xaxis: { type: 'category' as const },
  margin: { l: 60, r: 20, t: 10, b: 80 },
}

export default function SecurityValueTab({ data }: { data?: SecurityData | null }) {
  const threatData = React.useMemo(() => {
    if (!data?.threat_time_series?.length) return []
    const grouped: Record<string, { x: string[]; y: number[] }> = {}
    for (const p of data.threat_time_series) {
      const cat = p.category ?? 'blocked'
      if (!grouped[cat]) grouped[cat] = { x: [], y: [] }
      grouped[cat].x.push(p.time ?? '')
      grouped[cat].y.push(p.value)
    }
    const colors: Record<string, string> = { blocked: '#ef4444', logged: '#f59e0b' }
    return Object.entries(grouped).map(([cat, d]) => ({
      x: d.x,
      y: d.y,
      type: 'bar',
      name: cat.charAt(0).toUpperCase() + cat.slice(1),
      marker: { color: colors[cat] ?? '#6b7280' },
    }))
  }, [data])

  const signalData = React.useMemo(() => {
    if (!data?.top_waf_signals?.length) return []
    return [{
      x: data.top_waf_signals.map(s => s.signal),
      y: data.top_waf_signals.map(s => s.count),
      type: 'bar',
      marker: { color: '#6366f1' },
    }]
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No WAF data available. Enable WAF logging to see security metrics.</p>
  }

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Threats Blocked"
          icon={ShieldAlert}
          value={data.waf_blocked != null ? formatCompactCount(data.waf_blocked) : '—'}
          sub="requests blocked by WAF"
        />
        <StatCard
          title="Threats Logged"
          icon={Shield}
          value={data.waf_logged != null ? formatCompactCount(data.waf_logged) : '—'}
          sub="flagged but allowed"
        />
        <StatCard
          title="WAF Passed"
          icon={ShieldCheck}
          value={data.waf_passed != null ? formatCompactCount(data.waf_passed) : '—'}
          sub="clean requests"
        />
        <StatCard
          title="WAF Signals"
          icon={AlertTriangle}
          value={data.top_waf_signals?.length ?? 0}
          sub="distinct signal types detected"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title="Threat Timeline" isEmpty={!threatData.length}>
          <PlotlyChart data={threatData} layout={THREAT_LAYOUT} a11yTitle="WAF blocked and logged requests over time" />
        </AnalyticsCard>
        <AnalyticsCard title="Top WAF Signals" isEmpty={!signalData.length}>
          <PlotlyChart data={signalData} layout={SIGNAL_LAYOUT} a11yTitle="Top WAF signal types by count" />
        </AnalyticsCard>
      </div>
    </div>
  )
}
