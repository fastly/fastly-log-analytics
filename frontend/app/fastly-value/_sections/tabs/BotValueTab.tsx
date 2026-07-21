'use client'

import React from 'react'
import { Bot, ShieldCheck, Users } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatCompactCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type BotData = components['schemas']['BotMetrics']

const BOT_DONUT_LAYOUT = {
  margin: { l: 20, r: 20, t: 10, b: 10 },
  showlegend: true,
  legend: { orientation: 'h' as const, y: -0.15 },
}

const TOP_BOTS_LAYOUT = {
  yaxis: { title: 'Requests' },
  xaxis: { type: 'category' as const },
  margin: { l: 60, r: 20, t: 10, b: 100 },
}

export default function BotValueTab({ data }: { data?: BotData | null }) {
  const donutData = React.useMemo(() => {
    if (!data?.total_requests || !data?.bot_requests) return []
    const human = data.total_requests - data.bot_requests
    return [{
      values: [data.bot_requests, human],
      labels: ['Bot Traffic', 'Human Traffic'],
      type: 'pie',
      hole: 0.5,
      marker: { colors: ['#f59e0b', '#10b981'] },
      textinfo: 'label+percent',
    }]
  }, [data])

  const topBotsData = React.useMemo(() => {
    if (!data?.top_bots?.length) return []
    return [{
      x: data.top_bots.map(b => b.name),
      y: data.top_bots.map(b => b.count),
      type: 'bar',
      marker: { color: '#f59e0b' },
    }]
  }, [data])

  if (!data) {
    return <p className="text-muted-foreground py-8 text-center">No bot data available. Bot detection requires User-Agent logging.</p>
  }

  const botPct = data.total_requests && data.bot_requests
    ? ((data.bot_requests / data.total_requests) * 100).toFixed(1)
    : null

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-3">
        <StatCard
          title="Bot Requests"
          icon={Bot}
          value={data.bot_requests != null ? formatCompactCount(data.bot_requests) : '—'}
          sub={botPct ? `${botPct}% of all traffic` : ''}
        />
        <StatCard
          title="Verified Bots"
          icon={ShieldCheck}
          value={data.verified_bots != null ? formatCompactCount(data.verified_bots) : '—'}
          sub="known good bots (Googlebot, etc.)"
        />
        <StatCard
          title="Total Requests"
          icon={Users}
          value={data.total_requests != null ? formatCompactCount(data.total_requests) : '—'}
          sub="all traffic in range"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title="Bot vs Human Traffic" isEmpty={!donutData.length}>
          <PlotlyChart data={donutData} layout={BOT_DONUT_LAYOUT} a11yTitle="Bot versus human traffic breakdown" />
        </AnalyticsCard>
        <AnalyticsCard title="Top Bots" isEmpty={!topBotsData.length}>
          <PlotlyChart data={topBotsData} layout={TOP_BOTS_LAYOUT} a11yTitle="Top bot names by request count" />
        </AnalyticsCard>
      </div>
    </div>
  )
}
