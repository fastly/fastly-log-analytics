'use client'

import React from 'react'
import { AlertCircle, Globe, Zap, Activity } from 'lucide-react'
import type { components, PlotlyLayout } from '@/types/api'
import { StatCard } from '@/components/ui/confirm-dialog'
import { approxMark, type NetworkRollupMeta } from '@/components/network/rollupLabels'
import {
  AvgRttHelp,
  GlobalHealthHelp,
  WorstAsnHelp,
  WorstRegionHelp,
} from './NetworkHelp'

export const HEATMAP_LAYOUT: PlotlyLayout = { xaxis: { tickangle: -45 } }

export function NetworkKpiGrid({
  summary,
  isCoreLoadingInitial,
  rollup,
}: {
  summary: components['schemas']['NetworkHealthResponse']['summary'] | undefined
  isCoreLoadingInitial: boolean
  rollup?: NetworkRollupMeta
}) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      {[
        {
          title: 'Global Health', icon: Activity, loadingMessage: 'Compiling ASN health matrix...',
          value: <>{summary?.global_health_score != null && approxMark(rollup, 'global_health_score')}{summary?.global_health_score ?? '--'}/100</>, sub: 'Average across top ASNs',
          helpTitle: 'Global Health Score', helpContent: <GlobalHealthHelp />,
        },
        {
          title: 'Avg RTT', icon: Zap, loadingMessage: 'Analyzing TCP round-trip times...',
          value: <>{summary?.avg_rtt_ms != null && (summary.avg_rtt_ms > 0 || summary.global_health_score != null) ? `${summary.avg_rtt_ms}ms` : '--'}</>, sub: 'Across all regions',
          helpTitle: 'Average RTT', helpContent: <AvgRttHelp />,
        },
        {
          title: 'Worst ASN', icon: AlertCircle, iconClassName: 'text-red-500', loadingMessage: 'Scanning ASN degradation...',
          value: <span className="text-sm truncate block">{summary?.worst_asn?.label ?? '--'}</span>,
          sub: <span className="text-red-500 font-medium">Score: {summary?.worst_asn?.score != null && approxMark(rollup, 'worst_asn')}{summary?.worst_asn?.score ?? '--'}</span>,
          helpTitle: 'Worst-Performing ASN', helpContent: <WorstAsnHelp />,
        },
        {
          title: 'Worst Region', icon: Globe, loadingMessage: 'Evaluating regional latency...',
          value: <span className="text-sm truncate block">{summary?.worst_country?.label ?? '--'}</span>,
          sub: <span className="text-red-500 font-medium">Score: {summary?.worst_country?.score != null && approxMark(rollup, 'worst_country')}{summary?.worst_country?.score ?? '--'}</span>,
          helpTitle: 'Worst-Performing Region', helpContent: <WorstRegionHelp />,
        },
      ].map((card) => (
        <StatCard key={card.title} {...card} loading={isCoreLoadingInitial} className="min-h-[110px]" />
      ))}
    </div>
  )
}
