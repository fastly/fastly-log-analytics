'use client'

import React from 'react'
import { Activity, Cpu, Eye, Monitor, PieChart, ShieldAlert, TrendingUp } from 'lucide-react'
import { AnalyticsCard, TimeSeriesChart } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { CHART_COLORS, formatCount, formatDate } from '@/lib/utils'
import type { components } from '@/types/api'
import type { RumAnalytics } from './RumStatusSection'

export type RumCwvDistribution = components['schemas']['RumCwvDistribution']

export interface CwvColors {
  text: string
  bg: string
  label: string
}

export function getThresholdColors(p75: number | null | undefined, goodMax: number, poorMin: number): CwvColors {
  if (p75 == null) return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' }
  if (p75 <= goodMax) return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' }
  if (p75 > poorMin) return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' }
  return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' }
}

export function buildEnvPieData(envMap: Record<string, number> | undefined) {
  return [{
    labels: Object.keys(envMap || {}), values: Object.values(envMap || {}),
    type: 'pie' as const, hole: 0.4, textinfo: 'label+percent' as const,
  }]
}

export const TREND_CHART_LAYOUT = {
  margin: { t: 30, b: 40, l: 30, r: 10 },
  legend: { orientation: 'h' as const, y: 1.08, x: 0.5, xanchor: 'center' as const, yanchor: 'bottom' as const },
}
export const STACKED_BAR_LAYOUT = { ...TREND_CHART_LAYOUT, barmode: 'stack' as const }
export const DONUT_CHART_LAYOUT = { height: 250, margin: { t: 10, b: 10, l: 10, r: 10 }, showlegend: false }

function renderDistributionBar(distribution: RumCwvDistribution | null) {
  const dist = distribution ?? { good: 0, needs_improvement: 0, poor: 0 }
  return (
    <div className="w-full mt-2">
      <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-secondary">
        <div style={{ width: `${dist.good}%` }} className="bg-emerald-500" title={`Good: ${dist.good}%`} />
        <div style={{ width: `${dist.needs_improvement}%` }} className="bg-amber-400" title={`Needs Improvement: ${dist.needs_improvement}%`} />
        <div style={{ width: `${dist.poor}%` }} className="bg-rose-500" title={`Poor: ${dist.poor}%`} />
      </div>
      <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
        <span>{dist.good}% Good</span>
        <span>{dist.needs_improvement}% Needs Imp.</span>
        <span>{dist.poor}% Poor</span>
      </div>
    </div>
  )
}

export function RumVitalsTopSection({
  analytics,
  startTime,
  endTime,
  timezone,
}: {
  analytics: RumAnalytics
  startTime: string | null
  endTime: string | null
  timezone: string
}) {
  const formattedTimestamps = React.useMemo(
    () => (analytics.trends?.timestamps || []).map((t: string) => formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')),
    [analytics.trends?.timestamps, timezone],
  )

  const webVitalsTrendData = React.useMemo(
    () => [
      { x: formattedTimestamps, y: analytics.trends?.lcp || [], name: 'LCP (s)', type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: CHART_COLORS.success } },
      { x: formattedTimestamps, y: analytics.trends?.cls || [], name: 'CLS', type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: CHART_COLORS.info } },
    ],
    [formattedTimestamps, analytics.trends?.lcp, analytics.trends?.cls],
  )

  const errorRateTrendData = React.useMemo(
    () => [{
      x: formattedTimestamps, y: analytics.trends?.error_rate || [], name: 'Error Rate (%)',
      type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: '#f43f5e' }, fill: 'tozeroy' as const,
    }],
    [formattedTimestamps, analytics.trends?.error_rate],
  )

  return (
    <>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Beacons', value: analytics.beacon_count ?? 0, textClass: 'text-blue-500', bgClass: 'bg-blue-500/10', Icon: Activity },
          { label: 'Pageviews', value: analytics.pageview_count ?? 0, textClass: 'text-emerald-500', bgClass: 'bg-emerald-500/10', Icon: Eye },
          { label: 'Interactions', value: analytics.interaction_count ?? 0, textClass: 'text-amber-500', bgClass: 'bg-amber-500/10', Icon: Cpu },
          { label: 'JavaScript Errors', value: analytics.error_count ?? 0, textClass: 'text-rose-500', bgClass: 'bg-rose-500/10', Icon: ShieldAlert },
        ].map(({ label, value, textClass, bgClass, Icon }) => (
          <div key={label} className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
            <div className="space-y-1">
              <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">{label}</p>
              <p className={`text-2xl font-extrabold ${textClass}`}>{formatCount(value)}</p>
            </div>
            <div className={`p-2.5 ${bgClass} rounded-lg`}><Icon className={`h-5 w-5 ${textClass}`} /></div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          { title: 'Largest Contentful Paint (LCP)', Icon: Activity, vital: analytics.vitals.lcp, good: 2.5, poor: 4.0, unit: 's' },
          { title: 'Cumulative Layout Shift (CLS)', Icon: Cpu, vital: analytics.vitals.cls, good: 0.1, poor: 0.25, unit: '' },
          { title: 'Interaction to Next Paint (INP)', Icon: Monitor, vital: analytics.vitals.inp, good: 200, poor: 500, unit: 'ms' },
        ].map(({ title, Icon, vital, good, poor, unit }) => {
          const colors = getThresholdColors(vital.p75, good, poor)
          const formatted = vital.p75 != null ? `${vital.p75.toFixed(2)}${unit}` : '—'
          return (
            <AnalyticsCard
              key={title} title={<span className={colors.text}>{title}</span>} icon={<Icon className={colors.text} />}
              headerAction={<span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${colors.bg} ${colors.text}`}>{colors.label}</span>}
            >
              <div className="space-y-2">
                <p className={`text-3xl font-extrabold ${colors.text}`}>{formatted}</p>
                <p className={`text-xs ${colors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
                {renderDistributionBar(vital.distribution)}
              </div>
            </AnalyticsCard>
          )
        })}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Web Vitals Trend" icon={<TrendingUp />}>
          <TimeSeriesChart data={webVitalsTrendData} layout={TREND_CHART_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>
        <AnalyticsCard title="JS Error Rate Trend" icon={<TrendingUp className="text-rose-500" />}>
          <TimeSeriesChart data={errorRateTrendData} layout={TREND_CHART_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>
      </div>
    </>
  )
}

export function RumEnvironmentAndBeaconsSection({
  analytics,
  startTime,
  endTime,
  timezone,
}: {
  analytics: RumAnalytics
  startTime: string | null
  endTime: string | null
  timezone: string
}) {
  const formattedTimestamps = React.useMemo(
    () => (analytics?.trends?.timestamps || []).map((t: string) => formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')),
    [analytics?.trends?.timestamps, timezone],
  )

  const browserPieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.browsers), [analytics?.environments?.browsers])
  const osPieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.os), [analytics?.environments?.os])
  const devicePieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.devices), [analytics?.environments?.devices])

  const beaconsOverTimeData = React.useMemo(
    () => [
      { x: formattedTimestamps, y: analytics?.trends?.pageviews || [], name: 'Pageviews', type: 'bar' as const, marker: { color: CHART_COLORS.info } },
      { x: formattedTimestamps, y: analytics?.trends?.interactions || [], name: 'Interactions', type: 'bar' as const, marker: { color: CHART_COLORS.success } },
      { x: formattedTimestamps, y: analytics?.trends?.errors || [], name: 'Errors', type: 'bar' as const, marker: { color: '#f43f5e' } },
    ],
    [formattedTimestamps, analytics?.trends?.pageviews, analytics?.trends?.interactions, analytics?.trends?.errors],
  )

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          { title: 'Browser Distribution', data: browserPieData, a11yTitle: 'Browser Distribution Donut Chart' },
          { title: 'OS Distribution', data: osPieData, a11yTitle: 'OS Distribution Donut Chart' },
          { title: 'Device Distribution', data: devicePieData, a11yTitle: 'Device Distribution Donut Chart' },
        ].map(({ title, data, a11yTitle }) => (
          <AnalyticsCard key={title} title={title} icon={<PieChart />}>
            <PlotlyChart data={data} layout={DONUT_CHART_LAYOUT} a11yTitle={a11yTitle} />
          </AnalyticsCard>
        ))}
      </div>

      <div className="mb-6">
        <AnalyticsCard title="Beacons Over Time" icon={<Activity className="text-primary" />}>
          <TimeSeriesChart data={beaconsOverTimeData} layout={STACKED_BAR_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>
      </div>
    </>
  )
}
