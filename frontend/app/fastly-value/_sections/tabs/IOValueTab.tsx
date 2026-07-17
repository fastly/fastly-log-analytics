'use client'

import React from 'react'
import { ImageIcon, Activity, DollarSign, Sparkles, TrendingDown, ArrowRight, Zap } from 'lucide-react'
import { Skeleton } from '@/components/ui/skeleton'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatCompactCount, formatBytes, formatCurrency } from '@/lib/format'
import { FilterValueCell } from '@/components/FilterValueCell'
import type { components } from '@/types/api.generated'

type IOData = components['schemas']['IOMetrics']

interface IOOpportunity {
  url?: string
  request_count?: number
  total_bytes?: number
  avg_kb?: number
}

interface IOFormatPair {
  input_format?: string
  output_format?: string
  count?: number
  avg_ratio?: number | null
}

const VOLUME_LAYOUT = {
  yaxis: { title: 'IO Transforms', ticksuffix: '' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const SAVINGS_LAYOUT = {
  yaxis: { title: 'Bandwidth Saved', ticksuffix: '' },
  xaxis: { type: 'date' as const },
  margin: { l: 60, r: 20, t: 10, b: 40 },
}

const DONUT_LAYOUT = {
  margin: { l: 20, r: 20, t: 10, b: 10 },
  showlegend: true,
  legend: { orientation: 'h' as const, y: -0.15 },
}

const FORMAT_COLORS: Record<string, string> = {
  webp: '#8b5cf6',
  avif: '#06b6d4',
  jpeg: '#f59e0b',
  png: '#10b981',
  gif: '#ef4444',
  jpegxl: '#3b82f6',
  svg: '#6366f1',
  mp4: '#ec4899',
}

function truncateUrl(url: string, max = 60): string {
  if (url.length <= max) return url
  return url.slice(0, max - 1) + '…'
}

function OpportunitiesTable({ opps }: { opps: IOOpportunity[] }) {
  if (!opps.length) return null
  return (
    <AnalyticsCard
      title="Top Image URLs"
      description="Highest-bandwidth image URLs that could benefit from IO optimization"
    >
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-muted-foreground border-b text-left">
              <th className="pb-2 pr-4 font-medium">URL</th>
              <th className="pb-2 pr-4 text-right font-medium">Requests</th>
              <th className="pb-2 pr-4 text-right font-medium">Avg KB</th>
              <th className="pb-2 text-right font-medium">Total Bandwidth</th>
            </tr>
          </thead>
          <tbody>
            {opps.map((opp) => (
              <tr key={opp.url} className="border-b last:border-0">
                <td className="py-2 pr-4 font-mono text-xs">
                  <FilterValueCell
                    filters={[{ column: 'url', value: opp.url ?? '' }]}
                    display={truncateUrl(opp.url ?? '')}
                  />
                </td>
                <td className="py-2 pr-4 text-right">
                  {formatCompactCount(opp.request_count ?? 0)}
                </td>
                <td className="py-2 pr-4 text-right">
                  {opp.avg_kb != null ? opp.avg_kb.toFixed(1) : '—'}
                </td>
                <td className="py-2 text-right">
                  {formatBytes(opp.total_bytes ?? 0)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </AnalyticsCard>
  )
}

function IOUpsellView({ data }: { data: IOData }) {
  const opps = (data.optimization_opportunities ?? []) as IOOpportunity[]

  return (
    <div className="space-y-6 pt-4">
      <div className="rounded-lg border border-purple-500/20 bg-purple-500/5 p-4">
        <p className="text-sm text-muted-foreground">
          <strong className="text-foreground">Image Optimizer is not enabled on this service.</strong>{' '}
          Fastly IO automatically converts images to modern formats (WebP, AVIF) and resizes on the fly,
          reducing bandwidth by up to 40%.
        </p>
      </div>

      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Image Requests"
          icon={ImageIcon}
          value={data.image_request_count != null ? formatCompactCount(data.image_request_count) : '—'}
          sub="detected via URL pattern"
        />
        <StatCard
          title="Image Bandwidth"
          icon={Activity}
          value={data.image_bandwidth_bytes != null ? formatBytes(data.image_bandwidth_bytes) : '—'}
          sub="serving unoptimized images"
        />
        <StatCard
          title="Potential Savings"
          icon={TrendingDown}
          value={data.estimated_savings_bytes != null ? formatBytes(data.estimated_savings_bytes) : '—'}
          sub="estimated at ~40% compression"
        />
        <StatCard
          title="Optimization Candidates"
          icon={Zap}
          value={opps.length > 0 ? String(opps.length) : '—'}
          sub="high-bandwidth image URLs"
        />
      </div>

      <OpportunitiesTable opps={opps} />
    </div>
  )
}

function padSinglePoint(points: { time: string; value: number }[]): { time: string; value: number }[] {
  if (points.length !== 1) return points
  const d = new Date(points[0].time)
  const prev = new Date(d); prev.setDate(prev.getDate() - 1)
  const next = new Date(d); next.setDate(next.getDate() + 1)
  const fmt = (dt: Date) => dt.toISOString().slice(0, 10)
  return [{ time: fmt(prev), value: 0 }, points[0], { time: fmt(next), value: 0 }]
}

function IOEnabledView({ data }: { data: IOData }) {
  const trendData = React.useMemo(() => {
    const hasTransforms = data.io_time_series?.some(p => p.value > 0)
    if (hasTransforms) {
      const pts = padSinglePoint(data.io_time_series!.map(p => ({ time: p.time, value: p.value })))
      return [{
        x: pts.map(p => p.time),
        y: pts.map(p => p.value),
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        name: 'IO Transforms',
        line: { color: '#8b5cf6' },
      }]
    }
    if (data.io_compression_time_series?.length) {
      const pts = padSinglePoint(data.io_compression_time_series.map(p => ({ time: p.time, value: p.value })))
      return [{
        x: pts.map(p => p.time),
        y: pts.map(p => p.value),
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        name: 'Bandwidth Saved (bytes)',
        line: { color: '#10b981' },
      }]
    }
    return []
  }, [data])

  const donutData = React.useMemo(() => {
    if (data.format_distribution?.length) {
      const dist = data.format_distribution
      return [{
        labels: dist.map(d => d.format.toUpperCase()),
        values: dist.map(d => d.count),
        type: 'pie',
        hole: 0.5,
        marker: { colors: dist.map(d => FORMAT_COLORS[d.format] ?? '#94a3b8') },
        textinfo: 'label+percent',
        hovertemplate: '%{label}: %{value:,} transforms (%{percent})<extra></extra>',
      }]
    }
    const pairs = data.io_format_conversion_pairs as IOFormatPair[] | undefined
    if (pairs?.length) {
      const counts: Record<string, number> = {}
      for (const p of pairs) {
        const fmt = (p.output_format ?? 'other').toLowerCase()
        counts[fmt] = (counts[fmt] ?? 0) + (p.count ?? 0)
      }
      const labels = Object.keys(counts)
      return [{
        labels: labels.map(f => f.toUpperCase()),
        values: labels.map(f => counts[f]),
        type: 'pie',
        hole: 0.5,
        marker: { colors: labels.map(f => FORMAT_COLORS[f] ?? '#94a3b8') },
        textinfo: 'label+percent',
        hovertemplate: '%{label}: %{value:,} conversions (%{percent})<extra></extra>',
      }]
    }
    return []
  }, [data])

  const savingsData = React.useMemo(() => {
    if (!data.io_compression_time_series?.length) return []
    const pts = padSinglePoint(data.io_compression_time_series.map(p => ({ time: p.time, value: p.value })))
    return [{
      x: pts.map(p => p.time),
      y: pts.map(p => p.value),
      type: 'scatter',
      mode: 'lines',
      fill: 'tozeroy',
      name: 'Bandwidth Saved',
      line: { color: '#10b981' },
    }]
  }, [data])

  const opps = (data.optimization_opportunities ?? []) as IOOpportunity[]

  return (
    <div className="space-y-6 pt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="IO Transforms"
          icon={ImageIcon}
          value={data.io_transforms != null ? formatCompactCount(data.io_transforms) : '—'}
          sub={data.io_pct_of_traffic != null ? `${data.io_pct_of_traffic}% of traffic` : ''}
        />
        <StatCard
          title="IO Bandwidth"
          icon={Activity}
          value={data.io_bandwidth_bytes != null ? formatBytes(data.io_bandwidth_bytes) : '—'}
          sub="optimized image delivery"
        />
        <StatCard
          title="Estimated IO Cost"
          icon={DollarSign}
          value={data.io_estimated_cost_usd != null ? formatCurrency(data.io_estimated_cost_usd) : '—'}
          sub="at $0.0025/transform"
        />
        <StatCard
          title="Modern Format Adoption"
          icon={Sparkles}
          value={data.modern_format_pct != null ? `${data.modern_format_pct}%` : '—'}
          sub="WebP + AVIF + JPEGXL"
        />
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <AnalyticsCard title={data.io_time_series?.some(p => p.value > 0) ? 'IO Volume Trend' : 'IO Savings Trend'} isEmpty={!trendData.length}>
          <PlotlyChart
            data={trendData}
            layout={data.io_time_series?.some(p => p.value > 0) ? VOLUME_LAYOUT : SAVINGS_LAYOUT}
            a11yTitle="Image Optimizer activity over time"
          />
        </AnalyticsCard>
        {donutData.length ? (
          <AnalyticsCard title="Format Distribution">
            <PlotlyChart data={donutData} layout={DONUT_LAYOUT} a11yTitle="Image format distribution" />
          </AnalyticsCard>
        ) : (
          <AnalyticsCard title="Format Distribution">
            <div className="flex items-center justify-center h-48 text-sm text-muted-foreground">
              Per-format breakdown not available from the Stats API
            </div>
          </AnalyticsCard>
        )}
      </div>

      {data.io_actual_bandwidth_saved_bytes != null && (
        <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
          <StatCard
            title="Actual Bandwidth Saved"
            icon={TrendingDown}
            value={formatBytes(data.io_actual_bandwidth_saved_bytes)}
            sub={data.io_actual_compression_ratio != null ? `${data.io_actual_compression_ratio}x compression ratio` : ''}
          />
        </div>
      )}

      {(data.io_format_conversion_pairs as IOFormatPair[] | undefined)?.length ? (
        <AnalyticsCard
          title="Format Conversions"
          description="Input → output format pairs by volume"
        >
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-muted-foreground border-b text-left">
                  <th className="pb-2 pr-4 font-medium">From</th>
                  <th className="pb-2 pr-4 font-medium">To</th>
                  <th className="pb-2 pr-4 text-right font-medium">Count</th>
                  <th className="pb-2 text-right font-medium">Avg Ratio</th>
                </tr>
              </thead>
              <tbody>
                {(data.io_format_conversion_pairs as IOFormatPair[]).map((pair) => (
                  <tr key={`${pair.input_format}-${pair.output_format}`} className="border-b last:border-0">
                    <td className="py-2 pr-4 font-mono text-xs uppercase">{pair.input_format}</td>
                    <td className="py-2 pr-4 font-mono text-xs uppercase flex items-center gap-1">
                      <ArrowRight className="h-3 w-3 text-muted-foreground" />
                      {pair.output_format}
                    </td>
                    <td className="py-2 pr-4 text-right">{formatCompactCount(pair.count ?? 0)}</td>
                    <td className="py-2 text-right">{pair.avg_ratio != null ? `${pair.avg_ratio}x` : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </AnalyticsCard>
      ) : null}

      {savingsData.length > 0 && (
        <AnalyticsCard title="Compression Savings Trend" isEmpty={false}>
          <PlotlyChart data={savingsData} layout={SAVINGS_LAYOUT} a11yTitle="IO bandwidth savings over time" />
        </AnalyticsCard>
      )}

      <OpportunitiesTable opps={opps} />
    </div>
  )
}

export default function IOValueTab({ data, loading }: { data?: IOData | null; loading?: boolean }) {
  if (loading) {
    return (
      <div className="space-y-6 pt-4">
        <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-24 rounded-xl" />
          ))}
        </div>
        <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
          <Skeleton className="h-64 rounded-xl" />
          <Skeleton className="h-64 rounded-xl" />
        </div>
      </div>
    )
  }

  if (!data) {
    return (
      <p className="text-muted-foreground py-8 text-center">
        No image traffic detected for this service.
      </p>
    )
  }

  if (data.io_transforms != null) {
    return <IOEnabledView data={data} />
  }

  return <IOUpsellView data={data} />
}
