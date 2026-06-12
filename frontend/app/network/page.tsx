'use client'

import { GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp, AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp, HealthBadge, SHIELDING_COLUMNS, getShieldingLabels } from "./help-content";
import React, { useState } from 'react'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import { DashboardLinkCell } from '@/components/DashboardLinkCell'
import { downloadAsCsv } from '@/lib/utils'
import { cn } from '@/lib/utils'
import dynamic from 'next/dynamic'
// PlotlyChart renders conditionally on heatmapData (the RTT heatmap card).
// Static-importing it dragged the ~1MB plotly chunk into the critical path
// for every /network cold load even when the heatmap wasn't being rendered.
// Dynamic-import defers the chunk to when the heatmap card actually mounts.
const PlotlyChart = dynamic(
  () => import('@/components/PlotlyChart').then(mod => mod.PlotlyChart),
  {
    ssr: false,
    loading: () => (
      <div className="w-full h-[300px] flex items-center justify-center bg-muted/20 border rounded-lg">
        Loading chart...
      </div>
    ),
  },
)
const NetworkMap = dynamic(() => import('@/components/Map/NetworkMap').then(mod => mod.NetworkMap), {
  ssr: false,
  loading: () => (
    <div className="w-full h-full min-h-[400px] flex items-center justify-center bg-muted/20 border rounded-lg">
      Loading Map...
    </div>
  )
})
const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then(mod => mod.ShieldingMap), {
  ssr: false,
  loading: () => (
    <div className="w-full h-[420px] flex items-center justify-center bg-muted/20 border rounded-xl">
      Loading Map...
    </div>
  )
})
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/stat-card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { SkeletonGrid } from '@/components/ui/skeleton-grid'
import { Network as NetworkIcon, AlertCircle, Globe, Zap, Activity, Shield, ExternalLink, Download } from 'lucide-react'
import Link from 'next/link'
import { ReportLayout } from '@/components/ReportLayout'

// Static — module-level keeps the reference stable across renders without
// adding a hook call (we can't safely add hooks after the render-prop's
// early-return at `data.available === false`).
const HEATMAP_LAYOUT = { xaxis: { tickangle: -45 } }

// ── Help content ──────────────────────────────────────────────────────────────

export default function NetworkPage() {
  const [metric, setMetric] = useState('health_score')
  const [mapAsn, setMapAsn] = useState('all')
  const [animBucketSeconds, setAnimBucketSeconds] = useState(5)
  const [shieldingVisibility, setShieldingVisibility, onShieldingVisChange] = useColumnVisibility()

  return (
    <ReportLayout
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
      icon={NetworkIcon}
    >
      {({
        startTime,
        endTime,
        activeServiceId,
        filterPayload,
        bucketSeconds: chartBucketSeconds,
        intervalButtons,
      }) => {

  const { data, isLoading, isFetching } = useServiceQuery(
    ['network', 'health', activeServiceId, startTime, endTime, filterPayload, animBucketSeconds, mapAsn],
    async ({ signal }) => {
      const { data } = await client.POST("/api/network-health", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          metric,
          bucket_seconds: animBucketSeconds,
          top_n: 30,
          map_asn: mapAsn,
        }
      })
      return data as any
    }
  )

  const isLoadingInitial = isLoading || (isFetching && !data)

  const shieldingData = data?.shielding_analysis as any
  const shieldingLoading = isLoadingInitial

  const asnOptions = React.useMemo(() => {
    if (!data?.leaderboard) return []
    return data.leaderboard.map((a: any) => ({ value: String(a.asn), label: a.label }))
  }, [data?.leaderboard])

  const heatmapData = React.useMemo(() => {
    if (!data?.heatmap?.length || !data.buckets?.length) return null
    const yLabels = data.heatmap.map((d: any) => d.label)
    const xBuckets = data.buckets
    const z = data.heatmap.map((row: any) => {
      const byBucket: Record<string, number | null> = {}
      row.buckets?.forEach((b: any) => { byBucket[b.bucket] = b.health_score })
      return xBuckets.map((bk: string) => byBucket[bk] ?? null)
    })
    return { x: xBuckets, y: yLabels, z }
  }, [data?.heatmap, data?.buckets])

  // Stable ref so PlotlyChart's React.memo treats unrelated parent re-renders
  // as no-ops. Must live ABOVE the `data.available === false` early-return so
  // the hook order is identical on every render.
  const heatmapTrace = React.useMemo(() => (
    heatmapData ? [{
      type: 'heatmap',
      x: heatmapData.x,
      y: heatmapData.y,
      z: heatmapData.z,
      colorscale: 'RdYlGn',
      zmin: 0,
      zmax: 100,
      colorbar: { title: 'Score', thickness: 12 },
      hovertemplate: '<b>%{y}</b><br>%{x}<br>Score: %{z}<extra></extra>',
    }] : []
  ), [heatmapData])

        if (data?.available === false) {
          return (
            <div className="flex flex-col items-center justify-center h-[50vh] text-center max-w-md mx-auto">
              <AlertCircle className="h-10 w-10 text-yellow-500 mb-4" />
              <h2 className="text-xl font-semibold">Network Metrics Unavailable</h2>
              <p className="text-muted-foreground mt-2">{data.reason || 'Log fields required for network analysis are not enabled.'}</p>
            </div>
          )
        }

        const summary = data?.summary || {}

  // heatmapTrace + HEATMAP_LAYOUT defined above the early-return / at
  // module scope — keeps hook order stable and gives PlotlyChart stable
  // identity for its React.memo shallow-compare.

        return (
          <>
      {/* ── Map ── */}
      <NetworkMap
        data={data}
        isLoading={isLoadingInitial}
        metric={metric}
        onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds}
        onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn}
        onAsnChange={setMapAsn}
        asnOptions={asnOptions}
      />

      {/* ── Summary cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Global Health"
          icon={Activity}
          loading={isLoadingInitial}
          value={<>{summary.global_health_score ?? '--'}/100</>}
          sub="Average across top ASNs"
          helpTitle="Global Health Score"
          helpContent={<GlobalHealthHelp />}
        />
        <StatCard
          title="Avg RTT"
          icon={Zap}
          loading={isLoadingInitial}
          value={<>{summary.avg_rtt_ms ?? '--'}ms</>}
          sub="Across all regions"
          helpTitle="Average RTT"
          helpContent={<AvgRttHelp />}
        />
        <StatCard
          title="Worst ASN"
          icon={AlertCircle}
          iconClassName="text-red-500"
          loading={isLoadingInitial}
          value={<span className="text-sm truncate block">{summary.worst_asn?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary.worst_asn?.score ?? '--'}</span>}
          helpTitle="Worst-Performing ASN"
          helpContent={<WorstAsnHelp />}
        />
        <StatCard
          title="Worst Region"
          icon={Globe}
          loading={isLoadingInitial}
          value={<span className="text-sm truncate block">{summary.worst_country?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary.worst_country?.score ?? '--'}</span>}
          helpTitle="Worst-Performing Region"
          helpContent={<WorstRegionHelp />}
        />
      </div>

      {/* ── Heatmap ── */}
      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time"
          helpTitle="ASN Health Score over Time"
          helpContent={<HeatmapHelp />}
        >
          <PlotlyChart data={heatmapTrace as any[]} layout={HEATMAP_LAYOUT} height={Math.min(60 + heatmapData.y.length * 28, 600)} />
        </AnalyticsCard>
      )}

      {/* ── Edge → Shield POP Map ── */}
      {(shieldingData?.has_data || (shieldingData as any)?.edge_only || shieldingLoading) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <AnalyticsCard
            title="Edge → Shield Transit Map"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
          >
            <ShieldingMap
              rows={shieldingData?.rows ?? []}
              isLoading={shieldingLoading}
              edgeOnly={(shieldingData as any)?.edge_only}
            />
          </AnalyticsCard>

          <AnalyticsCard
            title="Shielding Analysis"
            contentClassName="p-0"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
            headerAction={
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8"
                  onClick={() => {
                    if (!shieldingData?.rows?.length) return
                    downloadAsCsv(
                      shieldingData.rows,
                      ['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'light_speed_rtt_ms', 'efficiency_ratio'],
                      'shielding_analysis.csv'
                    )
                  }}
                  disabled={!shieldingData?.has_data}
                >
                  <Download className="w-3.5 h-3.5 mr-2" />
                  Export
                </Button>
                <ColumnVisibilityDropdown
                  columns={getShieldingLabels(['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'light_speed_rtt_ms', 'efficiency_ratio'])}
                  visibility={shieldingVisibility}
                  onChange={onShieldingVisChange}
                />
              </div>
            }
          >
            <div className="flex flex-col h-[420px] overflow-auto">
              {shieldingData?.has_data ? (
                <DataTable
                  columns={SHIELDING_COLUMNS}
                  data={shieldingData?.rows || []}
                  hideToolbar
                  columnVisibility={shieldingVisibility}
                  onColumnVisibilityChange={setShieldingVisibility}
                />
              ) : (
                <div className="flex flex-col items-center justify-center py-12 h-full text-center px-4 gap-1">
                  <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
                  {(shieldingData as any)?.edge_only ? (
                    <>
                      <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
                      <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Your service is currently logging edge requests only — enable full (non-edge-only) logging to use this analysis.</p>
                    </>
                  ) : (shieldingData as any)?.requires_fields ? (
                    <p className="text-sm text-muted-foreground italic">Required fields missing: {(shieldingData as any).requires_fields.join(', ')}</p>
                  ) : (
                    <p className="text-sm text-muted-foreground italic">No shielding data detected in this time range.</p>
                  )}
                </div>
              )}
            </div>
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* ── ASN Leaderboard ── */}
        <AnalyticsCard
          title="ASN Performance Leaderboard"
          className="flex flex-col"
          contentClassName="p-0 flex-1"
          helpTitle="ASN Performance Leaderboard"
          helpContent={<AsnLeaderboardHelp />}
        >
          {isLoadingInitial ? (
            <div className="space-y-2 p-4">
              <SkeletonGrid count={5} height="48px" className="rounded-md" />
            </div>
          ) : (!data?.leaderboard || data.leaderboard.length === 0) ? (
            <div className="p-8 text-center text-sm text-muted-foreground">
              <p className="mb-1">No data available</p>
              <p className="text-[10px] opacity-70">
                Requires Network Quality (Group F) fields to be enabled in Fastly logging.
              </p>
            </div>
          ) : (
            <div className="relative w-full overflow-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/30">
                    <th className="h-10 px-4 text-left font-medium text-muted-foreground">ASN</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Requests</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Health Score</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">P95 RTT</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">P99 RTT</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">1h Change</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Trend</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.leaderboard ?? []).map((asn: any) => {
                    const delta = (asn.health_score_now ?? 0) - (asn.health_score_1h_ago ?? 0)
                    return (
                      <tr key={asn.asn} className="border-b last:border-0 hover:bg-muted/50 transition-colors">
                        <td className="px-4 py-3 font-medium">
                          <div className="flex items-center gap-2 group">
                            <span>{asn.label}</span>
                            <Link
                              href={`/dashboard?filter_asn=${encodeURIComponent(asn.asn)}`}
                              className="opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
                              title="View in Dashboard"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              <ExternalLink className="h-3 w-3 text-muted-foreground hover:text-primary" />
                            </Link>
                          </div>
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-xs">{(asn.total_reqs ?? 0).toLocaleString()}</td>
                        <td className="px-4 py-3 text-right"><HealthBadge score={asn.health_score_now} /></td>
                        <td className="px-4 py-3 text-right font-mono text-xs">
                          {asn.p95_rtt_us != null ? `${(asn.p95_rtt_us / 1000).toFixed(1)}ms` : '—'}
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-xs">
                          {asn.p99_rtt_us != null ? `${(asn.p99_rtt_us / 1000).toFixed(1)}ms` : '—'}
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-xs">
                          <span className={delta > 0 ? 'text-green-500' : delta < 0 ? 'text-red-500' : 'text-muted-foreground'}>
                            {delta > 0 ? '+' : ''}{delta.toFixed(1)}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right">
                          <Badge
                            variant={asn.trend === 'degrading' ? 'destructive' : 'outline'}
                            className={cn("text-[10px]", asn.trend === 'improving' ? 'text-green-600 dark:text-green-400 border-green-300 dark:border-green-700' : '')}
                          >
                            {(asn.trend ?? 'stable').toUpperCase()}
                          </Badge>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </AnalyticsCard>

        {/* ── Metro Leaderboard ── */}
        {data?.has_metro && (
          <AnalyticsCard
            title="Metro / Region Leaderboard"
            className="flex flex-col"
            contentClassName="p-0 flex-1"
            helpTitle="Metro / Region Leaderboard"
            helpContent={<MetroLeaderboardHelp />}
          >
            {isLoadingInitial ? (
              <div className="space-y-2 p-4">
                <SkeletonGrid count={5} height="48px" className="rounded-md" />
              </div>
            ) : (!data?.metro_leaderboard || data.metro_leaderboard.length === 0) ? (
              <div className="p-8 text-center text-sm text-muted-foreground">
                <p className="mb-1">No data available</p>
                <p className="text-[10px] opacity-70">
                  Requires Geolocation (Group D/E) fields to be enabled in Fastly logging.
                </p>
              </div>
            ) : (
              <div className="relative w-full overflow-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/30">
                      <th className="h-10 px-4 text-left font-medium text-muted-foreground">Metro / City</th>
                      <th className="h-10 px-4 text-right font-medium text-muted-foreground">Requests</th>
                      <th className="h-10 px-4 text-right font-medium text-muted-foreground">Health Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data?.metro_leaderboard ?? []).map((m: any, i: number) => (
                      <tr key={i} className="border-b last:border-0 hover:bg-muted/50 transition-colors">
                        <td className="px-4 py-3 font-medium">
                          <div className="flex items-center gap-2 group">
                            <span>{m.city}</span>
                            {m.raw_city && (
                              <Link
                                href={`/dashboard?filter_city=${encodeURIComponent(m.raw_city)}${m.region ? `&filter_region=${encodeURIComponent(m.region)}` : ''}${m.country ? `&filter_country=${encodeURIComponent(m.country)}` : ''}`}
                                className="opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
                                title="View in Dashboard"
                                target="_blank"
                                rel="noopener noreferrer"
                              >
                                <ExternalLink className="h-3 w-3 text-muted-foreground hover:text-primary" />
                              </Link>
                            )}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-xs">{(m.total_reqs ?? 0).toLocaleString()}</td>
                        <td className="px-4 py-3 text-right"><HealthBadge score={m.health_score} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </AnalyticsCard>
        )}
      </div>
      </>
        )
      }}
    </ReportLayout>
  )
}
