'use client'

import {
  GlobalHealthHelp,
  AvgRttHelp,
  WorstAsnHelp,
  WorstRegionHelp,
  HeatmapHelp,
  AsnLeaderboardHelp,
  MetroLeaderboardHelp,
  ShieldingHelp,
  NetworkQualityHelp,
  SHIELDING_COLUMNS,
  getShieldingLabels,
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  HealthBadge,
} from "./help-content";
import React, { useState, type Dispatch, type SetStateAction } from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { PopHealthHeatmap } from '@/components/network/PopHealthHeatmap'
import { TransportSection } from './_sections/TransportSection'
import { DataTable, DataTableReadonly, ColumnVisibilityDropdown } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import type { components } from '@/types/api'
import { useServiceQuery } from '@/hooks/useDashboardCards'
import { useAnalyticsReportQuery } from '@/hooks/useDashboardCards'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { FilterValueCell } from '@/components/DataTable'
import { PopLabel } from '@/components/DataTable'
import { downloadAsCsv, cn } from '@/lib/utils'
import { formatCount } from '@/lib/format'
import dynamic from 'next/dynamic'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'

const PlotlyChart = dynamic(
  () => import('@/components/PlotlyChart').then(mod => mod.PlotlyChart),
  {
    ssr: false,
    loading: () => <Skeleton className="w-full h-[300px]" />,
  },
)
const NetworkMap = dynamic(() => import('@/components/Map/NetworkMap').then(mod => mod.NetworkMap), {
  ssr: false,
  loading: () => <Skeleton className="w-full h-full min-h-[400px]" />,
})
const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then(mod => mod.ShieldingMap), {
  ssr: false,
  loading: () => <Skeleton className="w-full h-[420px] rounded-xl" />,
})
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { useReportRangeAnchor } from '@/hooks/useDashboardCards'
import type { RangeWireArgs } from '@/lib/utils'
import { LazyMount } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/stat-card'
import { Button } from '@/components/ui/button'
import { Network as NetworkIcon, AlertCircle, Globe, Zap, Activity, Shield, Download, Info } from 'lucide-react'
import { ReportLayout, type ReportLayoutRenderProps } from '@/components/ReportLayout'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

const HEATMAP_LAYOUT: PlotlyLayout = { xaxis: { tickangle: -45 } }

const QUALITY_SCATTER_LAYOUT: PlotlyLayout = {
  xaxis: { title: { text: 'TCP RTT (ms)' } },
  yaxis: { title: { text: 'TTFB (ms)' } },
}

type QualityBarRow = components['schemas']['NetworkQualityBarRow']
type ColumnVisibilityTuple = ReturnType<typeof useColumnVisibility>
type AsnRow = components['schemas']['NetworkLeaderboardRow']
type MetroRow = components['schemas']['NetworkMetroRow']

const rttMs = (us: number | null | undefined) => (us != null ? `${(us / 1000).toFixed(1)}ms` : '—')
const healthDelta = (a: AsnRow) => (a.health_score_now ?? 0) - (a.health_score_1h_ago ?? 0)

const ASN_COLUMNS: ColumnDef<AsnRow>[] = [
  {
    id: 'asn',
    header: 'ASN',
    accessorFn: (a) => a.label,
    size: 240,
    cell: ({ row }) => (
      <span className="text-xs font-medium">
        <FilterValueCell
          filters={[{ column: 'asn', value: String(row.original.asn) }]}
          display={row.original.label}
        />
      </span>
    ),
  },
  {
    id: 'requests',
    header: 'Requests',
    accessorFn: (a) => a.total_reqs ?? 0,
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>
    ),
  },
  {
    id: 'health',
    header: 'Health Score',
    accessorFn: (a) => a.health_score_now ?? -1,
    size: 120,
    cell: ({ row }) => (
      <span className="block text-right">
        <HealthBadge score={row.original.health_score_now ?? null} />
      </span>
    ),
  },
  {
    id: 'p95',
    header: 'P95 RTT',
    accessorFn: (a) => a.p95_rtt_us ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{rttMs(row.original.p95_rtt_us)}</span>
    ),
  },
  {
    id: 'p99',
    header: 'P99 RTT',
    accessorFn: (a) => a.p99_rtt_us ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{rttMs(row.original.p99_rtt_us)}</span>
    ),
  },
  {
    id: 'delta',
    header: '1h Change',
    accessorFn: healthDelta,
    size: 110,
    cell: ({ row }) => {
      const delta = healthDelta(row.original)
      return (
        <span className="block text-right font-mono text-xs">
          <span className={delta > 0 ? 'text-green-500' : delta < 0 ? 'text-red-500' : 'text-muted-foreground'}>
            {delta > 0 ? '+' : ''}{delta.toFixed(1)}
          </span>
        </span>
      )
    },
  },
  {
    id: 'trend',
    header: 'Trend',
    accessorFn: (a) => a.trend ?? 'stable',
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right">
        <Badge
          variant={row.original.trend === 'degrading' ? 'destructive' : 'outline'}
          className={cn('text-[10px]', row.original.trend === 'improving' ? 'text-green-600 dark:text-green-400 border-green-300 dark:border-green-700' : '')}
        >
          {(row.original.trend ?? 'stable').toUpperCase()}
        </Badge>
      </span>
    ),
  },
]

const METRO_COLUMNS: ColumnDef<MetroRow>[] = [
  {
    id: 'city',
    header: 'Metro / City',
    accessorFn: (m) => m.city,
    size: 260,
    cell: ({ row }) => {
      const m = row.original
      return (
        <span className="text-xs font-medium">
          {m.raw_city ? (
            <FilterValueCell
              filters={[
                { column: 'city', value: m.raw_city },
                ...(m.region ? [{ column: 'region', value: m.region }] : []),
                ...(m.country ? [{ column: 'country', value: m.country }] : []),
              ]}
              display={m.city}
            />
          ) : (
            <span>{m.city}</span>
          )}
        </span>
      )
    },
  },
  {
    id: 'requests',
    header: 'Requests',
    accessorFn: (m) => m.total_reqs ?? 0,
    size: 110,
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>
    ),
  },
  {
    id: 'health',
    header: 'Health Score',
    accessorFn: (m) => m.health_score ?? -1,
    size: 120,
    cell: ({ row }) => (
      <span className="block text-right">
        <HealthBadge score={row.original.health_score ?? null} />
      </span>
    ),
  },
]

export function AsnLeaderboardTable({ rows }: { rows: AsnRow[] }) {
  return (
    <DataTableReadonly
      columns={ASN_COLUMNS}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="ASN performance leaderboard"
    />
  )
}

export function MetroLeaderboardTable({ rows }: { rows: MetroRow[] }) {
  return (
    <DataTableReadonly
      columns={METRO_COLUMNS}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="Metro and region leaderboard"
    />
  )
}

function RttBarList({ rows, filterColumn }: { rows: readonly QualityBarRow[]; filterColumn?: string }) {
  if (!rows?.length) {
    return <div className="p-6 text-center text-sm text-muted-foreground">No data in this range</div>
  }
  const max = Math.max(...rows.map((r) => r.rtt_ms), 1)
  return (
    <div className="space-y-1.5 p-3">
      {rows.map((r) => {
        const filterVal = r.value ?? r.label
        const display = filterColumn === 'pop' ? <PopLabel code={filterVal} /> : r.label
        return (
          <div key={filterVal} className="flex items-center gap-2 text-xs">
            <div className="w-48 truncate shrink-0" title={r.label}>
              {filterColumn ? (
                <FilterValueCell filters={[{ column: filterColumn, value: filterVal }]} display={display} />
              ) : (
                <span>{display}</span>
              )}
            </div>
            <div className="flex-1 h-4 bg-muted/40 rounded-sm overflow-hidden">
              <div className="h-full bg-primary/70" style={{ width: `${(r.rtt_ms / max) * 100}%` }} />
            </div>
            <div className="w-16 text-right font-mono shrink-0">{r.rtt_ms.toFixed(1)}ms</div>
            <div className="w-16 text-right font-mono text-muted-foreground shrink-0">{formatCount(r.reqs ?? 0)}</div>
          </div>
        )
      })}
    </div>
  )
}

function LeaderboardStatusBody<T>({
  isLoading,
  rows,
  isActiveField,
  emptyActiveMsg,
  emptyInactiveMsg,
  children,
}: {
  isLoading: boolean
  rows: readonly T[] | undefined
  isActiveField: boolean
  emptyActiveMsg: string
  emptyInactiveMsg: string
  children: (rows: readonly T[]) => React.ReactNode
}) {
  if (isLoading) {
    return (
      <div className="space-y-2 p-4">
        <SkeletonGrid count={5} height="48px" className="rounded-md" />
      </div>
    )
  }
  if (!rows || rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        <p className="mb-1">No data available</p>
        <p className="text-[10px] opacity-70">{isActiveField ? emptyActiveMsg : emptyInactiveMsg}</p>
      </div>
    )
  }
  return <>{children(rows)}</>
}

type NetworkSections = NonNullable<components['schemas']['NetworkHealthRequest']['sections']>
const NETWORK_CORE_SECTIONS: NetworkSections = [
  'summary',
  'leaderboard',
  'metro_leaderboard',
  'heatmap',
  'buckets',
  'cities',
  'map_buckets',
]
const NETWORK_MAP_SECTIONS: NetworkSections = ['cities', 'map_buckets']
const NETWORK_SHIELDING_SECTIONS: NetworkSections = ['shielding_analysis']

export default function NetworkPage() {
  const [metric, setMetric] = useState('health_score')
  const [mapAsn, setMapAsn] = useState('all')
  const [animBucketSeconds, setAnimBucketSeconds] = useState(3600)
  const [regionCountry, setRegionCountry] = useState('US')
  const [shieldingVisibility, setShieldingVisibility, onShieldingVisChange] = useColumnVisibility()
  const [minRequests, setMinRequests] = useState(SHIELDING_MIN_REQUESTS_DEFAULT)

  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()

  return (
    <ReportLayout
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
      icon={NetworkIcon}
    >
      {(ctx) => (
        <NetworkBody
          {...ctx}
          metric={metric}
          setMetric={setMetric}
          mapAsn={mapAsn}
          setMapAsn={setMapAsn}
          animBucketSeconds={animBucketSeconds}
          setAnimBucketSeconds={setAnimBucketSeconds}
          regionCountry={regionCountry}
          setRegionCountry={setRegionCountry}
          shieldingVisibility={shieldingVisibility}
          setShieldingVisibility={setShieldingVisibility}
          onShieldingVisChange={onShieldingVisChange}
          minRequests={minRequests}
          setMinRequests={setMinRequests}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}

type NetworkBodyProps = ReportLayoutRenderProps &
  Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> & {
    metric: string
    setMetric: Dispatch<SetStateAction<string>>
    mapAsn: string
    setMapAsn: Dispatch<SetStateAction<string>>
    animBucketSeconds: number
    setAnimBucketSeconds: Dispatch<SetStateAction<number>>
    regionCountry: string
    setRegionCountry: Dispatch<SetStateAction<string>>
    shieldingVisibility: ColumnVisibilityTuple[0]
    setShieldingVisibility: ColumnVisibilityTuple[1]
    onShieldingVisChange: ColumnVisibilityTuple[2]
    minRequests: number
    setMinRequests: Dispatch<SetStateAction<number>>
  }

function NetworkBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  metric,
  setMetric,
  mapAsn,
  setMapAsn,
  animBucketSeconds,
  setAnimBucketSeconds,
  regionCountry,
  setRegionCountry,
  shieldingVisibility,
  setShieldingVisibility,
  onShieldingVisChange,
  minRequests,
  setMinRequests,
  relativeRange,
  isAutoRange,
  anchor,
}: NetworkBodyProps) {
  const { isFieldActive } = useActiveLogFields()
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }

  const postNetworkHealth = (sections: NetworkSections, rangeToken: string | null, signal: AbortSignal | undefined, targetAsn = mapAsn) =>
    unwrapClient(client.POST("/api/network-health", {
      signal,
      body: {
        start_time: startTime!,
        end_time: endTime!,
        filters: filterPayload,
        metric,
        bucket_seconds: animBucketSeconds,
        top_n: 30,
        map_asn: targetAsn,
        sections,
        range_token: rangeToken,
        anchor,
      }
    }))

  const coreQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'core'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [animBucketSeconds, metric],
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_CORE_SECTIONS, rangeToken, signal, 'all'),
  })

  const mapQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'map'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [animBucketSeconds, mapAsn, metric],
    queryOptions: {
      enabled: mapAsn !== 'all',
    },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_MAP_SECTIONS, rangeToken, signal, mapAsn),
  })

  const shieldingQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'shielding'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_SHIELDING_SECTIONS, rangeToken, signal, mapAsn),
  })

  const qualityQuery = useServiceQuery(
    ['network', 'quality', activeServiceId, startTime, endTime, filterPayload, regionCountry],
    async ({ signal }) => {
      return unwrapClient(client.POST("/api/network-quality", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          region_country: regionCountry,
        }
      }))
    },
  )

  const data = coreQuery.data
  const mapData = (mapAsn === 'all' ? coreQuery.data : mapQuery.data) ?? coreQuery.data
  const qualityData = qualityQuery.data
  const isCoreLoadingInitial = coreQuery.isLoading || (coreQuery.isFetching && !coreQuery.data)
  const isMapLoadingInitial =
    mapAsn === 'all'
      ? isCoreLoadingInitial
      : mapQuery.isLoading || (mapQuery.isFetching && !mapQuery.data)
  const isShieldingLoadingInitial = shieldingQuery.isLoading || (shieldingQuery.isFetching && !shieldingQuery.data)
  const isQualityLoadingInitial = qualityQuery.isLoading || (qualityQuery.isFetching && !qualityQuery.data)

  const shieldingData = shieldingQuery.data?.shielding_analysis
  const adjustedShieldingRows = React.useMemo(
    () => adjustShieldingRows(shieldingData?.rows ?? [], minRequests),
    [shieldingData?.rows, minRequests],
  )

  const qualityScatterTrace = React.useMemo((): PlotlyTrace[] => {
    const pts = qualityData?.scatter ?? []
    if (!pts.length) return []
    return [{
      type: 'scattergl',
      mode: 'markers',
      x: pts.map((p) => p.rtt_ms),
      y: pts.map((p) => p.ttfb_ms),
      marker: { size: 4, opacity: 0.45 },
      hovertemplate: 'RTT %{x}ms<br>TTFB %{y}ms<extra></extra>',
    }]
  }, [qualityData?.scatter])

  const leaderboard = data?.leaderboard
  const asnOptions = React.useMemo(() => {
    if (!leaderboard) return []
    return leaderboard.map((a) => ({ value: String(a.asn), label: a.label }))
  }, [leaderboard])

  const heatmapRows = data?.heatmap
  const heatmapBuckets = data?.buckets
  const heatmapData = React.useMemo(() => {
    if (!heatmapRows?.length || !heatmapBuckets?.length) return null
    const yLabels = heatmapRows.map((d) => d.label)
    const xBuckets = heatmapBuckets
    const z = heatmapRows.map((row) => {
      const byBucket: Record<string, number | null> = {}
      row.buckets?.forEach((b) => { byBucket[b.bucket] = b.health_score })
      return xBuckets.map((bk) => byBucket[bk] ?? null)
    })
    return { x: xBuckets, y: yLabels, z }
  }, [heatmapRows, heatmapBuckets])

  const heatmapTrace = React.useMemo((): PlotlyTrace[] => (
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

  const summary = data?.summary

  const qualityBreakdowns: Array<{
    title: string
    loadingMessage: string
    rows: readonly QualityBarRow[]
    filterColumn: string
    headerAction?: React.ReactNode
  }> = [
    {
      title: 'Avg RTT by Country',
      loadingMessage: 'Analyzing RTT by country...',
      rows: qualityData?.by_country ?? [],
      filterColumn: 'country',
    },
    {
      title: 'Avg RTT by ASN',
      loadingMessage: 'Analyzing RTT by ASN...',
      rows: qualityData?.by_asn ?? [],
      filterColumn: 'asn',
    },
    {
      title: 'Avg RTT by Region',
      loadingMessage: 'Analyzing RTT by region...',
      rows: qualityData?.by_region ?? [],
      filterColumn: 'region',
      headerAction: (
        <select
          aria-label="Region breakdown country"
          value={regionCountry}
          onChange={(e) => setRegionCountry(e.target.value)}
          className="h-7 rounded-md border bg-background px-2 text-xs"
        >
          {(qualityData?.countries ?? []).length === 0 && (
            <option value={regionCountry}>{regionCountry}</option>
          )}
          {(qualityData?.countries ?? []).map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      ),
    },
    {
      title: 'Avg RTT by POP',
      loadingMessage: 'Analyzing RTT by POP...',
      rows: qualityData?.by_pop ?? [],
      filterColumn: 'pop',
    },
  ]

  return (
    <>
      {coreQuery.error && !coreQuery.data && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 text-destructive p-3 text-sm flex items-start gap-2">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Network health unavailable</div>
            <div className="text-xs opacity-80">
              {coreQuery.error instanceof Error ? coreQuery.error.message : 'The backend returned an error. Leaderboards below may show incorrect "fields not enabled" hints.'}
            </div>
          </div>
        </div>
      )}
      {/* ── Map ── */}
      <NetworkMap
        data={mapData}
        isLoading={isMapLoadingInitial}
        error={mapAsn === 'all' ? coreQuery.error : mapQuery.error}
        metric={metric}
        onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds}
        onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn}
        onAsnChange={setMapAsn}
        asnOptions={asnOptions}
      />

      {/* ── Summary cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 [contain-intrinsic-size:120px] [content-visibility:auto]">
        <StatCard
          title="Global Health"
          icon={Activity}
          loading={isCoreLoadingInitial}
          loadingMessage="Compiling ASN health matrix..."
          value={<>{summary?.global_health_score ?? '--'}/100</>}
          sub="Average across top ASNs"
          helpTitle="Global Health Score"
          helpContent={<GlobalHealthHelp />}
          className="min-h-[110px]"
        />
        <StatCard
          title="Avg RTT"
          icon={Zap}
          loading={isCoreLoadingInitial}
          loadingMessage="Analyzing TCP round-trip times..."
          value={<>{summary?.avg_rtt_ms ?? '--'}ms</>}
          sub="Across all regions"
          helpTitle="Average RTT"
          helpContent={<AvgRttHelp />}
          className="min-h-[110px]"
        />
        <StatCard
          title="Worst ASN"
          icon={AlertCircle}
          iconClassName="text-red-500"
          loading={isCoreLoadingInitial}
          loadingMessage="Scanning ASN degradation..."
          value={<span className="text-sm truncate block">{summary?.worst_asn?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary?.worst_asn?.score ?? '--'}</span>}
          helpTitle="Worst-Performing ASN"
          helpContent={<WorstAsnHelp />}
          className="min-h-[110px]"
        />
        <StatCard
          title="Worst Region"
          icon={Globe}
          loading={isCoreLoadingInitial}
          loadingMessage="Evaluating regional latency..."
          value={<span className="text-sm truncate block">{summary?.worst_country?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary?.worst_country?.score ?? '--'}</span>}
          helpTitle="Worst-Performing Region"
          helpContent={<WorstRegionHelp />}
          className="min-h-[110px]"
        />
      </div>

      {/* ── Edge Transport & TCP Performance ── */}
      <TransportSection
        activeServiceId={activeServiceId}
        startTime={startTime}
        endTime={endTime}
        filterPayload={filterPayload}
      />

      <div className="my-6 [contain-intrinsic-size:280px] [content-visibility:auto]">
        {activeServiceId && (
          <PopHealthHeatmap serviceId={activeServiceId} startTime={startTime} endTime={endTime} />
        )}
      </div>

      {/* ── Heatmap ── */}
      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time"
          helpTitle="ASN Health Score over Time"
          helpContent={<HeatmapHelp />}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          className="[contain-intrinsic-size:380px] [content-visibility:auto]"
        >
          <PlotlyChart data={heatmapTrace} layout={HEATMAP_LAYOUT} height={Math.min(60 + heatmapData.y.length * 28, 600)} />
        </AnalyticsCard>
      )}

      {/* ── Edge → Shield POP Map ── */}
      {(shieldingData || isShieldingLoadingInitial || shieldingQuery.error) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:460px] [content-visibility:auto]">
          <AnalyticsCard
            title="Edge → Shield Transit Map"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching}
            error={shieldingQuery.error as AnalyticsCardError | null}
          >
            <LazyMount minHeight={420}>
              <ShieldingMap
                rows={adjustedShieldingRows}
                isLoading={isShieldingLoadingInitial}
                edgeOnly={Boolean(shieldingData?.edge_only) && !isFieldActive('prid')}
                errored={Boolean(shieldingData?.error)}
                expandable
              />
            </LazyMount>
          </AnalyticsCard>

          <AnalyticsCard
            title="Shielding Analysis"
            contentClassName="p-0"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching}
            error={shieldingQuery.error as AnalyticsCardError | null}
            headerAction={
              <div className="flex items-center gap-2">
                <div className="flex items-center gap-1.5">
                  <span className="hidden sm:inline text-[11px] uppercase font-semibold tracking-wide text-muted-foreground">
                    Min reqs
                  </span>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <span
                            className="relative inline-flex items-center text-muted-foreground hover:text-foreground transition-colors cursor-help"
                          />
                        }
                      >
                        <Info className="w-3.5 h-3.5 opacity-60" />
                        <span className="sr-only">What does the minimum requests floor do?</span>
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-[260px] text-xs leading-relaxed">
                        Routes with fewer than this many edge→shield requests are greyed and never flagged as “suboptimal peering” — their transit median is too small a sample to trust. Raise it on busy services; lower it (or pick “No minimum”) to scrutinize quiet routes.
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                  <Select value={String(minRequests)} onValueChange={(v) => v && setMinRequests(Number(v))}>
                    <SelectTrigger className="h-8 w-[124px] text-xs" aria-label="Minimum requests to flag a shielding route">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => (
                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8"
                  onClick={() => {
                    if (!adjustedShieldingRows.length) return
                    downloadAsCsv(
                      adjustedShieldingRows,
                      ['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'distance_km', 'light_speed_rtt_ms', 'efficiency_ratio', 'anomaly_static', 'low_sample'],
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
            <div className="flex flex-col h-[420px] overflow-hidden">
              {shieldingData?.has_data ? (
                <>
                  <DataTable
                    columns={SHIELDING_COLUMNS}
                    data={adjustedShieldingRows}
                    hideToolbar
                    columnVisibility={shieldingVisibility}
                    onColumnVisibilityChange={setShieldingVisibility}
                    tableContainerClassName="max-h-[295px]"
                  />
                  {shieldingData?.truncated && (
                    <p className="px-4 py-2 text-[11px] text-muted-foreground border-t bg-muted/20">
                      Showing {formatCount(shieldingData?.rows?.length ?? 0)} of {formatCount(Number(shieldingData.total_routes ?? 0))} routes — top by request volume and by transit overhead.
                    </p>
                  )}
                </>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 h-full text-center px-4 gap-1">
                  <Shield className={cn("h-8 w-8 mb-2 opacity-20", shieldingData?.error ? 'text-destructive opacity-30' : 'text-muted-foreground')} />
                  {shieldingData?.error ? (
                    <>
                      <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
                      <p className="text-xs text-muted-foreground max-w-sm">The analysis failed to compute for this window (server-side error, not an absence of data). Try again or narrow the time range.</p>
                    </>
                  ) : (shieldingData?.edge_only && !isFieldActive('prid')) ? (
                    <>
                      <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
                      <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Your service is currently logging edge requests only — enable full (non-edge-only) logging to use this analysis.</p>
                    </>
                  ) : shieldingData?.requires_fields ? (
                    <p className="text-sm text-muted-foreground italic">Required fields missing: {shieldingData.requires_fields.join(', ')}</p>
                  ) : (
                    <p className="text-sm text-muted-foreground italic">No shielding data detected in this time range.</p>
                  )}
                </div>
              )}
            </div>
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:420px] [content-visibility:auto]">
        {/* ── ASN Leaderboard ── */}
        <AnalyticsCard
          title="ASN Performance Leaderboard"
          className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
          contentClassName="p-0 flex-1"
          helpTitle="ASN Performance Leaderboard"
          helpContent={<AsnLeaderboardHelp />}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          loadingMessage="Compiling ASN leaderboard..."
        >
          <LeaderboardStatusBody
            isLoading={isCoreLoadingInitial}
            rows={data?.leaderboard}
            isActiveField={isFieldActive('asn')}
            emptyActiveMsg="No ASN data in this time range."
            emptyInactiveMsg="Requires Network Quality (Group F) fields to be enabled in Fastly logging."
          >
            {(rows) => <AsnLeaderboardTable rows={[...rows]} />}
          </LeaderboardStatusBody>
        </AnalyticsCard>

        {/* ── Metro Leaderboard ── */}
        {data?.has_metro && (
          <AnalyticsCard
            title="Metro / Region Leaderboard"
            className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
            contentClassName="p-0 flex-1"
            helpTitle="Metro / Region Leaderboard"
            helpContent={<MetroLeaderboardHelp />}
            isFetching={coreQuery.isFetching}
            error={coreQuery.error as AnalyticsCardError | null}
            loadingMessage="Compiling Metro leaderboard..."
          >
            <LeaderboardStatusBody
              isLoading={isCoreLoadingInitial}
              rows={data?.metro_leaderboard}
              isActiveField={isFieldActive('metro')}
              emptyActiveMsg="No metro data in this time range."
              emptyInactiveMsg="Requires Geolocation (Group D/E) fields to be enabled in Fastly logging."
            >
              {(rows) => <MetroLeaderboardTable rows={[...rows]} />}
            </LeaderboardStatusBody>
          </AnalyticsCard>
        )}
      </div>

      {/* ── Network Quality (TCP RTT breakdowns + RTT-vs-TTFB scatter) ── */}
      {(isQualityLoadingInitial || qualityData?.available || qualityQuery.error) && (
        <div className="space-y-4 [contain-intrinsic-size:400px] [content-visibility:auto]">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Network Quality
          </h2>
          {isQualityLoadingInitial ? (
            <SkeletonGrid count={4} height="180px" className="rounded-xl" />
          ) : (
            <>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {qualityBreakdowns.map((b) => (
                  <AnalyticsCard
                    key={b.title}
                    title={b.title}
                    contentClassName="p-0"
                    helpTitle="Network Quality"
                    helpContent={<NetworkQualityHelp />}
                    isFetching={qualityQuery.isFetching}
                    error={qualityQuery.error as AnalyticsCardError | null}
                    loadingMessage={b.loadingMessage}
                    className="[contain-intrinsic-size:240px] [content-visibility:auto]"
                    headerAction={b.headerAction}
                  >
                    <RttBarList rows={b.rows} filterColumn={b.filterColumn} />
                  </AnalyticsCard>
                ))}
              </div>

              {qualityScatterTrace.length > 0 && (
                <AnalyticsCard
                  title="RTT vs TTFB"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                  loadingMessage="Plotting RTT vs TTFB correlation..."
                  className="[contain-intrinsic-size:380px] [content-visibility:auto]"
                >
                  <PlotlyChart data={qualityScatterTrace} layout={QUALITY_SCATTER_LAYOUT} height={360} />
                </AnalyticsCard>
              )}
            </>
          )}
        </div>
      )}
    </>
  )
}
