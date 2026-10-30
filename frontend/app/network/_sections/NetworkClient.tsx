'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
import { PopHealthHeatmap } from '@/components/network/PopHealthHeatmap'
import { client, unwrapClient } from '@/lib/api'
import type { components, PlotlyTrace } from '@/types/api'
import {
  useServiceQuery,
  useAnalyticsReportQuery,
  useColumnVisibility,
  useReportPageState,
} from '@/hooks/useReportPageState'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { Skeleton } from '@/components/ui/skeleton'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import {
  Network as NetworkIcon,
  AlertCircle,
} from 'lucide-react'
import { ReportLayout, type ReportLayoutRenderProps } from '@/components/ReportLayout'
import {
  AsnLeaderboardHelp,
  AvgRttHelp,
  GlobalHealthHelp,
  HealthBadge,
  HeatmapHelp,
  MetroLeaderboardHelp,
  ShieldingHelp,
  WorstAsnHelp,
  WorstRegionHelp,
} from './NetworkHelp'
import {
  adjustShieldingRows,
  getShieldingLabels,
  SHIELDING_COLUMNS,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  ShieldingSection,
  TransportSection,
  type ShieldingRowInput,
} from './TransportAndShielding'
import {
  HEATMAP_LAYOUT,
  NetworkKpiGrid,
  NetworkLeaderboardsSection,
  NetworkQualitySection,
} from './NetworkLeaderboardsAndQuality'

export {
  GlobalHealthHelp,
  AvgRttHelp,
  WorstAsnHelp,
  WorstRegionHelp,
  HeatmapHelp,
  AsnLeaderboardHelp,
  MetroLeaderboardHelp,
  ShieldingHelp,
  HealthBadge,
  SHIELDING_COLUMNS,
  getShieldingLabels,
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  type ShieldingRowInput,
}

const PlotlyChart = dynamic(() => import('@/components/PlotlyChart').then((mod) => mod.PlotlyChart), {
  ssr: false, loading: () => <Skeleton className="w-full h-[300px]" />,
})
const NetworkMap = dynamic(() => import('@/components/Map/NetworkMap').then((mod) => mod.NetworkMap), {
  ssr: false, loading: () => <Skeleton className="w-full h-full min-h-[400px]" />,
})

type NetworkHealthResponse = components['schemas']['NetworkHealthResponse']
type NetworkSections = NonNullable<components['schemas']['NetworkHealthRequest']['sections']>
const NETWORK_CORE_SECTIONS: NetworkSections = ['summary', 'leaderboard', 'metro_leaderboard', 'heatmap', 'buckets', 'cities', 'map_buckets']
const NETWORK_MAP_SECTIONS: NetworkSections = ['buckets', 'cities', 'map_buckets']
const NETWORK_SHIELDING_SECTIONS: NetworkSections = ['shielding_analysis']

function defaultStepSecondsForWindow(startIso?: string | null, endIso?: string | null): number | null {
  if (!startIso || !endIso) return null
  const startMs = Date.parse(startIso)
  const endMs = Date.parse(endIso)
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return null
  const windowSecs = (endMs - startMs) / 1000
  if (windowSecs <= 1800) return 60
  if (windowSecs <= 7200) return 300
  if (windowSecs <= 21600) return 900
  return 3600
}

export function NetworkClient() {
  return (
    <ReportLayout
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
      icon={NetworkIcon}
    >
      {(ctx) => <NetworkBody {...ctx} />}
    </ReportLayout>
  )
}

export default NetworkClient

function NetworkBody({ startTime, endTime, activeServiceId, filterPayload }: ReportLayoutRenderProps) {
  const [metric, setMetric] = useState('health_score')
  const [mapAsn, setMapAsn] = useState('all')
  const [animBucketSeconds, setAnimBucketSeconds] = useState(3600)
  const [seenWindowKey, setSeenWindowKey] = useState<string>('')
  const [regionCountry, setRegionCountry] = useState('US')
  const [shieldingVisibility, setShieldingVisibility, onShieldingVisChange] = useColumnVisibility()
  const [minRequests, setMinRequests] = useState(SHIELDING_MIN_REQUESTS_DEFAULT)
  const { relativeRange, isAutoRange, anchor } = useReportPageState()
  const { isFieldActive } = useActiveLogFields()
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }

  const windowKey = `${relativeRange ?? ''}|${startTime ?? ''}|${endTime ?? ''}`
  if (windowKey !== seenWindowKey) {
    setSeenWindowKey(windowKey)
    const recommended = defaultStepSecondsForWindow(startTime, endTime)
    if (recommended != null && startTime && endTime) {
      const windowSecs = Math.max(1, (Date.parse(endTime) - Date.parse(startTime)) / 1000)
      const estBuckets = windowSecs / animBucketSeconds
      if (estBuckets < 4 || estBuckets > 750) {
        setAnimBucketSeconds(recommended)
      }
    }
  }

  const postNetworkHealth = (sections: NetworkSections, rangeToken: string | null, signal: AbortSignal | undefined, targetAsn = mapAsn) =>
    unwrapClient(client.POST('/api/network-health', {
      signal,
      body: {
        start_time: startTime!, end_time: endTime!, filters: filterPayload, metric,
        bucket_seconds: animBucketSeconds, top_n: 30, map_asn: targetAsn, sections, range_token: rangeToken, anchor,
      },
    }))

  const coreQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'core'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds],
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_CORE_SECTIONS, rangeToken, signal, 'all'),
  })

  const mapQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'map'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds, mapAsn],
    queryOptions: { enabled: mapAsn !== 'all' },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_MAP_SECTIONS, rangeToken, signal, mapAsn),
  })

  const shieldingQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'shielding'], serviceId: activeServiceId, filterPayload, range,
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_SHIELDING_SECTIONS, rangeToken, signal, mapAsn),
  })

  const qualityQuery = useServiceQuery(
    ['network', 'quality', activeServiceId, startTime, endTime, filterPayload, regionCountry],
    async ({ signal }) => unwrapClient(client.POST('/api/network-quality', {
      signal, body: { start_time: startTime!, end_time: endTime!, filters: filterPayload, region_country: regionCountry },
    })),
  )

  const data = coreQuery.data
  const mapData = (mapAsn === 'all' ? coreQuery.data : mapQuery.data) ?? coreQuery.data
  const qualityData = qualityQuery.data
  const isCoreLoadingInitial = coreQuery.isLoading || (coreQuery.isFetching && !coreQuery.data)
  const isMapLoadingInitial = !mapData && (mapAsn === 'all' ? isCoreLoadingInitial : mapQuery.isLoading || (mapQuery.isFetching && !mapQuery.data))
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
      type: 'scattergl', mode: 'markers', x: pts.map((p) => p.rtt_ms), y: pts.map((p) => p.ttfb_ms),
      marker: { size: 4, opacity: 0.45 }, hovertemplate: 'RTT %{x}ms<br>TTFB %{y}ms<extra></extra>',
    }]
  }, [qualityData?.scatter])

  const leaderboard = data?.leaderboard
  const asnOptions = React.useMemo(() => leaderboard ? leaderboard.map((a) => ({ value: String(a.asn), label: a.label })) : [], [leaderboard])

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
      type: 'heatmap', x: heatmapData.x, y: heatmapData.y, z: heatmapData.z,
      colorscale: 'RdYlGn', zmin: 0, zmax: 100, colorbar: { title: 'Score', thickness: 12 },
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
      <NetworkMap
        data={mapData} isLoading={isMapLoadingInitial}
        error={mapAsn === 'all' ? coreQuery.error : mapQuery.error}
        metric={metric} onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds} onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn} onAsnChange={setMapAsn} asnOptions={asnOptions}
      />

      <NetworkKpiGrid summary={data?.summary} isCoreLoadingInitial={isCoreLoadingInitial} />

      <TransportSection activeServiceId={activeServiceId} startTime={startTime} endTime={endTime} filterPayload={filterPayload} />

      <div className="my-6 [contain-intrinsic-size:280px] [content-visibility:auto]">
        {activeServiceId && <PopHealthHeatmap serviceId={activeServiceId} startTime={startTime} endTime={endTime} />}
      </div>

      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time" helpTitle="ASN Health Score over Time" helpContent={<HeatmapHelp />}
          isFetching={coreQuery.isFetching} error={coreQuery.error as AnalyticsCardError | null}
          className="[contain-intrinsic-size:380px] [content-visibility:auto]"
        >
          <PlotlyChart data={heatmapTrace} layout={HEATMAP_LAYOUT} height={Math.min(60 + heatmapData.y.length * 28, 600)} />
        </AnalyticsCard>
      )}

      {(shieldingData || isShieldingLoadingInitial || shieldingQuery.error) && (
        <ShieldingSection
          shieldingData={shieldingData}
          adjustedShieldingRows={adjustedShieldingRows}
          isShieldingLoadingInitial={isShieldingLoadingInitial}
          isFetching={shieldingQuery.isFetching}
          error={shieldingQuery.error}
          minRequests={minRequests}
          setMinRequests={setMinRequests}
          shieldingVisibility={shieldingVisibility}
          setShieldingVisibility={setShieldingVisibility}
          onShieldingVisChange={onShieldingVisChange}
          isPridActive={isFieldActive('prid')}
        />
      )}

      <NetworkLeaderboardsSection
        data={data}
        isCoreLoadingInitial={isCoreLoadingInitial}
        isFetching={coreQuery.isFetching}
        error={coreQuery.error}
        isFieldActive={isFieldActive}
      />

      {(isQualityLoadingInitial || qualityData?.available || qualityQuery.error) && (
        <NetworkQualitySection
          qualityData={qualityData}
          isQualityLoadingInitial={isQualityLoadingInitial}
          isFetching={qualityQuery.isFetching}
          error={qualityQuery.error}
          regionCountry={regionCountry}
          setRegionCountry={setRegionCountry}
          qualityScatterTrace={qualityScatterTrace}
        />
      )}
    </>
  )
}
