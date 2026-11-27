'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
import { approxMark, RawWindowNote, readRawWindow, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { isCloudMode } from '@/lib/cloud-engine/mode'
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
import { AnalyticsCard, useInViewOnce, type AnalyticsCardError } from '@/components/AnalyticsCard'
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
  TransportSection,
  type ShieldingRowInput,
} from './TransportAndShielding'
import {
  HEATMAP_LAYOUT,
  NetworkKpiGrid,
  NetworkLeaderboardsSection,
} from './NetworkLeaderboardsAndQuality'

const PopHealthHeatmap = dynamic(
  () => import('@/components/network/PopHealthHeatmap').then((m) => m.PopHealthHeatmap),
  { ssr: false, loading: () => <Skeleton className="w-full h-[280px] rounded-xl" /> },
)
const ShieldingSection = dynamic(
  () => import('./TransportAndShielding').then((m) => m.ShieldingSection),
  { ssr: false, loading: () => <Skeleton className="w-full h-[460px] rounded-xl" /> },
)
const NetworkQualitySection = dynamic(
  () => import('./NetworkLeaderboardsAndQuality').then((m) => m.NetworkQualitySection),
  { ssr: false, loading: () => <Skeleton className="w-full h-[400px] [contain-intrinsic-size:400px] rounded-xl" /> },
)

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
type NetworkBundleResponse = components['schemas']['NetworkBundleResponse']
type NetworkSections = NonNullable<components['schemas']['NetworkHealthRequest']['sections']>
type NetworkBundleSections = NonNullable<components['schemas']['NetworkBundleRequest']['sections']>
const NETWORK_ROLLUP_SECTIONS: NetworkSections = ['summary', 'leaderboard', 'metro_leaderboard', 'heatmap', 'buckets']
const NETWORK_CORE_SECTIONS: NetworkSections = [...NETWORK_ROLLUP_SECTIONS, 'cities', 'map_buckets']
const NETWORK_MAP_SECTIONS: NetworkSections = ['buckets', 'cities', 'map_buckets']
const NETWORK_SHIELDING_SECTIONS: NetworkSections = ['shielding_analysis']
const NETWORK_CLOUD_FILTERED_BUNDLE_SECTIONS: NetworkBundleSections = [
  ...NETWORK_CORE_SECTIONS, ...NETWORK_SHIELDING_SECTIONS, 'quality', 'transport',
]

function defaultStepSecondsForWindow(startIso?: string | null, endIso?: string | null): number | null {
  if (!startIso || !endIso) return null
  const startMs = Date.parse(startIso)
  const endMs = Date.parse(endIso)
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return null
  const windowSecs = (endMs - startMs) / 1000
  if (windowSecs <= 1800) return 60
  if (windowSecs <= 7200) return 300
  if (windowSecs <= 21600) return 900
  if (windowSecs <= 86400) return 3600
  if (windowSecs <= 345600) return 7200
  return 14400
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
  // Cloud answers these sections from summaries only when unfiltered; the map needs raw rows, so it is
  // read apart rather than holding the summary-served core behind a raw scan.
  const rollupFirst = isCloudMode() && Object.keys(filterPayload ?? {}).length === 0
  // When filtered on Cloud, consolidate health + shielding + quality + transport into one bundle scan so
  // the raw Parquet files are projected and loaded once instead of evicted and re-fetched four times.
  const cloudFiltered = isCloudMode() && !rollupFirst

  const windowKey = `${relativeRange ?? ''}|${startTime ?? ''}|${endTime ?? ''}`
  if (windowKey !== seenWindowKey) {
    setSeenWindowKey(windowKey)
    const recommended = defaultStepSecondsForWindow(startTime, endTime)
    if (recommended != null && startTime && endTime) {
      const windowSecs = Math.max(1, (Date.parse(endTime) - Date.parse(startTime)) / 1000)
      const estBuckets = windowSecs / animBucketSeconds
      if (estBuckets < 4 || estBuckets > 180) {
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

  const filteredBundleQuery = useAnalyticsReportQuery<NetworkBundleResponse>({
    namespace: ['network', 'bundle', 'filtered'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds, regionCountry],
    queryOptions: { enabled: cloudFiltered },
    queryFn: ({ rangeToken, signal }) => unwrapClient(client.POST('/api/network/bundle', {
      signal,
      body: {
        start_time: startTime!, end_time: endTime!, filters: filterPayload ?? {}, metric,
        bucket_seconds: animBucketSeconds, top_n: 30, map_asn: 'all', region_country: regionCountry,
        sections: NETWORK_CLOUD_FILTERED_BUNDLE_SECTIONS, range_token: rangeToken, anchor,
      },
    })),
  })

  const coreQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'core'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds],
    queryOptions: { enabled: !cloudFiltered },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(rollupFirst ? NETWORK_ROLLUP_SECTIONS : NETWORK_CORE_SECTIONS, rangeToken, signal, 'all'),
  })

  const mapQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'map'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds, mapAsn],
    queryOptions: { enabled: rollupFirst || mapAsn !== 'all' },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_MAP_SECTIONS, rangeToken, signal, mapAsn),
  })

  // Shielding always reads raw rows in Cloud, so it waits until its panel nears the viewport.
  const shieldingRef = React.useRef<HTMLDivElement>(null)
  const shieldingNear = useInViewOnce(shieldingRef, { rootMargin: '600px' })
  const shieldingEnabled = !rollupFirst || shieldingNear
  const shieldingQuery = useAnalyticsReportQuery<NetworkHealthResponse>({
    namespace: ['network', 'health', 'shielding'], serviceId: activeServiceId, filterPayload, range,
    queryOptions: { enabled: !cloudFiltered && shieldingEnabled },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_SHIELDING_SECTIONS, rangeToken, signal, mapAsn),
  })

  const qualityQuery = useServiceQuery(
    ['network', 'quality', activeServiceId, startTime, endTime, filterPayload, regionCountry],
    async ({ signal }) => unwrapClient(client.POST('/api/network-quality', {
      signal, body: { start_time: startTime!, end_time: endTime!, filters: filterPayload, region_country: regionCountry },
    })),
    { enabled: !cloudFiltered },
  )

  const data = cloudFiltered ? (filteredBundleQuery.data?.health ?? undefined) : coreQuery.data
  const coreError = cloudFiltered ? filteredBundleQuery.error : coreQuery.error
  const coreIsFetching = cloudFiltered ? filteredBundleQuery.isFetching : coreQuery.isFetching
  const rollup = readRollupMeta(data)
  const mapFromCore = mapAsn === 'all' && !rollupFirst
  const mapData = mapFromCore ? data : (mapQuery.data ?? (rollupFirst ? undefined : data))
  const qualityData = cloudFiltered ? (filteredBundleQuery.data?.quality ?? undefined) : qualityQuery.data
  const qualityError = cloudFiltered ? filteredBundleQuery.error : qualityQuery.error
  const qualityIsFetching = cloudFiltered ? filteredBundleQuery.isFetching : qualityQuery.isFetching
  const isCoreLoadingInitial = cloudFiltered
    ? (filteredBundleQuery.isLoading || (filteredBundleQuery.isFetching && !filteredBundleQuery.data))
    : (coreQuery.isLoading || (coreQuery.isFetching && !coreQuery.data))
  const isMapLoadingInitial = !mapData && (mapFromCore ? isCoreLoadingInitial : mapQuery.isLoading || (mapQuery.isFetching && !mapQuery.data))
  const isShieldingLoadingInitial = cloudFiltered
    ? isCoreLoadingInitial
    : (!shieldingEnabled || shieldingQuery.isLoading || (shieldingQuery.isFetching && !shieldingQuery.data))
  const isQualityLoadingInitial = cloudFiltered
    ? isCoreLoadingInitial
    : (qualityQuery.isLoading || (qualityQuery.isFetching && !qualityQuery.data))

  const shieldingData = cloudFiltered ? filteredBundleQuery.data?.health?.shielding_analysis : shieldingQuery.data?.shielding_analysis
  const shieldingError = cloudFiltered ? filteredBundleQuery.error : shieldingQuery.error
  const shieldingIsFetching = cloudFiltered ? filteredBundleQuery.isFetching : shieldingQuery.isFetching
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

  const heatmapApprox = approxMark(rollup, 'health_score')
  const heatmapTrace = React.useMemo((): PlotlyTrace[] => (
    heatmapData ? [{
      type: 'heatmap', x: heatmapData.x, y: heatmapData.y, z: heatmapData.z,
      colorscale: 'RdYlGn', zmin: 0, zmax: 100, colorbar: { title: 'Score', thickness: 12 },
      hovertemplate: `<b>%{y}</b><br>%{x}<br>Score: ${heatmapApprox}%{z}<extra></extra>`,
    }] : []
  ), [heatmapData, heatmapApprox])

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
      {coreError && !data && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 text-destructive p-3 text-sm flex items-start gap-2">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Network health unavailable</div>
            <div className="text-xs opacity-80">
              {coreError instanceof Error ? coreError.message : 'The backend returned an error. Leaderboards below may show incorrect "fields not enabled" hints.'}
            </div>
          </div>
        </div>
      )}
      <RollupDataThroughSlot reserve={rollupFirst} meta={rollup} />
      <RawWindowNote window={readRawWindow(cloudFiltered ? filteredBundleQuery.data : data)} />
      <NetworkMap
        data={mapData} isLoading={isMapLoadingInitial}
        error={mapFromCore ? coreError : mapQuery.error}
        metric={metric} onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds} onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn} onAsnChange={setMapAsn} asnOptions={asnOptions}
      />

      <NetworkKpiGrid summary={data?.summary} isCoreLoadingInitial={isCoreLoadingInitial} rollup={rollup} />

      <TransportSection
        activeServiceId={activeServiceId} startTime={startTime} endTime={endTime} filterPayload={filterPayload}
        externalData={cloudFiltered ? (filteredBundleQuery.data?.transport ?? null) : undefined}
        externalLoading={cloudFiltered ? isCoreLoadingInitial : undefined}
      />

      <div className="my-6 min-h-[280px] [contain-intrinsic-size:280px]">
        {activeServiceId && <PopHealthHeatmap serviceId={activeServiceId} startTime={startTime} endTime={endTime} filterPayload={filterPayload} />}
      </div>

      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time" helpTitle="ASN Health Score over Time" helpContent={<HeatmapHelp />}
          isFetching={coreIsFetching} error={coreError as AnalyticsCardError | null}
          className="[contain-intrinsic-size:380px]"
        >
          <PlotlyChart
            data={heatmapTrace}
            layout={HEATMAP_LAYOUT}
            height={Math.min(60 + heatmapData.y.length * 28, 600)}
            a11yTitle="ASN Health Score over Time"
          />
        </AnalyticsCard>
      )}

      <div ref={shieldingRef} className={shieldingData || isShieldingLoadingInitial || shieldingError ? 'min-h-[460px]' : undefined}>
      {(shieldingData || isShieldingLoadingInitial || shieldingError) && (
        <ShieldingSection
          shieldingData={shieldingData}
          adjustedShieldingRows={adjustedShieldingRows}
          isShieldingLoadingInitial={isShieldingLoadingInitial}
          isFetching={shieldingIsFetching}
          error={shieldingError}
          minRequests={minRequests}
          setMinRequests={setMinRequests}
          shieldingVisibility={shieldingVisibility}
          setShieldingVisibility={setShieldingVisibility}
          onShieldingVisChange={onShieldingVisChange}
          isPridActive={isFieldActive('prid')}
        />
      )}
      </div>

      <NetworkLeaderboardsSection
        data={data}
        isCoreLoadingInitial={isCoreLoadingInitial}
        isFetching={coreIsFetching}
        error={coreError}
        isFieldActive={isFieldActive}
      />

      {(isQualityLoadingInitial || qualityData?.available || qualityError) && (
        <NetworkQualitySection
          qualityData={qualityData}
          isQualityLoadingInitial={isQualityLoadingInitial}
          isFetching={qualityIsFetching}
          error={qualityError}
          regionCountry={regionCountry}
          setRegionCountry={setRegionCountry}
          qualityScatterTrace={qualityScatterTrace}
        />
      )}
    </>
  )
}
