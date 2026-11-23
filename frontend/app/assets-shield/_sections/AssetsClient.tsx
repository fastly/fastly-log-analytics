'use client'

import React from 'react'
import { AlertTriangle, Layers } from 'lucide-react'
import { useInViewOnce } from '@/components/AnalyticsCard'
import { ReportLayout } from '@/components/ReportLayout'
import { RawWindowNote, readRawWindow, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useReportPageState'
import { client, unwrapClient } from '@/lib/api'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import {
  ASSETS_RAW_SECTIONS,
  ASSETS_ROLLUP_SECTIONS,
  assetsRollupCacheFilter,
  type AssetsSection,
} from '@/lib/cloud-engine/rollup-sections'
import { cn, type RangeWire, type RangeWireArgs } from '@/lib/utils'
import type { components, FiltersPayload } from '@/types/api'
import {
  AssetsHotspotsSection,
  LargeUncompressedTable,
  LoadingBlock,
  LowTtlTable,
  UrlCell,
  type AssetsAggregates,
  type AssetsTableProps,
  type LargeUncompressedRow,
  type LowTtlRow,
} from './AssetsTables'
import {
  AssetsEfficiencySection,
  AssetsHelpModal,
  AssetsOverviewGrid,
  buildAssetFilterQueryString,
  CACHE_STATUS_COLORS,
  COMPRESSION_COLORS,
  MissingAssetFieldsBanner,
  type AssetHelpContent,
  type AssetTypeBreakdownRow,
  type CachePerformanceRow,
  type CompressionPerformanceRow,
} from './AssetsSummaryAndHelp'

export {
  LargeUncompressedTable,
  LowTtlTable,
  UrlCell,
  LoadingBlock,
  AssetsHotspotsSection,
  MissingAssetFieldsBanner,
  AssetsOverviewGrid,
  AssetsEfficiencySection,
  AssetsHelpModal,
  buildAssetFilterQueryString,
  CACHE_STATUS_COLORS,
  COMPRESSION_COLORS,
  type AssetsTableProps,
  type AssetsAggregates,
  type LargeUncompressedRow,
  type LowTtlRow,
  type AssetHelpContent,
  type AssetTypeBreakdownRow,
  type CachePerformanceRow,
  type CompressionPerformanceRow,
}

/** Optional log fields the asset panels read; the service's log field settings decide which are on. */
const ASSET_LOG_FIELDS = ['ttl', 'resp_header_content_encoding'] as const

export interface AssetsReportContentProps extends Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: FiltersPayload
}

export function AssetsReportContent({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: AssetsReportContentProps) {
  // Cloud answers the summary cards and breakdowns from summaries when unfiltered or filtered on cache
  // status alone; read them apart so they paint without waiting on the URL-level raw scan.
  const rollupFirst = isCloudMode() && assetsRollupCacheFilter(filterPayload) !== false
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }
  const postAssets = (sections: readonly AssetsSection[] | undefined, rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) => {
    // `sections` is a Cloud-only request field.
    const body: components['schemas']['AssetsRequest'] & { sections?: readonly AssetsSection[] } = {
      filters: filterPayload,
      ...sections ? { sections } : {},
      ...rangeBody,
    }
    return unwrapClient(client.POST('/api/assets/aggregates', { signal, body }))
  }

  // The URL tables always read raw rows, so in Cloud they wait until their panel nears the viewport.
  const hotspotsRef = React.useRef<HTMLDivElement>(null)
  const hotspotsNear = useInViewOnce(hotspotsRef, { rootMargin: '600px' })
  const bundle = useAnalyticsReportQuery<AssetsAggregates | undefined>({
    namespace: ['assets', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: rollupFirst ? [ASSETS_RAW_SECTIONS] : [],
    queryOptions: { enabled: !rollupFirst || hotspotsNear },
    queryFn: ({ rangeBody, signal }) => postAssets(rollupFirst ? ASSETS_RAW_SECTIONS : undefined, rangeBody, signal),
  })

  const rollupQuery = useAnalyticsReportQuery<AssetsAggregates | undefined>({
    namespace: ['assets', 'aggregates', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    queryOptions: { enabled: rollupFirst },
    queryFn: ({ rangeBody, signal }) => postAssets(ASSETS_ROLLUP_SECTIONS, rangeBody, signal),
  })

  const summaryQuery = rollupFirst ? rollupQuery : bundle
  const isLoading = summaryQuery.isLoading
  // The URL tables show their own loading block, so only a refetch over shown rows dims the page.
  const isFetching = summaryQuery.isFetching || (bundle.isFetching && bundle.data !== undefined)
  const error = summaryQuery.error ?? bundle.error
  const data = rollupFirst ? { ...bundle.data, ...rollupQuery.data } as AssetsAggregates : bundle.data
  const rollup = rollupFirst ? readRollupMeta(rollupQuery.data) : undefined

  const getTimeQueryString = React.useCallback(() => {
    if (relativeRange) {
      return `&range=${encodeURIComponent(relativeRange)}`
    }
    if (startTime && endTime) {
      return `&start_time=${encodeURIComponent(startTime)}&end_time=${encodeURIComponent(endTime)}`
    }
    return ''
  }, [relativeRange, startTime, endTime])

  const getFilterQueryString = React.useCallback((assetType: string) => buildAssetFilterQueryString(assetType), [])

  const [activeHelp, setActiveHelp] = React.useState<AssetHelpContent | null>(null)
  const logFields = useActiveLogFields()

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <AlertTriangle className="h-12 w-12 text-destructive mb-4" />
        <h3 className="text-lg font-bold">Error Loading Assets Data</h3>
        <p className="text-muted-foreground mt-2">There was a problem querying the database.</p>
      </div>
    )
  }

  const breakdown = data?.asset_type_breakdown || []
  const cachePerf = data?.cache_performance || []
  const compPerf = data?.compression_performance || []
  const largeUncompressed = data?.large_uncompressed_assets || []
  const lowTtl = data?.low_ttl_assets || []
  const urlFilterHrefPrefix = `/dashboard?service=${activeServiceId}${getTimeQueryString()}&filter_url=`
  const missingFields = ASSET_LOG_FIELDS.filter((field) => logFields.ready && !logFields.isFieldActive(field))

  return (
    <div className={cn('space-y-8 py-6 transition-opacity duration-150', isFetching && !isLoading && 'opacity-60 pointer-events-none')}>
      <RollupDataThroughSlot reserve={rollupFirst} meta={rollup} />
      <RawWindowNote window={readRawWindow(bundle.data)} />

      <AssetsOverviewGrid
        breakdown={breakdown}
        isLoading={isLoading}
        activeServiceId={activeServiceId}
        getTimeQueryString={getTimeQueryString}
        getFilterQueryString={getFilterQueryString}
        onSelectHelp={setActiveHelp}
      />

      <AssetsEfficiencySection
        breakdown={breakdown}
        cachePerf={cachePerf}
        compPerf={compPerf}
        isLoading={isLoading}
        activeServiceId={activeServiceId}
        getTimeQueryString={getTimeQueryString}
        getFilterQueryString={getFilterQueryString}
        onSelectHelp={setActiveHelp}
      />

      <div ref={hotspotsRef}>
        <AssetsHotspotsSection
          largeUncompressed={largeUncompressed}
          lowTtl={lowTtl}
          isLoading={!bundle.data}
          urlFilterHrefPrefix={urlFilterHrefPrefix}
          onSelectHelp={setActiveHelp}
        />
      </div>

      <MissingAssetFieldsBanner missingFields={missingFields} activeServiceId={activeServiceId} />

      <AssetsHelpModal activeHelp={activeHelp} onClose={() => setActiveHelp(null)} />
    </div>
  )
}

export function AssetsClient({ nowServerStr }: { nowServerStr?: string; params?: unknown; searchParams?: unknown } = {}) {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Assets & Shield"
      description="Static asset delivery analysis, payload compression rates, and caching performance analyzer."
      icon={Layers}
      defaultInterval="1 hour"
    >
      {(props) => (
        <AssetsReportContent
          {...props}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}

export default AssetsClient
