'use client'

import React from 'react'
import { AlertTriangle, Layers } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useReportPageState'
import { client, unwrapClient } from '@/lib/api'
import { cn, type RangeWireArgs } from '@/lib/utils'
import type { FiltersPayload } from '@/types/api'
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
  const bundle = useAnalyticsReportQuery<AssetsAggregates | undefined>({
    namespace: ['assets', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    queryFn: async ({ rangeBody, signal }) => {
      return unwrapClient(client.POST('/api/assets/aggregates', {
        signal,
        body: {
          filters: filterPayload,
          ...rangeBody,
        },
      }))
    },
  })

  const isLoading = bundle.isLoading
  const isFetching = bundle.isFetching
  const error = bundle.error
  const data = bundle.data

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
  const missingFields = data?.missing_fields || []

  return (
    <div className={cn('space-y-8 py-6 transition-opacity duration-150', isFetching && !isLoading && 'opacity-60 pointer-events-none')}>
      <MissingAssetFieldsBanner missingFields={missingFields} activeServiceId={activeServiceId} />

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

      <AssetsHotspotsSection
        largeUncompressed={largeUncompressed}
        lowTtl={lowTtl}
        isLoading={isLoading}
        urlFilterHrefPrefix={urlFilterHrefPrefix}
        onSelectHelp={setActiveHelp}
      />

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
