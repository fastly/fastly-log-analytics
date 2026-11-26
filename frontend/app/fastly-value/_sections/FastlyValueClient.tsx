'use client'

import React, { useState } from 'react'
import { TrendingUp } from 'lucide-react'
import { useReportPageState, useAnalyticsReportQuery } from '@/hooks/useReportPageState'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { client, unwrapClient } from '@/lib/api'
import type { RangeWire } from '@/lib/utils'
import { ReportLayout } from '@/components/ReportLayout'
import { CardErrorState } from '@/components/AnalyticsCard'
import { readRawWindow, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { hasFilterValues, singleLowCardFilter, VALUE_ROLLUP_SECTIONS } from '@/lib/cloud-engine/rollup-sections'
import type { FiltersPayload } from '@/types/api'
import { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import {
  TABS,
  SUMMARY_SECTIONS,
  SummaryValueTab,
  CachingValueTab,
  SecurityValueTab,
  BotValueTab,
  PerformanceValueTab,
  NetworkValueTab,
  OffloadAndCacheCharts,
  type TabId,
  type ValueData,
} from './ValueDomainTabs'
import { IOValueTab } from './IOValueTab'

export { OffloadAndCacheCharts, IOValueTab }

type ValueSection = 'security' | 'caching' | 'bots' | 'performance' | 'network' | 'io' | 'overview'

interface FastlyValueBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: FiltersPayload
}

function FastlyValueBody({ startTime, endTime, activeServiceId, filterPayload }: FastlyValueBodyProps) {
  const { relativeRange, isAutoRange, anchor } = useReportPageState()
  const { ready: ioFieldsReady, isFieldActive } = useActiveLogFields()
  const ioFieldsEnabled = ioFieldsReady
    ? isFieldActive('io_input_format') || isFieldActive('io_output_format')
    : undefined
  const contentTypeLogged = ioFieldsReady
    ? isFieldActive('content_type') || isFieldActive('resp_header_content_type')
    : undefined
  const [activeTab, setActiveTab] = useState<TabId>('summary')
  const sections = React.useMemo(() => {
    if (activeTab === 'summary') return [...SUMMARY_SECTIONS]
    if (activeTab === 'assets') return ['overview'] // dummy section, query disabled
    return [activeTab]
  }, [activeTab]) as ValueSection[]
  // Cloud answers VALUE_ROLLUP_SECTIONS from summaries when unfiltered, and overview from
  // filtered_rollups for a single low-cardinality include filter on an hour-aligned range;
  // split them only when valueRollupEligible will actually serve the rollup leg.
  const rollupFirst = isCloudMode() && !hasFilterValues(filterPayload)
  const filteredRollup =
    isCloudMode() &&
    singleLowCardFilter(filterPayload, undefined, { start_time: startTime, end_time: endTime }) !== null
  const [rollupSections, rawSections] = React.useMemo(() => {
    if (activeTab === 'assets') return [[], sections]
    if (rollupFirst) {
      const servable = new Set<string>(VALUE_ROLLUP_SECTIONS)
      const fromRollup = sections.filter((section) => servable.has(section))
      return [fromRollup, sections.filter((section) => !fromRollup.includes(section))]
    }
    if (filteredRollup && sections.includes('overview')) {
      return [['overview'] as ValueSection[], sections.filter((section) => section !== 'overview')]
    }
    return [[], sections]
  }, [rollupFirst, filteredRollup, activeTab, sections])
  const split = rollupSections.length > 0
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }
  const postValue = (requested: ValueSection[], rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) =>
    unwrapClient(
      client.POST('/api/value/summary', {
        signal,
        body: {
          filters: filterPayload,
          chart_interval: '1 day',
          sections: requested,
          ...rangeBody,
        },
      }),
    )

  const valueQuery = useAnalyticsReportQuery<ValueData | undefined>({
    namespace: ['value', 'summary'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: split ? ['1 day', activeTab, rawSections] : ['1 day', activeTab],
    queryFn: ({ rangeBody, signal }) => postValue(rawSections, rangeBody, signal),
    queryOptions: {
      enabled: activeTab !== 'assets' && rawSections.length > 0,
    },
  })

  const rollupQuery = useAnalyticsReportQuery<ValueData | undefined>({
    namespace: ['value', 'summary', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: ['1 day', rollupSections],
    queryFn: ({ rangeBody, signal }) => postValue(rollupSections, rangeBody, signal),
    queryOptions: { enabled: split },
  })

  const firstQuery = split ? rollupQuery : valueQuery
  const data = React.useMemo(() => {
    if (!split) return valueQuery.data
    const raw = valueQuery.data
    const roll = rollupQuery.data
    const overview =
      roll?.overview || raw?.overview
        ? {
            ...raw?.overview,
            ...Object.fromEntries(
              Object.entries(roll?.overview ?? {}).filter(([, v]) => v !== null && v !== undefined),
            ),
          }
        : undefined
    return { ...raw, ...roll, ...(overview ? { overview } : {}) } as ValueData
  }, [split, valueQuery.data, rollupQuery.data])
  const hasActiveSectionData =
    activeTab === 'summary'
      ? Boolean(data?.overview || data?.caching || data?.network)
      : activeTab === 'caching'
        ? Boolean(data?.caching)
        : activeTab === 'security'
          ? Boolean(data?.security)
          : activeTab === 'bots'
            ? Boolean(data?.bots)
            : activeTab === 'performance'
              ? Boolean(data?.performance)
              : activeTab === 'network'
                ? Boolean(data?.network)
                : activeTab === 'io'
                  ? Boolean(data?.io)
                  : true
  const loading = activeTab !== 'assets' && (firstQuery.isLoading || (firstQuery.isFetching && !hasActiveSectionData))
  const refreshing = activeTab !== 'assets' && (valueQuery.isFetching || rollupQuery.isFetching) && hasActiveSectionData
  // On a split summary tab the caching cards wait on the raw scan while the overview cards paint.
  const cachingLoading = split && rawSections.length > 0 && valueQuery.isLoading
  const rollup = split ? readRollupMeta(rollupQuery.data) : undefined
  const rawWindow = readRawWindow(valueQuery.data) ?? readRawWindow(rollupQuery.data)

  return (
    <div className="space-y-6">
      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <TabsList className="flex flex-wrap h-auto gap-1">
            {TABS.map((tab) => (
              <TabsTrigger key={tab.id} value={tab.id}>
                <tab.icon className="h-4 w-4" />
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>
          <RollupDataThroughSlot reserve={split || Boolean(rollup)} meta={rollup} rawWindow={rawWindow} />
        </div>
        <div className="min-h-[820px]">
          {loading ? (
            <div className="space-y-6 pt-4">
              <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
              </div>
              <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
                <Skeleton className="h-28" />
              </div>
              <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
                <Skeleton className="h-80" />
                <Skeleton className="h-80" />
              </div>
            </div>
          ) : activeTab !== 'assets' && firstQuery.isError && !hasActiveSectionData ? (
            <div className="pt-4">
              <CardErrorState
                title="Service Summary unavailable"
                message="Could not load summary data for this time window."
                onRetry={() => {
                  void valueQuery.refetch()
                  if (split) void rollupQuery.refetch()
                }}
              />
            </div>
          ) : (
            <div className={refreshing ? 'opacity-40 pointer-events-none transition-opacity' : 'transition-opacity'}>
              <TabsContent value="summary">
                <SummaryValueTab data={data} loading={loading} cachingLoading={cachingLoading} />
              </TabsContent>
              <TabsContent value="caching">
                <CachingValueTab data={data?.caching} />
              </TabsContent>
              <TabsContent value="security">
                <SecurityValueTab data={data?.security} rollup={rollup} />
              </TabsContent>
              <TabsContent value="bots">
                <BotValueTab data={data?.bots} rollup={rollup} />
              </TabsContent>
              <TabsContent value="performance">
                <PerformanceValueTab data={data?.performance} rollup={rollup} />
              </TabsContent>
              <TabsContent value="network">
                <NetworkValueTab data={data?.network} />
              </TabsContent>
              <TabsContent value="assets">
                <AssetsReportContent
                  startTime={startTime}
                  endTime={endTime}
                  activeServiceId={activeServiceId}
                  filterPayload={filterPayload}
                  relativeRange={relativeRange}
                  isAutoRange={isAutoRange}
                  anchor={anchor}
                  embedded
                />
              </TabsContent>
              <TabsContent value="io">
                <IOValueTab
                  data={data?.io}
                  loading={loading}
                  rollup={rollup}
                  ioFieldsEnabled={ioFieldsEnabled}
                  contentTypeLogged={contentTypeLogged}
                />
              </TabsContent>
            </div>
          )}
        </div>
      </Tabs>
    </div>
  )
}

export function FastlyValueClient() {
  return (
    <ReportLayout
      title="Service Summary"
      description="Executive summary of how Fastly delivers value across all products."
      icon={TrendingUp}
      defaultInterval="1 day"
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <FastlyValueBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
        />
      )}
    </ReportLayout>
  )
}

export default FastlyValueClient
