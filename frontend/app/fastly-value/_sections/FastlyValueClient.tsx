'use client'

import React, { useState } from 'react'
import { TrendingUp } from 'lucide-react'
import { useReportPageState, useAnalyticsReportQuery } from '@/hooks/useReportPageState'
import { client, unwrapClient } from '@/lib/api'
import { ReportLayout } from '@/components/ReportLayout'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
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

interface FastlyValueBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: FiltersPayload
}

function FastlyValueBody({ startTime, endTime, activeServiceId, filterPayload }: FastlyValueBodyProps) {
  const { relativeRange, isAutoRange, anchor } = useReportPageState()
  const [activeTab, setActiveTab] = useState<TabId>('summary')
  const sections = React.useMemo(() => {
    if (activeTab === 'summary') return [...SUMMARY_SECTIONS]
    if (activeTab === 'assets') return ['overview'] // dummy section, query disabled
    return [activeTab]
  }, [activeTab]) as ('security' | 'caching' | 'bots' | 'performance' | 'network' | 'io' | 'overview')[]

  const valueQuery = useAnalyticsReportQuery<ValueData | undefined>({
    namespace: ['value', 'summary'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: ['1 day', activeTab],
    queryFn: async ({ rangeBody, signal }) => {
      return unwrapClient(
        client.POST('/api/value/summary', {
          signal,
          body: {
            filters: filterPayload,
            chart_interval: '1 day',
            sections,
            ...rangeBody,
          },
        }),
      )
    },
    queryOptions: {
      enabled: activeTab !== 'assets',
    },
  })

  const data = valueQuery.data
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
  const loading = activeTab !== 'assets' && (valueQuery.isLoading || (valueQuery.isFetching && !hasActiveSectionData))
  const refreshing = activeTab !== 'assets' && valueQuery.isFetching && hasActiveSectionData

  return (
    <div className="space-y-6">
      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
        <TabsList className="flex flex-wrap h-auto gap-1">
          {TABS.map((tab) => (
            <TabsTrigger key={tab.id} value={tab.id}>
              <tab.icon className="h-4 w-4" />
              {tab.label}
            </TabsTrigger>
          ))}
        </TabsList>
        {loading ? (
          <div className="space-y-4 pt-4">
            <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
              <Skeleton className="h-28" />
            </div>
            <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
              <Skeleton className="h-72" />
              <Skeleton className="h-72" />
            </div>
          </div>
        ) : (
          <div className={refreshing ? 'opacity-40 pointer-events-none transition-opacity' : 'transition-opacity'}>
            <TabsContent value="summary">
              <SummaryValueTab data={data} loading={loading} />
            </TabsContent>
            <TabsContent value="caching">
              <CachingValueTab data={data?.caching} />
            </TabsContent>
            <TabsContent value="security">
              <SecurityValueTab data={data?.security} />
            </TabsContent>
            <TabsContent value="bots">
              <BotValueTab data={data?.bots} />
            </TabsContent>
            <TabsContent value="performance">
              <PerformanceValueTab data={data?.performance} />
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
              />
            </TabsContent>
            <TabsContent value="io">
              <IOValueTab data={data?.io} loading={loading} />
            </TabsContent>
          </div>
        )}
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
