'use client'

import React, { useState } from 'react'
import {
  Shield,
  Network,
  Bot,
  Gauge,
  HardDrive,
  ImageIcon,
  BarChart3,
  Layers,
} from 'lucide-react'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { resolveRangeWire } from '@/lib/range-wire'
import { client } from '@/lib/api'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import type { components } from '@/types/api.generated'
import { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import SummaryValueTab from './tabs/SummaryValueTab'
import CachingValueTab from './tabs/CachingValueTab'
import SecurityValueTab from './tabs/SecurityValueTab'
import BotValueTab from './tabs/BotValueTab'
import PerformanceValueTab from './tabs/PerformanceValueTab'
import NetworkValueTab from './tabs/NetworkValueTab'
import IOValueTab from './tabs/IOValueTab'

type ValueData = components['schemas']['ValueSummaryResponse']

const TABS = [
  { id: 'summary', label: 'Summary', icon: BarChart3 },
  { id: 'caching', label: 'CDN & Caching', icon: HardDrive },
  { id: 'assets', label: 'Assets & Shield', icon: Layers },
  { id: 'security', label: 'Security', icon: Shield },
  { id: 'bots', label: 'Bot Management', icon: Bot },
  { id: 'performance', label: 'Performance', icon: Gauge },
  { id: 'network', label: 'Network', icon: Network },
  { id: 'io', label: 'Image Optimizer', icon: ImageIcon },
] as const

const SUMMARY_SECTIONS = ['overview', 'caching', 'network'] as const

type TabId = (typeof TABS)[number]['id']

interface FastlyValueBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- matches ReportLayout render-prop type
  filterPayload: any

  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

export default function FastlyValueBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: FastlyValueBodyProps) {
  const [activeTab, setActiveTab] = useState<TabId>('summary')

  const { rangeKey, rangeBody } = resolveRangeWire({
    relativeRange,
    isAutoRange,
    startTime,
    endTime,
    anchor,
  })

  const sections = React.useMemo(() => {
    if (activeTab === 'summary') return [...SUMMARY_SECTIONS]
    if (activeTab === 'assets') return ['overview'] // dummy section, query disabled
    return [activeTab]
  }, [activeTab]) as ("security" | "caching" | "bots" | "performance" | "network" | "io" | "overview")[]

  const valueQuery = useServiceQuery<ValueData | undefined>(
    ['value', 'summary', activeServiceId, rangeKey, anchor, filterPayload, '1 day', activeTab],
    async ({ signal }) => {
      const { data } = await client.POST('/api/value/summary', {
        signal,
        body: {
          filters: filterPayload,
          chart_interval: '1 day',
          sections,
          ...rangeBody,
        },
      })
      return data
    },
    {
      enabled: activeTab !== 'assets',
    }
  )

  const data = valueQuery.data
  const loading = activeTab !== 'assets' && (valueQuery.isLoading || (valueQuery.isFetching && valueQuery.isPlaceholderData))

  return (
    <div className="space-y-6">
      <Tabs
        value={activeTab}
        onValueChange={(v) => setActiveTab(v as TabId)}
      >
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
          <>
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
          </>
        )}
      </Tabs>
    </div>
  )
}
