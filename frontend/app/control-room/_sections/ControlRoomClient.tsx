'use client'

import React, { useMemo } from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { Radio, BarChart3, Gauge, HardDrive, Shield, Network, Settings } from 'lucide-react'
import { ReportShell } from '@/components/ReportLayout'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { useTimezone } from '@/hooks/useSSE'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import {
  type MetricsData,
  type MetricsTick,
  useTickHistory,
  RealtimeMetricCard,
  RealtimeChart,
  CorrelatorLeaderboard,
  ConnectionBadge,
  FreshnessBadge,
  useErrorStream,
  useRealtimeStream,
} from './ControlRoomWidgets'
import {
  OverviewTab,
  PerformanceTab,
  OriginTab,
  SecurityTab,
  NetworkTab,
  AdminHealthTab,
  MobileOverview,
} from './ControlRoomTabs'
import { QuarantineSection, SystemJobsStrip, BotSourcesPanel } from './ControlRoomAdminPanels'

export {
  useTickHistory,
  RealtimeMetricCard,
  RealtimeChart,
  CorrelatorLeaderboard,
  QuarantineSection,
  SystemJobsStrip,
  BotSourcesPanel,
}
export type { MetricsData, MetricsTick }

const TABS = [
  { id: 'overview', label: 'Summary', icon: BarChart3, historicalHref: '/dashboard' },
  { id: 'origin', label: 'CDN & Caching', icon: HardDrive, historicalHref: '/origin' },
  { id: 'security', label: 'Security', icon: Shield, historicalHref: '/security' },
  { id: 'performance', label: 'Performance', icon: Gauge, historicalHref: '/performance' },
  { id: 'network', label: 'Network', icon: Network, historicalHref: '/network' },
  { id: 'admin', label: 'Admin', icon: Settings, historicalHref: null, adminOnly: true },
] as const

const TAB_IDS = ['overview', 'origin', 'security', 'performance', 'network', 'admin'] as const
type TabId = (typeof TABS)[number]['id']

export default function ControlRoomClient() {
  const [activeTab, setActiveTab] = useQueryState('tab', parseAsStringLiteral(TAB_IDS).withDefault('overview'))
  const isAnalyst = useIsAnalyst()
  const timezone = useTimezone()
  const { connected, lastTickTime, latestTick, allTicks, rtDown } = useRealtimeStream()
  const { connected: errorsConnected, events: errorEvents } = useErrorStream()
  const { series, rollingAvg } = useTickHistory(allTicks)
  const visibleTabs = useMemo(() => TABS.filter((tab) => !('adminOnly' in tab && tab.adminOnly) || !isAnalyst), [isAnalyst])
  const commonProps = { tick: latestTick, rtDown, series, rollingAvg }

  return (
    <ReportShell
      title="Control Room"
      icon={Radio}
      description="Real-time SRE monitoring"
      headerActions={
        <div className="flex items-center gap-2 flex-wrap">
          <ConnectionBadge connected={connected} />
          <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
        </div>
      }
    >
      <MobileOverview
        connected={connected}
        lastTickTime={lastTickTime}
        timezone={timezone}
        tick={latestTick}
        rtDown={rtDown}
      />
      <div className="hidden md:block">
        <Tabs defaultValue="overview" value={activeTab} onValueChange={(v) => void setActiveTab(v as TabId)}>
          <TabsList className="mb-4 flex-wrap">
            {visibleTabs.map((tab) => (
              <TabsTrigger key={tab.id} value={tab.id}>
                <tab.icon className="h-4 w-4" />
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>
          {visibleTabs.map((tab) => (
            <TabsContent key={tab.id} value={tab.id}>
              {tab.id === 'overview' ? (
                <OverviewTab
                  {...commonProps}
                  historicalHref={tab.historicalHref ?? '/dashboard'}
                  errorEvents={errorEvents}
                  errorsConnected={errorsConnected}
                  timezone={timezone}
                />
              ) : tab.id === 'performance' ? (
                <PerformanceTab {...commonProps} historicalHref={tab.historicalHref ?? '/performance'} />
              ) : tab.id === 'origin' ? (
                <OriginTab {...commonProps} historicalHref={tab.historicalHref ?? '/origin'} />
              ) : tab.id === 'security' ? (
                <SecurityTab {...commonProps} historicalHref={tab.historicalHref ?? '/security'} />
              ) : tab.id === 'network' ? (
                <NetworkTab {...commonProps} historicalHref={tab.historicalHref ?? '/network'} />
              ) : tab.id === 'admin' ? (
                <AdminHealthTab {...commonProps} connected={connected} />
              ) : null}
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </ReportShell>
  )
}
