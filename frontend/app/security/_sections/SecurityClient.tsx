'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useTimezone, useColumnVisibility } from '@/hooks/useSSE'
import { useFieldLabel } from '@/hooks/useBootstrap'
import { useTimeLayout } from '@/lib/utils'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { ReportLayout } from '@/components/ReportLayout'
import { client, extractApiError } from '@/lib/api'
import { Shield } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import type { components } from '@/types/api.generated'
import { BotsSection, type SecurityData } from './BotsSection'
import { ProxyWatchdogSection } from './ProxyWatchdogSection'
import { ThreatIntelPanel, HeaderAnomaliesSection, NetworkSection } from './ThreatIntelSection'

export { BotsSection } from './BotsSection'
export { ProxyWatchdogSection } from './ProxyWatchdogSection'
export { ThreatIntelPanel, HeaderAnomaliesSection, NetworkSection } from './ThreatIntelSection'

const SECURITY_TABS = ['waf-activity', 'threat-intel', 'proxy-watchdog'] as const
const securityTabParser = parseAsStringLiteral(SECURITY_TABS).withDefault('waf-activity')

// MUST stay in lockstep with SECURITY_SSR_SECTIONS in lib/ssr/security.ts
type SecuritySections = NonNullable<components['schemas']['SecurityAggregatesRequest']['sections']>
const SECURITY_SECTIONS: SecuritySections = [
  'ngwaf_verified_bots',
  'ngwaf_verified_bots_ts',
  'wellknown_bots',
  'bot_efficiency',
  'tls_fingerprints',
  'fingerprint_coverage',
  'req_size_dist',
  'top_ips_header',
  'ipv6_adoption',
  'proxy_dist',
  'conn_reuse_dist',
  'proxies',
]

type SecurityFilters = components['schemas']['SecurityAggregatesRequest']['filters']

interface SecurityBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: SecurityFilters
  bucketSeconds: number
  intervalButtons: React.ReactNode
  timezone: string
  getFieldLabel: (field: string) => string
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

function SecurityBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  bucketSeconds,
  intervalButtons,
  timezone,
  getFieldLabel,
  relativeRange,
  isAutoRange,
  anchor,
}: SecurityBodyProps) {
  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)
  const [activeTab, setActiveTab] = useQueryState('tab', securityTabParser)
  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [botEfficiencyVisibility, setBotEfficiencyVisibility, onBotEfficiencyVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility] = useColumnVisibility()

  const securityQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [bucketSeconds],
    queryFn: async ({ rangeBody, signal }) => {
      const { data, error } = await client.POST('/api/security/aggregates', {
        signal,
        body: { filters: filterPayload, bucket_seconds: bucketSeconds, sections: SECURITY_SECTIONS, ...rangeBody },
      })
      if (error) throw new Error(extractApiError(error))
      return data
    },
  })

  const qCommon = {
    data: securityQuery.data,
    isLoading: securityQuery.isLoading,
    isFetching: securityQuery.isFetching,
    error: securityQuery.error ?? null,
  }

  return (
    <Tabs
      value={activeTab}
      onValueChange={(v) => void setActiveTab(v as (typeof SECURITY_TABS)[number])}
      className="space-y-6"
    >
      <TabsList className="grid grid-cols-3 max-w-[600px]">
        <TabsTrigger value="waf-activity">WAF Activity</TabsTrigger>
        <TabsTrigger value="threat-intel">Threat Intelligence</TabsTrigger>
        <TabsTrigger value="proxy-watchdog">VPN & Proxy Watchdog</TabsTrigger>
      </TabsList>

      <TabsContent value="waf-activity" className="space-y-6">
        <BotsSection
          {...qCommon}
          intervalButtons={intervalButtons}
          bucketSeconds={bucketSeconds}
          timezone={timezone}
          commonTimeLayout={commonTimeLayout}
          getFieldLabel={getFieldLabel}
          ngwafBotVisibility={ngwafBotVisibility}
          setNgwafBotVisibility={setNgwafBotVisibility}
          onNgwafBotVisChange={(id, vis) => setNgwafBotVisibility((prev) => ({ ...prev, [id]: vis }))}
          botVisibility={botVisibility}
          setBotVisibility={setBotVisibility}
          onBotVisChange={onBotVisChange}
          botEfficiencyVisibility={botEfficiencyVisibility}
          setBotEfficiencyVisibility={setBotEfficiencyVisibility}
          onBotEfficiencyVisChange={onBotEfficiencyVisChange}
          fingerprintVisibility={fingerprintVisibility}
          setFingerprintVisibility={setFingerprintVisibility}
          onFingerprintVisChange={onFingerprintVisChange}
        />
        <HeaderAnomaliesSection
          {...qCommon}
          getFieldLabel={getFieldLabel}
          topIpVisibility={topIpVisibility}
          setTopIpVisibility={setTopIpVisibility}
          onTopIpVisChange={onTopIpVisChange}
        />
        <NetworkSection {...qCommon} timezone={timezone} commonTimeLayout={commonTimeLayout} />
      </TabsContent>

      <TabsContent value="threat-intel">
        <ThreatIntelPanel serviceId={activeServiceId} startTime={startTime} endTime={endTime} />
      </TabsContent>

      <TabsContent value="proxy-watchdog">
        <ProxyWatchdogSection
          data={securityQuery.data}
          isLoading={securityQuery.isLoading}
          error={securityQuery.error ?? null}
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
        />
      </TabsContent>
    </Tabs>
  )
}

export default function SecurityClient({ nowServerStr }: { nowServerStr?: string }) {
  const getFieldLabel = useFieldLabel()
  const timezone = useTimezone()
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
      icon={Shield}
    >
      {(props) => (
        <SecurityBody
          {...props}
          timezone={timezone}
          getFieldLabel={getFieldLabel}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}
