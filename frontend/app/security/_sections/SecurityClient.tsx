'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useTimezone, useColumnVisibility } from '@/hooks/useSSE'
import { useFieldLabel } from '@/hooks/useBootstrap'
import { useTimeLayout, type RangeWire } from '@/lib/utils'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { ReportLayout } from '@/components/ReportLayout'
import { client, extractApiError } from '@/lib/api'
import { Shield } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { mergeRollupMeta, RawWindowNote, readRawWindow, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { hasFilterValues } from '@/lib/cloud-engine/rollup-sections'
import type { components } from '@/types/api.generated'
import { BotsSection, type SecurityData, type SummaryQueryState } from './BotsSection'
import { ProxyWatchdogSection } from './ProxyWatchdogSection'
import { ThreatIntelPanel, HeaderAnomaliesSection, NetworkSection } from './ThreatIntelSection'

export { BotsSection } from './BotsSection'
export { ProxyWatchdogSection } from './ProxyWatchdogSection'
export { ThreatIntelPanel, HeaderAnomaliesSection, NetworkSection } from './ThreatIntelSection'

const SECURITY_TABS = ['waf-activity', 'threat-intel', 'proxy-watchdog'] as const
const securityTabParser = parseAsStringLiteral(SECURITY_TABS).withDefault('waf-activity')

// MUST stay in lockstep with SECURITY_SSR_SECTIONS in lib/ssr/upstream.ts
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
// Row-level sections, read apart so the distributions paint first: Cloud answers them from the
// bot, header-IP and proxy summary rows when unfiltered, or from raw Parquet when summaries predate them.
const RAW_SECTIONS: SecuritySections = ['wellknown_bots', 'bot_efficiency', 'top_ips_header', 'proxies']
const SUMMARY_SECTIONS = SECURITY_SECTIONS.filter((s) => !RAW_SECTIONS.includes(s))

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

  const rollupFirst = isCloudMode() && !hasFilterValues(filterPayload)
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }
  const postSecurity = async (sections: SecuritySections, rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) => {
    const { data, error } = await client.POST('/api/security/aggregates', {
      signal,
      body: { filters: filterPayload, bucket_seconds: bucketSeconds, sections, ...rangeBody },
    })
    if (error) throw new Error(extractApiError(error))
    return data
  }

  const securityQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: rollupFirst ? [bucketSeconds, RAW_SECTIONS] : [bucketSeconds],
    queryFn: ({ rangeBody, signal }) => postSecurity(rollupFirst ? RAW_SECTIONS : SECURITY_SECTIONS, rangeBody, signal),
  })

  const rollupQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [bucketSeconds],
    queryOptions: { enabled: rollupFirst },
    queryFn: ({ rangeBody, signal }) => postSecurity(SUMMARY_SECTIONS, rangeBody, signal),
  })

  const rawData = securityQuery.data
  const rollupData = rollupFirst ? rollupQuery.data : undefined
  const data = React.useMemo(
    () => (rollupData ? ({ ...rawData, ...rollupData } as SecurityData) : rawData),
    [rawData, rollupData],
  )
  const rowRollup = rollupFirst ? readRollupMeta(rawData) : undefined
  const rollup = mergeRollupMeta(readRollupMeta(rollupData), rowRollup)
  const summary: SummaryQueryState | undefined = rollupFirst
    ? {
        isLoading: rollupQuery.isLoading,
        isFetching: rollupQuery.isFetching,
        error: rollupQuery.error ?? null,
        rollup,
      }
    : undefined

  const qCommon = {
    data,
    isLoading: securityQuery.isLoading,
    isFetching: securityQuery.isFetching,
    error: securityQuery.error ?? null,
    summary,
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
        <RollupDataThroughSlot reserve={rollupFirst} meta={rollup} />
        <RawWindowNote window={readRawWindow(rawData)} />
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
        <RollupDataThroughSlot reserve={rollupFirst} meta={rowRollup} className="mb-6" />
        <ProxyWatchdogSection
          data={securityQuery.data}
          rollup={rowRollup}
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
