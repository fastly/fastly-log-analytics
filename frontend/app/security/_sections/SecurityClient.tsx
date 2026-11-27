'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useTimezone, useColumnVisibility } from '@/hooks/useSSE'
import { useFieldLabel } from '@/hooks/useBootstrap'
import { useTimeLayout, type RangeWire } from '@/lib/utils'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { ReportLayout } from '@/components/ReportLayout'
import { client, extractApiError } from '@/lib/api'
import { Shield } from 'lucide-react'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { mergeRollupMeta, RawWindowNote, readRawWindow, readRollupMeta, RollupDataThroughSlot } from '@/components/network/rollupLabels'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { hasFilterValues } from '@/lib/cloud-engine/rollup-sections'
import type { components, operations } from '@/types/api.generated'
import { BotsSection, type SecurityData, type SummaryQueryState } from './BotsSection'

const ThreatIntelPanel = dynamic(
  () => import('./ThreatIntelSection').then((m) => m.ThreatIntelPanel),
  { ssr: false, loading: () => <Skeleton className="w-full h-[360px] rounded-xl" /> },
)

const HeaderAnomaliesSection = dynamic(
  () => import('./ThreatIntelSection').then((m) => m.HeaderAnomaliesSection),
  { ssr: false, loading: () => <Skeleton className="w-full h-[320px] rounded-xl" /> },
)

const NetworkSection = dynamic(
  () => import('./ThreatIntelSection').then((m) => m.NetworkSection),
  { ssr: false, loading: () => <Skeleton className="w-full h-[320px] rounded-xl" /> },
)

const ProxyWatchdogSection = dynamic(
  () => import('./ProxyWatchdogSection').then((m) => m.ProxyWatchdogSection),
  { ssr: false, loading: () => <Skeleton className="w-full h-[420px] rounded-xl" /> },
)

export { BotsSection } from './BotsSection'
export { ThreatIntelPanel, HeaderAnomaliesSection, NetworkSection }

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
const WAF_ACTIVITY_SECTIONS: SecuritySections = SECURITY_SECTIONS.filter((s) => s !== 'proxies')
const WAF_ACTIVITY_RAW_SECTIONS: SecuritySections = ['wellknown_bots', 'bot_efficiency', 'top_ips_header']
const PROXIES_SECTIONS: SecuritySections = ['proxies']

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

  const cloudMode = isCloudMode()
  const unfiltered = !hasFilterValues(filterPayload)
  const rollupFirst = cloudMode && unfiltered
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }
  const fetchSecurity = async (sections: SecuritySections, rangeBody: RangeWire['rangeBody'], signal: AbortSignal | undefined) => {
    if (!cloudMode && unfiltered) {
      type BundleQuery = NonNullable<
        operations['security_bundle_get_api_security_bundle_get']['parameters']['query']
      > & { sections?: SecuritySections }
      const query: BundleQuery = {
        bucket_seconds: bucketSeconds,
        sections,
        ...rangeBody,
      }
      const { data, error } = await client.GET('/api/security/bundle', {
        signal,
        params: { query },
      })
      if (error) throw new Error(extractApiError(error))
      return data
    }
    const { data, error } = await client.POST('/api/security/aggregates', {
      signal,
      body: { filters: filterPayload, bucket_seconds: bucketSeconds, sections, ...rangeBody },
    })
    if (error) throw new Error(extractApiError(error))
    return data
  }

  // On Cloud, defer the `proxies` section (10 geo/RTT columns) until the Proxy Watchdog tab is active.
  const mainSections = cloudMode
    ? (rollupFirst ? WAF_ACTIVITY_RAW_SECTIONS : WAF_ACTIVITY_SECTIONS)
    : SECURITY_SECTIONS

  const securityQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: cloudMode ? [bucketSeconds, mainSections] : [bucketSeconds],
    queryFn: ({ rangeBody, signal }) => fetchSecurity(mainSections, rangeBody, signal),
  })

  const proxiesQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates', 'proxies'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [bucketSeconds],
    queryOptions: { enabled: cloudMode && activeTab === 'proxy-watchdog' },
    queryFn: ({ rangeBody, signal }) => fetchSecurity(PROXIES_SECTIONS, rangeBody, signal),
  })

  const rollupQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates', 'rollup'],
    serviceId: activeServiceId,
    filterPayload,
    range,
    keyParts: [bucketSeconds],
    queryOptions: { enabled: rollupFirst },
    queryFn: ({ rangeBody, signal }) => fetchSecurity(SUMMARY_SECTIONS, rangeBody, signal),
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
        <RollupDataThroughSlot
          reserve={rollupFirst}
          meta={cloudMode ? readRollupMeta(proxiesQuery.data) : rowRollup}
          className="mb-6"
        />
        <ProxyWatchdogSection
          data={cloudMode ? proxiesQuery.data : securityQuery.data}
          rollup={cloudMode ? readRollupMeta(proxiesQuery.data) : rowRollup}
          isLoading={cloudMode ? proxiesQuery.isLoading : securityQuery.isLoading}
          error={cloudMode ? (proxiesQuery.error ?? null) : (securityQuery.error ?? null)}
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
      description="Monitor TLS cipher and protocol health, verify well-known bot and AI crawler identities, analyze request anomalies, inspect proxy exposure, and track client fingerprint coverage."
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
