'use client'

import React from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import { useTimezone } from '@/hooks/useSSE'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields, useFieldLabel } from '@/hooks/useBootstrap'
import { useTimeLayout } from '@/lib/chart-helpers'
import { CHART_COLORS, useTimeseriesToTraces } from '@/lib/chart-helpers'
import { useAnalyticsReportQuery, useReportRangeAnchor, useServiceQuery } from '@/hooks/useDashboardCards'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, ChartEmptyState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown, FilterValueCell } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { formatCount, formatCountMaybe } from '@/lib/format'
import { client, extractApiError } from '@/lib/api'
import { Activity, Cpu, Globe, HelpCircle, Network, Repeat, Scale, Shield, ShieldAlert } from 'lucide-react'
import { BotsSection, SECURITY_INFO, TOP_IP_COLUMN_IDS } from './BotsSection'
import { ProxyWatchdogSection } from './ProxyWatchdogSection'
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import type { PlotlyLayout, PlotlyTrace } from '@/types/api'
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityAggregatesResponse']
type TopIpHeader = components['schemas']['TopIpHeaderRow']
type ThreatIntelItem = components['schemas']['ThreatIntelItem']

export function ThreatIntelPanel({
  serviceId,
  startTime,
  endTime,
}: {
  serviceId: string | null
  startTime: string | null
  endTime: string | null
}) {
  const [isHelpOpen, React_useState] = React.useState(false)

  const { data, isLoading, error } = useServiceQuery<ThreatIntelItem[]>(
    ['security', 'threat-intel', serviceId, startTime, endTime],
    async ({ signal }) => {
      if (!serviceId) return []
      const { data: res, error: reqErr } = await client.GET('/api/security/threat-intel', {
        signal,
        params: {
          query: {
            start_time: startTime ?? undefined,
            end_time: endTime ?? undefined,
          },
        },
      })
      if (reqErr) throw new Error(extractApiError(reqErr))
      return res?.data || []
    },
    { enabled: !!serviceId },
  )

  const columns = React.useMemo<ColumnDef<ThreatIntelItem>[]>(() => [
    {
      accessorKey: 'tls_fingerprint',
      header: 'TLS Fingerprint (JA3/JA4)',
      cell: ({ row }) => {
        const val = row.original.tls_fingerprint
        const filterKey = val.includes('_') ? 'ja4' : 'ja3'
        return (
          <div className="font-mono text-xs font-semibold select-all w-full max-w-[320px] truncate">
            <FilterValueCell filters={[{ column: filterKey, value: val }]} display={val} />
          </div>
        )
      },
    },
    {
      accessorKey: 'requests',
      header: 'Requests',
      cell: ({ row }) => (
        <span className="font-semibold font-mono text-xs text-foreground/80 tabular-nums">
          {formatCount(row.original.requests)}
        </span>
      ),
    },
    {
      accessorKey: 'top_country',
      header: 'Top Country',
      cell: ({ row }) => {
        const country = row.original.top_country || 'Unknown'
        return (
          <div className="flex items-center gap-1.5 text-xs font-medium text-foreground/80">
            <Globe className="h-3.5 w-3.5 text-muted-foreground" />
            <FilterValueCell filters={[{ column: 'country', value: country }]} display={country} />
          </div>
        )
      },
    },
    {
      accessorKey: 'matched_waf_rules_count',
      header: 'Matched WAF Rules',
      cell: ({ row }) => {
        const count = row.original.matched_waf_rules_count
        return count > 0 ? (
          <Badge variant="outline" className="font-mono bg-destructive/10 text-destructive border-destructive/20 hover:bg-destructive/15">
            {formatCount(count)} triggers
          </Badge>
        ) : (
          <span className="text-xs text-muted-foreground font-medium">None</span>
        )
      },
    },
    {
      accessorKey: 'bot_percentage',
      header: 'Bot Percentage',
      cell: ({ row }) => {
        const pct = row.original.bot_percentage
        return pct > 0 ? (
          <div className="flex items-center gap-1.5">
            <Cpu className="h-3.5 w-3.5 text-orange-500" />
            <span className="font-mono font-bold text-xs text-orange-500">{pct.toFixed(1)}%</span>
          </div>
        ) : (
          <span className="text-xs text-muted-foreground font-medium">0.0%</span>
        )
      },
    },
    {
      accessorKey: 'is_anonymized_proxy',
      header: 'Anonymity Category',
      cell: ({ row }) =>
        row.original.is_anonymized_proxy ? (
          <Badge
            variant="outline"
            className="font-semibold text-[10px] uppercase tracking-wider px-2 py-0.5 bg-red-500/10 text-red-500 border-red-500/20 hover:bg-red-500/15 border shadow-none"
          >
            VPN / Proxy / Tor
          </Badge>
        ) : (
          <Badge
            variant="secondary"
            className="font-semibold text-[10px] uppercase tracking-wider px-2 py-0.5 bg-emerald-500/10 text-emerald-500 border-emerald-500/20 hover:bg-emerald-500/15 border shadow-none"
          >
            Residential
          </Badge>
        ),
    },
  ], [])

  if (isLoading) {
    return (
      <Card className="col-span-12 border-muted/50 bg-background/50 backdrop-blur-xs">
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
          <div className="space-y-1">
            <CardTitle className="text-lg font-semibold tracking-tight flex items-center gap-2">
              <Shield className="h-5 w-5 text-primary animate-pulse" />
              Client TLS Fingerprints & Threat Intelligence
            </CardTitle>
          </div>
        </CardHeader>
        <CardContent className="h-64 flex flex-col items-center justify-center space-y-4">
          <div className="relative flex h-10 w-10 items-center justify-center">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary/20 opacity-75"></span>
            <Activity className="h-5 w-5 text-primary animate-pulse" />
          </div>
          <span className="text-sm font-medium text-muted-foreground animate-pulse">Aggregating real-time threat telemetry...</span>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card className="col-span-12 border-destructive/20 bg-destructive/5">
        <CardContent className="py-10 flex flex-col items-center justify-center text-center space-y-3">
          <ShieldAlert className="h-10 w-10 text-destructive" />
          <h3 className="text-base font-semibold text-destructive">Threat Intelligence Analysis Failed</h3>
          <p className="text-sm text-muted-foreground max-w-md">{error.message || "An unexpected error occurred while analyzing telemetry."}</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <>
      <Card className="col-span-12 border-muted/40 shadow-sm bg-card p-0">
        <DataTable
          columns={columns}
          data={data || []}
          isLoading={isLoading}
          searchKey="tls_fingerprint"
          initialSorting={[{ id: 'requests', desc: true }]}
          title={
            <div className="space-y-1 py-1">
              <div className="flex items-center gap-2">
                <Shield className="h-5 w-5 text-primary" />
                <span className="text-base font-bold tracking-tight">
                  Client TLS Fingerprints & Threat Intelligence
                </span>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0 rounded-full"
                  onClick={() => React_useState(true)}
                  title="Learn more about Threat Intelligence"
                >
                  <HelpCircle className="h-4 w-4" />
                </Button>
              </div>
              <p className="text-xs text-muted-foreground font-normal">
                Correlated TLS fingerprinting (JA3/JA4) with active WAF signals, proxy routes, and automated client behavior.
              </p>
            </div>
          }
        />
      </Card>

      <HelpDialog
        open={isHelpOpen}
        onOpenChange={React_useState}
        title="Client TLS Fingerprints & Threat Intelligence Guide"
        icon={<Shield className="h-5 w-5 text-primary" />}
        size="xl"
      >
        <div className="space-y-4 text-sm text-muted-foreground">
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">
              How This Data Works
            </h4>
            <p className="leading-relaxed">
              This panel correlates client <strong className="font-semibold text-foreground">TLS Fingerprints (JA3/JA4)</strong> with active <strong className="font-semibold text-foreground">WAF rule triggers</strong>, <strong className="font-semibold text-foreground">automated bot percentage</strong>, and <strong className="font-semibold text-foreground">network anonymity</strong> attributes. Rather than relying on easily spoofable HTTP User-Agents, TLS fingerprinting analyzes the unique cryptographic handshake parameters (SSL version, cipher suites, extension details) sent by the client&apos;s socket library.
            </p>
          </div>
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">
              Anonymity Categories
            </h4>
            <ul className="space-y-3 mt-2">
              <li className="flex gap-2.5 items-start">
                <Badge
                  variant="outline"
                  className="font-semibold text-[9px] uppercase tracking-wider px-1.5 py-0.5 bg-red-500/10 text-red-500 border-red-500/20 shrink-0 select-none shadow-none mt-0.5"
                >
                  VPN / Proxy / Tor
                </Badge>
                <div>
                  <strong className="text-foreground font-medium block text-xs">Anonymized / Proxy Traffic</strong>
                  Indicates the connection originates from a commercial VPN provider, public or private proxy, Tor Exit Node, or hosting data center.
                </div>
              </li>
              <li className="flex gap-2.5 items-start">
                <Badge
                  variant="secondary"
                  className="font-semibold text-[9px] uppercase tracking-wider px-1.5 py-0.5 bg-emerald-500/10 text-emerald-500 border-emerald-500/20 shrink-0 select-none shadow-none mt-0.5"
                >
                  Residential
                </Badge>
                <div>
                  <strong className="text-foreground font-medium block text-xs">Residential Broadband & Mobile</strong>
                  Indicates standard consumer broadband, home Wi-Fi, or cellular networks.
                </div>
              </li>
            </ul>
          </div>
        </div>
      </HelpDialog>
    </>
  )
}

type HeaderAnomaliesProps = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
  getFieldLabel: (id: string) => string
  topIpVisibility: VisibilityState
  setTopIpVisibility: Dispatch<SetStateAction<VisibilityState>>
  onTopIpVisChange: (id: string, vis: boolean) => void
}

export function HeaderAnomaliesSection({
  data,
  isLoading,
  isFetching,
  error,
  getFieldLabel,
  topIpVisibility,
  setTopIpVisibility,
  onTopIpVisChange,
}: HeaderAnomaliesProps) {
  // Distinguish "field group not enabled" from "enabled but no data yet" so a
  // low-traffic/fresh service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  const headerSizeData = React.useMemo((): PlotlyTrace[] => {
    const req_size_dist = data?.req_size_dist
    if (!req_size_dist?.length) return []
    return [{
      x: req_size_dist.map((d) => d.bucket ?? d.label ?? null),
      y: req_size_dist.map((d) => d.count),
      type: 'bar',
      marker: { color: '#ec4899' }
    }]
  }, [data])

  const topIpHeaderColumns: ColumnDef<TopIpHeader>[] = [
    {
      accessorKey: 'ip',
      header: 'IP Address',
      cell: ({ row }) => (
        <FilterValueCell
          filters={[{ column: 'client_ip', value: row.original.ip ?? '' }]}
          className="font-mono text-xs"
        />
      )
    },
    { accessorKey: 'max_header', header: 'Max Header (Bytes)', cell: ({ row }) => formatCountMaybe(row.original.max_header) ?? '—' },
  ]

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
      <AnalyticsCard
        title="Request Header Size Distribution"
        icon={<Scale className="h-4 w-4" />}
        isLoading={isLoading}
        isFetching={isFetching}
        loadingMessage="Loading request header size distribution..."
        error={error as AnalyticsCardError | null}
        className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
        contentClassName="p-2"
        helpTitle={SECURITY_INFO.req_size.title}
        helpContent={SECURITY_INFO.req_size.body}
      >
        {headerSizeData.length === 0 && !isLoading ? (
          <ChartEmptyState requires={isFieldActive('req_header_bytes') ? undefined : "Request Identity (Group A) fields to be enabled in Fastly logging."} />
        ) : (
          <PlotlyChart
            data={headerSizeData}
            layout={{ yaxis: { title: 'Count' } }}
            height="100%"
          />
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Oversized Request Headers (by IP)"
        icon={<Shield className="h-4 w-4" />}
        headerAction={
          <ColumnVisibilityDropdown
            columns={TOP_IP_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))}
            visibility={topIpVisibility}
            onChange={onTopIpVisChange}
          />
        }
        isLoading={isLoading}
        isFetching={isFetching}
        loadingMessage="Loading oversized request headers..."
        error={error as AnalyticsCardError | null}
        className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
        contentClassName="p-0"
        helpTitle={SECURITY_INFO.top_ips_header.title}
        helpContent={SECURITY_INFO.top_ips_header.body}
      >
        <DataTable
          columns={topIpHeaderColumns}
          data={data?.top_ips_header || []}
          emptyMessage={isLoading ? "" : (isFieldActive('req_header_bytes') ? "No data in this time range yet." : "Requires Request Identity (Group A) log fields to be enabled in Fastly logging.")}
          hideToolbar
          columnVisibility={topIpVisibility}
          onColumnVisibilityChange={setTopIpVisibility}
        />
      </AnalyticsCard>
    </div>
  )
}

type NetworkSectionProps = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
  timezone: string
  commonTimeLayout: PlotlyLayout
}

export function NetworkSection({
  data,
  isLoading,
  isFetching,
  error,
  timezone,
  commonTimeLayout,
}: NetworkSectionProps) {
  // Distinguish "field group not enabled" from "enabled but no data yet" so a
  // low-traffic/fresh service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  const ipv6Data = useTimeseriesToTraces(
    data?.ipv6_adoption,
    [{ key: 'pct', name: 'IPv6 %', color: '#8b5cf6', fill: 'tozeroy' }],
    timezone,
  )

  const proxyData = React.useMemo((): PlotlyTrace[] => {
    const proxy_dist = data?.proxy_dist
    if (!proxy_dist?.length) return []
    return [{
      values: proxy_dist.map((d) => d.count),
      labels: proxy_dist.map((d) => d.type ?? null),
      type: 'pie',
      hole: 0.4,
      marker: { colors: [CHART_COLORS.info, CHART_COLORS.success, CHART_COLORS.warning, CHART_COLORS.danger, '#8b5cf6'] }
    }]
  }, [data])

  const connReuseData = React.useMemo((): PlotlyTrace[] => {
    const conn_reuse_dist = data?.conn_reuse_dist
    if (!conn_reuse_dist?.length) return []
    return [{
      x: conn_reuse_dist.map((d) => d.bucket ?? d.label ?? null),
      y: conn_reuse_dist.map((d) => d.count),
      type: 'bar',
      marker: { color: '#06b6d4' }
    }]
  }, [data])

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="IPv6 Adoption over Time"
          icon={<Globe className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading IPv6 adoption data..."
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.ipv6.title}
          helpContent={SECURITY_INFO.ipv6.body}
        >
          {ipv6Data.length === 0 && !isLoading ? (
            <ChartEmptyState requires={isFieldActive('is_ipv6') ? undefined : "Infrastructure (Group C) fields to be enabled in Fastly logging."} />
          ) : (
            <PlotlyChart
              data={ipv6Data}
              layout={{
                ...commonTimeLayout,
                yaxis: { title: 'IPv6 %', range: [0, 100] }
              }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Proxy/Anonymizer Breakdown"
          icon={<Network className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading proxy/anonymizer breakdown..."
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.proxy.title}
          helpContent={SECURITY_INFO.proxy.body}
        >
          {proxyData.length === 0 && !isLoading ? (
            <ChartEmptyState requires={isFieldActive('p_type') ? undefined : "Security: Proxy & Anonymization (Group I) fields to be enabled in Fastly logging."} />
          ) : (
            <PlotlyChart
              data={proxyData}
              height="100%"
            />
          )}
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Connection Reuse (Requests per Connection)"
          icon={<Repeat className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading connection reuse distribution..."
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.conn_reuse.title}
          helpContent={SECURITY_INFO.conn_reuse.body}
        >
          {connReuseData.length === 0 && !isLoading ? (
            <ChartEmptyState requires={isFieldActive('conn_requests') ? undefined : "Infrastructure (Group C) fields to be enabled in Fastly logging."} />
          ) : (
            <PlotlyChart
              data={connReuseData}
              layout={{ yaxis: { title: 'Count' } }}
              height="100%"
            />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}

// Per-section field lists. The backend gates each section block on
// `sections is None or 'name' in sections`; passing only the names a
// given React section needs keeps the other section blocks from
// running. The fingerprint cards + fingerprint_coverage are bundled
// together because security.py:858 reuses a shared scan for them; the
// two ngwaf_verified_bots* fields share a per-conn ngwaf_cache ATTACH.
// BotsSection currently renders the fingerprint cards, so they live
// in its request rather than HeaderAnomaliesSection's.
type SecuritySections = NonNullable<components['schemas']['SecurityAggregatesRequest']['sections']>
const BOTS_SECTIONS: SecuritySections = [
  'ngwaf_verified_bots',
  'ngwaf_verified_bots_ts',
  'wellknown_bots',
  'bot_efficiency',
  'tls_fingerprints',
  'fingerprint_coverage',
]
const HEADER_ANOMALIES_SECTIONS: SecuritySections = ['req_size_dist', 'top_ips_header']
const NETWORK_SECTIONS: SecuritySections = ['ipv6_adoption', 'proxy_dist', 'conn_reuse_dist']
const PROXY_SECTIONS: SecuritySections = ['proxies']
// Union of all section groups — fetched in a single POST so the page
// makes one /api/security/aggregates call instead of concurrent ones.
// On the 4-core prod VM, parallel DuckDB scans oversubscribe the cores
// (see memory: cpu-bound queries don't parallelize there); one combined scan
// avoids that contention. Trade-off vs. the prior split: the sections
// now share a single loading state (no progressive above-the-fold paint) and
// a single error boundary.
//
// MUST stay in lockstep with SECURITY_SSR_SECTIONS in lib/ssr/security.ts —
// the section list is part of the SSR-seed query key, so a divergence here
// would miss the dehydrated cache and double-fetch on first paint.
const SECURITY_SECTIONS: SecuritySections = [...BOTS_SECTIONS, ...HEADER_ANOMALIES_SECTIONS, ...NETWORK_SECTIONS, ...PROXY_SECTIONS]

// Lifted out of the ReportLayout render-prop so useTimeLayout + useServiceQuery
// live at the top of a STABLE component instead of being called inside the
// `{(ctx) => { …hooks… }}` callback. Mirrors InsightsBody (app/insights/
// page.tsx) and DashboardBody. ReportLayout currently calls children(...)
// unconditionally so the in-callback hooks were safe today, but the moment a
// conditional/early-return appears above them (ReportShell's `!isReady`
// placeholder/toggle re-render path can hit one) React's hook order changes
// across renders and throws #310 ("Rendered fewer/more hooks than the
// previous render"). Hooks at the top of a dedicated child are immune.
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
  fingerprintVisibility: ReturnType<typeof useColumnVisibility>[0]
  setFingerprintVisibility: ReturnType<typeof useColumnVisibility>[1]
  onFingerprintVisChange: ReturnType<typeof useColumnVisibility>[2]
  topIpVisibility: ReturnType<typeof useColumnVisibility>[0]
  setTopIpVisibility: ReturnType<typeof useColumnVisibility>[1]
  onTopIpVisChange: ReturnType<typeof useColumnVisibility>[2]
  botVisibility: ReturnType<typeof useColumnVisibility>[0]
  setBotVisibility: ReturnType<typeof useColumnVisibility>[1]
  onBotVisChange: ReturnType<typeof useColumnVisibility>[2]
  botEfficiencyVisibility: ReturnType<typeof useColumnVisibility>[0]
  setBotEfficiencyVisibility: ReturnType<typeof useColumnVisibility>[1]
  onBotEfficiencyVisChange: ReturnType<typeof useColumnVisibility>[2]
  ngwafBotVisibility: ReturnType<typeof useColumnVisibility>[0]
  setNgwafBotVisibility: ReturnType<typeof useColumnVisibility>[1]
  onNgwafBotVisChange: ReturnType<typeof useColumnVisibility>[2]
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
  fingerprintVisibility,
  setFingerprintVisibility,
  onFingerprintVisChange,
  topIpVisibility,
  setTopIpVisibility,
  onTopIpVisChange,
  botVisibility,
  setBotVisibility,
  onBotVisChange,
  botEfficiencyVisibility,
  setBotEfficiencyVisibility,
  onBotEfficiencyVisChange,
  ngwafBotVisibility,
  setNgwafBotVisibility,
  onNgwafBotVisChange,
}: SecurityBodyProps) {
  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)

  const [activeTab, setActiveTab] = React.useState('waf-activity')

  // Single combined query for all three section groups (see SECURITY_SECTIONS).
  // One POST → one DuckDB scan, shared loading/error.
  //
  // SSR-seed key contract (lib/ssr/security.ts + app/security/page.tsx): in token
  // mode keys on the SERVER-REPRODUCIBLE (rangeKey, anchor) pair instead of the
  // client-now()-anchored start/end, so the seed byte-matches and paints from
  // cache. On a cold load rangeKey is "24h". A custom absolute range instead keys
  // on "abs:<start>|<end>" and sends the explicit bounds, so it scans exactly
  // what the chart x-axis (startTime/endTime) displays.
  const securityQuery = useAnalyticsReportQuery<SecurityData | undefined>({
    namespace: ['security', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: [bucketSeconds],
    queryFn: async ({ rangeBody, signal }) => {
      const { data, error } = await client.POST('/api/security/aggregates', {
        signal,
        body: {
          // Token mode → {range_token, anchor}; custom mode → {start_time,
          // end_time} (backend uses these when range_token is absent).
          filters: filterPayload,
          bucket_seconds: bucketSeconds,
          sections: SECURITY_SECTIONS,
          ...rangeBody,
        },
      })
      if (error) {
        throw new Error(extractApiError(error))
      }
      return data
    },
  })

  return (
    <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
      <TabsList className="grid grid-cols-3 max-w-[600px]">
        <TabsTrigger value="waf-activity">WAF Activity</TabsTrigger>
        <TabsTrigger value="threat-intel">Threat Intelligence</TabsTrigger>
        <TabsTrigger value="proxy-watchdog">VPN & Proxy Watchdog</TabsTrigger>
      </TabsList>

      <TabsContent value="waf-activity" className="space-y-6">
        <BotsSection
          data={securityQuery.data}
          isLoading={securityQuery.isLoading}
          isFetching={securityQuery.isFetching}
          error={securityQuery.error ?? null}
          intervalButtons={intervalButtons}
          bucketSeconds={bucketSeconds}
          timezone={timezone}
          commonTimeLayout={commonTimeLayout}
          getFieldLabel={getFieldLabel}
          ngwafBotVisibility={ngwafBotVisibility}
          setNgwafBotVisibility={setNgwafBotVisibility}
          onNgwafBotVisChange={onNgwafBotVisChange}
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
          data={securityQuery.data}
          isLoading={securityQuery.isLoading}
          isFetching={securityQuery.isFetching}
          error={securityQuery.error ?? null}
          getFieldLabel={getFieldLabel}
          topIpVisibility={topIpVisibility}
          setTopIpVisibility={setTopIpVisibility}
          onTopIpVisChange={onTopIpVisChange}
        />
        <NetworkSection
          data={securityQuery.data}
          isLoading={securityQuery.isLoading}
          isFetching={securityQuery.isFetching}
          error={securityQuery.error ?? null}
          timezone={timezone}
          commonTimeLayout={commonTimeLayout}
        />
      </TabsContent>

      <TabsContent value="threat-intel">
        <ThreatIntelPanel
          serviceId={activeServiceId}
          startTime={startTime}
          endTime={endTime}
        />
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

  // Time-range wire inputs (lib/range-wire.ts; resolved in SecurityBody where
  // startTime/endTime are available). A quick-preset pill / the cold-load default
  // → a server-reproducible token ("24h"), matching the hard-clamped chart x-axis
  // AND keeping the SSR seed key byte-matched. A custom absolute range
  // (relativeRange null + isAutoRange false) → the explicit start/end bounds, so
  // it scans exactly what it displays.
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [botEfficiencyVisibility, setBotEfficiencyVisibility, onBotEfficiencyVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange] = useColumnVisibility()

  return (
    <ReportLayout
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
      icon={Shield}
    >
      {({ startTime, endTime, activeServiceId, filterPayload, bucketSeconds, intervalButtons }) => (
        <SecurityBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          bucketSeconds={bucketSeconds}
          intervalButtons={intervalButtons}
          timezone={timezone}
          getFieldLabel={getFieldLabel}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
          fingerprintVisibility={fingerprintVisibility}
          setFingerprintVisibility={setFingerprintVisibility}
          onFingerprintVisChange={onFingerprintVisChange}
          topIpVisibility={topIpVisibility}
          setTopIpVisibility={setTopIpVisibility}
          onTopIpVisChange={onTopIpVisChange}
          botVisibility={botVisibility}
          setBotVisibility={setBotVisibility}
          onBotVisChange={onBotVisChange}
          botEfficiencyVisibility={botEfficiencyVisibility}
          setBotEfficiencyVisibility={setBotEfficiencyVisibility}
          onBotEfficiencyVisChange={onBotEfficiencyVisChange}
          ngwafBotVisibility={ngwafBotVisibility}
          setNgwafBotVisibility={setNgwafBotVisibility}
          onNgwafBotVisChange={onNgwafBotVisChange}
        />
      )}
    </ReportLayout>
  )
}
