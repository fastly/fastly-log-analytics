'use client'

import React from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { CHART_COLORS, useTimeseriesToTraces, formatCount, formatCountMaybe } from '@/lib/utils'
import { useServiceQuery } from '@/hooks/useDashboardCards'
import { AnalyticsCard, ChartEmptyState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown, FilterValueCell } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { client, extractApiError } from '@/lib/api'
import { Activity, Cpu, Globe, HelpCircle, Network, Repeat, Scale, Shield, ShieldAlert } from 'lucide-react'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import type { PlotlyLayout, PlotlyTrace } from '@/types/api'
import type { components } from '@/types/api.generated'
import { SECURITY_INFO, type SecurityData } from './BotsSection'

type TopIpHeader = components['schemas']['TopIpHeaderRow']
type ThreatIntelItem = components['schemas']['ThreatIntelItem']

const TOP_IP_COLUMN_IDS = ['ip', 'max_header']

export function ThreatIntelPanel({
  serviceId,
  startTime,
  endTime,
}: {
  serviceId: string | null
  startTime: string | null
  endTime: string | null
}) {
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)

  const { data, isLoading, error } = useServiceQuery<ThreatIntelItem[]>(
    ['security', 'threat-intel', serviceId, startTime, endTime],
    async ({ signal }) => {
      if (!serviceId) return []
      const { data: res, error: reqErr } = await client.GET('/api/security/threat-intel', {
        signal,
        params: { query: { start_time: startTime ?? undefined, end_time: endTime ?? undefined } },
      })
      if (reqErr) throw new Error(extractApiError(reqErr))
      return res?.data || []
    },
    { enabled: !!serviceId },
  )

  const columns = React.useMemo<ColumnDef<ThreatIntelItem>[]>(
    () => [
      {
        accessorKey: 'tls_fingerprint',
        header: 'TLS Fingerprint (JA3/JA4)',
        cell: ({ row }) => {
          const val = row.original.tls_fingerprint
          return (
            <div className="font-mono text-xs font-semibold select-all w-full max-w-[320px] truncate">
              <FilterValueCell filters={[{ column: val.includes('_') ? 'ja4' : 'ja3', value: val }]} display={val} />
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
        cell: ({ row }) =>
          row.original.matched_waf_rules_count > 0 ? (
            <Badge
              variant="outline"
              className="font-mono bg-destructive/10 text-destructive border-destructive/20 hover:bg-destructive/15"
            >
              {formatCount(row.original.matched_waf_rules_count)} triggers
            </Badge>
          ) : (
            <span className="text-xs text-muted-foreground font-medium">None</span>
          ),
      },
      {
        accessorKey: 'bot_percentage',
        header: 'Bot Percentage',
        cell: ({ row }) =>
          row.original.bot_percentage > 0 ? (
            <div className="flex items-center gap-1.5">
              <Cpu className="h-3.5 w-3.5 text-orange-500" />
              <span className="font-mono font-bold text-xs text-orange-500">
                {row.original.bot_percentage.toFixed(1)}%
              </span>
            </div>
          ) : (
            <span className="text-xs text-muted-foreground font-medium">0.0%</span>
          ),
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
    ],
    [],
  )

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
          <span className="text-sm font-medium text-muted-foreground animate-pulse">
            Aggregating real-time threat telemetry...
          </span>
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
          <p className="text-sm text-muted-foreground max-w-md">
            {error.message || 'An unexpected error occurred while analyzing telemetry.'}
          </p>
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
                  onClick={() => setIsHelpOpen(true)}
                  title="Learn more about Threat Intelligence"
                >
                  <HelpCircle className="h-4 w-4" />
                </Button>
              </div>
              <p className="text-xs text-muted-foreground font-normal">
                Correlated TLS fingerprinting (JA3/JA4) with active WAF signals, proxy routes, and automated client
                behavior.
              </p>
            </div>
          }
        />
      </Card>

      <HelpDialog
        open={isHelpOpen}
        onOpenChange={setIsHelpOpen}
        title="Client TLS Fingerprints & Threat Intelligence Guide"
        icon={<Shield className="h-5 w-5 text-primary" />}
        size="xl"
      >
        <div className="space-y-4 text-sm text-muted-foreground">
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">How This Data Works</h4>
            <p className="leading-relaxed">
              This panel correlates client{' '}
              <strong className="font-semibold text-foreground">TLS Fingerprints (JA3/JA4)</strong> with active{' '}
              <strong className="font-semibold text-foreground">WAF rule triggers</strong>,{' '}
              <strong className="font-semibold text-foreground">automated bot percentage</strong>, and{' '}
              <strong className="font-semibold text-foreground">network anonymity</strong> attributes. Rather than
              relying on easily spoofable HTTP User-Agents, TLS fingerprinting analyzes the unique cryptographic
              handshake parameters (SSL version, cipher suites, extension details) sent by the client&apos;s socket
              library.
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
                  Indicates the connection originates from a commercial VPN provider, public or private proxy, Tor Exit
                  Node, or hosting data center.
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

export type HeaderAnomaliesProps = {
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
  const { isFieldActive } = useActiveLogFields()
  const headerSizeData = React.useMemo((): PlotlyTrace[] => {
    const req_size_dist = data?.req_size_dist
    if (!req_size_dist?.length) return []
    return [
      {
        x: req_size_dist.map((d) => d.bucket ?? d.label ?? null),
        y: req_size_dist.map((d) => d.count),
        type: 'bar',
        marker: { color: '#ec4899' },
      },
    ]
  }, [data])

  const topIpHeaderColumns: ColumnDef<TopIpHeader>[] = [
    {
      accessorKey: 'ip',
      header: 'IP Address',
      cell: ({ row }) => (
        <FilterValueCell filters={[{ column: 'client_ip', value: row.original.ip ?? '' }]} className="font-mono text-xs" />
      ),
    },
    {
      accessorKey: 'max_header',
      header: 'Max Header (Bytes)',
      cell: ({ row }) => formatCountMaybe(row.original.max_header) ?? '—',
    },
  ]

  const cardErr = error as AnalyticsCardError | null

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
      <AnalyticsCard
        title="Request Header Size Distribution"
        icon={<Scale className="h-4 w-4" />}
        isLoading={isLoading}
        isFetching={isFetching}
        loadingMessage="Loading request header size distribution..."
        error={cardErr}
        className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
        contentClassName="p-2"
        helpTitle={SECURITY_INFO.req_size.title}
        helpContent={SECURITY_INFO.req_size.body}
      >
        {headerSizeData.length === 0 && !isLoading ? (
          <ChartEmptyState
            requires={
              isFieldActive('req_header_bytes')
                ? undefined
                : 'Request Identity (Group A) fields to be enabled in Fastly logging.'
            }
          />
        ) : (
          <PlotlyChart data={headerSizeData} layout={{ yaxis: { title: 'Count' } }} height="100%" />
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Oversized Request Headers (by IP)"
        icon={<Shield className="h-4 w-4" />}
        headerAction={
          <ColumnVisibilityDropdown
            columns={TOP_IP_COLUMN_IDS.map((id) => ({ id, label: getFieldLabel(id) }))}
            visibility={topIpVisibility}
            onChange={onTopIpVisChange}
          />
        }
        isLoading={isLoading}
        isFetching={isFetching}
        loadingMessage="Loading oversized request headers..."
        error={cardErr}
        className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
        contentClassName="p-0"
        helpTitle={SECURITY_INFO.top_ips_header.title}
        helpContent={SECURITY_INFO.top_ips_header.body}
      >
        <DataTable
          columns={topIpHeaderColumns}
          data={data?.top_ips_header || []}
          emptyMessage={
            isLoading
              ? ''
              : isFieldActive('req_header_bytes')
                ? 'No data in this time range yet.'
                : 'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.'
          }
          hideToolbar
          columnVisibility={topIpVisibility}
          onColumnVisibilityChange={setTopIpVisibility}
        />
      </AnalyticsCard>
    </div>
  )
}

export type NetworkSectionProps = {
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
  const { isFieldActive } = useActiveLogFields()
  const ipv6Data = useTimeseriesToTraces(
    data?.ipv6_adoption,
    [{ key: 'pct', name: 'IPv6 %', color: '#8b5cf6', fill: 'tozeroy' }],
    timezone,
  )

  const proxyData = React.useMemo((): PlotlyTrace[] => {
    const proxy_dist = data?.proxy_dist
    if (!proxy_dist?.length) return []
    return [
      {
        values: proxy_dist.map((d) => d.count),
        labels: proxy_dist.map((d) => d.type ?? null),
        type: 'pie',
        hole: 0.4,
        marker: {
          colors: [CHART_COLORS.info, CHART_COLORS.success, CHART_COLORS.warning, CHART_COLORS.danger, '#8b5cf6'],
        },
      },
    ]
  }, [data])

  const connReuseData = React.useMemo((): PlotlyTrace[] => {
    const conn_reuse_dist = data?.conn_reuse_dist
    if (!conn_reuse_dist?.length) return []
    return [
      {
        x: conn_reuse_dist.map((d) => d.bucket ?? d.label ?? null),
        y: conn_reuse_dist.map((d) => d.count),
        type: 'bar',
        marker: { color: '#06b6d4' },
      },
    ]
  }, [data])

  const cardErr = error as AnalyticsCardError | null

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="IPv6 Adoption over Time"
          icon={<Globe className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading IPv6 adoption data..."
          error={cardErr}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.ipv6.title}
          helpContent={SECURITY_INFO.ipv6.body}
        >
          {ipv6Data.length === 0 && !isLoading ? (
            <ChartEmptyState
              requires={isFieldActive('is_ipv6') ? undefined : 'Infrastructure (Group C) fields to be enabled in Fastly logging.'}
            />
          ) : (
            <PlotlyChart
              data={ipv6Data}
              layout={{ ...commonTimeLayout, yaxis: { title: 'IPv6 %', range: [0, 100] } }}
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
          error={cardErr}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.proxy.title}
          helpContent={SECURITY_INFO.proxy.body}
        >
          {proxyData.length === 0 && !isLoading ? (
            <ChartEmptyState
              requires={
                isFieldActive('p_type')
                  ? undefined
                  : 'Security: Proxy & Anonymization (Group I) fields to be enabled in Fastly logging.'
              }
            />
          ) : (
            <PlotlyChart data={proxyData} height="100%" />
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
          error={cardErr}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.conn_reuse.title}
          helpContent={SECURITY_INFO.conn_reuse.body}
        >
          {connReuseData.length === 0 && !isLoading ? (
            <ChartEmptyState
              requires={
                isFieldActive('conn_requests')
                  ? undefined
                  : 'Infrastructure (Group C) fields to be enabled in Fastly logging.'
              }
            />
          ) : (
            <PlotlyChart data={connReuseData} layout={{ yaxis: { title: 'Count' } }} height="100%" />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}
