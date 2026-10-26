'use client'

import React, { useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import dynamic from 'next/dynamic'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useTimezone, useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields, useFieldLabel } from '@/hooks/useBootstrap'
import {
  useTimeLayout, CHART_COLORS, useTimeseriesToTraces, formatDate,
  formatBytes, formatCount, formatCountMaybe, downloadBlob,
} from '@/lib/utils'
import { useAnalyticsReportQuery, useReportRangeAnchor, useServiceQuery } from '@/hooks/useDashboardCards'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, ChartEmptyState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown, FilterValueCell } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { client, adminFetch, extractApiError } from '@/lib/api'
import {
  Activity, AlertTriangle, Bot, CheckCircle2, Clock, Cpu, Download, ExternalLink,
  Fingerprint, Globe, HelpCircle, Info, MapIcon, Network, Repeat, Scale, Server, Shield, ShieldAlert,
} from 'lucide-react'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { HelpDialog, StatCard } from '@/components/ui/confirm-dialog'
import type { ImpossibleDistanceData } from '@/components/Insights/InsightCard'
import type { PlotlyLayout, PlotlyTrace } from '@/types/api'
import type { components } from '@/types/api.generated'

const ImpossibleDistanceModal = dynamic(
  () => import('@/components/Insights/InsightDetailModal').then((m) => m.ImpossibleDistanceModal),
  { ssr: false },
)

type SecurityData = components['schemas']['SecurityAggregatesResponse']
type TopIpHeader = components['schemas']['TopIpHeaderRow']
type ThreatIntelItem = components['schemas']['ThreatIntelItem']
type WellknownBot = components['schemas']['WellknownBotRow']
type BotEfficiency = components['schemas']['BotEfficiencyRow']
type NgwafVerifiedBot = components['schemas']['NgwafVerifiedBotRow']
type TlsFingerprint = components['schemas']['TlsFingerprintRow']
type ActiveClientItem = components['schemas']['ActiveClientItem']

const SECURITY_TABS = ['waf-activity', 'threat-intel', 'proxy-watchdog'] as const
const securityTabParser = parseAsStringLiteral(SECURITY_TABS).withDefault('waf-activity')

const FINGERPRINT_COLUMN_IDS = ['fingerprint', 'ip_count', 'request_count']
const TOP_IP_COLUMN_IDS = ['ip', 'max_header']
const BOT_COLUMN_IDS = ['name', 'category', 'request_count', 'verified_count', 'impersonator_count', 'unverified_count', 'pending_count']
const NGWAF_BOT_COLUMN_IDS = ['bot_name', 'category', 'request_count']
const BOT_EFFICIENCY_COLUMN_IDS = [
  'bot_name', 'category', 'request_count', 'resp_bytes', 'bandwidth_pct',
  'referrals', 'requests_per_referral', 'bytes_per_referral',
]

function InfoBullets({ intro, items }: { intro: string; items: { icon: React.ReactNode; label: string; text: React.ReactNode }[] }) {
  return (
    <div className="space-y-4 text-sm text-muted-foreground">
      <p>{intro}</p>
      <ul className="space-y-3 list-none pl-0">
        {items.map((item) => (
          <li key={item.label} className="flex gap-3">
            {item.icon}
            <span><strong>{item.label}:</strong> {item.text}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

const SECURITY_INFO = {
  wellknown_bots: {
    title: 'Well-Known Bots',
    body: (
      <InfoBullets
        intro="Detects bot traffic based on a continuously updated database of well-known User-Agent patterns and verifies them using FCrDNS and CIDR matches."
        items={[
          { icon: <CheckCircle2 className="h-5 w-5 shrink-0 text-green-500" />, label: 'Verified', text: "The IP address matched the official CIDR block or passed Forward-Confirmed reverse DNS for the bot's known domains." },
          { icon: <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />, label: 'Spoofed', text: 'The request claimed to be this bot in the User-Agent, but the IP failed verification. Highly likely to be malicious scrapers or scammers.' },
          { icon: <HelpCircle className="h-5 w-5 shrink-0 text-muted-foreground" />, label: 'Unverified', text: 'The bot source does not provide official IPs or domains for verification.' },
          { icon: <Clock className="h-5 w-5 shrink-0 text-yellow-500" />, label: 'Pending', text: 'The reverse DNS lookup is still pending in the background. Check back soon.' },
        ]}
      />
    ),
  },
  fingerprints: {
    title: 'Top TLS Fingerprints',
    body: (
      <InfoBullets
        intro="Identifies groups of traffic sharing the exact same TLS negotiation parameters (cipher suites, extensions), often indicating the same underlying software or script."
        items={[
          { icon: <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Botnet Detection', text: 'IP addresses change frequently, but the custom scripting tools attackers use rarely change their TLS handshakes. A single fingerprint spread across thousands of IPs usually indicates a coordinated botnet.' },
        ]}
      />
    ),
  },
  req_size: {
    title: 'Request Header Size Distribution',
    body: (
      <InfoBullets
        intro="A histogram showing the distribution of HTTP request header sizes across your traffic."
        items={[
          { icon: <Scale className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Anomaly Detection', text: 'Normal web requests have header sizes between 500 bytes and 2KB. Spikes in the 8KB+ range can indicate buffer overflow attempts or overly aggressive cookie stuffing.' },
        ]}
      />
    ),
  },
  top_ips_header: {
    title: 'Oversized Request Headers',
    body: (
      <InfoBullets
        intro="Highlights specific IP addresses sending the largest request headers."
        items={[
          { icon: <Shield className="h-5 w-5 shrink-0 text-yellow-500" />, label: 'Investigation', text: 'Helps isolate the source of oversized requests seen in the distribution chart. These IPs may be malfunctioning clients or malicious actors probing for vulnerabilities.' },
        ]}
      />
    ),
  },
  ipv6: {
    title: 'IPv6 Adoption over Time',
    body: (
      <InfoBullets
        intro="Tracks the percentage of requests connecting to Fastly via IPv6 vs IPv4."
        items={[
          { icon: <Globe className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Infrastructure Readiness', text: 'Sudden drops in IPv6 traffic might indicate an ISP routing failure or a DNS configuration issue dropping AAAA records.' },
        ]}
      />
    ),
  },
  proxy: {
    title: 'Proxy/Anonymizer Breakdown',
    body: (
      <InfoBullets
        intro="Categorizes traffic by the underlying network type, using Fastly's geolocation intelligence."
        items={[
          { icon: <Network className="h-5 w-5 shrink-0 text-yellow-500" />, label: 'Traffic Quality', text: "A high percentage of traffic from 'hosting' or 'tor' categories is a strong indicator of non-human traffic, scraping, or evasion attempts." },
        ]}
      />
    ),
  },
  conn_reuse: {
    title: 'Connection Reuse',
    body: (
      <InfoBullets
        intro="Shows how many HTTP requests are made over a single TCP connection."
        items={[
          { icon: <Repeat className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Efficiency', text: "High reuse (10+ requests per connection) is highly efficient and typical for browsers loading a webpage. A spike in '1' (no reuse) means connections are constantly being torn down, which is typical of basic scraping tools or misconfigured API clients." },
        ]}
      />
    ),
  },
  ngwaf_bots: {
    title: 'Verified Bots (NGWAF)',
    body: (
      <InfoBullets
        intro="Shows named bots identified by Fastly NGWAF. By definition, all traffic matching these signals has been verified by Fastly's Signal Sciences engine."
        items={[
          { icon: <Bot className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Bot Name', text: 'The verified bot name extracted from the NGWAF VERIFIED-BOT signal (e.g. "OpenAI SearchBot").' },
        ]}
      />
    ),
  },
  bot_efficiency: {
    title: 'Bot & Crawler Value vs. Cost',
    body: (
      <InfoBullets
        intro="Compares each bot or crawler's resource footprint (request count and egress bandwidth) against the organic human referral traffic it returns to your site."
        items={[
          { icon: <Scale className="h-5 w-5 shrink-0 text-blue-500" />, label: 'Cost-to-Value Ratio', text: <>Search and AI answer crawlers that drive human referrals should show a low <em>Requests / Referral</em> and <em>Bandwidth / Referral</em> ratio.</> },
          { icon: <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />, label: 'Zero-Referral Scrapers', text: <>AI training crawlers, SEO scrapers, and headless bots often consume significant bandwidth while returning <code>0</code> referred visitors — prime candidates for edge throttling or blocking.</> },
        ]}
      />
    ),
  },
}

const LOW_COVERAGE_THRESHOLD = 0.01
const COVERAGE_HINT_MESSAGE: Record<string, string> = {
  tls_ciphers_sha: 'TLS fingerprints are only captured when the request lands at the true edge PoP (not shielded). Sparse coverage typically means most traffic is shielded.',
}

function FingerprintCoverageHint({ coverage, field }: { coverage: number | undefined; field: string }) {
  if (coverage === undefined || coverage === null || coverage >= LOW_COVERAGE_THRESHOLD) return null
  const pct = coverage === 0 ? '0%' : coverage < 0.001 ? '<0.1%' : `${(coverage * 100).toFixed(2)}%`
  const msg = COVERAGE_HINT_MESSAGE[field] || 'This field is populated for only a small fraction of requests in the current window.'
  return (
    <div className="flex items-start gap-2 px-3 py-2 text-[11px] text-muted-foreground bg-muted/30 border-b">
      <Info className="h-3.5 w-3.5 mt-0.5 shrink-0" />
      <span><span className="font-medium text-foreground">Low coverage ({pct}).</span> {msg}</span>
    </div>
  )
}

type BotsSectionProps = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
  intervalButtons: React.ReactNode
  bucketSeconds: number
  timezone: string
  commonTimeLayout: PlotlyLayout
  getFieldLabel: (id: string) => string
  ngwafBotVisibility: VisibilityState
  setNgwafBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onNgwafBotVisChange: (id: string, vis: boolean) => void
  botVisibility: VisibilityState
  setBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onBotVisChange: (id: string, vis: boolean) => void
  botEfficiencyVisibility?: VisibilityState
  setBotEfficiencyVisibility?: Dispatch<SetStateAction<VisibilityState>>
  onBotEfficiencyVisChange?: (id: string, vis: boolean) => void
  fingerprintVisibility: VisibilityState
  setFingerprintVisibility: Dispatch<SetStateAction<VisibilityState>>
  onFingerprintVisChange: (id: string, vis: boolean) => void
}

export function BotsSection({
  data, isLoading, isFetching, error, intervalButtons, bucketSeconds, timezone, commonTimeLayout, getFieldLabel,
  ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange,
  botVisibility, setBotVisibility, onBotVisChange,
  botEfficiencyVisibility = {}, setBotEfficiencyVisibility, onBotEfficiencyVisChange,
  fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange,
}: BotsSectionProps) {
  const { isFieldActive } = useActiveLogFields()
  const labeledColumns = React.useCallback((ids: readonly string[]) => ids.map((id) => ({ id, label: getFieldLabel(id) })), [getFieldLabel])

  const ngwafBotsData = React.useMemo(() => {
    const timeseries = data?.ngwaf_verified_bots_ts
    if (!timeseries?.length) return []
    const byName: Record<string, { x: string[]; y: number[] }> = {}
    const allTimesSet = new Set<string>()
    timeseries.forEach((d) => allTimesSet.add(formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss')))
    const allTimes = Array.from(allTimesSet).sort()
    const names = Array.from(new Set(timeseries.map((d) => d.bot_name ?? '')))
    for (const n of names) byName[n] = { x: [...allTimes], y: new Array(allTimes.length).fill(0) }
    for (const d of timeseries) {
      const t = formatDate(d.time, timezone, 'yyyy-MM-dd HH:mm:ss')
      const idx = allTimes.indexOf(t)
      if (idx !== -1) byName[d.bot_name ?? ''].y[idx] = d.count
    }
    return Object.entries(byName).map(([name, d], i): PlotlyTrace => ({
      x: d.x,
      y: d.y.map((val) => (val === 0 ? null : val)),
      type: 'bar',
      name,
      width: bucketSeconds * 1000,
      hovertemplate: `${name}: %{y:,}<extra></extra>`,
      marker: { color: `hsl(${(i * 47 + 210) % 360}, 70%, 50%)` },
    }))
  }, [data, timezone, bucketSeconds])

  const botEfficiencyChartData = React.useMemo((): PlotlyTrace[] => {
    const rows = data?.bot_efficiency
    if (!rows?.length) return []
    const top = rows.slice(0, 10)
    return [
      { x: top.map((r) => r.bot_name), y: top.map((r) => r.request_count), name: 'Bot Requests', type: 'bar', marker: { color: '#f97316' } },
      { x: top.map((r) => r.bot_name), y: top.map((r) => r.referrals), name: 'Referred Traffic', type: 'bar', marker: { color: '#22c55e' } },
    ]
  }, [data?.bot_efficiency])

  const botColumns: ColumnDef<WellknownBot>[] = [
    {
      accessorKey: 'name',
      header: 'Bot',
      cell: ({ row }) => <FilterValueCell filters={[{ column: '_wellknown_bot_id', value: row.original.id }]} display={row.original.name} className="font-medium" containerClassName="max-w-[200px]" />,
    },
    { accessorKey: 'category', header: 'Category', cell: ({ row }) => <span className="capitalize">{row.original.category.replace(/-/g, ' ')}</span> },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCount(row.original.request_count) },
    {
      accessorKey: 'verified_count',
      header: 'Verified',
      cell: ({ row }) => row.original.verified_count > 0 ? <span className="flex items-center gap-1 text-green-500"><CheckCircle2 className="h-3 w-3" />{formatCount(row.original.verified_count)}</span> : '—',
    },
    {
      accessorKey: 'impersonator_count',
      header: 'Spoofed',
      cell: ({ row }) => row.original.impersonator_count > 0 ? <span className="flex items-center gap-1 text-red-500"><AlertTriangle className="h-3 w-3" />{formatCount(row.original.impersonator_count)}</span> : '—',
    },
    {
      accessorKey: 'unverified_count',
      header: 'Unverified',
      cell: ({ row }) => row.original.unverified_count > 0 ? <span className="flex items-center gap-1 text-muted-foreground" title="Unverifiable (no IPs/domains in source)"><HelpCircle className="h-3 w-3" />{formatCount(row.original.unverified_count)}</span> : '—',
    },
    {
      accessorKey: 'pending_count',
      header: 'Pending',
      cell: ({ row }) => row.original.pending_count > 0 ? <span className="flex items-center gap-1 text-yellow-500" title="Pending rDNS lookup"><Clock className="h-3 w-3" />{formatCount(row.original.pending_count)}</span> : '—',
    },
  ]

  const ngwafBotColumns: ColumnDef<NgwafVerifiedBot>[] = [
    {
      accessorKey: 'bot_name',
      header: 'Bot Name',
      cell: ({ row }) => <FilterValueCell filters={[{ column: '_ngwaf_bot_name', value: row.original.bot_name || '' }]} display={row.original.bot_name ?? undefined} className="font-medium" containerClassName="max-w-[200px]" />,
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: ({ row }) => row.original.category ? <span className="capitalize">{row.original.category.replace(/-/g, ' ')}</span> : <span className="text-muted-foreground">—</span>,
    },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCount(row.original.request_count) },
  ]

  const fingerprintColumns: ColumnDef<TlsFingerprint>[] = [
    {
      accessorKey: 'fingerprint',
      header: 'Cipher Fingerprint (SHA)',
      cell: ({ row }) => <FilterValueCell filters={[{ column: 'tls_client_hello', value: row.original.fingerprint ?? '' }]} className="font-mono text-[10px]" containerClassName="max-w-[200px]" />,
    },
    { accessorKey: 'ip_count', header: 'Unique IPs', cell: ({ row }) => formatCountMaybe(row.original.ip_count) ?? '—' },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCountMaybe(row.original.request_count) ?? '—' },
  ]

  const botEfficiencyColumns: ColumnDef<BotEfficiency>[] = [
    { accessorKey: 'bot_name', header: 'Bot / Crawler', cell: ({ row }) => <span className="font-medium">{row.original.bot_name}</span> },
    { accessorKey: 'category', header: 'Category', cell: ({ row }) => <span className="capitalize text-muted-foreground">{row.original.category.replace(/-/g, ' ')}</span> },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCount(row.original.request_count) },
    { accessorKey: 'resp_bytes', header: 'Bandwidth', cell: ({ row }) => formatBytes(row.original.resp_bytes) },
    { accessorKey: 'bandwidth_pct', header: 'Site BW Share', cell: ({ row }) => `${row.original.bandwidth_pct.toFixed(1)}%` },
    {
      accessorKey: 'referrals',
      header: 'Referred Traffic',
      cell: ({ row }) =>
        row.original.referrals === 0 ? (
          <span className="flex items-center gap-1 text-red-500 font-medium" title="Zero human visitors referred">
            <AlertTriangle className="h-3 w-3" />0
          </span>
        ) : (
          <span className="text-green-600 dark:text-green-400 font-medium">{formatCount(row.original.referrals)}</span>
        ),
    },
    {
      accessorKey: 'requests_per_referral',
      header: 'Requests / Referral',
      cell: ({ row }) => {
        const val = row.original.requests_per_referral
        const isHighCost = row.original.referrals === 0 || val >= 50
        return <span className={isHighCost ? 'text-red-500 font-semibold' : undefined}>{formatCount(val, { maximumFractionDigits: 1 })}</span>
      },
    },
    {
      accessorKey: 'bytes_per_referral',
      header: 'Bandwidth / Referral',
      cell: ({ row }) => <span className={row.original.referrals === 0 ? 'text-red-500 font-semibold' : undefined}>{formatBytes(row.original.bytes_per_referral)}</span>,
    },
  ]

  return (
    <>
      {data?.ngwaf_configured !== false && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <AnalyticsCard
            title="Verified Bots (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={intervalButtons}
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF verified bots..."
            error={error as AnalyticsCardError | null}
            className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-2"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            {ngwafBotsData.length === 0 && !isLoading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
                {data?.ngwaf_configured
                  ? 'No NGWAF bot detections in this time window.'
                  : <>Set <code className="mx-1 text-xs bg-muted px-1 rounded">ngwaf_workspace_id</code> in service settings to enable NGWAF bot tracking.</>}
              </div>
            ) : (
              <PlotlyChart
                data={ngwafBotsData}
                layout={{ ...commonTimeLayout, barmode: 'stack', showlegend: true, yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' } }}
                height="100%"
              />
            )}
          </AnalyticsCard>

          <AnalyticsCard
            title="Verified Bot Names (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={<ColumnVisibilityDropdown columns={labeledColumns(NGWAF_BOT_COLUMN_IDS)} visibility={ngwafBotVisibility} onChange={onNgwafBotVisChange} />}
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF bot names..."
            error={error as AnalyticsCardError | null}
            className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-0"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            <DataTable
              columns={ngwafBotColumns}
              data={data?.ngwaf_verified_bots || []}
              emptyMessage={isLoading ? '' : (data?.ngwaf_configured ? 'No NGWAF bot detections in this time window.' : 'Set ngwaf_workspace_id in service settings to enable NGWAF bot tracking.')}
              hideToolbar
              columnVisibility={ngwafBotVisibility}
              onColumnVisibilityChange={setNgwafBotVisibility}
            />
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Well-Known Bots"
          icon={<Bot className="h-4 w-4" />}
          headerAction={<ColumnVisibilityDropdown columns={labeledColumns(BOT_COLUMN_IDS)} visibility={botVisibility} onChange={onBotVisChange} />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading well-known bots..."
          error={error as AnalyticsCardError | null}
          className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.wellknown_bots.title}
          helpContent={SECURITY_INFO.wellknown_bots.body}
        >
          <DataTable
            columns={botColumns}
            data={data?.wellknown_bots || []}
            emptyMessage={isLoading ? '' : 'No known bots detected. Ensure bot sources are cached in Admin settings.'}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botVisibility}
            onColumnVisibilityChange={setBotVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Top TLS Fingerprints"
          icon={<Fingerprint className="h-4 w-4" />}
          headerAction={<ColumnVisibilityDropdown columns={labeledColumns(FINGERPRINT_COLUMN_IDS)} visibility={fingerprintVisibility} onChange={onFingerprintVisChange} />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading TLS fingerprints..."
          error={error as AnalyticsCardError | null}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.fingerprints.title}
          helpContent={SECURITY_INFO.fingerprints.body}
        >
          <FingerprintCoverageHint coverage={data?.fingerprint_coverage?.tls_ciphers_sha} field="tls_ciphers_sha" />
          <DataTable
            columns={fingerprintColumns}
            data={data?.tls_fingerprints || []}
            emptyMessage={isLoading ? '' : ((isFieldActive('ja3') || isFieldActive('ja4')) ? 'No TLS fingerprints in this time range.' : 'Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.')}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={fingerprintVisibility}
            onColumnVisibilityChange={setFingerprintVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Bot & Crawler Cost vs. Referrals"
          icon={<Scale className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot cost vs. referrals chart..."
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          {botEfficiencyChartData.length === 0 && !isLoading ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
              {isFieldActive('ua') ? 'No bot or crawler traffic detected in this time window.' : 'Requires User-Agent (ua) field to be enabled in Fastly logging.'}
            </div>
          ) : (
            <PlotlyChart
              data={botEfficiencyChartData}
              layout={{ barmode: 'group', showlegend: true, yaxis: { title: 'Count', separatethousands: true, exponentformat: 'none' } }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)"
          icon={<Scale className="h-4 w-4" />}
          headerAction={
            onBotEfficiencyVisChange ? (
              <ColumnVisibilityDropdown columns={labeledColumns(BOT_EFFICIENCY_COLUMN_IDS)} visibility={botEfficiencyVisibility} onChange={onBotEfficiencyVisChange} />
            ) : undefined
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot bandwidth & referral efficiency..."
          error={error as AnalyticsCardError | null}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          <DataTable
            columns={botEfficiencyColumns}
            data={data?.bot_efficiency || []}
            emptyMessage={isLoading ? '' : (isFieldActive('ua') ? 'No bot or crawler traffic detected in this time window.' : 'Requires User-Agent (ua) field to be enabled in Fastly logging.')}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botEfficiencyVisibility}
            onColumnVisibilityChange={setBotEfficiencyVisibility}
          />
        </AnalyticsCard>
      </div>
    </>
  )
}

interface ProxyWatchdogSectionProps {
  data: SecurityData | undefined
  isLoading: boolean
  error?: Error | null
  startTime?: string | null
  endTime?: string | null
  activeServiceId?: string | null
}

const WATCHDOG_COLUMNS_LIST = [
  { id: 'ip', label: 'IP Address' },
  { id: 'asn_name', label: 'ASN / Network' },
  { id: 'risk_level', label: 'Risk Level' },
  { id: 'distance_km', label: 'Distance (km)' },
  { id: 'rtt_min_ms', label: 'RTT Min' },
  { id: 'tcp_rtt_ms', label: 'TCP RTT' },
]

const WATCHDOG_HELP_MODAL_CONTENT: Record<'traffic' | 'isp' | 'export' | 'clients', { title: string; body: React.ReactNode }> = {
  traffic: {
    title: 'Traffic Quality Assessment',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>Traffic Quality categorizes client connections based on their end-to-end network profiles, highlighting direct connection reliability vs active tunneling.</p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li><strong className="text-foreground">Active Tunnel / Proxy:</strong> Triggered when a client&apos;s minimum edge latency is a tiny fraction (&lt;10%) of their actual TCP round-trip time.</li>
            <li><strong className="text-foreground">WiFi / Mobile:</strong> Typical of standard commercial networks, showing standard Jitter, RTT ratios between 10% and 70%, and moderate packet overhead.</li>
            <li><strong className="text-foreground">Direct Connection:</strong> Highly stable connections with low jitter where physical edge RTT strongly correlates with the application handshake.</li>
          </ul>
        </div>
      </div>
    ),
  },
  isp: {
    title: 'Suspicious ISPs & Network Providers',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>Identifies the Autonomous System Numbers (ASNs) and network operators hosting the highest volume of proxy/VPN connections.</p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
          <p>Groups all requests matching active proxy signatures by their client network ASN and resolves their authoritative names (e.g. DigitalOcean, OVH, M247, NordVPN).</p>
        </div>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Security Tip:</h4>
          <p>High request volume from consumer ISPs (Comcast, Verizon, Spectrum) is normal VPN use. High request volumes from datacenter hosting providers (AWS, DigitalOcean, Hetzner, Linode) almost always represents automated crawlers, headless bots, or commercial proxy nodes.</p>
        </div>
      </div>
    ),
  },
  export: {
    title: 'VPN/Proxy Export Center Guide',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>The Export Center allows security administrators to download compiled list files containing active, high-risk proxy and VPN IP addresses observed on this service.</p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is used:</h4>
          <p>These exported lists can be directly uploaded to Fastly Edge Dictionaries, Access Control Lists (ACLs), or WAF custom rules to instantly block or rate-limit these suspect clients at the absolute network edge, saving server resources and database overhead.</p>
        </div>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Export Risk Thresholds:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li><strong className="text-foreground">High Risk:</strong> Extremely strict criteria, matching clients exhibiting active speed-of-light coordinate spoofing violations or hosting datacenter exit nodes.</li>
            <li><strong className="text-foreground">Medium & Above:</strong> Recommended default. Includes verified high-risk clients along with commercial VPN addresses showing aggressive request volumes.</li>
            <li><strong className="text-foreground">Include Low Risk:</strong> Comprehensive list. Includes residential proxies, consumer VPNs with standard browsing behavior, and occasional high-jitter mobile networks.</li>
          </ul>
        </div>
      </div>
    ),
  },
  clients: {
    title: 'Active Suspicious Clients Auditing',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>An interactive high-fidelity table displaying real-time client sessions flagged with active VPN or Proxy signatures.</p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Key Column Definitions:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li><strong className="text-foreground">IP Address:</strong> The client&apos;s IPv4/IPv6 edge address. Fully interactive (copy, filter, or open in the central query dashboard).</li>
            <li><strong className="text-foreground">Risk Level:</strong> Behavioral scoring threshold (High, Medium, Low) derived from distance anomalies, RTT ratios, and provider threat profiles.</li>
            <li><strong className="text-foreground">Distance:</strong> The physical distance (in km) between the edge landing POP and the client&apos;s registered geographic location.</li>
            <li><strong className="text-foreground">RTT Min:</strong> The minimum measured packet round-trip time between the client and the edge server.</li>
            <li><strong className="text-foreground">TCP RTT:</strong> The transport layer&apos;s established TCP round-trip. Anomalous differences between RTT Min and TCP RTT reveal tunnel boundaries.</li>
          </ul>
        </div>
      </div>
    ),
  },
}

export function ProxyWatchdogSection({
  data, isLoading, error = null, startTime, endTime, activeServiceId,
}: ProxyWatchdogSectionProps) {
  const [threshold, setThreshold] = useState<'High' | 'Medium' | 'Low'>('High')
  const [format, setFormat] = useState<'fastly-acl' | 'plain'>('fastly-acl')
  const [isExporting, setIsExporting] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)
  const [activeHelpKey, setActiveHelpKey] = useState<'traffic' | 'isp' | 'export' | 'clients' | null>(null)
  const [columnVisibility, setColumnVisibility, onColumnVisibilityChange] = useColumnVisibility()

  const watchdogColumns = React.useMemo((): ColumnDef<ActiveClientItem>[] => [
    {
      accessorKey: 'ip',
      id: 'ip',
      header: 'IP Address',
      cell: ({ row }) => {
        const ip = row.original.ip
        return (
          <div className="flex items-center gap-1.5 min-w-0 w-full">
            <FilterValueCell filters={[{ column: 'ip', value: ip }]} display={ip} className="font-mono font-medium" />
            <a
              href={`/dashboard?${new URLSearchParams({
                ...(activeServiceId ? { service: activeServiceId } : {}),
                ...(startTime ? { start_time: startTime } : {}),
                ...(endTime ? { end_time: endTime } : {}),
                filter_ip: ip,
              }).toString()}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-muted-foreground hover:text-rose-500 transition-colors p-0.5 rounded hover:bg-muted/80 shrink-0"
              title="View IP in Dashboard"
            >
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        )
      },
    },
    {
      accessorKey: 'asn_name',
      id: 'asn_name',
      header: 'ASN / Network',
      cell: ({ row }) => row.original.asn_name
        ? <span className="truncate block max-w-[200px]" title={row.original.asn_name}>{row.original.asn_name}</span>
        : <span className="text-muted-foreground">—</span>,
    },
    {
      accessorKey: 'risk_level',
      id: 'risk_level',
      header: 'Risk Level',
      cell: ({ row }) => {
        const val = row.original.risk_level
        const isHigh = val === 'High'
        const isMedium = val === 'Medium'
        return (
          <Badge
            variant={isHigh ? 'destructive' : isMedium ? 'default' : 'outline'}
            className={isMedium ? 'bg-amber-500/10 text-amber-500 border-amber-500/20 hover:bg-amber-500/10' : ''}
          >
            {val}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'distance_km',
      id: 'distance_km',
      header: 'Distance (km)',
      cell: ({ row: tableRow }) => {
        const row = tableRow.original
        return (
          <div className="flex flex-col items-start w-full">
            <span className="font-mono">
              {row.distance_km !== null && row.distance_km !== undefined ? formatCount(row.distance_km, { maximumFractionDigits: 1 }) : '—'}
            </span>
            {row.impossible_distance && (
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  setSelectedMapItem({
                    label: row.ip,
                    client_lat: row.client_lat ?? 0,
                    client_lon: row.client_lon ?? 0,
                    pop_lat: row.pop_lat ?? 0,
                    pop_lon: row.pop_lon ?? 0,
                    pop: row.pop ?? 'Unknown',
                    tcp_rtt: (row.tcp_rtt_ms ?? 0) * 1000,
                    distance_km: row.distance_km ?? 0,
                    max_km: (row.rtt_min_ms ?? 0) * 100.0 + 150.0,
                    country: row.country || undefined,
                    city: row.city || undefined,
                  })
                }}
                className="text-[10px] text-rose-500 font-semibold flex items-center gap-1 mt-1 px-1.5 py-0.5 rounded bg-rose-500/10 hover:bg-rose-500/20 active:scale-95 transition-all border border-rose-500/20 shrink-0"
                title="Visualize geographical physical anomaly on map"
              >
                <MapIcon className="h-2.5 w-2.5" /> Impossible
              </button>
            )}
          </div>
        )
      },
    },
    {
      accessorKey: 'rtt_min_ms',
      id: 'rtt_min_ms',
      header: 'RTT Min',
      cell: ({ row }) => <div className="font-mono">{row.original.rtt_min_ms !== null && row.original.rtt_min_ms !== undefined ? `${row.original.rtt_min_ms.toFixed(1)} ms` : '—'}</div>,
    },
    {
      accessorKey: 'tcp_rtt_ms',
      id: 'tcp_rtt_ms',
      header: 'TCP RTT',
      cell: ({ row }) => <div className="font-mono">{row.original.tcp_rtt_ms !== null && row.original.tcp_rtt_ms !== undefined ? `${row.original.tcp_rtt_ms.toFixed(1)} ms` : '—'}</div>,
    },
  ], [activeServiceId, startTime, endTime])

  const handleExport = async () => {
    setIsExporting(true)
    try {
      const params = new URLSearchParams({ start_time: startTime || '', end_time: endTime || '', threshold, format })
      const response = await adminFetch(`/api/security/proxies/export?${params.toString()}`, {
        headers: activeServiceId ? { 'x-fastly-service-id': activeServiceId } : {},
      })
      if (!response.ok) throw new Error(`Export failed: ${response.statusText}`)
      downloadBlob(
        await response.blob(),
        format === 'plain' ? `proxies_${threshold.toLowerCase()}.txt` : `proxies_acl_${threshold.toLowerCase()}.csv`,
      )
    } catch (err) {
      console.error('Export error:', err)
    } finally {
      setIsExporting(false)
    }
  }

  const buildFilteredDashboardUrl = React.useCallback(
    (filtersPayload: Record<string, { mode: string; values: string[] }>) => {
      const params = new URLSearchParams()
      if (activeServiceId) params.set('service', activeServiceId)
      if (startTime) params.set('start_time', startTime)
      if (endTime) params.set('end_time', endTime)
      params.set('filters', JSON.stringify(filtersPayload))
      return `/dashboard?${params.toString()}`
    },
    [activeServiceId, startTime, endTime],
  )

  const tunnelDashboardUrl = React.useMemo(
    () => buildFilteredDashboardUrl({ _tunnel_requests: { mode: 'include', values: ['true'] } }),
    [buildFilteredDashboardUrl],
  )

  const getIspDashboardUrl = React.useCallback(
    (asn: number | string | null | undefined) => (asn ? buildFilteredDashboardUrl({ asn: { mode: 'include', values: [String(asn)] } }) : '#'),
    [buildFilteredDashboardUrl],
  )

  if (error) {
    return (
      <Card className="border-destructive/50 bg-destructive/5 p-6 text-center">
        <AlertTriangle className="mx-auto h-10 w-10 text-destructive mb-3" />
        <CardTitle className="text-lg font-semibold text-destructive mb-1">Failed to Load Proxy Watchdog Data</CardTitle>
        <CardDescription className="text-sm text-muted-foreground">{error.message || 'An error occurred.'}</CardDescription>
      </Card>
    )
  }

  const activeProxiesCount = data?.active_proxies_count ?? 0
  const tunnelRequestsCount = data?.tunnel_requests_count ?? 0
  const distanceMismatchesCount = data?.distance_mismatches_count ?? 0
  const trafficQuality = data?.traffic_quality ?? []
  const suspiciousIsps = data?.suspicious_isps ?? []
  const activeClients = data?.active_clients ?? []

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 [contain-intrinsic-size:120px] [content-visibility:auto]">
        <StatCard
          title="Active VPN & Proxy Users"
          value={isLoading ? <Skeleton className="h-8 w-16" /> : formatCount(activeProxiesCount)}
          sub="Unique suspicious IP addresses detected"
          icon={Globe}
          iconClassName="text-blue-500"
          loading={isLoading}
          className="min-h-[120px] [contain-intrinsic-size:120px] [content-visibility:auto]"
          helpTitle="Active VPN & Proxy Users Detection"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>This metric represents the number of distinct client IP addresses actively routing requests through VPN servers, anonymizing proxies, or Tor exit nodes during the selected time window.</p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
                <p>Every client IP is evaluated in real-time against two high-accuracy behavioral indicators:</p>
                <ul className="list-disc pl-5 mt-1 space-y-1">
                  <li><strong className="text-foreground">Impossible Distance Mismatch:</strong> Compares measured edge network latency (min RTT) with physical geographical speed-of-light limits.</li>
                  <li><strong className="text-foreground">Latency-RTT Ratio Violation:</strong> Measures the discrepancy between the SSL/application handshake latency and raw TCP round-trip.</li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Security Impact:</h4>
                <p>While commercial VPN usage is standard for privacy-conscious human users, sudden surges in this cohort often indicate automated scrapers or credential stuffing attempts trying to evade IP-based blocking.</p>
              </div>
            </div>
          }
        />
        <StatCard
          title="Tunnel Requests"
          value={
            isLoading ? (
              <Skeleton className="h-8 w-16" />
            ) : (
              <a
                href={tunnelDashboardUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-2xl font-bold text-violet-600 dark:text-violet-400 hover:underline inline-flex items-center gap-1 group/link"
                title="Open Tunnel Requests on Dashboard"
              >
                {formatCount(tunnelRequestsCount)}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover/link:opacity-100 transition-opacity text-muted-foreground" />
              </a>
            )
          }
          sub="HTTP requests routed via anonymizers"
          icon={Activity}
          iconClassName="text-violet-500"
          loading={isLoading}
          className="min-h-[120px] [contain-intrinsic-size:120px] [content-visibility:auto]"
          helpTitle="Tunnel Requests Volume"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>This tracks the absolute HTTP request volume routed through VPNs, anonymizing networks, or hosting providers during the selected time period.</p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
                <p>Sums every request whose client IP is flagged as originating from a commercial VPN provider, a known datacenter host, or is otherwise exhibiting active behavioral tunneling fingerprints on our network interfaces.</p>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Why this matters:</h4>
                <p>Request volume is a key indicator of scraper velocity. An individual botnet using rotated proxy IPs might keep its &quot;per-IP&quot; rates extremely low to fly under the radar, but the aggregate &quot;Tunnel Requests&quot; volume will spike dramatically during active scanning.</p>
              </div>
            </div>
          }
        />
        <StatCard
          title="Impossible Distance Mismatches"
          value={isLoading ? <Skeleton className="h-8 w-16" /> : formatCount(distanceMismatchesCount)}
          sub="IP-to-latency geographical anomalies"
          icon={AlertTriangle}
          iconClassName="text-amber-500"
          loading={isLoading}
          className="min-h-[120px] [contain-intrinsic-size:120px] [content-visibility:auto]"
          helpTitle="Impossible Distance Mismatch Auditing"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>Geographical anomalies represent requests where the physical distance between the client&apos;s reported GeoIP location and the landing edge POP is physically incompatible with the recorded network latency.</p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">The Physics Formula:</h4>
                <p>We calculate the Haversine distance between the client GeoIP coordinate and the POP. Light in fiber travels at ~200,000 km/s (~100km per millisecond round-trip). We enforce:</p>
                <div className="font-mono text-xs bg-muted p-2 rounded text-center my-2 border">distance_km &gt; (rtt_min_ms * 100.0 + 150.0)</div>
                <p>If a request landing in Tokyo with a 3ms RTT claims to be from Paris (distance &gt; 9,000 km), it breaks the speed of light. This proves the request&apos;s origin was masked.</p>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Security Recommendation:</h4>
                <p>High volumes of distance mismatches are characteristic of commercial geo-bypass tooling, coordinate-spoofing browser extensions, or automated crawlers originating from overseas datacenters routed through localized tunnels.</p>
              </div>
            </div>
          }
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1 space-y-6">
          <Card className="min-h-[280px] [contain-intrinsic-size:320px] [content-visibility:auto]">
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Activity className="h-4 w-4 text-violet-500" /> Traffic Quality Breakdown
                </CardTitle>
                <CardDescription className="text-xs">Assessed request reliability index</CardDescription>
              </div>
              <Button variant="ghost" size="icon" className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0" onClick={() => setActiveHelpKey('traffic')} title="About Traffic Quality Breakdown">
                <HelpCircle className="h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent className="space-y-4">
              {isLoading ? (
                <div className="space-y-3"><Skeleton className="h-4 w-full" /><Skeleton className="h-4 w-full" /></div>
              ) : trafficQuality.length === 0 ? (
                <div className="text-xs text-muted-foreground py-4 text-center">No traffic quality metrics recorded.</div>
              ) : (
                trafficQuality.map((item) => {
                  const label = String(item.label || 'Unknown')
                  const val = Number(item.value || 0)
                  return (
                    <div key={label} className="space-y-1">
                      <div className="flex justify-between text-xs font-medium">
                        <span>{label}</span>
                        <span className="font-mono text-muted-foreground">{val}%</span>
                      </div>
                      <div className="h-2 w-full bg-muted rounded-full overflow-hidden">
                        <div className={`h-full rounded-full ${label === 'High' ? 'bg-emerald-500' : label === 'Medium' ? 'bg-amber-500' : 'bg-rose-500'}`} style={{ width: `${val}%` }} />
                      </div>
                    </div>
                  )
                })
              )}
            </CardContent>
          </Card>

          <Card className="min-h-[280px] [contain-intrinsic-size:320px] [content-visibility:auto]">
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Server className="h-4 w-4 text-blue-500" /> Suspicious ISPs & Providers
                </CardTitle>
                <CardDescription className="text-xs">Leading network providers hosting proxy connections</CardDescription>
              </div>
              <Button variant="ghost" size="icon" className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0" onClick={() => setActiveHelpKey('isp')} title="About Suspicious ISPs">
                <HelpCircle className="h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <div className="space-y-2"><Skeleton className="h-6 w-full" /><Skeleton className="h-6 w-full" /><Skeleton className="h-6 w-full" /></div>
              ) : suspiciousIsps.length === 0 ? (
                <div className="text-xs text-muted-foreground py-4 text-center">No suspicious ISPs detected.</div>
              ) : (
                <div className="divide-y divide-border text-xs">
                  {suspiciousIsps.map((item) => {
                    const ispName = String(item.isp || 'Unknown Provider')
                    const hasAsn = item.asn !== undefined && item.asn !== null
                    const dashboardUrl = hasAsn ? getIspDashboardUrl(item.asn as number) : '#'
                    return (
                      <div key={ispName} className="flex justify-between py-2 items-center group/item">
                        {hasAsn ? (
                          <a href={dashboardUrl} className="font-medium truncate max-w-[180px] hover:text-violet-600 dark:hover:text-violet-400 hover:underline inline-flex items-center gap-1 group/link" title={`Filter Dashboard by ${ispName}`}>
                            {ispName}
                            <ExternalLink className="h-3 w-3 opacity-0 group-hover/item:opacity-100 transition-opacity text-muted-foreground" />
                          </a>
                        ) : (
                          <span className="font-medium truncate max-w-[180px]">{ispName}</span>
                        )}
                        {hasAsn ? (
                          <a href={dashboardUrl} title={`Filter Dashboard by ${ispName}`}>
                            <Badge variant="outline" className="font-mono hover:bg-violet-50 dark:hover:bg-violet-950/30 hover:border-violet-300 dark:hover:border-violet-700 hover:text-violet-600 dark:hover:text-violet-400 cursor-pointer transition-colors">
                              {formatCount(Number(item.count || 0))} reqs
                            </Badge>
                          </a>
                        ) : (
                          <Badge variant="outline" className="font-mono">{formatCount(Number(item.count || 0))} reqs</Badge>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="lg:col-span-2 space-y-6">
          <Card className="border-primary/20 bg-primary/5 [contain-intrinsic-size:140px] [content-visibility:auto]">
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Download className="h-4 w-4 text-primary" /> VPN/Proxy Export Center
                </CardTitle>
                <CardDescription className="text-xs">Export verified anonymizer IP lists for ingestion into Fastly WAF Edge ACLs.</CardDescription>
              </div>
              <Button variant="ghost" size="icon" className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0" onClick={() => setActiveHelpKey('export')} title="About Export Center">
                <HelpCircle className="h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-4 items-end">
                <div className="space-y-1.5 shrink-0">
                  <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1">
                    <ShieldAlert className="h-3 w-3" /> Risk Threshold
                  </span>
                  <Select value={threshold} onValueChange={(val) => { if (val) setThreshold(val) }}>
                    <SelectTrigger className="w-36 h-9 text-xs bg-background"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="High">High Risk Only</SelectItem>
                      <SelectItem value="Medium">Medium & Above</SelectItem>
                      <SelectItem value="Low">Include Low Risk</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-1.5 shrink-0">
                  <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1">
                    <Download className="h-3 w-3" /> Target Format
                  </span>
                  <Select value={format} onValueChange={(val) => { if (val) setFormat(val) }}>
                    <SelectTrigger className="w-48 h-9 text-xs bg-background"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="fastly-acl">Fastly ACL (CSV format)</SelectItem>
                      <SelectItem value="plain">Plain text (IP address list)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <Button size="sm" className="h-9 font-medium text-xs ml-auto" onClick={handleExport} disabled={isLoading || isExporting}>
                  <Download className={`h-3.5 w-3.5 mr-1.5 ${isExporting ? 'animate-spin' : ''}`} />
                  {isExporting ? 'Generating Export...' : 'Generate Blocklist'}
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="min-h-[400px] [contain-intrinsic-size:600px] [content-visibility:auto]">
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  Active Suspicious Clients
                  <Button variant="ghost" size="icon" className="h-5 w-5 text-muted-foreground hover:text-foreground shrink-0 rounded-full" onClick={() => setActiveHelpKey('clients')} title="About Active Suspicious Clients">
                    <HelpCircle className="h-3.5 w-3.5" />
                  </Button>
                </CardTitle>
                <CardDescription className="text-xs">Current active sessions matching behavioral proxy profiles</CardDescription>
              </div>
              <ColumnVisibilityDropdown columns={WATCHDOG_COLUMNS_LIST} visibility={columnVisibility} onChange={onColumnVisibilityChange} />
            </CardHeader>
            <CardContent className="p-0">
              <DataTable
                columns={watchdogColumns}
                data={activeClients}
                isLoading={isLoading}
                emptyMessage="No active proxy client sessions observed in this window."
                hideToolbar
                columnVisibility={columnVisibility}
                onColumnVisibilityChange={setColumnVisibility}
              />
            </CardContent>
          </Card>
        </div>
      </div>

      <ImpossibleDistanceModal isOpen={!!selectedMapItem} onOpenChange={(open) => !open && setSelectedMapItem(null)} data={selectedMapItem} />

      <HelpDialog
        open={activeHelpKey !== null}
        onOpenChange={(open) => !open && setActiveHelpKey(null)}
        title={activeHelpKey ? WATCHDOG_HELP_MODAL_CONTENT[activeHelpKey].title : ''}
        size="lg"
      >
        {activeHelpKey ? WATCHDOG_HELP_MODAL_CONTENT[activeHelpKey].body : null}
      </HelpDialog>
    </div>
  )
}

function ThreatIntelPanel({ serviceId, startTime, endTime }: { serviceId: string | null; startTime: string | null; endTime: string | null }) {
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
      cell: ({ row }) => <span className="font-semibold font-mono text-xs text-foreground/80 tabular-nums">{formatCount(row.original.requests)}</span>,
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
          <Badge variant="outline" className="font-semibold text-[10px] uppercase tracking-wider px-2 py-0.5 bg-red-500/10 text-red-500 border-red-500/20 hover:bg-red-500/15 border shadow-none">
            VPN / Proxy / Tor
          </Badge>
        ) : (
          <Badge variant="secondary" className="font-semibold text-[10px] uppercase tracking-wider px-2 py-0.5 bg-emerald-500/10 text-emerald-500 border-emerald-500/20 hover:bg-emerald-500/15 border shadow-none">
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
          <p className="text-sm text-muted-foreground max-w-md">{error.message || 'An unexpected error occurred while analyzing telemetry.'}</p>
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
                <span className="text-base font-bold tracking-tight">Client TLS Fingerprints & Threat Intelligence</span>
                <Button variant="ghost" size="icon" className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0 rounded-full" onClick={() => setIsHelpOpen(true)} title="Learn more about Threat Intelligence">
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

      <HelpDialog open={isHelpOpen} onOpenChange={setIsHelpOpen} title="Client TLS Fingerprints & Threat Intelligence Guide" icon={<Shield className="h-5 w-5 text-primary" />} size="xl">
        <div className="space-y-4 text-sm text-muted-foreground">
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">How This Data Works</h4>
            <p className="leading-relaxed">
              This panel correlates client <strong className="font-semibold text-foreground">TLS Fingerprints (JA3/JA4)</strong> with active <strong className="font-semibold text-foreground">WAF rule triggers</strong>, <strong className="font-semibold text-foreground">automated bot percentage</strong>, and <strong className="font-semibold text-foreground">network anonymity</strong> attributes. Rather than relying on easily spoofable HTTP User-Agents, TLS fingerprinting analyzes the unique cryptographic handshake parameters (SSL version, cipher suites, extension details) sent by the client&apos;s socket library.
            </p>
          </div>
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">Anonymity Categories</h4>
            <ul className="space-y-3 mt-2">
              <li className="flex gap-2.5 items-start">
                <Badge variant="outline" className="font-semibold text-[9px] uppercase tracking-wider px-1.5 py-0.5 bg-red-500/10 text-red-500 border-red-500/20 shrink-0 select-none shadow-none mt-0.5">
                  VPN / Proxy / Tor
                </Badge>
                <div>
                  <strong className="text-foreground font-medium block text-xs">Anonymized / Proxy Traffic</strong>
                  Indicates the connection originates from a commercial VPN provider, public or private proxy, Tor Exit Node, or hosting data center.
                </div>
              </li>
              <li className="flex gap-2.5 items-start">
                <Badge variant="secondary" className="font-semibold text-[9px] uppercase tracking-wider px-1.5 py-0.5 bg-emerald-500/10 text-emerald-500 border-emerald-500/20 shrink-0 select-none shadow-none mt-0.5">
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
  data, isLoading, isFetching, error, getFieldLabel, topIpVisibility, setTopIpVisibility, onTopIpVisChange,
}: HeaderAnomaliesProps) {
  const { isFieldActive } = useActiveLogFields()
  const headerSizeData = React.useMemo((): PlotlyTrace[] => {
    const req_size_dist = data?.req_size_dist
    if (!req_size_dist?.length) return []
    return [{
      x: req_size_dist.map((d) => d.bucket ?? d.label ?? null),
      y: req_size_dist.map((d) => d.count),
      type: 'bar',
      marker: { color: '#ec4899' },
    }]
  }, [data])

  const topIpHeaderColumns: ColumnDef<TopIpHeader>[] = [
    {
      accessorKey: 'ip',
      header: 'IP Address',
      cell: ({ row }) => <FilterValueCell filters={[{ column: 'client_ip', value: row.original.ip ?? '' }]} className="font-mono text-xs" />,
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
          <ChartEmptyState requires={isFieldActive('req_header_bytes') ? undefined : 'Request Identity (Group A) fields to be enabled in Fastly logging.'} />
        ) : (
          <PlotlyChart data={headerSizeData} layout={{ yaxis: { title: 'Count' } }} height="100%" />
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Oversized Request Headers (by IP)"
        icon={<Shield className="h-4 w-4" />}
        headerAction={
          <ColumnVisibilityDropdown columns={TOP_IP_COLUMN_IDS.map((id) => ({ id, label: getFieldLabel(id) }))} visibility={topIpVisibility} onChange={onTopIpVisChange} />
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
          emptyMessage={isLoading ? '' : (isFieldActive('req_header_bytes') ? 'No data in this time range yet.' : 'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.')}
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

export function NetworkSection({ data, isLoading, isFetching, error, timezone, commonTimeLayout }: NetworkSectionProps) {
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
      marker: { colors: [CHART_COLORS.info, CHART_COLORS.success, CHART_COLORS.warning, CHART_COLORS.danger, '#8b5cf6'] },
    }]
  }, [data])

  const connReuseData = React.useMemo((): PlotlyTrace[] => {
    const conn_reuse_dist = data?.conn_reuse_dist
    if (!conn_reuse_dist?.length) return []
    return [{
      x: conn_reuse_dist.map((d) => d.bucket ?? d.label ?? null),
      y: conn_reuse_dist.map((d) => d.count),
      type: 'bar',
      marker: { color: '#06b6d4' },
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
            <ChartEmptyState requires={isFieldActive('is_ipv6') ? undefined : 'Infrastructure (Group C) fields to be enabled in Fastly logging.'} />
          ) : (
            <PlotlyChart data={ipv6Data} layout={{ ...commonTimeLayout, yaxis: { title: 'IPv6 %', range: [0, 100] } }} height="100%" />
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
            <ChartEmptyState requires={isFieldActive('p_type') ? undefined : 'Security: Proxy & Anonymization (Group I) fields to be enabled in Fastly logging.'} />
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
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.conn_reuse.title}
          helpContent={SECURITY_INFO.conn_reuse.body}
        >
          {connReuseData.length === 0 && !isLoading ? (
            <ChartEmptyState requires={isFieldActive('conn_requests') ? undefined : 'Infrastructure (Group C) fields to be enabled in Fastly logging.'} />
          ) : (
            <PlotlyChart data={connReuseData} layout={{ yaxis: { title: 'Count' } }} height="100%" />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}

// MUST stay in lockstep with SECURITY_SSR_SECTIONS in lib/ssr/security.ts
type SecuritySections = NonNullable<components['schemas']['SecurityAggregatesRequest']['sections']>
const SECURITY_SECTIONS: SecuritySections = [
  'ngwaf_verified_bots', 'ngwaf_verified_bots_ts', 'wellknown_bots', 'bot_efficiency',
  'tls_fingerprints', 'fingerprint_coverage', 'req_size_dist', 'top_ips_header',
  'ipv6_adoption', 'proxy_dist', 'conn_reuse_dist', 'proxies',
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
  startTime, endTime, activeServiceId, filterPayload, bucketSeconds,
  intervalButtons, timezone, getFieldLabel, relativeRange, isAutoRange, anchor,
}: SecurityBodyProps) {
  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)
  const [activeTab, setActiveTab] = useQueryState('tab', securityTabParser)
  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [botEfficiencyVisibility, setBotEfficiencyVisibility, onBotEfficiencyVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange] = useColumnVisibility()

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

  return (
    <Tabs value={activeTab} onValueChange={(v) => void setActiveTab(v as (typeof SECURITY_TABS)[number])} className="space-y-6">
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
        />
      )}
    </ReportLayout>
  )
}
