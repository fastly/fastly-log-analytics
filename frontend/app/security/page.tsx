'use client'

import { useTimeseriesToTraces } from '@/hooks/useTimeseriesToTraces'
import React from 'react'
import { usePageContext } from '@/hooks/usePageContext'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable } from '@/components/DataTable'
import { formatDate } from '@/lib/date'
import { Shield, Fingerprint, Scale, Globe, Network, Repeat, Bot, CheckCircle2, AlertTriangle, Clock, HelpCircle } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DashboardLinkCell } from '@/components/DashboardLinkCell'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { useFieldLabel } from '@/hooks/useFieldLabel';
import { useTimeLayout } from '@/lib/chart-helpers'
import { ReportLayout } from '@/components/ReportLayout'
import { client } from '@/lib/api'

type NgwafVerifiedBot = {
  bot_name?: string
  category?: string
  request_count?: number
  [key: string]: any
}

const FINGERPRINT_COLUMN_IDS = ['fingerprint', 'ip_count', 'request_count']
const TOP_IP_COLUMN_IDS = ['ip', 'max_header']
const BOT_COLUMN_IDS = ['name', 'category', 'request_count', 'verified_count', 'impersonator_count', 'unverified_count', 'pending_count']
const NGWAF_BOT_COLUMN_IDS = ['bot_name', 'category', 'request_count']

const SECURITY_INFO = {
  wellknown_bots: {
    title: 'Well-Known Bots',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Detects bot traffic based on a continuously updated database of well-known User-Agent patterns and verifies them using FCrDNS and CIDR matches.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <CheckCircle2 className="h-5 w-5 shrink-0 text-green-500" />
            <span><strong>Verified:</strong> The IP address matched the official CIDR block or passed Forward-Confirmed reverse DNS for the bot's known domains.</span>
          </li>
          <li className="flex gap-3">
            <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
            <span><strong>Spoofed:</strong> The request claimed to be this bot in the User-Agent, but the IP failed verification. Highly likely to be malicious scrapers or scammers.</span>
          </li>
          <li className="flex gap-3">
            <HelpCircle className="h-5 w-5 shrink-0 text-muted-foreground" />
            <span><strong>Unverified:</strong> The bot source does not provide official IPs or domains for verification.</span>
          </li>
          <li className="flex gap-3">
            <Clock className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Pending:</strong> The reverse DNS lookup is still pending in the background. Check back soon.</span>
          </li>
        </ul>
      </div>
    )
  },
  fingerprints: {
    title: 'Top TLS Fingerprints',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Identifies groups of traffic sharing the exact same TLS negotiation parameters (cipher suites, extensions), often indicating the same underlying software or script.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Botnet Detection:</strong> IP addresses change frequently, but the custom scripting tools attackers use rarely change their TLS handshakes. A single fingerprint spread across thousands of IPs usually indicates a coordinated botnet.</span>
          </li>
        </ul>
      </div>
    )
  },
  req_size: {
    title: 'Request Header Size Distribution',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>A histogram showing the distribution of HTTP request header sizes across your traffic.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Scale className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Anomaly Detection:</strong> Normal web requests have header sizes between 500 bytes and 2KB. Spikes in the 8KB+ range can indicate buffer overflow attempts or overly aggressive cookie stuffing.</span>
          </li>
        </ul>
      </div>
    )
  },
  top_ips_header: {
    title: 'Oversized Request Headers',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Highlights specific IP addresses sending the largest request headers.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Shield className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Investigation:</strong> Helps isolate the source of oversized requests seen in the distribution chart. These IPs may be malfunctioning clients or malicious actors probing for vulnerabilities.</span>
          </li>
        </ul>
      </div>
    )
  },
  ipv6: {
    title: 'IPv6 Adoption over Time',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Tracks the percentage of requests connecting to Fastly via IPv6 vs IPv4.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Globe className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Infrastructure Readiness:</strong> Sudden drops in IPv6 traffic might indicate an ISP routing failure or a DNS configuration issue dropping AAAA records.</span>
          </li>
        </ul>
      </div>
    )
  },
  proxy: {
    title: 'Proxy/Anonymizer Breakdown',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Categorizes traffic by the underlying network type, using Fastly's geolocation intelligence.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Network className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Traffic Quality:</strong> A high percentage of traffic from 'hosting' or 'tor' categories is a strong indicator of non-human traffic, scraping, or evasion attempts.</span>
          </li>
        </ul>
      </div>
    )
  },
  conn_reuse: {
    title: 'Connection Reuse',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Shows how many HTTP requests are made over a single TCP connection.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Repeat className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Efficiency:</strong> High reuse (10+ requests per connection) is highly efficient and typical for browsers loading a webpage. A spike in '1' (no reuse) means connections are constantly being torn down, which is typical of basic scraping tools or misconfigured API clients.</span>
          </li>
        </ul>
      </div>
    )
  },
  ngwaf_bots: {
    title: 'Verified Bots (NGWAF)',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Shows named bots identified by Fastly NGWAF. By definition, all traffic matching these signals has been verified by Fastly's Signal Sciences engine.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Bot className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Bot Name:</strong> The verified bot name extracted from the NGWAF VERIFIED-BOT signal (e.g. "OpenAI SearchBot").</span>
          </li>
        </ul>
      </div>
    )
  }
}

export default function SecurityPage() {
  const getFieldLabel = useFieldLabel()
  const { startTime, endTime, timezone } = usePageContext()

  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange] = useColumnVisibility()

  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)

  return (
    <ReportLayout
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
      icon={Shield}
      queryKey="security"
      apiCall={async ({ startTime, endTime, filters, bucketSeconds }) => {
        const { data } = await client.POST("/api/security/aggregates", {
          body: {
            start_time: startTime,
            end_time: endTime,
            filters,
            bucket_seconds: bucketSeconds,
          }
        })
        return data
      }}
    >
      {({ data, isLoading, isFetching, intervalButtons, bucketSeconds }) => {
        const ngwafBotsData = React.useMemo(() => {
          const timeseries = (data as any)?.ngwaf_verified_bots_ts
          if (!timeseries?.length) return []
          const byName: Record<string, { x: string[], y: number[] }> = {}

          const allTimesSet = new Set<string>()
          timeseries.forEach((d: any) => {
            allTimesSet.add(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
          })
          const allTimes = Array.from(allTimesSet).sort()

          const names = Array.from(new Set(timeseries.map((d: any) => d.bot_name)))
          for (const n of names) {
            byName[n as string] = { x: [...allTimes], y: new Array(allTimes.length).fill(0) }
          }

          for (const d of timeseries) {
            const t = formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")
            const idx = allTimes.indexOf(t)
            if (idx !== -1) {
              byName[d.bot_name].y[idx] = d.count
            }
          }

          return Object.entries(byName).map(([name, d], i) => ({
            x: d.x,
            y: d.y.map(val => val === 0 ? null : val),
            type: 'bar',
            name,
            width: bucketSeconds * 1000,
            hovertemplate: `${name}: %{y:,}<extra></extra>`,
            marker: { color: `hsl(${(i * 47 + 210) % 360}, 70%, 50%)` }
          }))
        }, [data, timezone, bucketSeconds])

        const ipv6Data = useTimeseriesToTraces((data as any)?.ipv6_adoption, [
          { key: 'pct', name: 'IPv6 %', color: '#8b5cf6', fill: 'tozeroy' }
        ], timezone)

        const proxyData = React.useMemo(() => {
          const proxy_dist = (data as any)?.proxy_dist
          if (!proxy_dist?.length) return []
          return [{
            values: proxy_dist.map((d: any) => d.count),
            labels: proxy_dist.map((d: any) => d.type),
            type: 'pie',
            hole: 0.4,
            marker: { colors: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'] }
          }]
        }, [data])

        const headerSizeData = React.useMemo(() => {
          const req_size_dist = (data as any)?.req_size_dist
          if (!req_size_dist?.length) return []
          return [{
            x: req_size_dist.map((d: any) => d.bucket),
            y: req_size_dist.map((d: any) => d.count),
            type: 'bar',
            marker: { color: '#ec4899' }
          }]
        }, [data])

        const connReuseData = React.useMemo(() => {
          const conn_reuse_dist = (data as any)?.conn_reuse_dist
          if (!conn_reuse_dist?.length) return []
          return [{
            x: conn_reuse_dist.map((d: any) => d.bucket),
            y: conn_reuse_dist.map((d: any) => d.count),
            type: 'bar',
            marker: { color: '#06b6d4' }
          }]
        }, [data])

        const botColumns = [
          {
            accessorKey: 'name',
            header: 'Bot',
            cell: (info: any) => {
              const row = info.row.original
              return (
                <DashboardLinkCell
                  value={row.name}
                  href={`/dashboard?filter__wellknown_bot_id=${encodeURIComponent(row.id)}`}
                  className="font-medium"
                  containerClassName="max-w-[200px]"
                />
              )
            }
          },
          {
            accessorKey: 'category',
            header: 'Category',
            cell: (info: any) => <span className="capitalize">{info.getValue()?.replace(/-/g, ' ')}</span>
          },
          { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
          { 
            accessorKey: 'verified_count', 
            header: 'Verified', 
            cell: (info: any) => info.getValue() > 0 ? (
              <span className="flex items-center gap-1 text-green-500">
                <CheckCircle2 className="h-3 w-3" />{info.getValue().toLocaleString()}
              </span>
            ) : '—'
          },
          { 
            accessorKey: 'impersonator_count', 
            header: 'Spoofed', 
            cell: (info: any) => info.getValue() > 0 ? (
              <span className="flex items-center gap-1 text-red-500">
                <AlertTriangle className="h-3 w-3" />{info.getValue().toLocaleString()}
              </span>
            ) : '—'
          },
          { 
            accessorKey: 'unverified_count', 
            header: 'Unverified', 
            cell: (info: any) => info.getValue() > 0 ? (
              <span className="flex items-center gap-1 text-muted-foreground" title="Unverifiable (no IPs/domains in source)">
                <HelpCircle className="h-3 w-3" />{info.getValue().toLocaleString()}
              </span>
            ) : '—'
          },
          { 
            accessorKey: 'pending_count', 
            header: 'Pending', 
            cell: (info: any) => info.getValue() > 0 ? (
              <span className="flex items-center gap-1 text-yellow-500" title="Pending rDNS lookup">
                <Clock className="h-3 w-3" />{info.getValue().toLocaleString()}
              </span>
            ) : '—'
          }
        ]

        const ngwafBotColumns = [
          {
            accessorKey: 'bot_name',
            header: 'Bot Name',
            cell: (info: any) => {
              const row = info.row.original as NgwafVerifiedBot
              return (
                <DashboardLinkCell
                  value={row.bot_name}
                  href={`/dashboard?filter__ngwaf_bot_name=${encodeURIComponent(row.bot_name || '')}`}
                  className="font-medium"
                  containerClassName="max-w-[200px]"
                />
              )
            }
          },
          {
            accessorKey: 'category',
            header: 'Category',
            cell: (info: any) => info.getValue()
              ? <span className="capitalize">{info.getValue().replace(/-/g, ' ')}</span>
              : <span className="text-muted-foreground">—</span>
          },
          { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
        ]

        const fingerprintColumns = [
          {
            accessorKey: 'fingerprint',
            header: 'Cipher Fingerprint (SHA)',
            cell: (info: any) => (
              <DashboardLinkCell
                value={info.getValue()}
                href={`/dashboard?filter_tls_client_hello=${encodeURIComponent(info.getValue())}`}
                className="font-mono text-[10px]"
                containerClassName="max-w-[200px]"
              />
            )
          },
          { accessorKey: 'ip_count', header: 'Unique IPs', cell: (info: any) => info.getValue().toLocaleString() },
          { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
        ]

        const topIpHeaderColumns = [
          {
            accessorKey: 'ip',
            header: 'IP Address',
            cell: (info: any) => (
              <DashboardLinkCell
                value={info.getValue()}
                href={`/dashboard?filter_client_ip=${encodeURIComponent(info.getValue())}`}
                className="font-mono text-xs"
              />
            )
          },
          { accessorKey: 'max_header', header: 'Max Header (Bytes)', cell: (info: any) => info.getValue().toLocaleString() },
        ]

        return (
          <>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              <AnalyticsCard
                title="Verified Bots (NGWAF)"
                icon={<Bot className="h-4 w-4" />}
                headerAction={intervalButtons}
                isLoading={isLoading}
                isFetching={isFetching}
                className="h-[360px]"
                contentClassName="p-2"
                helpTitle={SECURITY_INFO.ngwaf_bots.title}
                helpContent={SECURITY_INFO.ngwaf_bots.body}
              >
                {ngwafBotsData.length === 0 && !isLoading ? (
                  <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
                    {(data as any)?.ngwaf_configured
                      ? "No NGWAF bot detections in this time window."
                      : <>Set <code className="mx-1 text-xs bg-muted px-1 rounded">ngwaf_workspace_id</code> in service settings to enable NGWAF bot tracking.</>}
                  </div>
                ) : (
                  <PlotlyChart
                    data={ngwafBotsData as any[]}
                    layout={{
                      ...commonTimeLayout,
                      barmode: 'stack',
                      showlegend: true,
                      yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' }
                    }}
                    height="100%"
                  />
                )}
              </AnalyticsCard>

              <AnalyticsCard
                title="Verified Bot Names (NGWAF)"
                icon={<Bot className="h-4 w-4" />}
                headerAction={
                  <ColumnVisibilityDropdown columns={NGWAF_BOT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={ngwafBotVisibility} onChange={onNgwafBotVisChange} />
                }
                isLoading={isLoading}
                isFetching={isFetching}
                className="min-h-[360px]"
                contentClassName="p-0"
                helpTitle={SECURITY_INFO.ngwaf_bots.title}
                helpContent={SECURITY_INFO.ngwaf_bots.body}
              >
                <DataTable
                  columns={ngwafBotColumns}
                  data={(data as any)?.ngwaf_verified_bots || []}
                  emptyMessage={isLoading ? "" : ((data as any)?.ngwaf_configured ? "No NGWAF bot detections in this time window." : "Set ngwaf_workspace_id in service settings to enable NGWAF bot tracking.")}
                  hideToolbar
                  columnVisibility={ngwafBotVisibility}
                  onColumnVisibilityChange={setNgwafBotVisibility}
                />
              </AnalyticsCard>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              <AnalyticsCard
                title="Well-Known Bots"
                icon={<Bot className="h-4 w-4" />}
                headerAction={
                  <ColumnVisibilityDropdown columns={BOT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={botVisibility} onChange={onBotVisChange} />
                }
                isLoading={isLoading}
                isFetching={isFetching}
                className="min-h-[360px]"
                contentClassName="p-0"
                helpTitle={SECURITY_INFO.wellknown_bots.title}
                helpContent={SECURITY_INFO.wellknown_bots.body}
              >
                <DataTable
                  columns={botColumns}
                  data={(data as any)?.wellknown_bots || []}
                  emptyMessage={isLoading ? "" : "No known bots detected. Ensure bot sources are cached in Admin settings."}
                  hideToolbar
                  columnVisibility={botVisibility}
                  onColumnVisibilityChange={setBotVisibility}
                />
              </AnalyticsCard>

              <AnalyticsCard
                title="Top TLS Fingerprints"
                icon={<Fingerprint className="h-4 w-4" />}
                headerAction={
                  <ColumnVisibilityDropdown columns={FINGERPRINT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={fingerprintVisibility} onChange={onFingerprintVisChange} />
                }
                isLoading={isLoading}
                isFetching={isFetching}
                className="min-h-[300px]"
                contentClassName="p-0"
                helpTitle={SECURITY_INFO.fingerprints.title}
                helpContent={SECURITY_INFO.fingerprints.body}
              >
                <DataTable
                  columns={fingerprintColumns}
                  data={(data as any)?.tls_fingerprints || []}
                  emptyMessage={isLoading ? "" : "Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging."}
                  hideToolbar
                  columnVisibility={fingerprintVisibility}
                  onColumnVisibilityChange={setFingerprintVisibility}
                />
              </AnalyticsCard>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
              <AnalyticsCard
                title="Request Header Size Distribution"
                icon={<Scale className="h-4 w-4" />}
                isLoading={isLoading}
                isFetching={isFetching}
                className="h-[360px]"
                contentClassName="p-2"
                helpTitle={SECURITY_INFO.req_size.title}
                helpContent={SECURITY_INFO.req_size.body}
              >
                {headerSizeData.length === 0 && !isLoading ? (
                  <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
                    <span className="text-sm font-medium mb-1">No data available</span>
                    <span className="text-[10px] opacity-70">
                      Requires Request Identity (Group A) fields to be enabled in Fastly logging.
                    </span>
                  </div>
                ) : (
                  <PlotlyChart
                    data={headerSizeData as any[]}
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
                className="min-h-[300px]"
                contentClassName="p-0"
                helpTitle={SECURITY_INFO.top_ips_header.title}
                helpContent={SECURITY_INFO.top_ips_header.body}
              >
                <DataTable
                  columns={topIpHeaderColumns}
                  data={(data as any)?.top_ips_header || []}
                  emptyMessage={isLoading ? "" : "Requires Request Identity (Group A) log fields to be enabled in Fastly logging."}
                  hideToolbar
                  columnVisibility={topIpVisibility}
                  onColumnVisibilityChange={setTopIpVisibility}
                />
              </AnalyticsCard>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
              <AnalyticsCard
                title="IPv6 Adoption over Time"
                icon={<Globe className="h-4 w-4" />}
                isLoading={isLoading}
                isFetching={isFetching}
                className="h-[360px]"
                contentClassName="p-2"
                helpTitle={SECURITY_INFO.ipv6.title}
                helpContent={SECURITY_INFO.ipv6.body}
              >
                {ipv6Data.length === 0 && !isLoading ? (
                  <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
                    <span className="text-sm font-medium mb-1">No data available</span>
                    <span className="text-[10px] opacity-70">
                      Requires Infrastructure (Group C) fields to be enabled in Fastly logging.
                    </span>
                  </div>
                ) : (
                  <PlotlyChart
                    data={ipv6Data as any[]}
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
                className="h-[360px]"
                contentClassName="p-2"
                helpTitle={SECURITY_INFO.proxy.title}
                helpContent={SECURITY_INFO.proxy.body}
              >
                {proxyData.length === 0 && !isLoading ? (
                  <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
                    <span className="text-sm font-medium mb-1">No data available</span>
                    <span className="text-[10px] opacity-70">
                      Requires Security: Proxy & Anonymization (Group I) fields to be enabled in Fastly logging.
                    </span>
                  </div>
                ) : (
                  <PlotlyChart
                    data={proxyData as any[]}
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
                className="h-[360px]"
                contentClassName="p-2"
                helpTitle={SECURITY_INFO.conn_reuse.title}
                helpContent={SECURITY_INFO.conn_reuse.body}
              >
                {connReuseData.length === 0 && !isLoading ? (
                  <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
                    <span className="text-sm font-medium mb-1">No data available</span>
                    <span className="text-[10px] opacity-70">
                      Requires Infrastructure (Group C) fields to be enabled in Fastly logging.
                    </span>
                  </div>
                ) : (
                  <PlotlyChart
                    data={connReuseData as any[]}
                    layout={{ yaxis: { title: 'Count' } }}
                    height="100%"
                  />
                )}
              </AnalyticsCard>
            </div>
          </>
        )
      }}
    </ReportLayout>
  )
}
