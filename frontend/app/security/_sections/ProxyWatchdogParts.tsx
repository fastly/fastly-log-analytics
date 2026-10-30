'use client'

import React from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { formatCount } from '@/lib/utils'
import { FilterValueCell } from '@/components/DataTable'
import {
  Activity,
  AlertTriangle,
  Download,
  ExternalLink,
  Globe,
  HelpCircle,
  MapIcon,
  Server,
  ShieldAlert,
} from 'lucide-react'
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { StatCard } from '@/components/ui/confirm-dialog'
import type { ImpossibleDistanceData } from '@/components/Insights'
import type { components } from '@/types/api.generated'
import type { SecurityData } from './BotsSection'

export type ActiveClientItem = components['schemas']['ActiveClientItem']
export type WatchdogHelpKey = 'traffic' | 'isp' | 'export' | 'clients'

export const WATCHDOG_COLUMNS_LIST = [
  { id: 'ip', label: 'IP Address' },
  { id: 'asn_name', label: 'ASN / Network' },
  { id: 'risk_level', label: 'Risk Level' },
  { id: 'distance_km', label: 'Distance (km)' },
  { id: 'rtt_min_ms', label: 'RTT Min' },
  { id: 'tcp_rtt_ms', label: 'TCP RTT' },
]

export function useWatchdogColumns({
  activeServiceId,
  startTime,
  endTime,
  onSelectMapItem,
}: {
  activeServiceId?: string | null
  startTime?: string | null
  endTime?: string | null
  onSelectMapItem: (item: ImpossibleDistanceData) => void
}): ColumnDef<ActiveClientItem>[] {
  return React.useMemo(
    (): ColumnDef<ActiveClientItem>[] => [
      {
        accessorKey: 'ip',
        id: 'ip',
        header: 'IP Address',
        cell: ({ row }) => {
          const ip = row.original.ip
          const qs = new URLSearchParams({
            ...(activeServiceId ? { service: activeServiceId } : {}),
            ...(startTime ? { start_time: startTime } : {}),
            ...(endTime ? { end_time: endTime } : {}),
            filter_ip: ip,
          }).toString()
          return (
            <div className="flex items-center gap-1.5 min-w-0 w-full">
              <FilterValueCell filters={[{ column: 'ip', value: ip }]} display={ip} className="font-mono font-medium" />
              <a
                href={`/dashboard?${qs}`}
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
        cell: ({ row }) =>
          row.original.asn_name ? (
            <span className="truncate block max-w-[200px]" title={row.original.asn_name}>
              {row.original.asn_name}
            </span>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
      },
      {
        accessorKey: 'risk_level',
        id: 'risk_level',
        header: 'Risk Level',
        cell: ({ row }) => {
          const val = row.original.risk_level
          return (
            <Badge
              variant={val === 'High' ? 'destructive' : val === 'Medium' ? 'default' : 'outline'}
              className={
                val === 'Medium' ? 'bg-amber-500/10 text-amber-500 border-amber-500/20 hover:bg-amber-500/10' : ''
              }
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
        cell: ({ row: { original: row } }) => (
          <div className="flex flex-col items-start w-full">
            <span className="font-mono">
              {row.distance_km !== null && row.distance_km !== undefined
                ? formatCount(row.distance_km, { maximumFractionDigits: 1 })
                : '—'}
            </span>
            {row.impossible_distance && (
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  onSelectMapItem({
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
        ),
      },
      {
        accessorKey: 'rtt_min_ms',
        id: 'rtt_min_ms',
        header: 'RTT Min',
        cell: ({ row }) => (
          <div className="font-mono">
            {row.original.rtt_min_ms !== null && row.original.rtt_min_ms !== undefined
              ? `${row.original.rtt_min_ms.toFixed(1)} ms`
              : '—'}
          </div>
        ),
      },
      {
        accessorKey: 'tcp_rtt_ms',
        id: 'tcp_rtt_ms',
        header: 'TCP RTT',
        cell: ({ row }) => (
          <div className="font-mono">
            {row.original.tcp_rtt_ms !== null && row.original.tcp_rtt_ms !== undefined
              ? `${row.original.tcp_rtt_ms.toFixed(1)} ms`
              : '—'}
          </div>
        ),
      },
    ],
    [activeServiceId, startTime, endTime, onSelectMapItem],
  )
}

export function ProxyWatchdogStatCards({
  isLoading,
  activeProxiesCount,
  tunnelRequestsCount,
  distanceMismatchesCount,
  tunnelDashboardUrl,
}: {
  isLoading: boolean
  activeProxiesCount: number
  tunnelRequestsCount: number
  distanceMismatchesCount: number
  tunnelDashboardUrl: string
}) {
  return (
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
            <p>
              This metric represents the number of distinct client IP addresses actively routing requests through VPN
              servers, anonymizing proxies, or Tor exit nodes during the selected time window.
            </p>
            <div>
              <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
              <p>Every client IP is evaluated in real-time against two high-accuracy behavioral indicators:</p>
              <ul className="list-disc pl-5 mt-1 space-y-1">
                <li>
                  <strong className="text-foreground">Impossible Distance Mismatch:</strong> Compares measured edge
                  network latency (min RTT) with physical geographical speed-of-light limits.
                </li>
                <li>
                  <strong className="text-foreground">Latency-RTT Ratio Violation:</strong> Measures the discrepancy
                  between the SSL/application handshake latency and raw TCP round-trip.
                </li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold text-foreground mb-1">Security Impact:</h4>
              <p>
                While commercial VPN usage is standard for privacy-conscious human users, sudden surges in this cohort
                often indicate automated scrapers or credential stuffing attempts trying to evade IP-based blocking.
              </p>
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
            <p>
              This tracks the absolute HTTP request volume routed through VPNs, anonymizing networks, or hosting
              providers during the selected time period.
            </p>
            <div>
              <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
              <p>
                Sums every request whose client IP is flagged as originating from a commercial VPN provider, a known
                datacenter host, or is otherwise exhibiting active behavioral tunneling fingerprints on our network
                interfaces.
              </p>
            </div>
            <div>
              <h4 className="font-semibold text-foreground mb-1">Why this matters:</h4>
              <p>
                Request volume is a key indicator of scraper velocity. An individual botnet using rotated proxy IPs
                might keep its &quot;per-IP&quot; rates extremely low to fly under the radar, but the aggregate
                &quot;Tunnel Requests&quot; volume will spike dramatically during active scanning.
              </p>
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
            <p>
              Geographical anomalies represent requests where the physical distance between the client&apos;s reported
              GeoIP location and the landing edge POP is physically incompatible with the recorded network latency.
            </p>
            <div>
              <h4 className="font-semibold text-foreground mb-1">The Physics Formula:</h4>
              <p>
                We calculate the Haversine distance between the client GeoIP coordinate and the POP. Light in fiber
                travels at ~200,000 km/s (~100km per millisecond round-trip). We enforce:
              </p>
              <div className="font-mono text-xs bg-muted p-2 rounded text-center my-2 border">
                distance_km &gt; (rtt_min_ms * 100.0 + 150.0)
              </div>
              <p>
                If a request landing in Tokyo with a 3ms RTT claims to be from Paris (distance &gt; 9,000 km), it
                breaks the speed of light. This proves the request&apos;s origin was masked.
              </p>
            </div>
            <div>
              <h4 className="font-semibold text-foreground mb-1">Security Recommendation:</h4>
              <p>
                High volumes of distance mismatches are characteristic of commercial geo-bypass tooling,
                coordinate-spoofing browser extensions, or automated crawlers originating from overseas datacenters
                routed through localized tunnels.
              </p>
            </div>
          </div>
        }
      />
    </div>
  )
}

export function TrafficQualityAndIspCards({
  isLoading,
  trafficQuality,
  suspiciousIsps,
  getIspDashboardUrl,
  onOpenHelp,
}: {
  isLoading: boolean
  trafficQuality: NonNullable<SecurityData['traffic_quality']>
  suspiciousIsps: NonNullable<SecurityData['suspicious_isps']>
  getIspDashboardUrl: (asn: number | string | null | undefined) => string
  onOpenHelp: (key: WatchdogHelpKey) => void
}) {
  return (
    <div className="lg:col-span-1 space-y-6">
      <Card className="min-h-[280px] [contain-intrinsic-size:320px] [content-visibility:auto]">
        <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
          <div>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Activity className="h-4 w-4 text-violet-500" /> Traffic Quality Breakdown
            </CardTitle>
            <CardDescription className="text-xs">Assessed request reliability index</CardDescription>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0"
            onClick={() => onOpenHelp('traffic')}
            title="About Traffic Quality Breakdown"
          >
            <HelpCircle className="h-4 w-4" />
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          {isLoading ? (
            <div className="space-y-3">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-full" />
            </div>
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
                    <div
                      className={`h-full rounded-full ${
                        label === 'High' ? 'bg-emerald-500' : label === 'Medium' ? 'bg-amber-500' : 'bg-rose-500'
                      }`}
                      style={{ width: `${val}%` }}
                    />
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
            <CardDescription className="text-xs">
              Leading network providers hosting proxy connections
            </CardDescription>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0"
            onClick={() => onOpenHelp('isp')}
            title="About Suspicious ISPs"
          >
            <HelpCircle className="h-4 w-4" />
          </Button>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">
              <Skeleton className="h-6 w-full" />
              <Skeleton className="h-6 w-full" />
              <Skeleton className="h-6 w-full" />
            </div>
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
                      <a
                        href={dashboardUrl}
                        className="font-medium truncate max-w-[180px] hover:text-violet-600 dark:hover:text-violet-400 hover:underline inline-flex items-center gap-1 group/link"
                        title={`Filter Dashboard by ${ispName}`}
                      >
                        {ispName}
                        <ExternalLink className="h-3 w-3 opacity-0 group-hover/item:opacity-100 transition-opacity text-muted-foreground" />
                      </a>
                    ) : (
                      <span className="font-medium truncate max-w-[180px]">{ispName}</span>
                    )}
                    {hasAsn ? (
                      <a href={dashboardUrl} title={`Filter Dashboard by ${ispName}`}>
                        <Badge
                          variant="outline"
                          className="font-mono hover:bg-violet-50 dark:hover:bg-violet-950/30 hover:border-violet-300 dark:hover:border-violet-700 hover:text-violet-600 dark:hover:text-violet-400 cursor-pointer transition-colors"
                        >
                          {formatCount(Number(item.count || 0))} reqs
                        </Badge>
                      </a>
                    ) : (
                      <Badge variant="outline" className="font-mono">
                        {formatCount(Number(item.count || 0))} reqs
                      </Badge>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export function ProxyExportCenterCard({
  threshold,
  setThreshold,
  format,
  setFormat,
  isLoading,
  isExporting,
  onExport,
  onOpenHelp,
}: {
  threshold: 'High' | 'Medium' | 'Low'
  setThreshold: (val: 'High' | 'Medium' | 'Low') => void
  format: 'fastly-acl' | 'plain'
  setFormat: (val: 'fastly-acl' | 'plain') => void
  isLoading: boolean
  isExporting: boolean
  onExport: () => void
  onOpenHelp: (key: WatchdogHelpKey) => void
}) {
  return (
    <Card className="border-primary/20 bg-primary/5 [contain-intrinsic-size:140px] [content-visibility:auto]">
      <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
        <div>
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Download className="h-4 w-4 text-primary" /> VPN/Proxy Export Center
          </CardTitle>
          <CardDescription className="text-xs">
            Export verified anonymizer IP lists for ingestion into Fastly WAF Edge ACLs.
          </CardDescription>
        </div>
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0"
          onClick={() => onOpenHelp('export')}
          title="About Export Center"
        >
          <HelpCircle className="h-4 w-4" />
        </Button>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-4 items-end">
          <div className="space-y-1.5 shrink-0">
            <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1">
              <ShieldAlert className="h-3 w-3" /> Risk Threshold
            </span>
            <Select
              value={threshold}
              onValueChange={(val) => {
                if (val) setThreshold(val)
              }}
            >
              <SelectTrigger className="w-36 h-9 text-xs bg-background">
                <SelectValue />
              </SelectTrigger>
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
            <Select
              value={format}
              onValueChange={(val) => {
                if (val) setFormat(val)
              }}
            >
              <SelectTrigger className="w-48 h-9 text-xs bg-background">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="fastly-acl">Fastly ACL (CSV format)</SelectItem>
                <SelectItem value="plain">Plain text (IP address list)</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <Button
            size="sm"
            className="h-9 font-medium text-xs ml-auto"
            onClick={onExport}
            disabled={isLoading || isExporting}
          >
            <Download className={`h-3.5 w-3.5 mr-1.5 ${isExporting ? 'animate-spin' : ''}`} />
            {isExporting ? 'Generating Export...' : 'Generate Blocklist'}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
