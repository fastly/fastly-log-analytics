'use client'

import React, { useState } from 'react'
import { ShieldAlert, Download, AlertTriangle, Activity, Server, Globe, MapIcon, ExternalLink, HelpCircle } from 'lucide-react'
import dynamic from 'next/dynamic'
import { StatCard } from '@/components/ui/stat-card'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { useAdminTokenStore } from '@/stores/adminTokenStore'
import type { components } from '@/types/api.generated'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { FilterValueCell } from '@/components/FilterValueCell'
import { HelpDialog } from '@/components/ui/help-dialog'

const ImpossibleDistanceModal = dynamic(
  () => import('@/components/Insights/ImpossibleDistanceModal').then((m) => m.ImpossibleDistanceModal),
  { ssr: false }
)

type SecurityProxiesResponse = components['schemas']['SecurityProxiesResponse']
type ActiveClientItem = components['schemas']['ActiveClientItem']

interface ProxyWatchdogSectionProps {
  data: SecurityProxiesResponse | undefined
  isLoading: boolean
  error?: Error | null
  startTime?: string | null
  endTime?: string | null
  activeServiceId?: string | null
}

export function ProxyWatchdogSection({
  data,
  isLoading,
  error = null,
  startTime,
  endTime,
  activeServiceId,
}: ProxyWatchdogSectionProps) {
  const [threshold, setThreshold] = useState<'High' | 'Medium' | 'Low'>('High')
  const [format, setFormat] = useState<'fastly-acl' | 'plain'>('fastly-acl')
  const [isExporting, setIsExporting] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<any | null>(null)

  const [trafficHelpOpen, setTrafficHelpOpen] = useState(false)
  const [ispHelpOpen, setIspHelpOpen] = useState(false)
  const [exportHelpOpen, setExportHelpOpen] = useState(false)
  const [clientsHelpOpen, setClientsHelpOpen] = useState(false)

  const [columnVisibility, setColumnVisibility, onColumnVisibilityChange] = useColumnVisibility()

  const WATCHDOG_COLUMNS_LIST = React.useMemo(() => [
    { id: 'ip', label: 'IP Address' },
    { id: 'asn_name', label: 'ASN / Network' },
    { id: 'risk_level', label: 'Risk Level' },
    { id: 'distance_km', label: 'Distance (km)' },
    { id: 'rtt_min_ms', label: 'RTT Min' },
    { id: 'tcp_rtt_ms', label: 'TCP RTT' },
  ], [])

  const watchdogColumns = React.useMemo(() => [
    {
      accessorKey: 'ip',
      id: 'ip',
      header: 'IP Address',
      cell: (info: any) => {
        const ip = info.getValue()
        return (
          <div className="flex items-center gap-1.5 min-w-0 w-full">
            <FilterValueCell
              filters={[{ column: 'ip', value: ip }]}
              display={ip}
              className="font-mono font-medium"
            />
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
      cell: (info: any) => {
        const val = info.getValue()
        return val ? (
          <span className="truncate block max-w-[200px]" title={val}>
            {val}
          </span>
        ) : (
          <span className="text-muted-foreground">—</span>
        )
      },
    },
    {
      accessorKey: 'risk_level',
      id: 'risk_level',
      header: 'Risk Level',
      cell: (info: any) => {
        const val = info.getValue()
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
      cell: (info: any) => {
        const row = info.row.original as ActiveClientItem
        return (
          <div className="flex flex-col items-start w-full">
            <span className="font-mono">
              {row.distance_km !== null && row.distance_km !== undefined
                ? row.distance_km.toLocaleString(undefined, { maximumFractionDigits: 1 })
                : '—'}
            </span>
            {row.impossible_distance && (
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  const client_lat = row.client_lat ?? 0
                  const client_lon = row.client_lon ?? 0
                  const pop_lat = row.pop_lat ?? 0
                  const pop_lon = row.pop_lon ?? 0
                  const pop = row.pop ?? 'Unknown'
                  const tcp_rtt = (row.tcp_rtt_ms ?? 0) * 1000
                  const distance_km = row.distance_km ?? 0
                  const max_km = (row.rtt_min_ms ?? 0) * 100.0 + 150.0

                  setSelectedMapItem({
                    label: row.ip,
                    client_lat,
                    client_lon,
                    pop_lat,
                    pop_lon,
                    pop,
                    tcp_rtt,
                    distance_km,
                    max_km,
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
      cell: (info: any) => {
        const val = info.getValue()
        return (
          <div className="font-mono">
            {val !== null && val !== undefined ? `${val.toFixed(1)} ms` : '—'}
          </div>
        )
      },
    },
    {
      accessorKey: 'tcp_rtt_ms',
      id: 'tcp_rtt_ms',
      header: 'TCP RTT',
      cell: (info: any) => {
        const val = info.getValue()
        return (
          <div className="font-mono">
            {val !== null && val !== undefined ? `${val.toFixed(1)} ms` : '—'}
          </div>
        )
      },
    },
  ], [setSelectedMapItem, activeServiceId, startTime, endTime])

  const handleExport = async () => {
    setIsExporting(true)
    try {
      const params = new URLSearchParams({
        start_time: startTime || '',
        end_time: endTime || '',
        threshold,
        format,
      })
      const url = `/api/security/proxies/export?${params.toString()}`

      const headers: Record<string, string> = {}
      if (activeServiceId) {
        headers['x-service-id'] = activeServiceId
        headers['x-fastly-service-id'] = activeServiceId
      }

      const token = useAdminTokenStore.getState().token
      if (token) {
        headers['X-Admin-Token'] = token
      }

      const response = await fetch(url, { headers })
      if (!response.ok) {
        throw new Error(`Export failed: ${response.statusText}`)
      }

      const blob = await response.blob()
      const blobUrl = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = blobUrl
      a.download = format === 'plain' ? `proxies_${threshold.toLowerCase()}.txt` : `proxies_acl_${threshold.toLowerCase()}.csv`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      window.URL.revokeObjectURL(blobUrl)
    } catch (err) {
      console.error('Export error:', err)
    } finally {
      setIsExporting(false)
    }
  }

  const tunnelDashboardUrl = React.useMemo(() => {
    const params = new URLSearchParams()
    if (activeServiceId) params.set('service', activeServiceId)
    if (startTime) params.set('start_time', startTime)
    if (endTime) params.set('end_time', endTime)

    const filtersPayload = {
      _tunnel_requests: {
        mode: 'include',
        values: ['true'],
      },
    }
    params.set('filters', JSON.stringify(filtersPayload))
    return `/dashboard?${params.toString()}`
  }, [activeServiceId, startTime, endTime])

  const getIspDashboardUrl = React.useCallback(
    (asn: number | string | null | undefined) => {
      if (!asn) return '#'
      const params = new URLSearchParams()
      if (activeServiceId) params.set('service', activeServiceId)
      if (startTime) params.set('start_time', startTime)
      if (endTime) params.set('end_time', endTime)

      const filtersPayload = {
        asn: {
          mode: 'include',
          values: [String(asn)],
        },
      }
      params.set('filters', JSON.stringify(filtersPayload))
      return `/dashboard?${params.toString()}`
    },
    [activeServiceId, startTime, endTime],
  )

  if (error) {
    return (
      <Card className="border-destructive/50 bg-destructive/5 p-6 text-center">
        <AlertTriangle className="mx-auto h-10 w-10 text-destructive mb-3" />
        <CardTitle className="text-lg font-semibold text-destructive mb-1">
          Failed to Load Proxy Watchdog Data
        </CardTitle>
        <CardDescription className="text-sm text-muted-foreground">
          {error.message || 'An error occurred.'}
        </CardDescription>
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
      {/* Top Stat Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <StatCard
          title="Active VPN & Proxy Users"
          value={isLoading ? <Skeleton className="h-8 w-16" /> : activeProxiesCount.toLocaleString()}
          sub="Unique suspicious IP addresses detected"
          icon={Globe}
          iconClassName="text-blue-500"
          loading={isLoading}
          helpTitle="Active VPN & Proxy Users Detection"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>
                This metric represents the number of distinct client IP addresses actively routing requests through VPN servers, anonymizing proxies, or Tor exit nodes during the selected time window.
              </p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
                <p>
                  Every client IP is evaluated in real-time against two high-accuracy behavioral indicators:
                </p>
                <ul className="list-disc pl-5 mt-1 space-y-1">
                  <li>
                    <strong className="text-foreground">Impossible Distance Mismatch:</strong> Compares measured edge network latency (min RTT) with physical geographical speed-of-light limits. If a packet arrives from London to a Tokyo edge POP in 3ms, the geographical coordinates are falsified (proxy/VPN tunnel).
                  </li>
                  <li>
                    <strong className="text-foreground">Latency-RTT Ratio Violation:</strong> Measures the discrepancy between the SSL/application handshake latency and raw TCP round-trip. Terminating TCP sessions at the edge while back-channeling application traffic indicates proxy routing.
                  </li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Security Impact:</h4>
                <p>
                  While commercial VPN usage is standard for privacy-conscious human users, sudden surges in this cohort often indicate automated scrapers or credential stuffing attempts trying to evade IP-based blocking.
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
                {tunnelRequestsCount.toLocaleString()}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover/link:opacity-100 transition-opacity text-muted-foreground" />
              </a>
            )
          }
          sub="HTTP requests routed via anonymizers"
          icon={Activity}
          iconClassName="text-violet-500"
          loading={isLoading}
          helpTitle="Tunnel Requests Volume"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>
                This tracks the absolute HTTP request volume routed through VPNs, anonymizing networks, or hosting providers during the selected time period.
              </p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
                <p>
                  Sums every request whose client IP is flagged as originating from a commercial VPN provider, a known datacenter host, or is otherwise exhibiting active behavioral tunneling fingerprints on our network interfaces.
                </p>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Why this matters:</h4>
                <p>
                  Request volume is a key indicator of scraper velocity. An individual botnet using rotated proxy IPs might keep its "per-IP" rates extremely low to fly under the radar, but the aggregate "Tunnel Requests" volume will spike dramatically during active scanning.
                </p>
              </div>
            </div>
          }
        />
        <StatCard
          title="Impossible Distance Mismatches"
          value={isLoading ? <Skeleton className="h-8 w-16" /> : distanceMismatchesCount.toLocaleString()}
          sub="IP-to-latency geographical anomalies"
          icon={AlertTriangle}
          iconClassName="text-amber-500"
          loading={isLoading}
          helpTitle="Impossible Distance Mismatch Auditing"
          helpContent={
            <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
              <p>
                Geographical anomalies represent requests where the physical distance between the client's reported GeoIP location and the landing edge POP is physically incompatible with the recorded network latency.
              </p>
              <div>
                <h4 className="font-semibold text-foreground mb-1">The Physics Formula:</h4>
                <p>
                  We calculate the Haversine distance between the client GeoIP coordinate and the POP. Light in fiber travels at ~200,000 km/s (~100km per millisecond round-trip). We enforce:
                </p>
                <div className="font-mono text-xs bg-muted p-2 rounded text-center my-2 border">
                  distance_km &gt; (rtt_min_ms * 100.0 + 150.0)
                </div>
                <p>
                  If a request landing in Tokyo with a 3ms RTT claims to be from Paris (distance &gt; 9,000 km), it breaks the speed of light. This proves the request's origin was masked.
                </p>
              </div>
              <div>
                <h4 className="font-semibold text-foreground mb-1">Security Recommendation:</h4>
                <p>
                  High volumes of distance mismatches are characteristic of commercial geo-bypass tooling, coordinate-spoofing browser extensions, or automated crawlers originating from overseas datacenters routed through localized tunnels.
                </p>
              </div>
            </div>
          }
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left column: Traffic Quality & Suspicious ISPs */}
        <div className="lg:col-span-1 space-y-6">
          <Card>
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
                onClick={() => setTrafficHelpOpen(true)}
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

          <Card>
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Server className="h-4 w-4 text-blue-500" /> Suspicious ISPs & Providers
                </CardTitle>
                <CardDescription className="text-xs">Leading network providers hosting proxy connections</CardDescription>
              </div>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-muted-foreground hover:text-foreground shrink-0"
                onClick={() => setIspHelpOpen(true)}
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
                            <Badge variant="outline" className="font-mono hover:bg-violet-50 dark:hover:bg-violet-950/30 hover:border-violet-300 dark:hover:border-violet-700 hover:text-violet-600 dark:hover:text-violet-400 cursor-pointer transition-colors">
                              {Number(item.count || 0).toLocaleString()} reqs
                            </Badge>
                          </a>
                        ) : (
                          <Badge variant="outline" className="font-mono">
                            {Number(item.count || 0).toLocaleString()} reqs
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

        {/* Right column: Export block & Active Clients */}
        <div className="lg:col-span-2 space-y-6">
          {/* Export block */}
          <Card className="border-primary/20 bg-primary/5">
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
                onClick={() => setExportHelpOpen(true)}
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
                  <Select value={threshold} onValueChange={(val) => { if (val) setThreshold(val) }}>
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
                  <Select value={format} onValueChange={(val) => { if (val) setFormat(val) }}>
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
                  onClick={handleExport}
                  disabled={isLoading || isExporting}
                >
                  <Download className={`h-3.5 w-3.5 mr-1.5 ${isExporting ? 'animate-spin' : ''}`} />
                  {isExporting ? 'Generating Export...' : 'Generate Blocklist'}
                </Button>
              </div>
            </CardContent>
          </Card>

          {/* Active Clients */}
          <Card>
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  Active Suspicious Clients
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 text-muted-foreground hover:text-foreground shrink-0 rounded-full"
                    onClick={() => setClientsHelpOpen(true)}
                    title="About Active Suspicious Clients"
                  >
                    <HelpCircle className="h-3.5 w-3.5" />
                  </Button>
                </CardTitle>
                <CardDescription className="text-xs">Current active sessions matching behavioral proxy profiles</CardDescription>
              </div>
              <ColumnVisibilityDropdown
                columns={WATCHDOG_COLUMNS_LIST}
                visibility={columnVisibility}
                onChange={onColumnVisibilityChange}
              />
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

      <ImpossibleDistanceModal
        isOpen={!!selectedMapItem}
        onOpenChange={(open) => !open && setSelectedMapItem(null)}
        data={selectedMapItem}
      />

      <HelpDialog
        open={trafficHelpOpen}
        onOpenChange={setTrafficHelpOpen}
        title="Traffic Quality Assessment"
        size="lg"
      >
        <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
          <p>
            Traffic Quality categorizes client connections based on their end-to-end network profiles, highlighting direct connection reliability vs active tunneling.
          </p>
          <div>
            <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
            <ul className="list-disc pl-5 space-y-1">
              <li>
                <strong className="text-foreground">Active Tunnel / Proxy:</strong> Triggered when a client's minimum edge latency is a tiny fraction (&lt;10%) of their actual TCP round-trip time. This means the client is terminating TCP nearby, but back-channeling through a proxy back-end.
              </li>
              <li>
                <strong className="text-foreground">WiFi / Mobile:</strong> Typical of standard commercial networks, showing standard Jitter, RTT ratios between 10% and 70%, and moderate packet overhead.
              </li>
              <li>
                <strong className="text-foreground">Direct Connection:</strong> Highly stable connections with low jitter where physical edge RTT strongly correlates with the application handshake.
              </li>
            </ul>
          </div>
        </div>
      </HelpDialog>

      <HelpDialog
        open={ispHelpOpen}
        onOpenChange={setIspHelpOpen}
        title="Suspicious ISPs & Network Providers"
        size="lg"
      >
        <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
          <p>
            Identifies the Autonomous System Numbers (ASNs) and network operators hosting the highest volume of proxy/VPN connections.
          </p>
          <div>
            <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
            <p>
              Groups all requests matching active proxy signatures by their client network ASN and resolves their authoritative names (e.g. DigitalOcean, OVH, M247, NordVPN).
            </p>
          </div>
          <div>
            <h4 className="font-semibold text-foreground mb-1">Security Tip:</h4>
            <p>
              High request volume from consumer ISPs (Comcast, Verizon, Spectrum) is normal VPN use. High request volumes from datacenter hosting providers (AWS, DigitalOcean, Hetzner, Linode) almost always represents automated crawlers, headless bots, or commercial proxy nodes.
            </p>
          </div>
        </div>
      </HelpDialog>

      <HelpDialog
        open={exportHelpOpen}
        onOpenChange={setExportHelpOpen}
        title="VPN/Proxy Export Center Guide"
        size="lg"
      >
        <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
          <p>
            The Export Center allows security administrators to download compiled list files containing active, high-risk proxy and VPN IP addresses observed on this service.
          </p>
          <div>
            <h4 className="font-semibold text-foreground mb-1">How it is used:</h4>
            <p>
              These exported lists can be directly uploaded to Fastly Edge Dictionaries, Access Control Lists (ACLs), or WAF custom rules to instantly block or rate-limit these suspect clients at the absolute network edge, saving server resources and database overhead.
            </p>
          </div>
          <div>
            <h4 className="font-semibold text-foreground mb-1">Export Risk Thresholds:</h4>
            <ul className="list-disc pl-5 space-y-1">
              <li>
                <strong className="text-foreground">High Risk:</strong> Extremely strict criteria, matching clients exhibiting active speed-of-light coordinate spoofing violations or hosting datacenter exit nodes. Extremely low false-positive rate.
              </li>
              <li>
                <strong className="text-foreground">Medium & Above:</strong> Recommended default. Includes verified high-risk clients along with commercial VPN addresses showing aggressive request volumes.
              </li>
              <li>
                <strong className="text-foreground">Include Low Risk:</strong> Comprehensive list. Includes residential proxies, consumer VPNs with standard browsing behavior, and occasional high-jitter mobile networks. Use with caution to avoid blocking standard users.
              </li>
            </ul>
          </div>
        </div>
      </HelpDialog>

      <HelpDialog
        open={clientsHelpOpen}
        onOpenChange={setClientsHelpOpen}
        title="Active Suspicious Clients Auditing"
        size="lg"
      >
        <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
          <p>
            An interactive high-fidelity table displaying real-time client sessions flagged with active VPN or Proxy signatures.
          </p>
          <div>
            <h4 className="font-semibold text-foreground mb-1">Key Column Definitions:</h4>
            <ul className="list-disc pl-5 space-y-1">
              <li>
                <strong className="text-foreground">IP Address:</strong> The client's IPv4/IPv6 edge address. Fully interactive (copy, filter, or open in the central query dashboard).
              </li>
              <li>
                <strong className="text-foreground">Risk Level:</strong> Behavioral scoring threshold (High, Medium, Low) derived from distance anomalies, RTT ratios, and provider threat profiles.
              </li>
              <li>
                <strong className="text-foreground">Distance:</strong> The physical distance (in km) between the edge landing POP and the client's registered geographic location.
              </li>
              <li>
                <strong className="text-foreground">RTT Min:</strong> The minimum measured packet round-trip time between the client and the edge server.
              </li>
              <li>
                <strong className="text-foreground">TCP RTT:</strong> The transport layer's established TCP round-trip. Anomalous differences between RTT Min and TCP RTT reveal tunnel boundaries.
              </li>
            </ul>
          </div>
        </div>
      </HelpDialog>
    </div>
  )
}
