'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
import { useColumnVisibility } from '@/hooks/useSSE'
import { downloadBlob } from '@/lib/utils'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { adminFetch } from '@/lib/api'
import { AlertTriangle, HelpCircle } from 'lucide-react'
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import type { ImpossibleDistanceData } from '@/components/Insights'
import type { SecurityData } from './BotsSection'
import {
  ProxyExportCenterCard,
  ProxyWatchdogStatCards,
  TrafficQualityAndIspCards,
  useWatchdogColumns,
  WATCHDOG_COLUMNS_LIST,
  type WatchdogHelpKey,
} from './ProxyWatchdogParts'

const ImpossibleDistanceModal = dynamic(
  () => import('@/components/Insights').then((m) => m.ImpossibleDistanceModal),
  { ssr: false },
)

const WATCHDOG_HELP_MODAL_CONTENT: Record<
  WatchdogHelpKey,
  { title: string; body: React.ReactNode }
> = {
  traffic: {
    title: 'Traffic Quality Assessment',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>
          Traffic Quality categorizes client connections based on their end-to-end network profiles, highlighting direct
          connection reliability vs active tunneling.
        </p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li>
              <strong className="text-foreground">Active Tunnel / Proxy:</strong> Triggered when a client&apos;s minimum
              edge latency is a tiny fraction (&lt;10%) of their actual TCP round-trip time.
            </li>
            <li>
              <strong className="text-foreground">WiFi / Mobile:</strong> Typical of standard commercial networks,
              showing standard Jitter, RTT ratios between 10% and 70%, and moderate packet overhead.
            </li>
            <li>
              <strong className="text-foreground">Direct Connection:</strong> Highly stable connections with low jitter
              where physical edge RTT strongly correlates with the application handshake.
            </li>
          </ul>
        </div>
      </div>
    ),
  },
  isp: {
    title: 'Suspicious ISPs & Network Providers',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>
          Identifies the Autonomous System Numbers (ASNs) and network operators hosting the highest volume of proxy/VPN
          connections.
        </p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is determined:</h4>
          <p>
            Groups all requests matching active proxy signatures by their client network ASN and resolves their
            authoritative names (e.g. DigitalOcean, OVH, M247, NordVPN).
          </p>
        </div>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Security Tip:</h4>
          <p>
            High request volume from consumer ISPs (Comcast, Verizon, Spectrum) is normal VPN use. High request volumes
            from datacenter hosting providers (AWS, DigitalOcean, Hetzner, Linode) almost always represents automated
            crawlers, headless bots, or commercial proxy nodes.
          </p>
        </div>
      </div>
    ),
  },
  export: {
    title: 'VPN/Proxy Export Center Guide',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>
          The Export Center allows security administrators to download compiled list files containing active, high-risk
          proxy and VPN IP addresses observed on this service.
        </p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">How it is used:</h4>
          <p>
            These exported lists can be directly uploaded to Fastly Edge Dictionaries, Access Control Lists (ACLs), or
            WAF custom rules to instantly block or rate-limit these suspect clients at the absolute network edge, saving
            server resources and database overhead.
          </p>
        </div>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Export Risk Thresholds:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li>
              <strong className="text-foreground">High Risk:</strong> Extremely strict criteria, matching clients
              exhibiting active speed-of-light coordinate spoofing violations or hosting datacenter exit nodes.
            </li>
            <li>
              <strong className="text-foreground">Medium & Above:</strong> Recommended default. Includes verified
              high-risk clients along with commercial VPN addresses showing aggressive request volumes.
            </li>
            <li>
              <strong className="text-foreground">Include Low Risk:</strong> Comprehensive list. Includes residential
              proxies, consumer VPNs with standard browsing behavior, and occasional high-jitter mobile networks.
            </li>
          </ul>
        </div>
      </div>
    ),
  },
  clients: {
    title: 'Active Suspicious Clients Auditing',
    body: (
      <div className="space-y-4 text-xs text-muted-foreground leading-relaxed">
        <p>
          An interactive high-fidelity table displaying real-time client sessions flagged with active VPN or Proxy
          signatures.
        </p>
        <div>
          <h4 className="font-semibold text-foreground mb-1">Key Column Definitions:</h4>
          <ul className="list-disc pl-5 space-y-1">
            <li>
              <strong className="text-foreground">IP Address:</strong> The client&apos;s IPv4/IPv6 edge address. Fully
              interactive (copy, filter, or open in the central query dashboard).
            </li>
            <li>
              <strong className="text-foreground">Risk Level:</strong> Behavioral scoring threshold (High, Medium, Low)
              derived from distance anomalies, RTT ratios, and provider threat profiles.
            </li>
            <li>
              <strong className="text-foreground">Distance:</strong> The physical distance (in km) between the edge
              landing POP and the client&apos;s registered geographic location.
            </li>
            <li>
              <strong className="text-foreground">RTT Min:</strong> The minimum measured packet round-trip time between
              the client and the edge server.
            </li>
            <li>
              <strong className="text-foreground">TCP RTT:</strong> The transport layer&apos;s established TCP
              round-trip. Anomalous differences between RTT Min and TCP RTT reveal tunnel boundaries.
            </li>
          </ul>
        </div>
      </div>
    ),
  },
}

export interface ProxyWatchdogSectionProps {
  data: SecurityData | undefined
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
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)
  const [activeHelpKey, setActiveHelpKey] = useState<WatchdogHelpKey | null>(null)
  const [columnVisibility, setColumnVisibility, onColumnVisibilityChange] = useColumnVisibility()

  const watchdogColumns = useWatchdogColumns({
    activeServiceId,
    startTime,
    endTime,
    onSelectMapItem: setSelectedMapItem,
  })

  const handleExport = async () => {
    setIsExporting(true)
    try {
      const params = new URLSearchParams({
        start_time: startTime || '',
        end_time: endTime || '',
        threshold,
        format,
      })
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
    (asn: number | string | null | undefined) =>
      asn ? buildFilteredDashboardUrl({ asn: { mode: 'include', values: [String(asn)] } }) : '#',
    [buildFilteredDashboardUrl],
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
      <ProxyWatchdogStatCards
        isLoading={isLoading}
        activeProxiesCount={activeProxiesCount}
        tunnelRequestsCount={tunnelRequestsCount}
        distanceMismatchesCount={distanceMismatchesCount}
        tunnelDashboardUrl={tunnelDashboardUrl}
      />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <TrafficQualityAndIspCards
          isLoading={isLoading}
          trafficQuality={trafficQuality}
          suspiciousIsps={suspiciousIsps}
          getIspDashboardUrl={getIspDashboardUrl}
          onOpenHelp={setActiveHelpKey}
        />

        <div className="lg:col-span-2 space-y-6">
          <ProxyExportCenterCard
            threshold={threshold}
            setThreshold={setThreshold}
            format={format}
            setFormat={setFormat}
            isLoading={isLoading}
            isExporting={isExporting}
            onExport={handleExport}
            onOpenHelp={setActiveHelpKey}
          />

          <Card className="min-h-[400px] [contain-intrinsic-size:600px] [content-visibility:auto]">
            <CardHeader className="pb-3 flex flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  Active Suspicious Clients
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 text-muted-foreground hover:text-foreground shrink-0 rounded-full"
                    onClick={() => setActiveHelpKey('clients')}
                    title="About Active Suspicious Clients"
                  >
                    <HelpCircle className="h-3.5 w-3.5" />
                  </Button>
                </CardTitle>
                <CardDescription className="text-xs">
                  Current active sessions matching behavioral proxy profiles
                </CardDescription>
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
