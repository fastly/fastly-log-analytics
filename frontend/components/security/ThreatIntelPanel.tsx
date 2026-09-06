'use client'

import React, { useMemo, useState } from 'react'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { client } from '@/lib/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Shield, ShieldAlert, Cpu, Globe, Activity, HelpCircle } from 'lucide-react'
import type { components } from '@/types/api.generated'
import { ColumnDef } from '@tanstack/react-table'
import { FilterValueCell } from '@/components/FilterValueCell'
import { DataTable } from '@/components/DataTable/DataTable'
import { HelpDialog } from '@/components/ui/help-dialog'
import { Button } from '@/components/ui/button'

type ThreatIntelItem = components['schemas']['ThreatIntelItem']

interface ThreatIntelPanelProps {
  serviceId: string | null
  startTime: string | null
  endTime: string | null
}

export function ThreatIntelPanel({ serviceId, startTime, endTime }: ThreatIntelPanelProps) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)

  const { data, isLoading, error } = useServiceQuery<ThreatIntelItem[]>(
    ['security', 'threat-intel', serviceId, startTime, endTime],
    async ({ signal }) => {
      if (!serviceId) return []
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { data: res, error: reqErr } = await client.GET('/api/security/threat-intel' as any, {
        signal,
        params: {
          query: {
            start_time: startTime ?? undefined,
            end_time: endTime ?? undefined,
          },
        },
      })
      if (reqErr) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        throw new Error((reqErr as any)?.detail || 'Failed to fetch threat intelligence data')
      }
      return res?.data || []
    },
    {
      enabled: !!serviceId,
    }
  )

  const columns = useMemo<ColumnDef<ThreatIntelItem>[]>(() => [
    {
      accessorKey: 'tls_fingerprint',
      header: 'TLS Fingerprint (JA3/JA4)',
      cell: ({ row }) => {
        const val = row.original.tls_fingerprint
        const isJa4 = val.includes('_')
        const filterKey = isJa4 ? 'ja4' : 'ja3'
        return (
          <div className="font-mono text-xs font-semibold select-all w-full max-w-[320px] truncate">
            <FilterValueCell
              filters={[{ column: filterKey, value: val }]}
              display={val}
            />
          </div>
        )
      }
    },
    {
      accessorKey: 'requests',
      header: 'Requests',
      cell: ({ row }) => (
        <span className="font-semibold font-mono text-xs text-foreground/80 tabular-nums">
          {row.original.requests.toLocaleString()}
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
            <FilterValueCell
              filters={[{ column: 'country', value: country }]}
              display={country}
            />
          </div>
        )
      }
    },
    {
      accessorKey: 'matched_waf_rules_count',
      header: 'Matched WAF Rules',
      cell: ({ row }) => {
        const count = row.original.matched_waf_rules_count
        return count > 0 ? (
          <Badge variant="outline" className="font-mono bg-destructive/10 text-destructive border-destructive/20 hover:bg-destructive/15">
            {count.toLocaleString()} triggers
          </Badge>
        ) : (
          <span className="text-xs text-muted-foreground font-medium">None</span>
        )
      }
    },
    {
      accessorKey: 'bot_percentage',
      header: 'Bot Percentage',
      cell: ({ row }) => {
        const pct = row.original.bot_percentage
        return pct > 0 ? (
          <div className="flex items-center gap-1.5">
            <Cpu className="h-3.5 w-3.5 text-orange-500" />
            <span className="font-mono font-bold text-xs text-orange-500">
              {pct.toFixed(1)}%
            </span>
          </div>
        ) : (
          <span className="text-xs text-muted-foreground font-medium">0.0%</span>
        )
      }
    },
    {
      accessorKey: 'is_anonymized_proxy',
      header: 'Anonymity Category',
      cell: ({ row }) => {
        return row.original.is_anonymized_proxy ? (
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
        )
      }
    }
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
                  onClick={() => setIsHelpOpen(true)}
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
        onOpenChange={setIsHelpOpen}
        title="Client TLS Fingerprints & Threat Intelligence Guide"
        icon={<Shield className="h-5 w-5 text-primary" />}
        size="lg"
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
                  Indicates the connection originates from a commercial VPN provider, public or private proxy, Tor Exit Node, or hosting data center. These routes hide the end user&apos;s true residential IP and are highly favored by automated scrapers, vulnerability scanners, and malicious bots.
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
                  Indicates standard consumer broadband, home Wi-Fi, or cellular networks. These carry typical human user traffic and generally represent a high-trust network baseline.
                </div>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-foreground mb-1">
              How They Are Determined
            </h4>
            <p className="leading-relaxed">
              Detection is handled at the Fastly real-time edge. Our VCL logs capture metadata fields:
            </p>
            <ul className="list-disc pl-5 space-y-1.5 mt-2 leading-relaxed">
              <li>
                <strong className="text-foreground font-medium">p_type / Proxy Type:</strong> Evaluated using Fastly&apos;s edge geolocation databases, classifying the connection (e.g., <code className="text-[11px] font-mono bg-muted/60 px-1 py-0.5 rounded">vpn</code>, <code className="text-[11px] font-mono bg-muted/60 px-1 py-0.5 rounded">hosting</code>, or <code className="text-[11px] font-mono bg-muted/60 px-1 py-0.5 rounded">tor</code>).
              </li>
              <li>
                <strong className="text-foreground font-medium">WAF Rules:</strong> Counts requests hitting OWASP rules or application protection blocks within the session window.
              </li>
              <li>
                <strong className="text-foreground font-medium">Bot Percentage:</strong> Percentage of requests from the fingerprint that triggered automated client classifiers or hit known bot signatures.
              </li>
            </ul>
          </div>
        </div>
      </HelpDialog>
    </>
  )
}
