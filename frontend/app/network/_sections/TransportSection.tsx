'use client'

import React from 'react'
import Link from 'next/link'
import { StatCard } from '@/components/ui/stat-card'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { useServiceQuery } from '@/hooks/useDashboardCards'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { formatCompactCount } from '@/lib/format'
import { Activity, AlertTriangle, ArrowDownUp, Gauge, Network, Wifi } from 'lucide-react'
import type { components } from '@/types/api'
import type { FiltersPayload } from '@/types/filters'

type TransportMetrics = components['schemas']['TransportMetrics']
type TransportProtocolShare = components['schemas']['TransportProtocolShare']

interface TransportSectionProps {
  activeServiceId?: string | null
  startTime?: string | null
  endTime?: string | null
  filterPayload?: FiltersPayload
}

export function TransportSection({
  activeServiceId,
  startTime,
  endTime,
  filterPayload,
}: TransportSectionProps) {
  const transportQuery = useServiceQuery(
    ['network', 'bundle', 'transport', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const data = await unwrapClient(client.POST('/api/network/bundle', {
        signal,
        body: {
          start_time: startTime ?? undefined,
          end_time: endTime ?? undefined,
          filters: filterPayload ?? {},
          sections: ['transport'],
          // The generated request type marks defaulted fields required; these
          // equal the backend NetworkBundleRequest defaults.
          metric: 'health_score',
          bucket_seconds: 300,
          top_n: 30,
          map_asn: 'all',
          region_country: 'US',
        },
      }))
      return (data as { transport?: TransportMetrics } | undefined)?.transport ?? null
    },
    { enabled: Boolean(activeServiceId && startTime && endTime) }
  )

  const transport = transportQuery.data
  const isLoading = transportQuery.isLoading || (transportQuery.isFetching && !transport)
  const missingFields = transport?.missing_fields ?? []

  return (
    <div className="space-y-4 my-6 [contain-intrinsic-size:240px] [content-visibility:auto]">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide flex items-center gap-2">
            <Network className="h-4 w-4" />
            Edge Transport & TCP Performance
          </h2>
          <p className="text-xs text-muted-foreground mt-0.5">
            TCP connection round-trip latency, retransmission overhead, and protocol distribution recorded at the Fastly edge.
          </p>
        </div>
      </div>

      {missingFields.length > 0 && (
        <div
          data-testid="missing-transport-fields-banner"
          className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-400 flex items-start gap-2.5"
        >
          <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
          <div className="flex-1">
            <span className="font-semibold">Optional transport fields not logged: </span>
            <code className="text-[11px] font-mono bg-amber-500/20 px-1 py-0.5 rounded">
              {missingFields.join(', ')}
            </code>
            . Enable these fields under{' '}
            <Link
              href="/service-config"
              className="underline font-medium hover:text-amber-900 dark:hover:text-amber-200"
            >
              Service Configuration
            </Link>{' '}
            to unlock edge TCP latency percentiles, packet retransmissions, and delivery rates.
          </div>
        </div>
      )}

      {/* KPI Cards Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          title="TCP RTT (p50 / p95)"
          icon={Activity}
          loading={isLoading}
          loadingMessage="Analyzing TCP round-trip times..."
          value={
            transport?.tcp_rtt_p50_ms != null ? (
              <span className="font-mono">
                {transport.tcp_rtt_p50_ms.toFixed(1)}
                <span className="text-xs text-muted-foreground font-normal ml-1">
                  / {transport.tcp_rtt_p95_ms != null ? `${transport.tcp_rtt_p95_ms.toFixed(1)}ms` : '—'}
                </span>
              </span>
            ) : (
              '—'
            )
          }
          sub="Median & tail edge RTT"
          helpTitle="TCP Round-Trip Time"
          helpContent="Time taken for a data packet to travel from the Fastly edge node to the client and back."
        />

        <StatCard
          title="TCP Retransmissions"
          icon={ArrowDownUp}
          loading={isLoading}
          loadingMessage="Calculating retransmission rate..."
          value={
            transport?.retrans_rate_pct != null ? (
              <span className="font-mono">
                {transport.retrans_rate_pct.toFixed(2)}
                <span className="text-xs text-muted-foreground font-normal ml-0.5">%</span>
              </span>
            ) : (
              '—'
            )
          }
          sub="Average packet retransmit rate"
          helpTitle="TCP Retransmission Rate"
          helpContent="Percentage of TCP segments retransmitted due to network congestion or packet drop."
        />

        <StatCard
          title="Packet Loss"
          icon={Wifi}
          loading={isLoading}
          loadingMessage="Evaluating packet loss..."
          value={
            transport?.packet_loss_pct != null ? (
              <span className="font-mono">
                {transport.packet_loss_pct.toFixed(2)}
                <span className="text-xs text-muted-foreground font-normal ml-0.5">%</span>
              </span>
            ) : (
              '—'
            )
          }
          sub="Edge observed packet loss"
          helpTitle="Packet Loss Percentage"
          helpContent="Estimated packet loss between the client and Fastly edge cache nodes."
        />

        <StatCard
          title="Delivery Rate"
          icon={Gauge}
          loading={isLoading}
          loadingMessage="Measuring delivery rate..."
          value={
            transport?.delivery_rate_mbps != null ? (
              <span className="font-mono">
                {transport.delivery_rate_mbps.toFixed(1)}
                <span className="text-xs text-muted-foreground font-normal ml-1">Mbps</span>
              </span>
            ) : (
              '—'
            )
          }
          sub="Estimated client bandwidth"
          helpTitle="Delivery Throughput"
          helpContent="Paced delivery throughput achieved during response body transfer."
        />
      </div>

      {/* Protocol Distribution Breakdown */}
      {transport?.protocol_distribution && transport.protocol_distribution.length > 0 && (
        <AnalyticsCard
          title="Transport Protocol Share"
          helpTitle="Protocol Share"
          helpContent="Proportion of client requests served over modern HTTP protocols (HTTP/3, HTTP/2, HTTP/1.1)."
          className="[contain-intrinsic-size:120px] [content-visibility:auto]"
        >
          <div className="space-y-3 py-1">
            <div className="flex h-3 w-full rounded-full overflow-hidden bg-muted/40">
              {transport.protocol_distribution.map((p: TransportProtocolShare, idx: number) => {
                const colors = [
                  'bg-emerald-500',
                  'bg-sky-500',
                  'bg-amber-500',
                  'bg-purple-500',
                  'bg-rose-500',
                ]
                const color = colors[idx % colors.length]
                return (
                  <div
                    key={p.protocol}
                    className={`${color} transition-all duration-300`}
                    style={{ width: `${Math.max(p.share_pct, 1)}%` }}
                    title={`${p.protocol}: ${p.share_pct.toFixed(1)}% (${formatCompactCount(p.requests)} reqs)`}
                  />
                )
              })}
            </div>
            <div className="flex flex-wrap items-center gap-4 text-xs">
              {transport.protocol_distribution.map((p: TransportProtocolShare, idx: number) => {
                const dotColors = [
                  'bg-emerald-500',
                  'bg-sky-500',
                  'bg-amber-500',
                  'bg-purple-500',
                  'bg-rose-500',
                ]
                const dotColor = dotColors[idx % dotColors.length]
                return (
                  <div key={p.protocol} className="flex items-center gap-1.5 font-mono">
                    <span className={`h-2.5 w-2.5 rounded-full ${dotColor}`} />
                    <span className="font-semibold text-foreground">{p.protocol}</span>
                    <span className="text-muted-foreground">{p.share_pct.toFixed(1)}%</span>
                    <span className="text-muted-foreground/60 text-[10px]">
                      ({formatCompactCount(p.requests)})
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        </AnalyticsCard>
      )}
    </div>
  )
}
