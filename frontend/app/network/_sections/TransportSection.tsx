'use client'

import React from 'react'
import Link from 'next/link'
import {
  Network as NetworkIcon,
  AlertTriangle,
  ArrowDownUp,
  Gauge,
  Wifi,
  Activity,
} from 'lucide-react'
import { client, unwrapClient } from '@/lib/api'
import type { components, FiltersPayload } from '@/types/api'
import { useServiceQuery } from '@/hooks/useReportPageState'
import { formatCompactCount } from '@/lib/utils'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/confirm-dialog'
import { ApproxBadge } from '@/app/origin/_sections/Aggregates'

const PROTOCOL_COLORS = ['bg-emerald-500', 'bg-sky-500', 'bg-amber-500', 'bg-purple-500', 'bg-rose-500'] as const

type TransportMetrics = components['schemas']['TransportMetrics']
type TransportProtocolShare = components['schemas']['TransportProtocolShare']

export function TransportSection({ activeServiceId, startTime, endTime, filterPayload, externalData, externalLoading }: {
  activeServiceId?: string | null; startTime?: string | null; endTime?: string | null; filterPayload?: FiltersPayload
  externalData?: TransportMetrics | null; externalLoading?: boolean
}) {
  const useExternal = externalData !== undefined || externalLoading !== undefined
  const transportQuery = useServiceQuery(
    ['network', 'bundle', 'transport', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const data = await unwrapClient(client.POST('/api/network/bundle', {
        signal,
        body: {
          start_time: startTime ?? undefined, end_time: endTime ?? undefined, filters: filterPayload ?? {},
          sections: ['transport'], metric: 'health_score', bucket_seconds: 300, top_n: 30, map_asn: 'all', region_country: 'US',
        },
      }))
      return (data as { transport?: TransportMetrics } | undefined)?.transport ?? null
    },
    { enabled: !useExternal && Boolean(activeServiceId && startTime && endTime) },
    3,
  )

  const transport = useExternal ? externalData : transportQuery.data
  const isLoading = useExternal ? Boolean(externalLoading) : (transportQuery.isLoading || (transportQuery.isFetching && !transport))
  const missingFields = transport?.missing_fields ?? []

  return (
    <div className="space-y-4 my-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide flex items-center gap-2">
            <NetworkIcon className="h-4 w-4" />
            Edge Transport & TCP Performance
            {transport?._approx === true && <ApproxBadge message={transport._approx_note ?? ''} />}
          </h2>
          <p className="text-xs text-muted-foreground mt-0.5">
            TCP connection round-trip latency, retransmission overhead, and protocol distribution recorded at the Fastly edge.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          {
            title: 'TCP RTT (p50 / p95)', icon: Activity, loadingMessage: 'Analyzing TCP round-trip times...',
            value: transport?.tcp_rtt_p50_ms != null ? (
              <span className="font-mono">
                {transport.tcp_rtt_p50_ms.toFixed(1)}
                <span className="text-xs text-muted-foreground font-normal ml-1">
                  / {transport.tcp_rtt_p95_ms != null ? `${transport.tcp_rtt_p95_ms.toFixed(1)}ms` : '—'}
                </span>
              </span>
            ) : '—',
            sub: 'Median & tail edge RTT', helpTitle: 'TCP Round-Trip Time',
            helpContent: 'Time taken for a data packet to travel from the Fastly edge node to the client and back.',
          },
          {
            title: 'TCP Retransmissions', icon: ArrowDownUp, loadingMessage: 'Calculating retransmission rate...',
            value: transport?.retrans_rate_pct != null ? <span className="font-mono">{transport.retrans_rate_pct.toFixed(2)}<span className="text-xs text-muted-foreground font-normal ml-0.5">%</span></span> : '—',
            sub: 'Average packet retransmit rate', helpTitle: 'TCP Retransmission Rate',
            helpContent: 'Percentage of TCP segments retransmitted due to network congestion or packet drop.',
          },
          {
            title: 'Packet Loss', icon: Wifi, loadingMessage: 'Evaluating packet loss...',
            value: transport?.packet_loss_pct != null ? <span className="font-mono">{transport.packet_loss_pct.toFixed(2)}<span className="text-xs text-muted-foreground font-normal ml-0.5">%</span></span> : '—',
            sub: 'Edge observed packet loss', helpTitle: 'Packet Loss Percentage',
            helpContent: 'Estimated packet loss between the client and Fastly edge cache nodes.',
          },
          {
            title: 'Delivery Rate', icon: Gauge, loadingMessage: 'Measuring delivery rate...',
            value: transport?.delivery_rate_mbps != null ? <span className="font-mono">{transport.delivery_rate_mbps.toFixed(1)}<span className="text-xs text-muted-foreground font-normal ml-1">Mbps</span></span> : '—',
            sub: 'Estimated client bandwidth', helpTitle: 'Delivery Throughput',
            helpContent: 'Paced delivery throughput achieved during response body transfer.',
          },
        ].map((card) => (
          <StatCard key={card.title} {...card} loading={isLoading} />
        ))}
      </div>

      {transport?.protocol_distribution && transport.protocol_distribution.length > 0 && (
        <AnalyticsCard
          title="Transport Protocol Share" helpTitle="Protocol Share"
          helpContent="Proportion of client requests served over modern HTTP protocols (HTTP/3, HTTP/2, HTTP/1.1)."
        >
          <div className="space-y-3 py-1">
            <div className="flex h-3 w-full rounded-full overflow-hidden bg-muted/40">
              {transport.protocol_distribution.map((p: TransportProtocolShare, idx: number) => (
                <div
                  key={p.protocol} className={`${PROTOCOL_COLORS[idx % PROTOCOL_COLORS.length]} transition-all duration-300`}
                  style={{ width: `${Math.max(p.share_pct, 1)}%` }}
                  title={`${p.protocol}: ${p.share_pct.toFixed(1)}% (${formatCompactCount(p.requests)} reqs)`}
                />
              ))}
            </div>
            <div className="flex flex-wrap items-center gap-4 text-xs">
              {transport.protocol_distribution.map((p: TransportProtocolShare, idx: number) => (
                <div key={p.protocol} className="flex items-center gap-1.5 font-mono">
                  <span className={`h-2.5 w-2.5 rounded-full ${PROTOCOL_COLORS[idx % PROTOCOL_COLORS.length]}`} />
                  <span className="font-semibold text-foreground">{p.protocol}</span>
                  <span className="text-muted-foreground">{p.share_pct.toFixed(1)}%</span>
                  <span className="text-muted-foreground/60 text-[10px]">({formatCompactCount(p.requests)})</span>
                </div>
              ))}
            </div>
          </div>
        </AnalyticsCard>
      )}

      {missingFields.length > 0 && (
        <div
          data-testid="missing-transport-fields-banner"
          className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-400 flex items-start gap-2.5"
        >
          <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
          <div className="flex-1">
            <span className="font-semibold">Optional transport fields not logged: </span>
            <code className="text-[11px] font-mono bg-amber-500/20 px-1 py-0.5 rounded">{missingFields.join(', ')}</code>
            . Enable these fields under{' '}
            <Link href="/admin" className="underline font-medium hover:text-amber-900 dark:hover:text-amber-200">
              Service Configuration
            </Link>{' '}
            to unlock edge TCP latency percentiles, packet retransmissions, and delivery rates.
          </div>
        </div>
      )}
    </div>
  )
}
