'use client'

import React, { useState, type Dispatch, type SetStateAction } from 'react'
import Link from 'next/link'
import dynamic from 'next/dynamic'
import type { ColumnDef } from '@tanstack/react-table'
import { PopHealthHeatmap } from '@/components/network/PopHealthHeatmap'
import {
  DataTable,
  DataTableReadonly,
  ColumnVisibilityDropdown,
  FilterValueCell,
  PopLabel,
  columnHeader,
  msCell,
  numberValue,
  textValue,
} from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { client, unwrapClient } from '@/lib/api'
import type { components, FiltersPayload, PlotlyLayout, PlotlyTrace } from '@/types/api'
import { useServiceQuery, useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useDashboardCards'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { downloadAsCsv, cn, type RangeWireArgs } from '@/lib/utils'
import { formatCount, formatCompactCount } from '@/lib/format'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import { AnalyticsCard, LazyMount, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/confirm-dialog'
import { Button } from '@/components/ui/button'
import {
  Network as NetworkIcon,
  AlertCircle,
  AlertTriangle,
  ArrowDownUp,
  Gauge,
  Globe,
  Wifi,
  Zap,
  Activity,
  Shield,
  Download,
  Info,
} from 'lucide-react'
import { ReportLayout, type ReportLayoutRenderProps } from '@/components/ReportLayout'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

const PlotlyChart = dynamic(() => import('@/components/PlotlyChart').then(mod => mod.PlotlyChart), {
  ssr: false, loading: () => <Skeleton className="w-full h-[300px]" />,
})
const NetworkMap = dynamic(() => import('@/components/Map/NetworkMap').then(mod => mod.NetworkMap), {
  ssr: false, loading: () => <Skeleton className="w-full h-full min-h-[400px]" />,
})
const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then(mod => mod.ShieldingMap), {
  ssr: false, loading: () => <Skeleton className="w-full h-[420px] rounded-xl" />,
})

/** A route as the map and table consume it: derived flags may be absent until adjusted. */
export type ShieldingRowInput = Partial<components['schemas']['ShieldingRow']>

export function adjustShieldingRows(
  rows: readonly ShieldingRowInput[] | null | undefined,
  minRequests: number,
): ShieldingRowInput[] {
  if (!Array.isArray(rows)) return []
  return rows.map((row) => {
    const lowSample = minRequests > 0 && (row.requests ?? 0) < minRequests
    return {
      ...row,
      low_sample: lowSample,
      anomaly_static: Boolean(row.anomaly_eligible) && !lowSample,
    }
  })
}

export const SHIELDING_MIN_REQUESTS_OPTIONS: ReadonlyArray<{ value: string; label: string }> = [
  { value: '0', label: 'No minimum' },
  { value: '10', label: '10' },
  { value: '30', label: '30' },
  { value: '50', label: '50' },
  { value: '100', label: '100' },
]

export const SHIELDING_MIN_REQUESTS_DEFAULT = 10

function HelpSection({
  intro,
  items,
}: {
  intro: React.ReactNode
  items: Array<{ icon: React.ComponentType<{ className?: string }>; color: string; label: string; body: React.ReactNode }>
}) {
  return (
    <div className="space-y-4 text-sm text-muted-foreground">
      <p>{intro}</p>
      <ul className="space-y-3 list-none pl-0">
        {items.map(({ icon: Icon, color, label, body }) => (
          <li key={label} className="flex gap-3">
            <Icon className={cn("h-5 w-5 shrink-0 mt-0.5", color)} />
            <span><strong>{label}:</strong> {body}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

export const GlobalHealthHelp = () => (
  <HelpSection
    intro={<>A composite <strong>0–100 score</strong> aggregated across your top ASNs, weighted by request volume. 100 = perfect conditions, 0 = severe degradation.</>}
    items={[
      { icon: Activity, color: 'text-blue-500', label: "How it's calculated", body: <>Combines TCP RTT, packet loss (<code>ploss</code>), and RTT variance (jitter) into a single normalized score for each ASN, then takes a traffic-weighted average.</> },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'When to act', body: 'A score below 80 warrants investigation. A sustained drop often precedes visible user complaints or error-rate spikes by several minutes — making this a leading indicator.' },
    ]}
  />
)

export const AvgRttHelp = () => (
  <HelpSection
    intro={<>Mean <strong>TCP Round Trip Time</strong> in milliseconds across all logged requests in the selected time window.</>}
    items={[
      { icon: Zap, color: 'text-blue-500', label: 'Healthy baselines', body: '10–50ms for regional users, 100–200ms for international. Values above 300ms typically result in noticeable UX degradation.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Sudden increases', body: 'A 2× or greater spike often indicates ISP congestion, a Fastly routing change, or a DDoS volumetric attack saturating a link. Cross-reference with the ASN Leaderboard to isolate the source.' },
    ]}
  />
)

export const WorstAsnHelp = () => (
  <HelpSection
    intro={<>The <strong>Internet Service Provider or network</strong> (identified by Autonomous System Number) with the lowest composite health score during the selected window.</>}
    items={[
      { icon: NetworkIcon, color: 'text-blue-500', label: 'What causes this', body: "High RTT + packet loss from a specific ASN typically indicates a peering issue between that ISP and Fastly's edge nodes, or congestion on a transit link." },
      { icon: Activity, color: 'text-yellow-500', label: 'How to act', body: "Check the ASN Leaderboard to see how many requests this affects and whether the trend is degrading. If it's a large ISP with a worsening trend, Fastly support can investigate peering." },
    ]}
  />
)

export const WorstRegionHelp = () => (
  <HelpSection
    intro={<>The <strong>country experiencing the lowest composite network health score</strong>, based on TCP metrics from users in that geography.</>}
    items={[
      { icon: Globe, color: 'text-blue-500', label: 'ISP vs. regional', body: "If the same country appears in both this card and the Worst ASN, the issue is likely a single dominant ISP. If the ASN is healthy but the country isn't, a regional routing or infrastructure problem is more likely." },
      { icon: Activity, color: 'text-yellow-500', label: 'Follow-up', body: 'Use the Metro Leaderboard to narrow down to a specific city. A single underperforming city within an otherwise healthy country usually points to a specific datacenter or peering point.' },
    ]}
  />
)

export const HeatmapHelp = () => (
  <HelpSection
    intro={<>A grid where each <strong>row is an ISP (ASN)</strong>, each <strong>column is a time bucket</strong>, and the color represents the composite health score — red (0) to green (100).</>}
    items={[
      { icon: Activity, color: 'text-blue-500', label: 'Reading it', body: 'A row that turns red at a specific column pinpoints exactly when degradation started for a given ISP. Persistent red rows indicate long-standing peering or routing problems that predate your current time window.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Shared incidents', body: 'Multiple rows going red simultaneously suggests a widespread event — a major transit outage, a volumetric attack, or a Fastly-side issue rather than an ISP-specific problem.' },
    ]}
  />
)

export const AsnLeaderboardHelp = () => (
  <HelpSection
    intro={<>Rankings of every observed <strong>Internet Service Provider</strong> by composite health score, with tail-latency breakdowns and change signals.</>}
    items={[
      { icon: Zap, color: 'text-blue-500', label: 'P95 / P99 RTT', body: 'Tail latency — the slowest 5% and 1% of TCP handshakes for users on this network. High P99 with a healthy P95 means most users are fine but a small percentage are experiencing severe problems.' },
      { icon: Activity, color: 'text-yellow-500', label: '1h Change', body: 'Real-time signal — a negative value in red means the score is actively declining, often indicating an in-progress incident. A negative value in a currently-healthy ASN is worth watching.' },
      { icon: AlertCircle, color: 'text-red-500', label: 'Trend', body: 'Based on a longer trajectory than 1h Change. An ASN showing "DEGRADING" trend but a high current score may be slowly sliding toward a threshold breach.' },
    ]}
  />
)

export const MetroLeaderboardHelp = () => (
  <HelpSection
    intro={<>Health rankings segmented by <strong>city or metro area</strong> rather than ISP — useful when a carrier performs well nationally but has localized issues.</>}
    items={[
      { icon: Globe, color: 'text-blue-500', label: 'ISP vs. city', body: 'A low-scoring city with an otherwise-healthy ASN usually points to a specific datacenter, peering point, or submarine cable serving that metro. Large ISPs like Comcast or AT&T can have poor performance in one city while fine everywhere else.' },
      { icon: Activity, color: 'text-yellow-500', label: 'Requires metro logging', body: <>This panel only appears when <code>req.http.Fastly-Metro</code> or equivalent metro-level fields are present in your logs. If you don&apos;t see it, check your log field configuration.</> },
    ]}
  />
)

export const ShieldingHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Edge-to-Shield round-trip latency, isolated by subtracting the Shield-to-Origin fetch time from the Edge&apos;s total upstream wait. Requests are correlated via the <code>rid</code>/<code>prid</code> fields.</p>

    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Efficiency Legend</h4>
      <ul className="space-y-2 list-none pl-0">
        {[
          { cls: 'bg-[#22c55e]', title: '< 1.5× (Excellent):', text: 'Near-optimal transit performance.' },
          { cls: 'bg-[#eab308]', title: '1.5–3× (Moderate):', text: 'Acceptable but has room for improvement.' },
          { cls: 'bg-[#ef4444]', title: '> 3× (Investigate):', text: 'Significant latency overhead; check for suboptimal peering.' },
          { cls: 'bg-[#94a3b8]', title: 'Grey (Low sample):', text: 'Fewer than 30 requests — too few samples for the median/percentiles to be reliable. Shown for visibility but never colour-graded or flagged.' },
        ].map(({ cls, title, text }) => (
          <li key={title} className="flex items-center gap-2">
            <span className={cn("w-3 h-1 rounded-full", cls)} />
            <span><strong>{title}</strong> {text}</span>
          </li>
        ))}
      </ul>
    </div>

    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Map Markers & Arcs</h4>
      <ul className="space-y-2 list-none pl-0">
        {[
          { swatch: <span className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />, title: 'Blue Dot:', body: 'Edge POP (first arrival).' },
          { swatch: <span className="w-2.5 h-2.5 rounded-full bg-[#a855f7]" />, title: 'Purple Dot:', body: 'Shield POP.' },
          { swatch: null, title: 'Arc Width:', body: 'Proportional to request volume.' },
          { swatch: <span className="w-4 border-t-2 border-dashed border-[#ef4444]" />, title: 'Dashed Red Arc:', body: <>Flagged route — efficiency &gt; 3× <em>and</em> &ge; 20ms overhead above the light-speed floor, <em>and</em> at least 30 requests. Worth investigating for suboptimal peering.</> },
          { swatch: <span className="w-4 border-t-2 border-solid border-[#94a3b8]" />, title: 'Grey Arc:', body: 'Low-sample route (< 30 requests) — drawn for visibility but not colour-graded or flagged.' },
        ].map(({ swatch, title, body }) => (
          <li key={title} className="flex items-center gap-2">
            {swatch}
            <span><strong>{title}</strong> {body}</span>
          </li>
        ))}
      </ul>
    </div>
  </div>
)

export const NetworkQualityHelp = () => (
  <HelpSection
    intro={<>Breaks down <strong>average TCP Round Trip Time</strong> (<code>tcp_rtt</code>) by country, ASN, region, and Fastly POP so you can localise where latency is concentrated, plus an RTT-vs-TTFB sample to separate network latency from origin/edge processing time.</>}
    items={[
      { icon: NetworkIcon, color: 'text-blue-500', label: 'Bars (by country / ASN / region / POP)', body: 'mean RTT in ms for the busiest groups in the window. The region breakdown is scoped to the selected country.' },
      { icon: Zap, color: 'text-blue-500', label: 'RTT vs TTFB scatter', body: 'points far above the diagonal mean time-to-first-byte is dominated by something other than network RTT (origin or edge work); points hugging it are network-bound.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Requires Network Quality (Group F) fields', body: <><code>tcp_rtt</code> must be enabled in Fastly logging, with <code>ttfb</code> for the scatter. The cards hide when those fields aren&apos;t present.</> },
    ]}
  />
)

export function HealthBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-muted-foreground">—</span>
  const variant = score >= 80 ? 'outline' : score >= 50 ? 'secondary' : 'destructive'
  const color = score >= 80 ? 'text-green-600 dark:text-green-400' : score >= 50 ? 'text-yellow-600 dark:text-yellow-400' : ''
  return (
    <Badge variant={variant} className={cn("text-[10px]", color)}>
      {score.toFixed(0)}/100
    </Badge>
  )
}

export const SHIELDING_COLUMNS: ColumnDef<ShieldingRowInput>[] = [
  {
    accessorKey: 'edge_pop',
    id: 'edge_pop', meta: { label: 'Edge POP' }, header: columnHeader('Edge POP'),
    cell: (info) => (
      <FilterValueCell
        filters={[{ column: 'pop', value: textValue(info.getValue()) }]}
        display={<PopLabel code={textValue(info.getValue())} />}
        className={cn('font-bold', info.row.original.anomaly_static ? 'text-destructive' : '')}
      />
    ),
  },
  {
    accessorKey: 'shield_pop',
    id: 'shield_pop', meta: { label: 'Shield POP' }, header: columnHeader('Shield POP'),
    cell: (info) => (
      <FilterValueCell
        filters={[{ column: 'shield_pop', value: textValue(info.getValue()) }]}
        display={<PopLabel code={textValue(info.getValue())} />}
        className="font-bold text-purple-500"
      />
    ),
  },
  {
    accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: columnHeader('Reqs'),
    cell: (info) => {
      const lowSample = info.row.original.low_sample
      return (
        <span
          className={cn('tabular-nums', lowSample ? 'text-muted-foreground' : '')}
          title={lowSample ? 'Fewer than 30 requests — percentiles unreliable, route not flagged' : undefined}
        >
          {formatCount(numberValue(info.getValue()) ?? 0)}
        </span>
      )
    },
  },
  ...([
    ['p50_ms', 'Median (P50)', 'P50 (E→S)'],
    ['p95_ms', 'P95 Latency', 'P95 (E→S)'],
    ['p99_ms', 'P99 Latency', 'P99 (E→S)'],
  ] as const).map(([key, label, hdr]): ColumnDef<ShieldingRowInput> => ({
    accessorKey: key, id: key, meta: { label }, header: columnHeader(hdr), cell: (info) => msCell(info.getValue()),
  })),
  {
    accessorKey: 'light_speed_rtt_ms', id: 'light_speed_rtt_ms', meta: { label: 'Light-Speed Floor' }, header: columnHeader('Light Floor'),
    cell: (info) => {
      const floor = numberValue(info.getValue())
      return floor != null ? <span className="text-muted-foreground">{floor.toFixed(1)}ms</span> : <span className="text-muted-foreground">—</span>
    },
  },
  {
    accessorKey: 'efficiency_ratio', id: 'efficiency_ratio', meta: { label: 'Efficiency' }, header: columnHeader('Efficiency'),
    cell: (info) => {
      const ratio = numberValue(info.getValue())
      if (ratio == null) return <span className="text-muted-foreground">—</span>
      const lowSample = info.row.original.low_sample
      const cls = lowSample || ratio < 0 ? 'bg-muted text-muted-foreground' : ratio < 1.5 ? 'bg-green-500/10 text-green-600 dark:text-green-400' : ratio < 3 ? 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400' : 'bg-destructive/10 text-destructive'
      return <span title={lowSample ? 'Fewer than 30 requests — too few samples to grade; not flagged' : undefined} className={cn("inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-bold tabular-nums", cls)}>{ratio.toFixed(1)}×</span>
    },
  },
]

const SHIELDING_LABELS: Record<string, string> = Object.fromEntries(
  SHIELDING_COLUMNS.map((c) => [c.id!, (c.meta as { label: string }).label]),
)
export const getShieldingLabels = (ids: string[]) => ids.map(id => ({ id, label: SHIELDING_LABELS[id] || id }))

const PROTOCOL_COLORS = ['bg-emerald-500', 'bg-sky-500', 'bg-amber-500', 'bg-purple-500', 'bg-rose-500'] as const

type TransportMetrics = components['schemas']['TransportMetrics']
type TransportProtocolShare = components['schemas']['TransportProtocolShare']

interface TransportSectionProps {
  activeServiceId?: string | null; startTime?: string | null; endTime?: string | null; filterPayload?: FiltersPayload
}

export function TransportSection({ activeServiceId, startTime, endTime, filterPayload }: TransportSectionProps) {
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
            <NetworkIcon className="h-4 w-4" />
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
            <code className="text-[11px] font-mono bg-amber-500/20 px-1 py-0.5 rounded">{missingFields.join(', ')}</code>
            . Enable these fields under{' '}
            <Link href="/service-config" className="underline font-medium hover:text-amber-900 dark:hover:text-amber-200">
              Service Configuration
            </Link>{' '}
            to unlock edge TCP latency percentiles, packet retransmissions, and delivery rates.
          </div>
        </div>
      )}

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
          className="[contain-intrinsic-size:120px] [content-visibility:auto]"
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
    </div>
  )
}

const HEATMAP_LAYOUT: PlotlyLayout = { xaxis: { tickangle: -45 } }
const QUALITY_SCATTER_LAYOUT: PlotlyLayout = { xaxis: { title: { text: 'TCP RTT (ms)' } }, yaxis: { title: { text: 'TTFB (ms)' } } }

type QualityBarRow = components['schemas']['NetworkQualityBarRow']
type ColumnVisibilityTuple = ReturnType<typeof useColumnVisibility>
type AsnRow = components['schemas']['NetworkLeaderboardRow']
type MetroRow = components['schemas']['NetworkMetroRow']

const rttMs = (us: number | null | undefined) => (us != null ? `${(us / 1000).toFixed(1)}ms` : '—')
const healthDelta = (a: AsnRow) => (a.health_score_now ?? 0) - (a.health_score_1h_ago ?? 0)

const ASN_COLUMNS: ColumnDef<AsnRow>[] = [
  {
    id: 'asn', header: 'ASN', accessorFn: (a) => a.label, size: 240,
    cell: ({ row }) => (
      <span className="text-xs font-medium">
        <FilterValueCell filters={[{ column: 'asn', value: String(row.original.asn) }]} display={row.original.label} />
      </span>
    ),
  },
  {
    id: 'requests', header: 'Requests', accessorFn: (a) => a.total_reqs ?? 0, size: 110,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (a) => a.health_score_now ?? -1, size: 120,
    cell: ({ row }) => <span className="block text-right"><HealthBadge score={row.original.health_score_now ?? null} /></span>,
  },
  {
    id: 'p95', header: 'P95 RTT', accessorFn: (a) => a.p95_rtt_us ?? -1, size: 100,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{rttMs(row.original.p95_rtt_us)}</span>,
  },
  {
    id: 'p99', header: 'P99 RTT', accessorFn: (a) => a.p99_rtt_us ?? -1, size: 100,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{rttMs(row.original.p99_rtt_us)}</span>,
  },
  {
    id: 'delta', header: '1h Change', accessorFn: healthDelta, size: 110,
    cell: ({ row }) => {
      const delta = healthDelta(row.original)
      return (
        <span className="block text-right font-mono text-xs">
          <span className={delta > 0 ? 'text-green-500' : delta < 0 ? 'text-red-500' : 'text-muted-foreground'}>
            {delta > 0 ? '+' : ''}{delta.toFixed(1)}
          </span>
        </span>
      )
    },
  },
  {
    id: 'trend', header: 'Trend', accessorFn: (a) => a.trend ?? 'stable', size: 110,
    cell: ({ row }) => (
      <span className="block text-right">
        <Badge
          variant={row.original.trend === 'degrading' ? 'destructive' : 'outline'}
          className={cn('text-[10px]', row.original.trend === 'improving' ? 'text-green-600 dark:text-green-400 border-green-300 dark:border-green-700' : '')}
        >
          {(row.original.trend ?? 'stable').toUpperCase()}
        </Badge>
      </span>
    ),
  },
]

const METRO_COLUMNS: ColumnDef<MetroRow>[] = [
  {
    id: 'city', header: 'Metro / City', accessorFn: (m) => m.city, size: 260,
    cell: ({ row }) => {
      const m = row.original
      return (
        <span className="text-xs font-medium">
          {m.raw_city ? (
            <FilterValueCell
              filters={[
                { column: 'city', value: m.raw_city },
                ...(m.region ? [{ column: 'region', value: m.region }] : []),
                ...(m.country ? [{ column: 'country', value: m.country }] : []),
              ]}
              display={m.city}
            />
          ) : <span>{m.city}</span>}
        </span>
      )
    },
  },
  {
    id: 'requests', header: 'Requests', accessorFn: (m) => m.total_reqs ?? 0, size: 110,
    cell: ({ row }) => <span className="block text-right font-mono text-xs">{formatCount(row.original.total_reqs ?? 0)}</span>,
  },
  {
    id: 'health', header: 'Health Score', accessorFn: (m) => m.health_score ?? -1, size: 120,
    cell: ({ row }) => <span className="block text-right"><HealthBadge score={row.original.health_score ?? null} /></span>,
  },
]

export function AsnLeaderboardTable({ rows }: { rows: AsnRow[] }) {
  return <DataTableReadonly columns={ASN_COLUMNS} data={rows} hideToolbar showPagination={false} tableCaption="ASN performance leaderboard" />
}

export function MetroLeaderboardTable({ rows }: { rows: MetroRow[] }) {
  return <DataTableReadonly columns={METRO_COLUMNS} data={rows} hideToolbar showPagination={false} tableCaption="Metro and region leaderboard" />
}

function RttBarList({ rows, filterColumn }: { rows: readonly QualityBarRow[]; filterColumn?: string }) {
  if (!rows?.length) {
    return <div className="p-6 text-center text-sm text-muted-foreground">No data in this range</div>
  }
  const max = Math.max(...rows.map((r) => r.rtt_ms), 1)
  return (
    <div className="space-y-1.5 p-3">
      {rows.map((r) => {
        const filterVal = r.value ?? r.label
        const display = filterColumn === 'pop' ? <PopLabel code={filterVal} /> : r.label
        return (
          <div key={filterVal} className="flex items-center gap-2 text-xs">
            <div className="w-48 truncate shrink-0" title={r.label}>
              {filterColumn ? (
                <FilterValueCell filters={[{ column: filterColumn, value: filterVal }]} display={display} />
              ) : (
                <span>{display}</span>
              )}
            </div>
            <div className="flex-1 h-4 bg-muted/40 rounded-sm overflow-hidden">
              <div className="h-full bg-primary/70" style={{ width: `${(r.rtt_ms / max) * 100}%` }} />
            </div>
            <div className="w-16 text-right font-mono shrink-0">{r.rtt_ms.toFixed(1)}ms</div>
            <div className="w-16 text-right font-mono text-muted-foreground shrink-0">{formatCount(r.reqs ?? 0)}</div>
          </div>
        )
      })}
    </div>
  )
}

function LeaderboardStatusBody<T>({
  isLoading,
  rows,
  isActiveField,
  emptyActiveMsg,
  emptyInactiveMsg,
  children,
}: {
  isLoading: boolean
  rows: readonly T[] | undefined
  isActiveField: boolean
  emptyActiveMsg: string
  emptyInactiveMsg: string
  children: (rows: readonly T[]) => React.ReactNode
}) {
  if (isLoading) {
    return (
      <div className="space-y-2 p-4">
        <SkeletonGrid count={5} height="48px" className="rounded-md" />
      </div>
    )
  }
  if (!rows || rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        <p className="mb-1">No data available</p>
        <p className="text-[10px] opacity-70">{isActiveField ? emptyActiveMsg : emptyInactiveMsg}</p>
      </div>
    )
  }
  return <>{children(rows)}</>
}

type NetworkSections = NonNullable<components['schemas']['NetworkHealthRequest']['sections']>
const NETWORK_CORE_SECTIONS: NetworkSections = ['summary', 'leaderboard', 'metro_leaderboard', 'heatmap', 'buckets', 'cities', 'map_buckets']
const NETWORK_MAP_SECTIONS: NetworkSections = ['cities', 'map_buckets']
const NETWORK_SHIELDING_SECTIONS: NetworkSections = ['shielding_analysis']

export default function NetworkPage() {
  const [metric, setMetric] = useState('health_score')
  const [mapAsn, setMapAsn] = useState('all')
  const [animBucketSeconds, setAnimBucketSeconds] = useState(3600)
  const [regionCountry, setRegionCountry] = useState('US')
  const [shieldingVisibility, setShieldingVisibility, onShieldingVisChange] = useColumnVisibility()
  const [minRequests, setMinRequests] = useState(SHIELDING_MIN_REQUESTS_DEFAULT)
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()

  return (
    <ReportLayout
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
      icon={NetworkIcon}
    >
      {(ctx) => (
        <NetworkBody
          {...ctx} metric={metric} setMetric={setMetric} mapAsn={mapAsn} setMapAsn={setMapAsn}
          animBucketSeconds={animBucketSeconds} setAnimBucketSeconds={setAnimBucketSeconds}
          regionCountry={regionCountry} setRegionCountry={setRegionCountry}
          shieldingVisibility={shieldingVisibility} setShieldingVisibility={setShieldingVisibility}
          onShieldingVisChange={onShieldingVisChange} minRequests={minRequests} setMinRequests={setMinRequests}
          relativeRange={relativeRange} isAutoRange={isAutoRange} anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}

type NetworkBodyProps = ReportLayoutRenderProps &
  Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> & {
    metric: string; setMetric: Dispatch<SetStateAction<string>>
    mapAsn: string; setMapAsn: Dispatch<SetStateAction<string>>
    animBucketSeconds: number; setAnimBucketSeconds: Dispatch<SetStateAction<number>>
    regionCountry: string; setRegionCountry: Dispatch<SetStateAction<string>>
    shieldingVisibility: ColumnVisibilityTuple[0]; setShieldingVisibility: ColumnVisibilityTuple[1]; onShieldingVisChange: ColumnVisibilityTuple[2]
    minRequests: number; setMinRequests: Dispatch<SetStateAction<number>>
  }

function NetworkBody({
  startTime, endTime, activeServiceId, filterPayload, metric, setMetric, mapAsn, setMapAsn,
  animBucketSeconds, setAnimBucketSeconds, regionCountry, setRegionCountry,
  shieldingVisibility, setShieldingVisibility, onShieldingVisChange,
  minRequests, setMinRequests, relativeRange, isAutoRange, anchor,
}: NetworkBodyProps) {
  const { isFieldActive } = useActiveLogFields()
  const range = { relativeRange, isAutoRange, startTime, endTime, anchor }

  const postNetworkHealth = (sections: NetworkSections, rangeToken: string | null, signal: AbortSignal | undefined, targetAsn = mapAsn) =>
    unwrapClient(client.POST("/api/network-health", {
      signal,
      body: {
        start_time: startTime!, end_time: endTime!, filters: filterPayload, metric,
        bucket_seconds: animBucketSeconds, top_n: 30, map_asn: targetAsn, sections, range_token: rangeToken, anchor,
      }
    }))

  const coreQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'core'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds, metric],
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_CORE_SECTIONS, rangeToken, signal, 'all'),
  })

  const mapQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'map'], serviceId: activeServiceId, filterPayload, range,
    keyParts: [animBucketSeconds, mapAsn, metric],
    queryOptions: { enabled: mapAsn !== 'all' },
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_MAP_SECTIONS, rangeToken, signal, mapAsn),
  })

  const shieldingQuery = useAnalyticsReportQuery({
    namespace: ['network', 'health', 'shielding'], serviceId: activeServiceId, filterPayload, range,
    queryFn: ({ rangeToken, signal }) => postNetworkHealth(NETWORK_SHIELDING_SECTIONS, rangeToken, signal, mapAsn),
  })

  const qualityQuery = useServiceQuery(
    ['network', 'quality', activeServiceId, startTime, endTime, filterPayload, regionCountry],
    async ({ signal }) => unwrapClient(client.POST("/api/network-quality", {
      signal, body: { start_time: startTime!, end_time: endTime!, filters: filterPayload, region_country: regionCountry }
    })),
  )

  const data = coreQuery.data
  const mapData = (mapAsn === 'all' ? coreQuery.data : mapQuery.data) ?? coreQuery.data
  const qualityData = qualityQuery.data
  const isCoreLoadingInitial = coreQuery.isLoading || (coreQuery.isFetching && !coreQuery.data)
  const isMapLoadingInitial = mapAsn === 'all' ? isCoreLoadingInitial : mapQuery.isLoading || (mapQuery.isFetching && !mapQuery.data)
  const isShieldingLoadingInitial = shieldingQuery.isLoading || (shieldingQuery.isFetching && !shieldingQuery.data)
  const isQualityLoadingInitial = qualityQuery.isLoading || (qualityQuery.isFetching && !qualityQuery.data)

  const shieldingData = shieldingQuery.data?.shielding_analysis
  const adjustedShieldingRows = React.useMemo(
    () => adjustShieldingRows(shieldingData?.rows ?? [], minRequests),
    [shieldingData?.rows, minRequests],
  )

  const qualityScatterTrace = React.useMemo((): PlotlyTrace[] => {
    const pts = qualityData?.scatter ?? []
    if (!pts.length) return []
    return [{
      type: 'scattergl', mode: 'markers', x: pts.map((p) => p.rtt_ms), y: pts.map((p) => p.ttfb_ms),
      marker: { size: 4, opacity: 0.45 }, hovertemplate: 'RTT %{x}ms<br>TTFB %{y}ms<extra></extra>',
    }]
  }, [qualityData?.scatter])

  const leaderboard = data?.leaderboard
  const asnOptions = React.useMemo(() => leaderboard ? leaderboard.map((a) => ({ value: String(a.asn), label: a.label })) : [], [leaderboard])

  const heatmapRows = data?.heatmap
  const heatmapBuckets = data?.buckets
  const heatmapData = React.useMemo(() => {
    if (!heatmapRows?.length || !heatmapBuckets?.length) return null
    const yLabels = heatmapRows.map((d) => d.label)
    const xBuckets = heatmapBuckets
    const z = heatmapRows.map((row) => {
      const byBucket: Record<string, number | null> = {}
      row.buckets?.forEach((b) => { byBucket[b.bucket] = b.health_score })
      return xBuckets.map((bk) => byBucket[bk] ?? null)
    })
    return { x: xBuckets, y: yLabels, z }
  }, [heatmapRows, heatmapBuckets])

  const heatmapTrace = React.useMemo((): PlotlyTrace[] => (
    heatmapData ? [{
      type: 'heatmap', x: heatmapData.x, y: heatmapData.y, z: heatmapData.z,
      colorscale: 'RdYlGn', zmin: 0, zmax: 100, colorbar: { title: 'Score', thickness: 12 },
      hovertemplate: '<b>%{y}</b><br>%{x}<br>Score: %{z}<extra></extra>',
    }] : []
  ), [heatmapData])

  if (data?.available === false) {
    return (
      <div className="flex flex-col items-center justify-center h-[50vh] text-center max-w-md mx-auto">
        <AlertCircle className="h-10 w-10 text-yellow-500 mb-4" />
        <h2 className="text-xl font-semibold">Network Metrics Unavailable</h2>
        <p className="text-muted-foreground mt-2">{data.reason || 'Log fields required for network analysis are not enabled.'}</p>
      </div>
    )
  }

  const summary = data?.summary

  const qualityBreakdowns: Array<{
    title: string; loadingMessage: string; rows: readonly QualityBarRow[]; filterColumn: string; headerAction?: React.ReactNode
  }> = [
    { title: 'Avg RTT by Country', loadingMessage: 'Analyzing RTT by country...', rows: qualityData?.by_country ?? [], filterColumn: 'country' },
    { title: 'Avg RTT by ASN', loadingMessage: 'Analyzing RTT by ASN...', rows: qualityData?.by_asn ?? [], filterColumn: 'asn' },
    {
      title: 'Avg RTT by Region', loadingMessage: 'Analyzing RTT by region...', rows: qualityData?.by_region ?? [], filterColumn: 'region',
      headerAction: (
        <select
          aria-label="Region breakdown country" value={regionCountry}
          onChange={(e) => setRegionCountry(e.target.value)} className="h-7 rounded-md border bg-background px-2 text-xs"
        >
          {(qualityData?.countries ?? []).length === 0 && <option value={regionCountry}>{regionCountry}</option>}
          {(qualityData?.countries ?? []).map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      ),
    },
    { title: 'Avg RTT by POP', loadingMessage: 'Analyzing RTT by POP...', rows: qualityData?.by_pop ?? [], filterColumn: 'pop' },
  ]

  return (
    <>
      {coreQuery.error && !coreQuery.data && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 text-destructive p-3 text-sm flex items-start gap-2">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Network health unavailable</div>
            <div className="text-xs opacity-80">
              {coreQuery.error instanceof Error ? coreQuery.error.message : 'The backend returned an error. Leaderboards below may show incorrect "fields not enabled" hints.'}
            </div>
          </div>
        </div>
      )}
      <NetworkMap
        data={mapData} isLoading={isMapLoadingInitial}
        error={mapAsn === 'all' ? coreQuery.error : mapQuery.error}
        metric={metric} onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds} onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn} onAsnChange={setMapAsn} asnOptions={asnOptions}
      />

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 [contain-intrinsic-size:120px] [content-visibility:auto]">
        {[
          {
            title: 'Global Health', icon: Activity, loadingMessage: 'Compiling ASN health matrix...',
            value: <>{summary?.global_health_score ?? '--'}/100</>, sub: 'Average across top ASNs',
            helpTitle: 'Global Health Score', helpContent: <GlobalHealthHelp />,
          },
          {
            title: 'Avg RTT', icon: Zap, loadingMessage: 'Analyzing TCP round-trip times...',
            value: <>{summary?.avg_rtt_ms ?? '--'}ms</>, sub: 'Across all regions',
            helpTitle: 'Average RTT', helpContent: <AvgRttHelp />,
          },
          {
            title: 'Worst ASN', icon: AlertCircle, iconClassName: 'text-red-500', loadingMessage: 'Scanning ASN degradation...',
            value: <span className="text-sm truncate block">{summary?.worst_asn?.label ?? '--'}</span>,
            sub: <span className="text-red-500 font-medium">Score: {summary?.worst_asn?.score ?? '--'}</span>,
            helpTitle: 'Worst-Performing ASN', helpContent: <WorstAsnHelp />,
          },
          {
            title: 'Worst Region', icon: Globe, loadingMessage: 'Evaluating regional latency...',
            value: <span className="text-sm truncate block">{summary?.worst_country?.label ?? '--'}</span>,
            sub: <span className="text-red-500 font-medium">Score: {summary?.worst_country?.score ?? '--'}</span>,
            helpTitle: 'Worst-Performing Region', helpContent: <WorstRegionHelp />,
          },
        ].map((card) => (
          <StatCard key={card.title} {...card} loading={isCoreLoadingInitial} className="min-h-[110px]" />
        ))}
      </div>

      <TransportSection activeServiceId={activeServiceId} startTime={startTime} endTime={endTime} filterPayload={filterPayload} />

      <div className="my-6 [contain-intrinsic-size:280px] [content-visibility:auto]">
        {activeServiceId && <PopHealthHeatmap serviceId={activeServiceId} startTime={startTime} endTime={endTime} />}
      </div>

      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time" helpTitle="ASN Health Score over Time" helpContent={<HeatmapHelp />}
          isFetching={coreQuery.isFetching} error={coreQuery.error as AnalyticsCardError | null}
          className="[contain-intrinsic-size:380px] [content-visibility:auto]"
        >
          <PlotlyChart data={heatmapTrace} layout={HEATMAP_LAYOUT} height={Math.min(60 + heatmapData.y.length * 28, 600)} />
        </AnalyticsCard>
      )}

      {(shieldingData || isShieldingLoadingInitial || shieldingQuery.error) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:460px] [content-visibility:auto]">
          <AnalyticsCard
            title="Edge → Shield Transit Map" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching} error={shieldingQuery.error as AnalyticsCardError | null}
          >
            <LazyMount minHeight={420}>
              <ShieldingMap
                rows={adjustedShieldingRows} isLoading={isShieldingLoadingInitial}
                edgeOnly={Boolean(shieldingData?.edge_only) && !isFieldActive('prid')}
                errored={Boolean(shieldingData?.error)} expandable
              />
            </LazyMount>
          </AnalyticsCard>

          <AnalyticsCard
            title="Shielding Analysis" contentClassName="p-0" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching} error={shieldingQuery.error as AnalyticsCardError | null}
            headerAction={
              <div className="flex items-center gap-2">
                <div className="flex items-center gap-1.5">
                  <span className="hidden sm:inline text-[11px] uppercase font-semibold tracking-wide text-muted-foreground">Min reqs</span>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger render={<span className="relative inline-flex items-center text-muted-foreground hover:text-foreground transition-colors cursor-help" />}>
                        <Info className="w-3.5 h-3.5 opacity-60" />
                        <span className="sr-only">What does the minimum requests floor do?</span>
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-[260px] text-xs leading-relaxed">
                        Routes with fewer than this many edge→shield requests are greyed and never flagged as “suboptimal peering” — their transit median is too small a sample to trust. Raise it on busy services; lower it (or pick “No minimum”) to scrutinize quiet routes.
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                  <Select value={String(minRequests)} onValueChange={(v) => v && setMinRequests(Number(v))}>
                    <SelectTrigger className="h-8 w-[124px] text-xs" aria-label="Minimum requests to flag a shielding route"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  variant="outline" size="sm" className="h-8" disabled={!shieldingData?.has_data}
                  onClick={() => {
                    if (!adjustedShieldingRows.length) return
                    downloadAsCsv(adjustedShieldingRows, ['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'distance_km', 'light_speed_rtt_ms', 'efficiency_ratio', 'anomaly_static', 'low_sample'], 'shielding_analysis.csv')
                  }}
                >
                  <Download className="w-3.5 h-3.5 mr-2" />Export
                </Button>
                <ColumnVisibilityDropdown
                  columns={getShieldingLabels(['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'light_speed_rtt_ms', 'efficiency_ratio'])}
                  visibility={shieldingVisibility} onChange={onShieldingVisChange}
                />
              </div>
            }
          >
            <div className="flex flex-col h-[420px] overflow-hidden">
              {shieldingData?.has_data ? (
                <>
                  <DataTable
                    columns={SHIELDING_COLUMNS} data={adjustedShieldingRows} hideToolbar
                    columnVisibility={shieldingVisibility} onColumnVisibilityChange={setShieldingVisibility}
                    tableContainerClassName="max-h-[295px]"
                  />
                  {shieldingData?.truncated && (
                    <p className="px-4 py-2 text-[11px] text-muted-foreground border-t bg-muted/20">
                      Showing {formatCount(shieldingData?.rows?.length ?? 0)} of {formatCount(Number(shieldingData.total_routes ?? 0))} routes — top by request volume and by transit overhead.
                    </p>
                  )}
                </>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 h-full text-center px-4 gap-1">
                  <Shield className={cn("h-8 w-8 mb-2 opacity-20", shieldingData?.error ? 'text-destructive opacity-30' : 'text-muted-foreground')} />
                  {shieldingData?.error ? (
                    <>
                      <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
                      <p className="text-xs text-muted-foreground max-w-sm">The analysis failed to compute for this window (server-side error, not an absence of data). Try again or narrow the time range.</p>
                    </>
                  ) : (shieldingData?.edge_only && !isFieldActive('prid')) ? (
                    <>
                      <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
                      <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Your service is currently logging edge requests only — enable full (non-edge-only) logging to use this analysis.</p>
                    </>
                  ) : shieldingData?.requires_fields ? (
                    <p className="text-sm text-muted-foreground italic">Required fields missing: {shieldingData.requires_fields.join(', ')}</p>
                  ) : (
                    <p className="text-sm text-muted-foreground italic">No shielding data detected in this time range.</p>
                  )}
                </div>
              )}
            </div>
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:420px] [content-visibility:auto]">
        <AnalyticsCard
          title="ASN Performance Leaderboard" className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
          contentClassName="p-0 flex-1" helpTitle="ASN Performance Leaderboard" helpContent={<AsnLeaderboardHelp />}
          isFetching={coreQuery.isFetching} error={coreQuery.error as AnalyticsCardError | null} loadingMessage="Compiling ASN leaderboard..."
        >
          <LeaderboardStatusBody
            isLoading={isCoreLoadingInitial} rows={data?.leaderboard} isActiveField={isFieldActive('asn')}
            emptyActiveMsg="No ASN data in this time range."
            emptyInactiveMsg="Requires Network Quality (Group F) fields to be enabled in Fastly logging."
          >
            {(rows) => <AsnLeaderboardTable rows={[...rows]} />}
          </LeaderboardStatusBody>
        </AnalyticsCard>

        {data?.has_metro && (
          <AnalyticsCard
            title="Metro / Region Leaderboard" className="flex flex-col [contain-intrinsic-size:420px] [content-visibility:auto]"
            contentClassName="p-0 flex-1" helpTitle="Metro / Region Leaderboard" helpContent={<MetroLeaderboardHelp />}
            isFetching={coreQuery.isFetching} error={coreQuery.error as AnalyticsCardError | null} loadingMessage="Compiling Metro leaderboard..."
          >
            <LeaderboardStatusBody
              isLoading={isCoreLoadingInitial} rows={data?.metro_leaderboard} isActiveField={isFieldActive('metro')}
              emptyActiveMsg="No metro data in this time range."
              emptyInactiveMsg="Requires Geolocation (Group D/E) fields to be enabled in Fastly logging."
            >
              {(rows) => <MetroLeaderboardTable rows={[...rows]} />}
            </LeaderboardStatusBody>
          </AnalyticsCard>
        )}
      </div>

      {(isQualityLoadingInitial || qualityData?.available || qualityQuery.error) && (
        <div className="space-y-4 [contain-intrinsic-size:400px] [content-visibility:auto]">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Network Quality</h2>
          {isQualityLoadingInitial ? (
            <SkeletonGrid count={4} height="180px" className="rounded-xl" />
          ) : (
            <>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {qualityBreakdowns.map((b) => (
                  <AnalyticsCard
                    key={b.title} title={b.title} contentClassName="p-0" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
                    isFetching={qualityQuery.isFetching} error={qualityQuery.error as AnalyticsCardError | null}
                    loadingMessage={b.loadingMessage} className="[contain-intrinsic-size:240px] [content-visibility:auto]" headerAction={b.headerAction}
                  >
                    <RttBarList rows={b.rows} filterColumn={b.filterColumn} />
                  </AnalyticsCard>
                ))}
              </div>
              {qualityScatterTrace.length > 0 && (
                <AnalyticsCard
                  title="RTT vs TTFB" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching} error={qualityQuery.error as AnalyticsCardError | null}
                  loadingMessage="Plotting RTT vs TTFB correlation..." className="[contain-intrinsic-size:380px] [content-visibility:auto]"
                >
                  <PlotlyChart data={qualityScatterTrace} layout={QUALITY_SCATTER_LAYOUT} height={360} />
                </AnalyticsCard>
              )}
            </>
          )}
        </div>
      )}
    </>
  )
}
