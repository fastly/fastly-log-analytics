'use client'

import React from 'react'
import Link from 'next/link'
import dynamic from 'next/dynamic'
import type { ColumnDef } from '@tanstack/react-table'
import {
  DataTable,
  ColumnVisibilityDropdown,
  FilterValueCell,
  PopLabel,
  columnHeader,
  msCell,
  numberValue,
  textValue,
} from '@/components/DataTable'
import { client, unwrapClient } from '@/lib/api'
import type { components, FiltersPayload } from '@/types/api'
import { useServiceQuery, type useColumnVisibility } from '@/hooks/useReportPageState'
import { downloadAsCsv, cn, formatCount, formatCompactCount } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import { AnalyticsCard, LazyMount, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { StatCard } from '@/components/ui/confirm-dialog'
import { Button } from '@/components/ui/button'
import {
  Network as NetworkIcon,
  AlertTriangle,
  ArrowDownUp,
  Gauge,
  Wifi,
  Activity,
  Shield,
  Download,
  Info,
} from 'lucide-react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ShieldingHelp } from './NetworkHelp'

const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then((mod) => mod.ShieldingMap), {
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
    return { ...row, low_sample: lowSample, anomaly_static: Boolean(row.anomaly_eligible) && !lowSample }
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

export const SHIELDING_COLUMNS: ColumnDef<ShieldingRowInput>[] = [
  {
    accessorKey: 'edge_pop', id: 'edge_pop', meta: { label: 'Edge POP' }, header: columnHeader('Edge POP'),
    cell: (info) => (
      <FilterValueCell
        filters={[{ column: 'pop', value: textValue(info.getValue()) }]}
        display={<PopLabel code={textValue(info.getValue())} />}
        className={cn('font-bold', info.row.original.anomaly_static ? 'text-destructive' : '')}
      />
    ),
  },
  {
    accessorKey: 'shield_pop', id: 'shield_pop', meta: { label: 'Shield POP' }, header: columnHeader('Shield POP'),
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
      return <span className="text-muted-foreground">{floor != null ? `${floor.toFixed(1)}ms` : '—'}</span>
    },
  },
  {
    accessorKey: 'efficiency_ratio', id: 'efficiency_ratio', meta: { label: 'Efficiency' }, header: columnHeader('Efficiency'),
    cell: (info) => {
      const ratio = numberValue(info.getValue())
      if (ratio == null) return <span className="text-muted-foreground">—</span>
      const lowSample = info.row.original.low_sample
      const cls = lowSample || ratio < 0 ? 'bg-muted text-muted-foreground' : ratio < 1.5 ? 'bg-green-500/10 text-green-600 dark:text-green-400' : ratio < 3 ? 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400' : 'bg-destructive/10 text-destructive'
      return <span title={lowSample ? 'Fewer than 30 requests — too few samples to grade; not flagged' : undefined} className={cn('inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-bold tabular-nums', cls)}>{ratio.toFixed(1)}×</span>
    },
  },
]

const SHIELDING_LABELS: Record<string, string> = Object.fromEntries(
  SHIELDING_COLUMNS.map((c) => [c.id!, (c.meta as { label: string }).label]),
)
export const getShieldingLabels = (ids: string[]) => ids.map((id) => ({ id, label: SHIELDING_LABELS[id] || id }))

const PROTOCOL_COLORS = ['bg-emerald-500', 'bg-sky-500', 'bg-amber-500', 'bg-purple-500', 'bg-rose-500'] as const

type TransportMetrics = components['schemas']['TransportMetrics']
type TransportProtocolShare = components['schemas']['TransportProtocolShare']

export function TransportSection({ activeServiceId, startTime, endTime, filterPayload }: {
  activeServiceId?: string | null; startTime?: string | null; endTime?: string | null; filterPayload?: FiltersPayload
}) {
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
    { enabled: Boolean(activeServiceId && startTime && endTime) },
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

type ShieldingAnalysisData = NonNullable<components['schemas']['NetworkHealthResponse']['shielding_analysis']>

export interface ShieldingSectionProps {
  shieldingData: ShieldingAnalysisData | null | undefined
  adjustedShieldingRows: ShieldingRowInput[]
  isShieldingLoadingInitial: boolean
  isFetching: boolean
  error: unknown
  minRequests: number
  setMinRequests: (val: number) => void
  shieldingVisibility: ReturnType<typeof useColumnVisibility>[0]
  setShieldingVisibility: ReturnType<typeof useColumnVisibility>[1]
  onShieldingVisChange: ReturnType<typeof useColumnVisibility>[2]
  isPridActive: boolean
}

export function ShieldingSection({
  shieldingData,
  adjustedShieldingRows,
  isShieldingLoadingInitial,
  isFetching,
  error,
  minRequests,
  setMinRequests,
  shieldingVisibility,
  setShieldingVisibility,
  onShieldingVisChange,
  isPridActive,
}: ShieldingSectionProps) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 [contain-intrinsic-size:460px] [content-visibility:auto]">
      <AnalyticsCard
        title="Edge → Shield Transit Map" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null}
      >
        <LazyMount minHeight={420}>
          <ShieldingMap
            rows={adjustedShieldingRows} isLoading={isShieldingLoadingInitial}
            edgeOnly={Boolean(shieldingData?.edge_only) && !isPridActive}
            errored={Boolean(shieldingData?.error)} expandable
          />
        </LazyMount>
      </AnalyticsCard>

      <AnalyticsCard
        title="Shielding Analysis" contentClassName="p-0" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null}
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
              <Shield className={cn('h-8 w-8 mb-2 opacity-20', shieldingData?.error ? 'text-destructive opacity-30' : 'text-muted-foreground')} />
              {shieldingData?.error ? (
                <>
                  <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
                  <p className="text-xs text-muted-foreground max-w-sm">The analysis failed to compute for this window (server-side error, not an absence of data). Try again or narrow the time range.</p>
                </>
              ) : (shieldingData?.edge_only && !isPridActive) ? (
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
  )
}
