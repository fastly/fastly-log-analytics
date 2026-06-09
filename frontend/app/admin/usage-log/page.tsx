'use client'

import React, { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { DataTable } from '@/components/DataTable/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { PageHeader } from '@/components/ui/page-header'
import { StatCard } from '@/components/ui/stat-card'
import { ArrowLeft, Download, Database, Zap, Globe, DollarSign, Settings, Trash2, RefreshCw, AlertTriangle, Loader2 } from 'lucide-react'
import { buttonVariants } from '@/components/ui/button'
import { useRouter } from 'next/navigation'
import { useDateFormat } from '@/hooks/useDateFormat'
import { formatBytes } from '@/lib/utils'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'

type UsageLogEntry = {
  id: number
  timestamp: string
  service_id: string | null
  operation_class: string | null
  operation_type: string | null
  url: string | null
  bytes: number | null
  duration_ms: number | null
  function_name: string | null
  process_context: string | null
  status: string | null
  estimated_cost: number | null
}

type UsageLogAggregate = {
  total_class_a: number
  total_class_b: number
  total_cdn_downloads: number
  total_cdn_bytes: number
  total_fos_bytes: number
  estimated_cost_class_a: number
  estimated_cost_class_b: number
  estimated_cost_cdn: number
  estimated_cost_total: number
  class_a_breakdown: Record<string, number>
  class_b_breakdown: Record<string, number>
}

const DATE_PRESETS = [
  { label: 'Last 1h', hours: 1 },
  { label: 'Last 24h', hours: 24 },
  { label: 'Last 7d', hours: 168 },
  { label: 'Last 30d', hours: 720 },
]

function toQueryDate(d: Date): string {
  return d.toISOString().slice(0, 19) + 'Z'
}

function fmtCost(n: number): string {
  if (n === 0) return '$0.000000'
  if (n < 0.000001) return `$${n.toExponential(2)}`
  return `$${n.toFixed(6)}`
}

function fmtOps(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString()
}

const LOG_ACCOUNTING_PRESETS = [
  { label: 'Last 1h', hours: 1, by: 'hour' as const },
  { label: 'Last 24h', hours: 24, by: 'hour' as const },
  { label: 'Last 7d', hours: 168, by: 'day' as const },
  { label: 'Last 30d', hours: 720, by: 'day' as const },
]

function LogAccountingPanel() {
  const [presetIdx, setPresetIdx] = useState(1)
  const preset = LOG_ACCOUNTING_PRESETS[presetIdx]
  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['log-accounting', preset.hours, preset.by],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/log-accounting', { signal, 
        params: { query: { hours: preset.hours, by: preset.by } },
      })
      if (error) throw new Error(extractApiError(error))
      return data
    },
    staleTime: 30_000,
    refetchInterval: 60_000,
  })

  const totals = data?.totals
  const buckets = data?.buckets ?? []
  const gapPct = totals?.gap_pct ?? 0
  const gapAbsPct = Math.abs(gapPct) * 100
  const gapColor =
    gapAbsPct <= 0.1 ? 'text-green-600 dark:text-green-400'
    : gapAbsPct <= 1 ? 'text-yellow-600 dark:text-yellow-400'
    : 'text-destructive'

  const catchup = data?.catchup
  const catchupBadge = useMemo(() => {
    if (!catchup) return null
    const fmtLag = (s: number | null | undefined) => {
      if (s == null) return ''
      if (s < 60) return `${s}s ago`
      if (s < 3600) return `${Math.floor(s / 60)}m ago`
      if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ago`
      return `${Math.floor(s / 86400)}d ago`
    }
    const palette: Record<string, { label: string; dot: string; text: string }> = {
      caught_up: { label: 'Caught up', dot: 'bg-green-500', text: 'text-green-700 dark:text-green-400' },
      backfilling: { label: `Backfilling${catchup.lag_seconds != null ? ` · ${fmtLag(catchup.lag_seconds)}` : ''}`, dot: 'bg-yellow-500', text: 'text-yellow-700 dark:text-yellow-400' },
      stalled: { label: `Stalled · ${fmtLag(catchup.lag_seconds)}`, dot: 'bg-red-500', text: 'text-destructive' },
      no_data: { label: 'No ingests yet', dot: 'bg-muted-foreground', text: 'text-muted-foreground' },
    }
    const p = palette[catchup.status] ?? palette.no_data
    return (
      <span className={`inline-flex items-center gap-1.5 text-[10px] font-medium ${p.text}`} title={catchup.latest_ingest_ts ? `Latest ingest: ${catchup.latest_ingest_ts}` : 'No ingests recorded'}>
        <span className={`h-1.5 w-1.5 rounded-full ${p.dot}`} />
        {p.label}
      </span>
    )
  }, [catchup])

  const chartData = useMemo(() => ([
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.fastly_logs),
      type: 'scatter',
      mode: 'lines',
      name: 'Fastly emitted (lines)',
      line: { color: '#3b82f6', width: 2 },
    },
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.our_rows),
      type: 'scatter',
      mode: 'lines',
      name: 'We ingested (lines)',
      line: { color: '#10b981', width: 2, dash: 'dot' },
    },
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.file_count),
      type: 'scatter',
      mode: 'lines',
      name: 'Files ingested',
      yaxis: 'y2',
      line: { color: '#a855f7', width: 1.5, dash: 'dash' },
    },
  ]), [buckets])

  const chartLayout = useMemo(() => ({
    yaxis: { title: { text: 'log lines' } },
    yaxis2: { title: { text: 'files' }, overlaying: 'y', side: 'right', showgrid: false },
  }), [])

  return (
    <AnalyticsCard
      title="Log Line Accounting"
      description="Fastly's authoritative log-line emission counter (Stats API) vs our locally-ingested row_count, per bucket. A non-zero gap means lines were emitted by Fastly but never landed in our table."
      icon={<Database className="h-4 w-4" />}
      headerAction={
        <div className="flex items-center gap-2">
          {catchupBadge}
          {isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
          <Select value={String(presetIdx)} onValueChange={(v) => { if (v) setPresetIdx(parseInt(v)) }}>
            <SelectTrigger className="h-8 w-[120px] text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {LOG_ACCOUNTING_PRESETS.map((p, i) => (
                <SelectItem key={p.label} value={String(i)}>{p.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      }
    >
      {isLoading ? (
        <div className="text-xs text-muted-foreground italic px-1 py-4">Loading log accounting…</div>
      ) : error ? (
        <div className="text-xs text-destructive px-1 py-4">{(error as Error).message}</div>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Fastly emitted</div>
              <div className="font-mono text-base">{(totals?.fastly_logs ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">We ingested</div>
              <div className="font-mono text-base">{(totals?.our_rows ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Gap (lines)</div>
              <div className={`font-mono text-base ${gapColor}`}>{(totals?.gap ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Gap %</div>
              <div className={`font-mono text-base ${gapColor}`}>{(gapPct * 100).toFixed(3)}%</div>
            </div>
          </div>
          {data?.sustained_loss && (
            <div className="mb-3 text-xs px-3 py-2 rounded-md border border-destructive/40 bg-destructive/10 text-destructive">
              <AlertTriangle className="h-3 w-3 inline mr-1.5" />
              Sustained loss: {data.sustained_loss.n_buckets} consecutive {preset.by === 'hour' ? 'hours' : 'days'} ≥5% gap since {data.sustained_loss.started_at} — peak {(data.sustained_loss.max_gap_pct * 100).toFixed(1)}%, {data.sustained_loss.total_lost_lines.toLocaleString()} lines missing
            </div>
          )}
          {totals?.worst_bucket_ts && (totals.worst_bucket_gap_pct ?? 0) > 0.01 && (
            <div className="mb-3 text-xs px-3 py-2 rounded-md border border-yellow-500/30 bg-yellow-500/10 text-yellow-700 dark:text-yellow-300">
              <AlertTriangle className="h-3 w-3 inline mr-1.5" />
              Worst bucket: {totals.worst_bucket_ts} — {((totals.worst_bucket_gap_pct ?? 0) * 100).toFixed(2)}% gap
            </div>
          )}
          {buckets.length > 0 ? (
            <TimeSeriesChart data={chartData} layout={chartLayout} timezone="UTC" height={240} />
          ) : (
            <div className="text-xs text-muted-foreground italic px-1 py-4">No data in this window yet.</div>
          )}
          {data?.fastly_field_used === null && (
            <div className="mt-2 text-[10px] text-muted-foreground italic">
              Note: Fastly Stats response did not contain a recognized log-count field; treating Fastly counts as 0.
            </div>
          )}
        </>
      )}
    </AnalyticsCard>
  )
}

export default function UsageLogPage() {
  const router = useRouter()
  const { activeServiceId } = useServiceStore()
  const { full } = useDateFormat()

  const [preset, setPreset] = useState<number>(24)
  const [usageType, setUsageType] = useState('')
  const [processFilter, setProcessFilter] = useState('')
  const [operationFilter, setOperationFilter] = useState('')
  const [purgeOpen, setPurgeOpen] = useState(false)
  const [purging, setPurging] = useState(false)

  const [now, setNow] = useState(() => new Date())
  useEffect(() => {
    // Gate the 30s tick on tab visibility so a backgrounded admin tab
    // doesn't keep rotating `now` and refetching ~MB of usage_log every
    // minute. Re-tick immediately on visibility-restore so the rolled
    // window matches the moment the user returns to the tab.
    const tick = () => setNow(new Date())
    let id: ReturnType<typeof setInterval> | null = null
    const start = () => {
      if (id !== null) return
      tick()
      id = setInterval(tick, 30_000)
    }
    const stop = () => {
      if (id !== null) {
        clearInterval(id)
        id = null
      }
    }
    const onVis = () => {
      if (document.visibilityState === 'visible') start()
      else stop()
    }
    if (document.visibilityState === 'visible') start()
    document.addEventListener('visibilitychange', onVis)
    return () => {
      document.removeEventListener('visibilitychange', onVis)
      stop()
    }
  }, [])
  const startTime = useMemo(() => toQueryDate(new Date(now.getTime() - preset * 3600 * 1000)), [preset, now])
  const endTime = useMemo(() => toQueryDate(now), [now])

  const exportParams = new URLSearchParams({
    service_id: activeServiceId || '',
    start: startTime,
    end: endTime,
    ...(usageType ? { usage_type: usageType } : {}),
    ...(processFilter ? { process_context: processFilter } : {}),
    ...(operationFilter ? { operation_type: operationFilter } : {}),
    page_size: '500',
  })

  const { data, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['usage-log', activeServiceId, startTime, endTime, usageType, processFilter, operationFilter],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/usage-log', { signal, 
        params: {
          query: {
            service_id: activeServiceId || '',
            start: startTime,
            end: endTime,
            ...(usageType ? { usage_type: usageType } : {}),
            ...(processFilter ? { process_context: processFilter } : {}),
            ...(operationFilter ? { operation_type: operationFilter } : {}),
            page_size: 500,
          } as any,
        },
      })
      if (error) throw new Error(extractApiError(error))
      return data as unknown as { entries: UsageLogEntry[]; total: number; aggregate: UsageLogAggregate }
    },
    enabled: !!activeServiceId,
    staleTime: 30_000,
    refetchInterval: 30_000,
  })

  async function purgeAll() {
    setPurging(true)
    try {
      await client.DELETE('/api/admin/usage-log', {
        params: { query: { service_id: activeServiceId || '' } as any },
      })
      setPurgeOpen(false)
      refetch()
    } finally {
      setPurging(false)
    }
  }

  const agg = data?.aggregate
  const entries = data?.entries ?? []

  const renderBreakdown = (breakdown: Record<string, number> | undefined) => {
    if (!breakdown || Object.keys(breakdown).length === 0) return null
    return (
      <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
        {Object.entries(breakdown)
          .sort((a, b) => b[1] - a[1])
          .map(([op, count]) => (
            <div key={op} className="flex items-center justify-between text-[10px] uppercase tracking-wider opacity-70">
              <span className="truncate mr-2">{op}</span>
              <span className="font-mono">{count.toLocaleString()}</span>
            </div>
          ))}
      </div>
    )
  }

  const columns: ColumnDef<UsageLogEntry>[] = [
    {
      accessorKey: 'timestamp',
      header: 'Timestamp',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">
          {full(row.original.timestamp)}
        </span>
      ),
    },
    {
      accessorKey: 'service_id',
      header: 'Service',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.service_id ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'operation_class',
      header: 'Class',
      cell: ({ row }) => {
        const cls = row.original.operation_class
        if (!cls) return <span className="text-muted-foreground text-xs">—</span>
        const variant = cls === 'A' ? 'default' : cls === 'B' ? 'secondary' : 'outline'
        return <Badge variant={variant} className="text-[10px] px-1.5 py-0 font-mono">{cls === 'CDN' ? 'CDN' : `FOS ${cls}`}</Badge>
      },
    },
    {
      accessorKey: 'operation_type',
      header: 'Operation',
      cell: ({ row }) => (
        <span className="font-mono text-xs">{row.original.operation_type ?? '—'}</span>
      ),
    },
    {
      accessorKey: 'url',
      header: 'URL / Path',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.url ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'bytes',
      header: 'Bytes',
      cell: ({ row }) => row.original.bytes != null
        ? <span className="font-mono text-xs tabular-nums">{formatBytes(row.original.bytes)}</span>
        : <span className="text-muted-foreground text-xs">—</span>,
    },
    {
      accessorKey: 'function_name',
      header: 'Function',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">{row.original.function_name ?? '—'}</span>
      ),
    },
    {
      accessorKey: 'process_context',
      header: 'Process',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {row.original.process_context ?? '—'}
        </span>
      ),
    },
    {
      accessorKey: 'status',
      header: 'Status',
      cell: ({ row }) => {
        const s = row.original.status
        return <Badge variant={s === 'OK' ? 'secondary' : 'destructive'} className="text-[10px] px-1.5 py-0">{s ?? '—'}</Badge>
      },
    },
    {
      accessorKey: 'estimated_cost',
      header: 'Est. Cost',
      cell: ({ row }) => row.original.estimated_cost != null
        ? <span className="font-mono text-xs tabular-nums">{fmtCost(row.original.estimated_cost)}</span>
        : <span className="text-muted-foreground text-xs">—</span>,
    },
  ]

  const exportUrl = `/api/admin/usage-log/export?${exportParams.toString()}`

  return (
    <div className="space-y-6">
      <PageHeader
        title="FOS Usage Log"
        description="Fastly Object Storage and CDN operations captured for cost analysis."
      >
        <Button variant="outline" size="sm" onClick={() => router.push('/admin')}>
          <ArrowLeft className="h-3.5 w-3.5 mr-1.5" /> Admin
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            setNow(new Date())
            refetch()
          }}
          disabled={isFetching}
        >
          <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
        <Button variant="outline" size="sm" onClick={() => setPurgeOpen(true)} className="text-destructive hover:bg-destructive hover:text-white border-destructive/40">
          <Trash2 className="h-3.5 w-3.5 mr-1.5" /> Purge Logs
        </Button>
        <a href={exportUrl} download="usage_log.csv" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <Download className="h-3.5 w-3.5 mr-1.5" /> Export CSV
        </a>
      </PageHeader>

      <div className="bg-muted/30 border border-dashed rounded-lg p-3 text-xs text-muted-foreground flex items-center justify-between">
        <div className="flex items-center gap-2">
          <DollarSign className="h-3.5 w-3.5" />
          <span>Pricing rates and log retention are managed globally in <strong>Admin Settings</strong>.</span>
        </div>
        <Button variant="link" size="sm" className="h-auto p-0 text-xs font-bold" onClick={() => router.push('/admin')}>
          Go to Admin <ArrowLeft className="h-3 w-3 ml-1 rotate-180" />
        </Button>
      </div>

      {/* Aggregate stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          title="FOS Class A Ops"
          value={agg ? fmtOps(agg.total_class_a) : '—'}
          icon={Zap}
          iconClassName="text-blue-500"
          sub={
            <div className="space-y-1">
              <div>{agg ? fmtCost(agg.estimated_cost_class_a) : '—'}</div>
              {renderBreakdown(agg?.class_a_breakdown)}
            </div>
          }
          loading={isLoading}
        />
        <StatCard
          title="FOS Class B Ops"
          value={agg ? fmtOps(agg.total_class_b) : '—'}
          icon={Database}
          iconClassName="text-green-500"
          sub={
            <div className="space-y-1">
              <div>{agg ? fmtCost(agg.estimated_cost_class_b) : '—'}</div>
              {renderBreakdown(agg?.class_b_breakdown)}
            </div>
          }
          loading={isLoading}
        />
        <StatCard
          title="CDN Egress"
          value={agg ? formatBytes(agg.total_cdn_bytes) : '—'}
          icon={Globe}
          iconClassName="text-purple-500"
          sub={
            <div className="space-y-1">
              <div>{agg ? fmtCost(agg.estimated_cost_cdn) : '—'}</div>
              {agg && (
                <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider opacity-70">
                    <span className="truncate mr-2">Requests</span>
                    <span className="font-mono">{agg.total_cdn_downloads.toLocaleString()}</span>
                  </div>
                </div>
              )}
            </div>
          }
          loading={isLoading}
        />
        <StatCard
          title="Est. Total Cost"
          value={agg ? `$${agg.estimated_cost_total.toFixed(2)}` : '—'}
          icon={DollarSign}
          iconClassName="text-amber-500"
          sub={
            <div className="space-y-1">
              <div>for selected period</div>
              {agg && (
                <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider opacity-70">
                    <span className="truncate mr-2">FOS Class A</span>
                    <span className="font-mono">{fmtCost(agg.estimated_cost_class_a)}</span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider opacity-70">
                    <span className="truncate mr-2">FOS Class B</span>
                    <span className="font-mono">{fmtCost(agg.estimated_cost_class_b)}</span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider opacity-70">
                    <span className="truncate mr-2">CDN Egress</span>
                    <span className="font-mono">{fmtCost(agg.estimated_cost_cdn)}</span>
                  </div>
                </div>
              )}
            </div>
          }
          loading={isLoading}
        />
      </div>

      <LogAccountingPanel />

      <div className="rounded-lg border bg-card">
        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3 px-4 py-3 border-b">
          <div className="flex items-center gap-1.5">
            {DATE_PRESETS.map(p => (
              <Button
                key={p.hours}
                size="sm"
                variant={preset === p.hours ? 'default' : 'outline'}
                className="h-7 px-3 text-xs"
                onClick={() => setPreset(p.hours)}
              >
                {p.label}
              </Button>
            ))}
          </div>

          <div className="flex items-center gap-1.5">
            <Label className="text-xs text-muted-foreground shrink-0">Type</Label>
            <Select value={usageType || 'all'} onValueChange={v => setUsageType(!v || v === 'all' ? '' : v)}>
              <SelectTrigger className="h-7 text-xs w-32">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all" className="text-xs">All</SelectItem>
                <SelectItem value="FOS" className="text-xs">FOS (A+B)</SelectItem>
                <SelectItem value="FOS-A" className="text-xs">FOS Class A</SelectItem>
                <SelectItem value="FOS-B" className="text-xs">FOS Class B</SelectItem>
                <SelectItem value="CDN" className="text-xs">CDN</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center gap-1.5">
            <Label className="text-xs text-muted-foreground shrink-0">Operation</Label>
            <Input
              className="h-7 text-xs w-40 font-mono"
              placeholder="e.g. GET_OBJECT"
              value={operationFilter}
              onChange={e => setOperationFilter(e.target.value)}
            />
          </div>

          <div className="flex items-center gap-1.5">
            <Label className="text-xs text-muted-foreground shrink-0">Process</Label>
            <Input
              className="h-7 text-xs w-44 font-mono"
              placeholder="e.g. cron:sync"
              value={processFilter}
              onChange={e => setProcessFilter(e.target.value)}
            />
          </div>

          {isFetching && !isLoading && (
            <span className="text-xs text-muted-foreground animate-pulse">Refreshing…</span>
          )}
        </div>

        <DataTable
          columns={columns}
          data={entries}
          isLoading={isLoading}
          searchKey="url"
        />
      </div>

      {/* Purge confirmation dialog */}
      <Dialog open={purgeOpen} onOpenChange={setPurgeOpen}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>Purge all usage logs?</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground py-2">
            This will permanently delete every entry in the usage log for this service. This cannot be undone.
          </p>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setPurgeOpen(false)}>Cancel</Button>
            <Button variant="destructive" size="sm" onClick={purgeAll} disabled={purging}>
              {purging ? 'Purging…' : 'Purge all logs'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
