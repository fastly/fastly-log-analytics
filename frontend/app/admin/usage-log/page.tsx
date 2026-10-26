'use client'

import React, { Suspense, useState, useMemo, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useRouter, useSearchParams } from 'next/navigation'
import { parseAsString, useQueryStates } from 'nuqs'
import {
  ArrowLeft,
  Download,
  Database,
  Zap,
  Globe,
  DollarSign,
  Trash2,
  RefreshCw,
  AlertTriangle,
  Loader2,
} from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { adminFetch, client, extractApiError, unwrapClient } from '@/lib/api'
import {
  downloadBlob,
  buildServiceHref,
  formatAbbreviatedCount,
  formatBytes,
  formatCount,
  toQueryDate,
  CHART_COLORS,
} from '@/lib/utils'
import type { components } from '@/types/api'
import { useServiceStore } from '@/stores/filterStore'
import { DataTable } from '@/components/DataTable'
import { Button, buttonVariants } from '@/components/ui/button'
import { PageHeader, StatCard, ConfirmDialog } from '@/components/ui/confirm-dialog'
import { useDateFormat } from '@/hooks/useSSE'
import { useMounted } from '@/hooks/useBootstrap'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { AnalyticsCard, TimeSeriesChart, type AnalyticsCardError } from '@/components/AnalyticsCard'

const LOG_ACCOUNTING_PRESETS = [
  { label: 'Last 1h', hours: 1, by: 'hour' as const },
  { label: 'Last 24h', hours: 24, by: 'hour' as const },
  { label: 'Last 7d', hours: 168, by: 'day' as const },
  { label: 'Last 30d', hours: 720, by: 'day' as const },
]

export const usageLogUrlParsers = {
  service: parseAsString,
  category: parseAsString,
  event_type: parseAsString,
  source: parseAsString,
}

function useSafeUsageLogQueryParams() {
  const searchParams = useSearchParams()
  let nuqsParams: {
    service: string | null
    category: string | null
    event_type: string | null
    source: string | null
  } | null = null
  let setNuqsParams:
    | ((
        vals: Partial<{
          service: string | null
          category: string | null
          event_type: string | null
          source: string | null
        }>,
      ) => unknown)
    | null = null
  try {
    ;[nuqsParams, setNuqsParams] = useQueryStates(usageLogUrlParsers, { history: 'replace', shallow: true })
  } catch {
    // Fallback when rendered outside NuqsAdapter in unit tests
  }
  return {
    service: nuqsParams?.service ?? searchParams?.get('service') ?? undefined,
    category: nuqsParams?.category ?? searchParams?.get('category') ?? '',
    event_type: nuqsParams?.event_type ?? searchParams?.get('event_type') ?? '',
    source: nuqsParams?.source ?? searchParams?.get('source') ?? '',
    setNuqsParams,
  }
}

export function LogAccountingPanel() {
  const mounted = useMounted()
  const [presetIdx, setPresetIdx] = useState(1)
  const preset = LOG_ACCOUNTING_PRESETS[presetIdx]
  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['log-accounting', preset.hours, preset.by],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/log-accounting', {
        signal,
        params: { query: { hours: preset.hours, by: preset.by } },
      })
      if (error) throw new Error(extractApiError(error))
      return data
    },
    placeholderData: keepPreviousData,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  const totals = data?.totals
  const buckets = data?.buckets ?? []
  const gapPct = totals?.gap_pct ?? 0
  const gapAbsPct = Math.abs(gapPct) * 100
  const gapColor =
    gapAbsPct <= 0.1
      ? 'text-green-700 dark:text-green-400'
      : gapAbsPct <= 1
        ? 'text-yellow-700 dark:text-yellow-400'
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
      backfilling: {
        label: `Backfilling${catchup.lag_seconds != null ? ` · ${fmtLag(catchup.lag_seconds)}` : ''}`,
        dot: 'bg-yellow-500',
        text: 'text-yellow-700 dark:text-yellow-400',
      },
      stalled: { label: `Stalled · ${fmtLag(catchup.lag_seconds)}`, dot: 'bg-red-500', text: 'text-destructive' },
      no_data: { label: 'No ingests yet', dot: 'bg-muted-foreground', text: 'text-muted-foreground' },
    }
    const p = palette[catchup.status] ?? palette.no_data
    return (
      <span
        className={`inline-flex items-center gap-1.5 text-[10px] font-medium ${p.text}`}
        title={catchup.latest_ingest_ts ? `Latest ingest: ${catchup.latest_ingest_ts}` : 'No ingests recorded'}
      >
        <span className={`h-1.5 w-1.5 rounded-full ${p.dot}`} />
        {p.label}
      </span>
    )
  }, [catchup])

  const chartData = useMemo(
    () => [
      {
        x: buckets.map((b) => b.ts),
        y: buckets.map((b) => b.fastly_requests),
        type: 'scatter',
        mode: 'lines',
        name: 'Fastly requests',
        line: { color: CHART_COLORS.info, width: 2 },
      },
      {
        x: buckets.map((b) => b.ts),
        y: buckets.map((b) => b.our_rows),
        type: 'scatter',
        mode: 'lines',
        name: 'We ingested (rows)',
        line: { color: CHART_COLORS.success, width: 2, dash: 'dot' },
      },
      {
        x: buckets.map((b) => b.ts),
        y: buckets.map((b) => b.file_count),
        type: 'scatter',
        mode: 'lines',
        name: 'Files ingested',
        yaxis: 'y2',
        line: { color: '#a855f7', width: 1.5, dash: 'dash' },
      },
    ],
    [buckets],
  )

  const chartLayout = useMemo(
    () => ({
      yaxis: { title: { text: 'requests / rows' } },
      yaxis2: { title: { text: 'files' }, overlaying: 'y', side: 'right', showgrid: false },
    }),
    [],
  )

  const summaryTiles = [
    { label: 'Fastly requests', value: formatCount(totals?.fastly_requests ?? 0), color: '' },
    { label: 'We ingested', value: formatCount(totals?.our_rows ?? 0), color: '' },
    { label: 'Gap (rows)', value: formatCount(totals?.gap ?? 0), color: gapColor },
    { label: 'Gap %', value: `${(gapPct * 100).toFixed(3)}%`, color: gapColor },
  ]

  return (
    <AnalyticsCard
      title="Ingest Accounting"
      description="Fastly's authoritative request counter (Stats API) vs our locally-ingested rows, per bucket. Requests sit 1:1 with our rows, so a positive gap means requests Fastly served that never landed in our table."
      icon={<Database className="h-4 w-4" />}
      error={error as AnalyticsCardError | null}
      headerAction={
        <div className="flex items-center gap-2">
          {catchupBadge}
          {isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
          <Select
            value={String(presetIdx)}
            onValueChange={(v) => {
              if (v) setPresetIdx(parseInt(v))
            }}
          >
            <SelectTrigger data-testid="admin-usage-log-accounting-granularity" className="h-8 w-[120px] text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {LOG_ACCOUNTING_PRESETS.map((p, i) => (
                <SelectItem key={p.label} value={String(i)}>
                  {p.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      }
    >
      <div
        className={`min-h-[312px] transition-opacity ${
          mounted && isFetching && !!data ? 'opacity-40 pointer-events-none' : ''
        }`}
      >
        {!mounted || (isLoading && !data) ? (
          <div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
              {[0, 1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-[54px] w-full rounded-md" />
              ))}
            </div>
            <div className="h-[240px] flex items-center justify-center rounded-md border border-dashed border-muted bg-muted/10 text-xs text-muted-foreground italic">
              Loading log accounting…
            </div>
          </div>
        ) : error ? null : (
          <>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
              {summaryTiles.map((tile) => (
                <div key={tile.label} className="rounded-md border border-muted bg-muted/20 px-3 py-2">
                  <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">
                    {tile.label}
                  </div>
                  <div className={`font-mono text-base ${tile.color}`}>{tile.value}</div>
                </div>
              ))}
            </div>
            {data?.sustained_loss && (
              <div className="mb-3 text-xs px-3 py-2 rounded-md border border-destructive/40 bg-destructive/10 text-destructive">
                <AlertTriangle className="h-3 w-3 inline mr-1.5" />
                Sustained loss: {data.sustained_loss.n_buckets} consecutive{' '}
                {preset.by === 'hour' ? 'hours' : 'days'} ≥5% gap since {data.sustained_loss.started_at} — peak{' '}
                {(data.sustained_loss.max_gap_pct * 100).toFixed(1)}%,{' '}
                {formatCount(data.sustained_loss.total_lost_lines)} rows missing
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
              <div className="h-[240px] flex items-center justify-center rounded-md border border-dashed border-muted bg-muted/10 text-xs text-muted-foreground italic">
                No data in this window yet.
              </div>
            )}
          </>
        )}
      </div>
    </AnalyticsCard>
  )
}

type UsageLogResponse = components['schemas']['UsageLogResponse']

export type UsageLogEntry = components['schemas']['UsageLogEntry'] & {
  service_id: string | null
}

export const DATE_PRESETS = [
  { label: 'Last 1h', hours: 1 },
  { label: 'Last 24h', hours: 24 },
  { label: 'Last 7d', hours: 168 },
  { label: 'Last 30d', hours: 720 },
]

export function fmtCost(n: number): string {
  if (n === 0) return '$0.000000'
  if (n < 0.000001) return `$${n.toExponential(2)}`
  return `$${n.toFixed(6)}`
}

const monoMutedCell = <K extends keyof UsageLogEntry>(key: K) =>
  function MonoMutedCell({ row }: { row: { original: UsageLogEntry } }) {
    return <span className="font-mono text-xs text-muted-foreground">{(row.original[key] as string | null) ?? '—'}</span>
  }

export function buildUsageLogColumns(full: (ts: string) => string): ColumnDef<UsageLogEntry>[] {
  return [
    {
      accessorKey: 'timestamp',
      header: 'Timestamp',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">{full(row.original.timestamp)}</span>
      ),
    },
    { accessorKey: 'service_id', header: 'Service', cell: monoMutedCell('service_id') },
    {
      accessorKey: 'operation_class',
      header: 'Class',
      cell: ({ row }) => {
        const cls = row.original.operation_class
        if (!cls) return <span className="text-muted-foreground text-xs">—</span>
        const variant = cls === 'A' ? 'default' : cls === 'B' ? 'secondary' : 'outline'
        return (
          <Badge variant={variant} className="text-[10px] px-1.5 py-0 font-mono">
            {cls === 'CDN' ? 'CDN' : `FOS ${cls}`}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'operation_type',
      header: 'Operation',
      cell: ({ row }) => <span className="font-mono text-xs">{row.original.operation_type ?? '—'}</span>,
    },
    { accessorKey: 'url', header: 'URL / Path', cell: monoMutedCell('url') },
    {
      accessorKey: 'bytes',
      header: 'Bytes',
      cell: ({ row }) =>
        row.original.bytes != null ? (
          <span className="font-mono text-xs tabular-nums">{formatBytes(row.original.bytes)}</span>
        ) : (
          <span className="text-muted-foreground text-xs">—</span>
        ),
    },
    { accessorKey: 'function_name', header: 'Function', cell: monoMutedCell('function_name') },
    { accessorKey: 'process_context', header: 'Process', cell: monoMutedCell('process_context') },
    {
      accessorKey: 'status',
      header: 'Status',
      cell: ({ row }) => {
        const s = row.original.status
        return (
          <Badge variant={s === 'OK' ? 'secondary' : 'destructive'} className="text-[10px] px-1.5 py-0">
            {s ?? '—'}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'estimated_cost',
      header: 'Est. Cost',
      cell: ({ row }) =>
        row.original.estimated_cost != null ? (
          <span className="font-mono text-xs tabular-nums">{fmtCost(row.original.estimated_cost)}</span>
        ) : (
          <span className="text-muted-foreground text-xs">—</span>
        ),
    },
  ]
}

interface UsageLogFiltersProps {
  preset: number
  setPreset: (n: number) => void
  usageType: string
  setUsageType: (v: string) => void
  operationFilter: string
  setOperationFilter: (v: string) => void
  processFilter: string
  setProcessFilter: (v: string) => void
  isFetching: boolean
  isLoading: boolean
}

export function UsageLogFilters({
  preset,
  setPreset,
  usageType,
  setUsageType,
  operationFilter,
  setOperationFilter,
  processFilter,
  setProcessFilter,
  isFetching,
  isLoading,
}: UsageLogFiltersProps) {
  return (
    <div data-testid="admin-usage-log-filters" className="flex flex-wrap items-center gap-3 px-4 py-3 border-b">
      <div className="flex items-center gap-1.5">
        {DATE_PRESETS.map((p) => (
          <Button
            key={p.hours}
            data-testid={`admin-usage-log-preset-${p.hours}h`}
            aria-pressed={preset === p.hours}
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
        <Label htmlFor="usage-log-type" className="text-xs text-muted-foreground shrink-0">
          Type
        </Label>
        <Select value={usageType || 'all'} onValueChange={(v) => setUsageType(!v || v === 'all' ? '' : v)}>
          <SelectTrigger id="usage-log-type" data-testid="admin-usage-log-type-select" className="h-7 text-xs w-32">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {[
              ['all', 'All'],
              ['FOS', 'FOS (A+B)'],
              ['FOS-A', 'FOS Class A'],
              ['FOS-B', 'FOS Class B'],
              ['CDN', 'CDN'],
            ].map(([val, label]) => (
              <SelectItem key={val} value={val} className="text-xs">
                {label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-operation" className="text-xs text-muted-foreground shrink-0">
          Operation
        </Label>
        <Input
          id="usage-log-operation"
          data-testid="admin-usage-log-operation-input"
          className="h-7 text-xs w-40 font-mono"
          placeholder="e.g. GET_OBJECT"
          value={operationFilter}
          onChange={(e) => setOperationFilter(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-process" className="text-xs text-muted-foreground shrink-0">
          Process
        </Label>
        <Input
          id="usage-log-process"
          data-testid="admin-usage-log-process-input"
          className="h-7 text-xs w-44 font-mono"
          placeholder="e.g. cron:sync"
          value={processFilter}
          onChange={(e) => setProcessFilter(e.target.value)}
        />
      </div>

      {isFetching && !isLoading && (
        <span data-testid="admin-usage-log-refreshing-badge" className="text-xs text-muted-foreground animate-pulse">
          Refreshing…
        </span>
      )}
    </div>
  )
}

interface UsageLogClientProps {
  initialServiceId?: string
  initialNowMs?: number
}

export function UsageLogClient({ initialServiceId, initialNowMs }: UsageLogClientProps = {}) {
  const router = useRouter()
  const storeActiveServiceId = useServiceStore((s) => s.activeServiceId)
  const urlParams = useSafeUsageLogQueryParams()
  const activeServiceId = storeActiveServiceId ?? initialServiceId ?? urlParams.service ?? null
  const { full } = useDateFormat()
  const mounted = useMounted()

  const [preset, setPreset] = useState<number>(24)
  const [usageType, setUsageTypeState] = useState(() => urlParams.category)
  const [processFilter, setProcessFilterState] = useState(() => urlParams.source)
  const [operationFilter, setOperationFilterState] = useState(() => urlParams.event_type)
  const [purgeOpen, setPurgeOpen] = useState(false)
  const [purging, setPurging] = useState(false)

  const setUsageType = (v: string) => {
    setUsageTypeState(v)
    if (urlParams.setNuqsParams) void urlParams.setNuqsParams({ category: v || null })
  }
  const setOperationFilter = (v: string) => {
    setOperationFilterState(v)
    if (urlParams.setNuqsParams) void urlParams.setNuqsParams({ event_type: v || null })
  }
  const setProcessFilter = (v: string) => {
    setProcessFilterState(v)
    if (urlParams.setNuqsParams) void urlParams.setNuqsParams({ source: v || null })
  }

  const [now, setNow] = useState(() => (initialNowMs ? new Date(initialNowMs) : new Date()))
  useEffect(() => {
    const tick = () => setNow(new Date())
    let id: ReturnType<typeof setInterval> | null = null
    const start = (tickNow: boolean) => {
      if (id !== null) return
      if (tickNow) tick()
      id = setInterval(tick, 30_000)
    }
    const stop = () => {
      if (id !== null) {
        clearInterval(id)
        id = null
      }
    }
    const onVis = () => (document.visibilityState === 'visible' ? start(true) : stop())
    if (document.visibilityState === 'visible') start(false)
    document.addEventListener('visibilitychange', onVis)
    return () => {
      document.removeEventListener('visibilitychange', onVis)
      stop()
    }
  }, [])

  const nowFlooredMs = Math.floor(now.getTime() / 60_000) * 60_000
  const startTime = useMemo(() => toQueryDate(new Date(nowFlooredMs - preset * 3600 * 1000)), [preset, nowFlooredMs])
  const endTime = useMemo(() => toQueryDate(new Date(nowFlooredMs)), [nowFlooredMs])

  const baseQuery = {
    service_id: activeServiceId || '',
    start: startTime,
    end: endTime,
    ...(usageType ? { usage_type: usageType } : {}),
    ...(processFilter ? { process_context: processFilter } : {}),
    ...(operationFilter ? { operation_type: operationFilter } : {}),
  }

  const exportParams = new URLSearchParams({ ...baseQuery, page_size: '500' })

  const makeUsageQueryOpts = (kind: 'head' | 'full', pageSize: number, enabled: boolean) => ({
    queryKey: ['usage-log', kind, activeServiceId, startTime, endTime, usageType, processFilter, operationFilter],
    queryFn: async ({ signal }: { signal: AbortSignal }) => {
      const { data, error } = await client.GET('/api/admin/usage-log', {
        signal,
        params: { query: { ...baseQuery, page_size: pageSize } },
      })
      if (error) throw new Error(extractApiError(error))
      return data as UsageLogResponse
    },
    enabled,
    placeholderData: keepPreviousData,
    staleTime: 30_000,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
  })

  const headQuery = useQuery(makeUsageQueryOpts('head', 50, !!activeServiceId))
  const fullQuery = useQuery(makeUsageQueryOpts('full', 500, !!activeServiceId && !!headQuery.data))

  const data = fullQuery.data ?? headQuery.data
  const isLoading = !mounted || (headQuery.isLoading && !data)
  const isFetching = headQuery.isFetching || fullQuery.isFetching
  const isRefreshing = mounted && isFetching && !isLoading
  const refetch = () => {
    headQuery.refetch()
    fullQuery.refetch()
  }

  async function purgeAll() {
    setPurging(true)
    try {
      await unwrapClient(
        client.DELETE('/api/admin/usage-log', { params: { query: { service_id: activeServiceId || '' } } }),
      )
      setPurgeOpen(false)
      refetch()
    } finally {
      setPurging(false)
    }
  }

  const agg = data?.aggregate
  const entries = useMemo(() => {
    const rows = data?.entries ?? []
    const sid = (data as { service_id?: string | null } | undefined)?.service_id ?? activeServiceId ?? null
    return rows.map((e) => ({ ...e, service_id: sid }))
  }, [data, activeServiceId])

  const renderBreakdown = (breakdown: Record<string, number> | undefined) => {
    if (!breakdown || Object.keys(breakdown).length === 0) return null
    return (
      <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
        {Object.entries(breakdown)
          .sort((a, b) => b[1] - a[1])
          .map(([op, count]) => (
            <div
              key={op}
              className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              <span className="truncate mr-2">{op}</span>
              <span className="font-mono">{formatCount(count)}</span>
            </div>
          ))}
      </div>
    )
  }

  const columns = useMemo(() => buildUsageLogColumns(full), [full])
  const exportUrl = `/api/admin/usage-log/export?${exportParams.toString()}`

  return (
    <div data-testid="admin-usage-log-root" className="space-y-6">
      <PageHeader title="FOS Usage Log" description="Fastly Object Storage and CDN operations captured for cost analysis.">
        <div
          data-testid="admin-usage-log-toolbar"
          className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
        >
          <Button
            data-testid="admin-usage-log-back-btn"
            variant="outline"
            size="sm"
            onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}
          >
            <ArrowLeft className="h-3.5 w-3.5 mr-1.5" /> Admin
          </Button>
          <Button
            data-testid="admin-usage-log-refresh-btn"
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
          <Button
            data-testid="admin-usage-log-purge-btn"
            variant="outline"
            size="sm"
            onClick={() => setPurgeOpen(true)}
            className="text-destructive hover:bg-destructive hover:text-white border-destructive/40"
          >
            <Trash2 className="h-3.5 w-3.5 mr-1.5" /> Purge Logs
          </Button>
          <a
            data-testid="admin-usage-log-export-btn"
            href={mounted ? exportUrl : undefined}
            download="usage_log.csv"
            onClick={(e) => {
              e.preventDefault()
              void adminFetch(exportUrl).then(async (res) => {
                if (res.ok) downloadBlob(await res.blob(), 'usage_log.csv')
              })
            }}
            className={buttonVariants({ variant: 'outline', size: 'sm' })}
          >
            <Download className="h-3.5 w-3.5 mr-1.5" /> Export CSV
          </a>
        </div>
      </PageHeader>

      <div className="bg-muted/30 border border-dashed rounded-lg p-3 text-xs text-muted-foreground flex items-center justify-between">
        <div className="flex items-center gap-2">
          <DollarSign className="h-3.5 w-3.5" />
          <span>
            Pricing rates and log retention are managed globally in <strong>Admin Settings</strong>.
          </span>
        </div>
        <Button
          variant="link"
          size="sm"
          className="h-auto p-0 text-xs font-bold"
          onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}
        >
          Go to Admin <ArrowLeft className="h-3 w-3 ml-1 rotate-180" />
        </Button>
      </div>

      <div
        data-testid="admin-usage-log-kpi-reservation"
        className={`grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[164px] [contain-intrinsic-size:auto_164px] [content-visibility:auto] transition-opacity ${
          isRefreshing ? 'opacity-40 pointer-events-none' : ''
        }`}
      >
        <StatCard
          title="FOS Class A Ops"
          value={agg ? formatAbbreviatedCount(agg.total_class_a) : '—'}
          icon={Zap}
          iconClassName="text-blue-500"
          className="min-h-[164px]"
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
          value={agg ? formatAbbreviatedCount(agg.total_class_b) : '—'}
          icon={Database}
          iconClassName="text-green-500"
          className="min-h-[164px]"
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
          className="min-h-[164px]"
          sub={
            <div className="space-y-1">
              <div>{agg ? fmtCost(agg.estimated_cost_cdn) : '—'}</div>
              {agg && (
                <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
                    <span className="truncate mr-2">Requests</span>
                    <span className="font-mono">{formatCount(agg.total_cdn_downloads)}</span>
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
          className="min-h-[164px]"
          sub={
            <div className="space-y-1">
              <div>for selected period</div>
              {agg && (
                <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                  {[
                    ['FOS Class A', agg.estimated_cost_class_a],
                    ['FOS Class B', agg.estimated_cost_class_b],
                    ['CDN Egress', agg.estimated_cost_cdn],
                  ].map(([lbl, cost]) => (
                    <div
                      key={lbl as string}
                      className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground"
                    >
                      <span className="truncate mr-2">{lbl}</span>
                      <span className="font-mono">{fmtCost(cost as number)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          }
          loading={isLoading}
        />
      </div>

      <div
        data-testid="admin-usage-log-chart-reservation"
        className="min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
      >
        <LogAccountingPanel />
      </div>

      <div
        data-testid="admin-usage-log-table-reservation"
        className="rounded-lg border bg-card min-h-[520px] [contain-intrinsic-size:auto_520px] [content-visibility:auto]"
      >
        <UsageLogFilters
          preset={preset}
          setPreset={setPreset}
          usageType={usageType}
          setUsageType={setUsageType}
          operationFilter={operationFilter}
          setOperationFilter={setOperationFilter}
          processFilter={processFilter}
          setProcessFilter={setProcessFilter}
          isFetching={isFetching}
          isLoading={isLoading}
        />

        <div className={isRefreshing ? 'opacity-40 pointer-events-none transition-opacity' : 'transition-opacity'}>
          {isLoading && (
            <div className="sr-only" role="status">
              Loading FOS usage ledger...
            </div>
          )}
          <DataTable columns={columns} data={entries} isLoading={isLoading} searchKey="url" />
        </div>
      </div>

      <ConfirmDialog
        open={purgeOpen}
        onOpenChange={setPurgeOpen}
        title="Purge all usage logs?"
        description="This will permanently delete every entry in the usage log for this service. This cannot be undone."
        confirmLabel="Purge all logs"
        isDangerous
        isPending={purging}
        onConfirm={purgeAll}
      />
    </div>
  )
}

function UsageLogPageInner() {
  const { service } = useSafeUsageLogQueryParams()
  return <UsageLogClient initialServiceId={service} />
}

export default function UsageLogPage() {
  return (
    <Suspense
      fallback={
        <div data-testid="admin-usage-log-root" className="space-y-6">
          <div className="h-[52px]" />
          <div className="h-[46px] rounded-lg border border-dashed bg-muted/30" />
          <div
            data-testid="admin-usage-log-kpi-reservation"
            className="grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[164px]"
          />
          <div data-testid="admin-usage-log-chart-reservation" className="min-h-[380px]" />
          <div data-testid="admin-usage-log-table-reservation" className="rounded-lg border bg-card min-h-[520px]" />
        </div>
      }
    >
      <UsageLogPageInner />
    </Suspense>
  )
}
