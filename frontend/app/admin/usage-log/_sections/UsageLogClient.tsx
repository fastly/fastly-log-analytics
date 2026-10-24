'use client'

import React, { useState, useMemo, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { adminFetch, client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { downloadBlob } from '@/lib/utils'
import type { components } from '@/types/api'
import { useServiceStore } from '@/stores/serviceStore'
import { DataTable } from '@/components/DataTable/DataTable'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { StatCard } from '@/components/ui/stat-card'
import { ArrowLeft, Download, Database, Zap, Globe, DollarSign, Trash2, RefreshCw } from 'lucide-react'
import { buttonVariants } from '@/components/ui/button'
import { useRouter } from 'next/navigation'
import { buildServiceHref } from '@/lib/navigation'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useMounted } from '@/hooks/useMounted'
import { formatAbbreviatedCount, formatBytes, formatCount } from '@/lib/format'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
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
import { ColumnDef } from '@tanstack/react-table'
import { LogAccountingPanel } from './UsageChart'

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

export function toQueryDate(d: Date): string {
  return d.toISOString().slice(0, 19) + 'Z'
}

export function fmtCost(n: number): string {
  if (n === 0) return '$0.000000'
  if (n < 0.000001) return `$${n.toExponential(2)}`
  return `$${n.toFixed(6)}`
}

export function buildUsageLogColumns(
  full: (ts: string) => string,
): ColumnDef<UsageLogEntry>[] {
  return [
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
        {DATE_PRESETS.map(p => (
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
        <Label htmlFor="usage-log-type" className="text-xs text-muted-foreground shrink-0">Type</Label>
        <Select value={usageType || 'all'} onValueChange={v => setUsageType(!v || v === 'all' ? '' : v)}>
          <SelectTrigger id="usage-log-type" data-testid="admin-usage-log-type-select" className="h-7 text-xs w-32">
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
        <Label htmlFor="usage-log-operation" className="text-xs text-muted-foreground shrink-0">Operation</Label>
        <Input
          id="usage-log-operation"
          data-testid="admin-usage-log-operation-input"
          className="h-7 text-xs w-40 font-mono"
          placeholder="e.g. GET_OBJECT"
          value={operationFilter}
          onChange={e => setOperationFilter(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-process" className="text-xs text-muted-foreground shrink-0">Process</Label>
        <Input
          id="usage-log-process"
          data-testid="admin-usage-log-process-input"
          className="h-7 text-xs w-44 font-mono"
          placeholder="e.g. cron:sync"
          value={processFilter}
          onChange={e => setProcessFilter(e.target.value)}
        />
      </div>

      {isFetching && !isLoading && (
        <span data-testid="admin-usage-log-refreshing-badge" className="text-xs text-muted-foreground animate-pulse">Refreshing…</span>
      )}
    </div>
  )
}

interface UsageLogClientProps {
  initialServiceId?: string
  initialNowMs?: number
}

export default function UsageLogClient({ initialServiceId, initialNowMs }: UsageLogClientProps = {}) {
  const router = useRouter()
  const storeActiveServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = storeActiveServiceId ?? initialServiceId ?? null
  const { full } = useDateFormat()
  // SSR-safe: `now` below is seeded from `initialNowMs` passed by the RSC
  // shell (page.tsx). When not provided, it falls back to `new Date()`.
  // Pinning `initialNowMs` ensures the server render and the client's
  // hydration render compute the exact same minute-floored query window,
  // preventing React hydration mismatches (#418) when navigating across
  // a minute boundary. Gate the export href on mount so the server HTML
  // never carries an unsynchronized time-derived URL.
  const mounted = useMounted()

  const [preset, setPreset] = useState<number>(24)
  const [usageType, setUsageType] = useState('')
  const [processFilter, setProcessFilter] = useState('')
  const [operationFilter, setOperationFilter] = useState('')
  const [purgeOpen, setPurgeOpen] = useState(false)
  const [purging, setPurging] = useState(false)

  const [now, setNow] = useState(() => (initialNowMs ? new Date(initialNowMs) : new Date()))
  useEffect(() => {
    // Gate the 30s tick on tab visibility so a backgrounded admin tab
    // doesn't keep rotating `now` and refetching ~MB of usage_log every
    // minute. Re-tick immediately on visibility-restore so the rolled
    // window matches the moment the user returns to the tab, while
    // preserving `initialNowMs` on first mount so the SSR-seeded query
    // key is not invalidated on initial hydration.
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
    const onVis = () => {
      if (document.visibilityState === 'visible') start(true)
      else stop()
    }
    if (document.visibilityState === 'visible') start(false)
    document.addEventListener('visibilitychange', onVis)
    return () => {
      document.removeEventListener('visibilitychange', onVis)
      stop()
    }
  }, [])
  // Floor `now` to the minute so the 30 s setInterval tick that drives
  // `now` doesn't churn the React Query cache key twice a minute on
  // tabs left open. The aggregate has minute-grain at best and the
  // user-facing windows are multi-hour; minute-rounding here trades
  // a ≤60 s lag for halving refetches and bounding the cache leak
  // on long-lived admin sessions.
  const nowFlooredMs = Math.floor(now.getTime() / 60_000) * 60_000
  const startTime = useMemo(() => toQueryDate(new Date(nowFlooredMs - preset * 3600 * 1000)), [preset, nowFlooredMs])
  const endTime = useMemo(() => toQueryDate(new Date(nowFlooredMs)), [nowFlooredMs])

  const exportParams = new URLSearchParams({
    service_id: activeServiceId || '',
    start: startTime,
    end: endTime,
    ...(usageType ? { usage_type: usageType } : {}),
    ...(processFilter ? { process_context: processFilter } : {}),
    ...(operationFilter ? { operation_type: operationFilter } : {}),
    page_size: '500',
  })

  // Chunked first-paint: a 50-row HEAD batch lands quickly so the aggregate
  // StatCards + the first screenful of rows render long before the full
  // 500-row backfill returns. The aggregate is window-wide (computed over
  // every row in the filter window, not page-bounded; see
  // backend/routers/admin_usage.py:127-136), so the head response carries
  // the correct totals immediately. Both queries share the same
  // ``nowFlooredMs``-derived window so the two fetches correlate.
  const baseQuery = {
    service_id: activeServiceId || '',
    start: startTime,
    end: endTime,
    ...(usageType ? { usage_type: usageType } : {}),
    ...(processFilter ? { process_context: processFilter } : {}),
    ...(operationFilter ? { operation_type: operationFilter } : {}),
  }

  const headQuery = useQuery({
    queryKey: ['usage-log', 'head', activeServiceId, startTime, endTime, usageType, processFilter, operationFilter],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/usage-log', { signal,
        params: { query: { ...baseQuery, page_size: 50 } },
      })
      if (error) throw new Error(extractApiError(error))
      return data as UsageLogResponse
    },
    enabled: !!activeServiceId,
    placeholderData: keepPreviousData,
    staleTime: 30_000,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
  })

  const fullQuery = useQuery({
    queryKey: ['usage-log', 'full', activeServiceId, startTime, endTime, usageType, processFilter, operationFilter],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/usage-log', { signal,
        params: { query: { ...baseQuery, page_size: 500 } },
      })
      if (error) throw new Error(extractApiError(error))
      return data as UsageLogResponse
    },
    // Gate the heavier 500-row backfill on the head landing first so we
    // never block the aggregate render on the slower query.
    enabled: !!activeServiceId && !!headQuery.data,
    placeholderData: keepPreviousData,
    staleTime: 30_000,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
  })

  // Prefer the full backfill once it lands; fall back to the head for
  // first-paint of the aggregate + the first 50 rows.
  const data = fullQuery.data ?? headQuery.data
  // First-paint loading state tracks the head — once 50 rows are visible
  // the page is usable; the backfill just expands the table. Force loading
  // until mounted so the server render (live client-only query → no data) and
  // the client's first render agree on the loading skeleton instead of the
  // server painting '—'/0 values that the client then diverges from (#418).
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
      await unwrapClient(client.DELETE('/api/admin/usage-log', {
        params: { query: { service_id: activeServiceId || '' } },
      }))
      setPurgeOpen(false)
      refetch()
    } finally {
      setPurging(false)
    }
  }

  const agg = data?.aggregate
  // service_id is hoisted to the response root (saves ~50 B per row on
  // the wire). The UsageTable still wants it per-row for its Service
  // column, so re-inject it from the response context here.
  const entries = useMemo(() => {
    const rows = data?.entries ?? []
    const sid = (data as { service_id?: string | null } | undefined)?.service_id ?? activeServiceId ?? null
    return rows.map(e => ({ ...e, service_id: sid }))
  }, [data, activeServiceId])

  const renderBreakdown = (breakdown: Record<string, number> | undefined) => {
    if (!breakdown || Object.keys(breakdown).length === 0) return null
    return (
      <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
        {Object.entries(breakdown)
          .sort((a, b) => b[1] - a[1])
          .map(([op, count]) => (
            <div key={op} className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
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
      <PageHeader
        title="FOS Usage Log"
        description="Fastly Object Storage and CDN operations captured for cost analysis."
      >
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
          <span>Pricing rates and log retention are managed globally in <strong>Admin Settings</strong>.</span>
        </div>
        <Button variant="link" size="sm" className="h-auto p-0 text-xs font-bold" onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}>
          Go to Admin <ArrowLeft className="h-3 w-3 ml-1 rotate-180" />
        </Button>
      </div>

      {/* Aggregate stats */}
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
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
                    <span className="truncate mr-2">FOS Class A</span>
                    <span className="font-mono">{fmtCost(agg.estimated_cost_class_a)}</span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
                    <span className="truncate mr-2">FOS Class B</span>
                    <span className="font-mono">{fmtCost(agg.estimated_cost_class_b)}</span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
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
          <DataTable
            columns={columns}
            data={entries}
            isLoading={isLoading}
            searchKey="url"
          />
        </div>
      </div>

      {/* Purge confirmation dialog */}
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
