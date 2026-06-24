'use client'

import React, { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api'
import { useServiceStore } from '@/stores/serviceStore'
import { DataTable } from '@/components/DataTable/DataTable'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { StatCard } from '@/components/ui/stat-card'
import { ArrowLeft, Download, Database, Zap, Globe, DollarSign, Trash2, RefreshCw } from 'lucide-react'
import { buttonVariants } from '@/components/ui/button'
import { useRouter } from 'next/navigation'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useMounted } from '@/hooks/useMounted'
import { formatBytes } from '@/lib/format'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import {
  toQueryDate,
  fmtCost,
  fmtOps,
} from './shared'

type UsageLogResponse = components['schemas']['UsageLogResponse']
import { LogAccountingPanel } from './UsageChart'
import { buildUsageLogColumns } from './UsageTable'
import { UsageLogFilters } from './Filters'

export default function UsageLogClient() {
  const router = useRouter()
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { full } = useDateFormat()
  // SSR-safe: `now` below is seeded from `new Date()`, which differs between
  // the server render and the client's hydration render (they straddle a
  // minute boundary), so the export href's start/end window diverges and
  // throws a React hydration mismatch on /admin/usage-log. Gate the href on
  // mount so the server HTML never carries the time-derived URL.
  const mounted = useMounted()

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
        params: { query: { ...baseQuery, page_size: 50 } as any },
      })
      if (error) throw new Error(extractApiError(error))
      return data as UsageLogResponse
    },
    enabled: !!activeServiceId,
    staleTime: 30_000,
    refetchInterval: 30_000,
  })

  const fullQuery = useQuery({
    queryKey: ['usage-log', 'full', activeServiceId, startTime, endTime, usageType, processFilter, operationFilter],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/usage-log', { signal,
        params: { query: { ...baseQuery, page_size: 500 } as any },
      })
      if (error) throw new Error(extractApiError(error))
      return data as UsageLogResponse
    },
    // Gate the heavier 500-row backfill on the head landing first so we
    // never block the aggregate render on the slower query.
    enabled: !!activeServiceId && !!headQuery.data,
    staleTime: 30_000,
    refetchInterval: 30_000,
  })

  // Prefer the full backfill once it lands; fall back to the head for
  // first-paint of the aggregate + the first 50 rows.
  const data = fullQuery.data ?? headQuery.data
  // First-paint loading state tracks the head — once 50 rows are visible
  // the page is usable; the backfill just expands the table. Force loading
  // until mounted so the server render (live client-only query → no data) and
  // the client's first render agree on the loading skeleton instead of the
  // server painting '—'/0 values that the client then diverges from (#418).
  const isLoading = !mounted || headQuery.isLoading
  const isFetching = headQuery.isFetching || fullQuery.isFetching
  const refetch = () => {
    headQuery.refetch()
    fullQuery.refetch()
  }

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
              <span className="font-mono">{count.toLocaleString()}</span>
            </div>
          ))}
      </div>
    )
  }

  const columns = useMemo(() => buildUsageLogColumns(full), [full])

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
        <a href={mounted ? exportUrl : undefined} download="usage_log.csv" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
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
                  <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
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

      <LogAccountingPanel />

      <div className="rounded-lg border bg-card">
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
