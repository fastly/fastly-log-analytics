'use client'

import React, { Suspense, useState, useMemo, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useRouter, useSearchParams } from 'next/navigation'
import { parseAsString, useQueryStates } from 'nuqs'
import { ArrowLeft, Download, DollarSign, Trash2, RefreshCw } from 'lucide-react'
import { adminFetch, client, extractApiError, unwrapClient } from '@/lib/api'
import { downloadBlob, buildServiceHref, toQueryDate } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import { DataTable } from '@/components/DataTable'
import { Button, buttonVariants } from '@/components/ui/button'
import { PageHeader, ConfirmDialog } from '@/components/ui/confirm-dialog'
import { useDateFormat } from '@/hooks/useSSE'
import { useMounted } from '@/hooks/useBootstrap'
import { LogAccountingPanel } from './LogAccountingPanel'
import {
  DATE_PRESETS,
  fmtCost,
  buildUsageLogColumns,
  UsageLogFilters,
  UsageLogKpiCards,
  type UsageLogEntry,
  type UsageLogResponse,
} from './UsageLogTableAndCards'

export { LogAccountingPanel, DATE_PRESETS, fmtCost, buildUsageLogColumns, UsageLogFilters, type UsageLogEntry }

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

      <UsageLogKpiCards agg={agg} isLoading={isLoading} isRefreshing={isRefreshing} />

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
