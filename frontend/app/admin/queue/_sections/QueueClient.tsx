'use client'

import React from 'react'
import { AlertTriangle, CheckCircle2, RefreshCw, RotateCcw, Trash2 } from 'lucide-react'
import { formatLocalDateTime } from '@/lib/date'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Sparkline } from '@/components/Sparkline'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import type { components } from '@/types/api.generated'

import { useAdminEventStream } from '@/hooks/useAdminEventStream'
import { queryKeys } from '@/lib/query-keys'
import { QuarantineSection } from '@/app/admin/_sections/QuarantineSection'

type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
type ActiveTask = components['schemas']['CeleryActiveTask']
type ScheduleEntry = components['schemas']['CeleryScheduleEntry']
type InProcessScheduleEntry = components['schemas']['InProcessScheduleEntry']
type InProcessActiveRun = components['schemas']['InProcessActiveRun']
type LedgerEntry = components['schemas']['IngestLedgerEntry']
type QueueDepth = [name: string, count: number]
type LedgerStatusFilter = 'problem' | 'all' | 'dead_letter' | 'quarantined' | 'discovered' | 'claimed' | 'committed'

function formatBytes(bytes: number | null | undefined): string | null {
  if (bytes === null || bytes === undefined || bytes <= 0) return null
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

const QUEUE_COLUMNS: ColumnDef<QueueDepth>[] = [
  {
    id: 'queue',
    header: 'Queue Name',
    accessorFn: (q) => q[0],
    size: 320,
    cell: ({ row }) => <span className="font-mono text-sm">{row.original[0]}</span>,
  },
  {
    id: 'messages',
    header: 'Messages',
    accessorFn: (q) => q[1],
    size: 140,
    cell: ({ row }) => <span className="block text-right tabular-nums">{row.original[1]}</span>,
  },
]

const TASK_COLUMNS: ColumnDef<ActiveTask>[] = [
  {
    id: 'id',
    header: 'ID',
    accessorFn: (t) => t.id,
    size: 300,
    enableSorting: false,
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.id}</span>,
  },
  {
    id: 'name',
    header: 'Name',
    accessorFn: (t) => t.name,
    size: 260,
    enableSorting: false,
    cell: ({ row }) => <span className="text-sm font-medium">{row.original.name}</span>,
  },
  {
    id: 'args',
    header: 'Args',
    accessorFn: (t) => JSON.stringify(t.args),
    size: 240,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground">{JSON.stringify(row.original.args)}</span>
    ),
  },
  {
    id: 'start',
    header: 'Start Time',
    accessorFn: (t) => t.time_start,
    size: 200,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-xs">{formatLocalDateTime(row.original.time_start * 1000)}</span>
    ),
  },
]

const SCHEDULE_COLUMNS: ColumnDef<ScheduleEntry>[] = [
  {
    id: 'name',
    header: 'Schedule Name',
    accessorFn: (s) => s.name,
    size: 320,
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.name}</span>,
  },
  {
    id: 'task',
    header: 'Task',
    accessorFn: (s) => s.task,
    size: 420,
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.task}</span>,
  },
]

const INPROCESS_SCHEDULE_COLUMNS: ColumnDef<InProcessScheduleEntry>[] = [
  {
    id: 'name',
    header: 'Job ID',
    accessorFn: (s) => s.name,
    size: 320,
    cell: ({ row }) => <span className="font-mono text-xs font-medium">{row.original.name}</span>,
  },
  {
    id: 'trigger',
    header: 'Trigger / Cadence',
    accessorFn: (s) => s.trigger,
    size: 280,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground">{row.original.trigger}</span>
    ),
  },
  {
    id: 'next_run_at',
    header: 'Next Scheduled Run',
    accessorFn: (s) => s.next_run_at ?? 0,
    size: 240,
    cell: ({ row }) => (
      <span className="text-xs tabular-nums">
        {row.original.next_run_at ? formatLocalDateTime(row.original.next_run_at * 1000) : '—'}
      </span>
    ),
  },
]

const INPROCESS_ACTIVE_COLUMNS: ColumnDef<InProcessActiveRun>[] = [
  {
    id: 'run_id',
    header: 'Run ID',
    accessorFn: (r) => r.run_id,
    size: 140,
    enableSorting: false,
    cell: ({ row }) => <span className="font-mono text-xs">#{row.original.run_id}</span>,
  },
  {
    id: 'task',
    header: 'Task',
    accessorFn: (r) => r.task ?? '—',
    size: 260,
    enableSorting: false,
    cell: ({ row }) => <span className="font-mono text-xs font-medium">{row.original.task ?? '—'}</span>,
  },
  {
    id: 'service_id',
    header: 'Service ID',
    accessorFn: (r) => r.service_id ?? 'global',
    size: 260,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground">{row.original.service_id ?? 'global'}</span>
    ),
  },
  {
    id: 'started_at',
    header: 'Started At',
    accessorFn: (r) => r.started_at ?? 0,
    size: 220,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-xs tabular-nums">
        {row.original.started_at ? formatLocalDateTime(row.original.started_at * 1000) : '—'}
      </span>
    ),
  },
]

export function QueueClient() {
  const queryClient = useQueryClient()
  const { state: sseConnected } = useAdminEventStream(true, ['celery-status'])
  const [ledgerFilter, setLedgerFilter] = React.useState<LedgerStatusFilter>('problem')
  const [actionBusyKey, setActionBusyKey] = React.useState<string | null>(null)
  const [actionNotice, setActionNotice] = React.useState<{
    kind: 'success' | 'error'
    message: string
  } | null>(null)

  const {
    data,
    isLoading,
    isFetching,
    error,
    refetch,
  } = useQuery({
    queryKey: queryKeys.admin.celeryStatus(),
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/admin/celery/status', { signal })
      if (!response.ok || !data) throw new Error(`HTTP ${response.status}`)
      return data
    },
    // We only poll if SSE isn't fully established as a fallback,
    // otherwise the stream pushes to this query key directly.
    refetchInterval: sseConnected === 'open' ? false : 5000,
  })

  const deadLetterCount = data?.ledger?.dead_letter ?? 0
  const quarantinedCount = data?.ledger?.quarantined ?? 0
  const problemCount = deadLetterCount + quarantinedCount
  const shouldInspectLedger = problemCount > 0 || ledgerFilter !== 'problem'

  const {
    data: ledgerData,
    isLoading: ledgerLoading,
    isFetching: ledgerFetching,
    error: ledgerError,
    refetch: refetchLedger,
  } = useQuery({
    queryKey: queryKeys.admin.celeryLedger(ledgerFilter, null, 0),
    queryFn: async ({ signal }) => {
      const { data: resData, response } = await client.GET('/api/admin/celery/ledger', {
        params: {
          query: {
            status: ledgerFilter,
            limit: 50,
            offset: 0,
          },
        },
        signal,
      })
      if (!response.ok || !resData) throw new Error(`HTTP ${response.status}`)
      return resData
    },
    enabled: shouldInspectLedger,
  })

  const {
    data: trends,
    isLoading: trendsLoading,
    isFetching: trendsFetching,
    error: trendsError,
    refetch: refetchTrends,
  } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch('1h'),
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/admin/metric-history/batch', {
        params: { query: { since: '1h' } },
        signal,
      })
      if (!response.ok || !data) throw new Error(`HTTP ${response.status}`)
      return data as TrendBatch
    },
    refetchInterval: 60_000,
  })

  const isRefreshing =
    (isFetching && !isLoading) ||
    (trendsFetching && !trendsLoading) ||
    (ledgerFetching && !ledgerLoading)

  const handleRefresh = React.useCallback(() => {
    void refetch()
    void refetchTrends()
    if (shouldInspectLedger) {
      void refetchLedger()
    }
  }, [refetch, refetchTrends, refetchLedger, shouldInspectLedger])

  const refreshLedgerAfterAction = React.useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.celeryLedgerAll() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.celeryStatus() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.overviewCeleryStatus() }),
    ])
    await Promise.all([refetch(), refetchLedger()])
  }, [queryClient, refetch, refetchLedger])

  const handleRetryItem = React.useCallback(
    async (entry: LedgerEntry) => {
      const busyId = `retry:${entry.service_id}:${entry.object_key}`
      setActionBusyKey(busyId)
      setActionNotice(null)
      try {
        const { data: res, response } = await client.POST('/api/admin/celery/ledger/retry', {
          body: {
            items: [{ service_id: entry.service_id, object_key: entry.object_key }],
          },
        })
        if (!response.ok || !res) throw new Error(`HTTP ${response.status}`)
        setActionNotice({
          kind: 'success',
          message: `Requeued ${res.affected ?? 1} file for ingest retry${res.dispatched ? ` (dispatched ${res.dispatched} to Celery)` : ''}.`,
        })
        await refreshLedgerAfterAction()
      } catch (err) {
        setActionNotice({
          kind: 'error',
          message: `Retry failed: ${(err as Error).message}`,
        })
      } finally {
        setActionBusyKey(null)
      }
    },
    [refreshLedgerAfterAction],
  )

  const handlePurgeItem = React.useCallback(
    async (entry: LedgerEntry) => {
      const busyId = `purge:${entry.service_id}:${entry.object_key}`
      setActionBusyKey(busyId)
      setActionNotice(null)
      try {
        const { data: res, response } = await client.POST('/api/admin/celery/ledger/purge', {
          body: {
            items: [{ service_id: entry.service_id, object_key: entry.object_key }],
          },
        })
        if (!response.ok || !res) throw new Error(`HTTP ${response.status}`)
        setActionNotice({
          kind: 'success',
          message: `Dismissed ${res.affected ?? 1} ledger entry.`,
        })
        await refreshLedgerAfterAction()
      } catch (err) {
        setActionNotice({
          kind: 'error',
          message: `Dismiss failed: ${(err as Error).message}`,
        })
      } finally {
        setActionBusyKey(null)
      }
    },
    [refreshLedgerAfterAction],
  )

  const handleBulkRetry = React.useCallback(async () => {
    const targetStatus: 'dead_letter' | 'quarantined' | 'problem' =
      ledgerFilter === 'dead_letter' || ledgerFilter === 'quarantined' ? ledgerFilter : 'problem'
    setActionBusyKey('bulk-retry')
    setActionNotice(null)
    try {
      const { data: res, response } = await client.POST('/api/admin/celery/ledger/retry', {
        body: { status: targetStatus },
      })
      if (!response.ok || !res) throw new Error(`HTTP ${response.status}`)
      setActionNotice({
        kind: 'success',
        message: `Requeued ${res.affected ?? 0} file(s) for retry${res.dispatched ? ` (dispatched ${res.dispatched} to Celery)` : ''}.`,
      })
      await refreshLedgerAfterAction()
    } catch (err) {
      setActionNotice({
        kind: 'error',
        message: `Bulk retry failed: ${(err as Error).message}`,
      })
    } finally {
      setActionBusyKey(null)
    }
  }, [ledgerFilter, refreshLedgerAfterAction])

  const handleBulkPurge = React.useCallback(async () => {
    const targetStatus: 'dead_letter' | 'quarantined' | 'problem' =
      ledgerFilter === 'dead_letter' || ledgerFilter === 'quarantined' ? ledgerFilter : 'problem'
    setActionBusyKey('bulk-purge')
    setActionNotice(null)
    try {
      const { data: res, response } = await client.POST('/api/admin/celery/ledger/purge', {
        body: { status: targetStatus },
      })
      if (!response.ok || !res) throw new Error(`HTTP ${response.status}`)
      setActionNotice({
        kind: 'success',
        message: `Dismissed ${res.affected ?? 0} dead-letter/quarantined ledger entry(s).`,
      })
      await refreshLedgerAfterAction()
    } catch (err) {
      setActionNotice({
        kind: 'error',
        message: `Bulk dismiss failed: ${(err as Error).message}`,
      })
    } finally {
      setActionBusyKey(null)
    }
  }, [ledgerFilter, refreshLedgerAfterAction])

  const isStandardMode = data?.deployment_mode === 'standard' && !data?.broker_reachable
  const queues = data?.queues ? Object.entries(data.queues).sort((a, b) => a[0].localeCompare(b[0])) : []
  const activeWorkers = data?.workers?.active_tasks ? Object.entries(data.workers.active_tasks) : []
  const schedules = data?.schedules || []
  const workerCount = data?.workers?.count ?? 0
  const inprocessSchedules = data?.inprocess_schedules || []
  const inprocessActiveRuns = data?.inprocess_active_runs || []
  const standardIngestedFiles = data?.standard_ingest?.ingested_files ?? 0
  const standardQuarantinedItems = data?.standard_ingest?.quarantined_items ?? 0

  const queuePoints = trends?.series?.['celery_queue_depth'] ?? []
  const workerPoints = trends?.series?.['celery_active_tasks'] ?? []
  const latestQueue = queuePoints.length ? queuePoints[queuePoints.length - 1].value : null
  const latestWorker = workerPoints.length ? workerPoints[workerPoints.length - 1].value : null

  return (
    <div className="grid gap-6" data-testid="admin-queue-client">
      <div
        data-testid="admin-queue-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        <div className="flex flex-wrap items-center gap-3">
          {isLoading ? (
            <>
              <Skeleton className="h-5 w-28 rounded-full" />
              <span className="text-xs text-muted-foreground">
                Inspecting scheduler &amp; ingest status...
              </span>
            </>
          ) : (
            <Badge
              variant={data?.broker_reachable || isStandardMode ? 'outline' : 'secondary'}
              data-testid="admin-queue-broker-badge"
              className={
                data?.broker_reachable
                  ? 'text-emerald-600 dark:text-emerald-400 border-emerald-500/30'
                  : isStandardMode
                    ? 'text-sky-600 dark:text-sky-400 border-sky-500/30'
                    : undefined
              }
            >
              {data?.broker_reachable
                ? 'Broker Reachable'
                : isStandardMode
                  ? 'Standard Mode · In-Process Scheduler'
                  : 'Broker Unreachable / Standard Mode'}
            </Badge>
          )}
          {!isStandardMode && data?.workers?.error ? (
            <span className="text-xs text-amber-600 dark:text-amber-400 font-mono">
              Inspect warning: {data.workers.error}
            </span>
          ) : null}
          {error ? (
            <span role="alert" className="text-xs text-red-500">
              Error: {(error as Error).message}
            </span>
          ) : null}
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleRefresh}
          disabled={isLoading || isRefreshing}
          data-testid="admin-queue-refresh-btn"
          className="gap-1.5"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      <div
        data-testid="admin-queue-trends-reservation"
        className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
        {isStandardMode ? (
          <>
            <AnalyticsCard
              title="Standard Ingest Summary"
              description="Tracked synchronous file ingestion and quarantine state across all services."
              className="min-h-[200px]"
              isLoading={isLoading}
              isFetching={isFetching}
              loadingMessage="Loading ingest telemetry..."
              error={error as AnalyticsCardError | null}
            >
              <div className="grid grid-cols-2 gap-4 pt-1" data-testid="admin-queue-standard-ingest-summary">
                <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
                  <div className="text-xs text-muted-foreground uppercase">Ingested Files</div>
                  <div className="text-2xl font-semibold tabular-nums mt-0.5">
                    {standardIngestedFiles.toLocaleString()}
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5 truncate">
                    Tracked in PostgreSQL ingested_files
                  </div>
                </div>
                <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
                  <div className="text-xs text-muted-foreground uppercase">Quarantined Items</div>
                  <div
                    className={`text-2xl font-semibold tabular-nums mt-0.5 ${
                      standardQuarantinedItems > 0 ? 'text-amber-600 dark:text-amber-400' : ''
                    }`}
                  >
                    {standardQuarantinedItems.toLocaleString()}
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5 truncate">
                    Corrupt files &amp; lines preserved on disk
                  </div>
                </div>
              </div>
            </AnalyticsCard>

            <AnalyticsCard
              title="In-Process Scheduler Overview"
              description="APScheduler background cron tasks and active PostgreSQL job leases."
              className="min-h-[200px]"
              isLoading={isLoading}
              isFetching={isFetching}
              loadingMessage="Loading scheduler telemetry..."
              error={error as AnalyticsCardError | null}
            >
              <div className="grid grid-cols-2 gap-4 pt-1" data-testid="admin-queue-standard-scheduler-summary">
                <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
                  <div className="text-xs text-muted-foreground uppercase">Registered Schedules</div>
                  <div className="text-2xl font-semibold tabular-nums mt-0.5">
                    {inprocessSchedules.length}
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5 truncate">
                    Active APScheduler jobs
                  </div>
                </div>
                <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
                  <div className="text-xs text-muted-foreground uppercase">Active Cron Runs</div>
                  <div className="text-2xl font-semibold tabular-nums mt-0.5">
                    {inprocessActiveRuns.length}
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5 truncate">
                    Currently executing in-process jobs
                  </div>
                </div>
              </div>
            </AnalyticsCard>
          </>
        ) : (
          <>
            <AnalyticsCard
              title="Total Queue Depth (1h)"
              description="Aggregate sum of all Celery queue lengths."
              className="min-h-[200px]"
              isLoading={isLoading || trendsLoading}
              isFetching={trendsFetching}
              loadingMessage="Loading queue telemetry..."
              error={trendsError as AnalyticsCardError | null}
              isEmpty={!isLoading && !trendsLoading && !trendsError && queuePoints.length === 0}
            >
              <div className="flex items-baseline justify-between mb-1">
                <div className="text-2xl font-semibold tabular-nums">
                  {latestQueue === null ? '–' : latestQueue}
                </div>
                <div className="text-xs text-muted-foreground">
                  {queuePoints.length > 0 ? `${queuePoints.length} samples` : 'no samples yet'}
                </div>
              </div>
              <div className="h-[64px] text-foreground/70">
                <Sparkline
                  points={isLoading ? [] : queuePoints}
                  height={64}
                  label="Queue Depth"
                  formatValue={(v) => String(v)}
                />
              </div>
            </AnalyticsCard>

            <AnalyticsCard
              title="Active Tasks (1h)"
              description="Total actively executing Celery tasks across all workers."
              className="min-h-[200px]"
              isLoading={isLoading || trendsLoading}
              isFetching={trendsFetching}
              loadingMessage="Loading queue telemetry..."
              error={trendsError as AnalyticsCardError | null}
              isEmpty={!isLoading && !trendsLoading && !trendsError && workerPoints.length === 0}
            >
              <div className="flex items-baseline justify-between mb-1">
                <div className="text-2xl font-semibold tabular-nums">
                  {latestWorker === null ? '–' : latestWorker}
                </div>
                <div className="text-xs text-muted-foreground">
                  {workerPoints.length > 0 ? `${workerPoints.length} samples` : 'no samples yet'}
                </div>
              </div>
              <div className="h-[64px] text-foreground/70">
                <Sparkline
                  points={isLoading ? [] : workerPoints}
                  height={64}
                  label="Active Tasks"
                  formatValue={(v) => String(v)}
                />
              </div>
            </AnalyticsCard>
          </>
        )}
      </div>

      <div
        data-testid="admin-queue-depths-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
        {isStandardMode ? (
          <AnalyticsCard
            title={`Quarantined Files & Corrupt Lines (${standardQuarantinedItems})`}
            helpTitle="Quarantined Files & Corrupt Lines"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Inspecting quarantine directory..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  Displays corrupt raw archive files and malformed JSON log lines isolated on local disk during synchronous ingest.
                </p>
                <p>
                  Operators can inspect or download quarantined artifacts for forensic debugging or purge them once resolved.
                </p>
              </div>
            }
          >
            {standardQuarantinedItems === 0 ? (
              <p className="text-sm text-muted-foreground">
                No corrupt files or malformed log lines are currently quarantined on disk.
              </p>
            ) : (
              <QuarantineSection />
            )}
          </AnalyticsCard>
        ) : (
          <AnalyticsCard
            title="Queue Depths"
            helpTitle="Queue Depths"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Inspecting Celery task broker..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  This table shows the current number of pending messages waiting to be processed in each Celery queue.
                </p>
                <ul className="list-disc pl-5 space-y-2">
                  <li><strong>celery:</strong> The default fallback queue.</li>
                  <li><strong>q.ingest:</strong> Primary ingest queue for discovering and batch-processing log files.</li>
                  <li><strong>q.control:</strong> Control-plane queue for commits, compaction, and maintenance tasks.</li>
                </ul>
              </div>
            }
          >
            {queues.length === 0 ? (
              <p className="text-sm text-muted-foreground">No queues found or Celery mode is inactive.</p>
            ) : (
              <div data-testid="admin-queue-depths-table">
                <DataTableReadonly
                  columns={QUEUE_COLUMNS}
                  data={queues}
                  hideToolbar
                  showPagination={false}
                  tableCaption="Celery queue depths"
                />
              </div>
            )}
          </AnalyticsCard>
        )}
      </div>

      <div
        data-testid="admin-queue-workers-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
        {isStandardMode ? (
          <AnalyticsCard
            title={`Active In-Process Jobs (${inprocessActiveRuns.length})`}
            helpTitle="Active In-Process Jobs"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Inspecting active cron leases..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  Displays all background cron tasks currently holding an active execution lease in PostgreSQL (`job_runs`).
                </p>
                <p>
                  In Standard deployment mode, background sync, commit, compaction, and maintenance tasks execute in-process via APScheduler rather than external Celery workers.
                </p>
              </div>
            }
          >
            {inprocessActiveRuns.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No background cron jobs are actively executing right now (scheduler is idle between ticks).
              </p>
            ) : (
              <div data-testid="admin-queue-inprocess-active-table">
                <DataTableReadonly
                  columns={INPROCESS_ACTIVE_COLUMNS}
                  data={inprocessActiveRuns}
                  hideToolbar
                  showPagination={false}
                  tableCaption="Active in-process cron jobs"
                />
              </div>
            )}
          </AnalyticsCard>
        ) : (
          <AnalyticsCard
            title={`Active Workers (${workerCount})`}
            helpTitle="Active Workers"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Inspecting Celery task broker..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  Displays all currently running Celery worker nodes and the tasks they are actively executing.
                </p>
                <p>
                  If a worker is listed but has no active tasks, it is idle and waiting for messages from the queue.
                </p>
              </div>
            }
          >
            {activeWorkers.length === 0 ? (
              <p className="text-sm text-muted-foreground">No active workers found.</p>
            ) : (
              <div className="space-y-6" data-testid="admin-queue-workers-list">
                {activeWorkers.map(([workerName, tasks]) => (
                  <div key={workerName}>
                    <h4 className="font-mono text-sm font-semibold mb-2 flex items-center">
                      {workerName}
                      <Badge variant="outline" className="ml-2">
                        {tasks.length} active task{tasks.length === 1 ? '' : 's'}
                      </Badge>
                    </h4>
                    {tasks.length > 0 ? (
                      <DataTableReadonly
                        columns={TASK_COLUMNS}
                        data={tasks}
                        hideToolbar
                        showPagination={false}
                        tableCaption={`Active tasks on ${workerName}`}
                      />
                    ) : (
                      <p className="text-xs text-muted-foreground italic">Idle</p>
                    )}
                  </div>
                ))}
              </div>
            )}
          </AnalyticsCard>
        )}
      </div>

      <div
        data-testid="admin-queue-ledger-reservation"
        className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]"
      >
        <AnalyticsCard
          title="Ingest Ledger Status"
          helpTitle="Ingest Ledger Status"
          className="min-h-[240px]"
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading queue telemetry..."
          error={error as AnalyticsCardError | null}
          helpContent={
            <div className="space-y-4 text-sm">
              <p>
                Shows the global sum of tracked file ingestion states across all services in the PostgreSQL ingest_ledger table.
              </p>
              <ul className="list-disc pl-5 space-y-2">
                <li><strong>DISCOVERED:</strong> Found in FOS, waiting to be claimed by a worker.</li>
                <li><strong>CLAIMED:</strong> Currently being downloaded, transformed, or queued for commit.</li>
                <li><strong>COMMITTED:</strong> Fully ingested into the long-term DuckLake / ClickHouse table.</li>
                <li><strong>QUARANTINED:</strong> Skipped due to corruption or parsing errors.</li>
                <li><strong>DEAD_LETTER:</strong> Failed all retries and needs operator attention.</li>
              </ul>
            </div>
          }
        >
          {!data?.ledger || Object.keys(data.ledger).length === 0 ? (
            <p className="text-sm text-muted-foreground">Ledger is empty.</p>
          ) : (
            <div className="space-y-4">
              <div className="flex flex-wrap gap-4" data-testid="admin-queue-ledger-cards">
                {Object.entries(data.ledger).map(([status, count]) => {
                  const isDeadLetter = status === 'dead_letter' && count > 0
                  const isQuarantined = status === 'quarantined' && count > 0
                  const isSelected = ledgerFilter === status
                  const validFilterStatus = (
                    ['discovered', 'claimed', 'committed', 'quarantined', 'dead_letter'] as const
                  ).includes(status as 'discovered' | 'claimed' | 'committed' | 'quarantined' | 'dead_letter')
                  return (
                    <button
                      type="button"
                      key={status}
                      data-testid={`admin-queue-ledger-${status}`}
                      onClick={() => {
                        if (!validFilterStatus) return
                        setLedgerFilter((prev) =>
                          prev === status ? 'problem' : (status as LedgerStatusFilter),
                        )
                      }}
                      className={[
                        'flex flex-col items-center justify-center p-4 border rounded-lg bg-card min-w-[120px] transition-colors text-left cursor-pointer hover:border-foreground/30',
                        isDeadLetter
                          ? 'border-red-500/40 bg-red-500/5 text-red-600 dark:text-red-400'
                          : isQuarantined
                            ? 'border-amber-500/40 bg-amber-500/5 text-amber-600 dark:text-amber-400'
                            : '',
                        isSelected ? 'ring-2 ring-primary/40 border-primary/50' : '',
                      ]
                        .filter(Boolean)
                        .join(' ')}
                    >
                      <span className="text-3xl font-bold tabular-nums">{count}</span>
                      <span className="text-xs text-muted-foreground uppercase mt-1">{status}</span>
                    </button>
                  )
                })}
              </div>

              {actionNotice ? (
                <div
                  role="status"
                  data-testid="admin-queue-ledger-notice"
                  className={`flex items-center justify-between gap-2 rounded-md border px-3 py-2 text-xs ${
                    actionNotice.kind === 'error'
                      ? 'border-red-500/40 bg-red-500/10 text-red-600 dark:text-red-400'
                      : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                  }`}
                >
                  <span>{actionNotice.message}</span>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-6 px-2 text-xs"
                    onClick={() => setActionNotice(null)}
                  >
                    Dismiss
                  </Button>
                </div>
              ) : null}

              {!shouldInspectLedger ? (
                <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-border/60 bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
                  <span className="flex items-center gap-1.5">
                    <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                    No dead-letter or quarantined files requiring operator attention. Click any status card above to inspect ledger rows.
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-xs"
                    data-testid="admin-queue-ledger-inspect-all-btn"
                    onClick={() => setLedgerFilter('all')}
                  >
                    Inspect Recent Entries
                  </Button>
                </div>
              ) : (
                <div
                  data-testid="admin-queue-ledger-inspector"
                  className="rounded-lg border border-border bg-muted/10 p-3 space-y-3"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="flex flex-wrap items-center gap-1.5">
                      <span className="text-xs font-medium text-muted-foreground mr-1 flex items-center gap-1">
                        {problemCount > 0 ? (
                          <AlertTriangle className="h-3.5 w-3.5 text-amber-500" />
                        ) : null}
                        Filter:
                      </span>
                      {(
                        [
                          ['problem', `Needs Attention (${problemCount})`],
                          ['dead_letter', `Dead Letter (${deadLetterCount})`],
                          ['quarantined', `Quarantined (${quarantinedCount})`],
                          ['all', 'All Recent'],
                        ] as const
                      ).map(([filterKey, label]) => (
                        <Button
                          key={filterKey}
                          type="button"
                          variant={ledgerFilter === filterKey ? 'secondary' : 'ghost'}
                          size="sm"
                          data-testid={`admin-queue-ledger-filter-${filterKey}`}
                          className="h-7 px-2.5 text-xs"
                          onClick={() => setLedgerFilter(filterKey)}
                        >
                          {label}
                        </Button>
                      ))}
                    </div>

                    {problemCount > 0 &&
                    (ledgerFilter === 'problem' ||
                      ledgerFilter === 'dead_letter' ||
                      ledgerFilter === 'quarantined') ? (
                      <div className="flex flex-wrap items-center gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-7 gap-1 text-xs"
                          disabled={actionBusyKey !== null}
                          data-testid="admin-queue-ledger-retry-all"
                          onClick={() => void handleBulkRetry()}
                        >
                          <RotateCcw className="h-3 w-3" />
                          Retry All
                        </Button>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-7 gap-1 text-xs text-red-600 dark:text-red-400 hover:text-red-700"
                          disabled={actionBusyKey !== null}
                          data-testid="admin-queue-ledger-purge-all"
                          onClick={() => void handleBulkPurge()}
                        >
                          <Trash2 className="h-3 w-3" />
                          Dismiss All
                        </Button>
                      </div>
                    ) : null}
                  </div>

                  {ledgerError ? (
                    <p role="alert" className="text-xs text-red-500">
                      Failed to load ledger entries: {(ledgerError as Error).message}
                    </p>
                  ) : ledgerLoading ? (
                    <div className="space-y-2 py-2" data-testid="admin-queue-ledger-loading">
                      <Skeleton className="h-10 w-full" />
                      <Skeleton className="h-10 w-full" />
                    </div>
                  ) : !ledgerData?.items || ledgerData.items.length === 0 ? (
                    <p className="text-xs text-muted-foreground py-2">
                      No ingest ledger entries match filter <span className="font-mono">{ledgerFilter}</span>.
                    </p>
                  ) : (
                    <div
                      className="max-h-[380px] overflow-auto rounded-md border border-border bg-card"
                      data-testid="admin-queue-ledger-entries"
                    >
                      <table className="w-full text-xs border-collapse">
                        <thead className="bg-muted/50 text-muted-foreground sticky top-0 z-10 border-b">
                          <tr>
                            <th className="text-left font-medium px-3 py-2 whitespace-nowrap">Status</th>
                            <th className="text-left font-medium px-3 py-2 whitespace-nowrap">Source</th>
                            <th className="text-left font-medium px-3 py-2 whitespace-nowrap">Object Key &amp; Error Details</th>
                            <th className="text-left font-medium px-3 py-2 whitespace-nowrap">Attempts / Task</th>
                            <th className="text-left font-medium px-3 py-2 whitespace-nowrap">Last Activity</th>
                            <th className="text-right font-medium px-3 py-2 whitespace-nowrap">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-border">
                          {ledgerData.items.map((item) => {
                            const isProblemRow =
                              item.status === 'dead_letter' || item.status === 'quarantined'
                            const tsSec =
                              item.claimed_at ?? item.discovered_at ?? item.committed_at ?? null
                            const sizeLabel = formatBytes(item.size_bytes)
                            const rowBusyRetry =
                              actionBusyKey === `retry:${item.service_id}:${item.object_key}`
                            const rowBusyPurge =
                              actionBusyKey === `purge:${item.service_id}:${item.object_key}`
                            return (
                              <tr
                                key={`${item.service_id}:${item.object_key}`}
                                data-testid="admin-queue-ledger-row"
                                className="hover:bg-muted/30 align-top"
                              >
                                <td className="px-3 py-2.5 whitespace-nowrap">
                                  <Badge
                                    variant={item.status === 'dead_letter' ? 'destructive' : 'outline'}
                                    className={
                                      item.status === 'quarantined'
                                        ? 'border-amber-500/40 text-amber-600 dark:text-amber-400'
                                        : item.status === 'committed'
                                          ? 'border-emerald-500/40 text-emerald-600 dark:text-emerald-400'
                                          : undefined
                                    }
                                  >
                                    {item.status}
                                  </Badge>
                                </td>
                                <td className="px-3 py-2.5 whitespace-nowrap">
                                  <div className="flex flex-col gap-0.5">
                                    <Badge variant="secondary" className="w-fit uppercase text-[10px]">
                                      {item.source_type}
                                    </Badge>
                                    <span className="font-mono text-[11px] text-muted-foreground">
                                      {item.service_id}
                                    </span>
                                  </div>
                                </td>
                                <td className="px-3 py-2.5 min-w-[280px]">
                                  <div className="font-mono text-xs break-all text-foreground">
                                    {item.object_key}
                                  </div>
                                  {item.error_kind || item.last_error ? (
                                    <div className="mt-1 flex flex-wrap items-baseline gap-1.5 text-[11px] text-red-600 dark:text-red-400">
                                      {item.error_kind ? (
                                        <span className="rounded bg-red-500/10 px-1.5 py-0.5 font-mono font-medium">
                                          {item.error_kind}
                                        </span>
                                      ) : null}
                                      {item.last_error ? (
                                        <span className="font-mono break-all">{item.last_error}</span>
                                      ) : null}
                                    </div>
                                  ) : null}
                                  {sizeLabel ? (
                                    <div className="mt-0.5 text-[11px] text-muted-foreground">
                                      Size: {sizeLabel}
                                    </div>
                                  ) : null}
                                </td>
                                <td className="px-3 py-2.5 whitespace-nowrap">
                                  <div className="tabular-nums font-medium">
                                    {item.attempts ?? 0} attempt{(item.attempts ?? 0) === 1 ? '' : 's'}
                                  </div>
                                  {item.originating_task ? (
                                    <div className="font-mono text-[11px] text-muted-foreground">
                                      {item.originating_task}
                                      {item.originating_run_id ? ` #${item.originating_run_id}` : ''}
                                    </div>
                                  ) : null}
                                </td>
                                <td className="px-3 py-2.5 whitespace-nowrap text-muted-foreground">
                                  {tsSec ? formatLocalDateTime(tsSec * 1000) : '—'}
                                </td>
                                <td className="px-3 py-2.5 whitespace-nowrap text-right">
                                  {isProblemRow ? (
                                    <div className="inline-flex items-center gap-1.5">
                                      <Button
                                        type="button"
                                        variant="outline"
                                        size="sm"
                                        className="h-7 px-2 text-xs gap-1"
                                        disabled={actionBusyKey !== null}
                                        data-testid="admin-queue-ledger-retry-row"
                                        onClick={() => void handleRetryItem(item)}
                                      >
                                        <RotateCcw
                                          className={`h-3 w-3 ${rowBusyRetry ? 'animate-spin' : ''}`}
                                        />
                                        Retry
                                      </Button>
                                      <Button
                                        type="button"
                                        variant="ghost"
                                        size="sm"
                                        className="h-7 px-2 text-xs gap-1 text-red-600 dark:text-red-400 hover:text-red-700"
                                        disabled={actionBusyKey !== null}
                                        data-testid="admin-queue-ledger-purge-row"
                                        onClick={() => void handlePurgeItem(item)}
                                      >
                                        <Trash2 className="h-3 w-3" />
                                        Dismiss
                                      </Button>
                                    </div>
                                  ) : (
                                    <span className="text-muted-foreground">—</span>
                                  )}
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </AnalyticsCard>
      </div>

      <div
        data-testid="admin-queue-schedules-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
        {isStandardMode ? (
          <AnalyticsCard
            title={`In-Process Schedules (${inprocessSchedules.length})`}
            helpTitle="In-Process Schedules"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading scheduler telemetry..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  Displays all registered background schedules running in-process via APScheduler in Standard deployment mode.
                </p>
                <p>
                  These recurrent jobs handle log discovery, DuckLake commits, local compaction, rollup healing, cloud maintenance, and alert evaluation.
                </p>
              </div>
            }
          >
            {inprocessSchedules.length === 0 ? (
              <p className="text-sm text-muted-foreground">No in-process schedules registered.</p>
            ) : (
              <div data-testid="admin-queue-inprocess-schedules-table">
                <DataTableReadonly
                  columns={INPROCESS_SCHEDULE_COLUMNS}
                  data={inprocessSchedules}
                  hideToolbar
                  showPagination={false}
                  tableCaption="In-process APScheduler jobs"
                />
              </div>
            )}
          </AnalyticsCard>
        ) : (
          <AnalyticsCard
            title={`RedBeat Schedules (${schedules.length})`}
            helpTitle="RedBeat Schedules"
            className="min-h-[200px]"
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading queue telemetry..."
            error={error as AnalyticsCardError | null}
            helpContent={
              <div className="space-y-4 text-sm">
                <p>
                  Displays all registered background schedules (crons/intervals) stored in Valkey/Redis via RedBeat.
                </p>
                <p>
                  These are responsible for ticking recurrent operations like syncing logs, compacting storage, and gathering metrics.
                </p>
              </div>
            }
          >
            {schedules.length === 0 ? (
              <p className="text-sm text-muted-foreground">No schedules found.</p>
            ) : (
              <div data-testid="admin-queue-schedules-table">
                <DataTableReadonly
                  columns={SCHEDULE_COLUMNS}
                  data={schedules}
                  hideToolbar
                  showPagination={false}
                  tableCaption="RedBeat schedules"
                />
              </div>
            )}
          </AnalyticsCard>
        )}
      </div>
    </div>
  )
}
