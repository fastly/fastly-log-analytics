'use client'

import React from 'react'
import { RefreshCw } from 'lucide-react'
import { formatLocalDateTime } from '@/lib/date'
import { useQuery } from '@tanstack/react-query'
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

type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
type ActiveTask = components['schemas']['CeleryActiveTask']
type ScheduleEntry = components['schemas']['CeleryScheduleEntry']
type QueueDepth = [name: string, count: number]

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

export function QueueClient() {
  const { state: sseConnected } = useAdminEventStream(true, ['celery-status'])

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

  const isRefreshing = (isFetching && !isLoading) || (trendsFetching && !trendsLoading)

  const handleRefresh = React.useCallback(() => {
    void refetch()
    void refetchTrends()
  }, [refetch, refetchTrends])

  const queues = data?.queues ? Object.entries(data.queues).sort((a, b) => a[0].localeCompare(b[0])) : []
  const activeWorkers = data?.workers?.active_tasks ? Object.entries(data.workers.active_tasks) : []
  const schedules = data?.schedules || []
  const workerCount = data?.workers?.count ?? 0

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
                Inspecting Celery task broker...
              </span>
            </>
          ) : (
            <Badge
              variant={data?.broker_reachable ? 'outline' : 'secondary'}
              data-testid="admin-queue-broker-badge"
              className={
                data?.broker_reachable
                  ? 'text-emerald-600 dark:text-emerald-400 border-emerald-500/30'
                  : undefined
              }
            >
              {data?.broker_reachable ? 'Broker Reachable' : 'Broker Unreachable / Standard Mode'}
            </Badge>
          )}
          {data?.workers?.error ? (
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
        <AnalyticsCard
          title="Total Queue Depth (1h)"
          description="Aggregate sum of all Celery queue lengths."
          className="min-h-[200px]"
          isLoading={trendsLoading}
          isFetching={trendsFetching}
          loadingMessage="Loading queue telemetry..."
          error={trendsError as AnalyticsCardError | null}
          isEmpty={!trendsLoading && !trendsError && queuePoints.length === 0}
        >
          <div className="flex items-baseline justify-between mb-2">
            <div className="text-2xl font-semibold tabular-nums">
              {latestQueue === null ? '–' : latestQueue}
            </div>
            <div className="text-xs text-muted-foreground">
              {queuePoints.length > 0 ? `${queuePoints.length} samples` : 'no samples yet'}
            </div>
          </div>
          <div className="text-foreground/70">
            <Sparkline
              points={queuePoints}
              height={80}
              label="Queue Depth"
              formatValue={(v) => String(v)}
            />
          </div>
        </AnalyticsCard>

        <AnalyticsCard
          title="Active Tasks (1h)"
          description="Total actively executing Celery tasks across all workers."
          className="min-h-[200px]"
          isLoading={trendsLoading}
          isFetching={trendsFetching}
          loadingMessage="Loading queue telemetry..."
          error={trendsError as AnalyticsCardError | null}
          isEmpty={!trendsLoading && !trendsError && workerPoints.length === 0}
        >
          <div className="flex items-baseline justify-between mb-2">
            <div className="text-2xl font-semibold tabular-nums">
              {latestWorker === null ? '–' : latestWorker}
            </div>
            <div className="text-xs text-muted-foreground">
              {workerPoints.length > 0 ? `${workerPoints.length} samples` : 'no samples yet'}
            </div>
          </div>
          <div className="text-foreground/70">
            <Sparkline
              points={workerPoints}
              height={80}
              label="Active Tasks"
              formatValue={(v) => String(v)}
            />
          </div>
        </AnalyticsCard>
      </div>

      <div
        data-testid="admin-queue-depths-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
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
      </div>

      <div
        data-testid="admin-queue-workers-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
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
      </div>

      <div
        data-testid="admin-queue-ledger-reservation"
        className="min-h-[180px] [contain-intrinsic-size:auto_180px] [content-visibility:auto]"
      >
        <AnalyticsCard
          title="Ingest Ledger Status"
          helpTitle="Ingest Ledger Status"
          className="min-h-[180px]"
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
            <div className="flex flex-wrap gap-4" data-testid="admin-queue-ledger-cards">
              {Object.entries(data.ledger).map(([status, count]) => (
                <div
                  key={status}
                  data-testid={`admin-queue-ledger-${status}`}
                  className="flex flex-col items-center justify-center p-4 border rounded-lg bg-card min-w-[120px]"
                >
                  <span className="text-3xl font-bold tabular-nums">{count}</span>
                  <span className="text-xs text-muted-foreground uppercase mt-1">{status}</span>
                </div>
              ))}
            </div>
          )}
        </AnalyticsCard>
      </div>

      <div
        data-testid="admin-queue-schedules-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
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
      </div>
    </div>
  )
}
