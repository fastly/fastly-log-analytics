'use client'

import React from 'react'
import { RefreshCw } from 'lucide-react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { BackToAdminLink } from '@/components/ReportLayout'
import { useAdminEventStream } from '@/hooks/useSSE'
import {
  type LedgerEntry,
  type LedgerStatusFilter,
  type TrendBatch,
  InProcessSchedulesSection,
  QueueDepthsAndQuarantineSection,
  QueueTrendsSection,
  QueueWorkersSection,
} from './_sections/QueueOverviewPanels'
import { IngestLedgerSection } from './_sections/QueueLedgerInspector'

export * from './_sections/QueueOverviewPanels'
export * from './_sections/QueueLedgerInspector'

export function QueueClient() {
  const queryClient = useQueryClient()
  const { state: sseConnected } = useAdminEventStream(true, ['celery-status'])
  const [ledgerFilter, setLedgerFilter] = React.useState<LedgerStatusFilter>('problem')
  const [actionBusyKey, setActionBusyKey] = React.useState<string | null>(null)
  const [actionNotice, setActionNotice] = React.useState<{
    kind: 'success' | 'error'
    message: string
  } | null>(null)

  const { data, isLoading, isFetching, isFetched, error, refetch } = useQuery({
    queryKey: queryKeys.admin.celeryStatus(),
    queryFn: async ({ signal }) =>
      unwrapClient(client.GET('/api/admin/celery/status', { signal })),
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
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/admin/celery/ledger', {
          params: { query: { status: ledgerFilter, limit: 50, offset: 0 } },
          signal,
        }),
      ),
    enabled: shouldInspectLedger,
  })

  const {
    data: trends,
    isLoading: trendsLoading,
    isFetching: trendsFetching,
    isFetched: trendsFetched,
    error: trendsError,
    refetch: refetchTrends,
  } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch('1h'),
    queryFn: async ({ signal }) =>
      (await unwrapClient(
        client.GET('/api/admin/metric-history/batch', {
          params: { query: { since: '1h' } },
          signal,
        }),
      )) as TrendBatch,
    refetchInterval: 60_000,
  })

  const isStatusRefetching = isFetching && isFetched && !isLoading
  const isTrendsRefetching = trendsFetching && trendsFetched && !trendsLoading
  const isRefreshing =
    isStatusRefetching || isTrendsRefetching || (ledgerFetching && !ledgerLoading)

  const handleRefresh = React.useCallback(() => {
    void refetch()
    void refetchTrends()
    if (shouldInspectLedger) void refetchLedger()
  }, [refetch, refetchTrends, refetchLedger, shouldInspectLedger])

  const refreshLedgerAfterAction = React.useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.celeryLedgerAll() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.celeryStatus() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.overviewCeleryStatus() }),
    ])
    await Promise.all([refetch(), refetchLedger()])
  }, [queryClient, refetch, refetchLedger])

  const runLedgerAction = React.useCallback(
    async (busyKey: string, errorPrefix: string, action: () => Promise<string>) => {
      setActionBusyKey(busyKey)
      setActionNotice(null)
      try {
        const message = await action()
        setActionNotice({ kind: 'success', message })
        await refreshLedgerAfterAction()
      } catch (err) {
        setActionNotice({
          kind: 'error',
          message: `${errorPrefix}: ${(err as Error).message}`,
        })
      } finally {
        setActionBusyKey(null)
      }
    },
    [refreshLedgerAfterAction],
  )

  const handleRetryItem = React.useCallback(
    (entry: LedgerEntry) =>
      runLedgerAction(
        `retry:${entry.service_id}:${entry.object_key}`,
        'Retry failed',
        async () => {
          const res = await unwrapClient(
            client.POST('/api/admin/celery/ledger/retry', {
              body: {
                items: [{ service_id: entry.service_id, object_key: entry.object_key }],
              },
            }),
          )
          return `Requeued ${res.affected ?? 1} file for ingest retry${
            res.dispatched ? ` (dispatched ${res.dispatched} to Celery)` : ''
          }.`
        },
      ),
    [runLedgerAction],
  )

  const handlePurgeItem = React.useCallback(
    (entry: LedgerEntry) =>
      runLedgerAction(
        `purge:${entry.service_id}:${entry.object_key}`,
        'Dismiss failed',
        async () => {
          const res = await unwrapClient(
            client.POST('/api/admin/celery/ledger/purge', {
              body: {
                items: [{ service_id: entry.service_id, object_key: entry.object_key }],
              },
            }),
          )
          return `Dismissed ${res.affected ?? 1} ledger entry.`
        },
      ),
    [runLedgerAction],
  )

  const bulkTargetStatus: 'dead_letter' | 'quarantined' | 'problem' =
    ledgerFilter === 'dead_letter' || ledgerFilter === 'quarantined'
      ? ledgerFilter
      : 'problem'

  const handleBulkRetry = React.useCallback(
    () =>
      runLedgerAction('bulk-retry', 'Bulk retry failed', async () => {
        const res = await unwrapClient(
          client.POST('/api/admin/celery/ledger/retry', {
            body: { status: bulkTargetStatus },
          }),
        )
        return `Requeued ${res.affected ?? 0} file(s) for retry${
          res.dispatched ? ` (dispatched ${res.dispatched} to Celery)` : ''
        }.`
      }),
    [bulkTargetStatus, runLedgerAction],
  )

  const handleBulkPurge = React.useCallback(
    () =>
      runLedgerAction('bulk-purge', 'Bulk dismiss failed', async () => {
        const res = await unwrapClient(
          client.POST('/api/admin/celery/ledger/purge', {
            body: { status: bulkTargetStatus },
          }),
        )
        return `Dismissed ${res.affected ?? 0} dead-letter/quarantined ledger entry(s).`
      }),
    [bulkTargetStatus, runLedgerAction],
  )

  const isStandardMode =
    data?.deployment_mode === 'standard' && !data?.broker_reachable
  const queues = data?.queues
    ? Object.entries(data.queues).sort((a, b) => a[0].localeCompare(b[0]))
    : []
  const activeWorkers = data?.workers?.active_tasks
    ? Object.entries(data.workers.active_tasks)
    : []
  const workerCount = data?.workers?.count ?? 0
  const inprocessSchedules = data?.inprocess_schedules || []
  const inprocessActiveRuns = data?.inprocess_active_runs || []
  const standardIngestedFiles = data?.standard_ingest?.ingested_files ?? 0
  const standardQuarantinedItems = data?.standard_ingest?.quarantined_items ?? 0

  const queuePoints = trends?.series?.['celery_queue_depth'] ?? []
  const workerPoints = trends?.series?.['celery_active_tasks'] ?? []
  const latestQueue = queuePoints.length
    ? queuePoints[queuePoints.length - 1].value
    : null
  const latestWorker = workerPoints.length
    ? workerPoints[workerPoints.length - 1].value
    : null

  return (
    <div className="grid gap-6 min-w-0 overflow-hidden" data-testid="admin-queue-client">
      <div
        data-testid="admin-queue-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 min-h-[36px]"
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

      <QueueTrendsSection
        isStandardMode={isStandardMode}
        isLoading={isLoading}
        isStatusRefetching={isStatusRefetching}
        error={error}
        standardIngestedFiles={standardIngestedFiles}
        standardQuarantinedItems={standardQuarantinedItems}
        inprocessSchedulesCount={inprocessSchedules.length}
        inprocessActiveRunsCount={inprocessActiveRuns.length}
        latestQueue={latestQueue}
        queuePoints={queuePoints}
        latestWorker={latestWorker}
        workerPoints={workerPoints}
        trendsLoading={trendsLoading}
        isTrendsRefetching={isTrendsRefetching}
        trendsError={trendsError}
      />

      <QueueDepthsAndQuarantineSection
        isStandardMode={isStandardMode}
        isLoading={isLoading}
        isStatusRefetching={isStatusRefetching}
        error={error}
        standardQuarantinedItems={standardQuarantinedItems}
        queues={queues}
      />

      <QueueWorkersSection
        isStandardMode={isStandardMode}
        isLoading={isLoading}
        isStatusRefetching={isStatusRefetching}
        error={error}
        inprocessActiveRuns={inprocessActiveRuns}
        workerCount={workerCount}
        activeWorkers={activeWorkers}
      />

      <IngestLedgerSection
        isLoading={isLoading}
        isStatusRefetching={isStatusRefetching}
        error={error}
        deploymentMode={data?.deployment_mode}
        ledger={data?.ledger}
        ledgerFilter={ledgerFilter}
        setLedgerFilter={setLedgerFilter}
        actionNotice={actionNotice}
        setActionNotice={setActionNotice}
        shouldInspectLedger={shouldInspectLedger}
        problemCount={problemCount}
        deadLetterCount={deadLetterCount}
        quarantinedCount={quarantinedCount}
        actionBusyKey={actionBusyKey}
        handleBulkRetry={handleBulkRetry}
        handleBulkPurge={handleBulkPurge}
        ledgerError={ledgerError}
        ledgerLoading={ledgerLoading}
        ledgerItems={ledgerData?.items}
        handleRetryItem={handleRetryItem}
        handlePurgeItem={handlePurgeItem}
      />

      <InProcessSchedulesSection
        isLoading={isLoading}
        isStatusRefetching={isStatusRefetching}
        error={error}
        inprocessSchedules={inprocessSchedules}
      />
    </div>
  )
}

export default function AdminQueuePage() {
  return (
    <div className="space-y-6" data-testid="admin-queue-root">
      <PageHeader
        title="Queue & Workers"
        description="Monitor background job schedules, active workers, queue depths, and dead-letter / quarantine state."
      >
        <BackToAdminLink />
      </PageHeader>
      <QueueClient />
    </div>
  )
}
