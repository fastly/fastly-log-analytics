'use client'

import React from 'react'
import { formatLocalDateTime, formatCount } from '@/lib/utils'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { QuarantineSection } from '@/app/control-room/_sections/ControlRoomClient'
import type { components } from '@/types/api.generated'

export type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
export type ActiveTask = components['schemas']['CeleryActiveTask']
export type InProcessScheduleEntry = components['schemas']['InProcessScheduleEntry']
export type InProcessActiveRun = components['schemas']['InProcessActiveRun']
export type LedgerEntry = components['schemas']['IngestLedgerEntry']
export type QueueDepth = [name: string, count: number]
export type LedgerStatusFilter =
  | 'problem'
  | 'all'
  | 'dead_letter'
  | 'quarantined'
  | 'discovered'
  | 'claimed'
  | 'committed'

export const VALID_LEDGER_STATUSES = [
  'discovered',
  'claimed',
  'committed',
  'quarantined',
  'dead_letter',
] as const

function makeCol<T>(
  id: string,
  header: string,
  size: number,
  accessorFn: (row: T) => string | number,
  render: (row: T) => React.ReactNode,
  enableSorting = true,
): ColumnDef<T> {
  return {
    id,
    header,
    size,
    accessorFn,
    enableSorting,
    cell: ({ row }) => render(row.original),
  }
}

export const QUEUE_COLUMNS: ColumnDef<QueueDepth>[] = [
  makeCol('queue', 'Queue Name', 320, (q) => q[0], (q) => (
    <span className="font-mono text-sm">{q[0]}</span>
  )),
  makeCol('messages', 'Pending Messages', 160, (q) => q[1], (q) => (
    <span className="text-sm font-medium">{q[1]}</span>
  )),
]

export const TASK_COLUMNS: ColumnDef<ActiveTask>[] = [
  makeCol(
    'id',
    'ID',
    300,
    (t) => t.id,
    (t) => <span className="font-mono text-xs">{t.id}</span>,
    false,
  ),
  makeCol(
    'name',
    'Name',
    260,
    (t) => t.name,
    (t) => <span className="text-sm font-medium">{t.name}</span>,
    false,
  ),
  makeCol(
    'args',
    'Args',
    240,
    (t) => JSON.stringify(t.args),
    (t) => (
      <span className="font-mono text-xs text-muted-foreground">
        {JSON.stringify(t.args)}
      </span>
    ),
    false,
  ),
  makeCol(
    'start',
    'Start Time',
    200,
    (t) => t.time_start,
    (t) => <span className="text-xs">{formatLocalDateTime(t.time_start * 1000)}</span>,
    false,
  ),
]

export const INPROCESS_SCHEDULE_COLUMNS: ColumnDef<InProcessScheduleEntry>[] = [
  makeCol('name', 'Job ID', 320, (s) => s.name, (s) => (
    <span className="font-mono text-xs font-medium">{s.name}</span>
  )),
  makeCol('trigger', 'Trigger / Cadence', 280, (s) => s.trigger, (s) => (
    <span className="font-mono text-xs text-muted-foreground">{s.trigger}</span>
  )),
  makeCol('next_run_at', 'Next Scheduled Run', 240, (s) => s.next_run_at ?? 0, (s) => (
    <span className="text-xs tabular-nums">
      {s.next_run_at ? formatLocalDateTime(s.next_run_at * 1000) : '—'}
    </span>
  )),
]

export const INPROCESS_ACTIVE_COLUMNS: ColumnDef<InProcessActiveRun>[] = [
  makeCol(
    'run_id',
    'Run ID',
    140,
    (r) => r.run_id,
    (r) => <span className="font-mono text-xs">#{r.run_id}</span>,
    false,
  ),
  makeCol(
    'task',
    'Task',
    260,
    (r) => r.task ?? '—',
    (r) => <span className="font-mono text-xs font-medium">{r.task ?? '—'}</span>,
    false,
  ),
  makeCol(
    'service_id',
    'Service ID',
    260,
    (r) => r.service_id ?? 'global',
    (r) => (
      <span className="font-mono text-xs text-muted-foreground">
        {r.service_id ?? 'global'}
      </span>
    ),
    false,
  ),
  makeCol(
    'started_at',
    'Started At',
    220,
    (r) => r.started_at ?? 0,
    (r) => (
      <span className="text-xs tabular-nums">
        {r.started_at ? formatLocalDateTime(r.started_at * 1000) : '—'}
      </span>
    ),
    false,
  ),
]

export function SummaryStatTile({
  label,
  value,
  sublabel,
  valueClassName = '',
}: {
  label: string
  value: React.ReactNode
  sublabel: React.ReactNode
  valueClassName?: string
}) {
  return (
    <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
      <div className="text-xs text-muted-foreground uppercase">{label}</div>
      <div className={`text-2xl font-semibold tabular-nums mt-0.5 ${valueClassName}`}>
        {value}
      </div>
      <div className="text-[11px] text-muted-foreground mt-0.5 truncate">{sublabel}</div>
    </div>
  )
}

export function TrendSparklineCard({
  title,
  description,
  label,
  latestValue,
  points,
  isLoading,
  trendsLoading,
  isTrendsRefetching,
  trendsError,
}: {
  title: string
  description: string
  label: string
  latestValue: number | null
  points: React.ComponentProps<typeof Sparkline>['points']
  isLoading: boolean
  trendsLoading: boolean
  isTrendsRefetching: boolean
  trendsError: unknown
}) {
  return (
    <AnalyticsCard
      title={title}
      description={description}
      className="min-h-[204px]"
      isLoading={isLoading || trendsLoading}
      isFetching={isTrendsRefetching}
      loadingMessage="Loading queue telemetry..."
      error={trendsError as AnalyticsCardError | null}
      isEmpty={!isLoading && !trendsLoading && !trendsError && points.length === 0}
    >
      <div className="flex items-baseline justify-between mb-1">
        <div className="text-2xl font-semibold tabular-nums">
          {latestValue === null ? '–' : latestValue}
        </div>
        <div className="text-xs text-muted-foreground">
          {points.length > 0 ? `${points.length} samples` : 'no samples yet'}
        </div>
      </div>
      <div className="h-[64px] text-foreground/70">
        <Sparkline
          points={isLoading ? [] : points}
          height={64}
          label={label}
          formatValue={(v) => String(v)}
        />
      </div>
    </AnalyticsCard>
  )
}

export function QueueTrendsSection({
  isStandardMode,
  isLoading,
  isStatusRefetching,
  error,
  standardIngestedFiles,
  standardQuarantinedItems,
  inprocessSchedulesCount,
  inprocessActiveRunsCount,
  latestQueue,
  queuePoints,
  latestWorker,
  workerPoints,
  trendsLoading,
  isTrendsRefetching,
  trendsError,
}: {
  isStandardMode: boolean
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  standardIngestedFiles: number
  standardQuarantinedItems: number
  inprocessSchedulesCount: number
  inprocessActiveRunsCount: number
  latestQueue: number | null
  queuePoints: React.ComponentProps<typeof Sparkline>['points']
  latestWorker: number | null
  workerPoints: React.ComponentProps<typeof Sparkline>['points']
  trendsLoading: boolean
  isTrendsRefetching: boolean
  trendsError: unknown
}) {
  return (
    <div
      data-testid="admin-queue-trends-reservation"
      className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[204px] [contain-intrinsic-size:auto_204px] [content-visibility:auto]"
    >
      {isStandardMode ? (
        [
          {
            key: 'standard-ingest-summary',
            title: 'Standard Ingest Summary',
            description:
              'Tracked synchronous file ingestion and quarantine state across all services.',
            loadingMessage: 'Loading ingest telemetry...',
            testId: 'admin-queue-standard-ingest-summary',
            tiles: [
              {
                label: 'Ingested Files',
                value: formatCount(standardIngestedFiles),
                sublabel: 'Tracked in PostgreSQL ingested_files',
              },
              {
                label: 'Quarantined Items',
                value: formatCount(standardQuarantinedItems),
                valueClassName:
                  standardQuarantinedItems > 0
                    ? 'text-amber-600 dark:text-amber-400'
                    : '',
                sublabel: 'Corrupt files & lines preserved on disk',
              },
            ],
          },
          {
            key: 'standard-scheduler-overview',
            title: 'In-Process Scheduler Overview',
            description:
              'APScheduler background cron tasks and active PostgreSQL job leases.',
            loadingMessage: 'Loading scheduler telemetry...',
            testId: 'admin-queue-standard-scheduler-summary',
            tiles: [
              {
                label: 'Registered Schedules',
                value: inprocessSchedulesCount,
                sublabel: 'Active APScheduler jobs',
              },
              {
                label: 'Active Cron Runs',
                value: inprocessActiveRunsCount,
                sublabel: 'Currently executing in-process jobs',
              },
            ],
          },
        ].map((card) => (
          <AnalyticsCard
            key={card.key}
            title={card.title}
            description={card.description}
            className="min-h-[204px]"
            isLoading={isLoading}
            isFetching={isStatusRefetching}
            loadingMessage={card.loadingMessage}
            error={error as AnalyticsCardError | null}
          >
            <div
              className="grid grid-cols-2 gap-4 pt-1 min-h-[100px]"
              data-testid={card.testId}
            >
              {card.tiles.map((tile) => (
                <SummaryStatTile key={tile.label} {...tile} />
              ))}
            </div>
          </AnalyticsCard>
        ))
      ) : (
        <>
          <TrendSparklineCard
            key="celery-queue-depth"
            title="Total Queue Depth (1h)"
            description="Aggregate sum of all Celery queue lengths."
            label="Queue Depth"
            latestValue={latestQueue}
            points={queuePoints}
            isLoading={isLoading}
            trendsLoading={trendsLoading}
            isTrendsRefetching={isTrendsRefetching}
            trendsError={trendsError}
          />
          <TrendSparklineCard
            key="celery-active-tasks"
            title="Active Tasks (1h)"
            description="Total actively executing Celery tasks across all workers."
            label="Active Tasks"
            latestValue={latestWorker}
            points={workerPoints}
            isLoading={isLoading}
            trendsLoading={trendsLoading}
            isTrendsRefetching={isTrendsRefetching}
            trendsError={trendsError}
          />
        </>
      )}
    </div>
  )
}

export function QueueDepthsAndQuarantineSection({
  isStandardMode,
  isLoading,
  isStatusRefetching,
  error,
  standardQuarantinedItems,
  queues,
}: {
  isStandardMode: boolean
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  standardQuarantinedItems: number
  queues: QueueDepth[]
}) {
  return (
    <div
      data-testid="admin-queue-depths-reservation"
      className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      {isStandardMode ? (
        <AnalyticsCard
          key="standard-quarantined-files"
          title={`Quarantined Files & Corrupt Lines (${standardQuarantinedItems})`}
          helpTitle="Quarantined Files & Corrupt Lines"
          className="min-h-[200px]"
          isLoading={isLoading}
          isFetching={isStatusRefetching}
          loadingMessage="Inspecting quarantine directory..."
          error={error as AnalyticsCardError | null}
          helpContent={
            <div className="space-y-4 text-sm">
              <p>
                Displays corrupt raw archive files and malformed JSON log lines isolated
                on local disk during synchronous ingest.
              </p>
              <p>
                Operators can inspect or download quarantined artifacts for forensic
                debugging or purge them once resolved.
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
          key="celery-queue-depths-card"
          title="Queue Depths"
          helpTitle="Queue Depths"
          className="min-h-[200px]"
          isLoading={isLoading}
          isFetching={isStatusRefetching}
          loadingMessage="Inspecting Celery task broker..."
          error={error as AnalyticsCardError | null}
          helpContent={
            <div className="space-y-4 text-sm">
              <p>
                This table shows the current number of pending messages waiting to be
                processed in each Celery queue.
              </p>
              <ul className="list-disc pl-5 space-y-2">
                <li>
                  <strong>celery:</strong> The default fallback queue.
                </li>
                <li>
                  <strong>q.ingest:</strong> Primary ingest queue for discovering and
                  batch-processing log files.
                </li>
                <li>
                  <strong>q.control:</strong> Control-plane queue for commits, compaction,
                  and maintenance tasks.
                </li>
              </ul>
            </div>
          }
        >
          {queues.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No queues found or Celery mode is inactive.
            </p>
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
  )
}

export function QueueWorkersSection({
  isStandardMode,
  isLoading,
  isStatusRefetching,
  error,
  inprocessActiveRuns,
  workerCount,
  activeWorkers,
}: {
  isStandardMode: boolean
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  inprocessActiveRuns: InProcessActiveRun[]
  workerCount: number
  activeWorkers: [string, ActiveTask[]][]
}) {
  return (
    <div
      data-testid="admin-queue-workers-reservation"
      className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      {isStandardMode ? (
        <AnalyticsCard
          key="standard-active-jobs"
          title={`Active In-Process Jobs (${inprocessActiveRuns.length})`}
          helpTitle="Active In-Process Jobs"
          className="min-h-[200px]"
          isLoading={isLoading}
          isFetching={isStatusRefetching}
          loadingMessage="Inspecting active cron leases..."
          error={error as AnalyticsCardError | null}
          helpContent={
            <div className="space-y-4 text-sm">
              <p>
                Displays all background cron tasks currently holding an active execution
                lease in PostgreSQL (`job_runs`).
              </p>
              <p>
                In Standard deployment mode, background sync, commit, compaction, and
                maintenance tasks execute in-process via APScheduler rather than external
                Celery workers.
              </p>
            </div>
          }
        >
          {inprocessActiveRuns.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No background cron jobs are actively executing right now (scheduler is idle
              between ticks).
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
          key="celery-active-workers"
          title={`Active Workers (${workerCount})`}
          helpTitle="Active Workers"
          className="min-h-[200px]"
          isLoading={isLoading}
          isFetching={isStatusRefetching}
          loadingMessage="Inspecting Celery task broker..."
          error={error as AnalyticsCardError | null}
          helpContent={
            <div className="space-y-4 text-sm">
              <p>
                Displays all currently running Celery worker nodes and the tasks they are
                actively executing.
              </p>
              <p>
                If a worker is listed but has no active tasks, it is idle and waiting for
                messages from the queue.
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
  )
}

export function InProcessSchedulesSection({
  isLoading,
  isStatusRefetching,
  error,
  inprocessSchedules,
}: {
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  inprocessSchedules: InProcessScheduleEntry[]
}) {
  return (
    <div
      data-testid="admin-queue-schedules-reservation"
      className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      <AnalyticsCard
        title={`In-Process Schedules (${inprocessSchedules.length})`}
        helpTitle="In-Process Schedules"
        className="min-h-[200px]"
        isLoading={isLoading}
        isFetching={isStatusRefetching}
        loadingMessage="Loading scheduler telemetry..."
        error={error as AnalyticsCardError | null}
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              Displays all registered background schedules running in-process via
              APScheduler across both Standard and High-Scale deployment modes.
            </p>
            <p>
              These recurrent jobs handle log discovery, DuckLake commits, local
              compaction, rollup healing, cloud maintenance, and alert evaluation.
            </p>
          </div>
        }
      >
        {inprocessSchedules.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No in-process schedules registered.
          </p>
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
    </div>
  )
}
