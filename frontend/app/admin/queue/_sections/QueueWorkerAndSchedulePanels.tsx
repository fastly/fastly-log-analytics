'use client'

import React from 'react'
import { formatLocalDateTime } from '@/lib/utils'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import type { components } from '@/types/api.generated'

export type ActiveTask = components['schemas']['CeleryActiveTask']
export type InProcessScheduleEntry = components['schemas']['InProcessScheduleEntry']
export type InProcessActiveRun = components['schemas']['InProcessActiveRun']
export type QueueDepth = [name: string, count: number]

export function makeCol<T>(
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
