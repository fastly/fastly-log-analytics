'use client'

import React from 'react'
import { formatLocalDateTime } from '@/lib/utils'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import type { components } from '@/types/api.generated'

export type InProcessScheduleEntry = components['schemas']['InProcessScheduleEntry']
export type InProcessActiveRun = components['schemas']['InProcessActiveRun']

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
      className="min-h-[200px] min-w-0 overflow-hidden [contain-intrinsic-size:auto_200px] [content-visibility:visible]"
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
              APScheduler.
            </p>
            <p>
              These recurrent jobs handle log discovery, RUM sync, cloud maintenance,
              insights prewarming, and alert evaluation.
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
