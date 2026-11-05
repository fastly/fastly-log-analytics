'use client'

import React from 'react'
import { DataTableReadonly } from '@/components/DataTable'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import type { components } from '@/types/api.generated'
import {
  QUEUE_COLUMNS,
  TASK_COLUMNS,
  INPROCESS_SCHEDULE_COLUMNS,
  INPROCESS_ACTIVE_COLUMNS,
  QueueWorkersSection,
  InProcessSchedulesSection,
  type ActiveTask,
  type InProcessScheduleEntry,
  type InProcessActiveRun,
  type QueueDepth,
} from './QueueWorkerAndSchedulePanels'

export type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
export type LedgerEntry = components['schemas']['IngestLedgerEntry']
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

export {
  QUEUE_COLUMNS,
  TASK_COLUMNS,
  INPROCESS_SCHEDULE_COLUMNS,
  INPROCESS_ACTIVE_COLUMNS,
  QueueWorkersSection,
  InProcessSchedulesSection,
  type ActiveTask,
  type InProcessScheduleEntry,
  type InProcessActiveRun,
  type QueueDepth,
}

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
        <div className="text-xs text-muted-foreground min-w-[92px] text-right tabular-nums">
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
  isLoading,
  latestQueue,
  queuePoints,
  latestWorker,
  workerPoints,
  trendsLoading,
  isTrendsRefetching,
  trendsError,
}: {
  isLoading: boolean
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
      className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[204px] min-w-0 overflow-hidden [contain-intrinsic-size:auto_204px] [content-visibility:visible]"
    >
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
    </div>
  )
}

export function QueueDepthsAndQuarantineSection({
  isLoading,
  isStatusRefetching,
  error,
  queues,
}: {
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  queues: QueueDepth[]
}) {
  return (
    <div
      data-testid="admin-queue-depths-reservation"
      className="min-h-[248px] min-w-0 overflow-hidden [contain-intrinsic-size:auto_248px] [content-visibility:visible]"
    >
      <AnalyticsCard
        key="celery-queue-depths-card"
        title="Queue Depths"
        helpTitle="Queue Depths"
        className="min-h-[248px]"
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
                <strong>q.control:</strong> Control-plane queue for maintenance and
                administrative tasks.
              </li>
            </ul>
          </div>
        }
      >
        {queues.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No queues found or Celery broker is inactive.
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
    </div>
  )
}
