'use client'

import React from 'react'
import { formatCount } from '@/lib/utils'
import { DataTableReadonly } from '@/components/DataTable'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { QuarantineSection } from '@/app/control-room/_sections/ControlRoomClient'
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
      className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[204px] min-w-0 overflow-hidden [contain-intrinsic-size:auto_204px] [content-visibility:visible]"
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
      className="min-h-[248px] min-w-0 overflow-hidden [contain-intrinsic-size:auto_248px] [content-visibility:visible]"
    >
      {isStandardMode ? (
        <AnalyticsCard
          key="standard-quarantined-files"
          title={`Quarantined Files & Corrupt Lines (${standardQuarantinedItems})`}
          helpTitle="Quarantined Files & Corrupt Lines"
          className="min-h-[248px]"
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
