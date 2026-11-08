'use client'

import React from 'react'
import { Loader2 } from 'lucide-react'
import { useDateFormat } from '@/hooks/useSSE'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { LiveElapsed, NextRunCountdown, TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import type { WithRequired } from '@/types/api'
import type { components } from '@/types/api.generated'
import type { CronRun } from './useLogsPageState'

export const CRON_EXPLANATIONS: Record<string, string> = {
  cloud_ingest_clock: 'Cloud tick coordinator. Dispatches due jobs; its outcome is not evidence that every job ran.',
  request_ingest: 'Ingests available request log objects on the configured log period, with backlog catch-up and retry backoff.',
  rum_ingest: 'Ingests RUM logs on their own configured log period. Disabled when RUM is off.',
  raw_delete: 'Drains queued raw-object deletions after ingestion. Runs only while work is pending.',
  log_flush: 'Flushes operational telemetry in a separate execution. Wakes periodically or sooner while work remains.',
  maintenance: 'Dispatches due maintenance work. Individual stages rotate when eligible; they do not have independent fixed timers.',
  retention_sweep: 'Resumes or starts a due operational-log retention sweep on a maintenance turn.',
  log_compact: 'Compacts operational-log files when enabled and eligible on a maintenance turn.',
  delete_history: 'Processes a manually queued operational-log history deletion on a maintenance turn.',
  storage_reconcile: 'Reconciles stored object metadata in bounded maintenance work.',
  summary_bundle: 'Bundles eligible request summaries on a maintenance turn.',
  request_compact: 'Merges small request Parquet files into larger compacts on its own schedule: every 30 s once caught up, every 10 s while a backlog remains. Disabled when request compaction is off.',
  delete_data: 'Resumes an explicitly queued manual data reset. This page never starts a reset.',
  log_discovery:
    'Lists recent raw/ minute prefixes in Fastly Object Storage to discover new files and registers them in the ingest ledger so stateless workers download, archive to Parquet, and publish batches into ClickHouse.',
  rum_sync:
    'Downloads RUM beacon logs from Fastly Object Storage, parses them, and ingests them into ClickHouse. Cleans up old beacons according to retention policy.',
  rum_discovery:
    'Discovers new RUM beacon logs in Fastly Object Storage and delegates ingestion across stateless workers into ClickHouse.',
  full_sync:
    "Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync's 4h lookback window.",
  gap_heal:
    "Reconciles Fastly's authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (>=2 consecutive hourly buckets >=5% gap), triggers a full_sweep — throttled to one heal per 4h.",
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
  metadata_cleanup:
    'Daily 03:15 UTC. Trims usage_log + ingested_files + cron_runs in PostgreSQL per the retention policy (defaults 1d/1d/7d).',
  insights_prewarmer:
    'Every 4 min. Pre-runs the default-selection insights query against ClickHouse and warms the response cache.',
  ledger_sweep:
    'Every 15 min. Crash net for the ingest ledger: reclaims stale worker claims, re-dispatches stuck files, and diffs a 4h lookback LIST against the ledger.',
}

export const CRON_DISPLAY_NAMES: Record<string, string> = {
  cloud_ingest_clock: 'Cloud Coordinator',
  request_ingest: 'Request Ingest',
  rum_ingest: 'RUM Ingest',
  raw_delete: 'Raw Object Cleanup',
  log_flush: 'Telemetry Flush',
  maintenance: 'Maintenance Dispatcher',
  retention_sweep: 'Retention Sweep',
  log_compact: 'Telemetry Compaction',
  delete_history: 'History Cleanup',
  storage_reconcile: 'Storage Reconciliation',
  summary_bundle: 'Summary Bundles',
  request_compact: 'Request Compaction',
  delete_data: 'Queued Data Reset',
  log_discovery: 'Log Discovery',
  full_sync: 'Full Discovery Sweep',
  gap_heal: 'Gap Heal',
  ledger_sweep: 'Ledger Sweep',
  insights_prewarmer: 'Insights Prewarmer',
  metadata_cleanup: 'Metadata Cleanup',
  ngwaf_sync: 'NGWAF Sync',
  alerts: 'Alerts Evaluation',
  rum_sync: 'RUM Discovery',
  rum_discovery: 'RUM Discovery',
}

export const CRON_GROUPS = [
  { title: 'Ingestion Pipeline', tasks: ['log_discovery', 'full_sync', 'gap_heal', 'ledger_sweep'] },
  { title: 'Metadata & Enrichments', tasks: ['metadata_cleanup', 'ngwaf_sync', 'alerts', 'insights_prewarmer'] },
  { title: 'Real User Monitoring', tasks: ['rum_sync', 'rum_discovery'] },
  { title: 'Cloud Ingestion & Telemetry', tasks: ['request_ingest', 'rum_ingest', 'raw_delete', 'request_compact', 'log_flush'] },
  { title: 'Cloud Maintenance', tasks: ['retention_sweep', 'log_compact', 'delete_history', 'storage_reconcile', 'summary_bundle', 'delete_data'] },
  { title: 'Cloud Coordination', tasks: ['cloud_ingest_clock', 'maintenance'] },
]

export type CronScheduleEntry = components['schemas']['CronScheduleEntry']
export type CronSchedule = Pick<WithRequired<CronScheduleEntry, 'task'>, 'task' | 'disabled_reason' | 'last_run_time' | 'next_run_time'> & {
  schedule_basis?: string
}
export type ActiveCronJob = { id: number | string; started_at?: string | null }

export function CronTip({
  className,
  side,
  contentClass = 'text-xs',
  tip,
  children,
}: {
  className?: string
  side?: 'top' | 'right'
  contentClass?: string
  tip: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <TooltipProvider delay={200}>
      <Tooltip>
        <TooltipTrigger render={<span className={className} />}>{children}</TooltipTrigger>
        <TooltipContent side={side} className={contentClass}>
          {tip}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function CronScheduleBox({
  schedule,
  activeJob = null,
  onOpenConsole,
}: {
  schedule: CronSchedule
  activeJob?: ActiveCronJob | null
  onOpenConsole?: (jobId: number | string) => void
}) {
  const { full, abbr } = useDateFormat()
  if (schedule.disabled_reason) {
    return (
      <div className="relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 bg-muted/20 border-muted min-w-[130px] flex-1">
        <div className="flex items-center gap-2 w-full">
          <CronTip
            className="text-[9px] font-bold uppercase text-muted-foreground tracking-wider shrink-0"
            side="top"
            contentClass="max-w-[250px] text-xs"
            tip={CRON_EXPLANATIONS[schedule.task]}
          >
            {CRON_DISPLAY_NAMES[schedule.task] || schedule.task}
          </CronTip>
          <div className="w-px h-4 bg-border shrink-0" />
          <span className="flex-1 min-w-0 truncate text-[9px] text-muted-foreground italic">
            {schedule.disabled_reason === 'no_alerts_configured' ? 'No alerts configured.' : schedule.disabled_reason}
          </span>
        </div>
      </div>
    )
  }

  const isRunning = !!activeJob
  const borderColor = isRunning
    ? 'border-blue-500/60 shadow-[0_0_8px_rgba(59,130,246,0.15)] bg-blue-500/5'
    : 'border-muted bg-muted/20'

  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 transition-all ${borderColor} min-w-[130px] flex-1`}>
      {isRunning && <div className="absolute inset-0 rounded-md border border-blue-500/50 animate-pulse pointer-events-none" />}
      <div className="flex items-center gap-2 w-full">
        <CronTip
          className={`text-[9px] font-bold uppercase tracking-wider shrink-0 ${isRunning ? 'text-blue-700 dark:text-blue-300' : 'text-muted-foreground'}`}
          side="top"
          contentClass="max-w-[250px] text-xs"
          tip={CRON_EXPLANATIONS[schedule.task] || 'Background job.'}
        >
          {CRON_DISPLAY_NAMES[schedule.task] || schedule.task}
        </CronTip>
        <div className="w-px h-4 bg-border shrink-0" />
        {isRunning ? (
          <button
            onClick={() => onOpenConsole?.(activeJob.id)}
            className="flex-1 min-w-0 text-left text-[9px] text-blue-700 dark:text-blue-300 hover:text-blue-800 dark:hover:text-blue-200 hover:underline font-medium flex items-center justify-between cursor-pointer"
            aria-label={`Running for ${schedule.task} — click for logs`}
          >
            <span className="flex items-center gap-1 min-w-0">
              <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-700 dark:text-blue-300" aria-hidden="true" />
              {activeJob.started_at && (
                <LiveElapsed
                  startedAt={activeJob.started_at}
                  className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-[9px] font-medium"
                />
              )}
            </span>
            <span className="text-[8px] bg-blue-500/20 px-1 py-0.2 rounded border border-blue-500/20 shrink-0 ml-1">LOGS</span>
          </button>
        ) : (
          <div className="flex-1 min-w-0 flex items-center justify-between text-[9px] text-muted-foreground whitespace-nowrap overflow-hidden">
            <CronTip className="truncate pr-1 flex-1 min-w-0 tabular-nums" tip={schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : schedule.schedule_basis ? 'No execution recorded in available history' : 'Never'}>
              Last: {schedule.last_run_time ? <TimeAgo timestamp={schedule.last_run_time} /> : schedule.schedule_basis ? 'No record' : 'Never'}
            </CronTip>
            <CronTip className="truncate w-[62px] text-right shrink-0 tabular-nums" tip={schedule.next_run_time ? `${full(schedule.next_run_time)} ${abbr()}` : schedule.schedule_basis ? 'Runs when eligible work is available; no fixed next execution time' : 'Disabled'}>
              Next: {!schedule.next_run_time && schedule.schedule_basis ? 'Waiting' : <NextRunCountdown when={schedule.next_run_time} />}
            </CronTip>
          </div>
        )}
      </div>
    </div>
  )
}

export function CronScheduleGroupSection({
  title,
  items,
  onOpenConsole,
}: {
  title: string
  items: Array<{ task: string; activeJob: CronRun | undefined; schedule: CronScheduleEntry }>
  onOpenConsole: (jobId: number | string) => void
}) {
  if (items.length === 0) return null
  return (
    <div className="space-y-3">
      <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">{title}</div>
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
        {items.map((item) => (
          <CronScheduleBox
            key={item.task}
            schedule={item.schedule || { task: item.task }}
            activeJob={item.activeJob}
            onOpenConsole={onOpenConsole}
          />
        ))}
      </div>
    </div>
  )
}
