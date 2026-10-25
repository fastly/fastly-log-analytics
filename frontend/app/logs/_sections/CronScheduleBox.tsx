'use client'

import React from 'react'
import { Loader2 } from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useDateFormat } from '@/hooks/useDateFormat'
import { LiveElapsed, NextRunCountdown, TimeAgo } from '@/components/TimeAgo'
import type { WithRequired, components } from '@/types/api'

export const CRON_EXPLANATIONS: Record<string, string> = {
  log_discovery: 'Lists recent raw/ minute prefixes in Fastly Object Storage to discover new files and enqueues one convert task per file — background Celery workers download, parse, and commit each file into the data lake.',
  rum_sync: 'Downloads RUM beacon logs from Fastly Object Storage, parses them, and ingests them into the local RUM beacon table. Cleans up old beacons according to retention policy.',
  rum_discovery: 'Discovers new RUM beacon logs in Fastly Object Storage and delegates ingestion across background Celery workers. Cleans up old beacons according to retention policy.',
  full_sync: 'Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync\'s 4h lookback window.',
  rum_commit: 'Aggregates RUM beacon buffer records and commits them as a snapshot to the local RUM beacons table for analytics.',
  gap_heal: 'Reconciles Fastly\'s authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (>=2 consecutive hourly buckets >=5% gap), triggers a full_sweep — throttled to one heal per 4h.',
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  commit: 'Finalizes ingested data: compacts small lake files, reports how many files the ingest workers landed since the last tick, and deletes raw .gz files whose rows are durably committed (when delete_after is on).',
  optimize: 'Compacts small Iceberg data files into larger ones (writes back to FOS — incurs 30-day-minimum cost on rewritten files).',
  local_compact: 'Merges small parquet files in the LOCAL CACHE every 2 min. Free vs FOS — speeds up dashboard scans without touching the cloud manifest.',
  expire_snapshots: 'Removes old snapshots and orphaned files to reclaim storage.',
  metadata_sync: 'Downloads the latest Iceberg metadata to sync with the remote data source.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
  metadata_cleanup: 'Daily 03:15 UTC. Trims usage_log + ingested_files + cron_runs in the per-service metadata.db per the retention policy (defaults 1d/1d/7d). VACUUMs the file only when something was actually deleted.',
  insights_prewarmer: 'Every 4 min. Pre-runs the default-selection insights query and warms the response cache so /insights returns warm (~80-130 ms) instead of cold-scanning the parquet (~3 s).',
  rollup_compact_daily: 'Daily 02:00 UTC. Consolidates closed-day per-hour rollup parquet into per-day files (local writes only).',
  rollup_hour_heal: 'Hourly at :05. Rebuilds rollup hour-bundles for closed hours the per-sync recompute missed.',
  partial_hour_merge: 'Every 30s. Incrementally merges newly-landed buffer/active-hour files into the partial-hour rollup, shrinking the live scan of the currently-open hour to just the tail since the last tick.',
  ledger_sweep: 'Every 15 min (Celery mode). Crash net for the ingest ledger: reclaims stale worker claims, re-dispatches stuck files, and diffs a 4h lookback LIST against the ledger.',
}

export const CRON_DISPLAY_NAMES: Record<string, string> = {
  log_discovery: 'Log Discovery',
  commit: 'Commit',
  full_sync: 'Full Discovery Sweep',
  gap_heal: 'Gap Heal',
  local_compact: 'Local Compact',
  optimize: 'Cloud Compact',
  expire_snapshots: 'Expire Snapshots',
  rollup_compact_daily: 'Rollup Compact',
  rollup_hour_heal: 'Rollup Heal',
  partial_hour_merge: 'Partial-Hour Merge',
  ledger_sweep: 'Ledger Sweep',
  insights_prewarmer: 'Insights Prewarmer',
  metadata_cleanup: 'Metadata Cleanup',
  ngwaf_sync: 'NGWAF Sync',
  metadata_sync: 'Metadata Sync',
  alerts: 'Alerts Evaluation',
  rum_sync: 'RUM Discovery',
  rum_discovery: 'RUM Discovery',
  rum_commit: 'Ingest RUM',
}

export const CRON_GROUPS = [
  {
    title: 'Ingestion Pipeline',
    tasks: ['log_discovery', 'commit', 'full_sync', 'gap_heal'],
  },
  {
    title: 'Storage Maintenance',
    tasks: ['local_compact', 'optimize', 'expire_snapshots'],
  },
  {
    title: 'Rollups & Caching',
    tasks: ['rollup_compact_daily', 'rollup_hour_heal', 'insights_prewarmer', 'partial_hour_merge'],
  },
  {
    title: 'Metadata & Enrichments',
    tasks: ['metadata_cleanup', 'ngwaf_sync', 'metadata_sync', 'alerts'],
  },
  {
    title: 'Real User Monitoring',
    tasks: ['rum_sync', 'rum_discovery', 'rum_commit'],
  }
]

type CronSchedule = Pick<
  WithRequired<components['schemas']['CronScheduleEntry'], 'task'>,
  'task' | 'disabled_reason' | 'last_run_time' | 'next_run_time'
>

type ActiveCronJob = {
  id: number | string
  started_at?: string | null
}

export function CronScheduleBox({
  schedule: _schedule,
  activeJob = null,
  onOpenConsole
}: {
  schedule: CronSchedule;
  activeJob?: ActiveCronJob | null;
  onOpenConsole?: (jobId: number | string) => void
}) {
  const { full, abbr } = useDateFormat()
  const schedule = _schedule

  if (schedule.disabled_reason === 'no_alerts_configured') {
    return (
      <div className="relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 bg-muted/20 border-muted min-w-[130px] flex-1">
        <div className="flex items-center gap-2 w-full">
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="text-[9px] font-bold uppercase text-muted-foreground tracking-wider shrink-0" />}>
                alerts
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[250px] text-xs">
                {CRON_EXPLANATIONS.alerts}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <div className="w-px h-4 bg-border shrink-0" />
          <span className="flex-1 min-w-0 truncate text-[9px] text-muted-foreground italic">
            No alerts configured.
          </span>
        </div>
      </div>
    )
  }

  const isRunning = !!activeJob
  const borderColor = isRunning ? 'border-blue-500/60 shadow-[0_0_8px_rgba(59,130,246,0.15)] bg-blue-500/5' : 'border-muted bg-muted/20'

  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 transition-all ${borderColor} min-w-[130px] flex-1`}>
      {isRunning && (
        <div className="absolute inset-0 rounded-md border border-blue-500/50 animate-pulse pointer-events-none" />
      )}
      <div className="flex items-center gap-2 w-full">
        <TooltipProvider delay={200}>
          <Tooltip>
            <TooltipTrigger render={
              <span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 ${isRunning ? 'text-blue-700 dark:text-blue-300' : 'text-muted-foreground'}`} />
            }>
              {CRON_DISPLAY_NAMES[schedule.task] || schedule.task}
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-[250px] text-xs">
              {CRON_EXPLANATIONS[schedule.task] || 'Background job.'}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
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
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate pr-2" />}>
                  Last: {schedule.last_run_time ? <TimeAgo timestamp={schedule.last_run_time} /> : 'Never'}
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate" />}>
                  Next: <NextRunCountdown when={schedule.next_run_time} />
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.next_run_time ? `${full(schedule.next_run_time)} ${abbr()}` : 'Disabled'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        )}
      </div>
    </div>
  )
}
