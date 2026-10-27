'use client'

import React, { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Database, RefreshCw, Loader2, ChevronDown, X, HelpCircle, Trash2 } from 'lucide-react'
import { queryKeys } from '@/lib/api'
import { cn, formatCount } from '@/lib/utils'
import { useSSE, useDateFormat, type SyncStatus } from '@/hooks/useSSE'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { ConfirmDialog, CopyButton, HelpDialog } from '@/components/ui/confirm-dialog'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { LiveElapsed, NextRunCountdown, TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DataTable, ColumnVisibilityDropdown, DateTimeCell } from '@/components/DataTable'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { ColumnDef } from '@tanstack/react-table'
import type { WithRequired } from '@/types/api'
import type { components } from '@/types/api.generated'
import type { CronRun } from './useLogsPageState'

export const CRON_EXPLANATIONS: Record<string, string> = {
  log_discovery:
    'Lists recent raw/ minute prefixes in Fastly Object Storage to discover new files and enqueues one convert task per file — background Celery workers download, parse, and commit each file into the data lake.',
  rum_sync:
    'Downloads RUM beacon logs from Fastly Object Storage, parses them, and ingests them into the local RUM beacon table. Cleans up old beacons according to retention policy.',
  rum_discovery:
    'Discovers new RUM beacon logs in Fastly Object Storage and delegates ingestion across background Celery workers. Cleans up old beacons according to retention policy.',
  full_sync:
    "Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync's 4h lookback window.",
  rum_commit:
    'Aggregates RUM beacon buffer records and commits them as a snapshot to the local RUM beacons table for analytics.',
  gap_heal:
    "Reconciles Fastly's authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (>=2 consecutive hourly buckets >=5% gap), triggers a full_sweep — throttled to one heal per 4h.",
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  commit:
    'Finalizes ingested data: compacts small lake files, reports how many files the ingest workers landed since the last tick, and deletes raw .gz files whose rows are durably committed (when delete_after is on).',
  optimize:
    'Compacts small DuckLake data files into larger ones (writes back to FOS — incurs 30-day-minimum cost on rewritten files).',
  local_compact:
    'Merges small parquet files in the LOCAL CACHE every 2 min. Free vs FOS — speeds up dashboard scans without touching the cloud manifest.',
  expire_snapshots: 'Removes old snapshots and orphaned files to reclaim storage.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
  metadata_cleanup:
    'Daily 03:15 UTC. Trims usage_log + ingested_files + cron_runs in the per-service metadata.db per the retention policy (defaults 1d/1d/7d). VACUUMs the file only when something was actually deleted.',
  insights_prewarmer:
    'Every 4 min. Pre-runs the default-selection insights query and warms the response cache so /insights returns warm (~80-130 ms) instead of cold-scanning the parquet (~3 s).',
  rollup_compact_daily:
    'Daily 02:00 UTC. Consolidates closed-day per-hour rollup parquet into per-day files (local writes only).',
  rollup_hour_heal:
    'Hourly at :05. Rebuilds rollup hour-bundles for closed hours the per-sync recompute missed.',
  partial_hour_merge:
    'Every 30s. Incrementally merges newly-landed buffer/active-hour files into the partial-hour rollup, shrinking the live scan of the currently-open hour to just the tail since the last tick.',
  ledger_sweep:
    'Every 15 min (Celery mode). Crash net for the ingest ledger: reclaims stale worker claims, re-dispatches stuck files, and diffs a 4h lookback LIST against the ledger.',
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
  alerts: 'Alerts Evaluation',
  rum_sync: 'RUM Discovery',
  rum_discovery: 'RUM Discovery',
  rum_commit: 'Ingest RUM',
}

export const CRON_GROUPS = [
  { title: 'Ingestion Pipeline', tasks: ['log_discovery', 'commit', 'full_sync', 'gap_heal'] },
  { title: 'Storage Maintenance', tasks: ['local_compact', 'optimize', 'expire_snapshots'] },
  { title: 'Rollups & Caching', tasks: ['rollup_compact_daily', 'rollup_hour_heal', 'insights_prewarmer', 'partial_hour_merge'] },
  { title: 'Metadata & Enrichments', tasks: ['metadata_cleanup', 'ngwaf_sync', 'alerts'] },
  { title: 'Real User Monitoring', tasks: ['rum_sync', 'rum_discovery', 'rum_commit'] },
]

type CronScheduleEntry = components['schemas']['CronScheduleEntry']
type CronSchedule = Pick<WithRequired<CronScheduleEntry, 'task'>, 'task' | 'disabled_reason' | 'last_run_time' | 'next_run_time'>
type ActiveCronJob = { id: number | string; started_at?: string | null }

function CronTip({
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
  if (schedule.disabled_reason === 'no_alerts_configured') {
    return (
      <div className="relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 bg-muted/20 border-muted min-w-[130px] flex-1">
        <div className="flex items-center gap-2 w-full">
          <CronTip
            className="text-[9px] font-bold uppercase text-muted-foreground tracking-wider shrink-0"
            side="top"
            contentClass="max-w-[250px] text-xs"
            tip={CRON_EXPLANATIONS.alerts}
          >
            alerts
          </CronTip>
          <div className="w-px h-4 bg-border shrink-0" />
          <span className="flex-1 min-w-0 truncate text-[9px] text-muted-foreground italic">No alerts configured.</span>
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
            <CronTip className="truncate pr-2" tip={schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}>
              Last: {schedule.last_run_time ? <TimeAgo timestamp={schedule.last_run_time} /> : 'Never'}
            </CronTip>
            <CronTip className="truncate" tip={schedule.next_run_time ? `${full(schedule.next_run_time)} ${abbr()}` : 'Disabled'}>
              Next: <NextRunCountdown when={schedule.next_run_time} />
            </CronTip>
          </div>
        )}
      </div>
    </div>
  )
}

function CronScheduleGroupSection({
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

export function CronTab({
  cronColumns,
  cronLogs,
  isLoadingCron,
  isFetchingCron,
  orderedSchedules,
  isLoadingSchedule = false,
  taskFilter,
  setTaskFilter,
  statusFilter,
  setStatusFilter,
  status,
  activeServiceId,
  setDisplayedJobs,
  setSelectedConsoleJobId,
  setConsoleOpen,
  isPurgeOpen,
  setIsPurgeOpen,
  purgeMutation,
}: {
  cronColumns: ColumnDef<CronRun>[]
  cronLogs: components['schemas']['CronRunsResponse'] | undefined
  isLoadingCron: boolean
  isFetchingCron: boolean
  orderedSchedules: Array<{ task: string; activeJob: CronRun | undefined; schedule: CronScheduleEntry }>
  isLoadingSchedule?: boolean
  taskFilter: string
  setTaskFilter: (v: string) => void
  statusFilter: string
  setStatusFilter: (v: string) => void
  status: SyncStatus | undefined
  activeServiceId: string | null | undefined
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
  isPurgeOpen: boolean
  setIsPurgeOpen: (open: boolean) => void
  purgeMutation: { isPending: boolean; mutate: () => void }
}) {
  const queryClient = useQueryClient()
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)
  const tableData = React.useMemo(() => (cronLogs?.entries || []).filter((e) => e.status !== 'running'), [cronLogs?.entries])
  const handleOpenConsole = React.useCallback(
    (jobId: number | string) => {
      setConsoleOpen(true)
      setSelectedConsoleJobId(jobId)
    },
    [setConsoleOpen, setSelectedConsoleJobId],
  )
  const groupedTaskNames = React.useMemo(() => CRON_GROUPS.flatMap((g) => g.tasks), [])
  const otherTasks = React.useMemo(
    () => orderedSchedules.filter((s) => !groupedTaskNames.includes(s.task)),
    [orderedSchedules, groupedTaskNames],
  )
  const taskFilterOptions: Array<[string, string]> = [
    ['rum_sync', 'RUM Sync'],
    ['full_sync', 'Full Sync'],
    ['gap_heal', 'Gap Heal'],
    ['alerts', 'Alerts'],
    ['commit', 'Commit'],
    ['rum_commit', 'RUM Commit'],
    ['optimize', 'Optimize'],
    ['local_compact', 'Local Compact'],
    ['rollup_compact_daily', 'Rollup Compact'],
    ['rollup_hour_heal', 'Rollup Heal'],
    ['expire_snapshots', 'Expire'],
    ...(status?.ngwaf_workspace_id ? [['ngwaf_sync', 'NGWAF Sync'] as [string, string]] : []),
    ['metadata_cleanup', 'Metadata Cleanup'],
  ]

  return (
    <div className="p-0">
      <DataTable
        columns={cronColumns}
        data={tableData}
        isLoading={isLoadingCron}
        initialSorting={[{ id: 'started_at', desc: true }]}
        onRowClick={(row: CronRun) => {
          setDisplayedJobs((prev) => (prev.some((j) => j.id === row.id) ? prev : [...prev, { ...row, status: row.status }]))
          setSelectedConsoleJobId(row.id)
          setConsoleOpen(true)
        }}
        renderToolbar={(table) => (
          <>
            {(orderedSchedules.length > 0 || isLoadingSchedule) && (
              <div className="p-4 border-b bg-muted/10 space-y-5">
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-medium">Cron Jobs</h3>
                  <button
                    onClick={() => setIsHelpOpen(true)}
                    aria-label="About background cron jobs"
                    className="text-muted-foreground hover:text-foreground"
                  >
                    <HelpCircle className="h-4 w-4" />
                  </button>
                </div>
                <HelpDialog
                  open={isHelpOpen}
                  onOpenChange={setIsHelpOpen}
                  title="Background Cron Jobs"
                  icon={<HelpCircle className="h-5 w-5" />}
                  size="xl"
                >
                  <div className="space-y-6">
                    <p>
                      Fastly Log Analytics uses a distributed architecture of background jobs to ingest logs, perform
                      maintenance, and pre-calculate analytics.
                    </p>
                    {CRON_GROUPS.map((group) => (
                      <div key={group.title} className="space-y-2">
                        <h4 className="font-semibold text-foreground">{group.title}</h4>
                        <ul className="space-y-3">
                          {group.tasks.map((task) => (
                            <li key={task} className="pl-4 border-l-2 border-muted">
                              <div className="flex items-center gap-2 mb-1">
                                <span className="font-medium text-foreground">{CRON_DISPLAY_NAMES[task] || task}</span>
                                <span className="text-[10px] font-mono text-muted-foreground px-1.5 py-0.5 bg-muted rounded border">
                                  {task}
                                </span>
                              </div>
                              <p className="text-xs text-muted-foreground/80 leading-relaxed">{CRON_EXPLANATIONS[task]}</p>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </HelpDialog>
                <div className="space-y-5">
                  {orderedSchedules.length === 0 ? (
                    CRON_GROUPS.map((group) => (
                      <div key={group.title} className="space-y-3">
                        <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">
                          {group.title}
                        </div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                          {group.tasks.slice(0, 3).map((task) => (
                            <div
                              key={task}
                              className="h-8 rounded-md border border-muted bg-muted/20 animate-pulse min-w-[130px] flex-1"
                            />
                          ))}
                        </div>
                      </div>
                    ))
                  ) : (
                    <>
                      {CRON_GROUPS.map((group) => (
                        <CronScheduleGroupSection
                          key={group.title}
                          title={group.title}
                          items={orderedSchedules.filter((s) => group.tasks.includes(s.task))}
                          onOpenConsole={handleOpenConsole}
                        />
                      ))}
                      <CronScheduleGroupSection title="Other Tasks" items={otherTasks} onOpenConsole={handleOpenConsole} />
                    </>
                  )}
                </div>
              </div>
            )}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4 bg-card">
              <div className="flex flex-wrap items-center gap-4">
                <h3 className="text-sm font-medium whitespace-nowrap">Recent Cron Activity</h3>
                <div className="flex items-center gap-2">
                  <Select value={taskFilter} onValueChange={(v) => setTaskFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs">
                      <SelectValue placeholder="All tasks" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All tasks</SelectItem>
                      <SelectItem value="log_discovery">Log Discovery</SelectItem>
                      {taskFilterOptions.map(([val, label]) => (
                        <SelectItem key={val} value={val}>
                          {label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs">
                      <SelectValue placeholder="All statuses" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All statuses</SelectItem>
                      <SelectItem value="success">Success</SelectItem>
                      <SelectItem value="partial_success">Partial Success</SelectItem>
                      <SelectItem value="error">Error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <ColumnVisibilityDropdown table={table} />
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 text-xs"
                  disabled={isFetchingCron}
                  onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })}
                >
                  <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingCron ? 'animate-spin' : ''}`} />
                  Refresh
                </Button>
                <button
                  className={cn(
                    buttonVariants({ variant: 'outline', size: 'sm' }),
                    'h-8 text-xs border-destructive/50 text-destructive hover:bg-destructive hover:text-white cursor-pointer',
                  )}
                  onClick={() => setIsPurgeOpen(true)}
                >
                  <Trash2 className="h-3 w-3 mr-1.5" /> Purge Logs
                </button>
              </div>
              <ConfirmDialog
                open={isPurgeOpen}
                onOpenChange={setIsPurgeOpen}
                title="Purge all cron logs?"
                description="This will permanently delete the entire history of automated background job records for this service."
                confirmLabel="Purge"
                isDangerous
                isPending={purgeMutation.isPending}
                onConfirm={() => purgeMutation.mutate()}
              />
            </div>
          </>
        )}
      />
    </div>
  )
}

const DASH_CELL = <span className="text-muted-foreground">—</span>
const PROCESSING_CELL = <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
const NO_FILES_TASKS = new Set([
  'expire_snapshots',
  'rollup_compact_daily',
  'rollup_hour_heal',
  'metadata_cleanup',
  'insights_prewarmer',
  'partial_hour_merge',
])
const NO_ROWS_TASKS = new Set([...NO_FILES_TASKS, 'optimize', 'commit', 'rum_commit', 'local_compact'])

const COMMIT_CRON_COLUMNS: ColumnDef<CronRun>[] = [
  {
    accessorKey: 'files_deleted_fos',
    header: 'Log Files Deleted',
    cell: ({ row }) =>
      row.original.status === 'running' || row.original.task !== 'log_discovery' ? (
        DASH_CELL
      ) : (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {formatCount(row.original.files_deleted_fos || 0)}
        </span>
      ),
  },
  {
    id: 'rows_committed',
    header: 'Rows Committed',
    cell: ({ row }) => {
      if (row.original.status === 'running') return PROCESSING_CELL
      const val = row.original.task === 'commit' ? row.original.rows_ingested : null
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {val != null ? formatCount(val) : DASH_CELL}
        </span>
      )
    },
  },
]

export function useCronColumns(): ColumnDef<CronRun>[] {
  return React.useMemo(
    () => [
      {
        accessorKey: 'started_at',
        id: 'started_at',
        meta: { label: 'Started At' },
        header: 'Started At',
        cell: ({ row }) => <DateTimeCell iso={row.original.started_at} />,
      },
      {
        id: 'finished_at',
        meta: { label: 'Finished At' },
        enableSorting: false,
        accessorFn: (row) =>
          row.started_at && row.duration_s != null
            ? new Date(new Date(row.started_at).getTime() + row.duration_s * 1000).toISOString()
            : null,
        header: () => <span className="text-xs font-medium px-2.5">Finished At</span>,
        cell: ({ row }) => {
          const { status, started_at: startIso, duration_s: dur } = row.original
          return status === 'running' || !startIso || dur == null ? (
            DASH_CELL
          ) : (
            <DateTimeCell iso={new Date(new Date(startIso).getTime() + dur * 1000).toISOString()} />
          )
        },
      },
      {
        accessorKey: 'task',
        id: 'task',
        meta: { label: 'Task' },
        header: 'Task',
        cell: ({ row }) => {
          const { task, summary } = row.original
          const isSync = task === 'log_discovery'
          return (
            <div className="flex flex-col gap-1 py-1">
              <TooltipProvider delay={200}>
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <Badge
                        className={cn(
                          'w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold',
                          isSync
                            ? 'bg-blue-500/10 text-blue-600 hover:bg-blue-500/20'
                            : 'bg-purple-500/10 text-purple-600 hover:bg-purple-500/20',
                        )}
                      >
                        {CRON_DISPLAY_NAMES[task] || task}
                      </Badge>
                    }
                  />
                  <TooltipContent side="right" className="max-w-[250px] text-xs">
                    <p>{CRON_EXPLANATIONS[task] || 'Background job.'}</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              {summary && (
                <span className="text-[11px] text-muted-foreground whitespace-normal break-words leading-tight">
                  {summary}
                </span>
              )}
            </div>
          )
        },
      },
      {
        accessorKey: 'status',
        id: 'status',
        meta: { label: 'Status' },
        header: 'Status',
        cell: ({ row }) => {
          const { status: val, error_message: err, corrupt_rows } = row.original
          if (val === 'running') {
            return (
              <Badge
                variant="outline"
                className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold border-blue-500/30 text-blue-700 dark:text-blue-400 bg-blue-500/10 flex items-center gap-1 w-fit"
              >
                <Loader2 className="w-3 h-3 animate-spin" /> Running
              </Badge>
            )
          }
          if (val === 'skipped') {
            return (
              <Badge variant="secondary" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                No Alerts
              </Badge>
            )
          }
          if (val === 'success' && (!corrupt_rows || corrupt_rows === 0)) {
            return (
              <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                Success
              </Badge>
            )
          }
          if (val === 'partial_success' || (val === 'success' && (corrupt_rows ?? 0) > 0)) {
            return (
              <Badge variant="warning" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                Partial Success
              </Badge>
            )
          }
          return (
            <div className="flex items-center gap-1.5">
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Badge variant="destructive" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold" />
                  }
                >
                  Error
                </TooltipTrigger>
                <TooltipContent className="max-w-[400px] break-words bg-destructive text-white dark:text-white">
                  <p className="text-xs font-mono">{err || 'Unknown error'}</p>
                </TooltipContent>
              </Tooltip>
              {err && (
                <CopyButton
                  text={err}
                  ariaLabel="Copy full error message"
                  title="Copy full error message"
                  stopPropagation
                  className="h-6 w-6 text-muted-foreground hover:text-foreground"
                />
              )}
            </div>
          )
        },
      },
      {
        accessorKey: 'duration_s',
        id: 'duration_s',
        meta: { label: 'Duration' },
        header: 'Duration',
        cell: ({ row }) => {
          const s = row.original.duration_s
          if (s == null) return <span className="font-mono text-muted-foreground tabular-nums text-xs">—</span>
          if (row.original.status === 'running') {
            return (
              <LiveElapsed
                startedAt={row.original.started_at}
                className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-xs font-medium"
              />
            )
          }
          const fmt =
            s < 1 ? `${Math.round(s * 1000)}ms` : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
          return <span className="font-mono text-muted-foreground tabular-nums text-xs">{fmt}</span>
        },
      },
      {
        accessorKey: 'files_downloaded',
        id: 'files_downloaded',
        meta: { label: 'Files Downloaded' },
        header: 'Files Processed',
        cell: ({ row }) => {
          if (row.original.status === 'running') return PROCESSING_CELL
          const { task, files_downloaded, rows_ingested, parquet_files_optimized } = row.original
          if (NO_FILES_TASKS.has(task) || ((task === 'commit' || task === 'rum_commit') && !rows_ingested)) {
            return DASH_CELL
          }
          const count =
            task === 'commit' || task === 'rum_commit'
              ? 1
              : task === 'optimize'
                ? parquet_files_optimized || 0
                : files_downloaded || 0
          const label =
            task === 'alerts'
              ? count === 1
                ? 'alert evaluated'
                : 'alerts evaluated'
              : task === 'commit' || task === 'rum_commit'
                ? 'cloud file cached'
                : task === 'optimize' || task === 'local_compact'
                  ? 'files merged'
                  : task === 'rum_sync' || task === 'rum_discovery'
                    ? 'raw RUM logs'
                    : task === 'ngwaf_sync'
                      ? count === 1
                        ? 'bot record'
                        : 'bot records'
                      : 'raw logs'
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs" title={label}>
              {formatCount(count)} <span className="text-[10px] text-muted-foreground">{label}</span>
            </span>
          )
        },
      },
      {
        accessorKey: 'rows_ingested',
        header: 'Log Entries Processed',
        cell: ({ row }) => {
          if (row.original.status === 'running') return PROCESSING_CELL
          const { task, rows_ingested, corrupt_rows, error_message } = row.original
          if (NO_ROWS_TASKS.has(task)) return DASH_CELL
          if (task === 'alerts') {
            const triggered = rows_ingested || 0
            return triggered === 0 ? (
              DASH_CELL
            ) : (
              <span className="font-mono tabular-nums text-xs text-amber-500 font-medium">
                {triggered} {triggered === 1 ? 'alert triggered' : 'alerts triggered'}
              </span>
            )
          }
          const rows = rows_ingested || 0
          const corrupt = corrupt_rows || 0
          return (
            <div className="flex items-center gap-2">
              <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(rows)}</span>
              {corrupt > 0 && (
                <div className="flex items-center gap-1 group/corrupt">
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <Badge variant="destructive" className="px-1.5 py-0 shadow-none text-[10px] font-bold" />
                      }
                    >
                      {formatCount(corrupt)} Skipped
                    </TooltipTrigger>
                    <TooltipContent>
                      These lines were skipped due to missing timestamps or invalid JSON structure.
                    </TooltipContent>
                  </Tooltip>
                  {error_message && (
                    <CopyButton
                      text={error_message}
                      ariaLabel="Copy corrupt-line details"
                      title="Copy corrupt lines"
                      stopPropagation
                      className="h-5 w-5 text-muted-foreground hover:text-foreground opacity-0 group-hover/corrupt:opacity-100 transition-opacity"
                    />
                  )}
                </div>
              )}
            </div>
          )
        },
      },
      ...COMMIT_CRON_COLUMNS,
    ],
    [],
  )
}

export function CronLiveLog({
  runId,
  singleLine = false,
  terminalMode = !singleLine,
  startedAt,
  onDone,
}: {
  runId: number | string | undefined
  singleLine?: boolean
  terminalMode?: boolean
  startedAt?: string
  onDone?: () => void
}) {
  const { lines, status, start, stop } = useSSE()
  const startedForRunId = useRef<number | string | undefined>(undefined)
  const doneFiredForRunId = useRef<number | string | undefined>(undefined)
  const { full, abbr } = useDateFormat()

  useEffect(() => {
    if (runId && startedForRunId.current !== runId) {
      startedForRunId.current = runId
      doneFiredForRunId.current = undefined
      start(`/api/cron-runs/${runId}/stream`)
    }
    return () => {
      if (startedForRunId.current !== undefined) {
        stop()
        startedForRunId.current = undefined
      }
    }
  }, [runId, start, stop])

  useEffect(() => {
    if ((status === 'done' || status === 'error') && onDone && doneFiredForRunId.current !== runId) {
      doneFiredForRunId.current = runId
      onDone()
    }
  }, [status, onDone, runId])

  const recentLines = singleLine ? lines.slice(-1) : terminalMode ? lines : lines.slice(-2)
  const renderLine = (line: (typeof lines)[number], truncateAt80: boolean) => {
    let text =
      (line.message as string) ||
      (line.type === 'file_done'
        ? `Processed ${line.file_name}`
        : line.type === 'done'
          ? 'Done.'
          : JSON.stringify(line))
    if (truncateAt80 && text.length > 80) text = text.substring(0, 80) + '...'
    const cls =
      line.type === 'error'
        ? truncateAt80
          ? 'text-red-500'
          : 'text-red-400 font-medium'
        : line.type === 'done'
          ? truncateAt80
            ? 'text-emerald-500'
            : 'text-emerald-400 font-semibold'
          : line.type === 'status'
            ? 'text-blue-400'
            : line.type === 'file_done'
              ? truncateAt80
                ? 'text-muted-foreground'
                : 'text-zinc-400'
              : undefined
    return {
      text,
      node: <span className={cls}>{line.type === 'file_done' ? `Processed ${String(line.file_name)}` : text}</span>,
    }
  }

  if (recentLines.length === 0) {
    if (singleLine) {
      return (
        <div className="flex items-center gap-1.5 text-[9px] font-mono text-muted-foreground truncate h-full w-full">
          {status === 'streaming' ? (
            <>
              <Loader2 className="w-3 h-3 animate-spin shrink-0" /> <span className="truncate">Loading logs...</span>
            </>
          ) : status === 'error' ? (
            <span className="text-red-500 truncate">Failed to load logs.</span>
          ) : (
            <span className="text-muted-foreground/50 truncate italic">Waiting...</span>
          )}
        </div>
      )
    }
    return (
      <div className="flex flex-col gap-1 w-full font-mono text-xs text-zinc-400 py-1">
        {startedAt && (
          <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
            [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
          </div>
        )}
        <div className="flex items-center gap-2">
          {status === 'streaming' ? (
            <>
              <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0 text-blue-400" />{' '}
              <span>Streaming console output...</span>
            </>
          ) : status === 'error' ? (
            <span className="text-red-400">Failed to stream logs.</span>
          ) : (
            <span className="text-zinc-500 italic">Waiting for stream...</span>
          )}
        </div>
      </div>
    )
  }

  if (singleLine) {
    return (
      <div className="flex flex-col text-[9px] font-mono text-muted-foreground truncate w-full h-full justify-center">
        {recentLines.map((line, i) => {
          const { text, node } = renderLine(line, true)
          return (
            <div key={line._id ?? `idx-${i}`} className="truncate w-full" title={typeof line.message === 'string' ? line.message : text}>
              {node}
            </div>
          )
        })}
      </div>
    )
  }

  return (
    <div
      role="log"
      aria-live="polite"
      aria-atomic="false"
      aria-label={terminalMode ? 'Cron job output stream' : 'Recent cron output'}
      className={
        terminalMode
          ? 'flex flex-col gap-1.5 w-full font-mono text-xs leading-relaxed text-zinc-200'
          : 'flex flex-col gap-0.5 mt-1.5 bg-background/50 border border-border/50 p-2 rounded text-[10px] font-mono text-muted-foreground w-[280px] max-w-sm overflow-hidden'
      }
    >
      {terminalMode && startedAt && (
        <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
          [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
        </div>
      )}
      {recentLines.map((line, i) => {
        const { text, node } = renderLine(line, false)
        return (
          <div
            key={line._id ?? `idx-${i}`}
            className={terminalMode ? 'whitespace-pre-wrap break-all w-full text-zinc-300' : 'truncate w-full'}
            title={typeof line.message === 'string' ? line.message : text}
          >
            {node}
          </div>
        )
      })}
    </div>
  )
}

export function FloatingOperationsDock({
  displayedJobs,
  setDisplayedJobs,
  isOpen,
  setIsOpen,
  selectedJobId,
  setSelectedJobId,
  onDismiss,
}: {
  displayedJobs: CronRun[]
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>
  isOpen: boolean
  setIsOpen: (open: boolean) => void
  selectedJobId: number | string | null
  setSelectedJobId: (id: number | string | null) => void
  onDismiss: (id: number) => void
}) {
  const { full, abbr } = useDateFormat()
  if (displayedJobs.length === 0) return null
  const activeJob = displayedJobs.find((j) => j.id === selectedJobId) || displayedJobs[0]
  const runningCount = displayedJobs.filter((j) => j.status === 'running').length

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2 pointer-events-auto">
      {!isOpen ? (
        <button
          onClick={() => setIsOpen(true)}
          aria-expanded={false}
          aria-controls="floating-ops-panel"
          aria-label={
            runningCount > 0
              ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running, open console`
              : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''}, open console`
          }
          className={cn(
            'px-4 py-2.5 rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-all hover:scale-105 duration-200 cursor-pointer border',
            runningCount > 0
              ? 'bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20 animate-bounce'
              : 'bg-zinc-850 hover:bg-zinc-800 text-zinc-300 border-zinc-700/50',
          )}
        >
          {runningCount > 0 ? (
            <RefreshCw className="h-3.5 w-3.5 animate-spin text-blue-200" />
          ) : (
            <Database className="h-3.5 w-3.5 text-zinc-400" />
          )}
          <span>
            {runningCount > 0
              ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running...`
              : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''} (logs)`}
          </span>
        </button>
      ) : (
        <div
          id="floating-ops-panel"
          className="bg-zinc-950 text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl w-[440px] sm:w-[500px] h-[380px] flex flex-col overflow-hidden animate-in slide-in-from-bottom-5 duration-300"
        >
          <div className="flex items-center justify-between px-3 py-2 bg-zinc-900 border-b border-zinc-800 shrink-0">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300">
              <Database className="h-3.5 w-3.5 text-blue-500" />
              <span>Console Log Terminal</span>
            </div>
            <button
              onClick={() => setIsOpen(false)}
              aria-expanded={true}
              aria-controls="floating-ops-panel"
              aria-label="Minimize console"
              className="text-zinc-400 hover:text-zinc-200 p-1 hover:bg-zinc-800 rounded cursor-pointer transition-colors"
            >
              <ChevronDown className="h-4 w-4" />
            </button>
          </div>
          {displayedJobs.length > 1 && (
            <div className="flex border-b border-zinc-800 bg-zinc-900/50 overflow-x-auto scrollbar-none shrink-0 px-2 pt-1 gap-1">
              {displayedJobs.map((job) => {
                const isActive = job.id === selectedJobId
                const taskLabel = job.task
                return (
                  <div
                    key={job.id}
                    className={cn(
                      'rounded-t text-[10px] font-mono uppercase tracking-wider flex items-center gap-1.5 border-t border-x transition-all shrink-0',
                      isActive
                        ? 'bg-zinc-950 text-blue-400 border-zinc-800 border-b-zinc-950 font-bold'
                        : 'bg-transparent text-zinc-400 border-transparent hover:text-zinc-200 hover:bg-zinc-800/30',
                    )}
                  >
                    <button
                      type="button"
                      onClick={() => setSelectedJobId(job.id)}
                      className="pl-3 py-1.5 flex items-center gap-1.5 cursor-pointer uppercase tracking-wider"
                    >
                      <span
                        className={cn(
                          'w-1.5 h-1.5 rounded-full transition-colors duration-300',
                          job.status === 'running' ? 'bg-blue-500 animate-pulse' : 'bg-zinc-600',
                        )}
                      />
                      {taskLabel}
                    </button>
                    <button
                      type="button"
                      onClick={() => onDismiss(job.id)}
                      className="mr-3 hover:bg-zinc-800 p-0.5 rounded text-zinc-500 hover:text-zinc-300 cursor-pointer"
                      title="Dismiss task"
                      aria-label={`Dismiss ${taskLabel} task`}
                    >
                      <X className="h-2.5 w-2.5" />
                    </button>
                  </div>
                )
              })}
            </div>
          )}
          <div className="flex-1 p-3 font-mono bg-zinc-950 overflow-y-auto flex flex-col justify-between">
            <div className="flex-1 overflow-hidden flex flex-col">
              <div className="text-[10px] text-zinc-500 border-b border-zinc-900 pb-1 mb-2 flex items-center justify-between shrink-0">
                <span>
                  STREAM ID: {activeJob?.id}
                  {activeJob?.started_at && ` • STARTED: ${full(activeJob.started_at)} ${abbr()}`}
                </span>
                {activeJob?.status === 'running' ? (
                  <span className="text-emerald-500 font-bold uppercase animate-pulse">● LIVE STREAMING</span>
                ) : (
                  <span className="text-zinc-500 font-bold uppercase">● COMPLETED</span>
                )}
              </div>
              <div className="flex-1 overflow-y-auto min-h-0 bg-black/30 rounded border border-zinc-900 p-2">
                <CronLiveLog
                  key={activeJob?.id}
                  runId={activeJob?.id}
                  singleLine={false}
                  startedAt={activeJob?.started_at}
                  onDone={() => {
                    if (activeJob?.id) {
                      setDisplayedJobs((prev) =>
                        prev.map((j) => (j.id === activeJob.id ? { ...j, status: 'completed' } : j)),
                      )
                    }
                  }}
                />
              </div>
            </div>
            <div className="mt-2 pt-2 border-t border-zinc-900 flex items-center justify-between text-[10px] text-zinc-500 shrink-0">
              <span>Task: {activeJob?.task}</span>
              <button
                onClick={() => onDismiss(activeJob?.id)}
                className="text-red-400 hover:text-red-300 hover:underline cursor-pointer"
              >
                Dismiss Active View
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
