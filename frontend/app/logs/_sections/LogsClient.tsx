'use client'
import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { parseAsString, useQueryState } from 'nuqs'
import dynamic from 'next/dynamic'
import { client, extractApiError, unwrapClient, queryKeys } from "@/lib/api";
import { showToast, showToastWithAction, cn, formatCount, formatCountMaybe, formatBytes } from "@/lib/utils";
import { cronCacheBust, useSSE, useSyncStatus, useDateFormat, type SyncStatus } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from "@/components/ui/button"
import { ConfirmDialog, CopyButton, HelpDialog, PageHeader } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Database, HardDrive, History, FileCode, Archive, ClipboardList, RefreshCw, Download, Bot, Terminal, ChevronRight, ChevronDown, Folder, Cloud, FileJson, Loader2, Maximize2, Minimize2, Settings, Clock, X, Check, HelpCircle, Trash2, } from 'lucide-react'
import { NoServiceSelected } from '@/components/ReportLayout'
import { useBootstrapResolved, useEffectiveServiceId, useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SyncFromCloudModal, LiveElapsed, NextRunCountdown, TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DataTable, DataTableReadonly, ColumnVisibilityDropdown, DateTimeCell, ingestedFilesColumns } from '@/components/DataTable'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import type { ColumnDef } from '@tanstack/react-table'
import type { WithRequired } from '@/types/api'
import type { components } from '@/types/api.generated'
import Loading from '../loading'
import { ControlledSSEModal as SSEModal } from '@/components/SSEModal'
export type CronRun = components['schemas']['CronRunEntry']
/**
 * Accumulate running and completed jobs, pruning older runs of the same task type to prevent
 * historical clutter: keep the active running job and the single most recent completed job
 * (last run) for each task. Returns `prev` untouched when nothing changed.
 */
function mergeRunningJobs(prev: CronRun[], entries: CronRun[]): CronRun[] {
  // 1. Identify which tasks are currently running in the poll response
  const runningTasks = new Set(entries.map((j) => j.task)), runningIds = new Set(entries.map((j) => j.id))
  // 2. Filter out completed jobs of the tasks that are now running a new instance
  const filtered = prev.filter((j) => runningIds.has(j.id) || !runningTasks.has(j.task))
  // 3. Keep only the single most recent completed job per task to prevent history clutter
  const jobsByTask: Record<string, CronRun[]> = {}
  for (const j of filtered) (jobsByTask[j.task] ??= []).push(j)
  const pruned: CronRun[] = []
  for (const taskJobs of Object.values(jobsByTask)) {
    const running = taskJobs.filter((j) => runningIds.has(j.id)), completed = taskJobs.filter((j) => !runningIds.has(j.id))
    if (running.length > 0) pruned.push(...running.map((j) => ({ ...j, status: 'running' })))
    if (completed.length > 0) pruned.push({ ...completed.reduce((max, job) => (job.id > max.id ? job : max), completed[0]), status: 'completed' })
  }
  // 4. Merge in brand new running jobs
  const prunedIds = new Set(pruned.map((j) => j.id))
  const nextJobs = [...pruned, ...entries.filter((j) => !prunedIds.has(j.id)).map((j) => ({ ...j, status: 'running' }))]
  // 5. Compare nextJobs with prev (by id, status, duration, and rows_ingested) to avoid updating state if nothing changed
  if (prev.length === nextJobs.length && prev.every((pj) => {
    const nj = nextJobs.find((j) => j.id === pj.id)
    return nj && nj.status === pj.status && nj.duration_s === pj.duration_s && nj.rows_ingested === pj.rows_ingested
  })) return prev
  return nextJobs
}
/**
 * Orchestrates all state, data queries, and side-effects for the
 * Logs / Data Management page. Returns the entire bag of values
 * needed by the rendering shell + its section components.
 */
export function useLogsPageState(initialTab?: string) {
  // Read activeServiceId via the bootstrap-aware fallback so a cold
  // load (empty localStorage, bootstrap still in flight or just
  // hydrated into the React Query cache) doesn't flash through the
  // <NoServiceSelected /> branch before useBootstrap's post-mount
  // effect writes the persisted Zustand store. ``services`` still
  // comes from the store — it's seeded synchronously by useBootstrap.
  const services = useServiceStore(s => s.services)
  const activeServiceId = useEffectiveServiceId() ?? null
  const activeService = services.find(s => s.id === activeServiceId)
  const isAnalyst = activeService?.accessLevel === 'read_only'
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useQueryState('tab', parseAsString.withDefault(initialTab || 'cron'))
  const [isPurgeOpen, setIsPurgeOpen] = useState(false)
  const [taskFilter, setTaskFilter] = useQueryState('task', parseAsString.withDefault('all'))
  const [statusFilter, setStatusFilter] = useQueryState('status', parseAsString.withDefault('all'))
  const [eventFilter, setEventFilter] = useQueryState('event', parseAsString.withDefault('all'))
  const { lines, status: sseStatus, error: sseError, start, stop, reset } = useSSE()
  const [isSSEModalOpen, setIsSSEModalOpen] = useState(false)
  const [isSyncModalOpen, setIsSyncModalOpen] = useState(false)
  const [sseTitle, setSseTitle] = useState('')
  const [sseDescription, setSseDescription] = useState('')
  const [consoleOpen, setConsoleOpen] = useState(false)
  const [selectedConsoleJobId, setSelectedConsoleJobId] = useState<number | string | null>(null)
  const notifyCronRun = useCallback((run: CronRun) => {
    const taskName = run.task === 'metadata_sync' ? 'sync' : run.task
    const label = run.status === 'running' ? 'Background Sync Started' : run.status === 'error' ? 'Background Sync Failed' : 'Background Sync Completed'
    const details = [taskName, run.status !== 'running' && run.rows_ingested != null ? `${formatCount(run.rows_ingested)} rows` : null, run.status !== 'running' && run.duration_s != null ? `${run.duration_s.toFixed(1)}s` : null].filter(Boolean).join(' · ')
    showToastWithAction(`${label} (${details})`, { actionLabel: 'View Console Logs', kind: run.status === 'error' ? 'error' : run.status === 'running' ? 'info' : 'success', durationMs: 8000, onAction: () => { setSelectedConsoleJobId(run.id); setConsoleOpen(true) } })
  }, [])
  // Multi-tenant safe run ID tracker to prevent alerting old runs or cross-tenant leaks
  const maxSeenIdRef = React.useRef<number | null>(null)
  // Reset tracker when switching active services
  useEffect(() => { maxSeenIdRef.current = null }, [activeServiceId])
  const { data: status } = useSyncStatus()
  const { data: cronLogs, isLoading: isLoadingCron, isFetching: isFetchingCron } = useQuery({
    queryKey: queryKeys.admin.cronLogs(activeServiceId, taskFilter, statusFilter),
    queryFn: ({ signal }) => unwrapClient(client.GET("/api/cron-runs", { signal, params: { query: { page: 1, per_page: 500, task: taskFilter === 'all' ? undefined : taskFilter, status: statusFilter === 'all' ? undefined : statusFilter } } })),
    enabled: !!activeServiceId && activeTab === 'cron',
    // Primary update path is the /api/admin/events/stream cron-runs channel
    // (wired in SyncStatusBadge via useAdminEventStream — runs whenever
    // the header is mounted). Every cron lifecycle event invalidates
    // this query key, so this interval is now a safety net only.
    // Five minutes is well under "stale" for a history table and cuts
    // the steady-state request rate by 10× vs. the prior 30 s cadence.
    refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, staleTime: 30_000,
  })
  // Separate query specifically for checking recent crons (including running) without reloading the entire 500-row table.
  // Delta poll (O5): reads `maxSeenIdRef.current` and passes (max - 1) as
  // `since_id` so steady-state polls return ~1 entry instead of 10.
  // Backend semantics (`backend/core/metadata_db.py::get_cron_runs`): rows
  // where id > since_id OR status = 'running'. The OR keeps still-running
  // rows visible across polls. The `-1` keeps the most-recently-seen row
  // in the response for ONE more poll so the toast-completion-detection
  // effect below can observe the running→completed transition for the row
  // backgroundCronToast is tracking. First poll (maxSeenIdRef.current is
  // null) omits since_id and returns up to per_page recent rows like before.
  const { data: recentCrons } = useQuery({
    queryKey: queryKeys.admin.cronLogsRecent(activeServiceId),
    queryFn: ({ signal }) => unwrapClient(client.GET("/api/cron-runs", { signal, params: { query: { page: 1, per_page: 10, since_id: maxSeenIdRef.current != null ? Math.max(0, maxSeenIdRef.current - 1) : undefined } } })),
    enabled: !!activeServiceId, // Tab independent polling!
    // Primary update path is the /api/admin/events/stream cron-runs channel
    // (wired in SyncStatusBadge via useAdminEventStream). The toast
    // notifier reads this query key, so SSE invalidations propagate
    // automatically; this interval is now a safety net only.
    refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, staleTime: 15_000,
  })
  // Derive currently running crons and loading state from recent crons to keep downstream compatibility intact
  const runningCrons = React.useMemo(() => ({ entries: (recentCrons?.entries || []).filter((e) => e.status === 'running') }), [recentCrons?.entries])
  // When a running cron completes, refresh the main table so it shows up in the history
  const prevRunningCount = React.useRef(0)
  React.useEffect(() => {
    const count = runningCrons?.entries?.length || 0
    if (prevRunningCount.current > 0 && count < prevRunningCount.current) { queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsAll() }); queryClient.invalidateQueries({ queryKey: queryKeys.admin.status() }) }
    prevRunningCount.current = count
  }, [runningCrons?.entries?.length, queryClient])
  // Accumulate running and completed jobs, pruning older runs of the same task type
  // to prevent historical clutter. We only keep the active running job and the single
  // most recent completed job (last run) for each task.
  const [displayedJobs, setDisplayedJobs] = useState<CronRun[]>([])
  const runningEntries = runningCrons?.entries
  const [seenRunningEntries, setSeenRunningEntries] = useState<typeof runningEntries | undefined>(undefined)
  if (runningEntries && runningEntries !== seenRunningEntries) {
    // Derived during render (the poll result is the input), not synced in an effect.
    setSeenRunningEntries(runningEntries)
    setDisplayedJobs((prev) => mergeRunningJobs(prev, runningEntries))
  }
  const removeDisplayedJob = useCallback((id: number) => setDisplayedJobs(prev => prev.filter((j) => j.id !== id)), [])
  // Effect to monitor recent crons and detect newly started or completed runs (even if they ran very fast)
  useEffect(() => {
    if (!recentCrons?.entries?.length) return
    const maxId = Math.max(...recentCrons.entries.map((e) => e.id))
    const enqueueJob = (run: CronRun) => {
      // Automatically add it to displayedJobs so it appears in the Console Terminal dock
      setDisplayedJobs(prev => (prev.some((j) => j.id === run.id) ? prev : [...prev, { ...run, status: run.status }]))
      notifyCronRun(run)
    }
    if (maxSeenIdRef.current === null) {
      // First load: initialize max seen ID so we don't alert on historical runs
      maxSeenIdRef.current = maxId
      // Eagerly capture any running crons at load time and display them as running with live streams
      recentCrons.entries.filter((e) => e.status === 'running').forEach(enqueueJob)
      return
    }
    // On subsequent polls, check if we have any brand new runs!
    if (maxId > maxSeenIdRef.current) {
      const newRuns = recentCrons.entries.filter((e) => e.id > (maxSeenIdRef.current || 0))
      // Update max seen ID
      maxSeenIdRef.current = maxId
      // Processes new runs and queue notifications/console placement
      newRuns.forEach(enqueueJob)
    }
  }, [recentCrons?.entries, notifyCronRun])
  // Auto-focus the floating console on the most relevant active job. Adjusted during render
  // from `displayedJobs` (the input), never synced through an effect.
  if (displayedJobs.length > 0) {
    if (selectedConsoleJobId === null || !displayedJobs.some((j) => j.id === selectedConsoleJobId)) setSelectedConsoleJobId(displayedJobs[0].id)
  } else if (selectedConsoleJobId !== null || consoleOpen) { setSelectedConsoleJobId(null); setConsoleOpen(false) }
  const { data: cronSchedule, isLoading: isLoadingSchedule } = useQuery({
    queryKey: queryKeys.admin.cronSchedule(activeServiceId),
    queryFn: ({ signal }) => unwrapClient(client.GET("/api/cron-schedule", { signal })), enabled: !!activeServiceId && activeTab === 'cron',
    // Freshness via useAdminEventStream (wired in SyncStatusBadge):
    // every cron lifecycle event invalidates this key so the tiles'
    // last_run / next_run / status update on the moment the cron
    // fires instead of on a 10s drift. 5-min interval is the safety
    // net for silently-dropped streams.
    staleTime: 10_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  const orderedSchedules = React.useMemo(() => {
    // Display priority for known tasks. Backend (/api/cron-schedule) is
    // the source of truth for WHICH tasks exist — anything not in this
    // map is still rendered, just appended after the prioritised tiles
    // in API order. That means a freshly-registered backend cron shows
    // up on the grid automatically; only its position needs curating.
    const TASK_PRIORITY: Record<string, number> = {
      log_discovery: 1, rum_sync: 2, rum_discovery: 2, alerts: 3, commit: 4, rum_commit: 5, optimize: 6, local_compact: 7,
      metadata_cleanup: 8, expire_snapshots: 9, full_sync: 10, gap_heal: 11, ngwaf_sync: 12, insights_prewarmer: 13, rollup_compact_daily: 14, rollup_hour_heal: 15, metadata_sync: 16,
    }
    // Analysts only see the read-only subset; nothing else is even
    // exposed via the analyst-facing /api/cron-schedule path.
    const analystAllowed = new Set(['metadata_sync', 'alerts'])
    // For admin views, hide `metadata_sync` — it's the analyst-only
    // read-only counterpart of `sync` and only shows up here as a
    // historical-run entry (next_run_time=null). Worse, CronScheduleBox
    // renders metadata_sync with the LABEL "sync" by design (so the
    // analyst tile reads naturally), which created a confusing duplicate
    // tile both labelled "sync" once the whitelist was lifted.
    const adminExcluded = new Set(['metadata_sync']), source = cronSchedule?.schedules ?? [], hasNgwaf = !!status?.ngwaf_workspace_id
    const filtered = isAnalyst ? source.filter((s) => analystAllowed.has(s.task)) : source.filter((s) => !adminExcluded.has(s.task) && (s.task !== 'ngwaf_sync' || hasNgwaf))
    const sorted = [...filtered].sort((a, b) => (TASK_PRIORITY[a.task] ?? 999) - (TASK_PRIORITY[b.task] ?? 999) || a.task.localeCompare(b.task))
    return sorted.map((schedule) => ({ task: schedule.task, activeJob: displayedJobs.find((j) => j.task === schedule.task && j.status === 'running'), schedule }))
  }, [cronSchedule?.schedules, displayedJobs, isAnalyst, status?.ngwaf_workspace_id])
  const { data: catalog } = useLogFieldsCatalog()
  const catalogMaps = React.useMemo(() => {
    if (!catalog) return { groups: {}, fields: {} }
    const groups: Record<string, { label: string; description: string }> = {}, fields: Record<string, { label: string; description: string }> = {}
    catalog.groups?.forEach((g) => { groups[g.id === null ? "null" : String(g.id)] = { label: g.label, description: g.description } })
    catalog.fields?.forEach((f) => { fields[f.id] = { label: f.label, description: f.description } })
    return { groups, fields }
  }, [catalog])
  const { data: auditLogs, isLoading: isLoadingAudit, isFetching: isFetchingAudit } = useQuery({
    queryKey: queryKeys.admin.auditLogs(activeServiceId, eventFilter),
    queryFn: ({ signal }) => unwrapClient(client.GET("/api/audit-logs", { signal, params: { query: { page: 1, per_page: 500, event_type: eventFilter === 'all' ? undefined : eventFilter } } })),
    enabled: !!activeServiceId && activeTab === 'service_history',
    // Kept fresh by useAdminEventStream (invalidates ['admin','audit-logs',svc]
    // on cron completion), so the old staleTime:0 — which forced a 1-2s
    // refetch spinner on every tab visit — is no longer needed. 60s makes
    // tab-switches instant from cache between real events.
    staleTime: 60_000,
  })
  const { data: ingestedFiles, isLoading: isLoadingIngested } = useQuery({
    queryKey: queryKeys.admin.ingestedFiles(activeServiceId), queryFn: ({ signal }) => unwrapClient(client.GET("/api/admin/ingested-files", { signal })), enabled: !!activeServiceId && activeTab === 'ingestion',
    // Freshness via useAdminEventStream (invalidates
    // ['admin','ingested-files',svc] on cron completion). 60s staleTime so
    // re-entering the tab is instant instead of re-fetching every time.
    staleTime: 60_000,
  })
  const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
    queryKey: queryKeys.admin.schema(activeServiceId), queryFn: ({ signal }) => unwrapClient(client.GET("/api/schema", { signal })), enabled: !!activeServiceId && activeTab === 'schema',
    // Freshness via useAdminEventStream (invalidates ['admin','schema',svc]
    // when a data-mutating cron — sync/full_sync/commit — completes).
    // Schema rarely changes, so 60s staleTime keeps the tab instant.
    staleTime: 60_000,
  })
  const purgeMutation = useMutation({
    mutationFn: async () => { await unwrapClient(client.DELETE("/api/cron-runs", {})) },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) }); setIsPurgeOpen(false) },
  })
  const handleTabChange = useCallback((value: string) => {
    void setActiveTab(value)
    // cron / service_history / ingestion / schema no longer force a refetch
    // on switch: cron is SSR-seeded and kept live by useAdminEventStream, and
    // the other three are now invalidated by that same stream on cron
    // completion (+ a 60s staleTime safety net). Switching to them serves
    // instantly from cache instead of spinning on every visit.
    //
    // iceberg / raw have no stream coverage, so keep refreshing on entry.
    if (value === 'iceberg') { queryClient.invalidateQueries({ queryKey: queryKeys.admin.iceberg(activeServiceId) }); queryClient.invalidateQueries({ queryKey: queryKeys.admin.icebergTree(activeServiceId) }) }
    else if (value === 'raw') queryClient.invalidateQueries({ queryKey: queryKeys.admin.rawTree(activeServiceId) })
  }, [activeServiceId, queryClient, setActiveTab])
  return {
    // identity
    activeServiceId, isAnalyst,
    // tab state
    activeTab, handleTabChange,
    // cron tab filters
    taskFilter, setTaskFilter, statusFilter, setStatusFilter,
    // audit tab filter
    eventFilter, setEventFilter,
    // SSE modal / sync modal
    sseStatus, sseError, sseTitle, sseDescription, setSseTitle, setSseDescription, lines, start, stop, reset, isSSEModalOpen, setIsSSEModalOpen, isSyncModalOpen, setIsSyncModalOpen,
    // floating console
    consoleOpen, setConsoleOpen, selectedConsoleJobId, setSelectedConsoleJobId, displayedJobs, setDisplayedJobs, removeDisplayedJob,
    // data
    status, cronLogs, isLoadingCron, isFetchingCron, recentCrons, orderedSchedules, isLoadingSchedule, catalogMaps, auditLogs, isLoadingAudit, isFetchingAudit, ingestedFiles, isLoadingIngested, schemaData, isLoadingSchema,
    // mutations / actions
    purgeMutation, isPurgeOpen, setIsPurgeOpen,
  }
}
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
  log_discovery: 'Log Discovery', commit: 'Commit', full_sync: 'Full Discovery Sweep', gap_heal: 'Gap Heal', local_compact: 'Local Compact', optimize: 'Cloud Compact', expire_snapshots: 'Expire Snapshots',
  rollup_compact_daily: 'Rollup Compact', rollup_hour_heal: 'Rollup Heal', partial_hour_merge: 'Partial-Hour Merge', ledger_sweep: 'Ledger Sweep', insights_prewarmer: 'Insights Prewarmer', metadata_cleanup: 'Metadata Cleanup',
  ngwaf_sync: 'NGWAF Sync', metadata_sync: 'Metadata Sync', alerts: 'Alerts Evaluation', rum_sync: 'RUM Discovery', rum_discovery: 'RUM Discovery', rum_commit: 'Ingest RUM',
}
export const CRON_GROUPS = [
  { title: 'Ingestion Pipeline', tasks: ['log_discovery', 'commit', 'full_sync', 'gap_heal'] },
  { title: 'Storage Maintenance', tasks: ['local_compact', 'optimize', 'expire_snapshots'] },
  { title: 'Rollups & Caching', tasks: ['rollup_compact_daily', 'rollup_hour_heal', 'insights_prewarmer', 'partial_hour_merge'] },
  { title: 'Metadata & Enrichments', tasks: ['metadata_cleanup', 'ngwaf_sync', 'metadata_sync', 'alerts'] },
  { title: 'Real User Monitoring', tasks: ['rum_sync', 'rum_discovery', 'rum_commit'] },
]
type CronScheduleEntry = components['schemas']['CronScheduleEntry']
type CronSchedule = Pick<WithRequired<CronScheduleEntry, 'task'>, 'task' | 'disabled_reason' | 'last_run_time' | 'next_run_time'>
type ActiveCronJob = { id: number | string; started_at?: string | null }
function CronTip({ className, side, contentClass = "text-xs", tip, children }: { className?: string; side?: 'top' | 'right'; contentClass?: string; tip: React.ReactNode; children: React.ReactNode }) {
  return <TooltipProvider delay={200}><Tooltip><TooltipTrigger render={<span className={className} />}>{children}</TooltipTrigger><TooltipContent side={side} className={contentClass}>{tip}</TooltipContent></Tooltip></TooltipProvider>
}
export function CronScheduleBox({ schedule, activeJob = null, onOpenConsole }: { schedule: CronSchedule; activeJob?: ActiveCronJob | null; onOpenConsole?: (jobId: number | string) => void }) {
  const { full, abbr } = useDateFormat()
  if (schedule.disabled_reason === 'no_alerts_configured') {
    return (
      <div className="relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 bg-muted/20 border-muted min-w-[130px] flex-1">
        <div className="flex items-center gap-2 w-full">
          <CronTip className="text-[9px] font-bold uppercase text-muted-foreground tracking-wider shrink-0" side="top" contentClass="max-w-[250px] text-xs" tip={CRON_EXPLANATIONS.alerts}>alerts</CronTip>
          <div className="w-px h-4 bg-border shrink-0" />
          <span className="flex-1 min-w-0 truncate text-[9px] text-muted-foreground italic">No alerts configured.</span>
        </div>
      </div>
    )
  }
  const isRunning = !!activeJob
  const borderColor = isRunning ? 'border-blue-500/60 shadow-[0_0_8px_rgba(59,130,246,0.15)] bg-blue-500/5' : 'border-muted bg-muted/20'
  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 transition-all ${borderColor} min-w-[130px] flex-1`}>
      {isRunning && <div className="absolute inset-0 rounded-md border border-blue-500/50 animate-pulse pointer-events-none" />}
      <div className="flex items-center gap-2 w-full">
        <CronTip className={`text-[9px] font-bold uppercase tracking-wider shrink-0 ${isRunning ? 'text-blue-700 dark:text-blue-300' : 'text-muted-foreground'}`} side="top" contentClass="max-w-[250px] text-xs" tip={CRON_EXPLANATIONS[schedule.task] || 'Background job.'}>{CRON_DISPLAY_NAMES[schedule.task] || schedule.task}</CronTip>
        <div className="w-px h-4 bg-border shrink-0" />
        {isRunning ? (
          <button onClick={() => onOpenConsole?.(activeJob.id)} className="flex-1 min-w-0 text-left text-[9px] text-blue-700 dark:text-blue-300 hover:text-blue-800 dark:hover:text-blue-200 hover:underline font-medium flex items-center justify-between cursor-pointer" aria-label={`Running for ${schedule.task} — click for logs`}>
            <span className="flex items-center gap-1 min-w-0">
              <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-700 dark:text-blue-300" aria-hidden="true" />
              {activeJob.started_at && <LiveElapsed startedAt={activeJob.started_at} className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-[9px] font-medium" />}
            </span>
            <span className="text-[8px] bg-blue-500/20 px-1 py-0.2 rounded border border-blue-500/20 shrink-0 ml-1">LOGS</span>
          </button>
        ) : (
          <div className="flex-1 min-w-0 flex items-center justify-between text-[9px] text-muted-foreground whitespace-nowrap overflow-hidden">
            <CronTip className="truncate pr-2" tip={schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}>Last: {schedule.last_run_time ? <TimeAgo timestamp={schedule.last_run_time} /> : 'Never'}</CronTip>
            <CronTip className="truncate" tip={schedule.next_run_time ? `${full(schedule.next_run_time)} ${abbr()}` : 'Disabled'}>Next: <NextRunCountdown when={schedule.next_run_time} /></CronTip>
          </div>
        )}
      </div>
    </div>
  )
}
function CronScheduleGroupSection({ title, items, onOpenConsole }: { title: string; items: Array<{ task: string; activeJob: CronRun | undefined; schedule: CronScheduleEntry }>; onOpenConsole: (jobId: number | string) => void }) {
  if (items.length === 0) return null
  return (
    <div className="space-y-3">
      <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">{title}</div>
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
        {items.map((item) => <CronScheduleBox key={item.task} schedule={item.schedule || { task: item.task }} activeJob={item.activeJob} onOpenConsole={onOpenConsole} />)}
      </div>
    </div>
  )
}
export function CronTab({ cronColumns, cronLogs, isLoadingCron, isFetchingCron, orderedSchedules, isLoadingSchedule = false, taskFilter, setTaskFilter, statusFilter, setStatusFilter, isAnalyst, status, activeServiceId, setDisplayedJobs, setSelectedConsoleJobId, setConsoleOpen, isPurgeOpen, setIsPurgeOpen, purgeMutation }: {
  cronColumns: ColumnDef<CronRun>[]; cronLogs: components['schemas']['CronRunsResponse'] | undefined; isLoadingCron: boolean; isFetchingCron: boolean
  orderedSchedules: Array<{ task: string; activeJob: CronRun | undefined; schedule: CronScheduleEntry }>; isLoadingSchedule?: boolean; taskFilter: string; setTaskFilter: (v: string) => void; statusFilter: string; setStatusFilter: (v: string) => void
  isAnalyst: boolean; status: SyncStatus | undefined; activeServiceId: string | null | undefined; setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>; setSelectedConsoleJobId: (id: number | string | null) => void; setConsoleOpen: (open: boolean) => void; isPurgeOpen: boolean; setIsPurgeOpen: (open: boolean) => void; purgeMutation: { isPending: boolean; mutate: () => void }
}) {
  const queryClient = useQueryClient()
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)
  const tableData = React.useMemo(() => (cronLogs?.entries || []).filter((e) => e.status !== 'running'), [cronLogs?.entries])
  const handleOpenConsole = React.useCallback((jobId: number | string) => { setConsoleOpen(true); setSelectedConsoleJobId(jobId) }, [setConsoleOpen, setSelectedConsoleJobId])
  const groupedTaskNames = React.useMemo(() => CRON_GROUPS.flatMap(g => g.tasks), [])
  const otherTasks = React.useMemo(() => orderedSchedules.filter(s => !groupedTaskNames.includes(s.task)), [orderedSchedules, groupedTaskNames])
  const adminTaskFilterOptions: Array<[string, string, boolean?]> = [
    ['rum_sync', 'RUM Sync'], ['full_sync', 'Full Sync'], ['gap_heal', 'Gap Heal'], ['alerts', 'Alerts', true],
    ['commit', 'Commit'], ['rum_commit', 'RUM Commit'], ['optimize', 'Optimize'], ['local_compact', 'Local Compact'],
    ['rollup_compact_daily', 'Rollup Compact'], ['rollup_hour_heal', 'Rollup Heal'], ['expire_snapshots', 'Expire'],
    ...(status?.ngwaf_workspace_id ? [['ngwaf_sync', 'NGWAF Sync'] as [string, string]] : []), ['metadata_cleanup', 'Metadata Cleanup'],
  ]
  return (
    <div className="p-0">
      <DataTable
        columns={cronColumns} data={tableData} isLoading={isLoadingCron} initialSorting={[{ id: 'started_at', desc: true }]}
        onRowClick={(row: CronRun) => { setDisplayedJobs(prev => prev.some((j) => j.id === row.id) ? prev : [...prev, { ...row, status: row.status }]); setSelectedConsoleJobId(row.id); setConsoleOpen(true) }}
        renderToolbar={(table) => (
          <>
            {(orderedSchedules.length > 0 || isLoadingSchedule) && (
              <div className="p-4 border-b bg-muted/10 space-y-5">
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-medium">Cron Jobs</h3>
                  <button onClick={() => setIsHelpOpen(true)} aria-label="About background cron jobs" className="text-muted-foreground hover:text-foreground"><HelpCircle className="h-4 w-4" /></button>
                </div>
                <HelpDialog open={isHelpOpen} onOpenChange={setIsHelpOpen} title="Background Cron Jobs" icon={<HelpCircle className="h-5 w-5" />} size="xl">
                  <div className="space-y-6">
                    <p>Fastly Log Analytics uses a distributed architecture of background jobs to ingest logs, perform maintenance, and pre-calculate analytics.</p>
                    {CRON_GROUPS.map(group => (
                      <div key={group.title} className="space-y-2">
                        <h4 className="font-semibold text-foreground">{group.title}</h4>
                        <ul className="space-y-3">
                          {group.tasks.map(task => (
                            <li key={task} className="pl-4 border-l-2 border-muted">
                              <div className="flex items-center gap-2 mb-1"><span className="font-medium text-foreground">{CRON_DISPLAY_NAMES[task] || task}</span><span className="text-[10px] font-mono text-muted-foreground px-1.5 py-0.5 bg-muted rounded border">{task}</span></div>
                              <p className="text-xs text-muted-foreground/80 leading-relaxed">{CRON_EXPLANATIONS[task]}</p>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </HelpDialog>
                <div className="space-y-5">
                  {orderedSchedules.length === 0 ? (isAnalyst ? [CRON_GROUPS[3]] : CRON_GROUPS).map(group => (
                    <div key={group.title} className="space-y-3">
                      <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">{group.title}</div>
                      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                        {group.tasks.slice(0, isAnalyst ? 2 : 3).map((task) => <div key={task} className="h-8 rounded-md border border-muted bg-muted/20 animate-pulse min-w-[130px] flex-1" />)}
                      </div>
                    </div>
                  )) : (
                    <>
                      {CRON_GROUPS.map(group => <CronScheduleGroupSection key={group.title} title={group.title} items={orderedSchedules.filter(s => group.tasks.includes(s.task))} onOpenConsole={handleOpenConsole} />)}
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
                    <SelectTrigger className="h-8 w-[140px] text-xs"><SelectValue placeholder="All tasks" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All tasks</SelectItem>
                      <SelectItem value={isAnalyst ? 'metadata_sync' : 'log_discovery'}>Log Discovery</SelectItem>
                      {adminTaskFilterOptions.filter(([, , allowAnalyst]) => !isAnalyst || allowAnalyst).map(([val, label]) => <SelectItem key={val} value={val}>{label}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs"><SelectValue placeholder="All statuses" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All statuses</SelectItem><SelectItem value="success">Success</SelectItem>
                      {!isAnalyst && <SelectItem value="partial_success">Partial Success</SelectItem>}<SelectItem value="error">Error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <ColumnVisibilityDropdown table={table} />
                <Button variant="outline" size="sm" className="h-8 text-xs" disabled={isFetchingCron} onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })}><RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingCron ? 'animate-spin' : ''}`} />Refresh</Button>
                <button className={cn(buttonVariants({ variant: "outline", size: "sm" }), "h-8 text-xs border-destructive/50 text-destructive hover:bg-destructive hover:text-white cursor-pointer")} onClick={() => setIsPurgeOpen(true)}><Trash2 className="h-3 w-3 mr-1.5" /> Purge Logs</button>
              </div>
              <ConfirmDialog open={isPurgeOpen} onOpenChange={setIsPurgeOpen} title="Purge all cron logs?" description="This will permanently delete the entire history of automated background job records for this service." confirmLabel="Purge" isDangerous isPending={purgeMutation.isPending} onConfirm={() => purgeMutation.mutate()} />
            </div>
          </>
        )}
      />
    </div>
  )
}
type CronRunsResponse = components['schemas']['CronRunsResponse']
type SchemaColumn = components['schemas']['SchemaResponse']['schema'][number]
type IngestedFile = components['schemas']['IngestedFile']
type AuditLogEntry = components['schemas']['AuditLogEntry']
type TreeNode = components['schemas']['TreeNode']
interface AuditDetails {
  bucket?: string; fos_bucket_name?: string; prefix?: string; fos_prefix?: string; region?: string; fos_region?: string; sample_rate?: number | string; log_period?: number | string
  edge_only?: boolean; cdn_url?: string; enable_cron_sync?: boolean; log_retention_days?: number; delete_after?: boolean; log_fields?: { groups?: string[]; field_overrides?: Record<string, boolean> }
  log_fields_deployed?: boolean; groups_before?: string[]; groups_after?: string[]; fields_added?: string[]; fields_removed?: string[]
}
export type CatalogMaps = { groups: Record<string, { label: string; description: string }>; fields: Record<string, { label: string; description: string }> }
const DASH_CELL = <span className="text-muted-foreground">—</span>
const PROCESSING_CELL = <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
const NO_FILES_TASKS = new Set(['expire_snapshots', 'rollup_compact_daily', 'rollup_hour_heal', 'metadata_cleanup', 'insights_prewarmer', 'partial_hour_merge'])
const NO_ROWS_TASKS = new Set([...NO_FILES_TASKS, 'optimize', 'commit', 'rum_commit', 'local_compact'])
const ADMIN_ONLY_CRON_COLUMNS: ColumnDef<CronRun>[] = [
  { accessorKey: 'files_deleted_fos', header: 'Log Files Deleted', cell: ({ row }) => row.original.status === 'running' || row.original.task !== 'log_discovery' ? DASH_CELL : <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(row.original.files_deleted_fos || 0)}</span> },
  { id: 'rows_committed', header: 'Rows Committed', cell: ({ row }) => {
    if (row.original.status === 'running') return PROCESSING_CELL
    const val = row.original.task === 'commit' ? row.original.rows_ingested : null
    return <span className="font-mono text-muted-foreground tabular-nums text-xs">{val != null ? formatCount(val) : DASH_CELL}</span>
  } },
]
export function useCronColumns(isAnalyst: boolean): ColumnDef<CronRun>[] {
  return React.useMemo(() => [
    { accessorKey: 'started_at', id: 'started_at', meta: { label: 'Started At' }, header: 'Started At', cell: ({ row }) => <DateTimeCell iso={row.original.started_at} /> },
    { id: 'finished_at', meta: { label: 'Finished At' }, enableSorting: false, accessorFn: (row) => row.started_at && row.duration_s != null ? new Date(new Date(row.started_at).getTime() + row.duration_s * 1000).toISOString() : null, header: () => <span className="text-xs font-medium px-2.5">Finished At</span>, cell: ({ row }) => {
      const { status, started_at: startIso, duration_s: dur } = row.original
      return status === 'running' || !startIso || dur == null ? DASH_CELL : <DateTimeCell iso={new Date(new Date(startIso).getTime() + dur * 1000).toISOString()} />
    } },
    { accessorKey: 'task', id: 'task', meta: { label: 'Task' }, header: 'Task', cell: ({ row }) => {
      const { task, summary } = row.original
      const isSync = task === 'log_discovery' || task === 'metadata_sync'
      return (
        <div className="flex flex-col gap-1 py-1">
          <TooltipProvider delay={200}><Tooltip>
            <TooltipTrigger render={<Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", isSync ? "bg-blue-500/10 text-blue-600 hover:bg-blue-500/20" : "bg-purple-500/10 text-purple-600 hover:bg-purple-500/20")}>{CRON_DISPLAY_NAMES[task] || task}</Badge>} />
            <TooltipContent side="right" className="max-w-[250px] text-xs"><p>{CRON_EXPLANATIONS[task] || 'Background job.'}</p></TooltipContent>
          </Tooltip></TooltipProvider>
          {summary && <span className="text-[11px] text-muted-foreground whitespace-normal break-words leading-tight">{summary}</span>}
        </div>
      )
    } },
    { accessorKey: 'status', id: 'status', meta: { label: 'Status' }, header: 'Status', cell: ({ row }) => {
      const { status: val, error_message: err, corrupt_rows } = row.original
      if (val === 'running') return <Badge variant="outline" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold border-blue-500/30 text-blue-700 dark:text-blue-400 bg-blue-500/10 flex items-center gap-1 w-fit"><Loader2 className="w-3 h-3 animate-spin" /> Running</Badge>
      if (val === 'skipped') return <Badge variant="secondary" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">No Alerts</Badge>
      if (val === 'success' && (!corrupt_rows || corrupt_rows === 0)) return <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Success</Badge>
      if (val === 'partial_success' || (val === 'success' && (corrupt_rows ?? 0) > 0)) return <Badge variant="warning" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Partial Success</Badge>
      return (
        <div className="flex items-center gap-1.5">
          <Tooltip><TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold" />}>Error</TooltipTrigger><TooltipContent className="max-w-[400px] break-words bg-destructive text-white dark:text-white"><p className="text-xs font-mono">{err || 'Unknown error'}</p></TooltipContent></Tooltip>
          {err && <CopyButton text={err} ariaLabel="Copy full error message" title="Copy full error message" stopPropagation className="h-6 w-6 text-muted-foreground hover:text-foreground" />}
        </div>
      )
    } },
    { accessorKey: 'duration_s', id: 'duration_s', meta: { label: 'Duration' }, header: 'Duration', cell: ({ row }) => {
      const s = row.original.duration_s
      if (s == null) return <span className="font-mono text-muted-foreground tabular-nums text-xs">—</span>
      if (row.original.status === 'running') return <LiveElapsed startedAt={row.original.started_at} className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-xs font-medium" />
      const fmt = s < 1 ? `${Math.round(s * 1000)}ms` : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
      return <span className="font-mono text-muted-foreground tabular-nums text-xs">{fmt}</span>
    } },
    { accessorKey: 'files_downloaded', id: 'files_downloaded', meta: { label: 'Files Downloaded' }, header: 'Files Processed', cell: ({ row }) => {
      if (row.original.status === 'running') return PROCESSING_CELL
      const { task, files_downloaded, rows_ingested, parquet_files_optimized } = row.original
      if (NO_FILES_TASKS.has(task) || (task === 'metadata_sync' && !isAnalyst) || ((task === 'commit' || task === 'rum_commit') && !rows_ingested)) return DASH_CELL
      const count = task === 'commit' || task === 'rum_commit' ? 1 : task === 'optimize' ? (parquet_files_optimized || 0) : (files_downloaded || 0)
      const label = task === 'alerts' ? (count === 1 ? 'alert evaluated' : 'alerts evaluated') : task === 'commit' || task === 'rum_commit' ? 'cloud file cached' : task === 'metadata_sync' ? 'cloud files downloaded' : task === 'optimize' || task === 'local_compact' ? 'files merged' : task === 'rum_sync' || task === 'rum_discovery' ? 'raw RUM logs' : task === 'ngwaf_sync' ? (count === 1 ? 'bot record' : 'bot records') : 'raw logs'
      return <span className="font-mono text-muted-foreground tabular-nums text-xs" title={label}>{formatCount(count)} <span className="text-[10px] text-muted-foreground">{label}</span></span>
    } },
    { accessorKey: 'rows_ingested', header: isAnalyst ? 'Log Entries Imported' : 'Log Entries Processed', cell: ({ row }) => {
      if (row.original.status === 'running') return PROCESSING_CELL
      const { task, rows_ingested, corrupt_rows, error_message } = row.original
      if (NO_ROWS_TASKS.has(task)) return DASH_CELL
      if (task === 'alerts') { const triggered = rows_ingested || 0; return triggered === 0 ? DASH_CELL : <span className="font-mono tabular-nums text-xs text-amber-500 font-medium">{triggered} {triggered === 1 ? 'alert triggered' : 'alerts triggered'}</span> }
      const rows = rows_ingested || 0, corrupt = corrupt_rows || 0
      if (task === 'metadata_sync') return rows === 0 ? DASH_CELL : <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(rows)}</span>
      return (
        <div className="flex items-center gap-2">
          <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(rows)}</span>
          {corrupt > 0 && (
            <div className="flex items-center gap-1 group/corrupt">
              <Tooltip><TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none text-[10px] font-bold" />}>{formatCount(corrupt)} Skipped</TooltipTrigger><TooltipContent>These lines were skipped due to missing timestamps or invalid JSON structure.</TooltipContent></Tooltip>
              {error_message && <CopyButton text={error_message} ariaLabel="Copy corrupt-line details" title="Copy corrupt lines" stopPropagation className="h-5 w-5 text-muted-foreground hover:text-foreground opacity-0 group-hover/corrupt:opacity-100 transition-opacity" />}
            </div>
          )}
        </div>
      )
    } },
    ...(isAnalyst ? [] : ADMIN_ONLY_CRON_COLUMNS),
  ], [isAnalyst])
}
function AuditKVRow({ label, value }: { label: string; value: React.ReactNode }) {
  return <div className="flex justify-between items-center text-xs"><span className="text-muted-foreground">{label}</span><span className="font-mono">{value}</span></div>
}
function AuditGroupDiffCard({ title, colorClass, bgClass, ids, groups }: { title: string; colorClass: string; bgClass: string; ids: string[]; groups: CatalogMaps['groups'] }) {
  return (
    <div className={cn("border rounded-md p-3", bgClass)}>
      <h4 className={cn("text-xs font-semibold mb-2 uppercase tracking-wide", colorClass)}>{title}</h4>
      <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
      <div className="flex flex-col gap-1">
        {ids.map((id) => { const g = groups[id === null ? "null" : String(id)]; return <div key={id} className="text-xs font-mono text-foreground/80 break-words">{g ? g.label : id}</div> })}
        {!ids.length && <div className="text-xs italic text-muted-foreground">None</div>}
      </div>
    </div>
  )
}
export function useAuditColumns(catalogMaps: CatalogMaps): ColumnDef<AuditLogEntry>[] {
  return React.useMemo(() => [
    { accessorKey: 'timestamp', id: 'timestamp', meta: { label: 'Time' }, header: 'Time', cell: ({ row }) => <DateTimeCell iso={row.original.timestamp} /> },
    { accessorKey: 'event_type', id: 'event_type', meta: { label: 'Event Type' }, header: 'Event Type', cell: ({ row }) => {
      const type = row.original.event_type || 'unknown'
      const colorClass = type === 'provision' ? 'bg-green-500/10 text-green-600' : type === 'teardown' ? 'bg-red-500/10 text-red-600' : type === 'fastly_activation' ? 'bg-blue-500/10 text-blue-600' : type.includes('update') ? 'bg-amber-500/10 text-amber-600' : 'bg-slate-500/10 text-slate-600'
      return <Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", colorClass)}>{type.replace(/_/g, ' ')}</Badge>
    } },
    { accessorKey: 'actor', id: 'actor', meta: { label: 'Actor' }, header: 'Actor', cell: ({ row }) => <span className="text-muted-foreground">{row.original.actor}</span> },
    { accessorKey: 'details', id: 'details', meta: { label: 'Details' }, header: 'Details', cell: ({ row }) => {
      const details: AuditDetails = row.original.details
      if (!details || typeof details !== 'object' || Object.keys(details).length === 0) return <span className="text-muted-foreground italic text-[10px]">No details available</span>
      const type = row.original.event_type || 'unknown'
      return (
        <Dialog>
          <DialogTrigger className={cn(buttonVariants({ variant: "ghost", size: "sm" }), "h-6 text-[10px] bg-muted/40 hover:bg-muted/60 text-muted-foreground")}><FileCode className="h-3 w-3 mr-1.5" /> View Details</DialogTrigger>
          <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
            <DialogHeader><DialogTitle className="text-sm font-semibold capitalize flex items-center gap-2"><Settings className="w-4 h-4 text-primary" />{type.replace(/_/g, ' ')} Details</DialogTitle></DialogHeader>
            {type === 'provision' ? (
              <div className="space-y-4 mt-2">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div className="border rounded-md p-3 bg-muted/20">
                    <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5"><Database className="w-3 h-3" /> Storage</h4>
                    <div className="space-y-1.5">
                      <AuditKVRow label="Bucket" value={details.bucket || details.fos_bucket_name || '-'} />
                      <AuditKVRow label="Prefix" value={details.prefix || details.fos_prefix || '(none)'} />
                      <AuditKVRow label="Region" value={details.region || details.fos_region || '-'} />
                    </div>
                  </div>
                  <div className="border rounded-md p-3 bg-muted/20">
                    <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5"><Settings className="w-3 h-3" /> Configuration</h4>
                    <div className="space-y-1.5">
                      <AuditKVRow label="Sample Rate" value={`${details.sample_rate || '-'}${details.sample_rate ? '%' : ''}`} />
                      {details.log_period && <AuditKVRow label="Log Period" value={`${details.log_period}s`} />}
                      {details.edge_only !== undefined && <AuditKVRow label="Edge Only" value={details.edge_only ? 'Yes' : 'No'} />}
                      {details.cdn_url && <div className="flex items-center text-sm"><span className="text-muted-foreground w-32">CDN URL</span><span className="font-mono truncate ml-2 max-w-[200px]" title={details.cdn_url}>{details.cdn_url}</span></div>}
                    </div>
                  </div>
                </div>
                {(details.enable_cron_sync !== undefined || details.log_retention_days !== undefined) && (
                  <div className="border rounded-md p-3 bg-muted/20">
                    <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5"><Clock className="w-3 h-3" /> Automation & Retention</h4>
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-1.5">
                      {details.enable_cron_sync !== undefined && <AuditKVRow label="Cron Sync" value={details.enable_cron_sync ? 'Enabled' : 'Disabled'} />}
                      {details.log_retention_days !== undefined && <AuditKVRow label="Retention" value={`${details.log_retention_days} days`} />}
                      {details.delete_after !== undefined && <AuditKVRow label="Auto Delete" value={details.delete_after ? 'Yes' : 'No'} />}
                    </div>
                  </div>
                )}
                {details.log_fields && (
                  <div className="border rounded-md p-3 bg-muted/20">
                    <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2"><ClipboardList className="w-3 h-3" /> Initial Log Fields</h4>
                    <div className="space-y-4">
                      <div>
                        <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Selected Groups</div>
                        <div className="flex flex-wrap gap-1.5">
                          {details.log_fields.groups?.map((id) => { const g = catalogMaps.groups[id === null ? "null" : String(id)]; return <Badge key={id} variant="outline" className="text-[10px] py-0 font-normal bg-background/50">{g ? g.label : id}</Badge> })}
                          {!details.log_fields.groups?.length && <span className="text-xs text-muted-foreground italic">None</span>}
                        </div>
                      </div>
                      {details.log_fields.field_overrides && Object.keys(details.log_fields.field_overrides).length > 0 && (
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Field Overrides</div>
                          <div className="flex flex-wrap gap-1.5">
                            {Object.entries(details.log_fields.field_overrides).map(([id, enabled]) => { const f = catalogMaps.fields[id]; return <Badge key={id} className={cn("text-[10px] py-0 font-normal border shadow-none", enabled ? "bg-green-500/10 text-green-600 border-green-500/20" : "bg-red-500/10 text-red-600 border-red-500/20")}>{enabled ? '+' : '-'}{f ? f.label : id}</Badge> })}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ) : type === 'logging_settings_update' ? (
              <div className="space-y-4 mt-2">
                <div className="border rounded-md p-3 bg-muted/20">
                  <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2"><Settings className="w-3 h-3" /> Settings Deployed</h4>
                  <div className="flex flex-col gap-2">
                    {Object.entries(details).map(([key, val]) => {
                      if (key === 'log_fields_deployed') return null
                      const { from, to } = val as { from?: unknown; to?: unknown }
                      return <div key={key} className="flex justify-between items-center text-xs"><span className="text-muted-foreground capitalize">{key.replace(/_/g, ' ')}</span><div className="flex items-center gap-2"><span className="text-muted-foreground line-through opacity-70">{String(from)}</span><ChevronRight className="w-3 h-3 text-muted-foreground" /><span className="font-mono">{String(to)}</span></div></div>
                    })}
                    {Object.keys(details).filter(k => k !== 'log_fields_deployed').length === 0 && <span className="text-xs text-muted-foreground italic">No settings changed.</span>}
                  </div>
                </div>
                {details.log_fields_deployed && (
                  <div className="border rounded-md p-3 bg-green-500/10 border-green-500/20">
                    <h4 className="text-[10px] font-semibold text-green-700 uppercase tracking-wide mb-2 flex items-center gap-1.5"><ClipboardList className="w-3 h-3" /> Log Format Updated</h4>
                    <p className="text-xs text-green-700/80">The latest standard and custom field selections have been compiled into VCL and deployed to Fastly.</p>
                  </div>
                )}
              </div>
            ) : type === 'log_format_update' && details.groups_before && details.groups_after ? (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <AuditGroupDiffCard title="Before" colorClass="text-red-600" bgClass="bg-red-500/5" ids={details.groups_before} groups={catalogMaps.groups} />
                  <AuditGroupDiffCard title="After" colorClass="text-green-600" bgClass="bg-green-500/5" ids={details.groups_after} groups={catalogMaps.groups} />
                </div>
                {((details.fields_added?.length ?? 0) > 0 || (details.fields_removed?.length ?? 0) > 0) && (
                  <div className="grid grid-cols-2 gap-4 mt-2">
                    {([
                      { title: 'Fields Removed', icon: X, ids: details.fields_removed, prefix: '-', border: 'border-red-200 dark:border-red-900/30', headerColor: 'text-red-600', itemColor: 'text-red-600/90' },
                      { title: 'Fields Added', icon: Check, ids: details.fields_added, prefix: '+', border: 'border-green-200 dark:border-green-900/30', headerColor: 'text-green-600', itemColor: 'text-green-600/90' },
                    ] as const).map(({ title, icon: Icon, ids, prefix, border, headerColor, itemColor }) => (
                      <div key={title} className={cn("border rounded-md p-3", border)}>
                        <div className={cn("text-[10px] font-medium mb-1 uppercase flex items-center gap-1.5", headerColor)}><Icon className="w-3 h-3" /> {title}</div>
                        <div className="flex flex-col gap-1">
                          {ids?.map((id) => { const f = catalogMaps.fields[id]; return <div key={id} className={cn("text-xs font-mono break-words", itemColor)} title={f?.description}>{prefix} {f ? f.label : id}</div> })}
                          {!ids?.length && <div className="text-xs italic text-muted-foreground">None</div>}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ) : (
              <div className="flex flex-col gap-2 mt-2">
                {Object.entries(details).map(([key, value]) => {
                  if ((key.toLowerCase().includes('prefix') && !value) || value === '' || value == null || (type === 'fastly_activation' && key === 'active')) return null
                  const valString = typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)
                  return (
                    <div key={key} className="flex flex-col border rounded p-3 bg-muted/20">
                      <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-1.5">{key.replace(/_/g, ' ')}</span>
                      {typeof value === 'object' ? <pre className="text-xs font-mono bg-background p-2 rounded overflow-x-auto text-foreground/90 whitespace-pre-wrap">{valString}</pre> : <span className="text-sm font-mono text-foreground/90 break-all">{valString}</span>}
                    </div>
                  )
                })}
              </div>
            )}
          </DialogContent>
        </Dialog>
      )
    } },
  ], [catalogMaps])
}
export function CronLiveLog({ runId, singleLine = false, terminalMode = !singleLine, startedAt, onDone }: {
  runId: number | string | undefined; singleLine?: boolean; terminalMode?: boolean; startedAt?: string; onDone?: () => void
}) {
  const { lines, status, start, stop } = useSSE()
  const startedForRunId = useRef<number | string | undefined>(undefined)
  const doneFiredForRunId = useRef<number | string | undefined>(undefined)
  const { full, abbr } = useDateFormat()
  useEffect(() => {
    if (runId && startedForRunId.current !== runId) { startedForRunId.current = runId; doneFiredForRunId.current = undefined; start(`/api/cron-runs/${runId}/stream`) }
    return () => { if (startedForRunId.current !== undefined) { stop(); startedForRunId.current = undefined } }
  }, [runId, start, stop])
  useEffect(() => {
    if ((status === 'done' || status === 'error') && onDone && doneFiredForRunId.current !== runId) { doneFiredForRunId.current = runId; onDone() }
  }, [status, onDone, runId])
  const recentLines = singleLine ? lines.slice(-1) : terminalMode ? lines : lines.slice(-2)
  const renderLine = (line: typeof lines[number], truncateAt80: boolean) => {
    let text = (line.message as string) || (line.type === 'file_done' ? `Processed ${line.file_name}` : line.type === 'done' ? 'Done.' : JSON.stringify(line))
    if (truncateAt80 && text.length > 80) text = text.substring(0, 80) + '...'
    const cls = line.type === 'error' ? (truncateAt80 ? 'text-red-500' : 'text-red-400 font-medium') : line.type === 'done' ? (truncateAt80 ? 'text-emerald-500' : 'text-emerald-400 font-semibold') : line.type === 'status' ? 'text-blue-400' : line.type === 'file_done' ? (truncateAt80 ? 'text-muted-foreground' : 'text-zinc-400') : undefined
    return { text, node: <span className={cls}>{line.type === 'file_done' ? `Processed ${String(line.file_name)}` : text}</span> }
  }
  if (recentLines.length === 0) {
    if (singleLine) {
      return (
        <div className="flex items-center gap-1.5 text-[9px] font-mono text-muted-foreground truncate h-full w-full">
          {status === 'streaming' ? <><Loader2 className="w-3 h-3 animate-spin shrink-0" /> <span className="truncate">Loading logs...</span></> : status === 'error' ? <span className="text-red-500 truncate">Failed to load logs.</span> : <span className="text-muted-foreground/50 truncate italic">Waiting...</span>}
        </div>
      )
    }
    return (
      <div className="flex flex-col gap-1 w-full font-mono text-xs text-zinc-400 py-1">
        {startedAt && <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">[SESSION INITIATED AT: {full(startedAt)} {abbr()}]</div>}
        <div className="flex items-center gap-2">
          {status === 'streaming' ? <><Loader2 className="w-3.5 h-3.5 animate-spin shrink-0 text-blue-400" /> <span>Streaming console output...</span></> : status === 'error' ? <span className="text-red-400">Failed to stream logs.</span> : <span className="text-zinc-500 italic">Waiting for stream...</span>}
        </div>
      </div>
    )
  }
  if (singleLine) {
    return (
      <div className="flex flex-col text-[9px] font-mono text-muted-foreground truncate w-full h-full justify-center">
        {recentLines.map((line, i) => { const { text, node } = renderLine(line, true); return <div key={line._id ?? `idx-${i}`} className="truncate w-full" title={typeof line.message === 'string' ? line.message : text}>{node}</div> })}
      </div>
    )
  }
  return (
    <div role="log" aria-live="polite" aria-atomic="false" aria-label={terminalMode ? "Cron job output stream" : "Recent cron output"} className={terminalMode ? "flex flex-col gap-1.5 w-full font-mono text-xs leading-relaxed text-zinc-200" : "flex flex-col gap-0.5 mt-1.5 bg-background/50 border border-border/50 p-2 rounded text-[10px] font-mono text-muted-foreground w-[280px] max-w-sm overflow-hidden"}>
      {terminalMode && startedAt && <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">[SESSION INITIATED AT: {full(startedAt)} {abbr()}]</div>}
      {recentLines.map((line, i) => { const { text, node } = renderLine(line, false); return <div key={line._id ?? `idx-${i}`} className={terminalMode ? "whitespace-pre-wrap break-all w-full text-zinc-300" : "truncate w-full"} title={typeof line.message === 'string' ? line.message : text}>{node}</div> })}
    </div>
  )
}
export function FloatingOperationsDock({ displayedJobs, setDisplayedJobs, isOpen, setIsOpen, selectedJobId, setSelectedJobId, onDismiss }: {
  displayedJobs: CronRun[]; setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>; isOpen: boolean; setIsOpen: (open: boolean) => void; selectedJobId: number | string | null; setSelectedJobId: (id: number | string | null) => void; onDismiss: (id: number) => void
}) {
  const { full, abbr } = useDateFormat()
  if (displayedJobs.length === 0) return null
  const activeJob = displayedJobs.find(j => j.id === selectedJobId) || displayedJobs[0]
  const runningCount = displayedJobs.filter(j => j.status === 'running').length
  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2 pointer-events-auto">
      {!isOpen ? (
        <button onClick={() => setIsOpen(true)} aria-expanded={false} aria-controls="floating-ops-panel" aria-label={runningCount > 0 ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running, open console` : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''}, open console`} className={cn("px-4 py-2.5 rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-all hover:scale-105 duration-200 cursor-pointer border", runningCount > 0 ? "bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20 animate-bounce" : "bg-zinc-850 hover:bg-zinc-800 text-zinc-300 border-zinc-700/50")}>
          {runningCount > 0 ? <RefreshCw className="h-3.5 w-3.5 animate-spin text-blue-200" /> : <Database className="h-3.5 w-3.5 text-zinc-400" />}
          <span>{runningCount > 0 ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running...` : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''} (logs)`}</span>
        </button>
      ) : (
        <div id="floating-ops-panel" className="bg-zinc-950 text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl w-[440px] sm:w-[500px] h-[380px] flex flex-col overflow-hidden animate-in slide-in-from-bottom-5 duration-300">
          <div className="flex items-center justify-between px-3 py-2 bg-zinc-900 border-b border-zinc-800 shrink-0">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300"><Database className="h-3.5 w-3.5 text-blue-500" /><span>Console Log Terminal</span></div>
            <button onClick={() => setIsOpen(false)} aria-expanded={true} aria-controls="floating-ops-panel" aria-label="Minimize console" className="text-zinc-400 hover:text-zinc-200 p-1 hover:bg-zinc-800 rounded cursor-pointer transition-colors"><ChevronDown className="h-4 w-4" /></button>
          </div>
          {displayedJobs.length > 1 && (
            <div className="flex border-b border-zinc-800 bg-zinc-900/50 overflow-x-auto scrollbar-none shrink-0 px-2 pt-1 gap-1">
              {displayedJobs.map((job) => {
                const isActive = job.id === selectedJobId, taskLabel = job.task === 'metadata_sync' ? 'sync' : job.task
                return (
                  <div key={job.id} className={cn("rounded-t text-[10px] font-mono uppercase tracking-wider flex items-center gap-1.5 border-t border-x transition-all shrink-0", isActive ? "bg-zinc-950 text-blue-400 border-zinc-800 border-b-zinc-950 font-bold" : "bg-transparent text-zinc-400 border-transparent hover:text-zinc-200 hover:bg-zinc-800/30")}>
                    <button type="button" onClick={() => setSelectedJobId(job.id)} className="pl-3 py-1.5 flex items-center gap-1.5 cursor-pointer uppercase tracking-wider">
                      <span className={cn("w-1.5 h-1.5 rounded-full transition-colors duration-300", job.status === 'running' ? "bg-blue-500 animate-pulse" : "bg-zinc-600")} />{taskLabel}
                    </button>
                    <button type="button" onClick={() => onDismiss(job.id)} className="mr-3 hover:bg-zinc-800 p-0.5 rounded text-zinc-500 hover:text-zinc-300 cursor-pointer" title="Dismiss task" aria-label={`Dismiss ${taskLabel} task`}><X className="h-2.5 w-2.5" /></button>
                  </div>
                )
              })}
            </div>
          )}
          <div className="flex-1 p-3 font-mono bg-zinc-950 overflow-y-auto flex flex-col justify-between">
            <div className="flex-1 overflow-hidden flex flex-col">
              <div className="text-[10px] text-zinc-500 border-b border-zinc-900 pb-1 mb-2 flex items-center justify-between shrink-0">
                <span>STREAM ID: {activeJob?.id}{activeJob?.started_at && ` • STARTED: ${full(activeJob.started_at)} ${abbr()}`}</span>
                {activeJob?.status === 'running' ? <span className="text-emerald-500 font-bold uppercase animate-pulse">● LIVE STREAMING</span> : <span className="text-zinc-500 font-bold uppercase">● COMPLETED</span>}
              </div>
              <div className="flex-1 overflow-y-auto min-h-0 bg-black/30 rounded border border-zinc-900 p-2">
                <CronLiveLog key={activeJob?.id} runId={activeJob?.id} singleLine={false} startedAt={activeJob?.started_at} onDone={() => { if (activeJob?.id) setDisplayedJobs(prev => prev.map(j => j.id === activeJob.id ? { ...j, status: 'completed' } : j)) }} />
              </div>
            </div>
            <div className="mt-2 pt-2 border-t border-zinc-900 flex items-center justify-between text-[10px] text-zinc-500 shrink-0">
              <span>Task: {activeJob?.task}</span>
              <button onClick={() => onDismiss(activeJob?.id)} className="text-red-400 hover:text-red-300 hover:underline cursor-pointer">Dismiss Active View</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
const hasStats = (col: SchemaColumn) => col.null_percentage !== undefined
const populatedPct = (col: SchemaColumn) => col.null_percentage != null ? Math.max(0, 100 - col.null_percentage) : null
const SCHEMA_COLUMNS: ColumnDef<SchemaColumn>[] = [
  { id: 'name', header: 'Column Name', accessorFn: (c) => c.name, size: 220, cell: ({ row }) => <span className="font-mono text-xs font-bold">{row.original.name}</span> },
  { id: 'type', header: 'DuckDB Type', accessorFn: (c) => c.type, size: 160, cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.type}</span> },
  { id: 'populated', header: 'Populated %', accessorFn: (c) => populatedPct(c) ?? -1, size: 130, cell: ({ row }) => {
    const pct = populatedPct(row.original), text = pct != null ? pct.toFixed(1) : '—'
    return <span className="block font-mono text-xs text-right tabular-nums">{hasStats(row.original) ? <span className={text === '0.0' ? 'text-muted-foreground/30' : ''}>{text}%</span> : '—'}</span>
  } },
  { id: 'approx_unique', header: 'Approx Unique', accessorFn: (c) => c.approx_unique ?? -1, size: 140, cell: ({ row }) => <span className="block font-mono text-xs text-right text-muted-foreground tabular-nums">{hasStats(row.original) ? formatCountMaybe(row.original.approx_unique) ?? '—' : '—'}</span> },
  ...(['min', 'max'] as const).map((field): ColumnDef<SchemaColumn> => ({
    id: field, header: field === 'min' ? 'Min Value' : 'Max Value', accessorFn: (c) => c[field] ?? '', size: 200,
    cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground" title={row.original[field] ?? undefined}>{hasStats(row.original) ? row.original[field] ?? '—' : '—'}</span>,
  })),
]
export function SchemaTab({ schemaData, isLoadingSchema }: { schemaData: components['schemas']['SchemaResponse'] | undefined; isLoadingSchema: boolean }) {
  return (
    <>
      <div className="p-4 border-b flex justify-between items-center"><div><h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3><p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p></div></div>
      <DataTableReadonly columns={SCHEMA_COLUMNS} data={schemaData?.schema ?? []} isLoading={isLoadingSchema} hideToolbar showPagination={false} tableCaption="DuckDB Table Schema and Statistics" />
    </>
  )
}
export function IngestionTab({ ingestedColumns, ingestedFiles, isLoadingIngested }: { ingestedColumns: ColumnDef<IngestedFile>[]; ingestedFiles: components['schemas']['IngestedFilesResponse'] | undefined; isLoadingIngested: boolean }) {
  return (
    <DataTableReadonly
      columns={ingestedColumns} data={ingestedFiles?.files || []} isLoading={isLoadingIngested} searchKey="file_name" initialSorting={[{ id: 'ingested_at', desc: true }]}
      renderToolbar={(table) => (
        <div className="p-4 border-b flex flex-wrap items-center justify-between gap-4">
          <h3 className="text-sm font-medium">Log Ingestion History</h3>
          <div className="flex items-center gap-2 ml-auto">
            <Input placeholder="Filter by filename..." className="max-w-sm h-8" value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''} onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)} />
            <ColumnVisibilityDropdown table={table} compact />
          </div>
        </div>
      )}
    />
  )
}
export function ServiceHistoryTab({ auditColumns, auditLogs, isLoadingAudit, isFetchingAudit, eventFilter, setEventFilter, activeServiceId }: {
  auditColumns: ColumnDef<AuditLogEntry>[]; auditLogs: components['schemas']['AuditLogsResponse'] | undefined; isLoadingAudit: boolean; isFetchingAudit: boolean; eventFilter: string; setEventFilter: (v: string) => void; activeServiceId: string | null | undefined
}) {
  const queryClient = useQueryClient()
  return (
    <div className="p-0">
      <DataTableReadonly
        columns={auditColumns} data={auditLogs?.entries || []} isLoading={isLoadingAudit} initialSorting={[{ id: 'timestamp', desc: true }]}
        renderToolbar={(table) => (
          <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
            <div className="flex items-center gap-4">
              <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
              <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                <SelectTrigger className="h-8 w-[200px] text-xs"><SelectValue placeholder="All events" /></SelectTrigger>
                <SelectContent>
                  {[['all', 'All events'], ['provision', 'Provision'], ['fastly_activation', 'Fastly Activation'], ['cron_settings_update', 'Cron Settings Update'], ['logging_settings_update', 'Log Settings Updated'], ['log_format_update', 'Log Format Update']].map(([val, label]) => <SelectItem key={val} value={val}>{label}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown table={table} />
              <Button variant="outline" size="sm" className="h-8 text-xs" disabled={isFetchingAudit} onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(activeServiceId) })}><RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />Refresh</Button>
            </div>
          </div>
        )}
      />
    </div>
  )
}
export function QuickActionsBar({ isAnalyst, status, activeServiceId, recentCrons, cronLogs, setSseTitle, setSseDescription, setIsSSEModalOpen, setIsSyncModalOpen, reset, start, setDisplayedJobs, setSelectedConsoleJobId, setConsoleOpen }: {
  isAnalyst: boolean; status: Partial<SyncStatus> | undefined; activeServiceId: string | null | undefined; recentCrons: Partial<CronRunsResponse> | undefined; cronLogs: Partial<CronRunsResponse> | undefined
  setSseTitle: (s: string) => void; setSseDescription: (s: string) => void; setIsSSEModalOpen: (open: boolean) => void; setIsSyncModalOpen: (open: boolean) => void; reset: () => void; start: ReturnType<typeof useSSE>['start']
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>; setSelectedConsoleJobId: (id: number | string | null) => void; setConsoleOpen: (open: boolean) => void
}) {
  const queryClient = useQueryClient()
  const isReadOnly = status?.access_level === 'read_only'
  const triggerCronAction = async (endpoint: '/api/admin/ingest-logs' | '/api/admin/commit-iceberg', title: string, description: string) => {
    try {
      const data = (await unwrapClient(client.POST(endpoint, {}))) as { run_id?: number | string } | undefined
      if (!data?.run_id) { showToast('No run id returned'); return }
      setSseTitle(title); setSseDescription(description); setIsSSEModalOpen(true); reset(); start(`/api/cron-runs/${data.run_id}/stream`); cronCacheBust(queryClient, activeServiceId)
    } catch (e) { showToast(extractApiError(e)) }
  }
  return (
    <div className="flex flex-wrap items-center gap-2 bg-muted/30 p-2 rounded-lg border">
      <div className="text-xs font-bold text-muted-foreground uppercase tracking-wider mx-2">Quick Actions</div>
      {!isAnalyst ? (
        <>
          <Button size="sm" variant="default" className="h-8 text-xs bg-primary text-primary-foreground hover:bg-primary/90" disabled={isReadOnly} onClick={() => triggerCronAction('/api/admin/ingest-logs', 'Importing Logs', 'Downloading new raw logs from Fastly Object Storage and processing them...')}><RefreshCw className="h-3 w-3 mr-1.5" /> Import Logs</Button>
          <Button size="sm" variant="outline" className="h-8 text-xs bg-background" disabled={isReadOnly} onClick={() => setIsSyncModalOpen(true)} title="Open the range-picker to backfill a specific historical window (e.g., to close an Ingest Gap)"><Download className="h-3 w-3 mr-1.5" /> Sync from Cloud</Button>
          <Button size="sm" variant="outline" className="h-8 text-xs bg-background" disabled={isReadOnly} onClick={() => triggerCronAction('/api/admin/commit-iceberg', 'Committing Buffer', 'Flushing local Parquet buffer to the shared Iceberg table in Object Storage...')}><Archive className="h-3 w-3 mr-1.5" /> Commit Buffer</Button>
        </>
      ) : (
        <Button size="sm" variant="default" className="h-8 text-xs bg-primary/90 hover:bg-primary" onClick={() => setIsSyncModalOpen(true)}><Download className="h-3 w-3 mr-1.5" /> Sync from Cloud</Button>
      )}
      {!isAnalyst && status?.ngwaf_workspace_id && (
        <Button size="sm" variant="outline" className="h-8 text-xs bg-background" onClick={() => {
          setSseTitle('NGWAF Bot Sync'); setSseDescription('Fetching verified bot records from Fastly NGWAF and caching them locally. Progress is saved after each page — run again if the time budget is reached.')
          setIsSSEModalOpen(true); reset(); start(`/api/services/${activeServiceId}/ngwaf-sync`, {}); queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })
        }}><Bot className="h-3 w-3 mr-1.5" /> NGWAF Bot Sync</Button>
      )}
      <Button size="sm" variant="outline" className="h-8 text-xs bg-background" onClick={() => {
        const latestSync = recentCrons?.entries?.find((e) => e.task === 'log_discovery') || cronLogs?.entries?.find((e) => e.task === 'log_discovery')
        if (latestSync) { setDisplayedJobs(prev => prev.some((j) => j.id === latestSync.id) ? prev : [...prev, { ...latestSync, status: latestSync.status }]); setSelectedConsoleJobId(latestSync.id); setConsoleOpen(true) }
        else window.alert("No recent sync run was found for this service.")
      }}><Terminal className="h-3 w-3 mr-1.5" /> View Recent Logs</Button>
    </div>
  )
}
interface TreeProps { prefix: string; level: number; type: 'iceberg' | 'raw'; forceExpand?: boolean }
function Node({ node, prefix, level, type, forceExpand }: TreeProps & { node: TreeNode }) {
  const [isOpen, setIsOpen] = useState(false)
  const { full, abbr, relative } = useDateFormat()
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const [seenForceExpand, setSeenForceExpand] = useState<boolean | undefined>(undefined)
  if (forceExpand !== seenForceExpand) { setSeenForceExpand(forceExpand); if (forceExpand !== undefined) setIsOpen(forceExpand) }
  if (node.type === 'directory') {
    return (
      <div>
        <div className={cn("flex items-center justify-between gap-2 py-1.5 hover:bg-muted/50 rounded-md transition-colors group", level === 0 ? "font-medium" : "text-sm text-muted-foreground")} style={{ paddingLeft: `${level * 16 + 8}px`, paddingRight: '8px' }}>
          <button type="button" aria-expanded={isOpen} onClick={() => setIsOpen(!isOpen)} className="flex items-center gap-2 min-w-0 flex-1 bg-transparent border-0 p-0 text-left cursor-pointer">
            {isOpen ? <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />}
            <Folder className="h-4 w-4 shrink-0 text-blue-500/70 dark:text-blue-400/70" />
            <span className="truncate group-hover:text-foreground transition-colors">{node.name}</span>
          </button>
          <div className="flex items-center gap-4 shrink-0">
            {node.size != null && node.size > 0 && <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">{formatBytes(node.size)}</span>}
            <div className="w-20 flex items-center justify-end gap-2">
              <Button variant="ghost" size="icon" aria-label="Download folder as ZIP" title="Download folder as ZIP" className="h-6 w-6 text-muted-foreground transition-opacity" onClick={(e) => { e.stopPropagation(); window.open(`/api/download-folder?service=${activeServiceId}&root=${type}&prefix=${encodeURIComponent(node.prefix || (prefix + node.name))}`, '_blank') }}><Download className="h-3.5 w-3.5" /></Button>
            </div>
          </div>
        </div>
        {isOpen && <Tree prefix={`${prefix}${node.name}/`} level={level + 1} type={type} forceExpand={forceExpand} />}
      </div>
    )
  }
  return (
    <div className="flex items-center justify-between gap-4 py-1 pr-2 hover:bg-muted/50 group rounded-md transition-colors" style={{ paddingLeft: `${level * 16 + 32}px` }}>
      <div className="flex items-center gap-2 min-w-0 flex-1"><FileJson className="h-4 w-4 shrink-0 text-muted-foreground opacity-70" /><span className="text-sm font-mono truncate text-muted-foreground group-hover:text-foreground transition-colors">{node.name}</span></div>
      <div className="flex items-center gap-4 shrink-0">
        {node.size != null && <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">{formatBytes(node.size)}</span>}
        {node.mtime && (
          <TooltipProvider><Tooltip>
            <TooltipTrigger render={<span tabIndex={0} role="button" aria-label="Show full modified timestamp" className="hidden sm:inline-block text-xs text-muted-foreground w-[120px] text-right truncate rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">{relative(node.mtime)}</span>} />
            <TooltipContent className="text-xs">{full(node.mtime)} {abbr()}</TooltipContent>
          </Tooltip></TooltipProvider>
        )}
        <div className="w-20 flex items-center justify-end gap-2">
          {node.sync_status === 'synced' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-green-500/10 text-green-600 border-green-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Synced</Badge>}
          {node.sync_status === 'local' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 shadow-none"><HardDrive className="w-3 h-3 mr-1"/> Local</Badge>}
          {(node.sync_status === 'cloud' || (!node.sync_status && node.is_cloud)) && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Cloud</Badge>}
          {node.key && <Button variant="ghost" size="icon" aria-label={`Download ${node.name}`} title="Download file" className="h-6 w-6 text-muted-foreground transition-opacity" onClick={(e) => { e.stopPropagation(); window.open(`/api/download?service=${activeServiceId}&key=${encodeURIComponent(node.key as string)}`, '_blank') }}><Download className="h-3.5 w-3.5" /></Button>}
        </div>
      </div>
    </div>
  )
}
function Tree({ prefix, level, type, forceExpand }: TreeProps) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.fileTree(type, activeServiceId, prefix),
    queryFn: () => { const init = { params: { query: { prefix: prefix || undefined } } }; return type === 'iceberg' ? unwrapClient(client.GET("/api/admin/iceberg-tree", init)) : unwrapClient(client.GET("/api/admin/raw-tree", init)) },
    enabled: !!activeServiceId,
  })
  if (isLoading) return <div className="flex items-center gap-2 py-2 text-sm text-muted-foreground animate-pulse" style={{ paddingLeft: `${level * 16 + 28}px` }}><Loader2 className="h-3 w-3 animate-spin" /> Loading...</div>
  if (error || !data) return <div className="py-2 text-sm text-destructive" style={{ paddingLeft: `${level * 16 + 28}px` }}>Failed to load directory.</div>
  if (data.nodes.length === 0) return <div className="py-2 text-xs text-muted-foreground italic" style={{ paddingLeft: `${level * 16 + 32}px` }}>Empty directory</div>
  return <div className="flex flex-col">{data.nodes.map((node: TreeNode) => <Node key={node.name} node={node} prefix={prefix} level={level} type={type} forceExpand={forceExpand} />)}</div>
}
export function FileBrowser({ type }: { type: 'iceberg' | 'raw' }) {
  const [forceExpand, setForceExpand] = useState<boolean | undefined>(undefined)
  const queryClient = useQueryClient()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const handleRefresh = async () => { setIsRefreshing(true); await queryClient.invalidateQueries({ queryKey: queryKeys.admin.fileTreeFor(type, activeServiceId) }); setTimeout(() => setIsRefreshing(false), 500) }
  return (
    <div className="flex flex-col font-sans h-full relative">
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/5 sticky top-0 z-10 backdrop-blur-sm">
        <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">{type === 'iceberg' ? 'Iceberg Data Lake' : 'Raw Archives'}</span>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="h-7 w-7 p-0 font-semibold shadow-none border-muted/60 bg-background" onClick={handleRefresh} title="Refresh file list"><RefreshCw className={cn("h-3.5 w-3.5", isRefreshing && "animate-spin")} /></Button>
          <Button variant="outline" size="sm" className="h-7 text-[10px] px-2 font-semibold shadow-none border-muted/60 bg-background" onClick={() => setForceExpand(prev => prev === undefined ? true : !prev)}>
            {forceExpand ? <Minimize2 className="h-3 w-3 mr-1" /> : <Maximize2 className="h-3 w-3 mr-1" />}{forceExpand ? 'Collapse All' : 'Expand All'}
          </Button>
        </div>
      </div>
      <div className="p-2 overflow-x-auto min-w-[500px]"><Tree prefix="" level={0} type={type} forceExpand={forceExpand} /></div>
    </div>
  )
}
const IcebergStatus = dynamic(() => import('@/components/SessionScoring/StatusPanel').then(m => m.IcebergStatus), { ssr: false })
const IcebergCalendar = dynamic(() => import('@/components/SessionScoring/StatusPanel').then(m => m.IcebergCalendar), { ssr: false })
const MetadataStorageCard = dynamic(() => import('@/components/SessionScoring/StatusPanel').then(m => m.MetadataStorageCard), { ssr: false })
const LOG_TABS = [
  { value: 'cron', label: 'Cron Runs', icon: History, adminOnly: false },
  { value: 'service_history', label: 'Service History', icon: ClipboardList, adminOnly: false },
  { value: 'ingestion', label: 'Ingestion History', icon: Database, adminOnly: true },
  { value: 'iceberg', label: 'DuckLake Storage', icon: Archive, adminOnly: false },
  { value: 'metadata_storage', label: 'Metadata Storage', icon: HardDrive, adminOnly: true },
  { value: 'raw', label: 'Available Logs', icon: FileCode, adminOnly: true },
  { value: 'schema', label: 'DuckDB Schema', icon: FileCode, adminOnly: false },
] as const
export default function LogsClient({ initialTab }: { initialTab?: string } = {}) {
  const s = useLogsPageState(initialTab)
  const queryClient = useQueryClient()
  const bootstrapResolved = useBootstrapResolved()
  const auditColumns = useAuditColumns(s.catalogMaps)
  const cronColumns = useCronColumns(s.isAnalyst)
  if (!s.activeServiceId && bootstrapResolved) return <NoServiceSelected icon={Database} message="Please select a service from the header to access admin controls." />
  if (!s.activeServiceId) return <Loading />
  return (
    <div className="space-y-6">
      <PageHeader title="Data Management" description="Monitor and manage log ingestion history and active data syncs." />
      <QuickActionsBar isAnalyst={s.isAnalyst} status={s.status} activeServiceId={s.activeServiceId} recentCrons={s.recentCrons} cronLogs={s.cronLogs} setSseTitle={s.setSseTitle} setSseDescription={s.setSseDescription} setIsSSEModalOpen={s.setIsSSEModalOpen} setIsSyncModalOpen={s.setIsSyncModalOpen} reset={s.reset} start={s.start} setDisplayedJobs={s.setDisplayedJobs} setSelectedConsoleJobId={s.setSelectedConsoleJobId} setConsoleOpen={s.setConsoleOpen} />
      <Tabs value={s.activeTab} onValueChange={s.handleTabChange} className="w-full">
        <ScrollArea className="w-full max-w-full overflow-hidden">
          <TabsList className="w-full flex">{LOG_TABS.filter(t => !s.isAnalyst || !t.adminOnly).map(({ value, label, icon: Icon }) => <TabsTrigger key={value} value={value} className="flex-1 flex items-center justify-center gap-2 text-xs"><Icon className="h-4 w-4" /> {label}</TabsTrigger>)}</TabsList>
        </ScrollArea>
        <TabsContent value="cron" className="mt-4 border rounded-lg overflow-hidden bg-card"><CronTab cronColumns={cronColumns} cronLogs={s.cronLogs} isLoadingCron={s.isLoadingCron} isFetchingCron={s.isFetchingCron} orderedSchedules={s.orderedSchedules} isLoadingSchedule={s.isLoadingSchedule} taskFilter={s.taskFilter} setTaskFilter={s.setTaskFilter} statusFilter={s.statusFilter} setStatusFilter={s.setStatusFilter} isAnalyst={s.isAnalyst} status={s.status} activeServiceId={s.activeServiceId} setDisplayedJobs={s.setDisplayedJobs} setSelectedConsoleJobId={s.setSelectedConsoleJobId} setConsoleOpen={s.setConsoleOpen} isPurgeOpen={s.isPurgeOpen} setIsPurgeOpen={s.setIsPurgeOpen} purgeMutation={s.purgeMutation} /></TabsContent>
        <TabsContent value="service_history" className="mt-4 border rounded-lg overflow-hidden bg-card"><ServiceHistoryTab auditColumns={auditColumns} auditLogs={s.auditLogs} isLoadingAudit={s.isLoadingAudit} isFetchingAudit={s.isFetchingAudit} eventFilter={s.eventFilter} setEventFilter={s.setEventFilter} activeServiceId={s.activeServiceId} /></TabsContent>
        <TabsContent value="ingestion" className="mt-4 border rounded-lg overflow-hidden bg-card"><IngestionTab ingestedColumns={ingestedFilesColumns} ingestedFiles={s.ingestedFiles} isLoadingIngested={s.isLoadingIngested} /></TabsContent>
        <TabsContent value="iceberg" className="mt-4 space-y-4">
          {s.activeTab === 'iceberg' && <><IcebergStatus accessLevel={s.status?.access_level ?? undefined} /><IcebergCalendar /><div className="border rounded-lg overflow-hidden bg-card"><div className="p-4 border-b"><h3 className="text-sm font-medium">Iceberg Data Lake Layout</h3><p className="text-xs text-muted-foreground mt-1">Direct view into the Iceberg metadata and data files in Object Storage.</p></div><FileBrowser type="iceberg" /></div></>}
        </TabsContent>
        <TabsContent value="metadata_storage" className="mt-4 space-y-4">{s.activeTab === 'metadata_storage' && <MetadataStorageCard />}</TabsContent>
        <TabsContent value="raw" className="mt-4 border rounded-lg overflow-hidden bg-card">{s.activeTab === 'raw' && <><div className="p-4 border-b"><h3 className="text-sm font-medium">Available Logs</h3><p className="text-xs text-muted-foreground mt-1">Raw .gz files delivered by Fastly waiting to be processed.</p></div><FileBrowser type="raw" /></>}</TabsContent>
        <TabsContent value="schema" className="mt-4 border rounded-lg overflow-hidden bg-card">{s.activeTab === 'schema' && <SchemaTab schemaData={s.schemaData} isLoadingSchema={s.isLoadingSchema} />}</TabsContent>
      </Tabs>
      <SyncFromCloudModal open={s.isSyncModalOpen} onOpenChange={s.setIsSyncModalOpen} onStartSync={async (range) => {
        try {
          const data = await unwrapClient(client.POST("/api/admin/ingest-logs", { params: { query: range ? { start_time: range.start, end_time: range.end } : {} } }))
          s.setSseTitle('Syncing from Cloud'); s.setSseDescription('Fetching latest snapshots and downloading new data files from the cloud...'); s.setIsSSEModalOpen(true); s.reset(); s.start(`/api/cron-runs/${data?.run_id}/stream`); cronCacheBust(queryClient, s.activeServiceId)
        } catch (e) { console.error(e) }
      }} />
      <SSEModal isSSEModalOpen={s.isSSEModalOpen} setIsSSEModalOpen={s.setIsSSEModalOpen} sseStatus={s.sseStatus} sseTitle={s.sseTitle} sseError={s.sseError} sseDescription={s.sseDescription} lines={s.lines} stop={s.stop} />
      <FloatingOperationsDock displayedJobs={s.displayedJobs} setDisplayedJobs={s.setDisplayedJobs} isOpen={s.consoleOpen} setIsOpen={s.setConsoleOpen} selectedJobId={s.selectedConsoleJobId} setSelectedJobId={s.setSelectedConsoleJobId} onDismiss={s.removeDisplayedJob} />
    </div>
  )
}
