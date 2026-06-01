'use client'

import React, { useState, useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { Button, buttonVariants } from "@/components/ui/button"
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Table,  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table"
import { Skeleton } from '@/components/ui/skeleton'
import { 
  Database, 
  RefreshCw, 
  History, 
  FileCode, 
  Archive,
  CheckCircle2,
  Trash2,
  Loader2,
  ArrowUpDown,
  Download,
  Copy,
  Check,
  ChevronDown,
  ChevronRight,
  ClipboardList,
  Clock,
  X,
  Bot,
  Settings,
  Terminal,
} from 'lucide-react'
import { FileBrowser } from '@/components/FileBrowser/FileBrowser'
import { IcebergStatus } from '@/components/IcebergStatus/IcebergStatus'
import { IcebergCalendar } from '@/components/IcebergStatus/IcebergCalendar'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { CronLiveLog } from '@/components/CronLiveLog'
import { formatBytes } from '@/lib/utils'
import { formatCompactDuration, toUTCDate } from '@/lib/date'
import { ScrollArea } from '@/components/ui/scroll-area'

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { ConfirmDialog } from "@/components/ui/confirm-dialog"
import { SyncFromCloudModal } from '@/components/SyncFromCloudModal/SyncFromCloudModal'

import { useDateFormat } from '@/hooks/useDateFormat'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useElapsedTime } from '@/hooks/useElapsedTime'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogTrigger,
} from '@/components/ui/dialog'

import { DataTable, DateTimeCell } from '@/components/DataTable'
import { ingestedFilesColumns } from '@/lib/table-columns'
import { Input } from '@/components/ui/input'
import { ColumnDef } from '@tanstack/react-table'

import { cn } from '@/lib/utils'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { PageHeader } from '@/components/ui/page-header'

function LiveTimer({ startedAt }: { startedAt: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className="font-mono text-blue-500 tabular-nums text-xs font-medium animate-pulse">{fmt}</span>
}

const CRON_EXPLANATIONS: Record<string, string> = {
  sync: 'Downloads raw logs from Fastly Object Storage, parses them, and saves them to a local Parquet buffer.',
  full_sync: 'Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync’s 4h lookback window.',
  gap_heal: 'Reconciles Fastly’s authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (≥2 consecutive hourly buckets ≥5% gap), triggers a full_sweep — throttled to one heal per 4h.',
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  commit: 'Aggregates local buffer files and commits them as a single snapshot to the remote Iceberg table.',
  optimize: 'Compacts small Iceberg data files into larger ones (writes back to FOS — incurs 30-day-minimum cost on rewritten files).',
  local_compact: 'Merges small parquet files in the LOCAL CACHE every 10 min. Free vs FOS — speeds up dashboard scans without touching the cloud manifest.',
  expire: 'Removes old snapshots and orphaned files to reclaim storage.',
  metadata_sync: 'Downloads the latest Iceberg metadata to sync with the remote data source.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
}

function CronJobBox({ job, onRemove }: { job: any, onRemove: (id: number) => void }) {
  const [isDone, setIsDone] = useState(false)
  const [fading, setFading] = useState(false)

  useEffect(() => {
    if (!isDone) return
    const fadeTimer = setTimeout(() => setFading(true), 2000)
    const removeTimer = setTimeout(() => onRemove(job.id), 2600) // 2s delay + 600ms fade
    return () => { clearTimeout(fadeTimer); clearTimeout(removeTimer) }
  }, [isDone, job.id, onRemove])

  return (
    <div
      className={[
        'relative flex items-center gap-2 border rounded-md px-2.5 h-8 shrink-0 min-w-[220px] max-w-[280px]',
        fading
          ? 'opacity-0 transition-opacity duration-500 bg-muted/20 border-muted'
          : isDone
            ? 'bg-muted/20 border-muted'
            : 'bg-muted/30 border-blue-500/20',
      ].join(' ')}
    >
      {!isDone && !fading && (
        <div className="absolute inset-0 rounded-md border border-blue-500/60 animate-pulse pointer-events-none" />
      )}
      <TooltipProvider delay={200}>
        <Tooltip>
          <TooltipTrigger render={<span className="text-[9px] font-bold uppercase text-blue-500 tracking-wider shrink-0" />}>
            {job.task === 'metadata_sync' ? 'sync' : job.task}
          </TooltipTrigger>
          <TooltipContent side="top" className="max-w-[250px] text-xs">
            {CRON_EXPLANATIONS[job.task] || 'Background job.'}
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      <div className="w-px h-4 bg-border shrink-0" />
      <div className="flex-1 overflow-hidden min-w-0">
        <CronLiveLog runId={job.id} singleLine={true} onDone={() => setIsDone(true)} />
      </div>
    </div>
  )
}

function CronScheduleBox({ 
  schedule, 
  compact = false, 
  activeJob = null, 
  onOpenConsole 
}: { 
  schedule: any; 
  compact?: boolean; 
  activeJob?: any; 
  onOpenConsole?: (jobId: number | string) => void 
}) {
  const { relative, timeAgo, full, abbr } = useDateFormat()
  const [nextRunText, setNextRunText] = useState('Disabled')

  useEffect(() => {
    function compute() {
      if (!schedule.next_run_time) { setNextRunText('Disabled'); return }
      const d = toUTCDate(schedule.next_run_time)
      const secs = Math.floor((d.getTime() - Date.now()) / 1000)
      setNextRunText(formatCompactDuration(secs))
    }
    compute()
    const id = setInterval(compute, 1000)
    return () => clearInterval(id)
  }, [schedule.next_run_time])

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

  const lastRunText = schedule.last_run_time ? timeAgo(schedule.last_run_time) : 'Never'
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
              <span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 flex items-center gap-1 ${isRunning ? 'text-blue-500' : 'text-muted-foreground'}`} />
            }>
              {isRunning && <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-500" />}
              {schedule.task === 'metadata_sync' ? 'sync' : schedule.task}
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
            className="flex-1 min-w-0 text-left text-[9px] text-blue-500 hover:text-blue-600 hover:underline font-medium flex items-center justify-between cursor-pointer truncate"
          >
            <span className="truncate">Running...</span>
            <span className="text-[8px] bg-blue-500/20 px-1 py-0.2 rounded border border-blue-500/20 shrink-0 ml-1">LOGS</span>
          </button>
        ) : (
          <div className="flex-1 min-w-0 flex items-center justify-between text-[9px] text-muted-foreground whitespace-nowrap overflow-hidden">
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate pr-2" />}>
                  Last: {lastRunText}
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate" />}>
                  Next: {nextRunText}
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

export default function LogsPage() {
  const { activeServiceId, services } = useServiceStore()
  const activeService = services.find(s => s.id === activeServiceId)
  const isAnalyst = activeService?.accessLevel === 'read_only'
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState('cron')
  const [isPurgeOpen, setIsPurgeOpen] = useState(false)
  const [taskFilter, setTaskFilter] = useState('all')
  const [statusFilter, setStatusFilter] = useState('all')
  const [eventFilter, setEventFilter] = useState('all')
  const { relative, timeAgo, full, abbr } = useDateFormat()

  const { lines, status: sseStatus, error: sseError, start, stop, reset } = useSSE()
  const [isSSEModalOpen, setIsSSEModalOpen] = useState(false)
  const [isSyncModalOpen, setIsSyncModalOpen] = useState(false)
  const [sseTitle, setSseTitle] = useState('')
  const [sseDescription, setSseDescription] = useState('')
  const [consoleOpen, setConsoleOpen] = useState(false)
  const [selectedConsoleJobId, setSelectedConsoleJobId] = useState<number | string | null>(null)

  // Background cron toast notification state
  const [backgroundCronToast, setBackgroundCronToast] = useState<{
    id: number
    task: string
    status: string
    started_at: string
    duration_s?: number
    rows_ingested?: number
  } | null>(null)

  // Multi-tenant safe run ID tracker to prevent alerting old runs or cross-tenant leaks
  const maxSeenIdRef = React.useRef<number | null>(null)

  // Reset tracker when switching active services
  useEffect(() => {
    maxSeenIdRef.current = null
    setBackgroundCronToast(null)
  }, [activeServiceId])
  
  const { setHasSyncedExtents } = useFilterStore()

  const { data: status, isLoading: isLoadingStatus } = useQuery({
    queryKey: ['admin', 'status', activeServiceId],
    queryFn: async () => {
      const { data, error } = await client.GET("/api/sync-status", {
        params: { query: { skip_fos: true } },
      })
      if (error) throw error
      return data
    },
    enabled: !!activeServiceId,
    refetchInterval: 30000,
    staleTime: 0
  })

  const { data: cronLogs, isLoading: isLoadingCron, isFetching: isFetchingCron } = useQuery({
    queryKey: ['admin', 'cron-logs', activeServiceId, taskFilter, statusFilter],
    queryFn: async () => {
      const { data } = await client.GET("/api/cron-runs", {
        params: {
          query: {
            page: 1,
            per_page: 500,
            task: taskFilter === 'all' ? undefined : taskFilter as any,
            status: statusFilter === 'all' ? undefined : statusFilter as any
          }
        }
      })
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'cron',
    refetchInterval: 5000,
    staleTime: 0
  })

  // Separate query specifically for checking recent crons (including running) without reloading the entire 500-row table
  const { data: recentCrons, isFetching: isFetchingRecent } = useQuery({
    queryKey: ['admin', 'cron-logs-recent', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/cron-runs", {
        params: {
          query: {
            page: 1,
            per_page: 10,
          }
        }
      })
      return data as any
    },
    enabled: !!activeServiceId, // Tab independent polling!
    refetchInterval: 5000,
    staleTime: 0
  })

  // Derive currently running crons and loading state from recent crons to keep downstream compatibility intact
  const runningCrons = React.useMemo(() => {
    if (!recentCrons?.entries) return { entries: [] }
    return {
      entries: recentCrons.entries.filter((e: any) => e.status === 'running')
    }
  }, [recentCrons])

  const isFetchingRunning = isFetchingRecent

  // When a running cron completes, refresh the main table so it shows up in the history
  const prevRunningCount = React.useRef(0)
  React.useEffect(() => {
    const count = runningCrons?.entries?.length || 0
    if (prevRunningCount.current > 0 && count < prevRunningCount.current) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs'] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'status'] })
    }
    prevRunningCount.current = count
  }, [runningCrons?.entries?.length, queryClient])

  // Accumulate running and completed jobs, pruning older runs of the same task type
  // to prevent historical clutter. We only keep the active running job and the single
  // most recent completed job (last run) for each task.
  const [displayedJobs, setDisplayedJobs] = useState<any[]>([])
  useEffect(() => {
    if (!runningCrons?.entries) return
    setDisplayedJobs(prev => {
      // 1. Identify which tasks are currently running in the poll response
      const runningTasks = new Set(runningCrons.entries.map((j: any) => j.task))
      
      // 2. Filter out completed jobs of the tasks that are now running a new instance
      const filtered = prev.filter((j: any) => {
        const isRunningNow = runningCrons.entries.some((rc: any) => rc.id === j.id)
        return isRunningNow || !runningTasks.has(j.task)
      })

      // 3. Keep only the single most recent completed job per task to prevent history clutter
      const jobsByTask: Record<string, any[]> = {}
      filtered.forEach(j => {
        if (!jobsByTask[j.task]) jobsByTask[j.task] = []
        jobsByTask[j.task].push(j)
      })

      const pruned: any[] = []
      Object.keys(jobsByTask).forEach(task => {
        const taskJobs = jobsByTask[task]
        const running = taskJobs.filter(j => runningCrons.entries.some((rc: any) => rc.id === j.id))
        const completed = taskJobs.filter(j => !runningCrons.entries.some((rc: any) => rc.id === j.id))
        
        if (running.length > 0) {
          pruned.push(...running.map(j => ({ ...j, status: 'running' })))
          if (completed.length > 0) {
            const latestCompleted = completed.reduce((max, job) => job.id > max.id ? job : max, completed[0])
            pruned.push({ ...latestCompleted, status: 'completed' })
          }
        } else if (completed.length > 0) {
          const latestCompleted = completed.reduce((max, job) => job.id > max.id ? job : max, completed[0])
          pruned.push({ ...latestCompleted, status: 'completed' })
        }
      })

      // 4. Merge in brand new running jobs
      const prunedIds = new Set(pruned.map(j => j.id))
      const brandNew = runningCrons.entries
        .filter((j: any) => !prunedIds.has(j.id))
        .map((j: any) => ({ ...j, status: 'running' }))
      
      return [...pruned, ...brandNew]
    })
  }, [runningCrons?.entries])

  const removeDisplayedJob = useCallback((id: number) => {
    setDisplayedJobs(prev => prev.filter((j: any) => j.id !== id))
  }, [])

  // Effect to monitor recent crons and detect newly started or completed runs (even if they ran very fast)
  useEffect(() => {
    if (!recentCrons?.entries || recentCrons.entries.length === 0) return
    const ids = recentCrons.entries.map((e: any) => e.id)
    const maxId = Math.max(...ids)

    if (maxSeenIdRef.current === null) {
      // First load: initialize max seen ID so we don't alert on historical runs
      maxSeenIdRef.current = maxId
      
      // Eagerly capture any running crons at load time and display them as running with live streams
      const runningRuns = recentCrons.entries.filter((e: any) => e.status === 'running')
      runningRuns.forEach((run: any) => {
        setDisplayedJobs(prev => {
          if (prev.some((j: any) => j.id === run.id)) return prev
          return [...prev, { ...run, status: run.status }]
        })
        setBackgroundCronToast({
          id: run.id,
          task: run.task,
          status: run.status,
          started_at: run.started_at,
          duration_s: run.duration_s,
          rows_ingested: run.rows_ingested
        })
      })
      return
    }

    // On subsequent polls, check if we have any brand new runs!
    if (maxId > maxSeenIdRef.current) {
      const newRuns = recentCrons.entries.filter((e: any) => e.id > (maxSeenIdRef.current || 0))
      
      // Update max seen ID
      maxSeenIdRef.current = maxId

      // Processes new runs and queue notifications/console placement
      newRuns.forEach((run: any) => {
        // Automatically add it to displayedJobs so it appears in the Console Terminal dock
        setDisplayedJobs(prev => {
          if (prev.some((j: any) => j.id === run.id)) return prev
          return [...prev, { ...run, status: run.status }]
        })

        // Pop up the premium floating notification toast!
        setBackgroundCronToast({
          id: run.id,
          task: run.task,
          status: run.status,
          started_at: run.started_at,
          duration_s: run.duration_s,
          rows_ingested: run.rows_ingested
        })
      })
    }
  }, [recentCrons?.entries])

  // Effect to update an active running toast when that specific run completes
  useEffect(() => {
    if (!backgroundCronToast || backgroundCronToast.status !== 'running' || !recentCrons?.entries) return
    const updatedRun = recentCrons.entries.find((e: any) => e.id === backgroundCronToast.id)
    if (updatedRun && updatedRun.status !== 'running') {
      setBackgroundCronToast({
        id: updatedRun.id,
        task: updatedRun.task,
        status: updatedRun.status,
        started_at: updatedRun.started_at,
        duration_s: updatedRun.duration_s,
        rows_ingested: updatedRun.rows_ingested
      })
    }
  }, [recentCrons?.entries, backgroundCronToast])

  // Effect to auto-dismiss non-running notifications after 8 seconds of inactivity
  useEffect(() => {
    if (!backgroundCronToast) return
    if (backgroundCronToast.status !== 'running') {
      const timer = setTimeout(() => {
        setBackgroundCronToast(null)
      }, 8000)
      return () => clearTimeout(timer)
    }
  }, [backgroundCronToast])

  // Auto-focus the floating console on the most relevant active job
  useEffect(() => {
    if (displayedJobs.length > 0) {
      if (selectedConsoleJobId === null || !displayedJobs.some(j => j.id === selectedConsoleJobId)) {
        setSelectedConsoleJobId(displayedJobs[0].id)
      }
    } else {
      setSelectedConsoleJobId(null)
      setConsoleOpen(false)
    }
  }, [displayedJobs, selectedConsoleJobId])

  const { data: cronSchedule } = useQuery({
    queryKey: ['admin', 'cron-schedule', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/cron-schedule")
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'cron',
    refetchInterval: 10000,
    staleTime: 0
  })

  const orderedSchedules = React.useMemo(() => {
    let order = ['sync', 'alerts', 'commit', 'optimize', 'local_compact', 'expire', 'full_sync', 'gap_heal', 'ngwaf_sync']
    if (isAnalyst) order = ['metadata_sync', 'alerts']
    
    return order.map(task => {
      const activeJob = displayedJobs.find(j => j.task === task && j.status === 'running')
      const schedule = cronSchedule?.schedules?.find((s: any) => s.task === task)
      return {
        task,
        activeJob,
        schedule
      }
    }).filter(item => item.schedule || item.activeJob)
  }, [cronSchedule?.schedules, displayedJobs, isAnalyst])

  const { data: catalog } = useLogFieldsCatalog()

  const catalogMaps = React.useMemo(() => {
    if (!catalog) return { groups: {}, fields: {} }
    const groups: Record<string, { label: string, description: string }> = {}
    const fields: Record<string, { label: string, description: string }> = {}
    catalog.groups?.forEach((g: any) => {
      groups[g.id === null ? "null" : String(g.id)] = { label: g.label, description: g.description }
    })
    catalog.fields?.forEach((f: any) => {
      fields[f.id] = { label: f.label, description: f.description }
    })
    return { groups, fields }
  }, [catalog])

  const { data: auditLogs, isLoading: isLoadingAudit, isFetching: isFetchingAudit } = useQuery({
    queryKey: ['admin', 'audit-logs', activeServiceId, eventFilter],
    queryFn: async () => {
      const { data } = await client.GET("/api/audit-logs", {
        params: {
          query: {
            page: 1,
            per_page: 500,
            event_type: eventFilter === 'all' ? undefined : eventFilter
          }
        }
      })
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'service_history',
    staleTime: 0
  })

  const { data: ingestedFiles, isLoading: isLoadingIngested } = useQuery({
    queryKey: ['admin', 'ingested-files', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/admin/ingested-files")
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'ingestion',
    staleTime: 0
  })

  const ingestedColumns = ingestedFilesColumns

  const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
    queryKey: ['admin', 'schema', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/schema")
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'schema',
    staleTime: 0
  })

  const purgeMutation = useMutation({
    mutationFn: async () => {
      await client.DELETE("/api/cron-runs")
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
      setIsPurgeOpen(false)
    }
  })

  const auditColumns: ColumnDef<any>[] = React.useMemo(() => [
    {
      accessorKey: 'timestamp',
      id: 'timestamp',
      meta: { label: 'Time' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Time
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => <DateTimeCell iso={row.original.timestamp} />
    },
    {
      accessorKey: 'event_type',
      id: 'event_type',
      meta: { label: 'Event Type' },
      header: 'Event Type',
      cell: ({ row }) => {
        const type = row.original.event_type || 'unknown'
        const colorClass = type === 'provision' ? 'bg-green-500/10 text-green-600' :
                           type === 'teardown' ? 'bg-red-500/10 text-red-600' :
                           type === 'fastly_activation' ? 'bg-blue-500/10 text-blue-600' :
                           type.includes('update') ? 'bg-amber-500/10 text-amber-600' :
                           'bg-slate-500/10 text-slate-600'
        return (
          <Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", colorClass)}>
            {type.replace(/_/g, ' ')}
          </Badge>
        )
      }
    },
    {
      accessorKey: 'actor',
      id: 'actor',
      meta: { label: 'Actor' },
      header: 'Actor',
      cell: ({ row }) => <span className="text-muted-foreground">{row.original.actor}</span>
    },
    {
      accessorKey: 'details',
      id: 'details',
      meta: { label: 'Details' },
      header: 'Details',
      cell: ({ row }) => {
        const details = row.original.details
        if (!details || typeof details !== 'object' || Object.keys(details).length === 0) {
          return <span className="text-muted-foreground italic text-[10px]">No details available</span>
        }

        const type = row.original.event_type || 'unknown'
        
        return (
          <Dialog>
            <DialogTrigger className={cn(buttonVariants({ variant: "ghost", size: "sm" }), "h-6 text-[10px] bg-muted/40 hover:bg-muted/60 text-muted-foreground")}>
              <FileCode className="h-3 w-3 mr-1.5" />
              View Details
            </DialogTrigger>
            <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
              <DialogHeader>
                <DialogTitle className="text-sm font-semibold capitalize flex items-center gap-2">
                  <Settings className="w-4 h-4 text-primary" />
                  {type.replace(/_/g, ' ')} Details
                </DialogTitle>
              </DialogHeader>
              
              {type === 'provision' ? (
                <div className="space-y-4 mt-2">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                        <Database className="w-3 h-3" /> Storage
                      </h4>
                      <div className="space-y-1.5">
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Bucket</span>
                          <span className="font-mono">{details.bucket || details.fos_bucket_name || '-'}</span>
                        </div>
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Prefix</span>
                          <span className="font-mono">{details.prefix || details.fos_prefix || '(none)'}</span>
                        </div>
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Region</span>
                          <span className="font-mono">{details.region || details.fos_region || '-'}</span>
                        </div>
                      </div>
                    </div>

                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                        <Settings className="w-3 h-3" /> Configuration
                      </h4>
                      <div className="space-y-1.5">
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Sample Rate</span>
                          <span className="font-mono">{details.sample_rate || '-'}{details.sample_rate ? '%' : ''}</span>
                        </div>
                        {details.log_period && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Log Period</span>
                            <span className="font-mono">{details.log_period}s</span>
                          </div>
                        )}
                        {details.edge_only !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Edge Only</span>
                            <span className="font-mono">{details.edge_only ? 'Yes' : 'No'}</span>
                          </div>
                        )}
                        {details.cdn_url && (
                            <div className="flex items-center text-sm">
                                <span className="text-muted-foreground w-32">CDN URL</span>
                                <span className="font-mono truncate ml-2 max-w-[200px]" title={details.cdn_url}>{details.cdn_url}</span>
                            </div>
                        )}
                      </div>
                    </div>
                  </div>

                  {(details.enable_cron_sync !== undefined || details.log_retention_days !== undefined) && (
                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                        <Clock className="w-3 h-3" /> Automation & Retention
                      </h4>
                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-1.5">
                        {details.enable_cron_sync !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Cron Sync</span>
                            <span className="font-mono">{details.enable_cron_sync ? 'Enabled' : 'Disabled'}</span>
                          </div>
                        )}
                        {details.log_retention_days !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Retention</span>
                            <span className="font-mono">{details.log_retention_days} days</span>
                          </div>
                        )}
                        {details.delete_after !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Auto Delete</span>
                            <span className="font-mono">{details.delete_after ? 'Yes' : 'No'}</span>
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  {details.log_fields && (
                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2">
                        <ClipboardList className="w-3 h-3" /> Initial Log Fields
                      </h4>
                      <div className="space-y-4">
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Selected Groups</div>
                          <div className="flex flex-wrap gap-1.5">
                            {details.log_fields.groups?.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return (
                                <Badge key={id} variant="outline" className="text-[10px] py-0 font-normal bg-background/50">
                                  {g ? g.label : id}
                                </Badge>
                              )
                            })}
                            {(!details.log_fields.groups || details.log_fields.groups.length === 0) && (
                                <span className="text-xs text-muted-foreground italic">None</span>
                            )}
                          </div>
                        </div>

                        {details.log_fields.field_overrides && Object.keys(details.log_fields.field_overrides).length > 0 && (
                          <div>
                            <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Field Overrides</div>
                            <div className="flex flex-wrap gap-1.5">
                              {Object.entries(details.log_fields.field_overrides).map(([id, enabled]) => {
                                const f = catalogMaps.fields[id]
                                return (
                                  <Badge 
                                    key={id} 
                                    className={cn(
                                        "text-[10px] py-0 font-normal border shadow-none",
                                        enabled ? "bg-green-500/10 text-green-600 border-green-500/20" : "bg-red-500/10 text-red-600 border-red-500/20"
                                    )}
                                  >
                                    {enabled ? '+' : '-'}{f ? f.label : id}
                                  </Badge>
                                )
                              })}
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
                    <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2">
                      <Settings className="w-3 h-3" /> Settings Deployed
                    </h4>
                    <div className="flex flex-col gap-2">
                      {Object.entries(details).map(([key, val]) => {
                        if (key === 'log_fields_deployed') return null;
                        const label = key.replace(/_/g, ' ');
                        const from = (val as any).from;
                        const to = (val as any).to;
                        return (
                          <div key={key} className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground capitalize">{label}</span>
                            <div className="flex items-center gap-2">
                              <span className="text-muted-foreground line-through opacity-70">{String(from)}</span>
                              <ChevronRight className="w-3 h-3 text-muted-foreground" />
                              <span className="font-mono">{String(to)}</span>
                            </div>
                          </div>
                        )
                      })}
                      {Object.keys(details).filter(k => k !== 'log_fields_deployed').length === 0 && (
                        <span className="text-xs text-muted-foreground italic">No settings changed.</span>
                      )}
                    </div>
                  </div>
                  {details.log_fields_deployed && (
                    <div className="border rounded-md p-3 bg-green-500/10 border-green-500/20">
                       <h4 className="text-[10px] font-semibold text-green-700 uppercase tracking-wide mb-2 flex items-center gap-1.5">
                         <ClipboardList className="w-3 h-3" /> Log Format Updated
                       </h4>
                       <p className="text-xs text-green-700/80">
                         The latest standard and custom field selections have been compiled into VCL and deployed to Fastly.
                       </p>
                    </div>
                  )}
                </div>
              ) : type === 'log_format_update' && details.groups_before && details.groups_after ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="border rounded-md p-3 bg-red-500/5">
                      <h4 className="text-xs font-semibold text-red-600 mb-2 uppercase tracking-wide">Before</h4>
                      <div className="space-y-3">
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
                          <div className="flex flex-col gap-1">
                            {details.groups_before.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return <div key={id} className="text-xs font-mono text-foreground/80 break-words">{g ? `${g.label}` : id}</div>
                            })}
                            {!details.groups_before.length && <div className="text-xs italic text-muted-foreground">None</div>}
                          </div>
                        </div>
                      </div>
                    </div>
                    
                    <div className="border rounded-md p-3 bg-green-500/5">
                      <h4 className="text-xs font-semibold text-green-600 mb-2 uppercase tracking-wide">After</h4>
                      <div className="space-y-3">
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
                          <div className="flex flex-col gap-1">
                            {details.groups_after.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return <div key={id} className="text-xs font-mono text-foreground/80 break-words">{g ? `${g.label}` : id}</div>
                            })}
                            {!details.groups_after.length && <div className="text-xs italic text-muted-foreground">None</div>}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  
                  {(details.fields_added?.length > 0 || details.fields_removed?.length > 0) && (
                    <div className="grid grid-cols-2 gap-4 mt-2">
                      <div className="border rounded-md p-3 border-red-200 dark:border-red-900/30">
                        <div className="text-[10px] font-medium text-red-600 mb-1 uppercase flex items-center gap-1.5"><X className="w-3 h-3" /> Fields Removed</div>
                        <div className="flex flex-col gap-1">
                          {details.fields_removed?.map((id: string) => {
                            const f = catalogMaps.fields[id]
                            return <div key={id} className="text-xs font-mono text-red-600/90 break-words" title={f?.description}>- {f ? f.label : id}</div>
                          })}
                          {(!details.fields_removed || !details.fields_removed.length) && <div className="text-xs italic text-muted-foreground">None</div>}
                        </div>
                      </div>
                      <div className="border rounded-md p-3 border-green-200 dark:border-green-900/30">
                        <div className="text-[10px] font-medium text-green-600 mb-1 uppercase flex items-center gap-1.5"><Check className="w-3 h-3" /> Fields Added</div>
                        <div className="flex flex-col gap-1">
                          {details.fields_added?.map((id: string) => {
                            const f = catalogMaps.fields[id]
                            return <div key={id} className="text-xs font-mono text-green-600/90 break-words" title={f?.description}>+ {f ? f.label : id}</div>
                          })}
                          {(!details.fields_added || !details.fields_added.length) && <div className="text-xs italic text-muted-foreground">None</div>}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex flex-col gap-2 mt-2">
                  {Object.entries(details).map(([key, value]) => {
                    if (
                      (key.toLowerCase().includes('prefix') && !value) || 
                      value === '' || 
                      value === null || 
                      value === undefined ||
                      (type === 'fastly_activation' && key === 'active')
                    ) {
                      return null
                    }
                    
                    const valString = typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)
                    
                    return (
                      <div key={key} className="flex flex-col border rounded p-3 bg-muted/20">
                        <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-1.5">{key.replace(/_/g, ' ')}</span>
                        {typeof value === 'object' ? (
                          <pre className="text-xs font-mono bg-background p-2 rounded overflow-x-auto text-foreground/90 whitespace-pre-wrap">{valString}</pre>
                        ) : (
                          <span className="text-sm font-mono text-foreground/90 break-all">{valString}</span>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}
            </DialogContent>
          </Dialog>
        )
      }
    }
  ], [catalogMaps])


  const cronColumns: ColumnDef<any>[] = React.useMemo(() => [
    {
      accessorKey: 'started_at',
      id: 'started_at',
      meta: { label: 'Started At' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Started At
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => <DateTimeCell iso={row.original.started_at} />
    },
    {
      id: 'finished_at',
      meta: { label: 'Finished At' },
      accessorFn: (row: any) => {
        if (!row.started_at || row.duration_s == null) return null
        return new Date(new Date(row.started_at).getTime() + row.duration_s * 1000).toISOString()
      },
      enableSorting: false,
      header: () => (
        <span className="text-xs font-medium px-2.5">Finished At</span>
      ),
      cell: ({ row }) => {
        if (row.original.status === 'running') {
          return <span className="text-muted-foreground/40">—</span>
        }
        const startIso = row.original.started_at
        const dur = row.original.duration_s
        if (!startIso || dur == null) {
          return <span className="text-muted-foreground/40">—</span>
        }
        const finishedIso = new Date(new Date(startIso).getTime() + dur * 1000).toISOString()
        return <DateTimeCell iso={finishedIso} />
      }
    },
    {
      accessorKey: 'task',
      id: 'task',
      meta: { label: 'Task' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Task
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        const isSync = row.original.task === 'sync' || row.original.task === 'metadata_sync'
        const exp = CRON_EXPLANATIONS[row.original.task] || 'Background job.'
        return (
          <div className="flex flex-col gap-1 py-1">
             <TooltipProvider delay={200}>
               <Tooltip>
                 <TooltipTrigger render={
                   <Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", isSync ? "bg-blue-500/10 text-blue-600 hover:bg-blue-500/20" : "bg-purple-500/10 text-purple-600 hover:bg-purple-500/20")}>
                     {row.original.task === 'metadata_sync' ? 'sync' : row.original.task}
                   </Badge>
                 } />
                 <TooltipContent side="right" className="max-w-[250px] text-xs">
                   <p>{exp}</p>
                 </TooltipContent>
               </Tooltip>
             </TooltipProvider>
             {row.original.summary && <span className="text-[11px] text-muted-foreground whitespace-normal break-words leading-tight">{row.original.summary}</span>}
          </div>
        )
      }
    },
    {
      accessorKey: 'status',
      id: 'status',
      meta: { label: 'Status' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Status
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        const val = row.original.status
        const err = row.original.error_message
        const [copied, setCopied] = useState(false)

        const handleCopy = (e: React.MouseEvent) => {
          e.stopPropagation()
          if (err) {
            navigator.clipboard.writeText(err)
            setCopied(true)
            setTimeout(() => setCopied(false), 2000)
          }
        }

        if (val === 'running') {
          return (
            <Badge variant="outline" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold border-blue-500/30 text-blue-500 bg-blue-500/10 flex items-center gap-1 w-fit">
              <Loader2 className="w-3 h-3 animate-spin" />
              Running
            </Badge>
          )
        }
        if (val === 'skipped') {
          return <Badge variant="secondary" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold opacity-60">No Alerts</Badge>
        }
        if (val === 'success' && (!row.original.corrupt_rows || row.original.corrupt_rows === 0)) {
          return <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Success</Badge>
        }
        if (val === 'partial_success' || (val === 'success' && row.original.corrupt_rows > 0)) {
          return <Badge variant="warning" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Partial Success</Badge>
        }
        return (
          <div className="flex items-center gap-1.5">
            <Tooltip>
              <TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none  uppercase text-[10px] font-bold" />}>
                Error
              </TooltipTrigger>
              <TooltipContent className="max-w-[400px] break-words bg-destructive text-white dark:text-white">
                <p className="text-xs font-mono">{err || 'Unknown error'}</p>
              </TooltipContent>
            </Tooltip>
            {err && (
              <Button 
                variant="ghost" 
                size="icon" 
                className="h-6 w-6 text-muted-foreground hover:text-foreground" 
                onClick={handleCopy}
                title="Copy full error message"
              >
                {copied ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
              </Button>
            )}
          </div>
        )
      }
    },
    {
      accessorKey: 'duration_s',
      id: 'duration_s',
      meta: { label: 'Duration' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Duration
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        const s = row.original.duration_s
        if (row.original.status === 'running') {
          // Use a simple localized timer component for running jobs
          return <LiveTimer startedAt={row.original.started_at} />
        }
        const fmt = s < 1 ? `${Math.round(s * 1000)}ms` : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
        return <span className="font-mono text-muted-foreground tabular-nums text-xs">{fmt}</span>
      }
    },
    {
      accessorKey: 'files_downloaded',
      id: 'files_downloaded',
      meta: { label: 'Files Downloaded' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Files Processed
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        if (row.original.status === 'running') {
          return (
             <span className="font-mono text-muted-foreground/60 text-xs italic">Processing...</span>
          )
        }
        
        const task = row.original.task;
        
        let count = row.original.files_downloaded || 0;
        let label = 'raw logs';

        if (task === 'alerts') {
          label = count === 1 ? 'alert evaluated' : 'alerts evaluated';
        } else if (task === 'commit') {
          if (!row.original.rows_ingested) return <span className="text-muted-foreground/40">—</span>
          // A commit task takes X local buffer files and turns them into 1 cloud file.
          // By eagerly pulling it, we cached that 1 new cloud file.
          count = 1;
          label = 'cloud file cached';
        } else if (task === 'metadata_sync') {
          if (!isAnalyst) return <span className="text-muted-foreground/40">—</span>
          label = 'cloud files downloaded';
        } else if (task === 'optimize') {
          count = row.original.parquet_files_optimized || 0;
          label = 'files merged';
        }

        return (
          <span className="font-mono text-muted-foreground tabular-nums text-xs" title={label}>
            {count.toLocaleString()} <span className="text-[10px] text-muted-foreground/50">{label}</span>
          </span>
        )
      }
    },
    {
      accessorKey: 'rows_ingested',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          {isAnalyst ? 'Log Entries Imported' : 'Log Entries Processed'}
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        if (row.original.status === 'running') {
           return <span className="font-mono text-muted-foreground/60 text-xs italic">Processing...</span>
        }
        
        const task = row.original.task
        if (task === 'optimize' || task === 'commit') {
          return <span className="text-muted-foreground/40">—</span>
        }

        if (task === 'alerts') {
          const triggered = row.original.rows_ingested || 0
          if (triggered === 0) return <span className="text-muted-foreground/40">—</span>
          return (
            <span className="font-mono tabular-nums text-xs text-amber-500 font-medium">
              {triggered} {triggered === 1 ? 'alert triggered' : 'alerts triggered'}
            </span>
          )
        }

        const rows = row.original.rows_ingested || 0
        const corrupt = row.original.corrupt_rows || 0
        const [copiedCorrupt, setCopiedCorrupt] = useState(false)
        
        if (task === 'metadata_sync') {
          if (rows === 0) return <span className="text-muted-foreground/40">—</span>
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {rows.toLocaleString()}
            </span>
          )
        }

        return (
          <div className="flex items-center gap-2">
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {rows.toLocaleString()}
            </span>
            {corrupt > 0 && (
              <div className="flex items-center gap-1 group/corrupt">
                <Tooltip>
                  <TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none  text-[10px] font-bold" />}>
                    {corrupt.toLocaleString()} Skipped
                  </TooltipTrigger>
                  <TooltipContent>
                    These lines were skipped due to missing timestamps or invalid JSON structure.
                  </TooltipContent>
                </Tooltip>
                {row.original.error_message && (
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 text-muted-foreground hover:text-foreground opacity-0 group-hover/corrupt:opacity-100 transition-opacity"
                    onClick={(e) => {
                      e.stopPropagation()
                      navigator.clipboard.writeText(row.original.error_message)
                      setCopiedCorrupt(true)
                      setTimeout(() => setCopiedCorrupt(false), 2000)
                    }}
                    title="Copy corrupt lines"
                  >
                    {copiedCorrupt ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
                  </Button>
                )}
              </div>
            )}
          </div>
        )
      }
    },
    ...(isAnalyst ? [] : [
      {
        accessorKey: 'files_deleted_fos',
        header: ({ column }: any) => (
          <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
            Log Files Deleted
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        ),
        cell: ({ row }: any) => {
          if (row.original.status === 'running' || row.original.task !== 'sync') {
             return <span className="text-muted-foreground/40">—</span>
          }
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {(row.original.files_deleted_fos || 0).toLocaleString()}
            </span>
          )
        }
      },
      {
        id: 'rows_committed',
        header: ({ column }: any) => (
          <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
            Rows Committed
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        ),
        cell: ({ row }: any) => {
          if (row.original.status === 'running') {
             return <span className="font-mono text-muted-foreground/60 text-xs italic">Processing...</span>
          }
          // For commit tasks, rows_ingested holds the rows committed to Iceberg.
          // For sync tasks, this field holds rows written to the local buffer.
          const val = row.original.task === 'commit' ? row.original.rows_ingested : null
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {val !== null ? val.toLocaleString() : <span className="text-muted-foreground/40">—</span>}
            </span>
          )
        }
      }
    ])
  ], [isAnalyst])


  const handleTabChange = (value: string) => {
    setActiveTab(value)
    
    // Invalidate queries based on the selected tab to trigger a fresh fetch
    if (value === 'cron') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
    } else if (value === 'service_history') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'audit-logs', activeServiceId] })
    } else if (value === 'ingestion') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'ingested-files', activeServiceId] })
    } else if (value === 'iceberg') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'iceberg', activeServiceId] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'iceberg-tree', activeServiceId] })
    } else if (value === 'raw') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'raw-tree', activeServiceId] })
    } else if (value === 'schema') {
      queryClient.invalidateQueries({ queryKey: ['admin', 'schema', activeServiceId] })
    }
  }

  if (!activeServiceId) {
    return <NoServiceSelected icon={Database} message="Please select a service from the header to access admin controls." />
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Management"
        description="Monitor and manage log ingestion history and active data syncs."
      />

      <div className="flex flex-wrap items-center gap-2 bg-muted/30 p-2 rounded-lg border">
        <div className="text-xs font-bold text-muted-foreground uppercase tracking-wider mx-2">Quick Actions</div>
        {!isAnalyst ? (
          <>
            <Button 
              size="sm" 
              variant="default" 
              className="h-8 text-xs bg-primary/90 hover:bg-primary" 
              disabled={status?.access_level === 'read_only'}
              onClick={async () => {
                try {
                  const { data } = await client.POST("/api/admin/ingest-logs")
                  setSseTitle('Importing Logs')
                  setSseDescription('Downloading new raw logs from Fastly Object Storage and processing them...')
                  setIsSSEModalOpen(true)
                  setHasSyncedExtents(false)
                  reset()
                  start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
                  queryClient.invalidateQueries({ queryKey: ['admin'] })
                  queryClient.invalidateQueries({ queryKey: ['dashboard'] })
                } catch (e) {
                  console.error(e)
                }
              }}
            >
              <RefreshCw className="h-3 w-3 mr-1.5" /> Import Logs
            </Button>
            <Button 
              size="sm" 
              variant="outline" 
              className="h-8 text-xs bg-background" 
              disabled={status?.access_level === 'read_only'}
              onClick={async () => {
                try {
                  const { data } = await client.POST("/api/admin/commit-iceberg")
                  setSseTitle('Committing Buffer')
                  setSseDescription('Flushing local Parquet buffer to the shared Iceberg table in Object Storage...')
                  setIsSSEModalOpen(true)
                  reset()
                  start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
                  queryClient.invalidateQueries({ queryKey: ['admin'] })
                } catch (e) {
                  console.error(e)
                }
              }}
            >
              <Archive className="h-3 w-3 mr-1.5" /> Commit Buffer
            </Button>
          </>
        ) : (
          <Button 
            size="sm" 
            variant="default" 
            className="h-8 text-xs bg-primary/90 hover:bg-primary" 
            onClick={() => setIsSyncModalOpen(true)}
          >
            <Download className="h-3 w-3 mr-1.5" /> Sync from Cloud
          </Button>
        )}
        {!isAnalyst && status?.ngwaf_workspace_id && (
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-xs bg-background"
            onClick={() => {
              setSseTitle('NGWAF Bot Sync')
              setSseDescription('Fetching verified bot records from Fastly NGWAF and caching them locally. Progress is saved after each page — run again if the time budget is reached.')
              setIsSSEModalOpen(true)
              reset()
              start(`/api/services/${activeServiceId}/ngwaf-sync`, {})
              queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
            }}
          >
            <Bot className="h-3 w-3 mr-1.5" /> NGWAF Bot Sync
          </Button>
        )}
        <Button
          size="sm"
          variant="outline"
          className="h-8 text-xs bg-background"
          onClick={() => {
            const latestSync = recentCrons?.entries?.find((e: any) => e.task === 'sync') || 
                               cronLogs?.entries?.find((e: any) => e.task === 'sync')
            if (latestSync) {
              setDisplayedJobs(prev => {
                if (prev.some((j: any) => j.id === latestSync.id)) return prev
                return [...prev, { ...latestSync, status: latestSync.status }]
              })
              setSelectedConsoleJobId(latestSync.id)
              setConsoleOpen(true)
            } else {
              window.alert("No recent sync run was found for this service.")
            }
          }}
        >
          <Terminal className="h-3 w-3 mr-1.5" /> View Recent Logs
        </Button>
      </div>

      <Tabs value={activeTab} onValueChange={handleTabChange} className="w-full">
        <ScrollArea className="w-full max-w-full overflow-hidden">
          <TabsList className="w-full flex">
            <TabsTrigger value="cron" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <History className="h-4 w-4" /> Cron Runs
            </TabsTrigger>
            <TabsTrigger value="service_history" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <ClipboardList className="h-4 w-4" /> Service History
            </TabsTrigger>
            {!isAnalyst && (
              <TabsTrigger value="ingestion" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <Database className="h-4 w-4" /> Ingestion History
              </TabsTrigger>
            )}
            <TabsTrigger value="iceberg" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <Archive className="h-4 w-4" /> Iceberg Storage
            </TabsTrigger>
            {!isAnalyst && (
              <TabsTrigger value="raw" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <FileCode className="h-4 w-4" /> Available Logs
              </TabsTrigger>
            )}
            <TabsTrigger value="schema" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <FileCode className="h-4 w-4" /> DuckDB Schema
            </TabsTrigger>
          </TabsList>
        </ScrollArea>

        <TabsContent value="cron" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <div className="p-0">
            <DataTable 
              columns={cronColumns} 
              data={(cronLogs?.entries || []).filter((e: any) => e.status !== 'running')} 
              isLoading={isLoadingCron} 
              initialSorting={[{ id: 'started_at', desc: true }]}
              onRowClick={(row: any) => {
                setDisplayedJobs(prev => {
                  if (prev.some((j: any) => j.id === row.id)) return prev
                  return [...prev, { ...row, status: row.status }]
                })
                setSelectedConsoleJobId(row.id)
                setConsoleOpen(true)
              }}
              renderToolbar={(table) => (
                <>
                  {orderedSchedules.length > 0 && (
                    <div className="p-4 border-b bg-muted/10">
                      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                        {orderedSchedules.map((item) => (
                          <CronScheduleBox 
                            key={item.task} 
                            schedule={item.schedule || { task: item.task }} 
                            activeJob={item.activeJob}
                            compact={item.task === 'expire'} 
                            onOpenConsole={(jobId) => {
                              setConsoleOpen(true)
                              setSelectedConsoleJobId(jobId)
                            }}
                          />
                        ))}
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
                            <SelectItem value={isAnalyst ? 'metadata_sync' : 'sync'}>Sync</SelectItem>
                            {!isAnalyst && <SelectItem value="full_sync">Full Sync</SelectItem>}
                            {!isAnalyst && <SelectItem value="gap_heal">Gap Heal</SelectItem>}
                            <SelectItem value="alerts">Alerts</SelectItem>
                            {!isAnalyst && <SelectItem value="commit">Commit</SelectItem>}
                            {!isAnalyst && <SelectItem value="optimize">Optimize</SelectItem>}
                            {!isAnalyst && <SelectItem value="local_compact">Local Compact</SelectItem>}
                            {!isAnalyst && <SelectItem value="expire">Expire</SelectItem>}
                            {!isAnalyst && <SelectItem value="ngwaf_sync">NGWAF Sync</SelectItem>}
                          </SelectContent>
                        </Select>
                        <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v || 'all')}>
                          <SelectTrigger className="h-8 w-[140px] text-xs">
                            <SelectValue placeholder="All statuses" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All statuses</SelectItem>
                            <SelectItem value="success">Success</SelectItem>
                            {!isAnalyst && <SelectItem value="partial_success">Partial Success</SelectItem>}
                            <SelectItem value="error">Error</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <DropdownMenu>
                          <DropdownMenuTrigger
                            className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}
                          >
                            <span className="flex items-center text-xs">
                              Columns <ChevronDown className="ml-2 h-4 w-4" />
                            </span>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                            {table
                              .getAllColumns()
                              .filter((column: any) => column.getCanHide())
                              .map((column: any) => {
                                return (
                                  <DropdownMenuCheckboxItem
                                    key={column.id}
                                    className="capitalize whitespace-nowrap"
                                    checked={column.getIsVisible()}
                                    onCheckedChange={(value) =>
                                      column.toggleVisibility(!!value)
                                    }
                                  >
                                    {column.id.replace(/_/g, ' ')}
                                  </DropdownMenuCheckboxItem>
                                )
                              })}
                          </DropdownMenuContent>
                        </DropdownMenu>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })}
                          disabled={isFetchingCron}
                          className="h-8 text-xs"
                        >
                          <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingCron ? 'animate-spin' : ''}`} />
                          Refresh
                        </Button>
                        <button
                          className={cn(buttonVariants({ variant: "outline", size: "sm" }), "h-8 text-xs border-destructive/50 text-destructive hover:bg-destructive hover:text-white cursor-pointer")}
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
        </TabsContent>

        <TabsContent value="service_history" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <div className="p-0">
            <DataTable 
              columns={auditColumns} 
              data={auditLogs?.entries || []} 
              isLoading={isLoadingAudit} 
              initialSorting={[{ id: 'timestamp', desc: true }]}
              renderToolbar={(table) => (
                <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
                  <div className="flex items-center gap-4">
                    <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
                    <div className="flex items-center gap-2">
                      <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                        <SelectTrigger className="h-8 w-[200px] text-xs">
                          <SelectValue placeholder="All events" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All events</SelectItem>
                          <SelectItem value="provision">Provision</SelectItem>
                          <SelectItem value="fastly_activation">Fastly Activation</SelectItem>
                          <SelectItem value="cron_settings_update">Cron Settings Update</SelectItem>
                          <SelectItem value="logging_settings_update">Log Settings Updated</SelectItem>
                          <SelectItem value="log_format_update">Log Format Update</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <DropdownMenu>
                      <DropdownMenuTrigger
                        className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}
                      >
                        <span className="flex items-center text-xs">
                          Columns <ChevronDown className="ml-2 h-4 w-4" />
                        </span>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                        {table
                          .getAllColumns()
                          .filter((column: any) => column.getCanHide())
                          .map((column: any) => {
                            return (
                              <DropdownMenuCheckboxItem
                                key={column.id}
                                className="capitalize whitespace-nowrap"
                                checked={column.getIsVisible()}
                                onCheckedChange={(value) =>
                                  column.toggleVisibility(!!value)
                                }
                              >
                                {column.id.replace(/_/g, ' ')}
                              </DropdownMenuCheckboxItem>
                            )
                          })}
                      </DropdownMenuContent>
                    </DropdownMenu>
                    <Button 
                      variant="outline" 
                      size="sm" 
                      onClick={() => queryClient.invalidateQueries({ queryKey: ['admin', 'audit-logs', activeServiceId] })}
                      disabled={isFetchingAudit}
                      className="h-8 text-xs"
                    >
                      <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />
                      Refresh
                    </Button>
                  </div>
                </div>
              )}
            />
          </div>
        </TabsContent>

        <TabsContent value="ingestion" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <DataTable
            columns={ingestedColumns}
            data={ingestedFiles?.files || []}
            isLoading={isLoadingIngested}
            searchKey="file_name"
            initialSorting={[{ id: 'ingested_at', desc: true }]}
            renderToolbar={(table) => (
              <div className="p-4 border-b flex flex-wrap items-center justify-between gap-4">
                <h3 className="text-sm font-medium">Log Ingestion History</h3>
                <div className="flex items-center gap-2 ml-auto">
                  <Input
                    placeholder="Filter by filename..."
                    value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''}
                    onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)}
                    className="max-w-sm h-8"
                  />
                  <DropdownMenu>
                    <DropdownMenuTrigger className="inline-flex items-center justify-center whitespace-nowrap rounded-md text-xs font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-8 px-3 py-2">
                        Columns <ChevronDown className="ml-2 h-4 w-4" />
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                      {table
                        .getAllColumns()
                        .filter((column: any) => column.getCanHide())
                        .map((column: any) => {
                          return (
                            <DropdownMenuCheckboxItem
                              key={column.id}
                              className="whitespace-nowrap"
                              checked={column.getIsVisible()}
                              onCheckedChange={(value) => column.toggleVisibility(!!value)}
                            >
                              {(column.columnDef.meta as any)?.label ??
                                (typeof column.columnDef.header === 'string'
                                  ? column.columnDef.header
                                  : column.id)}
                            </DropdownMenuCheckboxItem>
                          )
                        })}
                    </DropdownMenuContent>
                  </DropdownMenu>
                </div>
              </div>
            )}
          />
        </TabsContent>

        <TabsContent value="iceberg" className="mt-4 space-y-4">
          <IcebergStatus accessLevel={status?.access_level ?? undefined} />
          <IcebergCalendar />
          
          <div className="border rounded-lg overflow-hidden bg-card">
            <div className="p-4 border-b">
              <h3 className="text-sm font-medium">Iceberg Data Lake Layout</h3>
              <p className="text-xs text-muted-foreground mt-1">Direct view into the Iceberg metadata and data files in Object Storage.</p>
            </div>
            <div className="p-0">
              <FileBrowser type="iceberg" />
            </div>
          </div>
        </TabsContent>

        <TabsContent value="raw" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <div className="p-4 border-b">
            <h3 className="text-sm font-medium">Available Logs</h3>
            <p className="text-xs text-muted-foreground mt-1">Raw .gz files delivered by Fastly waiting to be processed.</p>
          </div>
          <div className="p-0">
            <FileBrowser type="raw" />
          </div>
        </TabsContent>

        <TabsContent value="schema" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <div className="p-4 border-b flex justify-between items-center">
            <div>
              <h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3>
              <p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p>
            </div>
          </div>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Column Name</TableHead>
                  <TableHead>DuckDB Type</TableHead>
                  <TableHead className="text-right">Populated %</TableHead>
                  <TableHead className="text-right">Approx Unique</TableHead>
                  <TableHead className="max-w-[200px]">Min Value</TableHead>
                  <TableHead className="max-w-[200px]">Max Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoadingSchema ? (
                  [1, 2, 3, 4, 5].map(i => (
                    <TableRow key={i}>
                      <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-12 ml-auto" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-16 ml-auto" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                    </TableRow>
                  ))
                ) : (schemaData as any)?.schema.map((col: any) => {
                  const hasStats = col.null_percentage !== undefined
                  const populatedPct = hasStats ? Math.max(0, 100 - col.null_percentage).toFixed(1) : '—'
                  
                  return (
                    <TableRow key={col.name}>
                      <TableCell className="font-mono text-xs font-bold">{col.name}</TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground">{col.type}</TableCell>
                      <TableCell className="font-mono text-xs text-right tabular-nums">
                        {hasStats ? (
                          <span className={populatedPct === '0.0' ? 'text-muted-foreground/30' : ''}>
                            {populatedPct}%
                          </span>
                        ) : '—'}
                      </TableCell>
                      <TableCell className="font-mono text-xs text-right text-muted-foreground tabular-nums">
                        {hasStats ? col.approx_unique?.toLocaleString() ?? '—' : '—'}
                      </TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground truncate max-w-[200px]" title={col.min}>
                        {hasStats ? col.min ?? '—' : '—'}
                      </TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground truncate max-w-[200px]" title={col.max}>
                        {hasStats ? col.max ?? '—' : '—'}
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        </TabsContent>
      </Tabs>

      <SyncFromCloudModal 
        open={isSyncModalOpen} 
        onOpenChange={setIsSyncModalOpen}
        onStartSync={async (range) => {
          const apiRange = range ? { start_time: range.start, end_time: range.end } : {}
          try {
            const { data } = await client.POST("/api/admin/ingest-logs", {
              params: { query: apiRange }
            })
            setSseTitle('Syncing from Cloud')
            setSseDescription('Fetching latest snapshots and downloading new data files from the cloud...')
            setIsSSEModalOpen(true)
            setHasSyncedExtents(false)
            reset()
            start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
            queryClient.invalidateQueries({ queryKey: ['admin'] })
            queryClient.invalidateQueries({ queryKey: ['dashboard'] })
          } catch (e) {
            console.error(e)
          }
        }}
      />

      <Dialog open={isSSEModalOpen} onOpenChange={(open) => {
        if (sseStatus === 'streaming') return
        setIsSSEModalOpen(open)
        if (!open) {
          stop()
          queryClient.invalidateQueries({ queryKey: ['admin'] })
          queryClient.invalidateQueries({ queryKey: ['dashboard'] })
        }
      }}>
        <DialogContent className="sm:max-w-4xl max-h-[85vh] min-h-[50vh] flex flex-col p-0 overflow-hidden" showCloseButton={sseStatus !== 'streaming'}>
          <DialogHeader className="px-6 pt-6 pb-4 border-b shrink-0 bg-background">
            <DialogTitle>{sseTitle}</DialogTitle>
          </DialogHeader>
          
          <SSEProgressView 
            lines={lines}
            status={sseStatus}
            error={sseError}
            description={sseDescription}
            className="flex-1 mx-6 my-4"
          />

          <DialogFooter className="px-6 py-4 bg-muted/10 border-t shrink-0">
            {sseStatus !== 'streaming' && (
               <Button variant="outline" onClick={() => {
                 setIsSSEModalOpen(false)
                 stop()
                 queryClient.invalidateQueries({ queryKey: ['admin'] })
               }}>
                 {sseStatus === 'done' ? 'Close' : 'Cancel'}
               </Button>
            )}
            {sseStatus === 'streaming' && (
              <Button variant="outline" onClick={stop}>Stop</Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <FloatingOperationsDock
        displayedJobs={displayedJobs}
        setDisplayedJobs={setDisplayedJobs}
        isOpen={consoleOpen}
        setIsOpen={setConsoleOpen}
        selectedJobId={selectedConsoleJobId}
        setSelectedJobId={setSelectedConsoleJobId}
        onDismiss={removeDisplayedJob}
        backgroundCronToast={backgroundCronToast}
        setBackgroundCronToast={setBackgroundCronToast}
      />
    </div>
  )
}

function FloatingOperationsDock({
  displayedJobs,
  setDisplayedJobs,
  isOpen,
  setIsOpen,
  selectedJobId,
  setSelectedJobId,
  onDismiss,
  backgroundCronToast,
  setBackgroundCronToast
}: {
  displayedJobs: any[];
  setDisplayedJobs: React.Dispatch<React.SetStateAction<any[]>>;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
  selectedJobId: number | string | null;
  setSelectedJobId: (id: number | string | null) => void;
  onDismiss: (id: number) => void;
  backgroundCronToast: any;
  setBackgroundCronToast: (toast: any) => void;
}) {
  const { full, abbr } = useDateFormat()

  if (displayedJobs.length === 0 && !backgroundCronToast) return null

  const activeJob = displayedJobs.find(j => j.id === selectedJobId) || displayedJobs[0]
  const runningJobs = displayedJobs.filter(j => j.status === 'running')
  const runningCount = runningJobs.length

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2 pointer-events-auto">
      {/* Integrated cool, premium, bottom-right notification toast stacked above minimized button */}
      {!isOpen && backgroundCronToast && (
        <div className="w-80 sm:w-96 bg-zinc-950/90 backdrop-blur-md text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl overflow-hidden animate-in fade-in slide-in-from-bottom-2 duration-300 pointer-events-auto">
          <div className="p-3.5 flex gap-3">
            {/* Live Indicator or Check/Error Icon */}
            <div className="shrink-0 pt-0.5">
              {backgroundCronToast.status === 'running' ? (
                <div className="relative flex h-3 w-3 mt-0.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span>
                </div>
              ) : backgroundCronToast.status === 'error' ? (
                <div className="h-3.5 w-3.5 rounded-full bg-red-950/40 border border-red-500/30 flex items-center justify-center text-red-500">
                  <X className="h-2 w-2" />
                </div>
              ) : (
                <div className="h-3.5 w-3.5 rounded-full bg-emerald-900/40 border border-emerald-500/30 flex items-center justify-center text-emerald-400">
                  <Check className="h-2 w-2" />
                </div>
              )}
            </div>

            {/* Content Details */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs font-semibold text-zinc-200">
                  {backgroundCronToast.status === 'running' ? 'Background Sync Started' : 
                   backgroundCronToast.status === 'error' ? 'Background Sync Failed' : 'Background Sync Completed'}
                </p>
                <button 
                  onClick={() => setBackgroundCronToast(null)}
                  className="text-zinc-500 hover:text-zinc-300 p-0.5 hover:bg-zinc-900 rounded transition-all cursor-pointer"
                  title="Close notification"
                >
                  <X className="h-3 w-3" />
                </button>
              </div>
              <p className="text-[10px] text-zinc-400 mt-1 font-mono uppercase tracking-wider">
                Task: {backgroundCronToast.task === 'metadata_sync' ? 'sync' : backgroundCronToast.task}
              </p>
              
              {/* Optional completed job statistics */}
              {backgroundCronToast.status !== 'running' && (
                <div className="mt-2 pt-2 border-t border-zinc-900 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-zinc-500 font-mono">
                  {backgroundCronToast.rows_ingested !== undefined && (
                    <span>Ingested: <strong className="text-zinc-300">{backgroundCronToast.rows_ingested.toLocaleString()} rows</strong></span>
                  )}
                  {backgroundCronToast.duration_s !== undefined && (
                    <span>Duration: <strong className="text-zinc-300">{backgroundCronToast.duration_s.toFixed(1)}s</strong></span>
                  )}
                </div>
              )}

              {/* Action Trigger Button */}
              <div className="mt-2.5 flex justify-end">
                <Button
                  size="sm"
                  variant="secondary"
                  className="h-6.5 text-[9px] font-medium bg-zinc-900 hover:bg-zinc-850 text-zinc-300 border border-zinc-800 cursor-pointer px-2"
                  onClick={() => {
                    setSelectedJobId(backgroundCronToast.id)
                    setIsOpen(true)
                    setBackgroundCronToast(null)
                  }}
                >
                  <Terminal className="h-2.5 w-2.5 mr-1" /> View Console Logs
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}

      {!isOpen ? (
        <button
          onClick={() => setIsOpen(true)}
          className={cn(
            "px-4 py-2.5 rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-all hover:scale-105 duration-200 cursor-pointer border",
            runningCount > 0
              ? "bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20 animate-bounce"
              : "bg-zinc-850 hover:bg-zinc-800 text-zinc-300 border-zinc-700/50"
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
        <div className="bg-zinc-950 text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl w-[440px] sm:w-[500px] h-[380px] flex flex-col overflow-hidden animate-in slide-in-from-bottom-5 duration-300">
          {/* Header */}
          <div className="flex items-center justify-between px-3 py-2 bg-zinc-900 border-b border-zinc-800 shrink-0">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300">
              <Database className="h-3.5 w-3.5 text-blue-500" />
              <span>Console Log Terminal</span>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                onClick={() => setIsOpen(false)}
                className="text-zinc-400 hover:text-zinc-200 p-1 hover:bg-zinc-800 rounded cursor-pointer transition-colors"
                title="Minimize console"
              >
                <ChevronDown className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* Tab Bar for jobs */}
          {displayedJobs.length > 1 && (
            <div className="flex border-b border-zinc-800 bg-zinc-900/50 overflow-x-auto scrollbar-none shrink-0 px-2 pt-1 gap-1">
              {displayedJobs.map((job) => {
                const isActive = job.id === selectedJobId
                return (
                  <button
                    key={job.id}
                    onClick={() => setSelectedJobId(job.id)}
                    className={cn(
                      "px-3 py-1.5 rounded-t text-[10px] font-mono uppercase tracking-wider flex items-center gap-1.5 cursor-pointer border-t border-x transition-all shrink-0",
                      isActive
                        ? "bg-zinc-950 text-blue-400 border-zinc-800 border-b-zinc-950 font-bold"
                        : "bg-transparent text-zinc-400 border-transparent hover:text-zinc-200 hover:bg-zinc-800/30"
                    )}
                  >
                    <span className={cn(
                      "w-1.5 h-1.5 rounded-full transition-colors duration-300",
                      job.status === 'running'
                        ? "bg-blue-500 animate-pulse"
                        : "bg-zinc-600"
                    )} />
                    {job.task === 'metadata_sync' ? 'sync' : job.task}
                    <span 
                      onClick={(e) => {
                        e.stopPropagation()
                        onDismiss(job.id)
                      }}
                      className="ml-1 hover:bg-zinc-800 p-0.5 rounded text-zinc-500 hover:text-zinc-300"
                      title="Dismiss task"
                    >
                      <X className="h-2.5 w-2.5" />
                    </span>
                  </button>
                )
              })}
            </div>
          )}

          {/* Terminal Body */}
          <div className="flex-1 p-3 font-mono bg-zinc-950 overflow-y-auto flex flex-col justify-between">
            <div className="flex-1 overflow-hidden flex flex-col">
              <div className="text-[10px] text-zinc-500 border-b border-zinc-900 pb-1 mb-2 flex items-center justify-between shrink-0">
                <span>STREAM ID: {activeJob?.id}{activeJob?.started_at && ` • STARTED: ${full(activeJob.started_at)} ${abbr()}`}</span>
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
                      setDisplayedJobs(prev => prev.map(j => j.id === activeJob.id ? { ...j, status: 'completed' } : j))
                    }
                  }} 
                />
              </div>
            </div>
            
            {/* Terminal Footer Actions */}
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
