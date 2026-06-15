'use client'

import React, { useState, useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useSyncStatus } from '@/hooks/useSyncStatus'
import { useSSE } from '@/hooks/useSSE'

export type BackgroundCronToast = {
  id: number
  task: string
  status: string
  started_at: string
  duration_s?: number
  rows_ingested?: number
} | null

/**
 * Orchestrates all state, data queries, and side-effects for the
 * Logs / Data Management page. Returns the entire bag of values
 * needed by the rendering shell + its section components.
 */
export function useLogsPageState() {
  const { activeServiceId, services } = useServiceStore()
  const activeService = services.find(s => s.id === activeServiceId)
  const isAnalyst = activeService?.accessLevel === 'read_only'
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState('cron')
  const [isPurgeOpen, setIsPurgeOpen] = useState(false)
  const [taskFilter, setTaskFilter] = useState('all')
  const [statusFilter, setStatusFilter] = useState('all')
  const [eventFilter, setEventFilter] = useState('all')

  const { lines, status: sseStatus, error: sseError, start, stop, reset } = useSSE()
  const [isSSEModalOpen, setIsSSEModalOpen] = useState(false)
  const [isSyncModalOpen, setIsSyncModalOpen] = useState(false)
  const [sseTitle, setSseTitle] = useState('')
  const [sseDescription, setSseDescription] = useState('')
  const [consoleOpen, setConsoleOpen] = useState(false)
  const [selectedConsoleJobId, setSelectedConsoleJobId] = useState<number | string | null>(null)

  // Background cron toast notification state
  const [backgroundCronToast, setBackgroundCronToast] = useState<BackgroundCronToast>(null)

  // Multi-tenant safe run ID tracker to prevent alerting old runs or cross-tenant leaks
  const maxSeenIdRef = React.useRef<number | null>(null)

  // Reset tracker when switching active services
  useEffect(() => {
    maxSeenIdRef.current = null
    setBackgroundCronToast(null)
  }, [activeServiceId])

  const { setHasSyncedExtents } = useFilterStore()

  const { data: status } = useSyncStatus()

  const { data: cronLogs, isLoading: isLoadingCron, isFetching: isFetchingCron } = useQuery({
    queryKey: ['admin', 'cron-logs', activeServiceId, taskFilter, statusFilter],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/cron-runs", { signal,
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
    // 30 s cadence on the 500-row cron-history pull — three full pulls
    // per cold load was burning the WAL writer's contention budget
    // every 5 s. The since_id delta poll (above) covers fresh activity
    // at 15 s; this big-payload pull only needs to refresh when the
    // user actively lingers on the cron tab.
    refetchInterval: 30_000,
    // Match staleTime to refetchInterval so an in-page tab toggle
    // (cron → audit → cron) within a poll window reuses the cached
    // 500-row payload instead of paying a fresh /api/cron-runs
    // round-trip on each remount.
    staleTime: 30_000,
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
    queryKey: ['admin', 'cron-logs-recent', activeServiceId],
    queryFn: async ({ signal }) => {
      const max = maxSeenIdRef.current
      const sinceId = max != null ? Math.max(0, max - 1) : undefined
      const { data } = await client.GET("/api/cron-runs", { signal,
        params: {
          query: {
            page: 1,
            per_page: 10,
            since_id: sinceId,
          }
        }
      })
      return data as any
    },
    enabled: !!activeServiceId, // Tab independent polling!
    // 15 s cadence on the since_id delta poll — passive awareness, no
    // tight loop required. Drops steady-state network noise ~3× and
    // takes one round-trip out of the cold-load settle window.
    refetchInterval: 15_000,
    staleTime: 15_000,
  })

  // Derive currently running crons and loading state from recent crons to keep downstream compatibility intact
  const runningCrons = React.useMemo(() => {
    if (!recentCrons?.entries) return { entries: [] }
    return {
      entries: recentCrons.entries.filter((e: any) => e.status === 'running')
    }
  }, [recentCrons])

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
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/cron-schedule", { signal })
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'cron',
    refetchInterval: 10000,
    // Schedule metadata changes only on admin config edits — caching for
    // a single poll window is safe and skips refetch on cron-tab remount.
    staleTime: 10_000,
  })

  const orderedSchedules = React.useMemo(() => {
    // Display priority for known tasks. Backend (/api/cron-schedule) is
    // the source of truth for WHICH tasks exist — anything not in this
    // map is still rendered, just appended after the prioritised tiles
    // in API order. That means a freshly-registered backend cron shows
    // up on the grid automatically; only its position needs curating.
    const TASK_PRIORITY: Record<string, number> = {
      sync: 1,
      alerts: 2,
      commit: 3,
      optimize: 4,
      local_compact: 5,
      metadata_cleanup: 6,
      expire: 7,
      full_sync: 8,
      gap_heal: 9,
      ngwaf_sync: 10,
      metadata_sync: 11,
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
    const adminExcluded = new Set(['metadata_sync'])
    const source = (cronSchedule?.schedules ?? []) as Array<{ task: string }>
    const filtered = isAnalyst
      ? source.filter((s) => analystAllowed.has(s.task))
      : source.filter((s) => !adminExcluded.has(s.task))
    const sorted = [...filtered].sort((a, b) => {
      const pa = TASK_PRIORITY[a.task] ?? 999
      const pb = TASK_PRIORITY[b.task] ?? 999
      return pa - pb || a.task.localeCompare(b.task)
    })
    return sorted.map((schedule) => ({
      task: schedule.task,
      activeJob: displayedJobs.find((j) => j.task === schedule.task && j.status === 'running'),
      schedule,
    }))
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
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/audit-logs", { signal,
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
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/ingested-files", { signal })
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'ingestion',
    staleTime: 0
  })

  const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
    queryKey: ['admin', 'schema', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/schema", { signal })
      return data as any
    },
    enabled: !!activeServiceId && activeTab === 'schema',
    staleTime: 0
  })

  const purgeMutation = useMutation({
    mutationFn: async () => {
      await client.DELETE("/api/cron-runs", {})
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
      setIsPurgeOpen(false)
    }
  })

  const handleTabChange = useCallback((value: string) => {
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
  }, [activeServiceId, queryClient])

  return {
    // identity
    activeServiceId,
    isAnalyst,
    // tab state
    activeTab,
    handleTabChange,
    // cron tab filters
    taskFilter,
    setTaskFilter,
    statusFilter,
    setStatusFilter,
    // audit tab filter
    eventFilter,
    setEventFilter,
    // SSE modal / sync modal
    sseStatus,
    sseError,
    sseTitle,
    sseDescription,
    setSseTitle,
    setSseDescription,
    lines,
    start,
    stop,
    reset,
    isSSEModalOpen,
    setIsSSEModalOpen,
    isSyncModalOpen,
    setIsSyncModalOpen,
    // floating console
    consoleOpen,
    setConsoleOpen,
    selectedConsoleJobId,
    setSelectedConsoleJobId,
    backgroundCronToast,
    setBackgroundCronToast,
    displayedJobs,
    setDisplayedJobs,
    removeDisplayedJob,
    // data
    status,
    cronLogs,
    isLoadingCron,
    isFetchingCron,
    recentCrons,
    orderedSchedules,
    catalogMaps,
    auditLogs,
    isLoadingAudit,
    isFetchingAudit,
    ingestedFiles,
    isLoadingIngested,
    schemaData,
    isLoadingSchema,
    // mutations / actions
    purgeMutation,
    setHasSyncedExtents,
    isPurgeOpen,
    setIsPurgeOpen,
  }
}
