'use client'

import React, { useCallback, useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { parseAsString, useQueryState } from 'nuqs'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { showToastWithAction, formatCount } from '@/lib/utils'
import { useSSE, useSyncStatus } from '@/hooks/useSSE'
import { useEffectiveServiceId, useLogFieldsCatalog } from '@/hooks/useBootstrap'
import type { components } from '@/types/api.generated'

export type CronRun = components['schemas']['CronRunEntry']

export type CatalogMaps = {
  groups: Record<string, { label: string; description: string }>
  fields: Record<string, { label: string; description: string }>
}

/**
 * Accumulate running and completed jobs, pruning older runs of the same task type to prevent
 * historical clutter: keep the active running job and the single most recent completed job
 * (last run) for each task. Returns `prev` untouched when nothing changed.
 */
function mergeRunningJobs(prev: CronRun[], entries: CronRun[]): CronRun[] {
  const runningTasks = new Set(entries.map((j) => j.task))
  const runningIds = new Set(entries.map((j) => j.id))
  const filtered = prev.filter((j) => runningIds.has(j.id) || !runningTasks.has(j.task))

  const jobsByTask: Record<string, CronRun[]> = {}
  for (const job of filtered) {
    ;(jobsByTask[job.task] ??= []).push(job)
  }

  const pruned: CronRun[] = []
  for (const taskJobs of Object.values(jobsByTask)) {
    const running = taskJobs.filter((j) => runningIds.has(j.id))
    const completed = taskJobs.filter((j) => !runningIds.has(j.id))
    if (running.length > 0) {
      pruned.push(...running.map((j) => ({ ...j, status: 'running' })))
    }
    if (completed.length > 0) {
      const latestCompleted = completed.reduce((max, job) => (job.id > max.id ? job : max), completed[0])
      pruned.push({ ...latestCompleted, status: 'completed' })
    }
  }

  const prunedIds = new Set(pruned.map((j) => j.id))
  const nextJobs = [
    ...pruned,
    ...entries.filter((j) => !prunedIds.has(j.id)).map((j) => ({ ...j, status: 'running' })),
  ]

  const isUnchanged =
    prev.length === nextJobs.length &&
    prev.every((pj) => {
      const nj = nextJobs.find((j) => j.id === pj.id)
      return nj && nj.status === pj.status && nj.duration_s === pj.duration_s && nj.rows_ingested === pj.rows_ingested
    })

  if (isUnchanged) return prev
  return nextJobs
}

/**
 * Orchestrates all state, data queries, and side-effects for the
 * Logs / Data Management page. Returns the entire bag of values
 * needed by the rendering shell + its section components.
 */
export function useLogsPageState(initialTab?: string) {
  const activeServiceId = useEffectiveServiceId() ?? null
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
    const taskName = run.task
    const label =
      run.status === 'running'
        ? 'Background Sync Started'
        : run.status === 'error'
          ? 'Background Sync Failed'
          : 'Background Sync Completed'
    const details = [
      taskName,
      run.status !== 'running' && run.rows_ingested != null ? `${formatCount(run.rows_ingested)} rows` : null,
      run.status !== 'running' && run.duration_s != null ? `${run.duration_s.toFixed(1)}s` : null,
    ]
      .filter(Boolean)
      .join(' · ')

    showToastWithAction(`${label} (${details})`, {
      actionLabel: 'View Console Logs',
      kind: run.status === 'error' ? 'error' : run.status === 'running' ? 'info' : 'success',
      durationMs: 8000,
      onAction: () => {
        setSelectedConsoleJobId(run.id)
        setConsoleOpen(true)
      },
    })
  }, [])

  const maxSeenIdRef = React.useRef<number | null>(null)
  useEffect(() => {
    maxSeenIdRef.current = null
  }, [activeServiceId])

  const { data: status } = useSyncStatus()
  const { data: cronLogs, isLoading: isLoadingCron, isFetching: isFetchingCron } = useQuery({
    queryKey: queryKeys.admin.cronLogs(activeServiceId, taskFilter, statusFilter),
    queryFn: ({ signal }) =>
      unwrapClient(
        client.GET('/api/cron-runs', {
          signal,
          params: {
            query: {
              page: 1,
              per_page: 500,
              task: taskFilter === 'all' ? undefined : taskFilter,
              status: statusFilter === 'all' ? undefined : statusFilter,
            },
          },
        }),
      ),
    enabled: !!activeServiceId && activeTab === 'cron',
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    staleTime: 30_000,
  })

  const { data: recentCrons } = useQuery({
    queryKey: queryKeys.admin.cronLogsRecent(activeServiceId),
    queryFn: ({ signal }) =>
      unwrapClient(
        client.GET('/api/cron-runs', {
          signal,
          params: {
            query: {
              page: 1,
              per_page: 10,
              since_id: maxSeenIdRef.current != null ? Math.max(0, maxSeenIdRef.current - 1) : undefined,
            },
          },
        }),
      ),
    enabled: !!activeServiceId,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    staleTime: 15_000,
  })

  const runningCrons = React.useMemo(
    () => ({ entries: (recentCrons?.entries || []).filter((e) => e.status === 'running') }),
    [recentCrons?.entries],
  )

  const prevRunningCount = React.useRef(0)
  React.useEffect(() => {
    const count = runningCrons?.entries?.length || 0
    if (prevRunningCount.current > 0 && count < prevRunningCount.current) {
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsAll() })
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.status() })
    }
    prevRunningCount.current = count
  }, [runningCrons?.entries?.length, queryClient])

  const [displayedJobs, setDisplayedJobs] = useState<CronRun[]>([])
  const runningEntries = runningCrons?.entries
  const [seenRunningEntries, setSeenRunningEntries] = useState<typeof runningEntries | undefined>(undefined)
  if (runningEntries && runningEntries !== seenRunningEntries) {
    setSeenRunningEntries(runningEntries)
    setDisplayedJobs((prev) => mergeRunningJobs(prev, runningEntries))
  }

  const removeDisplayedJob = useCallback((id: number) => {
    setDisplayedJobs((prev) => prev.filter((j) => j.id !== id))
  }, [])

  useEffect(() => {
    if (!recentCrons?.entries?.length) return
    const maxId = Math.max(...recentCrons.entries.map((e) => e.id))
    const enqueueJob = (run: CronRun) => {
      setDisplayedJobs((prev) => (prev.some((j) => j.id === run.id) ? prev : [...prev, { ...run, status: run.status }]))
      notifyCronRun(run)
    }
    if (maxSeenIdRef.current === null) {
      maxSeenIdRef.current = maxId
      recentCrons.entries.filter((e) => e.status === 'running').forEach(enqueueJob)
      return
    }
    if (maxId > maxSeenIdRef.current) {
      const newRuns = recentCrons.entries.filter((e) => e.id > (maxSeenIdRef.current || 0))
      maxSeenIdRef.current = maxId
      newRuns.forEach(enqueueJob)
    }
  }, [recentCrons?.entries, notifyCronRun])

  if (displayedJobs.length > 0) {
    if (selectedConsoleJobId === null || !displayedJobs.some((j) => j.id === selectedConsoleJobId)) {
      setSelectedConsoleJobId(displayedJobs[0].id)
    }
  } else if (selectedConsoleJobId !== null || consoleOpen) {
    setSelectedConsoleJobId(null)
    setConsoleOpen(false)
  }

  const { data: cronSchedule, isLoading: isLoadingSchedule } = useQuery({
    queryKey: queryKeys.admin.cronSchedule(activeServiceId),
    queryFn: ({ signal }) => unwrapClient(client.GET('/api/cron-schedule', { signal })),
    enabled: !!activeServiceId && activeTab === 'cron',
    staleTime: 10_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  const orderedSchedules = React.useMemo(() => {
    const TASK_PRIORITY: Record<string, number> = {
      log_discovery: 1,
      rum_sync: 2,
      rum_discovery: 2,
      alerts: 3,
      commit: 4,
      rum_commit: 5,
      optimize: 6,
      local_compact: 7,
      metadata_cleanup: 8,
      expire_snapshots: 9,
      full_sync: 10,
      gap_heal: 11,
      ngwaf_sync: 12,
      insights_prewarmer: 13,
      rollup_compact_daily: 14,
      rollup_hour_heal: 15,
    }
    const source = cronSchedule?.schedules ?? []
    const hasNgwaf = !!status?.ngwaf_workspace_id
    const filtered = source.filter((s) => s.task !== 'ngwaf_sync' || hasNgwaf)
    const sorted = [...filtered].sort(
      (a, b) => (TASK_PRIORITY[a.task] ?? 999) - (TASK_PRIORITY[b.task] ?? 999) || a.task.localeCompare(b.task),
    )
    return sorted.map((schedule) => ({
      task: schedule.task,
      activeJob: displayedJobs.find((j) => j.task === schedule.task && j.status === 'running'),
      schedule,
    }))
  }, [cronSchedule?.schedules, displayedJobs, status?.ngwaf_workspace_id])

  const { data: catalog } = useLogFieldsCatalog()
  const catalogMaps = React.useMemo<CatalogMaps>(() => {
    if (!catalog) return { groups: {}, fields: {} }
    const groups: Record<string, { label: string; description: string }> = {}
    const fields: Record<string, { label: string; description: string }> = {}
    catalog.groups?.forEach((g) => {
      groups[g.id === null ? 'null' : String(g.id)] = { label: g.label, description: g.description }
    })
    catalog.fields?.forEach((f) => {
      fields[f.id] = { label: f.label, description: f.description }
    })
    return { groups, fields }
  }, [catalog])

  const { data: auditLogs, isLoading: isLoadingAudit, isFetching: isFetchingAudit } = useQuery({
    queryKey: queryKeys.admin.auditLogs(activeServiceId, eventFilter),
    queryFn: ({ signal }) =>
      unwrapClient(
        client.GET('/api/audit-logs', {
          signal,
          params: { query: { page: 1, per_page: 500, event_type: eventFilter === 'all' ? undefined : eventFilter } },
        }),
      ),
    enabled: !!activeServiceId && activeTab === 'service_history',
    staleTime: 60_000,
  })

  const { data: ingestedFiles, isLoading: isLoadingIngested } = useQuery({
    queryKey: queryKeys.admin.ingestedFiles(activeServiceId),
    queryFn: ({ signal }) => unwrapClient(client.GET('/api/admin/ingested-files', { signal })),
    enabled: !!activeServiceId && activeTab === 'ingestion',
    staleTime: 60_000,
  })

  const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
    queryKey: queryKeys.admin.schema(activeServiceId),
    queryFn: ({ signal }) => unwrapClient(client.GET('/api/schema', { signal })),
    enabled: !!activeServiceId && activeTab === 'schema',
    staleTime: 60_000,
  })

  const purgeMutation = useMutation({
    mutationFn: async () => {
      await unwrapClient(client.DELETE('/api/cron-runs', {}))
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })
      setIsPurgeOpen(false)
    },
  })

  const handleTabChange = useCallback(
    (value: string) => {
      void setActiveTab(value)
      if (value === 'storage') {
        queryClient.invalidateQueries({ queryKey: queryKeys.admin.storage(activeServiceId) })
        queryClient.invalidateQueries({ queryKey: queryKeys.admin.storageTree(activeServiceId) })
      } else if (value === 'raw') {
        queryClient.invalidateQueries({ queryKey: queryKeys.admin.rawTree(activeServiceId) })
      }
    },
    [activeServiceId, queryClient, setActiveTab],
  )

  return {
    activeServiceId,
    activeTab,
    handleTabChange,
    taskFilter,
    setTaskFilter,
    statusFilter,
    setStatusFilter,
    eventFilter,
    setEventFilter,
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
    consoleOpen,
    setConsoleOpen,
    selectedConsoleJobId,
    setSelectedConsoleJobId,
    displayedJobs,
    setDisplayedJobs,
    removeDisplayedJob,
    status,
    cronLogs,
    isLoadingCron,
    isFetchingCron,
    recentCrons,
    orderedSchedules,
    isLoadingSchedule,
    catalogMaps,
    auditLogs,
    isLoadingAudit,
    isFetchingAudit,
    ingestedFiles,
    isLoadingIngested,
    schemaData,
    isLoadingSchema,
    purgeMutation,
    isPurgeOpen,
    setIsPurgeOpen,
  }
}
