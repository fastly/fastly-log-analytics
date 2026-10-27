'use client'

import type { QueryClient } from '@tanstack/react-query'
import { getApiBase, queryKeys } from '@/lib/api'
import type { components } from '@/types/api.generated'

// ── SSE Frame Parsing & Stream Request Builder ───────────────────────────────

export type SSEConnectionState = 'idle' | 'connecting' | 'open' | 'reconnecting'
export type SyncStatus = components['schemas']['SyncStatusResponse']

export interface EventStreamRequest {
  url: string
  method?: 'GET' | 'POST'
  body?: BodyInit
  headers?: Record<string, string>
  cache?: RequestCache
}

export interface BuildStreamRequestOptions {
  path: string
  serviceId?: string | null
  method?: 'GET' | 'POST'
  body?: BodyInit
  extraHeaders?: Record<string, string>
  cache?: RequestCache
}

export function buildStreamRequest({
  path,
  serviceId,
  method,
  body,
  extraHeaders,
  cache,
}: BuildStreamRequestOptions): EventStreamRequest {
  return {
    url: `${getApiBase()}${path}`,
    ...(method ? { method } : {}),
    ...(body !== undefined ? { body } : {}),
    headers: {
      Accept: 'text/event-stream',
      ...(serviceId ? { 'x-service-id': serviceId } : {}),
      ...extraHeaders,
    },
    ...(cache ? { cache } : {}),
  }
}

export function parseSSEFrames(buffer: string): { frames: string[]; remainder: string } {
  const parts = buffer.split(/\r\n\r\n|\n\n|\r\r/)
  return { frames: parts.slice(0, -1), remainder: parts.at(-1) ?? '' }
}

// ── Admin Stream Cache Appliers ──────────────────────────────────────────────

export function applySyncStatus(qc: QueryClient, serviceId: string | null, data: unknown): void {
  qc.setQueryData(queryKeys.syncStatus(serviceId), data as SyncStatus)
}

interface SystemMetricsPayload {
  health_snapshot?: unknown
  metric_history_1h?: unknown
  queries_summary?: unknown
  slow_queries_count?: unknown
  log_accounting?: unknown
  metadata_storage?: unknown
  system_jobs?: unknown
}

export function applySystemMetrics(qc: QueryClient, serviceId: string | null, data: unknown): void {
  if (!data || typeof data !== 'object') return
  const payload = data as SystemMetricsPayload
  if (payload.health_snapshot != null) qc.setQueryData(queryKeys.admin.healthSnapshot(), payload.health_snapshot)
  if (payload.metric_history_1h != null) qc.setQueryData(queryKeys.admin.metricHistoryBatch('1h'), payload.metric_history_1h)
  if (payload.queries_summary != null) qc.setQueryData(queryKeys.admin.overviewQueriesSummary(), payload.queries_summary)
  if (payload.slow_queries_count != null) qc.setQueryData(queryKeys.admin.overviewSlowQueriesCount(serviceId), payload.slow_queries_count)
  if (payload.log_accounting != null) qc.setQueryData(queryKeys.admin.overviewLogAccounting(serviceId), payload.log_accounting)
  if (payload.metadata_storage != null) qc.setQueryData(queryKeys.admin.metadataStorage(), payload.metadata_storage)
  if (payload.system_jobs != null) qc.setQueryData(queryKeys.systemJobs(), payload.system_jobs)
}

export function applyShare(qc: QueryClient, data: unknown): void {
  if (data === undefined) return
  qc.setQueryData(queryKeys.admin.shareLive(), data)
}

const INVALIDATION_COALESCE_MS = 100
const STORAGE_MUTATING_TASKS = new Set(['commit', 'optimize', 'expire_snapshots', 'metadata_cleanup'])
const SCHEMA_AFFECTING_TASKS = new Set(['sync', 'log_discovery', 'full_sync', 'commit'])

type InvalidationKey = 'table' | 'recent' | 'last-sync' | 'storage' | 'schedule' | 'audit' | 'ingested' | 'schema'

export interface CronRunsApplier {
  apply: (data: unknown) => void
  cleanup: () => void
}

export function makeCronRunsApplier(qc: QueryClient, serviceId: string | null): CronRunsApplier {
  const pending = new Set<InvalidationKey>()
  let flushTimer: ReturnType<typeof setTimeout> | null = null

  const flush = () => {
    flushTimer = null
    const due = new Set(pending)
    pending.clear()
    const opts = { cancelRefetch: false } as const
    if (due.has('table')) qc.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(serviceId) }, opts)
    if (due.has('recent')) qc.invalidateQueries({ queryKey: queryKeys.admin.cronLogsRecent(serviceId) }, opts)
    if (due.has('last-sync')) qc.invalidateQueries({ queryKey: queryKeys.lastSync(serviceId) }, opts)
    if (due.has('storage')) qc.invalidateQueries({ queryKey: queryKeys.admin.storageAll() }, opts)
    if (due.has('schedule')) qc.invalidateQueries({ queryKey: queryKeys.admin.cronSchedule(serviceId) }, opts)
    if (due.has('audit')) qc.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(serviceId) }, opts)
    if (due.has('ingested')) qc.invalidateQueries({ queryKey: queryKeys.admin.ingestedFiles(serviceId) }, opts)
    if (due.has('schema')) qc.invalidateQueries({ queryKey: queryKeys.admin.schema(serviceId) }, opts)
  }

  const schedule = (key: InvalidationKey) => {
    pending.add(key)
    if (flushTimer !== null) clearTimeout(flushTimer)
    flushTimer = setTimeout(flush, INVALIDATION_COALESCE_MS)
  }

  const apply = (data: unknown) => {
    schedule('recent')
    schedule('schedule')
    const payload = data && typeof data === 'object'
      ? (data as { task?: string; status?: string; started_at?: string; duration_s?: number | null })
      : null
    if (!payload) {
      schedule('table')
      return
    }
    const completed = payload.status && payload.status !== 'running'
    if (completed) schedule('table')
    if (payload.task === 'log_discovery') {
      if (typeof payload.started_at === 'string' && typeof payload.status === 'string') {
        qc.setQueryData(queryKeys.lastSync(serviceId), {
          started_at: payload.started_at,
          status: payload.status,
          duration_s: typeof payload.duration_s === 'number' ? payload.duration_s : null,
        })
      } else {
        schedule('last-sync')
      }
    }
    if (completed && payload.task && STORAGE_MUTATING_TASKS.has(payload.task)) {
      schedule('storage')
    }
    if (completed) {
      schedule('audit')
      schedule('ingested')
      if (payload.task && SCHEMA_AFFECTING_TASKS.has(payload.task)) {
        schedule('schema')
      }
    }
  }

  const cleanup = () => {
    if (flushTimer !== null) {
      clearTimeout(flushTimer)
      flushTimer = null
    }
    pending.clear()
  }

  return { apply, cleanup }
}

export function cronCacheBust(queryClient: QueryClient, _serviceId?: string | null): void {
  const keys = [
    queryKeys.admin.cronLogsAll(),
    queryKeys.admin.cronLogsRecentAll(),
    queryKeys.admin.storageAll(),
    queryKeys.admin.schemaAll(),
    queryKeys.lastSyncAll(),
    queryKeys.logExtentsAll(),
    queryKeys.syncStatusAll(),
    ['dashboard', 'aggregates'],
    ['dashboard', 'top-bots'],
    ['dashboard', 'raw'],
    ['dashboard', 'bundle'],
    queryKeys.bootstrap(),
  ]
  for (const queryKey of keys) {
    queryClient.invalidateQueries({ queryKey })
  }
}
