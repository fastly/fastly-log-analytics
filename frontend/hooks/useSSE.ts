'use client'

import {
  useState, useCallback, useRef, useEffect, useEffectEvent, useMemo, useSyncExternalStore,
  type Dispatch, type SetStateAction,
} from 'react'
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query'
import type { VisibilityState } from '@tanstack/react-table'
import { useBootstrapPending, useBootstrapSettled, useIsAnalyst } from '@/hooks/useBootstrap'
import { client, getApiBase, unwrapClient, queryKeys } from "@/lib/api";
import { formatDate, formatRelative, formatTimeAgo, getTimezoneAbbr, buildServiceHref, copyToClipboard, showToast, showToastWithAction } from "@/lib/utils";
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import type { Schema } from '@/types/api'
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

interface BuildStreamRequestOptions {
  path: string
  serviceId?: string | null
  method?: 'GET' | 'POST'
  body?: BodyInit
  extraHeaders?: Record<string, string>
  cache?: RequestCache
}

export function buildStreamRequest({
  path, serviceId, method, body, extraHeaders, cache,
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
const ICEBERG_MUTATING_TASKS = new Set(['commit', 'optimize', 'optimize_iceberg', 'expire_snapshots', 'metadata_sync', 'metadata_cleanup'])
const SCHEMA_AFFECTING_TASKS = new Set(['sync', 'log_discovery', 'full_sync', 'commit'])

type InvalidationKey = 'table' | 'recent' | 'last-sync' | 'iceberg' | 'schedule' | 'audit' | 'ingested' | 'schema'

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
    if (due.has('iceberg')) qc.invalidateQueries({ queryKey: queryKeys.admin.icebergAll() }, opts)
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
    if (!payload) { schedule('table'); return }
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
    if (completed && payload.task && ICEBERG_MUTATING_TASKS.has(payload.task)) schedule('iceberg')
    if (completed) {
      schedule('audit')
      schedule('ingested')
      if (payload.task && SCHEMA_AFFECTING_TASKS.has(payload.task)) schedule('schema')
    }
  }

  const cleanup = () => {
    if (flushTimer !== null) { clearTimeout(flushTimer); flushTimer = null }
    pending.clear()
  }

  return { apply, cleanup }
}

export function cronCacheBust(queryClient: QueryClient, _serviceId?: string | null): void {
  const keys = [
    queryKeys.admin.cronLogsAll(),
    queryKeys.admin.cronLogsRecentAll(),
    queryKeys.admin.icebergAll(),
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
  for (const queryKey of keys) queryClient.invalidateQueries({ queryKey })
}

// ── Event Stream Transport & Hooks ───────────────────────────────────────────

export class EventStreamHttpError extends Error {
  readonly status: number
  constructor(status: number) {
    super(`HTTP error! status: ${status}`)
    this.name = 'EventStreamHttpError'
    this.status = status
  }
}

interface RunEventStreamOptions {
  url: string
  method?: 'GET' | 'POST'
  body?: BodyInit
  headers?: Record<string, string>
  cache?: RequestCache
  signal: AbortSignal
  onOpen?: () => void
  onFrame: (frame: string) => void
  isActive?: () => boolean
  fetchImpl?: typeof fetch
}

export async function runEventStream(opts: RunEventStreamOptions): Promise<void> {
  const { url, method = 'GET', body, headers, cache, signal, onOpen, onFrame, isActive } = opts
  const fetchFn = opts.fetchImpl ?? fetch
  const response = await fetchFn(url, { method, ...(cache ? { cache } : {}), headers, body, signal })

  if (isActive && !isActive()) {
    try { response.body?.getReader().cancel().catch(() => {}) } catch { /* best-effort */ }
    return
  }
  if (!response.ok) throw new EventStreamHttpError(response.status)
  const reader = response.body?.getReader()
  if (!reader) throw new Error('Response body is null')

  const onAbort = () => { try { reader.cancel().catch(() => {}) } catch { /* best-effort */ } }
  signal.addEventListener('abort', onAbort, { once: true })
  onOpen?.()

  const decoder = new TextDecoder()
  let buffer = ''
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (isActive && !isActive()) { onAbort(); return }
      if (done) break
      if (value) {
        buffer += decoder.decode(value, { stream: true })
        const { frames, remainder } = parseSSEFrames(buffer)
        buffer = remainder
        for (const frame of frames) onFrame(frame)
      }
    }
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}

function requestKey(req: EventStreamRequest | null): string {
  if (!req) return ''
  return [req.method ?? 'GET', req.url, req.cache ?? '', JSON.stringify(req.headers ?? {}), typeof req.body === 'string' ? req.body : ''].join('\u0000')
}

export function useEventStream(
  enabled: boolean,
  request: EventStreamRequest | null,
  onEvent: (raw: string) => void,
  opts: { onStateChange?: (state: SSEConnectionState) => void } = {},
): { state: SSEConnectionState } {
  const [state, setState] = useState<SSEConnectionState>('idle')
  const emitEvent = useEffectEvent(onEvent)
  const emitState = useEffectEvent((next: SSEConnectionState) => opts.onStateChange?.(next))
  const latestRequest = useEffectEvent(() => request)
  const key = requestKey(enabled ? request : null)

  useEffect(() => {
    const req = latestRequest()
    if (!enabled || !req) { emitState('idle'); return }
    const abort = new AbortController()
    let cancelled = false
    let attempt = 0
    const safeSetState = (next: SSEConnectionState) => {
      if (!cancelled) { setState(next); emitState(next) }
    }

    const run = async () => {
      while (!cancelled) {
        safeSetState(attempt === 0 ? 'connecting' : 'reconnecting')
        try {
          await runEventStream({
            url: req.url, method: req.method, body: req.body, headers: req.headers, cache: req.cache,
            signal: abort.signal,
            onOpen: () => { attempt = 0; safeSetState('open') },
            onFrame: (frame) => {
              const dataLine = frame.split('\n').find((l) => l.startsWith('data:'))
              const raw = dataLine?.slice('data:'.length).trim()
              if (raw) emitEvent(raw)
            },
          })
        } catch (err) {
          if (cancelled) return
          if (err instanceof EventStreamHttpError && (err.status === 401 || err.status === 403)) {
            cancelled = true
            return
          }
        }
        if (cancelled) return
        safeSetState('reconnecting')
        await new Promise((resolve) => setTimeout(resolve, Math.min(30_000, 1_000 * 2 ** attempt++)))
      }
    }

    run()
    return () => { cancelled = true; abort.abort(); setState('idle') }
  }, [enabled, key])

  return { state }
}

type KeyFn = (serviceId: string) => readonly unknown[]

export function useSeededReady(serviceId: string | null | undefined, keyFns: readonly KeyFn[], readyWithoutService = false): boolean {
  const queryClient = useQueryClient()
  const [ready, setReady] = useState(false)
  useEffect(() => {
    if (serviceId) {
      for (const keyFn of keyFns) {
        const key = keyFn(serviceId)
        if (queryClient.getQueryData(key) === undefined) queryClient.setQueryData(key, null)
      }
    }
    const next = serviceId ? true : readyWithoutService
    queueMicrotask(() => setReady(next))
  }, [serviceId, queryClient, keyFns, readyWithoutService])
  return ready
}

const SEED_KEYS = [queryKeys.syncStatus] as const
const LAST_SYNC_SEED_KEYS = [queryKeys.lastSync] as const
type CronRunEntry = components['schemas']['CronRunEntry']
type LastSyncInfo = { [K in 'started_at' | 'status' | 'duration_s']: CronRunEntry[K] | null }

export function useSyncStatus() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const bootstrapPending = useBootstrapPending()
  const isAnalyst = useIsAnalyst()
  const ready = useSeededReady(activeServiceId, SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.syncStatus(activeServiceId),
    queryFn: async ({ signal }) =>
      (await unwrapClient(client.GET('/api/sync-status', { signal, params: { query: { skip_fos: true } } }))) as SyncStatus,
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

export function useLastSync() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const isAnalyst = useIsAnalyst()
  const bootstrapPending = useBootstrapPending()
  const ready = useSeededReady(activeServiceId, LAST_SYNC_SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.lastSync(activeServiceId),
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(
        client.GET('/api/cron-runs', { signal, params: { query: { task: 'log_discovery', per_page: 1, page: 1, skip_total: true } } }),
      )
      const e = data?.entries?.[0]
      return { started_at: e?.started_at ?? null, status: e?.status ?? null, duration_s: e?.duration_s ?? null } satisfies LastSyncInfo
    },
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 5 * 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Service & Admin Multiplex Streams ────────────────────────────────────────

function useServiceStoreHydrated(): boolean {
  const persist = useServiceStore.persist
  const subscribe = useCallback((onStoreChange: () => void) => persist?.onFinishHydration?.(onStoreChange) ?? (() => {}), [persist])
  const getSnapshot = () => persist?.hasHydrated?.() ?? true
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

export function useServiceStream(
  enabled: boolean,
  path: string,
  onEvent: (raw: string) => void,
  opts: { cache?: RequestCache; optionalService?: boolean } = {},
): { state: SSEConnectionState } {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const storeHydrated = useServiceStoreHydrated()
  const { cache, optionalService = false } = opts
  const awaitingHydration = optionalService && !storeHydrated
  const effectiveEnabled = enabled && (optionalService || !!activeServiceId) && !awaitingHydration
  const request: EventStreamRequest | null = effectiveEnabled ? buildStreamRequest({ path, serviceId: activeServiceId, cache }) : null
  return useEventStream(effectiveEnabled, request, onEvent)
}

export const adminStreamHooks = { useServiceStream, applySyncStatus, applySystemMetrics, applyShare, makeCronRunsApplier }

type BootstrapHeaderBadge = Schema<'ShareHeaderBadgeResponse'>
interface BootstrapShape { header_badge?: BootstrapHeaderBadge | null; [key: string]: unknown }
type BadgeStreamEvent = Schema<'ShareHeaderBadgeResponse'>

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()
  return useServiceStream(enabled, '/api/log-extents/stream', (raw) => {
    try {
      const payload = JSON.parse(raw) as BadgeStreamEvent
      queryClient.setQueryData<BootstrapShape | undefined>(queryKeys.bootstrap(), (prev) => {
        if (!prev) return prev
        return {
          ...prev,
          header_badge: {
            ...(prev.header_badge ?? {}),
            rum: payload.rum === undefined ? prev.header_badge?.rum : payload.rum === null ? null : { ...prev.header_badge?.rum, ...payload.rum },
            request: payload.request === undefined ? prev.header_badge?.request : payload.request === null ? null : { ...prev.header_badge?.request, ...payload.request },
            latest_log_at: payload.latest_log_at !== undefined ? payload.latest_log_at : prev.header_badge?.latest_log_at ?? null,
            local_rows: payload.local_rows !== undefined ? payload.local_rows : prev.header_badge?.local_rows ?? null,
          },
        }
      })
    } catch { /* ignore */ }
  })
}

export type AdminEventChannel = 'sync-status' | 'cron-runs' | 'system-metrics' | 'share' | 'celery-status'

export function useAdminEventStream(enabled: boolean, channels: AdminEventChannel[]) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()
  const bootstrapSettled = useBootstrapSettled()
  const channelsParam = useMemo(() => [...channels].sort().join(','), [channels])
  const cronApplierRef = useRef<CronRunsApplier | null>(null)

  useEffect(() => {
    if (!enabled || !channelsParam.split(',').includes('cron-runs')) return
    const applier = adminStreamHooks.makeCronRunsApplier(queryClient, activeServiceId)
    cronApplierRef.current = applier
    return () => {
      applier.cleanup()
      if (cronApplierRef.current === applier) cronApplierRef.current = null
    }
  }, [enabled, activeServiceId, channelsParam, queryClient])

  return adminStreamHooks.useServiceStream(
    enabled && bootstrapSettled && channelsParam.length > 0,
    `/api/admin/events/stream?channels=${channelsParam}`,
    (raw) => {
      let env: { channel?: string; data?: unknown }
      try { env = JSON.parse(raw) as { channel?: string; data?: unknown } } catch { return }
      if (!env || typeof env !== 'object') return
      switch (env.channel) {
        case 'sync-status': adminStreamHooks.applySyncStatus(queryClient, activeServiceId, env.data); break
        case 'system-metrics': adminStreamHooks.applySystemMetrics(queryClient, activeServiceId, env.data); break
        case 'share': adminStreamHooks.applyShare(queryClient, env.data); break
        case 'celery-status': queryClient.setQueryData(queryKeys.admin.celeryStatus(), env.data); break
        case 'cron-runs':
          if (!cronApplierRef.current) cronApplierRef.current = adminStreamHooks.makeCronRunsApplier(queryClient, activeServiceId)
          cronApplierRef.current.apply(env.data)
          break
        default: break
      }
    },
    { cache: 'no-store', optionalService: true },
  )
}

export function useAutoFlash<T = true>(resetMs: number = 2000) {
  const [value, setValue] = useState<T | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) { clearTimeout(timerRef.current); timerRef.current = null }
  }, [])
  const clear = useCallback(() => { clearTimer(); setValue(null) }, [clearTimer])
  const set = useCallback((nextValue: T | null) => { clearTimer(); setValue(nextValue) }, [clearTimer])
  useEffect(() => clearTimer, [clearTimer])
  const flash = useCallback((nextValue: T = true as unknown as T) => {
    clearTimer()
    setValue(nextValue)
    timerRef.current = setTimeout(() => { timerRef.current = null; setValue(null) }, resetMs)
  }, [clearTimer, resetMs])
  return { active: value !== null, value, flash, set, clear }
}

export function useCopyToClipboard(resetMs: number = 1500) {
  const { active: copied, flash } = useAutoFlash(resetMs)
  const copy = useCallback(async (value: string) => { if (await copyToClipboard(value)) flash() }, [flash])
  return { copied, copy }
}

interface DeferredDeleteOptions {
  id: string
  toastMessage: string
  performDelete: () => Promise<void>
  errorMessage: string | ((error: unknown) => string)
  durationMs?: number
}

export function useDeferredDeleteWithUndo() {
  const [pendingDeleteIds, setPendingDeleteIds] = useState<Set<string>>(() => new Set())
  const pendingTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

  useEffect(() => {
    const timers = pendingTimersRef.current
    return () => { for (const [, t] of timers) clearTimeout(t); timers.clear() }
  }, [])

  const scheduleDelete = useCallback(({ id, toastMessage, performDelete, errorMessage, durationMs = 5500 }: DeferredDeleteOptions) => {
    setPendingDeleteIds((prev) => new Set(prev).add(id))
    const timer = setTimeout(async () => {
      pendingTimersRef.current.delete(id)
      try {
        await performDelete()
      } catch (err) {
        setPendingDeleteIds((prev) => { const next = new Set(prev); next.delete(id); return next })
        showToast(typeof errorMessage === 'function' ? errorMessage(err) : errorMessage, 'error')
      }
    }, durationMs)
    pendingTimersRef.current.set(id, timer)
    showToastWithAction(toastMessage, {
      actionLabel: 'Undo',
      onAction: () => {
        const t = pendingTimersRef.current.get(id)
        if (t) { clearTimeout(t); pendingTimersRef.current.delete(id) }
        setPendingDeleteIds((prev) => { const next = new Set(prev); next.delete(id); return next })
      },
      durationMs,
    })
  }, [])
  return { pendingDeleteIds, scheduleDelete }
}

// ── Imperative SSE Hook & UI State Hooks ─────────────────────────────────────

export type SSEStatus = 'idle' | 'streaming' | 'done' | 'error'
export interface SSELine { type?: string; message?: string; _id?: number; [key: string]: unknown }

export function useSSE({ maxLines }: { maxLines?: number } = {}) {
  const [lines, setLines] = useState<SSELine[]>([])
  const [status, setStatus] = useState<SSEStatus>('idle')
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const requestIdRef = useRef<number>(0)
  const mountedRef = useRef<boolean>(true)
  const lineSeqRef = useRef<number>(0)

  const stop = useCallback(() => {
    if (abortRef.current) {
      try { abortRef.current.abort() } catch { /* best-effort */ }
      abortRef.current = null
    }
    requestIdRef.current++
    if (mountedRef.current) setStatus('idle')
  }, [])

  const start = useCallback(async (urlPath: string, body?: Record<string, unknown>, extraHeaders?: Record<string, string>) => {
    stop()
    const serviceId = useServiceStore.getState().activeServiceId
    let finalUrlPath = urlPath
    if (serviceId && !finalUrlPath.includes('service=') && !finalUrlPath.includes('/services/')) {
      finalUrlPath = buildServiceHref(finalUrlPath, serviceId)
    }
    if (mountedRef.current) {
      setLines([])
      setStatus('streaming')
      setError(null)
      lineSeqRef.current = 0
    }
    const currentReqId = requestIdRef.current
    const abort = new AbortController()
    abortRef.current = abort
    const isActive = () => currentReqId === requestIdRef.current && mountedRef.current

    try {
      const request = buildStreamRequest({
        path: finalUrlPath, serviceId, method: body ? 'POST' : 'GET',
        body: body ? JSON.stringify(body) : undefined, cache: 'no-store',
        extraHeaders: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', ...extraHeaders },
      })
      await runEventStream({
        ...request, signal: abort.signal, isActive,
        onFrame: (frame) => {
          const part = frame.trim()
          if (!part.startsWith('data:')) return
          const dataStr = part.replace(/^data:\s*/, '')
          let data: SSELine
          try { data = JSON.parse(dataStr) as SSELine } catch (e) { console.error('Failed to parse SSE message:', dataStr, e); return }
          let finalStatus: SSEStatus | null = null
          let finalError: string | null = null
          if (data.type === 'done') finalStatus = 'done'
          else if (data.type === 'error') {
            finalStatus = 'error'
            finalError = typeof data.message === 'string' ? data.message : 'Unknown error'
          }
          if (mountedRef.current) {
            const stamped = { ...data, _id: ++lineSeqRef.current }
            setLines((prev) => {
              const next = [...prev, stamped]
              return maxLines && next.length > maxLines ? next.slice(next.length - maxLines) : next
            })
            if (finalStatus) {
              setStatus(finalStatus)
              if (finalError) setError(finalError)
            }
          }
        },
      })
      if (!isActive()) return
      setStatus((prev) => {
        if (prev === 'streaming') {
          setError('Connection closed unexpectedly before completion.')
          return 'error'
        }
        return prev
      })
    } catch (err) {
      if (!isActive()) return
      setStatus('error')
      if (err instanceof TypeError && (err.message === 'Failed to fetch' || err.message.includes('network error'))) {
        setError('Backend API unreachable. Ensure the FastAPI server is running (usually on port 8000).')
      } else {
        setError(err instanceof Error ? err.message : 'Unknown error')
      }
    } finally {
      if (abortRef.current === abort) abortRef.current = null
    }
  }, [stop, maxLines])

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      if (abortRef.current) {
        try { abortRef.current.abort() } catch { /* best-effort */ }
        abortRef.current = null
      }
    }
  }, [])

  const reset = useCallback(() => {
    stop()
    if (mountedRef.current) { setLines([]); setStatus('idle'); setError(null) }
  }, [stop])

  return { lines, status, isDone: status === 'done' || status === 'error', error, start, stop, reset }
}

export function useTimezone(): string {
  return useTimezoneStore((s) => s.timezone)
}

export function useDateFormat() {
  const timezone = useTimezoneStore((state) => state.timezone)
  const short = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm a'), [timezone])
  const full = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm:ss a'), [timezone])
  const time = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm a'), [timezone])
  const timeMs = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm:ss.SSS a'), [timezone])
  const abbr = useCallback(() => getTimezoneAbbr(new Date(), timezone), [timezone])
  const relative = useCallback((date: string | Date) => formatRelative(date), [])
  const timeAgo = useCallback((date: string | Date) => formatTimeAgo(typeof date === 'string' ? date : date.toISOString()), [])
  const format = useCallback((date: string | Date, pattern: string) => formatDate(date, timezone, pattern), [timezone])
  return useMemo(() => ({ short, full, time, timeMs, abbr, relative, timeAgo, format }), [short, full, time, timeMs, abbr, relative, timeAgo, format])
}

let _now = Date.now()
const _listeners = new Set<() => void>()
let _interval: ReturnType<typeof setInterval> | null = null

function _subscribe(cb: () => void): () => void {
  if (_listeners.size === 0) _now = Date.now()
  _listeners.add(cb)
  if (_interval === null) {
    _interval = setInterval(() => { _now = Date.now(); for (const l of _listeners) l() }, 1000)
  }
  return () => {
    _listeners.delete(cb)
    if (_listeners.size === 0 && _interval !== null) { clearInterval(_interval); _interval = null }
  }
}

const _getSnapshot = (): number => _now
const _getServerSnapshot = (): number => 0
const _noopUnsubscribe = () => {}
const _noopSubscribe = (): (() => void) => _noopUnsubscribe

export function useNowMs(enabled = true): number {
  return useSyncExternalStore(enabled ? _subscribe : _noopSubscribe, _getSnapshot, _getServerSnapshot)
}

export function useElapsedTime(startedAt: string | null | undefined): number {
  const nowMs = useNowMs(Boolean(startedAt))
  if (!startedAt) return 0
  return Math.max(0, (nowMs - new Date(startedAt).getTime()) / 1000)
}

export function useDerivedState<T>(source: T): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(source)
  const [seen, setSeen] = useState(source)
  if (!Object.is(seen, source)) { setSeen(source); setState(source) }
  return [state, setState]
}

export function useOpenGeneration(open: boolean): number {
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) { setWasOpen(open); if (open) setGeneration((g) => g + 1) }
  return generation
}

export function useColumnVisibility(initial?: VisibilityState) {
  const [visibility, setVisibility] = useState<VisibilityState>(initial ?? {})
  const handleChange = useCallback((id: string, vis: boolean) => setVisibility((prev) => ({ ...prev, [id]: vis })), [])
  return [visibility, setVisibility, handleChange] as const
}

const storageListeners = new Set<() => void>()
function subscribeStorage(onChange: () => void): () => void {
  storageListeners.add(onChange)
  window.addEventListener('storage', onChange)
  return () => { storageListeners.delete(onChange); window.removeEventListener('storage', onChange) }
}
function readStorage(key: string): string | null {
  try { return window.localStorage.getItem(key) } catch { return null }
}

export function useLocalStorageString(key: string): readonly [string | null, (value: string) => void] {
  const value = useSyncExternalStore(subscribeStorage, () => readStorage(key), () => null)
  const write = useCallback((next: string) => {
    try { window.localStorage.setItem(key, next) } catch { /* ignore */ }
    storageListeners.forEach((notify) => notify())
  }, [key])
  return [value, write] as const
}
