/**
 * Per-channel apply logic for the multiplexed admin event stream.
 *
 * Extracted verbatim from the three single-purpose hooks that used to own
 * their own SSE connection (``useSyncStatusStream``,
 * ``useSystemMetricsStream``, ``useCronRunsStream``) so the new
 * ``useAdminEventStream`` demux can dispatch to the exact same React Query
 * cache behavior. Keeping these as pure functions (no React) means they're
 * unit-testable in isolation and there's a single source of truth for what
 * each channel does with its payload.
 */

import type { QueryClient } from '@tanstack/react-query'
import type { EventStreamRequest, SyncStatus } from '@/hooks/useEventStream'
import { getApiBase } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'

// ── sync-status ───────────────────────────────────────────────────────────────

/**
 * Push a full sync-status snapshot into the same key ``useSyncStatus``
 * reads (``['sync-status', activeServiceId]``); components re-render off
 * the shared cache. Mirrors the old ``useSyncStatusStream``.
 */
export function applySyncStatus(qc: QueryClient, serviceId: string | null, data: unknown): void {
  qc.setQueryData(queryKeys.syncStatus(serviceId), data as SyncStatus)
}

// ── system-metrics ────────────────────────────────────────────────────────────

interface SystemMetricsPayload {
  health_snapshot?: unknown
  metric_history_1h?: unknown
  queries_summary?: unknown
  slow_queries_count?: unknown
  log_accounting?: unknown
  metadata_storage?: unknown
  system_jobs?: unknown
}

/**
 * Fan the bundled metrics snapshot out into the seven admin-overview
 * slice keys the cards read. Only dispatch slices that are present
 * (non-null): a failed component sample arrives as ``null`` and we
 * deliberately keep the last-good cached value rather than blank the
 * card. Mirrors the old ``useSystemMetricsStream``.
 *
 * ``serviceId`` is the SSE connection's active service: the backend sampler
 * computes ``log_accounting`` and ``slow_queries_count`` for THAT service, so
 * those two slices are written to service-scoped keys matching what the cards
 * read (``['admin','overview','<slice>', serviceId]``). Without the suffix the
 * SSE push would land on a key no card reads (the cards are service-keyed) and
 * the freshness would be silently lost. ``queries_summary`` stays global (the
 * live-query registry is process-wide, not per service).
 */
export function applySystemMetrics(qc: QueryClient, serviceId: string | null, data: unknown): void {
  if (!data || typeof data !== 'object') return
  const payload = data as SystemMetricsPayload
  if (payload.health_snapshot != null) {
    qc.setQueryData(queryKeys.admin.healthSnapshot(), payload.health_snapshot)
  }
  if (payload.metric_history_1h != null) {
    qc.setQueryData(queryKeys.admin.metricHistoryBatch('1h'), payload.metric_history_1h)
  }
  if (payload.queries_summary != null) {
    qc.setQueryData(queryKeys.admin.overviewQueriesSummary(), payload.queries_summary)
  }
  if (payload.slow_queries_count != null) {
    qc.setQueryData(queryKeys.admin.overviewSlowQueriesCount(serviceId), payload.slow_queries_count)
  }
  if (payload.log_accounting != null) {
    qc.setQueryData(queryKeys.admin.overviewLogAccounting(serviceId), payload.log_accounting)
  }
  if (payload.metadata_storage != null) {
    qc.setQueryData(queryKeys.admin.metadataStorage(), payload.metadata_storage)
  }
  if (payload.system_jobs != null) {
    qc.setQueryData(queryKeys.systemJobs(), payload.system_jobs)
  }
}

// ── share ─────────────────────────────────────────────────────────────────────

/**
 * Push the lean share-live payload into the ``['admin','share','live']``
 * React Query cache the /admin/share page reads. Mirrors the old
 * ``useShareStream`` — folding it into the multiplex drops /admin/share from
 * two concurrent SSE connections over the H1 admin tunnel down to one.
 */
export function applyShare(qc: QueryClient, data: unknown): void {
  if (data === undefined) return
  qc.setQueryData(queryKeys.admin.shareLive(), data)
}

// ── cron-runs ─────────────────────────────────────────────────────────────────

// Trailing-edge coalesce window. Cron lifecycle emits ≥2 events per
// task (start + complete) and overlapping tasks (sync + commit + alerts
// firing within the same minute) produce a burst — without coalescing
// each event drives 2-3 invalidations × every matching React Query
// key, which on a busy /logs page shows up as a 6-10x refetch storm
// on /api/cron-runs during the first second after mount.
const INVALIDATION_COALESCE_MS = 100

// Cron tasks that mutate Iceberg snapshots/data files — see
// backend/cron/jobs/. Anything not in this set leaves the iceberg
// query cache alone.
const ICEBERG_MUTATING_TASKS = new Set([
  'commit',
  'optimize',
  'optimize_iceberg',
  'expire_snapshots',
  'metadata_sync',
  'metadata_cleanup',
])

// Tasks whose completion changes the Service History (audit log) and
// Ingestion-history tabs. Those rows are *written by* cron jobs (a sync
// ingests files and appends audit entries), so the cron-complete push is
// the correct live trigger — no dedicated backend stream needed. `sync`
// and `full_sync` add ingested files; most lifecycle tasks append an
// audit entry, so audit invalidation is unconditional-on-complete below.
const SCHEMA_AFFECTING_TASKS = new Set(['sync', 'log_discovery', 'full_sync', 'commit'])

type InvalidationKey =
  | 'table'
  | 'recent'
  | 'last-sync'
  | 'iceberg'
  | 'schedule'
  | 'audit'
  | 'ingested'
  | 'schema'

export interface CronRunsApplier {
  /** Apply one cron-run event payload (the demuxed ``data`` object). */
  apply: (data: unknown) => void
  /** Clear the pending coalesce set + flush timer. Call on connection-key
   *  change (service switch / stream disabled) so a pending invalidation
   *  doesn't carry into the next mount. */
  cleanup: () => void
}

/**
 * Build a stateful cron-runs applier bound to ``serviceId``. Owns its own
 * trailing-edge coalesce window (pending set + flush timer) internally so
 * the caller just invokes ``apply(data)`` per event and ``cleanup()`` on
 * teardown. Mirrors the old ``useCronRunsStream`` coalescer 1:1.
 */
export function makeCronRunsApplier(qc: QueryClient, serviceId: string | null): CronRunsApplier {
  const pending = new Set<InvalidationKey>()
  let flushTimer: ReturnType<typeof setTimeout> | null = null

  const flush = () => {
    flushTimer = null
    const due = new Set(pending)
    pending.clear()
    // Pass { cancelRefetch: false } so an SSE invalidation never aborts an
    // already in-flight request (which shows up as net::ERR_ABORTED in the
    // browser and wastes backend work when /api/cron-runs takes >100ms).
    const opts = { cancelRefetch: false } as const
    if (due.has('table')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(serviceId) }, opts)
    }
    if (due.has('recent')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.cronLogsRecent(serviceId) }, opts)
    }
    if (due.has('last-sync')) {
      qc.invalidateQueries({ queryKey: queryKeys.lastSync(serviceId) }, opts)
    }
    if (due.has('iceberg')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.icebergAll() }, opts)
    }
    if (due.has('schedule')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.cronSchedule(serviceId) }, opts)
    }
    // The /logs Service History, Ingestion and Schema tabs have no
    // dedicated stream — they piggyback on these cron-complete events.
    // Prefix-match invalidation covers the per-filter child keys
    // (audit-logs is keyed by eventFilter). The queries are tab-gated
    // (`enabled: activeTab === X`), so a stale mark only triggers a
    // refetch when that tab is actually open; otherwise it just freshens
    // the cache for the next visit.
    if (due.has('audit')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(serviceId) }, opts)
    }
    if (due.has('ingested')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.ingestedFiles(serviceId) }, opts)
    }
    if (due.has('schema')) {
      qc.invalidateQueries({ queryKey: queryKeys.admin.schema(serviceId) }, opts)
    }
  }

  const schedule = (key: InvalidationKey) => {
    pending.add(key)
    if (flushTimer !== null) {
      clearTimeout(flushTimer)
    }
    flushTimer = setTimeout(flush, INVALIDATION_COALESCE_MS)
  }

  const apply = (data: unknown) => {
    // The recent-rows delta-poll is cheap (since_id-filtered);
    // schedule it unconditionally so a "currently running"
    // row appears in toasts / the floating dock immediately.
    schedule('recent')
    // Cron-schedule tiles surface last_run / next_run / status per
    // task; any cron lifecycle event (start OR complete) advances
    // one of those fields. Unconditional schedule ensures the boxes
    // refresh immediately when a tile flips to "running" and again
    // when it lands. Coalesced with `recent` so a burst of events
    // costs a single refetch per flush window.
    schedule('schedule')

    const payload =
      data && typeof data === 'object'
        ? (data as { task?: string; status?: string; started_at?: string; duration_s?: number | null })
        : null
    if (!payload) {
      // Malformed payload — fall back to invalidating the table so a
      // running cron still refreshes if its event shape is anything we
      // couldn't parse. Skip last-sync: without status/task we'd
      // over-invalidate.
      schedule('table')
      return
    }

    const completed = payload.status && payload.status !== 'running'
    // The heavy 500-row table only needs re-fetching on COMPLETED
    // events — running-state events don't change row data the table
    // surfaces. _state.ts's running→completed effect ALSO invalidates
    // when the local copy sees a row land, so this gate doesn't drop
    // the eventual refresh, only the duplicate one on start.
    if (completed) {
      schedule('table')
    }
    // last-sync needs BOTH start and completion sync events so the
    // badge can flip to "Last Sync: running" the moment a sync starts
    // and flip back to "Xs ago" the moment it completes. When the SSE
    // event carries `started_at` + `status`, update the 1-row lastSync
    // cache directly in place to avoid redundant `/api/cron-runs` HTTP
    // refetches on every dashboard page load.
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
    // Iceberg panels only need to refresh once the mutating task
    // FINISHED — running-state events don't change the snapshot/file
    // listings the panels show.
    if (completed && payload.task && ICEBERG_MUTATING_TASKS.has(payload.task)) {
      schedule('iceberg')
    }
    // Service History (audit) + Ingestion tabs: a completed run is what
    // appends audit entries / ingests files, so refresh once it lands.
    if (completed) {
      schedule('audit')
      schedule('ingested')
      // Schema only shifts when ingestion introduces new columns —
      // limit to the data-mutating tasks rather than every cron.
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

// ── streamRequest ─────────────────────────────────────────────────────────────

interface BuildStreamRequestOptions {
  /** Path (with any query string) appended to `getApiBase()`. */
  path: string
  serviceId?: string | null
  method?: 'GET' | 'POST'
  body?: BodyInit
  /** Merged last, so callers can override any default header. */
  extraHeaders?: Record<string, string>
  cache?: RequestCache
}

/**
 * Single source of the URL + auth/scope headers for raw-fetch SSE streams
 * (useSSE, useServiceStream). String concat, never `new URL()`: an empty
 * `getApiBase()` (relative proxying) must not throw.
 */
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

/**
 * Shared SSE event-boundary parser.
 *
 * SSE specifies an empty line as the event separator, which the spec allows
 * as "\n\n", "\r\n\r\n", or "\r\r". sse-starlette emits CRLF; the previous
 * hand-rolled backend used LF. Splitting on "\n\n" alone misses every
 * sse-starlette event — the consumer's buffer just grows forever, no
 * frames are dispatched, and every useSSE/useServiceStream caller appears
 * stuck on "Waiting for stream...". The regex below covers all three
 * spec-allowed separators.
 *
 * Returns the complete frames from the buffer plus the remainder (any
 * trailing partial frame the caller should retain for the next chunk).
 */
export function parseSSEFrames(buffer: string): { frames: string[]; remainder: string } {
  const parts = buffer.split(/\r\n\r\n|\n\n|\r\r/)
  return { frames: parts.slice(0, -1), remainder: parts.at(-1) ?? "" }
}

// ── cronCacheBust ─────────────────────────────────────────────────────────────

export function cronCacheBust(queryClient: QueryClient, _serviceId?: string | null): void {
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsRecentAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.icebergAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.schemaAll() })

  queryClient.invalidateQueries({ queryKey: queryKeys.lastSyncAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.logExtentsAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.syncStatusAll() })

  queryClient.invalidateQueries({ queryKey: ['dashboard', 'aggregates'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'top-bots'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'raw'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'bundle'] })

  queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
}
