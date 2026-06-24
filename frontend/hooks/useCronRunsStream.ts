'use client'

import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { useServiceStream } from '@/hooks/useServiceStream'

/**
 * Subscribe to ``/api/cron-runs/stream`` and invalidate the React Query
 * keys that drive the Recent Cron Activity table, the floating-dock
 * toast, the "Last Sync" header badge, and the Iceberg admin panels.
 * The hook is fire-and-forget: it has no return value, and consumer
 * components don't need to know about it — they just read from the
 * same query keys as before.
 *
 * Four invalidations per event:
 *  1. ``['admin', 'cron-logs', activeServiceId, taskFilter, statusFilter]``
 *     — the main table on /logs (filter-agnostic invalidation; React
 *     Query's predicate matches every key that starts with the prefix).
 *  2. ``['admin', 'cron-logs-recent', activeServiceId]``
 *     — the 15s delta poll used for toast notifications.
 *  3. ``['last-sync', activeServiceId]`` — ONLY when ``event.task === 'sync'``,
 *     to refresh the header badge. We deliberately parse just the
 *     ``task`` field to avoid widening the wire contract.
 *  4. ``['admin', 'iceberg', ...]`` — when an Iceberg-mutating task
 *     completes (commit / optimize_iceberg / expire_snapshots /
 *     metadata_sync / metadata_cleanup). Replaces the 30s+60s polls
 *     IcebergStatus/IcebergCalendar used to drive themselves.
 *
 * Caller is responsible for gating ``enabled`` to admin sessions only —
 * ``/api/cron-runs/*`` is admin-only by middleware and the stream would
 * just 403-loop on analysts.
 */
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
const SCHEMA_AFFECTING_TASKS = new Set(['sync', 'full_sync', 'commit'])

type InvalidationKey =
  | 'table'
  | 'recent'
  | 'last-sync'
  | 'iceberg'
  | 'schedule'
  | 'audit'
  | 'ingested'
  | 'schema'

export function useCronRunsStream(enabled: boolean) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()

  // Per-hook coalesce state. useRef so the schedule()/flush() closure
  // captured by the useServiceStream onEvent callback always sees the
  // current Set/timer (the inline arrow passed to useServiceStream is
  // recreated each render, but we want the state pinned for the life
  // of the connection). Cleared on cleanup so a service switch / tab-
  // hide doesn't carry pending invalidations into the next mount.
  const pendingRef = useRef<Set<InvalidationKey>>(new Set())
  const flushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Clear coalesce state when the connection key changes (service
  // switch or stream disabled) — matches the previous per-effect
  // lifecycle even though the SSE loop now lives in useServiceStream.
  useEffect(() => {
    return () => {
      if (flushTimerRef.current !== null) {
        clearTimeout(flushTimerRef.current)
        flushTimerRef.current = null
      }
      pendingRef.current.clear()
    }
  }, [enabled, activeServiceId])

  const flush = () => {
    flushTimerRef.current = null
    const due = new Set(pendingRef.current)
    pendingRef.current.clear()
    if (due.has('table')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
    }
    if (due.has('recent')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs-recent', activeServiceId] })
    }
    if (due.has('last-sync')) {
      queryClient.invalidateQueries({ queryKey: ['last-sync', activeServiceId] })
    }
    if (due.has('iceberg')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'iceberg'] })
    }
    if (due.has('schedule')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'cron-schedule', activeServiceId] })
    }
    // The /logs Service History, Ingestion and Schema tabs have no
    // dedicated stream — they piggyback on these cron-complete events.
    // Prefix-match invalidation covers the per-filter child keys
    // (audit-logs is keyed by eventFilter). The queries are tab-gated
    // (`enabled: activeTab === X`), so a stale mark only triggers a
    // refetch when that tab is actually open; otherwise it just freshens
    // the cache for the next visit.
    if (due.has('audit')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'audit-logs', activeServiceId] })
    }
    if (due.has('ingested')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'ingested-files', activeServiceId] })
    }
    if (due.has('schema')) {
      queryClient.invalidateQueries({ queryKey: ['admin', 'schema', activeServiceId] })
    }
  }
  const schedule = (key: InvalidationKey) => {
    pendingRef.current.add(key)
    if (flushTimerRef.current === null) {
      flushTimerRef.current = setTimeout(flush, INVALIDATION_COALESCE_MS)
    }
  }

  useServiceStream(enabled, '/api/cron-runs/stream', (raw) => {
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

    try {
      const payload = JSON.parse(raw) as { task?: string; status?: string }
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
      // and flip back to "Xs ago" the moment it completes. The badge's
      // status-ternary renders text-vs-TimeAgo from lastSync.status —
      // when status='running' there IS no timer being shown, so no
      // "timer restart" concern. The matching a11y live-region
      // announcement in SyncStatusBadge.tsx also depends on this flip.
      if (payload.task === 'sync') {
        schedule('last-sync')
      }
      // Iceberg panels only need to refresh once the mutating task
      // FINISHED — running-state events don't change the snapshot/file
      // listings the panels show. Gating on `completed` mirrors the
      // table-invalidation rule above and keeps the panels stable
      // during long compactions.
      if (completed && payload.task && ICEBERG_MUTATING_TASKS.has(payload.task)) {
        schedule('iceberg')
      }
      // Service History (audit) + Ingestion tabs: a completed run is what
      // appends audit entries / ingests files, so refresh once it lands.
      // Running-state events don't change those listings, so gate on
      // `completed` to avoid a redundant refetch on start.
      if (completed) {
        schedule('audit')
        schedule('ingested')
        // Schema only shifts when ingestion introduces new columns —
        // limit to the data-mutating tasks rather than every cron.
        if (payload.task && SCHEMA_AFFECTING_TASKS.has(payload.task)) {
          schedule('schema')
        }
      }
    } catch {
      // Malformed payload — fall back to invalidating the
      // table so a running cron still refreshes if its event
      // shape is anything we couldn't parse. Skip last-sync:
      // without status/task we'd over-invalidate.
      schedule('table')
    }
  })
}
