'use client'

import * as React from 'react'

import type {
  ActiveOrPromotedRow,
  AttributionKind,
  CompletedRow,
  DbFilter,
  GroupedCompletedRow,
  SnapshotResponse,
} from '../_types'

/** Anything that completed in the last N seconds gets promoted into the
 *  Active section as a faded row with the outcome badge. Without this the
 *  Active list reads empty on typical traffic (p50 query duration is sub-ms;
 *  even 300ms polling misses every single one). */
const JUST_FINISHED_WINDOW_S = 10

/** Hard cap on the Notable Slow Queries list. Server-side history is
 *  bounded to 200 (deque maxlen); 30 fills several screens without making
 *  the table feel like a log dump. */
const SLOW_QUERIES_MAX = 30

/**
 * Derived views over a `/api/admin/queries?include_completed=true` snapshot.
 *
 * Splits four related memos out of the page component so the orchestrator
 * doesn't carry ~70 lines of filter/dedupe/sort logic. Pure transformation;
 * no fetching of its own.
 *
 * Returns:
 * - `justFinished` — completed rows in the last 10 s, used to promote rows
 *   into the Active section.
 * - `filteredActive` — active rows + just-finished promotions, deduped on
 *   `query_id` and filtered by kind/db/search.
 * - `completed` — full completed list filtered by db.
 * - `slowQueries` — completed rows above the threshold, db-filtered,
 *   sorted slowest-first, capped at 30.
 *
 * When ``groupCrons`` is true (default), rows sharing the same
 * ``attribution.cron_run_id`` collapse to a single representative row —
 * the longest-running one — with ``_groupedCount`` set to the original
 * group size. Cuts table noise during a heavy cron tick without losing
 * information (toggle off to see them all).
 */
export function useFilteredActive({
  snapshot,
  search,
  kindFilter,
  dbFilter,
  slowThresholdMs,
  groupCrons,
}: {
  snapshot: SnapshotResponse | undefined
  search: string
  kindFilter: AttributionKind | 'all'
  dbFilter: DbFilter
  slowThresholdMs: number
  groupCrons: boolean
}): {
  justFinished: CompletedRow[]
  filteredActive: ActiveOrPromotedRow[]
  completed: GroupedCompletedRow[]
  slowQueries: GroupedCompletedRow[]
} {
  const justFinished = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const cutoff = Date.now() / 1000 - JUST_FINISHED_WINDOW_S
    return all.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot])

  const slowQueries = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const filtered = all
      .filter((c) => c.duration_ms >= slowThresholdMs)
      .filter((c) => dbFilter === 'all' || c.db_type === dbFilter)
    const grouped = groupCrons ? collapseCronRunsCompleted(filtered) : filtered
    return [...grouped].sort((a, b) => b.duration_ms - a.duration_ms).slice(0, SLOW_QUERIES_MAX)
  }, [snapshot, slowThresholdMs, dbFilter, groupCrons])

  const filteredActive = React.useMemo(() => {
    const active: ActiveOrPromotedRow[] = (snapshot?.active ?? []).map((r) => ({ ...r }))
    const justRows: ActiveOrPromotedRow[] = justFinished.map((c) => ({
      query_id: c.query_id,
      db_type: c.db_type,
      sql_preview: c.sql_preview,
      sql: c.sql,
      sql_len: c.sql_len,
      attribution: c.attribution,
      service_id: c.service_id,
      started_at_utc: c.started_at_utc,
      duration_ms: c.duration_ms,
      cancellable: false,
      cancelled_at: null,
      _completed: c,
    }))
    // Dedupe on query_id — a row can theoretically appear in both lists
    // for one poll cycle as it transitions from active to completed.
    const seen = new Set<number>()
    const combined: ActiveOrPromotedRow[] = []
    for (const r of [...active, ...justRows]) {
      if (seen.has(r.query_id)) continue
      seen.add(r.query_id)
      combined.push(r)
    }
    const q = search.trim().toLowerCase()
    const filtered = combined.filter((r) => {
      if (kindFilter !== 'all' && r.attribution.kind !== kindFilter) return false
      if (dbFilter !== 'all' && r.db_type !== dbFilter) return false
      if (!q) return true
      return (
        r.sql_preview.toLowerCase().includes(q) ||
        r.attribution.caller_qualname.toLowerCase().includes(q) ||
        r.attribution.caller_file.toLowerCase().includes(q) ||
        r.attribution.label.toLowerCase().includes(q)
      )
    })
    return groupCrons ? collapseCronRunsActive(filtered) : filtered
  }, [snapshot, justFinished, search, kindFilter, dbFilter, groupCrons])

  const completed = React.useMemo<GroupedCompletedRow[]>(() => {
    const raw = snapshot?.completed ?? []
    const filtered = dbFilter === 'all' ? raw : raw.filter((c) => c.db_type === dbFilter)
    return groupCrons ? collapseCronRunsCompleted(filtered) : filtered
  }, [snapshot, dbFilter, groupCrons])

  return { justFinished, filteredActive, completed, slowQueries }
}

/** Collapse Active rows by ``cron_run_id``: keep the longest-running row in
 *  each run, tag it with the original group size. Non-cron rows and rows
 *  without a ``cron_run_id`` pass through untouched. Stable ordering — the
 *  representative row keeps the position of the longest-running sibling. */
function collapseCronRunsActive(rows: ActiveOrPromotedRow[]): ActiveOrPromotedRow[] {
  const groups = new Map<string, ActiveOrPromotedRow[]>()
  const out: ActiveOrPromotedRow[] = []
  const groupIndex = new Map<string, number>() // first-seen position
  for (const r of rows) {
    const runId = r.attribution.cron_run_id
    if (r.attribution.kind !== 'cron' || !runId) {
      out.push(r)
      continue
    }
    if (!groups.has(runId)) {
      groups.set(runId, [])
      groupIndex.set(runId, out.length)
      out.push(r) // placeholder; replaced below
    }
    groups.get(runId)!.push(r)
  }
  for (const [runId, members] of groups) {
    if (members.length === 1) {
      out[groupIndex.get(runId)!] = members[0]
      continue
    }
    // Pick the longest-running. Live (`!_completed`) rows beat promoted
    // ones — a still-running query is the most actionable representative.
    const sorted = [...members].sort((a, b) => {
      const liveDelta = (a._completed ? 1 : 0) - (b._completed ? 1 : 0)
      if (liveDelta !== 0) return liveDelta
      return b.duration_ms - a.duration_ms
    })
    out[groupIndex.get(runId)!] = { ...sorted[0], _groupedCount: members.length }
  }
  return out
}

/** Same idea for completed rows: collapse by ``cron_run_id``, keep the
 *  longest, tag with original group size. Used by ``completed`` and
 *  ``slowQueries`` views so a single noisy cron tick doesn't flood either
 *  list. */
function collapseCronRunsCompleted(rows: CompletedRow[]): GroupedCompletedRow[] {
  const groups = new Map<string, CompletedRow[]>()
  const out: GroupedCompletedRow[] = []
  const groupIndex = new Map<string, number>()
  for (const r of rows) {
    const runId = r.attribution.cron_run_id
    if (r.attribution.kind !== 'cron' || !runId) {
      out.push(r)
      continue
    }
    if (!groups.has(runId)) {
      groups.set(runId, [])
      groupIndex.set(runId, out.length)
      out.push(r)
    }
    groups.get(runId)!.push(r)
  }
  for (const [runId, members] of groups) {
    if (members.length === 1) {
      out[groupIndex.get(runId)!] = members[0]
      continue
    }
    const sorted = [...members].sort((a, b) => b.duration_ms - a.duration_ms)
    out[groupIndex.get(runId)!] = { ...sorted[0], _groupedCount: members.length }
  }
  return out
}
