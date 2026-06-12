'use client'

import * as React from 'react'

import type {
  ActiveOrPromotedRow,
  AttributionKind,
  CompletedRow,
  DbFilter,
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
 */
export function useFilteredActive({
  snapshot,
  search,
  kindFilter,
  dbFilter,
  slowThresholdMs,
}: {
  snapshot: SnapshotResponse | undefined
  search: string
  kindFilter: AttributionKind | 'all'
  dbFilter: DbFilter
  slowThresholdMs: number
}): {
  justFinished: CompletedRow[]
  filteredActive: ActiveOrPromotedRow[]
  completed: CompletedRow[]
  slowQueries: CompletedRow[]
} {
  const justFinished = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const cutoff = Date.now() / 1000 - JUST_FINISHED_WINDOW_S
    return all.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot])

  const slowQueries = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    return [...all]
      .filter((c) => c.duration_ms >= slowThresholdMs)
      .filter((c) => dbFilter === 'all' || c.db_type === dbFilter)
      .sort((a, b) => b.duration_ms - a.duration_ms)
      .slice(0, SLOW_QUERIES_MAX)
  }, [snapshot, slowThresholdMs, dbFilter])

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
    return combined.filter((r) => {
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
  }, [snapshot, justFinished, search, kindFilter, dbFilter])

  const completed = React.useMemo(() => {
    const raw = snapshot?.completed ?? []
    return dbFilter === 'all' ? raw : raw.filter((c) => c.db_type === dbFilter)
  }, [snapshot, dbFilter])

  return { justFinished, filteredActive, completed, slowQueries }
}
