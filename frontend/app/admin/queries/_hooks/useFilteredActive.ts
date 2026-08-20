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
 *  bounded to 400 (deque maxlen = _HISTORY_CAP in
 *  backend/core/query_registry.py); 30 fills several screens without making
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
 * information (toggle off to see them all). Per-group expansion: any
 * ``cron_run_id`` present in ``expandedRunIds`` is shown in full, with
 * the head row keeping the ``×N`` badge and the sibling rows tagged with
 * ``_expandedChild`` for visual indent.
 */
export function useFilteredActive({
  snapshot,
  search,
  kindFilter,
  dbFilter,
  slowThresholdMs,
  groupCrons,
  expandedRunIds,
}: {
  snapshot: SnapshotResponse | undefined
  search: string
  kindFilter: AttributionKind | 'all'
  dbFilter: DbFilter
  slowThresholdMs: number
  groupCrons: boolean
  expandedRunIds: ReadonlySet<string>
}): {
  justFinished: CompletedRow[]
  filteredActive: ActiveOrPromotedRow[]
  completed: GroupedCompletedRow[]
  slowQueries: GroupedCompletedRow[]
} {
  const justFinished = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    // eslint-disable-next-line react-hooks/purity
    const cutoff = Date.now() / 1000 - JUST_FINISHED_WINDOW_S
    return all.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot])

  const slowQueries = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const filtered = all
      .filter((c) => c.duration_ms >= slowThresholdMs)
      .filter((c) => dbFilter === 'all' || c.db_type === dbFilter)
    const grouped = groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
    return [...grouped].sort((a, b) => b.duration_ms - a.duration_ms).slice(0, SLOW_QUERIES_MAX)
  }, [snapshot, slowThresholdMs, dbFilter, groupCrons, expandedRunIds])

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
    // Default order: live rows first (longest-running at top), then
    // promoted/just-finished, then cancelled. Sorting by duration alone
    // let a 5 s just-finished row outrank a 50 ms live row, which hid the
    // very thing the admin was probably looking for. Users can still
    // click any column header to re-sort via TanStack.
    const ordered = [...filtered].sort((a, b) => {
      const pa = activeRowPriority(a)
      const pb = activeRowPriority(b)
      if (pa !== pb) return pa - pb
      return b.duration_ms - a.duration_ms
    })
    return groupCrons ? collapseCronRunsActive(ordered, expandedRunIds) : ordered
  }, [snapshot, justFinished, search, kindFilter, dbFilter, groupCrons, expandedRunIds])

  const completed = React.useMemo<GroupedCompletedRow[]>(() => {
    const raw = snapshot?.completed ?? []
    const filtered = dbFilter === 'all' ? raw : raw.filter((c) => c.db_type === dbFilter)
    return groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
  }, [snapshot, dbFilter, groupCrons, expandedRunIds])

  return { justFinished, filteredActive, completed, slowQueries }
}

/** Default ordering priority — lower sorts first.
 *  0 = live, 1 = promoted/just-finished, 2 = cancelled. */
function activeRowPriority(r: ActiveOrPromotedRow): number {
  if (r.cancelled_at !== null) return 2
  if (r._completed) return 1
  return 0
}

/** Generic: collapse rows sharing ``attribution.cron_run_id`` to one
 *  representative per run; non-cron rows and rows without a run_id pass
 *  through untouched. The representative is the row that sorts first under
 *  ``sortMembers`` (typically: live-before-completed, then longest-running).
 *  When a run_id is in ``expandedRunIds``, ALL siblings render: the head
 *  keeps the ``_groupedCount`` + ``_isGroupHead`` badge so the user can
 *  collapse it back, and the rest get ``_expandedChild`` for visual indent.
 *  Stable ordering — the representative row keeps the first-seen position
 *  of the run.
 */
type CollapsibleRow = {
  attribution: { kind: string; cron_run_id?: string | null }
  duration_ms: number
}
type CollapsedRow<T> = T & {
  _groupedCount?: number
  _isGroupHead?: boolean
  _expandedChild?: true
}

function collapseByRunId<T extends CollapsibleRow>(
  rows: readonly T[],
  expandedRunIds: ReadonlySet<string>,
  sortMembers: (a: T, b: T) => number,
): CollapsedRow<T>[] {
  const groups = new Map<string, T[]>()
  const out: CollapsedRow<T>[] = []
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
      out.push(r) // placeholder; replaced below
    }
    groups.get(runId)!.push(r)
  }
  // Walk groups in reverse insertion order so splice-insertion of expanded
  // children doesn't shift indices of yet-to-process groups.
  const reversed = [...groups.entries()].reverse()
  for (const [runId, members] of reversed) {
    if (members.length === 1) {
      out[groupIndex.get(runId)!] = members[0]
      continue
    }
    const sorted = [...members].sort(sortMembers)
    const head = sorted[0]
    const rest = sorted.slice(1)
    if (expandedRunIds.has(runId)) {
      out[groupIndex.get(runId)!] = { ...head, _groupedCount: members.length, _isGroupHead: true }
      const children = rest.map((r) => ({ ...r, _expandedChild: true as const }))
      out.splice(groupIndex.get(runId)! + 1, 0, ...children)
    } else {
      out[groupIndex.get(runId)!] = { ...head, _groupedCount: members.length }
    }
  }
  return out
}

/** Active rows: live (non-completed) wins over completed siblings, then
 *  longest-running first. */
function collapseCronRunsActive(
  rows: ActiveOrPromotedRow[],
  expandedRunIds: ReadonlySet<string>,
): ActiveOrPromotedRow[] {
  return collapseByRunId(rows, expandedRunIds, (a, b) => {
    const liveDelta = (a._completed ? 1 : 0) - (b._completed ? 1 : 0)
    if (liveDelta !== 0) return liveDelta
    return b.duration_ms - a.duration_ms
  })
}

/** Completed rows: longest-running first. Used by ``completed`` and
 *  ``slowQueries`` so a single noisy cron tick doesn't flood either list. */
function collapseCronRunsCompleted(
  rows: CompletedRow[],
  expandedRunIds: ReadonlySet<string>,
): GroupedCompletedRow[] {
  return collapseByRunId(rows, expandedRunIds, (a, b) => b.duration_ms - a.duration_ms)
}
