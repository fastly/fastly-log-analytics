'use client'

import { useNowMs } from '@/hooks/useNowSeconds'
import * as React from 'react'
import { useEffect } from 'react'
import { createParser, parseAsString, parseAsStringLiteral, useQueryStates } from 'nuqs'

import type {
  ActiveOrPromotedRow,
  AttributionKind,
  CompletedRow,
  DbFilter,
  GroupedCompletedRow,
  SnapshotResponse,
  ViewMode,
} from '../_types'

/**
 * Bind a small set of keyboard shortcuts to `window`. Designed for an
 * admin-only page so we don't have to worry about polluting the global
 * shortcut surface — the page mounts conditionally behind the admin gate.
 *
 * Each handler receives the raw `KeyboardEvent` and is responsible for
 * calling `preventDefault()` when it should swallow the key.
 *
 * Keys typed into form fields (`<input>`, `<textarea>`, `contenteditable`)
 * are ignored by default so `/` doesn't hijack searching inside the search
 * box itself. Pass `allowInForms: true` per binding to override (used by
 * `Esc`, which closes the expanded row even when focus is in the search
 * input).
 */
export type ShortcutBinding = {
  key: string
  description: string
  handler: (event: KeyboardEvent) => void
  allowInForms?: boolean
}

function isFormElement(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  const tag = target.tagName
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true
  if (target.isContentEditable) return true
  return false
}

/** Resolve the logical key for a binding match. Most browsers report the
 *  shifted character directly in `event.key` (Shift+/ → "?"), but some
 *  driver paths (Playwright, certain virtual keyboards, non-US layouts on
 *  older Chromium) report the unshifted base key and leave the caller to
 *  apply Shift. Normalise the handful of shifted characters we actually
 *  bind to so shortcuts work consistently. */
function logicalKey(event: KeyboardEvent): string {
  if (event.shiftKey) {
    if (event.key === '/' || event.code === 'Slash') return '?'
  }
  return event.key
}

export function useKeyboardShortcuts(bindings: ShortcutBinding[], enabled: boolean = true): void {
  useEffect(() => {
    if (!enabled) return
    const onKeyDown = (event: KeyboardEvent) => {
      // Don't fight the platform: meta/ctrl combinations belong to the
      // browser (cmd-K command palette, ctrl-R reload, etc.). Skip when
      // any modifier other than Shift is held.
      if (event.metaKey || event.ctrlKey || event.altKey) return
      const key = logicalKey(event)
      const inForm = isFormElement(event.target)
      for (const b of bindings) {
        if (b.key !== key) continue
        if (inForm && !b.allowInForms) continue
        b.handler(event)
        return
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [bindings, enabled])
}

/**
 * URL-backed filter state for the Live Query Monitor, via nuqs.
 *
 * Param names and formats are the shareable ops-link contract, e.g.
 * `…/admin/queries?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1`.
 * Defaults are stripped on write (`clearOnDefault`) so a clean view yields a
 * clean URL; unknown or malformed values fall back to the default, as the
 * previous hand-rolled hydration did.
 */
const KINDS = ['all', 'analyst', 'admin', 'cron', 'system'] as const satisfies readonly (AttributionKind | 'all')[]
const VIEWS = ['all', 'live', 'past'] as const satisfies readonly ViewMode[]
const DBS = ['all', 'DuckDB', 'Postgres', 'SQLite'] as const satisfies readonly DbFilter[]

// parseInt (not Number) on purpose: `?slow=250ms` keeps hydrating to 250.
const parseAsPositiveInt = createParser<number>({
  parse: (v) => {
    const n = parseInt(v, 10)
    return Number.isFinite(n) && n > 0 ? n : null
  },
  serialize: String,
})

// Stored as the negative (`nogroup=1`) so the default URL stays clean.
const parseAsNoGroupFlag = createParser<boolean>({
  parse: (v) => (v === '1' ? true : null),
  serialize: () => '1',
})

const queryMonitorUrlParsers = (defaultSlowMs: number) => ({
  q: parseAsString.withDefault('').withOptions({ clearOnDefault: true }),
  kind: parseAsStringLiteral(KINDS).withDefault('all').withOptions({ clearOnDefault: true }),
  view: parseAsStringLiteral(VIEWS).withDefault('all').withOptions({ clearOnDefault: true }),
  slow: parseAsPositiveInt.withDefault(defaultSlowMs).withOptions({ clearOnDefault: true }),
  db: parseAsStringLiteral(DBS).withDefault('all').withOptions({ clearOnDefault: true }),
  nogroup: parseAsNoGroupFlag.withDefault(false).withOptions({ clearOnDefault: true }),
})

type Updater<T> = T | ((prev: T) => T)

export function useQueryMonitorUrlState(defaultSlowMs: number) {
  const [params, setParams] = useQueryStates(queryMonitorUrlParsers(defaultSlowMs), {
    history: 'replace',
    shallow: true,
  })

  const setSearch = React.useCallback((v: string) => void setParams({ q: v }), [setParams])
  const setKindFilter = React.useCallback(
    (v: AttributionKind | 'all') => void setParams({ kind: v }),
    [setParams],
  )
  const setDbFilter = React.useCallback((v: DbFilter) => void setParams({ db: v }), [setParams])
  const setViewMode = React.useCallback((v: ViewMode) => void setParams({ view: v }), [setParams])
  const setSlowThresholdMs = React.useCallback((n: number) => void setParams({ slow: n }), [setParams])
  const setGroupCrons = React.useCallback(
    (v: Updater<boolean>) =>
      void setParams((prev) => ({ nogroup: !(typeof v === 'function' ? v(!prev.nogroup) : v) })),
    [setParams],
  )

  return {
    search: params.q,
    kindFilter: params.kind,
    dbFilter: params.db,
    viewMode: params.view,
    slowThresholdMs: params.slow,
    groupCrons: !params.nogroup,
    setSearch,
    setKindFilter,
    setDbFilter,
    setViewMode,
    setSlowThresholdMs,
    setGroupCrons,
  }
}

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
export function matchesQueryRowFilters(
  r: Pick<CompletedRow, 'db_type' | 'sql_preview' | 'attribution'>,
  kindFilter: AttributionKind | 'all',
  dbFilter: DbFilter,
  queryLower: string,
): boolean {
  if (kindFilter !== 'all' && r.attribution.kind !== kindFilter) return false
  if (dbFilter !== 'all' && r.db_type !== dbFilter) return false
  if (!queryLower) return true
  return (
    r.sql_preview.toLowerCase().includes(queryLower) ||
    r.attribution.caller_qualname.toLowerCase().includes(queryLower) ||
    r.attribution.caller_file.toLowerCase().includes(queryLower) ||
    r.attribution.label.toLowerCase().includes(queryLower)
  )
}

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
  const nowMs = useNowMs()
  const q = search.trim().toLowerCase()
  const justFinished = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const cutoff = nowMs / 1000 - JUST_FINISHED_WINDOW_S
    return all.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot, nowMs])

  const slowQueries = React.useMemo(() => {
    const all = snapshot?.completed ?? []
    const filtered = all
      .filter((c) => c.duration_ms >= slowThresholdMs)
      .filter((c) => matchesQueryRowFilters(c, kindFilter, dbFilter, q))
    const grouped = groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
    return [...grouped].sort((a, b) => b.duration_ms - a.duration_ms).slice(0, SLOW_QUERIES_MAX)
  }, [snapshot, slowThresholdMs, kindFilter, dbFilter, q, groupCrons, expandedRunIds])

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
    const filtered = combined.filter((r) =>
      matchesQueryRowFilters(r, kindFilter, dbFilter, q),
    )
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
  }, [snapshot, justFinished, q, kindFilter, dbFilter, groupCrons, expandedRunIds])

  const completed = React.useMemo<GroupedCompletedRow[]>(() => {
    const raw = snapshot?.completed ?? []
    const filtered = raw.filter((c) => matchesQueryRowFilters(c, kindFilter, dbFilter, q))
    return groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
  }, [snapshot, kindFilter, dbFilter, q, groupCrons, expandedRunIds])

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
