'use client'

/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button, plus shared TanStack ColumnDef builders for ActiveTable and CompletedTable.
 */

import * as React from 'react'
import { useEffect } from 'react'
import { createParser, parseAsString, parseAsStringLiteral, useQueryStates } from 'nuqs'
import type { components } from '@/types/api.generated'
import type { Expect, WireParity } from '@/types/api'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef, Row } from '@tanstack/react-table'
import {
  AlertTriangle, Check, ChevronDown, ChevronRight, Copy, Group, Keyboard, Pause, Play, RefreshCw, Search, X,
} from 'lucide-react'
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConfirmDialog, CopyButton, HelpDialog, PageHeader } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useCopyToClipboard, useNowMs } from '@/hooks/useSSE'
import { adminJson, adminJsonResult, extractApiError, queryKeys } from "@/lib/api";
import { formatCount } from "@/lib/utils"

export type AttributionKind = 'analyst' | 'admin' | 'cron' | 'system'

export interface Attribution {
  kind: AttributionKind
  label: string
  principal_id: string | null
  caller_qualname: string
  caller_file: string
  request_path: string | null
  request_id: string | null
  cron_job: string | null
  cron_run_id: string | null
  pool_slot: string | null
}

export interface ActiveRow {
  query_id: number
  db_type: 'DuckDB' | 'Postgres' | 'SQLite'
  sql_preview: string
  sql: string | null
  sql_len: number
  attribution: Attribution
  service_id: string | null
  started_at_utc: number
  duration_ms: number
  cancellable: boolean
  cancelled_at: number | null
}

export interface CompletedRow extends Omit<ActiveRow, 'cancellable' | 'cancelled_at'> {
  ended_at_utc: number
  outcome: 'ok' | 'error' | 'cancelled'
  error_type: string | null
  error_message: string | null
  peak_memory_mb: number | null
}

export interface SnapshotResponse {
  last_seq: number
  active: ActiveRow[]
  completed: CompletedRow[]
}

export type SummaryResponse = components['schemas']['SummaryResponse']
export type CancelResponse = components['schemas']['CancelResponse']

export type _AttributionParity = Expect<WireParity<Attribution, components['schemas']['QueryAttribution']>>
export type _ActiveRowParity = Expect<WireParity<ActiveRow, components['schemas']['ActiveQueryRow']>>
export type _CompletedRowParity = Expect<WireParity<CompletedRow, components['schemas']['CompletedQueryRow']>>
export type _SnapshotParity = Expect<WireParity<SnapshotResponse, components['schemas']['SnapshotResponse']>>

export interface SlowQueriesResult {
  rows: CompletedRow[]
  since_hours: number
  threshold_ms: number
}

export type _SlowQueriesParity = Expect<WireParity<SlowQueriesResult, components['schemas']['SlowQueriesResponse']>>
export interface MonitorConfig { enabled: boolean }
export type ViewMode = 'all' | 'live' | 'past'
export type DbFilter = 'all' | 'DuckDB' | 'Postgres' | 'SQLite'

type GroupingMeta = { _groupedCount?: number; _isGroupHead?: boolean; _expandedChild?: boolean }
export type ActiveOrPromotedRow = ActiveRow & GroupingMeta & { _completed?: CompletedRow }
export type GroupedCompletedRow = CompletedRow & GroupingMeta

export function useDocumentVisible(): boolean {
  const [visible, setVisible] = React.useState(
    typeof document !== 'undefined' ? document.visibilityState !== 'hidden' : true,
  )
  React.useEffect(() => {
    const onVis = () => setVisible(document.visibilityState !== 'hidden')
    document.addEventListener('visibilitychange', onVis)
    return () => document.removeEventListener('visibilitychange', onVis)
  }, [])
  return visible
}

export function durationColor(ms: number): string {
  if (ms < 500) return 'text-emerald-600 dark:text-emerald-400'
  if (ms < 2000) return 'text-amber-600 dark:text-amber-400'
  if (ms < 10_000) return 'text-orange-600 dark:text-orange-400'
  return 'text-red-600 dark:text-red-400'
}

export function formatDurationMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`
}

export function kindBadgeVariant(kind: AttributionKind): 'default' | 'secondary' | 'destructive' | 'outline' {
  return kind === 'analyst' ? 'default' : kind === 'admin' ? 'secondary' : 'outline'
}

export function formatMemoryMb(mb: number | null | undefined): string {
  if (mb == null) return ''
  if (mb < 1) return `${Math.round(mb * 1024)} KB`
  if (mb < 1024) return `${mb < 10 ? mb.toFixed(1) : Math.round(mb)} MB`
  return `${(mb / 1024).toFixed(2)} GB`
}

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
  return ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) || target.isContentEditable
}

/** Resolve the logical key for a binding match. Most browsers report the
 *  shifted character directly in `event.key` (Shift+/ → "?"), but some
 *  driver paths (Playwright, certain virtual keyboards, non-US layouts on
 *  older Chromium) report the unshifted base key and leave the caller to
 *  apply Shift. Normalise the handful of shifted characters we actually
 *  bind to so shortcuts work consistently. */
function logicalKey(event: KeyboardEvent): string {
  if (event.shiftKey && (event.key === '/' || event.code === 'Slash')) return '?'
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
        if (b.key !== key || (inForm && !b.allowInForms)) continue
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
  const setKindFilter = React.useCallback((v: AttributionKind | 'all') => void setParams({ kind: v }), [setParams])
  const setDbFilter = React.useCallback((v: DbFilter) => void setParams({ db: v }), [setParams])
  const setViewMode = React.useCallback((v: ViewMode) => void setParams({ view: v }), [setParams])
  const setSlowThresholdMs = React.useCallback((n: number) => void setParams({ slow: n }), [setParams])
  const setGroupCrons = React.useCallback(
    (v: Updater<boolean>) => void setParams((prev) => ({ nogroup: !(typeof v === 'function' ? v(!prev.nogroup) : v) })),
    [setParams],
  )
  return {
    search: params.q, kindFilter: params.kind, dbFilter: params.db, viewMode: params.view,
    slowThresholdMs: params.slow, groupCrons: !params.nogroup,
    setSearch, setKindFilter, setDbFilter, setViewMode, setSlowThresholdMs, setGroupCrons,
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
  snapshot, search, kindFilter, dbFilter, slowThresholdMs, groupCrons, expandedRunIds,
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
    const cutoff = nowMs / 1000 - JUST_FINISHED_WINDOW_S
    return (snapshot?.completed ?? []).filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot, nowMs])

  const slowQueries = React.useMemo(() => {
    const filtered = (snapshot?.completed ?? []).filter(
      (c) => c.duration_ms >= slowThresholdMs && matchesQueryRowFilters(c, kindFilter, dbFilter, q),
    )
    const grouped = groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
    return [...grouped].sort((a, b) => b.duration_ms - a.duration_ms).slice(0, SLOW_QUERIES_MAX)
  }, [snapshot, slowThresholdMs, kindFilter, dbFilter, q, groupCrons, expandedRunIds])

  const filteredActive = React.useMemo(() => {
    const active: ActiveOrPromotedRow[] = (snapshot?.active ?? []).map((r) => ({ ...r }))
    const justRows: ActiveOrPromotedRow[] = justFinished.map((c) => ({
      query_id: c.query_id, db_type: c.db_type, sql_preview: c.sql_preview, sql: c.sql, sql_len: c.sql_len,
      attribution: c.attribution, service_id: c.service_id, started_at_utc: c.started_at_utc, duration_ms: c.duration_ms,
      cancellable: false, cancelled_at: null, _completed: c,
    }))
    // Dedupe on query_id — a row can theoretically appear in both lists
    // for one poll cycle as it transitions from active to completed.
    const seen = new Set<number>()
    const combined: ActiveOrPromotedRow[] = []
    for (const r of [...active, ...justRows]) {
      if (!seen.has(r.query_id)) { seen.add(r.query_id); combined.push(r) }
    }
    const filtered = combined.filter((r) => matchesQueryRowFilters(r, kindFilter, dbFilter, q))
    // Default order: live rows first (longest-running at top), then
    // promoted/just-finished, then cancelled. Sorting by duration alone
    // let a 5 s just-finished row outrank a 50 ms live row, which hid the
    // very thing the admin was probably looking for. Users can still
    // click any column header to re-sort via TanStack.
    const ordered = [...filtered].sort((a, b) => activeRowPriority(a) - activeRowPriority(b) || b.duration_ms - a.duration_ms)
    return groupCrons ? collapseCronRunsActive(ordered, expandedRunIds) : ordered
  }, [snapshot, justFinished, q, kindFilter, dbFilter, groupCrons, expandedRunIds])

  const completed = React.useMemo<GroupedCompletedRow[]>(() => {
    const filtered = (snapshot?.completed ?? []).filter((c) => matchesQueryRowFilters(c, kindFilter, dbFilter, q))
    return groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
  }, [snapshot, kindFilter, dbFilter, q, groupCrons, expandedRunIds])

  return { justFinished, filteredActive, completed, slowQueries }
}

/** Default ordering priority — lower sorts first.
 *  0 = live, 1 = promoted/just-finished, 2 = cancelled. */
function activeRowPriority(r: ActiveOrPromotedRow): number {
  return r.cancelled_at !== null ? 2 : r._completed ? 1 : 0
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
type CollapsibleRow = { attribution: { kind: string; cron_run_id?: string | null }; duration_ms: number }
type CollapsedRow<T> = T & { _groupedCount?: number; _isGroupHead?: boolean; _expandedChild?: true }

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
    if (r.attribution.kind !== 'cron' || !runId) { out.push(r); continue }
    if (!groups.has(runId)) {
      groups.set(runId, [])
      groupIndex.set(runId, out.length)
      out.push(r) // placeholder; replaced below
    }
    groups.get(runId)!.push(r)
  }
  // Walk groups in reverse insertion order so splice-insertion of expanded
  // children doesn't shift indices of yet-to-process groups.
  for (const [runId, members] of [...groups.entries()].reverse()) {
    const idx = groupIndex.get(runId)!
    if (members.length === 1) { out[idx] = members[0]; continue }
    const [head, ...rest] = [...members].sort(sortMembers)
    if (expandedRunIds.has(runId)) {
      out[idx] = { ...head, _groupedCount: members.length, _isGroupHead: true }
      out.splice(idx + 1, 0, ...rest.map((r) => ({ ...r, _expandedChild: true as const })))
    } else {
      out[idx] = { ...head, _groupedCount: members.length }
    }
  }
  return out
}

/** Active rows: live (non-completed) wins over completed siblings, then
 *  longest-running first. */
function collapseCronRunsActive(rows: ActiveOrPromotedRow[], expandedRunIds: ReadonlySet<string>): ActiveOrPromotedRow[] {
  return collapseByRunId(rows, expandedRunIds, (a, b) => (a._completed ? 1 : 0) - (b._completed ? 1 : 0) || b.duration_ms - a.duration_ms)
}

/** Completed rows: longest-running first. Used by ``completed`` and
 *  ``slowQueries`` so a single noisy cron tick doesn't flood either list. */
function collapseCronRunsCompleted(rows: CompletedRow[], expandedRunIds: ReadonlySet<string>): GroupedCompletedRow[] {
  return collapseByRunId(rows, expandedRunIds, (a, b) => b.duration_ms - a.duration_ms)
}

type AnyRow = ActiveOrPromotedRow | CompletedRow
type AttributedRow = Pick<ActiveRow, 'attribution' | 'db_type' | 'service_id'> & GroupingMeta

const STUCK_LIVE_MS = 30_000
const outcomeVariant = (o: string): 'outline' | 'secondary' | 'destructive' =>
  o === 'ok' ? 'outline' : o === 'cancelled' ? 'secondary' : 'destructive'

function buildSourceCell(onToggleGroup: (runId: string) => void) {
  return function SourceCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
    const attr = row.original.attribution
    const groupCount = row.original._groupedCount
    const isGroupHead = !!row.original._isGroupHead
    return (
      <div className={`flex items-center gap-2 min-w-0 ${row.original._expandedChild ? 'pl-5' : ''}`}>
        <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize shrink-0">{attr.kind}</Badge>
        <span className="truncate text-xs" title={attr.label}>{attr.label}</span>
        {groupCount && groupCount > 1 ? (
          <button
            type="button"
            className="shrink-0 inline-flex items-center gap-0.5 rounded-md border border-input bg-secondary px-1.5 py-0.5 font-mono text-[10px] hover:bg-secondary/80"
            title={isGroupHead ? `Collapse this cron run (${groupCount} queries)` : `Expand this cron run (${groupCount} queries)`}
            onClick={(e) => {
              e.stopPropagation()
              if (attr.cron_run_id) onToggleGroup(attr.cron_run_id)
            }}
          >
            {isGroupHead ? <ChevronDown className="h-2.5 w-2.5" /> : <ChevronRight className="h-2.5 w-2.5" />}
            ×{groupCount}
          </button>
        ) : null}
      </div>
    )
  }
}

function callerCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
  const attr = row.original.attribution
  return (
    <div className="font-mono text-xs text-muted-foreground" title={`${attr.caller_qualname}\n${attr.caller_file}`}>
      <div className="truncate">{attr.caller_qualname}</div>
      <div className="truncate text-[10px] opacity-60">{attr.caller_file}</div>
    </div>
  )
}

function buildServiceCol<T extends AttributedRow>(): ColumnDef<T> {
  return {
    id: 'service_id',
    accessorFn: (r) => r.service_id ?? '',
    header: 'Service',
    size: 200,
    cell: ({ row }) => (
      <span className="text-xs font-mono truncate" title={row.original.service_id ?? ''}>
        {row.original.service_id ?? '—'}
      </span>
    ),
  }
}

function buildSharedAttributionColumns<T extends AttributedRow>(onToggleGroup: (runId: string) => void): ColumnDef<T>[] {
  return [
    { id: 'source', accessorFn: (r) => r.attribution.label, header: 'Source', size: 280, cell: buildSourceCell(onToggleGroup) },
    { id: 'caller', accessorFn: (r) => r.attribution.caller_qualname, header: 'Caller', size: 280, cell: callerCell },
    { id: 'db_type', accessorKey: 'db_type', header: 'DB', size: 80, cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span> },
  ]
}

function activeDurationCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
  const r = row.original
  const isCancelled = r.cancelled_at !== null
  const promoted = !!r._completed
  const stuck = !promoted && !isCancelled && r.duration_ms >= STUCK_LIVE_MS
  return (
    <span
      className={`inline-flex items-center gap-1.5 font-mono ${durationColor(r.duration_ms)} ${promoted ? 'opacity-60' : ''} ${stuck ? 'animate-pulse font-bold' : ''}`}
      title={stuck ? `Live query running for ${formatDurationMs(r.duration_ms)} — investigate` : undefined}
    >
      {!promoted && !isCancelled && (
        <span className="relative flex h-2 w-2" aria-hidden="true">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
        </span>
      )}
      {formatDurationMs(r.duration_ms)}
    </span>
  )
}

function buildActionsCell(onKill: (row: ActiveRow) => void, cancellingQid: number | null) {
  return function ActionsCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
    const r = row.original
    if (r._completed) {
      const outcome = r._completed.outcome
      return <Badge variant={outcomeVariant(outcome)} className="capitalize">{outcome === 'ok' ? '✓ done' : outcome}</Badge>
    }
    if (r.cancelled_at !== null) return <span className="text-xs text-muted-foreground">cancelling…</span>
    if (!r.cancellable) return <span className="text-xs text-muted-foreground">—</span>
    const cancelling = cancellingQid === r.query_id
    return (
      <Button
        variant="destructive" size="sm" className="h-7 px-2" disabled={cancelling}
        title="Interrupt the currently-running statement. A caller that retries will start a new query you may need to cancel again."
        onClick={(e) => { e.stopPropagation(); onKill(r) }}
      >
        {cancelling ? 'Cancelling…' : (<><X className="h-3 w-3 mr-1" /> Kill</>)}
      </Button>
    )
  }
}

export function buildActiveColumns(deps: {
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  showService: boolean
  showPool: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<ActiveOrPromotedRow>[] {
  const cols = buildSharedAttributionColumns<ActiveOrPromotedRow>(deps.onToggleGroup)
  if (deps.showService) cols.push(buildServiceCol())
  if (deps.showPool) {
    cols.push({
      id: 'pool_slot', accessorFn: (r) => r.attribution.pool_slot ?? '', header: 'Pool', size: 200,
      cell: ({ row }) => <span className="text-xs font-mono">{row.original.attribution.pool_slot ?? '—'}</span>,
    })
  }
  cols.push(
    { id: 'duration_ms', accessorKey: 'duration_ms', header: 'Duration', size: 120, cell: activeDurationCell },
    { id: 'actions', header: 'Actions', enableSorting: false, size: 110, cell: buildActionsCell(deps.onKill, deps.cancellingQid) },
  )
  return cols
}

export function buildCompletedColumns(opts: {
  showMemory: boolean
  showService: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<GroupedCompletedRow>[] {
  const cols: ColumnDef<GroupedCompletedRow>[] = [
    {
      id: 'outcome', accessorKey: 'outcome', header: 'Outcome', size: 130,
      cell: ({ row }) => {
        const r = row.original
        return (
          <div className="flex items-center gap-2">
            <Badge variant={outcomeVariant(r.outcome)} className="capitalize">{r.outcome}</Badge>
            {r.error_type && <span className="text-xs text-red-600 truncate" title={r.error_message ?? ''}>{r.error_type}</span>}
          </div>
        )
      },
    },
    ...buildSharedAttributionColumns<GroupedCompletedRow>(opts.onToggleGroup),
  ]
  if (opts.showService) cols.push(buildServiceCol())
  cols.push({
    id: 'duration_ms', accessorKey: 'duration_ms', header: 'Duration', size: 110,
    cell: ({ row }) => <span className={`font-mono text-xs ${durationColor(row.original.duration_ms)}`}>{formatDurationMs(row.original.duration_ms)}</span>,
  })
  if (opts.showMemory) {
    cols.push({
      id: 'peak_memory_mb', accessorKey: 'peak_memory_mb', header: 'Memory', size: 100,
      cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatMemoryMb(row.original.peak_memory_mb) || '—'}</span>,
    })
  }
  cols.push({
    id: 'sql', accessorKey: 'sql_preview', header: 'SQL', enableSorting: false, size: 400,
    cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground truncate block" title={row.original.sql_preview}>{row.original.sql_preview}</span>,
  })
  return cols
}

const hasAnyVal = <T,>(rows: T[], pick: (r: T) => unknown) => rows.some((r) => pick(r) != null)

export function ActiveTable({ rows, onRowClick, onKill, cancellingQid, onToggleGroup }: {
  rows: ActiveOrPromotedRow[]; onRowClick: (row: ActiveOrPromotedRow) => void; onKill: (row: ActiveRow) => void; cancellingQid: number | null; onToggleGroup: (runId: string) => void
}) {
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const showPool = hasAnyVal(rows, (r) => r.attribution.pool_slot)
  const columns = React.useMemo(
    () => buildActiveColumns({ onKill, cancellingQid, showService, showPool, onToggleGroup }),
    [onKill, cancellingQid, showService, showPool, onToggleGroup],
  )
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns} data={rows} onRowClick={onRowClick} hideToolbar showPagination={false}
      getRowClassName={(row) =>
        row._expandedChild ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
          : row._completed ? 'opacity-60' : row.cancelled_at !== null ? 'opacity-50' : 'bg-primary/5 border-l-2 border-l-primary/60'
      }
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      tableCaption="Active and just-finished queries"
    />
  )
}

export function CompletedTable({ rows, onRowClick, emptyMessage = 'No completed queries yet.', initialSorting, onToggleGroup }: {
  rows: GroupedCompletedRow[]; onRowClick: (row: GroupedCompletedRow) => void; emptyMessage?: string; initialSorting?: { id: string; desc: boolean }[]; onToggleGroup: (runId: string) => void
}) {
  const showMemory = hasAnyVal(rows, (r) => r.peak_memory_mb)
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const columns = React.useMemo(
    () => buildCompletedColumns({ showMemory, showService, onToggleGroup }),
    [showMemory, showService, onToggleGroup],
  )
  return (
    <DataTable<GroupedCompletedRow, unknown>
      columns={columns} data={rows} onRowClick={onRowClick} hideToolbar showPagination={rows.length > 50}
      getRowClassName={(row) => (row._expandedChild ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30' : '')}
      emptyMessage={emptyMessage} initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]} tableCaption="Completed queries"
    />
  )
}

const isCompleted = (row: AnyRow): row is CompletedRow => 'outcome' in row
const isActivePromoted = (row: AnyRow): row is ActiveOrPromotedRow => 'cancellable' in row

export function RowDetailDialog({ row, onClose, onKill, cancellingQid }: {
  row: AnyRow | null; onClose: () => void; onKill?: (row: ActiveRow) => void; cancellingQid?: number | null
}) {
  const isLive = row !== null && isActivePromoted(row) && !row._completed
  const { data: fullRow } = useQuery<AnyRow>({
    queryKey: queryKeys.admin.queryMonitorDetail(row?.query_id ?? null),
    queryFn: ({ signal }) => adminJson<'/api/admin/queries/{qid}', ActiveRow>('/api/admin/queries/{qid}', `/api/admin/queries/${row!.query_id}`, { signal }),
    enabled: row !== null && isLive, refetchInterval: 2000, refetchIntervalInBackground: false,
  })

  if (!row) return null
  const display = (fullRow ?? row) as AnyRow
  const sql = display.sql ?? row.sql_preview
  const attr = display.attribution
  const completed = isCompleted(display) ? display : null
  const ap = isActivePromoted(display) ? display : null
  const cancelling = ap !== null && cancellingQid !== undefined && cancellingQid === ap.query_id
  const canKill = ap !== null && ap.cancellable && ap.cancelled_at === null && onKill !== undefined

  return (
    <Dialog open={row !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize">{attr.kind}</Badge>
            <span className="truncate">{attr.label}</span>
            {completed && (
              <Badge variant={outcomeVariant(completed.outcome)} className="capitalize ml-2">
                {completed.outcome}{completed.error_type && `: ${completed.error_type}`}
              </Badge>
            )}
          </DialogTitle>
          <DialogDescription className="sr-only">Query {display.query_id} details</DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-2 gap-3 text-xs">
          <div><span className="text-muted-foreground">Caller:</span> <span className="font-mono">{attr.caller_qualname} <span className="opacity-60">({attr.caller_file})</span></span></div>
          <div><span className="text-muted-foreground">DB:</span> {display.db_type}</div>
          <div><span className="text-muted-foreground">Service:</span> <span className="font-mono">{display.service_id ?? '—'}</span></div>
          <div><span className="text-muted-foreground">Pool slot:</span> <span className="font-mono">{attr.pool_slot ?? '—'}</span></div>
          <div>
            <span className="text-muted-foreground">Request:</span> {attr.request_path || '—'}
            {attr.request_id && <span className="opacity-60"> · {attr.request_id.slice(0, 8)}</span>}
          </div>
          <div><span className="text-muted-foreground">Duration:</span> <span className={`font-mono ${durationColor(display.duration_ms)}`}>{formatDurationMs(display.duration_ms)}</span></div>
          {attr.cron_job && <div><span className="text-muted-foreground">Cron:</span> {attr.cron_job}{attr.cron_run_id && ` (run ${attr.cron_run_id})`}</div>}
          {completed?.peak_memory_mb != null && <div><span className="text-muted-foreground">Peak memory:</span> <span className="font-mono">{formatMemoryMb(completed.peak_memory_mb)}</span></div>}
          {completed?.error_message && <div className="col-span-2"><span className="text-muted-foreground">Error:</span> <span className="font-mono text-red-600">{completed.error_message}</span></div>}
        </div>

        <div className="relative">
          <CopyButton
            text={sql} label="Copy" copiedLabel="Copied" variant="outline" size="sm"
            className="absolute top-1.5 right-1.5 h-7 px-2 text-xs" title="Copy SQL to clipboard" timeoutMs={1500}
          />
          <pre className="bg-muted/50 border rounded p-3 pr-12 text-xs overflow-auto whitespace-pre-wrap font-mono max-h-96">{sql}</pre>
        </div>
        {row.sql_len > 200 && !fullRow && isLive && <div className="text-xs text-muted-foreground">Loading full SQL ({row.sql_len} chars)…</div>}

        <DialogFooter>
          {canKill && (
            <Button variant="destructive" size="sm" disabled={cancelling} onClick={() => onKill!(ap!)}>
              {cancelling ? 'Cancelling…' : (<><X className="h-3 w-3 mr-1" /> Kill</>)}
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={onClose}>Close</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function FilterChipRow<T extends string>({ value, onChange, options }: { value: T; onChange: (v: T) => void; options: { value: T; label: string }[] }) {
  return (
    <div className="flex items-center gap-1">
      {options.map((o) => (
        <Button key={o.value} variant={value === o.value ? 'default' : 'outline'} size="sm" className="h-7 px-2 text-xs capitalize" onClick={() => onChange(o.value)}>
          {o.label}
        </Button>
      ))}
    </div>
  )
}

export function PollingIndicator({ visible, isFetching, isError, paused = false }: { visible: boolean; isFetching: boolean; isError: boolean; paused?: boolean }) {
  if (isError) return <span className="text-xs text-red-500 ml-2">Error — retrying</span>
  if (paused) return <span className="text-xs text-amber-600 ml-2 font-medium">Paused</span>
  if (!visible) return <span className="text-xs text-muted-foreground ml-2">Paused (tab hidden)</span>
  return (
    <span className="flex items-center gap-1 text-xs text-muted-foreground ml-2">
      <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : 'opacity-50'}`} />
      Live
    </span>
  )
}

export function ShortcutsHelp({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  return (
    <HelpDialog open={open} onOpenChange={onOpenChange} title="Keyboard shortcuts" icon={<Keyboard className="h-4 w-4" />}>
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm text-foreground">
        {[
          ['/', 'Focus the search field'],
          ['.', 'Pause / resume the snapshot poll (stops rows from shifting under you)'],
          ['Esc', 'Close the row detail dialog, kill confirm, or this help overlay'],
          ['?', 'Show this help overlay'],
        ].map(([k, desc]) => (
          <React.Fragment key={k}>
            <dt className="self-center">
              <kbd className="inline-flex items-center justify-center min-w-[1.5rem] h-6 px-1.5 rounded border border-border bg-muted/50 text-xs font-mono">{k}</kbd>
            </dt>
            <dd>{desc}</dd>
          </React.Fragment>
        ))}
      </dl>
      <p className="mt-4 text-xs text-muted-foreground">
        Shortcuts are disabled while typing in the search box (except <kbd>Esc</kbd>, which always works).
      </p>
    </HelpDialog>
  )
}

export function SummaryStrip({ snapshot }: { snapshot: SnapshotResponse | undefined }) {
  const data = React.useMemo<SummaryResponse | null>(() => {
    if (!snapshot) return null
    const by_db_type: Record<string, number> = {}
    let longest_ms = 0
    for (const row of snapshot.active) {
      by_db_type[row.db_type] = (by_db_type[row.db_type] ?? 0) + 1
      if (row.duration_ms > longest_ms) longest_ms = row.duration_ms
    }
    return { active_total: snapshot.active.length, by_db_type, longest_ms }
  }, [snapshot])

  if (!data) return null
  return (
    <div className="flex items-center gap-3 text-sm">
      <Badge variant={data.active_total > 0 ? 'default' : 'outline'} className="gap-1">
        <span className="font-medium">{data.active_total}</span> active
      </Badge>
      {Object.entries(data.by_db_type).map(([db, n]) => (
        <Badge key={db} variant="outline" className="gap-1">{db} <span className="font-medium">{n}</span></Badge>
      ))}
      {data.longest_ms > 0 && <span className={`text-xs ${durationColor(data.longest_ms)}`}>longest: {formatDurationMs(data.longest_ms)}</span>}
      <div role="status" aria-live="polite" className="sr-only">
        {data.active_total} active {data.active_total === 1 ? 'query' : 'queries'}
      </div>
    </div>
  )
}

const DEFAULT_SLOW_THRESHOLD_MS = 500
const formatThresholdMs = (ms: number, space = false) =>
  ms < 1000 ? `${ms}${space ? ' ' : ''}ms` : `${ms / 1000}${space ? ' ' : ''}s`
const DB_FILTER_OPTIONS: { value: DbFilter; label: string }[] = [
  { value: 'all', label: 'All DBs' }, { value: 'DuckDB', label: 'DuckDB' }, { value: 'Postgres', label: 'Postgres' },
]
const KIND_FILTER_OPTIONS: { value: AttributionKind | 'all'; label: string }[] = (
  ['all', 'analyst', 'admin', 'cron', 'system'] as const
).map((o) => ({ value: o, label: o }))

type SlowHistoryMode = 'recent' | 'past_1h' | 'past_6h' | 'past_12h' | 'past_24h' | 'past_7d'

const SLOW_HISTORY_META: Record<SlowHistoryMode, { hours: number; buttonLabel: string; title: string; emptyWindow: string; reportLabel: string }> = {
  recent: { hours: 24, buttonLabel: 'Recent', title: 'Live in-memory ring (~10–30 min window, clears on restart)', emptyWindow: '', reportLabel: 'Recent In-Memory Ring' },
  past_1h: { hours: 1, buttonLabel: '1 h', title: 'Persistent history — last 1 h', emptyWindow: '1 hour', reportLabel: 'Past 1h' },
  past_6h: { hours: 6, buttonLabel: '6 h', title: 'Persistent history — last 6 h', emptyWindow: '6 hours', reportLabel: 'Past 6h' },
  past_12h: { hours: 12, buttonLabel: '12 h', title: 'Persistent history — last 12 h', emptyWindow: '12 hours', reportLabel: 'Past 12h' },
  past_24h: { hours: 24, buttonLabel: '24 h', title: 'Persistent history — last 24 h', emptyWindow: '24 hours', reportLabel: 'Past 24h' },
  past_7d: { hours: 168, buttonLabel: '7 d', title: 'Persistent history — last 7 d', emptyWindow: '7 days', reportLabel: 'Past 7d' },
}
const SLOW_HISTORY_MODES = Object.keys(SLOW_HISTORY_META) as SlowHistoryMode[]
const COMPLETED_BUFFER_CAP = 500

export default function QueryMonitorPage() {
  const queryClient = useQueryClient()
  const visible = useDocumentVisible()
  const {
    search, kindFilter, dbFilter, viewMode, slowThresholdMs, groupCrons,
    setSearch, setKindFilter, setDbFilter, setViewMode, setSlowThresholdMs, setGroupCrons,
  } = useQueryMonitorUrlState(DEFAULT_SLOW_THRESHOLD_MS)
  const [confirmKill, setConfirmKill] = React.useState<ActiveRow | null>(null)
  const [actionError, setActionError] = React.useState<string>('')
  const [slowHistoryMode, setSlowHistoryMode] = React.useState<SlowHistoryMode>('recent')
  const [paused, setPaused] = React.useState(false)
  const [expandedRunIds, setExpandedRunIds] = React.useState<ReadonlySet<string>>(() => new Set())
  const toggleGroup = React.useCallback((runId: string) => {
    setExpandedRunIds((prev) => {
      const next = new Set(prev)
      if (next.has(runId)) next.delete(runId); else next.add(runId)
      return next
    })
  }, [])
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false)
  const [detailRow, setDetailRow] = React.useState<AnyRow | null>(null)
  const searchInputRef = React.useRef<HTMLInputElement>(null)

  const { data: cfg } = useQuery<MonitorConfig>({
    queryKey: queryKeys.admin.queryMonitorConfig(),
    queryFn: ({ signal }) => adminJson('/api/admin/app-config/query-monitor', '/api/admin/app-config/query-monitor', { signal }),
    staleTime: 60_000,
  })

  const enabled = cfg?.enabled !== false
  const includeCompleted = viewMode !== 'live'
  const lastSeqRef = React.useRef(0)
  const completedRef = React.useRef<Map<number, CompletedRow>>(new Map())
  const resetPendingRef = React.useRef(false)
  const invalidateSnapshots = React.useCallback(
    () => queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() }),
    [queryClient],
  )

  const snapshotQuery = useQuery<SnapshotResponse>({
    queryKey: queryKeys.admin.queryMonitorSnapshot(includeCompleted),
    queryFn: async ({ signal }) => {
      if (resetPendingRef.current) {
        resetPendingRef.current = false; lastSeqRef.current = 0; completedRef.current = new Map()
      }
      const params = new URLSearchParams()
      if (includeCompleted) params.set('include_completed', 'true')
      if (lastSeqRef.current > 0) params.set('since_seq', String(lastSeqRef.current))
      else if (includeCompleted) params.set('cold_limit', '50')
      const qs = params.toString()
      const delta = await adminJson<'/api/admin/queries', SnapshotResponse>('/api/admin/queries', `/api/admin/queries${qs ? '?' + qs : ''}`, { signal })
      if (delta.last_seq > lastSeqRef.current) lastSeqRef.current = delta.last_seq
      const buffer = completedRef.current
      for (const row of delta.completed ?? []) buffer.set(row.query_id, row)
      if (buffer.size > COMPLETED_BUFFER_CAP) {
        const sortedIds = [...buffer.keys()].sort((a, b) => a - b)
        for (let k = 0; k < sortedIds.length - COMPLETED_BUFFER_CAP; k++) buffer.delete(sortedIds[k])
      }
      return { last_seq: delta.last_seq, active: delta.active, completed: [...buffer.values()] }
    },
    enabled: visible && enabled && !paused,
    refetchInterval: (query) => ((query.state.data as SnapshotResponse | undefined)?.active?.length ? 300 : 1500),
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => { resetPendingRef.current = true; void invalidateSnapshots() }, [viewMode, invalidateSnapshots])

  const wasVisibleRef = React.useRef(visible)
  React.useEffect(() => {
    if (visible && !wasVisibleRef.current) { resetPendingRef.current = true; void invalidateSnapshots() }
    wasVisibleRef.current = visible
  }, [visible, invalidateSnapshots])

  const bufferedSnapshot = snapshotQuery.data

  const cancelMutation = useMutation({
    mutationFn: async (qid: number): Promise<CancelResponse> => {
      const r = await adminJsonResult<'/api/admin/queries/{qid}/cancel', CancelResponse, 'post'>(
        '/api/admin/queries/{qid}/cancel', `/api/admin/queries/${qid}/cancel`, { method: 'POST' },
      )
      if (!r.ok) throw new Error(r.errorText || `status ${r.status}`)
      return r.data
    },
    onSuccess: (res) => {
      setActionError('')
      void invalidateSnapshots()
      if (res.state === 'connection_gone') setActionError('That query already finished — its connection is gone, so there was nothing left to interrupt.')
      else if (res.state === 'not_found') setActionError('That query is no longer active — it completed or aged out of the live buffer before the kill landed.')
      else if (res.state !== 'cancelled' && res.state !== 'already_finished') setActionError(`Cancel returned: ${res.state}`)
    },
    onError: (err: Error) => setActionError(extractApiError(err)),
  })

  const historicalQuery = useQuery<SlowQueriesResult>({
    queryKey: queryKeys.admin.queryMonitorSlowHistory(slowHistoryMode, slowThresholdMs),
    queryFn: ({ signal }) => adminJson<'/api/admin/slow-queries', SlowQueriesResult>(
      '/api/admin/slow-queries',
      `/api/admin/slow-queries?since_hours=${SLOW_HISTORY_META[slowHistoryMode].hours}&threshold_ms=${slowThresholdMs}&sort=duration&limit=200`,
      { signal },
    ),
    enabled: slowHistoryMode !== 'recent' && enabled, staleTime: 30_000,
  })

  const { filteredActive, completed, slowQueries } = useFilteredActive({
    snapshot: bufferedSnapshot, search, kindFilter, dbFilter, slowThresholdMs, groupCrons, expandedRunIds,
  })

  const filteredHistoricalRows = React.useMemo(() => {
    const q = search.trim().toLowerCase()
    return (historicalQuery.data?.rows ?? []).filter((r) => matchesQueryRowFilters(r, kindFilter, dbFilter, q))
  }, [historicalQuery.data?.rows, search, kindFilter, dbFilter])

  const { copied: copiedQueries, copy: copyText } = useCopyToClipboard(1500)

  const handleCopyQueries = React.useCallback(async () => {
    const queries = slowHistoryMode === 'recent' ? slowQueries : filteredHistoricalRows
    if (queries.length === 0) return

    const toIso = (ts?: number | null) => ts ? new Date(ts < 10_000_000_000 ? ts * 1000 : ts).toISOString() : '—'
    const lines = [
      '==================================================',
      'NOTABLE SLOW QUERIES REPORT',
      '==================================================',
      `Time Window:    ${SLOW_HISTORY_META[slowHistoryMode].reportLabel}`,
      `Threshold:      ≥ ${slowThresholdMs} ms`,
      `Total Queries:  ${queries.length}`,
      `Exported At:    ${new Date().toISOString()}`,
      '',
    ]

    queries.forEach((row, i) => {
      const attr = row.attribution
      lines.push(
        '--------------------------------------------------',
        `[Query #${i + 1}] ID: ${row.query_id} | DB: ${row.db_type} | Duration: ${formatCount(row.duration_ms)} ms | Outcome: ${row.outcome}`,
        `Timestamp:      Started ${toIso(row.started_at_utc)} · Ended ${toIso(row.ended_at_utc)}`,
        `Attribution:    Kind: ${attr.kind} · Caller: ${attr.caller_qualname} (${attr.caller_file})`,
      )
      if (attr.cron_job) lines.push(`Cron:           ${attr.cron_job}${attr.cron_run_id ? ` (Run: ${attr.cron_run_id})` : ''}`)
      if (attr.request_path) lines.push(`HTTP Request:   ${attr.request_path}${attr.request_id ? ` (ID: ${attr.request_id})` : ''}`)
      if (row.service_id) lines.push(`Service:        ${row.service_id}`)
      if (row.peak_memory_mb != null) lines.push(`Peak Memory:    ${row.peak_memory_mb.toFixed(1)} MB`)
      if (row.error_message) lines.push(`Error:          ${row.error_message}`)
      lines.push('', 'SQL:', row.sql || row.sql_preview, '')
    })
    lines.push('==================================================', '')
    await copyText(lines.join('\n'))
  }, [slowHistoryMode, slowQueries, filteredHistoricalRows, slowThresholdMs, copyText])

  const requestKill = React.useCallback((row: ActiveRow) => {
    setActionError('')
    if (row.attribution.kind === 'cron' || row.attribution.kind === 'system') setConfirmKill(row)
    else cancelMutation.mutate(row.query_id)
  }, [cancelMutation])

  const shortcuts = React.useMemo<ShortcutBinding[]>(() => [
    { key: '/', description: 'Focus the search field', handler: (e) => { e.preventDefault(); searchInputRef.current?.focus(); searchInputRef.current?.select() } },
    { key: '?', description: 'Show keyboard shortcuts', handler: (e) => { e.preventDefault(); setShortcutsOpen(true) } },
    { key: '.', description: 'Pause / resume the snapshot poll', handler: (e) => { e.preventDefault(); setPaused((p) => !p) } },
    {
      key: 'Escape', description: 'Close dialog / overlay', allowInForms: true,
      handler: () => {
        if (shortcutsOpen) return setShortcutsOpen(false)
        if (detailRow) return setDetailRow(null)
        if (confirmKill) return setConfirmKill(null)
        if (document.activeElement === searchInputRef.current) searchInputRef.current?.blur()
      },
    },
  ], [shortcutsOpen, confirmKill, detailRow])

  const [isManualRefreshing, setIsManualRefreshing] = React.useState(false)

  const handleManualRefresh = React.useCallback(() => {
    setIsManualRefreshing(true)
    resetPendingRef.current = true
    const pending: Promise<unknown>[] = [invalidateSnapshots()]
    if (slowHistoryMode !== 'recent') pending.push(historicalQuery.refetch())
    void Promise.allSettled(pending).then(() => setIsManualRefreshing(false))
  }, [invalidateSnapshots, slowHistoryMode, historicalQuery])

  useKeyboardShortcuts(shortcuts, enabled)

  const isInitialLoading = snapshotQuery.isPending && !bufferedSnapshot
  const isRefreshingActive = isManualRefreshing && snapshotQuery.isFetching
  const isRefreshingSlow =
    (isManualRefreshing && (snapshotQuery.isFetching || historicalQuery.isFetching)) ||
    (slowHistoryMode !== 'recent' && historicalQuery.isFetching && !!historicalQuery.data)
  const slowRows = slowHistoryMode === 'recent' ? slowQueries : filteredHistoricalRows

  return (
    <div className="space-y-6" data-testid="admin-queries-root">
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and Postgres query. Click a row to see the full SQL."
      >
        <BackToAdminLink variant="secondary" prefetch={false} />
      </PageHeader>

      {!enabled && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Live Query Monitor is disabled</AlertTitle>
          <AlertDescription>Set <code>QUERY_MONITOR_ENABLED=1</code> in the backend environment to enable.</AlertDescription>
        </Alert>
      )}

      {enabled && (
        <>
          <div
            data-testid="admin-queries-summary-reservation"
            className="flex items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
          >
            <SummaryStrip snapshot={bufferedSnapshot} />
            <div className="flex items-center gap-2">
              <Button
                variant="outline" size="sm" className="h-7 px-2 text-xs" data-testid="admin-queries-refresh-btn"
                onClick={handleManualRefresh} disabled={snapshotQuery.isFetching} title="Refresh query snapshot"
              >
                <RefreshCw className={`h-3 w-3 mr-1 ${isRefreshingActive ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
              <Button variant="ghost" size="sm" className="h-8 px-2" onClick={() => setShortcutsOpen(true)} title="Keyboard shortcuts (?)">
                <Keyboard className="h-4 w-4 text-muted-foreground" />
                <span className="sr-only">Show keyboard shortcuts</span>
              </Button>
            </div>
          </div>

          {actionError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{actionError}</AlertDescription>
            </Alert>
          )}

          <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as ViewMode)}>
            <TabsList>
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="live">Live only</TabsTrigger>
              <TabsTrigger value="past">Past only</TabsTrigger>
            </TabsList>
          </Tabs>

          {viewMode !== 'past' && (
            <div data-testid="admin-queries-active-reservation" className="min-h-[480px] [contain-intrinsic-size:auto_480px] [content-visibility:auto]">
              <Card className="min-h-[480px]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3 gap-2">
                  <CardTitle className="text-base flex items-center gap-2 whitespace-nowrap shrink-0">
                    Active &amp; Just-Finished
                    <Badge variant="secondary" className="whitespace-nowrap">
                      {filteredActive.filter((r) => !r._completed).length} active
                      {filteredActive.some((r) => r._completed) && ` + ${filteredActive.filter((r) => r._completed).length} just-finished`}
                    </Badge>
                    <PollingIndicator visible={visible} isFetching={snapshotQuery.isFetching} isError={snapshotQuery.isError} paused={paused} />
                  </CardTitle>
                  <div className="flex items-center gap-2 shrink-0">
                    <Button
                      variant={paused ? 'default' : 'outline'} size="sm" className="h-7 px-2 text-xs"
                      onClick={() => { if (paused) { setPaused(false); void invalidateSnapshots() } else setPaused(true) }}
                      title={paused ? 'Resume polling (.)' : 'Pause polling (.)'}
                    >
                      {paused ? <><Play className="h-3 w-3 mr-1" /> Resume</> : <><Pause className="h-3 w-3 mr-1" /> Pause</>}
                    </Button>
                    <FilterChipRow value={dbFilter} onChange={setDbFilter} options={DB_FILTER_OPTIONS} />
                    <FilterChipRow value={kindFilter} onChange={setKindFilter} options={KIND_FILTER_OPTIONS} />
                    <Button
                      variant={groupCrons ? 'default' : 'outline'} size="sm" className="h-7 px-2 text-xs"
                      onClick={() => setGroupCrons((v) => !v)}
                      title={groupCrons ? 'Cron rows from the same run are collapsed. Click to expand.' : 'Cron rows are shown individually. Click to group by run.'}
                    >
                      <Group className="h-3 w-3 mr-1" />
                      Group crons
                    </Button>
                    <div className="relative">
                      <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                      <Input
                        ref={searchInputRef} placeholder="Filter by SQL or caller…" value={search}
                        onChange={(e) => setSearch(e.target.value)} className="h-8 w-48 pl-7 text-sm"
                      />
                    </div>
                  </div>
                </CardHeader>
                <CardContent className={`p-0 transition-opacity duration-150 ${isRefreshingActive ? 'opacity-40 pointer-events-none' : ''}`}>
                  {isInitialLoading ? (
                    <div data-testid="admin-queries-active-skeleton">
                      <Skeleton className="h-10 w-full rounded-none" />
                      <div className="h-[400px] flex items-center justify-center text-xs text-muted-foreground">Inspecting DuckDB connection pool...</div>
                    </div>
                  ) : (
                    <ActiveTable
                      rows={filteredActive} onRowClick={(row) => setDetailRow(row)} onKill={requestKill}
                      cancellingQid={cancelMutation.variables ?? null} onToggleGroup={toggleGroup}
                    />
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {viewMode !== 'live' && (
            <div data-testid="admin-queries-slow-reservation" className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]">
              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    Notable Slow Queries
                    <Badge variant="outline">{slowRows.length}</Badge>
                    <span className="text-xs text-muted-foreground font-normal">
                      ≥ {formatThresholdMs(slowThresholdMs, slowThresholdMs < 1000)}, sorted slowest first
                    </span>
                    {slowHistoryMode !== 'recent' && historicalQuery.isFetching && (
                      <span className="text-xs text-muted-foreground italic">Loading query telemetry...</span>
                    )}
                  </CardTitle>
                  <div className="flex items-center gap-1">
                    <div className="flex items-center gap-1 mr-2">
                      {SLOW_HISTORY_MODES.map((m) => (
                        <Button
                          key={m} variant={slowHistoryMode === m ? 'default' : 'outline'} size="sm" className="h-7 px-2 text-xs"
                          onClick={() => setSlowHistoryMode(m)} title={SLOW_HISTORY_META[m].title}
                        >
                          {SLOW_HISTORY_META[m].buttonLabel}
                        </Button>
                      ))}
                    </div>
                    {[100, 500, 1000, 2000, 5000].map((ms) => (
                      <Button key={ms} variant={slowThresholdMs === ms ? 'default' : 'outline'} size="sm" className="h-7 px-2 text-xs" onClick={() => setSlowThresholdMs(ms)}>
                        {formatThresholdMs(ms)}
                      </Button>
                    ))}
                    <Button
                      variant="outline" size="sm" className="h-7 px-2 text-xs ml-2 font-medium shrink-0"
                      disabled={slowRows.length === 0} onClick={handleCopyQueries} title="Copy slow queries report to clipboard"
                    >
                      {copiedQueries ? (
                        <><Check className="h-3 w-3 mr-1 text-emerald-500 animate-in fade-in zoom-in-95 duration-150" />Copied!</>
                      ) : (
                        <><Copy className="h-3 w-3 mr-1" />Copy queries</>
                      )}
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className={`p-0 transition-opacity duration-150 ${isRefreshingSlow ? 'opacity-40 pointer-events-none' : ''}`}>
                  {isInitialLoading ? (
                    <div data-testid="admin-queries-slow-skeleton">
                      <Skeleton className="h-10 w-full rounded-none" />
                      <div className="h-24 flex items-center justify-center text-xs text-muted-foreground">Loading query telemetry...</div>
                    </div>
                  ) : (
                    <CompletedTable
                      rows={slowRows} onRowClick={(row) => setDetailRow(row)} initialSorting={[{ id: 'duration_ms', desc: true }]} onToggleGroup={toggleGroup}
                      emptyMessage={
                        slowHistoryMode === 'recent'
                          ? `No queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in recent history.`
                          : historicalQuery.isFetching
                            ? 'Loading query telemetry...'
                            : `No persisted queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in the last ${SLOW_HISTORY_META[slowHistoryMode].emptyWindow}.`
                      }
                    />
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {viewMode !== 'live' && (
            <div data-testid="admin-queries-completed-reservation" className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]">
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    Recently Completed
                    <Badge variant="outline">{completed.length}</Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent className={`p-0 transition-opacity duration-150 ${isRefreshingActive ? 'opacity-40 pointer-events-none' : ''}`}>
                  <CompletedTable rows={completed} onRowClick={(row) => setDetailRow(row)} onToggleGroup={toggleGroup} />
                </CardContent>
              </Card>
            </div>
          )}
        </>
      )}

      <RowDetailDialog
        row={detailRow} onClose={() => setDetailRow(null)}
        onKill={(row) => { setDetailRow(null); requestKill(row) }}
        cancellingQid={cancelMutation.variables ?? null}
      />

      {confirmKill && (
        <ConfirmDialog
          open={!!confirmKill} onOpenChange={(open) => !open && setConfirmKill(null)}
          title={`Cancel ${confirmKill.attribution.kind} query?`}
          description={
            (confirmKill.attribution.kind === 'cron'
              ? `This is a background ${confirmKill.attribution.cron_job || 'cron'} job. Cancelling may leave its work partial; the next tick will reconcile.`
              : `This is a system query (${confirmKill.attribution.caller_qualname}). Cancelling is rarely the right action.`)
            + ' Kill interrupts only the currently-running statement — a caller that retries will start a new query you may need to cancel again.'
          }
          confirmLabel="Cancel query" isDangerous
          onConfirm={() => { cancelMutation.mutate(confirmKill.query_id); setConfirmKill(null) }}
        />
      )}

      <ShortcutsHelp open={shortcutsOpen} onOpenChange={setShortcutsOpen} />
    </div>
  )
}
