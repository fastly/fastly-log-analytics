'use client'

import * as React from 'react'
import { useEffect } from 'react'
import { createParser, parseAsString, parseAsStringLiteral, useQueryStates } from 'nuqs'
import type { components } from '@/types/api.generated'
import type { Expect, WireParity } from '@/types/api'
import { useNowMs } from '@/hooks/useSSE'

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
export type _AttributionParity = Expect<
  WireParity<Attribution, components['schemas']['QueryAttribution']>
>
export type _ActiveRowParity = Expect<
  WireParity<ActiveRow, components['schemas']['ActiveQueryRow']>
>
export type _CompletedRowParity = Expect<
  WireParity<CompletedRow, components['schemas']['CompletedQueryRow']>
>
export type _SnapshotParity = Expect<
  WireParity<SnapshotResponse, components['schemas']['SnapshotResponse']>
>

export interface SlowQueriesResult {
  rows: CompletedRow[]
  since_hours: number
  threshold_ms: number
}

export type _SlowQueriesParity = Expect<
  WireParity<SlowQueriesResult, components['schemas']['SlowQueriesResponse']>
>

export interface MonitorConfig {
  enabled: boolean
}

export type ViewMode = 'all' | 'live' | 'past'
export type DbFilter = 'all' | 'DuckDB' | 'Postgres' | 'SQLite'

export type GroupingMeta = {
  _groupedCount?: number
  _isGroupHead?: boolean
  _expandedChild?: boolean
}

export type ActiveOrPromotedRow = ActiveRow & GroupingMeta & { _completed?: CompletedRow }
export type GroupedCompletedRow = CompletedRow & GroupingMeta
export type AnyRow = ActiveOrPromotedRow | CompletedRow
export type AttributedRow = Pick<ActiveRow, 'attribution' | 'db_type' | 'service_id'> &
  GroupingMeta

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

export function kindBadgeVariant(
  kind: AttributionKind,
): 'default' | 'secondary' | 'destructive' | 'outline' {
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

function logicalKey(event: KeyboardEvent): string {
  if (event.shiftKey && (event.key === '/' || event.code === 'Slash')) return '?'
  return event.key
}

export function useKeyboardShortcuts(
  bindings: ShortcutBinding[],
  enabled: boolean = true,
): void {
  useEffect(() => {
    if (!enabled) return
    const onKeyDown = (event: KeyboardEvent) => {
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
 */
const KINDS = ['all', 'analyst', 'admin', 'cron', 'system'] as const satisfies readonly (
  | AttributionKind
  | 'all'
)[]
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
  const setDbFilter = React.useCallback(
    (v: DbFilter) => void setParams({ db: v }),
    [setParams],
  )
  const setViewMode = React.useCallback(
    (v: ViewMode) => void setParams({ view: v }),
    [setParams],
  )
  const setSlowThresholdMs = React.useCallback(
    (n: number) => void setParams({ slow: n }),
    [setParams],
  )
  const setGroupCrons = React.useCallback(
    (v: Updater<boolean>) =>
      void setParams((prev) => ({
        nogroup: !(typeof v === 'function' ? v(!prev.nogroup) : v),
      })),
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

const JUST_FINISHED_WINDOW_S = 10
const SLOW_QUERIES_MAX = 30

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
    const cutoff = nowMs / 1000 - JUST_FINISHED_WINDOW_S
    return (snapshot?.completed ?? []).filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshot, nowMs])

  const slowQueries = React.useMemo(() => {
    const filtered = (snapshot?.completed ?? []).filter(
      (c) =>
        c.duration_ms >= slowThresholdMs &&
        matchesQueryRowFilters(c, kindFilter, dbFilter, q),
    )
    const grouped = groupCrons
      ? collapseCronRunsCompleted(filtered, expandedRunIds)
      : filtered
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
    const seen = new Set<number>()
    const combined: ActiveOrPromotedRow[] = []
    for (const r of [...active, ...justRows]) {
      if (!seen.has(r.query_id)) {
        seen.add(r.query_id)
        combined.push(r)
      }
    }
    const filtered = combined.filter((r) =>
      matchesQueryRowFilters(r, kindFilter, dbFilter, q),
    )
    const ordered = [...filtered].sort(
      (a, b) => activeRowPriority(a) - activeRowPriority(b) || b.duration_ms - a.duration_ms,
    )
    return groupCrons ? collapseCronRunsActive(ordered, expandedRunIds) : ordered
  }, [snapshot, justFinished, q, kindFilter, dbFilter, groupCrons, expandedRunIds])

  const completed = React.useMemo<GroupedCompletedRow[]>(() => {
    const filtered = (snapshot?.completed ?? []).filter((c) =>
      matchesQueryRowFilters(c, kindFilter, dbFilter, q),
    )
    return groupCrons ? collapseCronRunsCompleted(filtered, expandedRunIds) : filtered
  }, [snapshot, kindFilter, dbFilter, q, groupCrons, expandedRunIds])

  return { justFinished, filteredActive, completed, slowQueries }
}

function activeRowPriority(r: ActiveOrPromotedRow): number {
  return r.cancelled_at !== null ? 2 : r._completed ? 1 : 0
}

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
      out.push(r)
    }
    groups.get(runId)!.push(r)
  }

  for (const [runId, members] of [...groups.entries()].reverse()) {
    const idx = groupIndex.get(runId)!
    if (members.length === 1) {
      out[idx] = members[0]
      continue
    }
    const [head, ...rest] = [...members].sort(sortMembers)
    if (expandedRunIds.has(runId)) {
      out[idx] = { ...head, _groupedCount: members.length, _isGroupHead: true }
      out.splice(
        idx + 1,
        0,
        ...rest.map((r) => ({ ...r, _expandedChild: true as const })),
      )
    } else {
      out[idx] = { ...head, _groupedCount: members.length }
    }
  }
  return out
}

function collapseCronRunsActive(
  rows: ActiveOrPromotedRow[],
  expandedRunIds: ReadonlySet<string>,
): ActiveOrPromotedRow[] {
  return collapseByRunId(
    rows,
    expandedRunIds,
    (a, b) => (a._completed ? 1 : 0) - (b._completed ? 1 : 0) || b.duration_ms - a.duration_ms,
  )
}

function collapseCronRunsCompleted(
  rows: CompletedRow[],
  expandedRunIds: ReadonlySet<string>,
): GroupedCompletedRow[] {
  return collapseByRunId(rows, expandedRunIds, (a, b) => b.duration_ms - a.duration_ms)
}

export const DEFAULT_SLOW_THRESHOLD_MS = 500
export const COMPLETED_BUFFER_CAP = 500

export const formatThresholdMs = (ms: number, space = false) =>
  ms < 1000 ? `${ms}${space ? ' ' : ''}ms` : `${ms / 1000}${space ? ' ' : ''}s`

export const DB_FILTER_OPTIONS: { value: DbFilter; label: string }[] = [
  { value: 'all', label: 'All DBs' },
  { value: 'DuckDB', label: 'DuckDB' },
  { value: 'Postgres', label: 'Postgres' },
]

export const KIND_FILTER_OPTIONS: { value: AttributionKind | 'all'; label: string }[] = (
  ['all', 'analyst', 'admin', 'cron', 'system'] as const
).map((o) => ({ value: o, label: o }))

export type SlowHistoryMode =
  | 'recent'
  | 'past_1h'
  | 'past_6h'
  | 'past_12h'
  | 'past_24h'
  | 'past_7d'

export const SLOW_HISTORY_META: Record<
  SlowHistoryMode,
  {
    hours: number
    buttonLabel: string
    title: string
    emptyWindow: string
    reportLabel: string
  }
> = {
  recent: {
    hours: 24,
    buttonLabel: 'Recent',
    title: 'Live in-memory ring (~10–30 min window, clears on restart)',
    emptyWindow: '',
    reportLabel: 'Recent In-Memory Ring',
  },
  past_1h: {
    hours: 1,
    buttonLabel: '1 h',
    title: 'Persistent history — last 1 h',
    emptyWindow: '1 hour',
    reportLabel: 'Past 1h',
  },
  past_6h: {
    hours: 6,
    buttonLabel: '6 h',
    title: 'Persistent history — last 6 h',
    emptyWindow: '6 hours',
    reportLabel: 'Past 6h',
  },
  past_12h: {
    hours: 12,
    buttonLabel: '12 h',
    title: 'Persistent history — last 12 h',
    emptyWindow: '12 hours',
    reportLabel: 'Past 12h',
  },
  past_24h: {
    hours: 24,
    buttonLabel: '24 h',
    title: 'Persistent history — last 24 h',
    emptyWindow: '24 hours',
    reportLabel: 'Past 24h',
  },
  past_7d: {
    hours: 168,
    buttonLabel: '7 d',
    title: 'Persistent history — last 7 d',
    emptyWindow: '7 days',
    reportLabel: 'Past 7d',
  },
}

export const SLOW_HISTORY_MODES = Object.keys(SLOW_HISTORY_META) as SlowHistoryMode[]
