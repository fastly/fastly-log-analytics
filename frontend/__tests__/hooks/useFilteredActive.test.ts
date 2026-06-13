/**
 * @vitest-environment jsdom
 *
 * useFilteredActive — derived views over the query-monitor snapshot.
 * These tests pin the cron-grouping collapse behaviour so a regression
 * would show up immediately. The kind/db/search filters are exercised
 * implicitly via end-to-end behaviour in the page, not re-tested here.
 */
import { renderHook } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { useFilteredActive } from '@/app/admin/queries/_hooks/useFilteredActive'
import type {
  ActiveRow,
  Attribution,
  CompletedRow,
  SnapshotResponse,
} from '@/app/admin/queries/_types'

const attr = (over: Partial<Attribution> = {}): Attribution => ({
  kind: 'cron',
  label: 'log_consolidation',
  principal_id: null,
  caller_qualname: 'cron.run',
  caller_file: 'cron.py:1',
  request_path: null,
  request_id: null,
  cron_job: 'log_consolidation',
  cron_run_id: 'run-A',
  pool_slot: null,
  ...over,
})

const active = (qid: number, durationMs: number, over: Partial<ActiveRow> = {}): ActiveRow => ({
  query_id: qid,
  db_type: 'DuckDB',
  sql_preview: `SELECT ${qid}`,
  sql: null,
  sql_len: 10,
  attribution: attr(),
  service_id: 'svc-1',
  started_at_utc: 0,
  duration_ms: durationMs,
  cancellable: true,
  cancelled_at: null,
  ...over,
})

const completed = (qid: number, durationMs: number, over: Partial<CompletedRow> = {}): CompletedRow => {
  const a = active(qid, durationMs)
  // Drop the active-only fields so the literal matches CompletedRow exactly.
  return {
    query_id: a.query_id,
    db_type: a.db_type,
    sql_preview: a.sql_preview,
    sql: a.sql,
    sql_len: a.sql_len,
    attribution: a.attribution,
    service_id: a.service_id,
    started_at_utc: a.started_at_utc,
    duration_ms: a.duration_ms,
    ended_at_utc: Date.now() / 1000 + 3600, // far future so it never enters justFinished
    outcome: 'ok',
    error_type: null,
    error_message: null,
    peak_memory_mb: null,
    ...over,
  }
}

const snapshot = (over: Partial<SnapshotResponse> = {}): SnapshotResponse => ({
  last_seq: 0,
  active: [],
  completed: [],
  ...over,
})

const EMPTY_EXPANDED: ReadonlySet<string> = new Set()

describe('useFilteredActive cron-grouping', () => {
  it('returns rows unchanged when groupCrons=false', () => {
    const rows = [
      active(1, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 50, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(3, 75, { attribution: attr({ cron_run_id: 'run-A' }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: false,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(3)
    expect(result.current.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)
  })

  it('collapses cron rows sharing cron_run_id; keeps the longest-running representative', () => {
    const rows = [
      active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 250, { attribution: attr({ cron_run_id: 'run-A' }) }), // longest
      active(3, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(1)
    expect(result.current.filteredActive[0].query_id).toBe(2)
    expect(result.current.filteredActive[0]._groupedCount).toBe(3)
  })

  it('does not collapse cron rows from different cron_run_ids', () => {
    const rows = [
      active(1, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 100, { attribution: attr({ cron_run_id: 'run-B' }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(2)
    expect(result.current.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)
  })

  it('passes non-cron rows through untouched', () => {
    const rows = [
      active(1, 100, { attribution: attr({ kind: 'admin', cron_run_id: null }) }),
      active(2, 200, { attribution: attr({ kind: 'analyst', cron_run_id: null }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(2)
    expect(result.current.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)
  })

  it('passes cron rows lacking cron_run_id through untouched', () => {
    const rows = [
      active(1, 100, { attribution: attr({ cron_run_id: null }) }),
      active(2, 200, { attribution: attr({ cron_run_id: null }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(2)
  })

  it('collapses completed cron rows in slowQueries and completed', () => {
    const rows = [
      completed(1, 1000, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(2, 2000, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(3, 800, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(4, 1500, { attribution: attr({ kind: 'admin', cron_run_id: null }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ completed: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.completed).toHaveLength(2)
    expect(result.current.slowQueries).toHaveLength(2)
    const cronGroup = result.current.completed.find((r) => r._groupedCount)
    expect(cronGroup?._groupedCount).toBe(3)
    expect(cronGroup?.query_id).toBe(2) // longest of the three
  })

  it('prefers a still-live representative over a just-finished one in the same run', () => {
    const liveCron = active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) })
    // _completed promoted in the hook from the just-finished list — simulate by
    // putting it in snapshot.completed with a recent ended_at_utc so it shows up
    // in justFinished.
    const justFinished = completed(2, 500, {
      attribution: attr({ cron_run_id: 'run-A' }),
      ended_at_utc: Date.now() / 1000, // within the 10s window
    })
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: [liveCron], completed: [justFinished] }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 9999,
        expandedRunIds: EMPTY_EXPANDED, // exclude both from slow
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(1)
    // Live row wins the tie-breaker even though the promoted row is slower.
    expect(result.current.filteredActive[0].query_id).toBe(1)
    expect(result.current.filteredActive[0]._groupedCount).toBe(2)
  })
})

describe('useFilteredActive cron-group expansion', () => {
  it('expands a single run when its id is in expandedRunIds: head + children visible', () => {
    const rows = [
      active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 250, { attribution: attr({ cron_run_id: 'run-A' }) }), // head (longest)
      active(3, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: new Set(['run-A']),
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(3)
    const head = result.current.filteredActive[0]
    expect(head.query_id).toBe(2)
    expect(head._groupedCount).toBe(3)
    expect(head._isGroupHead).toBe(true)
    expect(head._expandedChild).toBeUndefined()
    const children = result.current.filteredActive.slice(1)
    expect(children.every((r) => r._expandedChild === true)).toBe(true)
    expect(children.every((r) => r._groupedCount === undefined)).toBe(true)
  })

  it('only the expanded run expands; other groups stay collapsed', () => {
    const rows = [
      active(1, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 200, { attribution: attr({ cron_run_id: 'run-A' }) }), // longest A
      active(3, 50, { attribution: attr({ cron_run_id: 'run-B' }) }),
      active(4, 150, { attribution: attr({ cron_run_id: 'run-B' }) }), // longest B
    ]
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: rows }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 500,
        expandedRunIds: new Set(['run-A']),
        groupCrons: true,
      }),
    )
    // A expanded (2 rows: head + 1 child), B collapsed (1 row)
    expect(result.current.filteredActive).toHaveLength(3)
    const heads = result.current.filteredActive.filter((r) => r._isGroupHead)
    expect(heads).toHaveLength(1)
    expect(heads[0].query_id).toBe(2)
    const children = result.current.filteredActive.filter((r) => r._expandedChild)
    expect(children).toHaveLength(1)
    expect(children[0].query_id).toBe(1)
    const collapsedReps = result.current.filteredActive.filter(
      (r) => r._groupedCount && !r._isGroupHead,
    )
    expect(collapsedReps).toHaveLength(1)
    expect(collapsedReps[0].query_id).toBe(4)
  })
})
