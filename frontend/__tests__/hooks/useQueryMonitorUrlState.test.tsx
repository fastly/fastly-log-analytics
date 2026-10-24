/**
 * @vitest-environment jsdom
 *
 * Deep-link contract for /admin/queries, keyboard shortcuts, and useFilteredActive.
 */
import { act, renderHook, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'

import { useQueryMonitorUrlState } from '@/app/admin/queries/_hooks/useQueryMonitorUrlState'
import {
  useKeyboardShortcuts,
  type ShortcutBinding,
} from '@/app/admin/queries/_hooks/useKeyboardShortcuts'
import { useFilteredActive } from '@/app/admin/queries/_hooks/useFilteredActive'
import type {
  ActiveRow,
  Attribution,
  CompletedRow,
  SnapshotResponse,
} from '@/app/admin/queries/_types'

const DEFAULT_SLOW = 500

function setup(search: string) {
  const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
  const hook = renderHook(() => useQueryMonitorUrlState(DEFAULT_SLOW), {
    wrapper: withNuqsTestingAdapter({ searchParams: search, onUrlUpdate, hasMemory: true }),
  })
  return { ...hook, onUrlUpdate }
}

describe('useQueryMonitorUrlState: legacy URLs parse', () => {
  it('a clean URL yields the defaults', () => {
    const { result } = setup('')
    expect(result.current).toMatchObject({
      search: '',
      kindFilter: 'all',
      dbFilter: 'all',
      viewMode: 'all',
      slowThresholdMs: DEFAULT_SLOW,
      groupCrons: true,
    })
  })

  it('hydrates every param from a fully-specified shared link', () => {
    const { result } = setup('?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    expect(result.current).toMatchObject({
      search: 'COMPACT',
      kindFilter: 'cron',
      viewMode: 'live',
      slowThresholdMs: 250,
      dbFilter: 'DuckDB',
      groupCrons: false,
    })
  })

  it('ignores unknown or malformed values like the old hydration did', () => {
    const { result } = setup('?kind=nope&view=weird&slow=-5&db=Oracle&nogroup=0')
    expect(result.current).toMatchObject({
      kindFilter: 'all',
      viewMode: 'all',
      slowThresholdMs: DEFAULT_SLOW,
      dbFilter: 'all',
      groupCrons: true,
    })
  })

  it('parses slow with parseInt semantics (trailing junk tolerated)', () => {
    expect(setup('?slow=250ms').result.current.slowThresholdMs).toBe(250)
    expect(setup('?slow=abc').result.current.slowThresholdMs).toBe(DEFAULT_SLOW)
  })
})

describe('useQueryMonitorUrlState: writes', () => {
  it('writes non-default values with the legacy param names and formats', async () => {
    const { result, onUrlUpdate } = setup('')
    act(() => result.current.setSearch('COMPACT'))
    act(() => result.current.setKindFilter('cron'))
    act(() => result.current.setViewMode('past'))
    act(() => result.current.setSlowThresholdMs(250))
    act(() => result.current.setDbFilter('Postgres'))
    act(() => result.current.setGroupCrons(false))
    await waitFor(() => {
      expect(Object.fromEntries(onUrlUpdate.mock.calls.at(-1)![0].searchParams)).toEqual({
        q: 'COMPACT',
        kind: 'cron',
        view: 'past',
        slow: '250',
        db: 'Postgres',
        nogroup: '1',
      })
    })
    expect(onUrlUpdate.mock.calls.at(-1)![0].options.history).toBe('replace')
  })

  it('strips params that return to their defaults', async () => {
    const { result, onUrlUpdate } = setup('?q=x&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    act(() => result.current.setSearch(''))
    act(() => result.current.setKindFilter('all'))
    act(() => result.current.setViewMode('all'))
    act(() => result.current.setSlowThresholdMs(DEFAULT_SLOW))
    act(() => result.current.setDbFilter('all'))
    act(() => result.current.setGroupCrons(true))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)![0].queryString).toBe(''))
  })

  it('setGroupCrons accepts an updater function', () => {
    const { result } = setup('')
    act(() => result.current.setGroupCrons((v) => !v))
    expect(result.current.groupCrons).toBe(false)
    act(() => result.current.setGroupCrons((v) => !v))
    expect(result.current.groupCrons).toBe(true)
  })
})

function press(init: KeyboardEventInit): KeyboardEvent {
  const event = new KeyboardEvent('keydown', { bubbles: true, ...init })
  window.dispatchEvent(event)
  return event
}

describe('useKeyboardShortcuts', () => {
  it('fires the matching handler for a simple key', () => {
    const handler = vi.fn()
    const bindings: ShortcutBinding[] = [{ key: '/', description: 'slash', handler }]
    renderHook(() => useKeyboardShortcuts(bindings))
    press({ key: '/' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("fires the '?' binding when the event reports key:'?' (real Chrome on US layout)", () => {
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: '?', description: 'help', handler }]))
    press({ key: '?', shiftKey: true })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("fires the '?' binding when the event reports key:'/' + shiftKey (Playwright, non-US layouts)", () => {
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: '?', description: 'help', handler }]))
    press({ key: '/', shiftKey: true, code: 'Slash' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it("does NOT fire '?' when '/' is pressed without Shift", () => {
    const qHandler = vi.fn()
    const slashHandler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([
        { key: '?', description: 'help', handler: qHandler },
        { key: '/', description: 'slash', handler: slashHandler },
      ]),
    )
    press({ key: '/' })
    expect(qHandler).not.toHaveBeenCalled()
    expect(slashHandler).toHaveBeenCalledOnce()
  })

  it('ignores keys while focus is in an INPUT unless allowInForms is set', () => {
    const escHandler = vi.fn()
    const slashHandler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([
        { key: '/', description: 'slash', handler: slashHandler },
        { key: 'Escape', description: 'esc', handler: escHandler, allowInForms: true },
      ]),
    )
    const input = document.createElement('input')
    document.body.appendChild(input)
    input.focus()
    try {
      input.dispatchEvent(new KeyboardEvent('keydown', { key: '/', bubbles: true }))
      expect(slashHandler).not.toHaveBeenCalled()
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))
      expect(escHandler).toHaveBeenCalledOnce()
    } finally {
      document.body.removeChild(input)
    }
  })

  it('skips when meta/ctrl/alt modifiers are held', () => {
    const handler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: 'k', description: 'up', handler }]))
    press({ key: 'k', metaKey: true })
    press({ key: 'k', ctrlKey: true })
    press({ key: 'k', altKey: true })
    expect(handler).not.toHaveBeenCalled()
    press({ key: 'k' })
    expect(handler).toHaveBeenCalledOnce()
  })

  it('is disabled when enabled=false (no listener attached)', () => {
    const handler = vi.fn()
    renderHook(() =>
      useKeyboardShortcuts([{ key: '/', description: 'slash', handler }], false),
    )
    press({ key: '/' })
    expect(handler).not.toHaveBeenCalled()
  })

  it('unmounts cleanly — listener removed on cleanup', () => {
    const handler = vi.fn()
    const { unmount } = renderHook(() =>
      useKeyboardShortcuts([{ key: '/', description: 'slash', handler }]),
    )
    press({ key: '/' })
    expect(handler).toHaveBeenCalledOnce()
    unmount()
    press({ key: '/' })
    expect(handler).toHaveBeenCalledOnce()
  })
})

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
    ended_at_utc: Date.now() / 1000 + 3600,
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
      active(2, 250, { attribution: attr({ cron_run_id: 'run-A' }) }),
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
    expect(cronGroup?.query_id).toBe(2)
  })

  it('prefers a still-live representative over a just-finished one in the same run', () => {
    const liveCron = active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) })
    const justFinished = completed(2, 500, {
      attribution: attr({ cron_run_id: 'run-A' }),
      ended_at_utc: Date.now() / 1000,
    })
    const { result } = renderHook(() =>
      useFilteredActive({
        snapshot: snapshot({ active: [liveCron], completed: [justFinished] }),
        search: '',
        kindFilter: 'all',
        dbFilter: 'all',
        slowThresholdMs: 9999,
        expandedRunIds: EMPTY_EXPANDED,
        groupCrons: true,
      }),
    )
    expect(result.current.filteredActive).toHaveLength(1)
    expect(result.current.filteredActive[0].query_id).toBe(1)
    expect(result.current.filteredActive[0]._groupedCount).toBe(2)
  })
})

describe('useFilteredActive cron-group expansion', () => {
  it('expands a single run when its id is in expandedRunIds: head + children visible', () => {
    const rows = [
      active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 250, { attribution: attr({ cron_run_id: 'run-A' }) }),
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
      active(2, 200, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(3, 50, { attribution: attr({ cron_run_id: 'run-B' }) }),
      active(4, 150, { attribution: attr({ cron_run_id: 'run-B' }) }),
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
