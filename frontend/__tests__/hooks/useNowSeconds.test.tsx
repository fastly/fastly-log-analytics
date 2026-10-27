/**
 * Tests for useNowMs, useColumnVisibility, useDebounce, useRumStatus, useStreamAggregates,
 * useTimeseriesToTraces, and useViewMetricUrlSync.
 *
 * @vitest-environment jsdom
 */
import React from 'react'
import { renderHook, act, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { useColumnVisibility } from '@/hooks/useSSE'
import { useDebounce, useRumStatus } from '@/hooks/useDashboardCards'
import { useStreamAggregates } from '@/app/sessions/stream/page'
import { useTimeseriesToTraces, type TimeseriesDataPoint } from '@/lib/utils'
import {
  useFilteredActive, useKeyboardShortcuts, useQueryMonitorUrlState,
  type ActiveRow, type Attribution, type CompletedRow, type ShortcutBinding, type SnapshotResponse,
} from '@/app/admin/queries/page'
import { server } from '@/__tests__/helpers/msw/handlers'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetMetric = vi.fn()

const mockStore = {
  addFilter: mockAddFilter, clearFilters: mockClearFilters, setRange: mockSetRange,
  setRelativeRange: vi.fn(), toggleEdgeOnly: vi.fn(), toggleCompareMode: vi.fn(),
  setCompareRange: vi.fn(), edgeOnly: false, compareMode: false,
}
const mockServiceStore = { activeServiceId: 'test-service-id', services: [] }
vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useFilterStore: Object.assign(
    (selector?: (s: typeof mockStore) => unknown) => (typeof selector === 'function' ? selector(mockStore) : mockStore),
    { getState: () => mockStore },
  ),
  useServiceStore: Object.assign(
    (selector?: (s: typeof mockServiceStore) => unknown) => (typeof selector === 'function' ? selector(mockServiceStore) : mockServiceStore),
    { getState: () => mockServiceStore },
  ),
}))

describe('useRumStatus', () => {
  it('maps 403 to an unauthorized-disabled status', async () => {
    server.use(http.get('*/api/services/svc/rum/status', () => HttpResponse.json({}, { status: 403 })))
    const { result } = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(result.current.data).toEqual({ enabled: false, unauthorized: true }))
  })

  it('returns parsed JSON on success and null on other failures', async () => {
    server.use(
      http.get('*/api/services/svc/rum/status', () => HttpResponse.json({ enabled: true })),
      http.get('*/api/services/svc2/rum/status', () => HttpResponse.json({}, { status: 500 })),
    )
    const ok = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(ok.result.current.data).toEqual({ enabled: true }))

    const bad = renderHook(() => useRumStatus('svc2'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(bad.result.current.isSuccess).toBe(true))
    expect(bad.result.current.data).toBeNull()
  })

  it('does not fetch without a service id', async () => {
    let hits = 0
    server.use(http.get('*/api/services/*/rum/status', () => { hits += 1; return HttpResponse.json({}) }))
    renderHook(() => useRumStatus(null), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await new Promise((r) => setTimeout(r, 50))
    expect(hits).toBe(0)
  })
})

describe('useNowMs', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-06-15T00:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('returns a value close to Date.now() on first render', async () => {
    const { useNowMs } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useNowMs())
    expect(Number.isFinite(result.current)).toBe(true)
    expect(result.current).toBeGreaterThan(0)
  })

  it('updates after the 1s interval fires', async () => {
    const { useNowMs } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useNowMs())
    const before = result.current
    await act(async () => {
      vi.setSystemTime(Date.now() + 1500)
      await vi.advanceTimersByTimeAsync(1500)
    })
    expect(result.current).toBeGreaterThan(before)
    expect(result.current - before).toBeGreaterThanOrEqual(1000)
  })

  it('multiple subscribers share the same snapshot value', async () => {
    const { useNowMs } = await import('@/hooks/useSSE')
    const { result: a } = renderHook(() => useNowMs())
    const { result: b } = renderHook(() => useNowMs())
    expect(a.current).toBe(b.current)
    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(a.current).toBe(b.current)
  })

  it('keeps ticking while at least one subscriber is mounted', async () => {
    const { useNowMs } = await import('@/hooks/useSSE')
    const { result: a, unmount: unmountA } = renderHook(() => useNowMs())
    const { result: b } = renderHook(() => useNowMs())
    unmountA()
    const before = b.current
    for (let i = 0; i < 2; i++) {
      await act(async () => {
        vi.setSystemTime(Date.now() + 1000)
        await vi.advanceTimersByTimeAsync(1000)
      })
    }
    expect(b.current).toBeGreaterThan(before)
    expect(typeof a.current).toBe('number')
  })

  it('cleans up the underlying interval when the last subscriber unmounts', async () => {
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
    const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')
    const { useNowMs } = await import('@/hooks/useSSE')
    const { unmount } = renderHook(() => useNowMs())
    unmount()
    await act(async () => { await Promise.resolve() })
    expect(clearIntervalSpy).toHaveBeenCalled()
    setIntervalSpy.mockRestore()
    clearIntervalSpy.mockRestore()
  })
})

describe('useColumnVisibility', () => {
  it('initializes to empty object by default or accepts initial state', () => {
    expect(renderHook(() => useColumnVisibility()).result.current[0]).toEqual({})
    expect(renderHook(() => useColumnVisibility({ col1: false, col2: true })).result.current[0]).toEqual({ col1: false, col2: true })
  })

  it.each([true, false])('handleChange sets a column to %s', (val) => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => { result.current[2]('col1', val) })
    expect(result.current[0]).toEqual({ col1: val })
  })

  it('handleChange accumulates changes across and within act calls without overwriting other columns', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: true }))
    act(() => { result.current[2]('col2', false) })
    expect(result.current[0]).toEqual({ col1: true, col2: false })

    const { result: r2 } = renderHook(() => useColumnVisibility())
    act(() => {
      r2.current[2]('col1', true)
      r2.current[2]('col2', false)
    })
    expect(r2.current[0]).toEqual({ col1: true, col2: false })
  })

  it('setVisibility replaces the full state and handleChange is stable across renders', () => {
    const { result, rerender } = renderHook(() => useColumnVisibility({ col1: true, col2: true }))
    const handleChangeBefore = result.current[2]
    act(() => { result.current[1]({ col3: false }) })
    expect(result.current[0]).toEqual({ col3: false })
    rerender()
    expect(result.current[2]).toBe(handleChangeBefore)
  })
})

describe('useDebounce', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('returns the initial value immediately', () => {
    expect(renderHook(() => useDebounce('initial', 300)).result.current).toBe('initial')
  })

  it('does not update before the delay elapses and updates once delay elapses', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), { initialProps: { val: 'first' } })
    rerender({ val: 'second' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('first')
    act(() => { vi.advanceTimersByTime(1) })
    expect(result.current).toBe('second')
  })

  it('resets the timer on rapid changes, only fires once', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), { initialProps: { val: 'a' } })
    rerender({ val: 'b' })
    act(() => { vi.advanceTimersByTime(100) })
    rerender({ val: 'c' })
    act(() => { vi.advanceTimersByTime(100) })
    rerender({ val: 'd' })
    expect(result.current).toBe('a')
    act(() => { vi.advanceTimersByTime(300) })
    expect(result.current).toBe('d')
  })

  it('uses 300ms default delay', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val), { initialProps: { val: 'x' } })
    rerender({ val: 'y' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('x')
    act(() => { vi.advanceTimersByTime(1) })
    expect(result.current).toBe('y')
  })
})

describe('useStreamAggregates', () => {
  it('returns null when rows are undefined or empty', () => {
    expect(renderHook(() => useStreamAggregates(undefined)).result.current).toBeNull()
    expect(renderHook(() => useStreamAggregates([])).result.current).toBeNull()
  })

  it('aggregates pure video rows (cmcd_ot=v)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'v', cmcd_br: 4000, cmcd_bl: 3000, cmcd_tb: 6000 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'v', cmcd_br: 6000, cmcd_bl: 4000, cmcd_tb: 6000 },
    ]
    const agg = renderHook(() => useStreamAggregates(rows)).result.current!
    expect(agg).not.toBeNull()
    expect(agg.summary).toMatchObject({ videoRequests: 2, totalRequests: 2, avgBitrate: 5000, topBitrate: 6000 })
    expect(agg.summary.utilization).toBeCloseTo(5000 / 6000)
    expect(agg.timeline).toHaveLength(2)
  })

  it('includes muxed audio/video rows (cmcd_ot=av)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'av', cmcd_br: 3500, cmcd_bl: 2500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'av', cmcd_br: 4500, cmcd_bl: 3500 },
    ]
    const agg = renderHook(() => useStreamAggregates(rows)).result.current!
    expect(agg).not.toBeNull()
    expect(agg.summary).toMatchObject({ videoRequests: 2, avgBitrate: 4000, topBitrate: 4500 })
    expect(agg.summary.utilization).toBeCloseTo(4000 / 4500)
  })

  it('falls back to rows with bitrate/buffer when no explicit v or av ot tag exists', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'm', cmcd_br: 2500, cmcd_bl: 4500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: null, cmcd_br: null },
    ]
    const agg = renderHook(() => useStreamAggregates(rows)).result.current!
    expect(agg).not.toBeNull()
    expect(agg.summary).toMatchObject({ totalRequests: 2, videoRequests: 1, avgBitrate: 2500, topBitrate: 2500, bufferHealthPct: 100 })
    expect(agg.timeline).toHaveLength(1)
  })
})

const SAMPLE_DATA = [
  { time: '2026-05-18T20:00:00Z', p50: 30, p95: 80 },
  { time: '2026-05-18T20:01:00Z', p50: 28, p95: 78 },
]

describe('useTimeseriesToTraces', () => {
  it('omits stackgroup and marker when not applicable and includes them when specified', () => {
    const tDefault = renderHook(() => useTimeseriesToTraces(SAMPLE_DATA, [{ key: 'p50', name: 'P50', color: '#000' }])).result.current[0] as Record<string, unknown>
    expect('stackgroup' in tDefault).toBe(false)

    const tScatter = renderHook(() => useTimeseriesToTraces(SAMPLE_DATA, [{ key: 'p50', name: 'P50', color: '#000', type: 'scatter' }])).result.current[0] as Record<string, unknown>
    expect('marker' in tScatter).toBe(false)

    const tBar = renderHook(() => useTimeseriesToTraces(SAMPLE_DATA, [{ key: 'p50', name: 'P50', color: '#abc', type: 'bar' }])).result.current[0] as Record<string, unknown>
    expect(tBar.marker).toEqual({ color: '#abc' })

    const tStack = renderHook(() => useTimeseriesToTraces(SAMPLE_DATA, [{ key: 'p50', name: 'P50', color: '#000', stackgroup: 'a' }])).result.current[0] as Record<string, unknown>
    expect(tStack.stackgroup).toBe('a')
  })

  it('produces no trace properties with the literal value undefined', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#000' },
        { key: 'p95', name: 'P95', color: '#f00', stackgroup: 'errors' },
      ]),
    )
    for (const trace of result.current) {
      for (const [k, v] of Object.entries(trace as Record<string, unknown>)) {
        expect(v, `trace property "${k}" was undefined`).not.toBeUndefined()
      }
    }
  })

  it('returns empty array when data is undefined or empty', () => {
    expect(renderHook(() => useTimeseriesToTraces(undefined, [{ key: 'p50', name: 'P50', color: '#000' }])).result.current).toEqual([])
    expect(renderHook(() => useTimeseriesToTraces([], [{ key: 'p50', name: 'P50', color: '#000' }])).result.current).toEqual([])
  })

  it('handles ts alias and coerces missing/null y values to 0 (no NaN in Plotly y array)', () => {
    const dataWithTs: Array<TimeseriesDataPoint & { p50: number }> = [
      { ts: '2026-05-18T20:00:00Z', p50: 30 },
      { ts: '2026-05-18T20:01:00Z', p50: 28 },
    ]
    const rTs = renderHook(() => useTimeseriesToTraces(dataWithTs, [{ key: 'p50', name: 'P50', color: '#000' }])).result.current
    expect(rTs[0]).toBeDefined()
    expect((rTs[0] as { y: number[] }).y).toEqual([30, 28])

    const sparse = [
      { time: '2026-05-18T20:00:00Z', p50: 30 },
      { time: '2026-05-18T20:01:00Z' },
      { time: '2026-05-18T20:02:00Z', p50: null },
    ]
    const y = (renderHook(() => useTimeseriesToTraces(sparse, [{ key: 'p50', name: 'P50', color: '#000' }])).result.current[0] as { y: number[] }).y
    expect(y).toEqual([30, 0, 0])
    expect(y.every((v) => Number.isFinite(v))).toBe(true)
  })
})

const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
function viewMetricAdapter(search = '') {
  const QueryWrapper = makeQueryWrapper(createTestQueryClient())
  const NuqsWrapper = withNuqsTestingAdapter({ searchParams: search, hasMemory: true, onUrlUpdate, resetUrlUpdateQueueOnMount: false })
  return { wrapper: ({ children }: { children: React.ReactNode }) => <QueryWrapper><NuqsWrapper>{children}</NuqsWrapper></QueryWrapper> }
}

describe('useViewMetricUrlSync', () => {
  beforeEach(() => { vi.clearAllMocks(); window.history.pushState({}, '', '/') })
  afterEach(() => { window.history.pushState({}, '', '/') })

  it('does nothing when there are no URL params', async () => {
    const { useViewMetricUrlSync } = await import('@/components/ReportLayout')
    renderHook(() => useViewMetricUrlSync({ setMetric: mockSetMetric }), viewMetricAdapter())
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockSetRange).not.toHaveBeenCalled()
    expect(mockSetMetric).not.toHaveBeenCalled()
  })

  it('parses metric param and calls setMetric', async () => {
    const { useViewMetricUrlSync } = await import('@/components/ReportLayout')
    renderHook(() => useViewMetricUrlSync({ setMetric: mockSetMetric }), viewMetricAdapter('?metric=bandwidth'))
    expect(mockSetMetric).toHaveBeenCalledWith('bandwidth')
  })

  it('strips metric param from URL after processing while leaving unrelated params untouched', async () => {
    const { useViewMetricUrlSync } = await import('@/components/ReportLayout')
    renderHook(() => useViewMetricUrlSync({ setMetric: mockSetMetric }), viewMetricAdapter('?metric=requests&other=keep'))
    await waitFor(() => expect(onUrlUpdate).toHaveBeenCalled())
    const remaining = onUrlUpdate.mock.calls.at(-1)![0].searchParams
    expect(remaining.get('metric')).toBeNull()
    expect(remaining.get('other')).toBe('keep')
  })

  it('loads a saved view by id and clears URL view param', async () => {
    let viewsRequested = false
    server.use(
      http.get('*/api/views/test-service-id', () => {
        viewsRequested = true
        return HttpResponse.json([{
          id: 'view-123', start_time: '2026-01-01T00:00:00Z', end_time: '2026-01-02T00:00:00Z',
          filters_json: JSON.stringify([{ column: 'status', value: '404', mode: 'include' }]),
        }])
      }),
    )
    const { useViewMetricUrlSync } = await import('@/components/ReportLayout')
    renderHook(() => useViewMetricUrlSync(), viewMetricAdapter('?view=view-123'))
    await waitFor(() => expect(viewsRequested).toBe(true))
    await waitFor(() => expect(mockSetRange).toHaveBeenCalledWith('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'))
    expect(mockAddFilter).toHaveBeenCalledWith('status', '404', 'include')
    await waitFor(() => expect(onUrlUpdate).toHaveBeenCalled())
    expect(onUrlUpdate.mock.calls.at(-1)![0].searchParams.get('view')).toBeNull()
  })
})

const DEFAULT_SLOW = 500
function setupQueryMonitorUrl(search: string) {
  const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
  const hook = renderHook(() => useQueryMonitorUrlState(DEFAULT_SLOW), {
    wrapper: withNuqsTestingAdapter({ searchParams: search, onUrlUpdate, hasMemory: true }),
  })
  return { ...hook, onUrlUpdate }
}

describe('useQueryMonitorUrlState: legacy URLs parse', () => {
  it('a clean URL yields the defaults', () => {
    expect(setupQueryMonitorUrl('').result.current).toMatchObject({
      search: '', kindFilter: 'all', dbFilter: 'all', viewMode: 'all', slowThresholdMs: DEFAULT_SLOW, groupCrons: true,
    })
  })

  it('hydrates every param from a fully-specified shared link', () => {
    expect(setupQueryMonitorUrl('?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1').result.current).toMatchObject({
      search: 'COMPACT', kindFilter: 'cron', viewMode: 'live', slowThresholdMs: 250, dbFilter: 'DuckDB', groupCrons: false,
    })
  })

  it('ignores unknown or malformed values like the old hydration did', () => {
    expect(setupQueryMonitorUrl('?kind=nope&view=weird&slow=-5&db=Oracle&nogroup=0').result.current).toMatchObject({
      kindFilter: 'all', viewMode: 'all', slowThresholdMs: DEFAULT_SLOW, dbFilter: 'all', groupCrons: true,
    })
  })

  it('parses slow with parseInt semantics (trailing junk tolerated)', () => {
    expect(setupQueryMonitorUrl('?slow=250ms').result.current.slowThresholdMs).toBe(250)
    expect(setupQueryMonitorUrl('?slow=abc').result.current.slowThresholdMs).toBe(DEFAULT_SLOW)
  })
})

describe('useQueryMonitorUrlState: writes', () => {
  it('writes non-default values with the legacy param names and formats', async () => {
    const { result, onUrlUpdate } = setupQueryMonitorUrl('')
    act(() => {
      result.current.setSearch('COMPACT')
      result.current.setKindFilter('cron')
      result.current.setViewMode('past')
      result.current.setSlowThresholdMs(250)
      result.current.setDbFilter('Postgres')
      result.current.setGroupCrons(false)
    })
    await waitFor(() => {
      expect(Object.fromEntries(onUrlUpdate.mock.calls.at(-1)![0].searchParams)).toEqual({
        q: 'COMPACT', kind: 'cron', view: 'past', slow: '250', db: 'Postgres', nogroup: '1',
      })
    })
    expect(onUrlUpdate.mock.calls.at(-1)![0].options.history).toBe('replace')
  })

  it('strips params that return to their defaults', async () => {
    const { result, onUrlUpdate } = setupQueryMonitorUrl('?q=x&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    act(() => {
      result.current.setSearch('')
      result.current.setKindFilter('all')
      result.current.setViewMode('all')
      result.current.setSlowThresholdMs(DEFAULT_SLOW)
      result.current.setDbFilter('all')
      result.current.setGroupCrons(true)
    })
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)![0].queryString).toBe(''))
  })

  it('setGroupCrons accepts an updater function', () => {
    const { result } = setupQueryMonitorUrl('')
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
  it('fires the matching handler for a simple key and handles ? variations', () => {
    const slashHandler = vi.fn()
    const qHandler = vi.fn()
    const bindings: ShortcutBinding[] = [
      { key: '/', description: 'slash', handler: slashHandler },
      { key: '?', description: 'help', handler: qHandler },
    ]
    renderHook(() => useKeyboardShortcuts(bindings))
    press({ key: '/' })
    expect(slashHandler).toHaveBeenCalledOnce()
    expect(qHandler).not.toHaveBeenCalled()

    press({ key: '?', shiftKey: true })
    expect(qHandler).toHaveBeenCalledTimes(1)
    press({ key: '/', shiftKey: true, code: 'Slash' })
    expect(qHandler).toHaveBeenCalledTimes(2)
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

  it('skips when meta/ctrl/alt modifiers are held, when enabled=false, or after unmount', () => {
    const handler = vi.fn()
    const { unmount } = renderHook(() => useKeyboardShortcuts([{ key: 'k', description: 'up', handler }]))
    press({ key: 'k', metaKey: true })
    press({ key: 'k', ctrlKey: true })
    press({ key: 'k', altKey: true })
    expect(handler).not.toHaveBeenCalled()
    press({ key: 'k' })
    expect(handler).toHaveBeenCalledOnce()
    unmount()
    press({ key: 'k' })
    expect(handler).toHaveBeenCalledOnce()

    const disabledHandler = vi.fn()
    renderHook(() => useKeyboardShortcuts([{ key: '/', description: 'slash', handler: disabledHandler }], false))
    press({ key: '/' })
    expect(disabledHandler).not.toHaveBeenCalled()
  })
})

const attr = (over: Partial<Attribution> = {}): Attribution => ({
  kind: 'cron', label: 'log_consolidation', principal_id: null, caller_qualname: 'cron.run',
  caller_file: 'cron.py:1', request_path: null, request_id: null, cron_job: 'log_consolidation',
  cron_run_id: 'run-A', pool_slot: null, ...over,
})

const active = (qid: number, durationMs: number, over: Partial<ActiveRow> = {}): ActiveRow => ({
  query_id: qid, db_type: 'DuckDB', sql_preview: `SELECT ${qid}`, sql: null, sql_len: 10,
  attribution: attr(), service_id: 'svc-1', started_at_utc: 0, duration_ms: durationMs,
  cancellable: true, cancelled_at: null, ...over,
})

const completed = (qid: number, durationMs: number, over: Partial<CompletedRow> = {}): CompletedRow => ({
  ...active(qid, durationMs),
  ended_at_utc: Date.now() / 1000 + 3600, outcome: 'ok', error_type: null, error_message: null, peak_memory_mb: null,
  ...over,
})

const snapshot = (over: Partial<SnapshotResponse> = {}): SnapshotResponse => ({
  last_seq: 0, active: [], completed: [], ...over,
})

const EMPTY_EXPANDED: ReadonlySet<string> = new Set()

function runFiltered(over: Partial<Parameters<typeof useFilteredActive>[0]>) {
  return renderHook(() =>
    useFilteredActive({
      snapshot: snapshot(), search: '', kindFilter: 'all', dbFilter: 'all',
      slowThresholdMs: 500, expandedRunIds: EMPTY_EXPANDED, groupCrons: true, ...over,
    }),
  ).result.current
}

describe('useFilteredActive cron-grouping', () => {
  it('returns rows unchanged when groupCrons=false', () => {
    const rows = [100, 50, 75].map((ms, i) => active(i + 1, ms, { attribution: attr({ cron_run_id: 'run-A' }) }))
    const res = runFiltered({ snapshot: snapshot({ active: rows }), groupCrons: false })
    expect(res.filteredActive).toHaveLength(3)
    expect(res.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)
  })

  it('collapses cron rows sharing cron_run_id; keeps the longest-running representative', () => {
    const rows = [50, 250, 100].map((ms, i) => active(i + 1, ms, { attribution: attr({ cron_run_id: 'run-A' }) }))
    const res = runFiltered({ snapshot: snapshot({ active: rows }) })
    expect(res.filteredActive).toHaveLength(1)
    expect(res.filteredActive[0]).toMatchObject({ query_id: 2, _groupedCount: 3 })
  })

  it('does not collapse cron rows from different cron_run_ids, non-cron rows, or cron rows lacking cron_run_id', () => {
    const diffRuns = [
      active(1, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 100, { attribution: attr({ cron_run_id: 'run-B' }) }),
    ]
    const r1 = runFiltered({ snapshot: snapshot({ active: diffRuns }) })
    expect(r1.filteredActive).toHaveLength(2)
    expect(r1.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)

    const nonCron = [
      active(1, 100, { attribution: attr({ kind: 'admin', cron_run_id: null }) }),
      active(2, 200, { attribution: attr({ kind: 'analyst', cron_run_id: null }) }),
    ]
    const r2 = runFiltered({ snapshot: snapshot({ active: nonCron }) })
    expect(r2.filteredActive).toHaveLength(2)
    expect(r2.filteredActive.every((r) => r._groupedCount === undefined)).toBe(true)

    const nullRun = [
      active(1, 100, { attribution: attr({ cron_run_id: null }) }),
      active(2, 200, { attribution: attr({ cron_run_id: null }) }),
    ]
    expect(runFiltered({ snapshot: snapshot({ active: nullRun }) }).filteredActive).toHaveLength(2)
  })

  it('collapses completed cron rows in slowQueries and completed', () => {
    const rows = [
      completed(1, 1000, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(2, 2000, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(3, 800, { attribution: attr({ cron_run_id: 'run-A' }) }),
      completed(4, 1500, { attribution: attr({ kind: 'admin', cron_run_id: null }) }),
    ]
    const res = runFiltered({ snapshot: snapshot({ completed: rows }) })
    expect(res.completed).toHaveLength(2)
    expect(res.slowQueries).toHaveLength(2)
    expect(res.completed.find((r) => r._groupedCount)).toMatchObject({ _groupedCount: 3, query_id: 2 })
  })

  it('prefers a still-live representative over a just-finished one in the same run', () => {
    const liveCron = active(1, 50, { attribution: attr({ cron_run_id: 'run-A' }) })
    const justFinished = completed(2, 500, { attribution: attr({ cron_run_id: 'run-A' }), ended_at_utc: Date.now() / 1000 })
    const res = runFiltered({ snapshot: snapshot({ active: [liveCron], completed: [justFinished] }), slowThresholdMs: 9999 })
    expect(res.filteredActive).toHaveLength(1)
    expect(res.filteredActive[0]).toMatchObject({ query_id: 1, _groupedCount: 2 })
  })
})

describe('useFilteredActive cron-group expansion', () => {
  it('expands a single run when its id is in expandedRunIds: head + children visible', () => {
    const rows = [50, 250, 100].map((ms, i) => active(i + 1, ms, { attribution: attr({ cron_run_id: 'run-A' }) }))
    const res = runFiltered({ snapshot: snapshot({ active: rows }), expandedRunIds: new Set(['run-A']) })
    expect(res.filteredActive).toHaveLength(3)
    expect(res.filteredActive[0]).toMatchObject({ query_id: 2, _groupedCount: 3, _isGroupHead: true })
    expect(res.filteredActive[0]._expandedChild).toBeUndefined()
    const children = res.filteredActive.slice(1)
    expect(children.every((r) => r._expandedChild === true && r._groupedCount === undefined)).toBe(true)
  })

  it('only the expanded run expands; other groups stay collapsed', () => {
    const rows = [
      active(1, 100, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(2, 200, { attribution: attr({ cron_run_id: 'run-A' }) }),
      active(3, 50, { attribution: attr({ cron_run_id: 'run-B' }) }),
      active(4, 150, { attribution: attr({ cron_run_id: 'run-B' }) }),
    ]
    const res = runFiltered({ snapshot: snapshot({ active: rows }), expandedRunIds: new Set(['run-A']) })
    expect(res.filteredActive).toHaveLength(3)
    expect(res.filteredActive.filter((r) => r._isGroupHead).map((r) => r.query_id)).toEqual([2])
    expect(res.filteredActive.filter((r) => r._expandedChild).map((r) => r.query_id)).toEqual([1])
    expect(res.filteredActive.filter((r) => r._groupedCount && !r._isGroupHead).map((r) => r.query_id)).toEqual([4])
  })
})
