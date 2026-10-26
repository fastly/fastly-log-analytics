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
import { useDebounce } from '@/hooks/useDashboardCards'
import { useRumStatus } from '@/hooks/useDashboardCards'
import { useStreamAggregates } from '@/app/sessions/stream/page'
import { useTimeseriesToTraces, type TimeseriesDataPoint } from "@/lib/utils"
import {
  useFilteredActive,
  useKeyboardShortcuts,
  useQueryMonitorUrlState,
  type ActiveRow,
  type Attribution,
  type CompletedRow,
  type ShortcutBinding,
  type SnapshotResponse,
} from '@/app/admin/queries/page'
import { server } from '@/__tests__/helpers/msw/handlers'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetMetric = vi.fn()

const mockStore = {
  addFilter: mockAddFilter,
  clearFilters: mockClearFilters,
  setRange: mockSetRange,
  setRelativeRange: vi.fn(),
  toggleEdgeOnly: vi.fn(),
  toggleCompareMode: vi.fn(),
  setCompareRange: vi.fn(),
  edgeOnly: false,
  compareMode: false,
}
const mockServiceStore = { activeServiceId: 'test-service-id', services: [] }
vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useFilterStore: Object.assign(
    (selector?: (s: typeof mockStore) => unknown) =>
      typeof selector === 'function' ? selector(mockStore) : mockStore,
    { getState: () => mockStore },
  ),
  useServiceStore: Object.assign(
    (selector?: (s: typeof mockServiceStore) => unknown) =>
      typeof selector === 'function' ? selector(mockServiceStore) : mockServiceStore,
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
    server.use(
      http.get('*/api/services/*/rum/status', () => {
        hits += 1
        return HttpResponse.json({})
      }),
    )
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

  afterEach(() => {
    vi.useRealTimers()
  })

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
    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })
    await act(async () => {
      vi.setSystemTime(Date.now() + 1000)
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(b.current).toBeGreaterThan(before)
    expect(typeof a.current).toBe('number')
  })

  it('cleans up the underlying interval when the last subscriber unmounts', async () => {
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
    const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

    const { useNowMs } = await import('@/hooks/useSSE')
    const { unmount } = renderHook(() => useNowMs())

    unmount()

    await act(async () => {
      await Promise.resolve()
    })

    expect(clearIntervalSpy).toHaveBeenCalled()
    setIntervalSpy.mockRestore()
    clearIntervalSpy.mockRestore()
  })
})

describe('useColumnVisibility', () => {
  it('initializes to empty object by default', () => {
    const { result } = renderHook(() => useColumnVisibility())
    expect(result.current[0]).toEqual({})
  })

  it('accepts an initial visibility state', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: false, col2: true }))
    expect(result.current[0]).toEqual({ col1: false, col2: true })
  })

  it('handleChange sets a column to true', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => { result.current[2]('col1', true) })
    expect(result.current[0]).toEqual({ col1: true })
  })

  it('handleChange sets a column to false', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => { result.current[2]('col1', false) })
    expect(result.current[0]).toEqual({ col1: false })
  })

  it('handleChange accumulates changes without overwriting other columns', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: true }))
    act(() => { result.current[2]('col2', false) })
    expect(result.current[0]).toEqual({ col1: true, col2: false })
  })

  it('multiple handleChange calls within one act accumulate', () => {
    const { result } = renderHook(() => useColumnVisibility())
    act(() => {
      result.current[2]('col1', true)
      result.current[2]('col2', false)
    })
    expect(result.current[0]).toEqual({ col1: true, col2: false })
  })

  it('setVisibility replaces the full state', () => {
    const { result } = renderHook(() => useColumnVisibility({ col1: true, col2: true }))
    act(() => { result.current[1]({ col3: false }) })
    expect(result.current[0]).toEqual({ col3: false })
  })

  it('handleChange is stable across renders', () => {
    const { result, rerender } = renderHook(() => useColumnVisibility())
    const handleChangeBefore = result.current[2]
    rerender()
    expect(result.current[2]).toBe(handleChangeBefore)
  })
})

describe('useDebounce', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('returns the initial value immediately', () => {
    const { result } = renderHook(() => useDebounce('initial', 300))
    expect(result.current).toBe('initial')
  })

  it('does not update before the delay elapses', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'first' },
    })
    rerender({ val: 'second' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('first')
  })

  it('updates after the delay elapses', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'first' },
    })
    rerender({ val: 'second' })
    act(() => { vi.advanceTimersByTime(300) })
    expect(result.current).toBe('second')
  })

  it('resets the timer on rapid changes, only fires once', () => {
    const { result, rerender } = renderHook(({ val }) => useDebounce(val, 300), {
      initialProps: { val: 'a' },
    })
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
    const { result, rerender } = renderHook(({ val }) => useDebounce(val), {
      initialProps: { val: 'x' },
    })
    rerender({ val: 'y' })
    act(() => { vi.advanceTimersByTime(299) })
    expect(result.current).toBe('x')
    act(() => { vi.advanceTimersByTime(1) })
    expect(result.current).toBe('y')
  })
})

describe('useStreamAggregates', () => {
  it('returns null when rows are undefined or empty', () => {
    const { result: r1 } = renderHook(() => useStreamAggregates(undefined))
    expect(r1.current).toBeNull()

    const { result: r2 } = renderHook(() => useStreamAggregates([]))
    expect(r2.current).toBeNull()
  })

  it('aggregates pure video rows (cmcd_ot=v)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'v', cmcd_br: 4000, cmcd_bl: 3000, cmcd_tb: 6000 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'v', cmcd_br: 6000, cmcd_bl: 4000, cmcd_tb: 6000 },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.videoRequests).toBe(2)
    expect(agg.summary.totalRequests).toBe(2)
    expect(agg.summary.avgBitrate).toBe(5000)
    expect(agg.summary.topBitrate).toBe(6000)
    expect(agg.summary.utilization).toBeCloseTo(5000 / 6000)
    expect(agg.timeline).toHaveLength(2)
  })

  it('includes muxed audio/video rows (cmcd_ot=av)', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'av', cmcd_br: 3500, cmcd_bl: 2500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: 'av', cmcd_br: 4500, cmcd_bl: 3500 },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.videoRequests).toBe(2)
    expect(agg.summary.avgBitrate).toBe(4000)
    expect(agg.summary.topBitrate).toBe(4500)
    expect(agg.summary.utilization).toBeCloseTo(4000 / 4500)
  })

  it('falls back to rows with bitrate/buffer when no explicit v or av ot tag exists', () => {
    const rows = [
      { timestamp: '2026-09-24T15:00:00Z', cmcd_ot: 'm', cmcd_br: 2500, cmcd_bl: 4500 },
      { timestamp: '2026-09-24T15:00:02Z', cmcd_ot: null, cmcd_br: null },
    ]
    const { result } = renderHook(() => useStreamAggregates(rows))
    expect(result.current).not.toBeNull()
    const agg = result.current!

    expect(agg.summary.totalRequests).toBe(2)
    expect(agg.summary.videoRequests).toBe(1)
    expect(agg.summary.avgBitrate).toBe(2500)
    expect(agg.summary.topBitrate).toBe(2500)
    expect(agg.summary.bufferHealthPct).toBe(100)
    expect(agg.timeline).toHaveLength(1)
  })
})

const SAMPLE_DATA = [
  { time: '2026-05-18T20:00:00Z', p50: 30, p95: 80 },
  { time: '2026-05-18T20:01:00Z', p50: 28, p95: 78 },
]

describe('useTimeseriesToTraces', () => {
  it('omits stackgroup property entirely when not supplied (no explicit undefined)', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#000' },
      ])
    )

    const trace = result.current[0] as Record<string, unknown>
    expect('stackgroup' in trace).toBe(false)
  })

  it('omits marker property for non-bar traces (no explicit undefined)', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#000', type: 'scatter' },
      ])
    )

    const trace = result.current[0] as Record<string, unknown>
    expect('marker' in trace).toBe(false)
  })

  it('includes marker when type=bar (the only case it should ever be set)', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#abc', type: 'bar' },
      ])
    )

    const trace = result.current[0] as Record<string, unknown>
    expect(trace.marker).toEqual({ color: '#abc' })
  })

  it('includes stackgroup ONLY when supplied (presence-based, not undefined)', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#000', stackgroup: 'a' },
      ])
    )

    const trace = result.current[0] as Record<string, unknown>
    expect(trace.stackgroup).toBe('a')
  })

  it('produces no trace properties with the literal value undefined', () => {
    const { result } = renderHook(() =>
      useTimeseriesToTraces(SAMPLE_DATA, [
        { key: 'p50', name: 'P50', color: '#000' },
        { key: 'p95', name: 'P95', color: '#f00', stackgroup: 'errors' },
      ])
    )

    for (const trace of result.current) {
      const tr = trace as Record<string, unknown>
      for (const [k, v] of Object.entries(tr)) {
        expect(v, `trace property "${k}" was undefined`).not.toBeUndefined()
      }
    }
  })

  it('returns empty array when data is undefined or empty', () => {
    const { result: r1 } = renderHook(() =>
      useTimeseriesToTraces(undefined, [{ key: 'p50', name: 'P50', color: '#000' }])
    )
    expect(r1.current).toEqual([])

    const { result: r2 } = renderHook(() =>
      useTimeseriesToTraces([], [{ key: 'p50', name: 'P50', color: '#000' }])
    )
    expect(r2.current).toEqual([])
  })

  it('handles ts alias (some endpoints emit ts instead of time)', () => {
    const dataWithTs: Array<TimeseriesDataPoint & { p50: number }> = [
      { ts: '2026-05-18T20:00:00Z', p50: 30 },
      { ts: '2026-05-18T20:01:00Z', p50: 28 },
    ]
    const { result } = renderHook(() =>
      useTimeseriesToTraces(dataWithTs, [{ key: 'p50', name: 'P50', color: '#000' }])
    )
    expect(result.current[0]).toBeDefined()
    expect((result.current[0] as { y: number[] }).y).toEqual([30, 28])
  })

  it('coerces missing/null y values to 0 (no NaN in Plotly y array)', () => {
    const sparse = [
      { time: '2026-05-18T20:00:00Z', p50: 30 },
      { time: '2026-05-18T20:01:00Z' },
      { time: '2026-05-18T20:02:00Z', p50: null },
    ]
    const { result } = renderHook(() =>
      useTimeseriesToTraces(sparse, [{ key: 'p50', name: 'P50', color: '#000' }])
    )

    const y = (result.current[0] as { y: number[] }).y
    expect(y).toEqual([30, 0, 0])
    expect(y.every(v => Number.isFinite(v))).toBe(true)
  })
})

const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
function viewMetricAdapter(search = '') {
  const QueryWrapper = makeQueryWrapper(createTestQueryClient())
  const NuqsWrapper = withNuqsTestingAdapter({
    searchParams: search,
    hasMemory: true,
    onUrlUpdate,
    resetUrlUpdateQueueOnMount: false,
  })
  return {
    wrapper: ({ children }: { children: React.ReactNode }) => (
      <QueryWrapper>
        <NuqsWrapper>{children}</NuqsWrapper>
      </QueryWrapper>
    ),
  }
}

describe('useViewMetricUrlSync', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
  })

  afterEach(() => {
    window.history.pushState({}, '', '/')
  })

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
        return HttpResponse.json([
          {
            id: 'view-123',
            start_time: '2026-01-01T00:00:00Z',
            end_time: '2026-01-02T00:00:00Z',
            filters_json: JSON.stringify([{ column: 'status', value: '404', mode: 'include' }]),
          },
        ])
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
    const { result } = setupQueryMonitorUrl('')
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
    const { result } = setupQueryMonitorUrl('?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
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
    const { result } = setupQueryMonitorUrl('?kind=nope&view=weird&slow=-5&db=Oracle&nogroup=0')
    expect(result.current).toMatchObject({
      kindFilter: 'all',
      viewMode: 'all',
      slowThresholdMs: DEFAULT_SLOW,
      dbFilter: 'all',
      groupCrons: true,
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
    const { result, onUrlUpdate } = setupQueryMonitorUrl('?q=x&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    act(() => result.current.setSearch(''))
    act(() => result.current.setKindFilter('all'))
    act(() => result.current.setViewMode('all'))
    act(() => result.current.setSlowThresholdMs(DEFAULT_SLOW))
    act(() => result.current.setDbFilter('all'))
    act(() => result.current.setGroupCrons(true))
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
