/**
 * @vitest-environment jsdom
 *
 * Regression coverage for the Origin Latency chart silently-blank bug
 * documented in TESTING_PLAN.md's Sidetracks section.
 *
 * Plotly's schema validator silently rejects an entire trace if it
 * sees a property explicitly set to `undefined` (e.g.
 * `stackgroup: undefined`, `marker: undefined`). No console error
 * fires — the chart just renders empty. The fix builds the trace
 * object incrementally; these tests pin that property-presence
 * invariant so a refactor can't re-introduce the bug.
 *
 * The previous regression coverage was a Playwright E2E that doesn't
 * run in CI.
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { useTimeseriesToTraces } from '@/hooks/useTimeseriesToTraces'

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
    // The presence check is the contract — Plotly rejects the trace if
    // `stackgroup` is the literal `undefined`, but happily accepts a
    // trace where the property is absent.
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
    // The most-load-bearing invariant. Pinned because losing it
    // brings back the exact bug — Plotly's validator iterates
    // `Object.keys(trace)` and rejects any whose value is undefined.
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
    // The TimeseriesDataPoint type requires `time`, but the hook also
    // accepts `ts` at runtime. We cast as `any` because we're
    // intentionally testing the looser runtime contract.
    const dataWithTs = [
      { ts: '2026-05-18T20:00:00Z', p50: 30 },
      { ts: '2026-05-18T20:01:00Z', p50: 28 },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ] as any
    const { result } = renderHook(() =>
      useTimeseriesToTraces(dataWithTs, [{ key: 'p50', name: 'P50', color: '#000' }])
    )
    expect(result.current[0]).toBeDefined()
    expect((result.current[0] as { y: number[] }).y).toEqual([30, 28])
  })

  it('coerces missing/null y values to 0 (no NaN in Plotly y array)', () => {
    // Plotly silently drops a series with any NaN value — we want a
    // deterministic 0 for "no data this bucket" instead.
    const sparse = [
      { time: '2026-05-18T20:00:00Z', p50: 30 },
      { time: '2026-05-18T20:01:00Z' /* p50 missing */ },
      { time: '2026-05-18T20:02:00Z', p50: null },
    ]
    const { result } = renderHook(() =>
      useTimeseriesToTraces(sparse as never, [{ key: 'p50', name: 'P50', color: '#000' }])
    )

    const y = (result.current[0] as { y: number[] }).y
    expect(y).toEqual([30, 0, 0])
    expect(y.every(v => Number.isFinite(v))).toBe(true)
  })
})
