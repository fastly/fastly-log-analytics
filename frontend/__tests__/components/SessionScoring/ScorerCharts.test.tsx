/**
 * ScorerLatencyChart / ScorerErrorsChart trace + layout contract. These pin
 * the exact Plotly traces both charts emit so the shared trace/layout
 * builders (lib/chart-builders) can replace the hand-built copies without a
 * visible change: dense zero-filled bars, sparse gap-connected lines, the
 * right-hand secondary axis, and the toggle that adds/removes it.
 */
import * as React from 'react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'

import type { PlotlyTrace, PlotlyLayout } from '@/types/plotly'

// Loosely-typed view of Plotly objects for assertions.
type Loose = { [k: string]: any } // eslint-disable-line @typescript-eslint/no-explicit-any
type Captured = { data: PlotlyTrace[]; layout?: PlotlyLayout }
let captured: Captured | null = null
let timeseries: unknown = undefined

vi.mock('@/components/PlotlyChart', async () =>
  (await import('../../helpers/page-smoke')).plotlyChartModuleMock((props) => {
    captured = { data: props.data, layout: props.layout }
  }),
)

vi.mock('@/components/SessionScoring/useScorerTimeseries', async (orig) => {
  const actual = await orig<typeof import('@/components/SessionScoring/useScorerTimeseries')>()
  return {
    ...actual,
    useScorerTimeseries: () => ({
      data: timeseries,
      isLoading: false,
      isFetching: false,
      isError: false,
      error: null,
    }),
  }
})

import { ScorerLatencyChart } from '@/components/SessionScoring/ScorerLatencyChart'
import { ScorerErrorsChart } from '@/components/SessionScoring/ScorerErrorsChart'

const T0 = '2026-10-01T00:00:00+00:00'
const T2 = '2026-10-01T02:00:00+00:00'

function rows() {
  // Hour 01:00 is missing: dense bars must zero-fill it, lines must not.
  return [
    { hour: T0, total_count: 100, fail_open_count: 5, rtt_p50_us: 1000, rtt_p95_us: 2000, rtt_p99_us: 3000, exec_p50_us: 100, exec_p95_us: 200 },
    { hour: T2, total_count: 0, fail_open_count: 0, rtt_p50_us: null, rtt_p95_us: 4000, rtt_p99_us: null, exec_p50_us: null, exec_p95_us: null },
  ]
}

beforeEach(() => {
  captured = null
  timeseries = { has_latency: true, granularity: 'hour', rows: rows() }
})

describe('ScorerErrorsChart', () => {
  it('emits dense fail-open bars plus a sparse error-rate line on y2', () => {
    render(<ScorerErrorsChart serviceId="s" sinceHours={24} />)
    const [bars, rate] = captured!.data as unknown as Record<string, unknown>[]
    expect(bars).toMatchObject({ type: 'bar', name: 'Fail-opens', marker: { color: 'rgba(225,29,72,0.45)' } })
    expect(bars.x).toEqual(['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z', '2026-10-01T02:00:00.000Z'])
    expect(bars.y).toEqual([5, 0, 0])
    expect(rate).toMatchObject({ type: 'scatter', mode: 'lines', name: 'Error rate (%)', yaxis: 'y2', line: { color: '#e11d48', width: 2 } })
    expect(rate.x).toEqual([T0, T2])
    expect(rate.y).toEqual([5, 0])
    const layout = captured!.layout as Loose
    expect(layout.showlegend).toBe(true)
    expect(layout.yaxis.title).toBe('Fail-opens')
    expect(layout.yaxis2).toMatchObject({ overlaying: 'y', side: 'right', showgrid: false, rangemode: 'tozero', ticksuffix: '%' })
    expect(layout.margin.r).toBe(50)
  })

  it('drops the rate line and secondary axis when the toggle is off', () => {
    render(<ScorerErrorsChart serviceId="s" sinceHours={24} />)
    fireEvent.click(screen.getByRole('switch'))
    expect(captured!.data).toHaveLength(1)
    const layout = captured!.layout as Loose
    expect(layout.yaxis2).toBeUndefined()
    expect(layout.showlegend).toBe(false)
    expect(layout.margin.r).toBe(20)
  })

  it('uses a per-minute grid and tick format for minute granularity', () => {
    timeseries = {
      has_latency: false,
      granularity: 'minute',
      rows: [
        { hour: '2026-10-01T00:00:00+00:00', total_count: 1, fail_open_count: 1 },
        { hour: '2026-10-01T00:02:00+00:00', total_count: 1, fail_open_count: 2 },
      ],
    }
    render(<ScorerErrorsChart serviceId="s" sinceHours={1} />)
    const bars = captured!.data[0] as unknown as Record<string, unknown>
    expect(bars.y).toEqual([1, 0, 2])
    expect((captured!.layout as Loose).xaxis.tickformat).toBe('%H:%M')
  })
})

describe('ScorerLatencyChart', () => {
  it('emits a dense volume bar on y2 plus five sparse latency lines in ms', () => {
    render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
    const traces = captured!.data as unknown as Loose[]
    expect(traces.map((t) => t.name)).toEqual([
      'Inspected requests', 'p50 rtt', 'p95 rtt', 'p99 rtt', 'p50 exec', 'p95 exec',
    ])
    expect(traces[0]).toMatchObject({ type: 'bar', yaxis: 'y2', showlegend: true, marker: { color: 'rgba(148, 163, 184, 0.12)' } })
    expect(traces[0].x).toEqual(['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z', '2026-10-01T02:00:00.000Z'])
    expect(traces[0].y).toEqual([100, 0, 0])
    expect(traces[1]).toMatchObject({ type: 'scatter', mode: 'lines', connectgaps: true, line: { color: '#60a5fa', width: 2 } })
    expect(traces[1].x).toEqual([T0, T2])
    expect(traces[1].y).toEqual([1, null])
    expect(traces[2].y).toEqual([2, 4])
    expect(traces[4].line).toMatchObject({ dash: 'dot', color: '#34d399' })
    const layout = captured!.layout as Loose
    expect(layout.yaxis.title).toBe('Round-trip / exec (ms)')
    expect(layout.yaxis2).toMatchObject({ overlaying: 'y', side: 'right', showgrid: false, rangemode: 'tozero' })
    expect(layout.yaxis2.ticksuffix).toBeUndefined()
    expect(layout.margin.r).toBe(50)
  })

  it('drops the volume bar and axis when the toggle is off', () => {
    render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
    fireEvent.click(screen.getByRole('switch'))
    expect((captured!.data as unknown as Loose[]).map((t) => t.name)).not.toContain('Inspected requests')
    expect((captured!.layout as Loose).yaxis2).toBeUndefined()
  })

  it('explains missing latency columns instead of drawing an empty axis', () => {
    timeseries = { has_latency: false, granularity: 'hour', rows: rows() }
    render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
    expect(captured).toBeNull()
    expect(screen.getByText(/Waiting for traffic logged with the scorer latency fields/)).toBeTruthy()
  })
})
