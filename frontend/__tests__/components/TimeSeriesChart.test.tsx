/**
 * C-4 (testing_suite_audit_2026-06-14.md). `TimeSeriesChart` is a thin
 * wrapper that merges its caller's layout into the shared
 * `TIME_HOVER_LAYOUT` + a time-x-axis derived from start/end/timezone,
 * then hands everything to `PlotlyChart`. Pin the merge contract so
 * a future "what's in the merged layout?" question is answered by the
 * test, not a paste from the source.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { ComponentProps } from 'react'
import React from 'react'

import { TimeSeriesChart } from '@/components/AnalyticsCard'
import type { PlotlyChart } from '@/components/PlotlyChart'
import type { PlotlyTrace, PlotlyLayout } from '@/types/api'

// Loosely-typed view of Plotly objects for assertions.
type Loose = { [k: string]: any } // eslint-disable-line @typescript-eslint/no-explicit-any
type Captured = { data: PlotlyTrace[]; layout?: PlotlyLayout }
let captured: Captured | null = null
let timeseries: unknown = undefined

type PlotlyChartProps = ComponentProps<typeof PlotlyChart>
const plotlyMock = vi.fn<(props: PlotlyChartProps) => void>()

function renderMockPlotly(props: PlotlyChartProps) {
  plotlyMock(props)
  captured = { data: props.data, layout: props.layout }
  return (
    <div
      data-testid="plotly"
      data-title={(props.layout as Loose | undefined)?.title ?? ''}
      data-layout={JSON.stringify(props.layout ?? {})}
      data-traces={JSON.stringify(props.data ?? [])}
    />
  )
}

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: PlotlyChartProps) => renderMockPlotly(props),
}))

vi.mock('@/components/SessionScoring/help-content', async (orig) => {
  const actual = await orig<typeof import('@/components/SessionScoring/help-content')>()
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

import { ScorerLatencyChart, ScorerErrorsChart } from '@/components/SessionScoring/ScorerSeriesCard'

vi.mock('@/lib/chart-helpers', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/chart-helpers')>()),
  TIME_HOVER_LAYOUT: { hovermode: 'x unified' },
  makeTimeXAxis: (start: string | null | undefined, end: string | null | undefined) => ({
    type: 'date',
    range: [start, end],
  }),
}))

describe('TimeSeriesChart', () => {
  beforeEach(() => {
    plotlyMock.mockClear()
  })

  it('passes through the data array verbatim', () => {
    const traces = [{ x: [1, 2, 3], y: [4, 5, 6] }]
    render(
      <TimeSeriesChart
        data={traces}
        startTime="2026-01-01T00:00:00Z"
        endTime="2026-01-01T01:00:00Z"
        timezone="UTC"
      />,
    )
    expect(plotlyMock).toHaveBeenCalled()
    expect(plotlyMock.mock.calls[0]![0].data).toBe(traces)
  })

  it('merges TIME_HOVER_LAYOUT + makeTimeXAxis output + caller layout (caller wins on collisions)', () => {
    render(
      <TimeSeriesChart
        data={[]}
        startTime="2026-01-01T00:00:00Z"
        endTime="2026-01-01T01:00:00Z"
        timezone="UTC"
        layout={{ hovermode: 'closest', yaxis: { title: 'count' } }}
      />,
    )
    const layout = plotlyMock.mock.calls[0]![0].layout
    // Caller's hovermode overrides TIME_HOVER_LAYOUT's
    expect(layout?.hovermode).toBe('closest')
    expect(layout?.xaxis).toEqual({
      type: 'date',
      range: ['2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z'],
    })
    expect(layout?.yaxis).toEqual({ title: 'count' })
  })

  it('forwards className, height, and onRelayout to PlotlyChart', () => {
    const onRelayout = vi.fn()
    render(
      <TimeSeriesChart
        data={[]}
        timezone="UTC"
        className="border"
        height={420}
        onRelayout={onRelayout}
      />,
    )
    const props = plotlyMock.mock.calls[0]![0]
    expect(props.className).toBe('border')
    expect(props.height).toBe(420)
    expect(props.onRelayout).toBe(onRelayout)
  })

  it('rebuilds the merged layout when the time range or timezone changes', () => {
    const { rerender } = render(
      <TimeSeriesChart
        data={[]}
        startTime="2026-01-01T00:00:00Z"
        endTime="2026-01-01T01:00:00Z"
        timezone="UTC"
      />,
    )
    const firstLayout = plotlyMock.mock.calls[0]![0].layout

    rerender(
      <TimeSeriesChart
        data={[]}
        startTime="2026-01-01T00:00:00Z"
        endTime="2026-01-01T02:00:00Z"
        timezone="UTC"
      />,
    )
    const secondLayout = plotlyMock.mock.calls.at(-1)![0].layout
    expect(secondLayout).not.toBe(firstLayout)
    expect(secondLayout?.xaxis?.range?.[1]).toBe('2026-01-01T02:00:00Z')
  })
})

const T0 = '2026-10-01T00:00:00+00:00'
const T2 = '2026-10-01T02:00:00+00:00'

function rows() {
  // Hour 01:00 is missing: dense bars must zero-fill it, lines must not.
  return [
    { hour: T0, total_count: 100, fail_open_count: 5, rtt_p50_us: 1000, rtt_p95_us: 2000, rtt_p99_us: 3000, exec_p50_us: 100, exec_p95_us: 200 },
    { hour: T2, total_count: 0, fail_open_count: 0, rtt_p50_us: null, rtt_p95_us: 4000, rtt_p99_us: null, exec_p50_us: null, exec_p95_us: null },
  ]
}

describe('ScorerErrorsChart', () => {
  beforeEach(() => {
    captured = null
    timeseries = { has_latency: true, granularity: 'hour', rows: rows() }
  })

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
  beforeEach(() => {
    captured = null
    timeseries = { has_latency: true, granularity: 'hour', rows: rows() }
  })

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
