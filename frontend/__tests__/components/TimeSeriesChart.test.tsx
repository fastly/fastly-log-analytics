/**
 * Consolidated tests for TimeSeriesChart, ScorerSeriesCard, PlotlyChart, ChartA11yTable, and tracesToTable.
 *
 * @vitest-environment jsdom
 */
import { act, render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { ComponentProps } from 'react'
import React from 'react'

import { TimeSeriesChart } from '@/components/AnalyticsCard'
import { PlotlyChart, ChartA11yTable, tracesToTable } from '@/components/PlotlyChart'
import type { PlotlyConfig, PlotlyGraphDiv, PlotlyLayout, PlotlyRelayoutEvent, PlotlyTrace } from '@/types/api'
import { stubResizeObserver } from '../helpers/page-smoke'

// Loosely-typed view of Plotly objects for assertions.
type Loose = { [k: string]: any } // eslint-disable-line @typescript-eslint/no-explicit-any
type Captured = { data: PlotlyTrace[]; layout?: PlotlyLayout }
let captured: Captured | null = null
let timeseries: unknown = undefined
let useRealPlotly = false

type PlotStubProps = {
  layout?: PlotlyLayout
  config?: PlotlyConfig
  onUpdate?: unknown
  onInitialized: (figure: unknown, graphDiv: PlotlyGraphDiv) => void
}
const plotCalls: PlotStubProps[] = []

vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: () => {
    const Stub = (props: PlotStubProps) => {
      plotCalls.push(props)
      return <div data-testid="plot-stub" />
    }
    return Stub
  },
}))

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'light' }),
}))

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

vi.mock('@/components/PlotlyChart', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/PlotlyChart')>()
  return {
    ...actual,
    PlotlyChart: (props: PlotlyChartProps) =>
      useRealPlotly ? <actual.PlotlyChart {...props} /> : renderMockPlotly(props),
  }
})

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

let lastIOCallback: ((entries: Array<{ isIntersecting: boolean }>) => void) | null = null
let lastIOOptions: IntersectionObserverInit | null = null
class MockIntersectionObserver {
  constructor(cb: (entries: Array<{ isIntersecting: boolean }>) => void, options?: IntersectionObserverInit) {
    lastIOCallback = cb
    lastIOOptions = options ?? null
  }
  observe() {}
  disconnect() {}
  unobserve() {}
  takeRecords() { return [] }
  root = null
  rootMargin = ''
  thresholds = []
}

beforeEach(() => {
  useRealPlotly = false
  plotCalls.length = 0
  lastIOCallback = null
  lastIOOptions = null
  vi.stubGlobal('IntersectionObserver', MockIntersectionObserver)
  stubResizeObserver()
})

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

describe('PlotlyChart', () => {
  beforeEach(() => {
    useRealPlotly = true
  })

  it('withholds the Plot subtree until IntersectionObserver fires isIntersecting', async () => {
    render(<PlotlyChart data={[]} a11yTitle="x" />)
    expect(screen.queryByTestId('plot-stub')).toBeNull()
    expect(lastIOCallback).not.toBeNull()
    await act(async () => {
      lastIOCallback!([{ isIntersecting: true }])
    })
    expect(screen.getByTestId('plot-stub')).toBeInTheDocument()
  })

  it('renders the Plot subtree when IntersectionObserver is unavailable', () => {
    vi.stubGlobal('IntersectionObserver', undefined)
    render(<PlotlyChart data={[]} a11yTitle="x" />)
    expect(screen.getByTestId('plot-stub')).toBeInTheDocument()
    expect(lastIOCallback).toBeNull()
  })

  it('uses the nearest scroll container as the observer root', () => {
    const { container } = render(
      <div style={{ overflowY: 'auto' }}>
        <PlotlyChart data={[]} a11yTitle="x" />
      </div>,
    )
    expect(lastIOOptions?.root).toBe(container.firstElementChild)
  })

  it('lets caller layout overrides win for legend and xaxis fields', async () => {
    render(
      <PlotlyChart
        data={[{ x: [1], y: [1] }]}
        layout={{
          legend: { orientation: 'v', x: 0.5 },
          xaxis: { type: 'date', range: ['a', 'b'] },
          yaxis: { title: 'count' },
        }}
        a11yTitle="x"
      />,
    )
    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    const props = plotCalls.at(-1)!
    expect(props.layout?.legend?.orientation).toBe('v')
    expect(props.layout?.legend?.x).toBe(0.5)
    expect(props.layout?.xaxis?.type).toBe('date')
    expect(props.layout?.xaxis?.range).toEqual(['a', 'b'])
    expect(props.layout?.yaxis?.title).toBe('count')
  })

  it('always renders the ChartA11yTable companion alongside the chart', () => {
    const data = [{ x: ['a', 'b'], y: [1, 2], name: 'series' }]
    render(<PlotlyChart data={data} a11yTitle="My A11y Caption" />)
    const table = screen.getByRole('table')
    expect(table).toBeInTheDocument()
    const caption = document.querySelector('caption')
    expect(caption?.textContent).toBe('My A11y Caption')
  })

  it('forwards onRelayout via the graphDiv listener path when initialized', async () => {
    const onRelayout = vi.fn()
    render(<PlotlyChart data={[]} onRelayout={onRelayout} a11yTitle="x" />)
    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    const props = plotCalls.at(-1)!
    let handler: ((e: PlotlyRelayoutEvent) => void) | undefined
    const fakeGraphDiv: PlotlyGraphDiv = Object.assign(document.createElement('div'), {
      on: (name: string, h: (e: PlotlyRelayoutEvent) => void) => {
        if (name === 'plotly_relayout') handler = h
      },
    })
    await act(async () => {
      props.onInitialized({}, fakeGraphDiv)
    })
    expect(handler).toBeDefined()
    handler!({ 'xaxis.range[0]': 1 })
    expect(onRelayout).toHaveBeenCalledWith({ 'xaxis.range[0]': 1 })
  })

  it('applies default config and forwards onUpdate prop straight through', async () => {
    const onUpdate = vi.fn()
    render(<PlotlyChart data={[]} onUpdate={onUpdate} a11yTitle="x" />)
    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    const props = plotCalls.at(-1)!
    expect(props.config?.responsive).toBe(true)
    expect(props.config?.displayModeBar).toBe(false)
    expect(props.onUpdate).toBe(onUpdate)
  })
})

describe('ChartA11yTable', () => {
  it('renders an sr-only table from a tracesToTable-shaped input with scope="col" headers', () => {
    const traces = [
      { x: ['a', 'b'], y: [1, 2], name: 'Series 1' },
      { x: ['a', 'b'], y: [3, 4], name: 'Series 2' },
    ]
    const shape = tracesToTable(traces, 'Requests over time')
    const { container } = render(<ChartA11yTable shape={shape} />)
    const table = container.querySelector('table')
    expect(table).not.toBeNull()
    expect(table?.className).toContain('sr-only')
    expect(screen.getByRole('columnheader', { name: 'x' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Series 1' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Series 2' })).toBeInTheDocument()
    expect(screen.getAllByRole('cell').length).toBeGreaterThan(0)
    expect(document.querySelector('caption')?.textContent).toBe('Requests over time')
    const ths = container.querySelectorAll('thead th')
    ths.forEach((th) => expect(th.getAttribute('scope')).toBe('col'))
  })

  it('emits a no-data fallback (not a table) for an empty shape or heatmap', () => {
    const shape = tracesToTable([], 'Empty chart')
    const { container } = render(<ChartA11yTable shape={shape} />)
    expect(container.querySelector('table')).toBeNull()
    expect(container.textContent).toContain('Empty chart')
    expect(container.textContent).toContain('no readable data')

    const heatmap = [
      {
        type: 'heatmap',
        x: ['t1', 't2'],
        y: ['asnA', 'asnB'],
        z: [[1, 2], [5, 6]],
      },
    ]
    const hmShape = tracesToTable(heatmap, 'ASN heatmap')
    expect(hmShape.empty).toBe(true)
    expect(hmShape.rows).toHaveLength(0)
  })

  it('caps very large flat tables and announces the truncation', () => {
    const x = Array.from({ length: 1200 }, (_, i) => i)
    const y = x.map((n) => n * 2)
    const shape = tracesToTable([{ x, y, name: 'S' }], 'Big chart')
    expect(shape.rows).toHaveLength(500)
    expect(shape.truncatedFrom).toBe(1200)
    render(<ChartA11yTable shape={shape} />)
    expect(document.querySelector('caption')?.textContent).toContain(
      'showing first 500 of 1200 rows',
    )
  })
})

describe('tracesToTable', () => {
  it('returns an empty shape when data is not an array or is empty', () => {
    expect(tracesToTable(null)).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable({})).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable(undefined)).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable([])).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable([], 'My chart').title).toBe('My chart')
    expect(tracesToTable([]).title).toBe('Chart data')
  })

  it('converts single and multi-trace line/bar traces, filling missing x values and nulls with empty string', () => {
    const single = tracesToTable(
      [{ type: 'scatter', mode: 'lines', name: 'Requests', x: ['t1', 't2'], y: [10, null] }],
      'Time series',
    )
    expect(single.empty).toBe(false)
    expect(single.title).toBe('Time series')
    expect(single.columns).toEqual(['x', 'Requests'])
    expect(single.rows).toEqual([
      ['t1', 10],
      ['t2', ''],
    ])

    const multi = tracesToTable([
      { type: 'scatter', name: 'A', x: ['t1', 't2'], y: [1, 2] },
      { type: 'scatter', name: 'B', x: ['t2', 't3'], y: [5, 6] },
    ])
    expect(multi.columns).toEqual(['x', 'A', 'B'])
    expect(multi.rows).toEqual([
      ['t1', 1, ''],
      ['t2', 2, 5],
      ['t3', '', 6],
    ])
  })

  it('handles pie traces and unnamed traces', () => {
    const pie = tracesToTable([
      { type: 'pie', labels: ['US', 'CN'], values: [100, 50], name: 'Country share' },
    ])
    expect(pie.title).toBe('Country share')
    expect(pie.columns).toEqual(['Label', 'Value'])
    expect(pie.rows).toEqual([
      ['US', 100],
      ['CN', 50],
    ])

    const unnamed = tracesToTable([{ type: 'bar', x: ['a'], y: [1] }])
    expect(unnamed.columns).toEqual(['x', 'Series 1'])
  })
})
