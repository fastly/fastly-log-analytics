/**
 * Consolidated tests for PlotlyChart, ChartA11yTable, and tracesToTable.
 *
 * @vitest-environment jsdom
 */
import { act, render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

import { PlotlyChart, ChartA11yTable, tracesToTable } from '@/components/PlotlyChart'
import type { PlotlyConfig, PlotlyGraphDiv, PlotlyLayout, PlotlyRelayoutEvent } from '@/types/api'
import { stubResizeObserver } from '../../helpers/page-smoke'

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
  plotCalls.length = 0
  lastIOCallback = null
  lastIOOptions = null
  vi.stubGlobal('IntersectionObserver', MockIntersectionObserver)
  stubResizeObserver()
})

describe('PlotlyChart', () => {
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
