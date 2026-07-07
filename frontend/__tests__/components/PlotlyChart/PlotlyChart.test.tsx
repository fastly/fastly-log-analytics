/**
 * R-3a C-4. PlotlyChart is the thin React shell around react-plotly.js's
 * factory build. The async ``next/dynamic`` import for the actual Plot
 * component is mocked out so we can drive it synchronously and assert
 * the props PlotlyChart passes in (layout merge, defaults, callbacks).
 * We also verify the IntersectionObserver visibility gate and the
 * always-rendered ChartA11yTable companion.
 *
 * @vitest-environment jsdom
 */
import { act, render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

import { PlotlyChart } from '@/components/PlotlyChart/PlotlyChart'

// Capture every Plot render so each test can pull its most recent props.
const plotCalls: any[] = []

// ``next/dynamic`` normally returns a lazy wrapper around the loader.
// For tests, return a stub that records its props and renders a marker.
vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: (_loader: any, _opts: any) => {
    const Stub = (props: any) => {
      plotCalls.push(props)
      return <div data-testid="plot-stub" />
    }
    return Stub
  },
}))

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'light' }),
}))

// IntersectionObserver: capture the callback so tests can fire it.
let lastIOCallback: ((entries: any[]) => void) | null = null
class MockIntersectionObserver {
  constructor(cb: (entries: any[]) => void) {
    lastIOCallback = cb
  }
  observe() {}
  disconnect() {}
  unobserve() {}
  takeRecords() { return [] }
  root = null
  rootMargin = ''
  thresholds = []
}

class MockResizeObserver {
  observe() {}
  disconnect() {}
  unobserve() {}
}

beforeEach(() => {
  plotCalls.length = 0
  lastIOCallback = null
  vi.stubGlobal('IntersectionObserver', MockIntersectionObserver as any)
  vi.stubGlobal('ResizeObserver', MockResizeObserver as any)
})

describe('PlotlyChart', () => {
  it('withholds the Plot subtree until IntersectionObserver fires isIntersecting', async () => {
    render(<PlotlyChart data={[]} a11yTitle="x" />)
    expect(screen.queryByTestId('plot-stub')).toBeNull()
    // Simulate the observer callback flipping the gate.
    expect(lastIOCallback).not.toBeNull()
    await act(async () => {
      lastIOCallback!([{ isIntersecting: true }])
    })
    expect(screen.getByTestId('plot-stub')).toBeInTheDocument()
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
    const props = plotCalls.at(-1)
    expect(props.layout.legend.orientation).toBe('v')
    expect(props.layout.legend.x).toBe(0.5)
    expect(props.layout.xaxis.type).toBe('date')
    expect(props.layout.xaxis.range).toEqual(['a', 'b'])
    expect(props.layout.yaxis.title).toBe('count')
  })

  it('always renders the ChartA11yTable companion alongside the chart', () => {
    const data = [{ x: ['a', 'b'], y: [1, 2], name: 'series' }]
    render(<PlotlyChart data={data} a11yTitle="My A11y Caption" />)
    // The table renders even before the chart visibility gate flips.
    const table = screen.getByRole('table')
    expect(table).toBeInTheDocument()
    const caption = document.querySelector('caption')
    expect(caption?.textContent).toBe('My A11y Caption')
  })

  it('forwards onRelayout via the graphDiv listener path when initialized', async () => {
    const onRelayout = vi.fn()
    render(<PlotlyChart data={[]} onRelayout={onRelayout} a11yTitle="x" />)
    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    const props = plotCalls.at(-1)
    // Simulate Plotly invoking onInitialized with a fake graphDiv that
    // records ``on(eventName, handler)`` so we can fire it manually.
    let handler: ((e: any) => void) | undefined
    const fakeGraphDiv = {
      on: (name: string, h: (e: any) => void) => {
        if (name === 'plotly_relayout') handler = h
      },
    }
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
    const props = plotCalls.at(-1)
    expect(props.config.responsive).toBe(true)
    expect(props.config.displayModeBar).toBe(false)
    expect(props.onUpdate).toBe(onUpdate)
  })
})
