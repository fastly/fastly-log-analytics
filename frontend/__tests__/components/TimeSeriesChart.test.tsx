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
import { render } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'

const plotlyMock = vi.fn((_props: any) => <div data-testid="plotly-chart" />)
vi.mock('@/components/PlotlyChart/PlotlyChart', () => ({
  PlotlyChart: (props: any) => plotlyMock(props),
}))

vi.mock('@/lib/chart-helpers', () => ({
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
    expect(layout.hovermode).toBe('closest')
    expect(layout.xaxis).toEqual({
      type: 'date',
      range: ['2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z'],
    })
    expect(layout.yaxis).toEqual({ title: 'count' })
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
    expect(secondLayout.xaxis.range[1]).toBe('2026-01-01T02:00:00Z')
  })
})
