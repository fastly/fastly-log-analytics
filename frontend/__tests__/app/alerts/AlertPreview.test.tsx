/**
 * Unit tests for AlertPreview (app/alerts/_sections/AlertPreview.tsx).
 *
 * The component is small and presentational, but it owns three behaviours
 * worth pinning down:
 *  - empty-state vs loading vs chart branch selection (driven by
 *    previewData / isPreviewLoading);
 *  - lookback button toggle (which button is the "active" variant);
 *  - the trace composition handed to PlotlyChart — specifically the
 *    threshold-overlay branches, which only fire when threshold parses to a
 *    truthy number and previewData.type matches.
 *
 * We mock PlotlyChart so we can capture the exact `data` array passed in,
 * and mock the three external hooks (useLogFieldsCatalog, useTimeLayout,
 * useTimezoneStore) so the component renders without any network or store
 * dependencies.
 */
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

// Capture the props handed to PlotlyChart on every render so the trace
// composition can be asserted from outside.
const plotlyChartCalls: Array<{ data: any[]; layout: any }> = []

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: { data: any[]; layout: any }) => {
    plotlyChartCalls.push({ data: props.data, layout: props.layout })
    return <div data-testid="plotly-chart" />
  },
}))

vi.mock('@/hooks/useLogFieldsCatalog', () => ({
  useLogFieldsCatalog: vi.fn(() => ({
    data: {
      fields: [
        { id: 'requests', unit: '', precision: 0 },
        { id: 'origin_latency_ms', unit: 'ms', precision: 1 },
      ],
    },
  })),
}))

vi.mock('@/lib/chart-helpers', () => ({
  useTimeLayout: vi.fn(() => ({
    hovermode: 'x unified',
    xaxis: { type: 'date', nticks: 8 },
  })),
}))

vi.mock('@/stores/timezoneStore', () => ({
  useTimezoneStore: vi.fn(() => ({ timezone: 'UTC' })),
}))

import { AlertPreview } from '@/app/alerts/_sections/AlertPreview'

beforeEach(() => {
  plotlyChartCalls.length = 0
  vi.clearAllMocks()
})

const baseProps = {
  previewData: null,
  isPreviewLoading: false,
  lookbackHours: 6,
  setLookbackHours: vi.fn(),
  metric: 'requests',
  evalType: 'absolute',
  threshold: '',
}

describe('AlertPreview', () => {
  test('renders the empty placeholder when previewData has no points', () => {
    render(<AlertPreview {...baseProps} previewData={{ times: [], values: [] }} />)
    expect(screen.getByText('No data available for preview.')).toBeInTheDocument()
    // PlotlyChart should NOT have been mounted in the empty branch.
    expect(plotlyChartCalls).toHaveLength(0)
  })

  test('shows the loading spinner when isPreviewLoading is true', () => {
    const { container } = render(
      <AlertPreview {...baseProps} isPreviewLoading={true} previewData={null} />,
    )
    // Loader2 from lucide renders as an svg with the animate-spin class. The
    // wrapper div uses the absolute/inset-0 overlay markers — assert on the
    // spinning svg to keep the test robust against layout changes.
    const spinner = container.querySelector('svg.animate-spin')
    expect(spinner).not.toBeNull()
  })

  test('highlights the active lookback button (default variant -> bg-primary)', async () => {
    const setLookbackHours = vi.fn()
    render(
      <AlertPreview
        {...baseProps}
        lookbackHours={12}
        setLookbackHours={setLookbackHours}
        previewData={{ times: [], values: [] }}
      />,
    )

    const active = screen.getByRole('button', { name: '12h' })
    const inactive = screen.getByRole('button', { name: '1h' })

    // The component swaps variant="default" (-> bg-primary class) onto the
    // active button and variant="ghost" onto the rest. Assert on the marker
    // class so the test stays decoupled from the cva variant internals.
    expect(active.className).toContain('bg-primary')
    expect(inactive.className).not.toContain('bg-primary')

    // Clicking a different lookback delegates to the prop callback.
    await userEvent.click(inactive)
    expect(setLookbackHours).toHaveBeenCalledWith(1)
  })

  test('passes the current xy trace to PlotlyChart (timestamps converted to the selected tz)', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute',
    }
    render(<AlertPreview {...baseProps} previewData={previewData} threshold="" />)

    expect(plotlyChartCalls.length).toBeGreaterThan(0)
    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    // Current trace is always first; bar type for 'requests' metric.
    const current = lastCall.data[0]
    expect(current.name).toBe('Current')
    // The component runs times through formatDate(t, timezone, 'yyyy-MM-dd
    // HH:mm:ss') so the plotted points share the x-axis range's coordinate
    // space (also formatDate-derived). With the mocked 'UTC' tz the instants
    // are unchanged, just reformatted to the naive Plotly date form — a point
    // left in UTC '...Z' form would fall outside the local-time axis range.
    expect(current.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
    expect(current.y).toEqual(previewData.values)
    expect(current.type).toBe('bar')
  })

  test('omits the threshold overlay when threshold does not parse to a number', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute',
    }
    render(
      <AlertPreview
        {...baseProps}
        previewData={previewData}
        // Empty string -> parseFloat('') is NaN -> branch skipped.
        threshold=""
      />,
    )

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const thresholdTrace = lastCall.data.find((t) => t?.name === 'Threshold')
    expect(thresholdTrace).toBeUndefined()
  })

  test('emits a horizontal threshold trace when previewData.type=absolute and threshold parses', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute',
    }
    render(<AlertPreview {...baseProps} previewData={previewData} threshold="42" />)

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const thresholdTrace = lastCall.data.find((t) => t?.name === 'Threshold')
    expect(thresholdTrace).toBeDefined()
    // Horizontal line: two-point trace at the parsed threshold on both ends.
    // x endpoints are the tz-converted first/last timestamps (see the current
    // -trace test) so the line spans the same axis range as the data.
    expect(thresholdTrace.x).toEqual(['2026-06-15 00:00:00', '2026-06-15 01:00:00'])
    expect(thresholdTrace.y).toEqual([42, 42])
    expect(thresholdTrace.line?.dash).toBe('dash')
  })

  test('locks the x-axis (fixedrange) so the preview has no zoom/pan affordance', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [10, 20],
      type: 'absolute',
    }
    render(<AlertPreview {...baseProps} previewData={previewData} threshold="" />)

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    // y-axis is locked by PlotlyChart's own default; the preview pins the
    // x-axis here so there's no zoom/pan and no on-hover drag affordance.
    expect(lastCall.layout.xaxis.fixedrange).toBe(true)
  })

  test('emits both baseline and calculated-threshold traces for relative_increase evalType', () => {
    const previewData = {
      times: ['2026-06-15T00:00:00Z', '2026-06-15T01:00:00Z'],
      values: [110, 220],
      hist_values: [100, 200],
      type: 'relative',
    }
    render(
      <AlertPreview
        {...baseProps}
        previewData={previewData}
        evalType="relative_increase"
        threshold="10" // +10% -> [110, 220]
      />,
    )

    const lastCall = plotlyChartCalls[plotlyChartCalls.length - 1]
    const baseline = lastCall.data.find((t) => t?.name === 'Baseline')
    const threshold = lastCall.data.find((t) => t?.name === 'Threshold')

    expect(baseline).toBeDefined()
    expect(baseline.y).toEqual(previewData.hist_values)

    expect(threshold).toBeDefined()
    // 100 * 1.1, 200 * 1.1 — compare with tolerance to absorb FP noise.
    expect(threshold.y).toHaveLength(2)
    expect(threshold.y[0]).toBeCloseTo(110, 5)
    expect(threshold.y[1]).toBeCloseTo(220, 5)
  })
})
