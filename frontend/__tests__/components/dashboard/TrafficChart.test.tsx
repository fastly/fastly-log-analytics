/**
 * TrafficChart is a presentational wrapper around TimeSeriesChart with
 * three interactive surfaces: a metric ButtonGroup, a Latency dropdown
 * (when one of p50/p95/p99 is the active metric), and a per-category
 * legend that toggles trace visibility via `toggleCategory`. The trend
 * row at the bottom invokes `setTrend`.
 *
 * Tests pin: (1) the loading skeleton renders when aggregates are
 * undefined, (2) the populated state mounts TimeSeriesChart with the
 * provided traffic data, (3) clicking a metric button calls
 * `setMetric` with the right id, and (4) clicking a category legend
 * button calls `toggleCategory` with the trace name.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

vi.mock('@/components/charts/TimeSeriesChart', () => ({
  TimeSeriesChart: ({ data }: { data: any[] }) => (
    <div data-testid="timeseries-chart">
      <span data-testid="trace-count">{data?.length ?? 0}</span>
    </div>
  ),
}))

// PlotlyChart is only transitively imported via TimeSeriesChart, but
// other parents in the same test process may pick up the un-mocked
// module — stub it to be safe.
vi.mock('@/components/PlotlyChart/PlotlyChart', () => ({
  PlotlyChart: () => <div data-testid="plotly-chart" />,
}))

// useActiveLogFields chains useBootstrap + useLogFieldsCatalog (both react-query),
// which would need a QueryClientProvider. Mock it instead — TrafficChart's
// contract here is "show the neutral 'No logs found for this period.' empty copy
// when the metric's backing field IS active, and the 'Requires Group …' hint when
// it is NOT". Default to no active fields so the not-enabled-hint test exercises
// the hint path; tests that want the neutral path add the field to `activeFields`.
const { activeFields } = vi.hoisted(() => ({ activeFields: new Set<string>() }))
vi.mock('@/hooks/useActiveLogFields', () => ({
  useActiveLogFields: () => ({
    ready: true,
    isFieldActive: (id: string) => activeFields.has(id),
    isGroupActive: () => false,
  }),
}))

import { TrafficChart } from '@/app/dashboard/_sections/TrafficChart'
import type { ReportConfiguration } from '@/hooks/useReportConfig'

const catalog = {
  fields: [
    { id: 'requests', label: 'Requests', group: 'METRICS' },
    { id: 'hit_rate', label: 'Cache Hit Rate', group: 'METRICS' },
    { id: '5xx', label: '5xx Errors', group: 'METRICS' },
    { id: '4xx', label: '4xx Errors', group: 'METRICS' },
    { id: 'p50_latency', label: 'p50 Latency', group: 'METRICS' },
    { id: 'p95_latency', label: 'p95 Latency', group: 'METRICS' },
    { id: 'p99_latency', label: 'p99 Latency', group: 'METRICS' },
  ],
}

const config = {
  effectiveInterval: '1 hour',
  validIntervals: new Set(['1 hour']),
  validTrends: new Set(['off', 'auto', '1m', '5m', '1h', '1d']),
} as unknown as ReportConfiguration

function renderChart(overrides: Partial<React.ComponentProps<typeof TrafficChart>> = {}) {
  const defaults: React.ComponentProps<typeof TrafficChart> = {
    catalog,
    metric: 'requests',
    setMetric: vi.fn(),
    trend: 'off',
    setTrend: vi.fn(),
    config,
    intervalButtons: <div data-testid="interval-buttons" />,
    trafficData: [],
    chartLayout: {},
    hiddenCategories: new Set<string>(),
    toggleCategory: vi.fn(),
    isReady: true,
    isLoadingAggs: false,
    isFetchingAggs: false,
    transformPending: false,
    aggregates: { time_series: [] },
    onChartRelayout: vi.fn(),
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    timezone: 'UTC',
  }
  return render(<TrafficChart {...defaults} {...overrides} />)
}

describe('TrafficChart', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    activeFields.clear()
  })

  it('renders the loading skeleton when aggregates are undefined', () => {
    renderChart({ isReady: false, aggregates: undefined })
    expect(screen.getByText('Initializing...')).toBeInTheDocument()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('renders the empty state when trafficData is empty but aggregates are ready', () => {
    // metric 'requests' has no field-gated branch → the neutral "no logs"
    // copy shows regardless of active-field state.
    renderChart({ aggregates: { time_series: [] }, trafficData: [] })
    expect(screen.getByText('No data available')).toBeInTheDocument()
    expect(screen.getByText('No logs found for this period.')).toBeInTheDocument()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('shows the "Requires Infrastructure (Group C)" hint when ttfb is NOT active', () => {
    // metric 'ttfb_client' depends on the `ttfb` field; with it inactive an
    // empty result means the group is not enabled, not just "no data".
    renderChart({ metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] })
    expect(screen.getByText('No data available')).toBeInTheDocument()
    expect(
      screen.getByText('Requires Infrastructure (Group C) fields to be enabled in Fastly logging.'),
    ).toBeInTheDocument()
  })

  it('shows the neutral "no logs" copy for ttfb when the ttfb field IS active', () => {
    activeFields.add('ttfb')
    renderChart({ metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] })
    expect(screen.getByText('No logs found for this period.')).toBeInTheDocument()
    expect(screen.queryByText(/Requires Infrastructure/)).toBeNull()
  })

  // Pins the fix for the "No data available" flash: when the bundle has
  // arrived (aggregates non-empty) but the worker round-trip hasn't yet
  // produced traces (trafficData still []), the skeleton must hold instead
  // of falling through to the empty-state branch.
  it('keeps the skeleton up while the worker transform is pending', () => {
    renderChart({
      aggregates: { time_series: [{ time: '2026-01-01T00:00:00Z', value: 1 }] },
      trafficData: [],
      transformPending: true,
    })
    expect(screen.getByText('Crunching logs...')).toBeInTheDocument()
    expect(screen.queryByText('No data available')).toBeNull()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('mounts TimeSeriesChart with one trace per row of trafficData', () => {
    const trafficData = [
      { type: 'scatter', name: 'requests', x: [], y: [], marker: { color: '#000' } },
      { type: 'scatter', name: 'cached', x: [], y: [], marker: { color: '#111' } },
      { type: 'scatter', name: 'origin', x: [], y: [], marker: { color: '#222' } },
    ]
    renderChart({ trafficData })
    expect(screen.getByTestId('timeseries-chart')).toBeInTheDocument()
    expect(screen.getByTestId('trace-count').textContent).toBe('3')
  })

  it('forwards metric-button clicks to setMetric', () => {
    const setMetric = vi.fn()
    renderChart({ setMetric })
    // shortLabels["5xx"] = "5xx" — render text is the button label.
    const fiveXX = screen.getByRole('button', { name: /^5xx$/i })
    fireEvent.click(fiveXX)
    expect(setMetric).toHaveBeenCalledWith('5xx')
  })

  it('forwards category-legend clicks to toggleCategory when bar traces are present', () => {
    const toggleCategory = vi.fn()
    const trafficData = [
      { type: 'bar', name: 'edge', marker: { color: '#aaa' } },
      { type: 'bar', name: 'origin', marker: { color: '#bbb' } },
    ]
    renderChart({ trafficData, toggleCategory })
    // The category legend renders a button per bar trace.
    fireEvent.click(screen.getByRole('button', { name: /edge/i }))
    expect(toggleCategory).toHaveBeenCalledWith('edge')
  })

  it('forwards trend-row clicks to setTrend', () => {
    const setTrend = vi.fn()
    renderChart({ setTrend })
    // The "1m" trend pill is the lowercase label in TRENDS.
    const oneMin = screen.getAllByRole('button', { name: /^1m$/ })[0]
    fireEvent.click(oneMin)
    expect(setTrend).toHaveBeenCalledWith('1m')
  })
})
