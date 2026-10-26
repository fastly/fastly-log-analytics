/**
 * Consolidated tests for TimeSeriesChart, ScorerSeriesCard, PlotlyChart,
 * ChartA11yTable, tracesToTable, TrafficChart, CardGrid, FieldSearchDialog,
 * TopTenTable, and GeoMap.
 *
 * @vitest-environment jsdom
 */
import { act, render, screen, fireEvent, cleanup, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import type { ComponentProps } from 'react'
import React from 'react'

import type { PlotlyConfig, PlotlyGraphDiv, PlotlyLayout, PlotlyRelayoutEvent, PlotlyTrace } from '@/types/api'
import { stubResizeObserver, partial } from '../helpers/page-smoke'

type Loose = { [k: string]: any } // eslint-disable-line @typescript-eslint/no-explicit-any
if (typeof Element !== 'undefined') {
  Element.prototype.getAnimations ??= () => []
}
type Captured = { data: PlotlyTrace[]; layout?: PlotlyLayout }
let captured: Captured | null = null
let timeseries: unknown = undefined
let useRealPlotly = false
let mockTimeSeriesChart = false
let mockTopTenTable = false

type PlotStubProps = {
  layout?: PlotlyLayout
  config?: PlotlyConfig
  onUpdate?: unknown
  onInitialized: (figure: unknown, graphDiv: PlotlyGraphDiv) => void
  data?: Array<{ country?: string; name?: string }>
  onCountryClick?: (c: string) => void
}
const plotCalls: PlotStubProps[] = []

vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: () => {
    const Stub = (props: PlotStubProps) => {
      if ('onCountryClick' in props && typeof props.onCountryClick === 'function') {
        const onCountryClick = props.onCountryClick
        return (
          <div data-testid="choropleth-map">
            <span data-testid="marker-count">{props.data?.length ?? 0}</span>
            {(props.data ?? []).map((row) => (
              <button
                key={row.country ?? row.name}
                type="button"
                data-testid={`country-${row.country ?? row.name}`}
                onClick={() => onCountryClick(row.country ?? row.name ?? '')}
              >
                {row.country ?? row.name}
              </button>
            ))}
          </div>
        )
      }
      plotCalls.push(props)
      return <div data-testid="plot-stub" />
    }
    return Stub
  },
}))

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'light' }),
}))

type PlotlyChartProps = ComponentProps<typeof import('@/components/PlotlyChart').PlotlyChart>
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

vi.mock('@/components/AnalyticsCard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/AnalyticsCard')>()
  return {
    ...actual,
    TimeSeriesChart: (props: ComponentProps<typeof actual.TimeSeriesChart>) =>
      mockTimeSeriesChart ? (
        <div data-testid="timeseries-chart">
          <span data-testid="trace-count">{props.data?.length ?? 0}</span>
        </div>
      ) : (
        <actual.TimeSeriesChart {...props} />
      ),
    LazyMount: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  }
})

vi.mock('@/components/Dashboard/TopTenTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Dashboard/TopTenTable')>()
  return {
    ...actual,
    TopTenTable: (props: ComponentProps<typeof actual.TopTenTable>) =>
      mockTopTenTable ? (
        <div data-testid={`card-${props.field ?? props.title}`}>{props.title}</div>
      ) : (
        <actual.TopTenTable {...props} />
      ),
  }
})

let __fieldValues: { values: { value: string; count: number; label?: string }[] } | undefined = undefined
let __isLoading = false
const { activeFields } = vi.hoisted(() => ({ activeFields: new Set<string>() }))
const useLogFieldsCatalogMock = vi.fn(() => ({ data: undefined as unknown }))
const useMaskIpsMock = vi.fn(() => false)

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useLogFieldsCatalog: () => useLogFieldsCatalogMock(),
  useFieldLabel: () => (id: string) => id,
  useFieldValues: () => ({ data: __fieldValues, isLoading: __isLoading, isFetching: false }),
  useActiveLogFields: () => ({
    ready: true,
    isFieldActive: (id: string) => activeFields.has(id),
    isGroupActive: () => false,
  }),
  useIsAnalyst: () => false,
  useMaskIps: () => useMaskIpsMock(),
  useEnforceMaskedFilters: () => {},
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

vi.mock('@/lib/chart-helpers', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/chart-helpers')>()),
  TIME_HOVER_LAYOUT: { hovermode: 'x unified' },
  makeTimeXAxis: (start: string | null | undefined, end: string | null | undefined) => ({
    type: 'date',
    range: [start, end],
  }),
}))

import { TimeSeriesChart } from '@/components/AnalyticsCard'
import { PlotlyChart, ChartA11yTable, tracesToTable } from '@/components/PlotlyChart'
import { ScorerLatencyChart, ScorerErrorsChart } from '@/components/SessionScoring/StatusPanel'
import { TrafficChart, CardGrid, CARD_CATEGORIES, GeoMap } from '@/app/dashboard/_sections/DashboardClient'
import { TopTenTable, FieldSearchDialog } from '@/components/Dashboard/TopTenTable'
import { useFilterStore, usePopGeoStore } from '@/stores/filterStore'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'

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
  mockTimeSeriesChart = false
  mockTopTenTable = false
  plotCalls.length = 0
  lastIOCallback = null
  lastIOOptions = null
  vi.stubGlobal('IntersectionObserver', MockIntersectionObserver)
  stubResizeObserver()
})

afterEach(() => {
  cleanup()
})

describe('TimeSeriesChart', () => {
  beforeEach(() => {
    plotlyMock.mockClear()
  })

  it('passes through data, merges layout (caller wins), and forwards className/height/onRelayout', () => {
    const traces = [{ x: [1, 2, 3], y: [4, 5, 6] }]
    const onRelayout = vi.fn()
    render(
      <TimeSeriesChart
        data={traces}
        startTime="2026-01-01T00:00:00Z"
        endTime="2026-01-01T01:00:00Z"
        timezone="UTC"
        layout={{ hovermode: 'closest', yaxis: { title: 'count' } }}
        className="border"
        height={420}
        onRelayout={onRelayout}
      />,
    )
    const props = plotlyMock.mock.calls[0]![0]
    expect(props.data).toBe(traces)
    expect(props.className).toBe('border')
    expect(props.height).toBe(420)
    expect(props.onRelayout).toBe(onRelayout)
    expect(props.layout).toMatchObject({
      hovermode: 'closest',
      xaxis: { type: 'date', range: ['2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z'] },
      yaxis: { title: 'count' },
    })
  })

  it('rebuilds the merged layout when the time range or timezone changes', () => {
    const { rerender } = render(
      <TimeSeriesChart data={[]} startTime="2026-01-01T00:00:00Z" endTime="2026-01-01T01:00:00Z" timezone="UTC" />,
    )
    const firstLayout = plotlyMock.mock.calls[0]![0].layout
    rerender(
      <TimeSeriesChart data={[]} startTime="2026-01-01T00:00:00Z" endTime="2026-01-01T02:00:00Z" timezone="UTC" />,
    )
    const secondLayout = plotlyMock.mock.calls.at(-1)![0].layout
    expect(secondLayout).not.toBe(firstLayout)
    expect(secondLayout?.xaxis?.range?.[1]).toBe('2026-01-01T02:00:00Z')
  })
})

const T0 = '2026-10-01T00:00:00+00:00'
const T2 = '2026-10-01T02:00:00+00:00'
function rows() {
  return [
    { hour: T0, total_count: 100, fail_open_count: 5, rtt_p50_us: 1000, rtt_p95_us: 2000, rtt_p99_us: 3000, exec_p50_us: 100, exec_p95_us: 200 },
    { hour: T2, total_count: 0, fail_open_count: 0, rtt_p50_us: null, rtt_p95_us: 4000, rtt_p99_us: null, exec_p50_us: null, exec_p95_us: null },
  ]
}

describe('ScorerErrorsChart & ScorerLatencyChart', () => {
  beforeEach(() => {
    captured = null
    timeseries = { has_latency: true, granularity: 'hour', rows: rows() }
  })

  it('ScorerErrorsChart emits dense fail-open bars + sparse error-rate line on y2 and toggles secondary axis', () => {
    render(<ScorerErrorsChart serviceId="s" sinceHours={24} />)
    const [bars, rate] = captured!.data as unknown as Record<string, unknown>[]
    expect(bars).toMatchObject({
      type: 'bar',
      name: 'Fail-opens',
      x: ['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z', '2026-10-01T02:00:00.000Z'],
      y: [5, 0, 0],
    })
    expect(rate).toMatchObject({ type: 'scatter', mode: 'lines', name: 'Error rate (%)', yaxis: 'y2', x: [T0, T2], y: [5, 0] })
    expect((captured!.layout as Loose).yaxis2).toMatchObject({ overlaying: 'y', side: 'right', ticksuffix: '%' })

    fireEvent.click(screen.getByRole('switch'))
    expect(captured!.data).toHaveLength(1)
    expect((captured!.layout as Loose).yaxis2).toBeUndefined()
  })

  it('ScorerErrorsChart uses per-minute grid and tick format for minute granularity', () => {
    timeseries = {
      has_latency: false,
      granularity: 'minute',
      rows: [
        { hour: '2026-10-01T00:00:00+00:00', total_count: 1, fail_open_count: 1 },
        { hour: '2026-10-01T00:02:00+00:00', total_count: 1, fail_open_count: 2 },
      ],
    }
    render(<ScorerErrorsChart serviceId="s" sinceHours={1} />)
    expect((captured!.data[0] as Loose).y).toEqual([1, 0, 2])
    expect((captured!.layout as Loose).xaxis.tickformat).toBe('%H:%M')
  })

  it('ScorerLatencyChart emits volume bar on y2 + 5 latency lines, toggles volume bar, and handles missing latency', () => {
    const { unmount } = render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
    const traces = captured!.data as unknown as Loose[]
    expect(traces.map(t => t.name)).toEqual(['Inspected requests', 'p50 rtt', 'p95 rtt', 'p99 rtt', 'p50 exec', 'p95 exec'])
    expect(traces[0].y).toEqual([100, 0, 0])
    expect(traces[1].y).toEqual([1, null])
    expect(traces[2].y).toEqual([2, 4])

    fireEvent.click(screen.getByRole('switch'))
    expect((captured!.data as unknown as Loose[]).map(t => t.name)).not.toContain('Inspected requests')
    expect((captured!.layout as Loose).yaxis2).toBeUndefined()
    unmount()

    captured = null
    timeseries = { has_latency: false, granularity: 'hour', rows: rows() }
    render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
    expect(captured).toBeNull()
    expect(screen.getByText(/Waiting for traffic logged with the scorer latency fields/)).toBeInTheDocument()
  })
})

describe('PlotlyChart, ChartA11yTable & tracesToTable', () => {
  beforeEach(() => {
    useRealPlotly = true
  })

  it('withholds Plot until IntersectionObserver fires, or renders immediately when IO is unavailable', async () => {
    const { unmount } = render(<PlotlyChart data={[]} a11yTitle="x" />)
    expect(screen.queryByTestId('plot-stub')).toBeNull()
    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    expect(screen.getByTestId('plot-stub')).toBeInTheDocument()
    unmount()

    vi.stubGlobal('IntersectionObserver', undefined)
    render(<PlotlyChart data={[]} a11yTitle="x" />)
    expect(screen.getByTestId('plot-stub')).toBeInTheDocument()
  })

  it('uses nearest scroll container as root, merges layout/config, renders ChartA11yTable, and forwards onRelayout', async () => {
    const onRelayout = vi.fn()
    const onUpdate = vi.fn()
    const { container } = render(
      <div style={{ overflowY: 'auto' }}>
        <PlotlyChart
          data={[{ x: ['a', 'b'], y: [1, 2], name: 'series' }]}
          layout={{ legend: { orientation: 'v', x: 0.5 }, xaxis: { type: 'date', range: ['a', 'b'] }, yaxis: { title: 'count' } }}
          onRelayout={onRelayout}
          onUpdate={onUpdate}
          a11yTitle="My A11y Caption"
        />
      </div>,
    )
    expect(lastIOOptions?.root).toBe(container.firstElementChild)
    expect(screen.getByRole('table')).toBeInTheDocument()
    expect(document.querySelector('caption')?.textContent).toBe('My A11y Caption')

    await act(async () => { lastIOCallback!([{ isIntersecting: true }]) })
    const props = plotCalls.at(-1)!
    expect(props.layout?.legend).toMatchObject({ orientation: 'v', x: 0.5 })
    expect(props.layout?.xaxis).toMatchObject({ type: 'date', range: ['a', 'b'] })
    expect(props.config).toMatchObject({ responsive: true, displayModeBar: false })
    expect(props.onUpdate).toBe(onUpdate)

    let handler: ((e: PlotlyRelayoutEvent) => void) | undefined
    const fakeGraphDiv: PlotlyGraphDiv = Object.assign(document.createElement('div'), {
      on: (name: string, h: (e: PlotlyRelayoutEvent) => void) => { if (name === 'plotly_relayout') handler = h },
    })
    await act(async () => { props.onInitialized({}, fakeGraphDiv) })
    handler!({ 'xaxis.range[0]': 1 })
    expect(onRelayout).toHaveBeenCalledWith({ 'xaxis.range[0]': 1 })
  })

  it('ChartA11yTable & tracesToTable handle empty, single/multi-series, pie, heatmap, and >500 row truncation', () => {
    for (const emptyInput of [null, {}, undefined, []]) {
      expect(tracesToTable(emptyInput)).toMatchObject({ empty: true, rows: [] })
    }
    expect(tracesToTable([], 'My chart').title).toBe('My chart')
    expect(tracesToTable([{ type: 'heatmap', x: ['t1'], y: ['a'], z: [[1]] }], 'HM').empty).toBe(true)

    const emptyDom = render(<ChartA11yTable shape={tracesToTable([], 'Empty chart')} />)
    expect(emptyDom.container.querySelector('table')).toBeNull()
    expect(emptyDom.container.textContent).toContain('no readable data')
    emptyDom.unmount()

    const multi = tracesToTable([
      { type: 'scatter', name: 'A', x: ['t1', 't2'], y: [1, null] },
      { type: 'scatter', name: 'B', x: ['t2', 't3'], y: [5, 6] },
    ], 'Requests over time')
    expect(multi.columns).toEqual(['x', 'A', 'B'])
    expect(multi.rows).toEqual([['t1', 1, ''], ['t2', '', 5], ['t3', '', 6]])

    const { container, unmount } = render(<ChartA11yTable shape={multi} />)
    expect(container.querySelector('table')?.className).toContain('sr-only')
    container.querySelectorAll('thead th').forEach(th => expect(th.getAttribute('scope')).toBe('col'))
    unmount()

    expect(tracesToTable([{ type: 'pie', labels: ['US', 'CN'], values: [100, 50], name: 'Country share' }])).toMatchObject({
      title: 'Country share',
      columns: ['Label', 'Value'],
      rows: [['US', 100], ['CN', 50]],
    })
    expect(tracesToTable([{ type: 'bar', x: ['a'], y: [1] }]).columns).toEqual(['x', 'Series 1'])

    const x = Array.from({ length: 1200 }, (_, i) => i)
    const bigShape = tracesToTable([{ x, y: x, name: 'S' }], 'Big chart')
    expect(bigShape.rows).toHaveLength(500)
    expect(bigShape.truncatedFrom).toBe(1200)
    render(<ChartA11yTable shape={bigShape} />)
    expect(document.querySelector('caption')?.textContent).toContain('showing first 500 of 1200 rows')
  })
})

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

const config = partial<ReportConfiguration>({
  effectiveInterval: '1 hour',
  validIntervals: new Set(['1 hour']),
  validTrends: new Set(['off', 'auto', '1m', '5m', '1h', '1d']),
})

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
    mockTimeSeriesChart = true
    activeFields.clear()
  })

  it.each([
    ['loading skeleton when aggregates undefined', { isReady: false, aggregates: undefined }, 'Initializing...', false],
    ['worker transform pending skeleton', { aggregates: { time_series: [{ time: '2026-01-01T00:00:00Z', value: 1 }] }, trafficData: [], transformPending: true }, 'Crunching logs...', false],
    ['empty state when trafficData is empty', { aggregates: { time_series: [] }, trafficData: [] }, 'No logs found for this period.', false],
    ['Group C hint when ttfb is not active', { metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] }, 'Requires Infrastructure (Group C) fields to be enabled in Fastly logging.', false],
    ['neutral no-logs copy when ttfb IS active', { metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] }, 'No logs found for this period.', true],
  ])('renders %s', (_label, overrides, expectedText, activateTtfb) => {
    if (activateTtfb) activeFields.add('ttfb')
    renderChart(overrides)
    expect(screen.getByText(expectedText)).toBeInTheDocument()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('mounts TimeSeriesChart with traces and forwards metric, category-legend, and trend clicks', () => {
    const setMetric = vi.fn()
    const toggleCategory = vi.fn()
    const setTrend = vi.fn()
    const trafficData = [
      { type: 'bar', name: 'edge', marker: { color: '#aaa' } },
      { type: 'bar', name: 'origin', marker: { color: '#bbb' } },
      { type: 'scatter', name: 'cached', x: [], y: [], marker: { color: '#111' } },
    ]
    renderChart({ trafficData, setMetric, toggleCategory, setTrend })
    expect(screen.getByTestId('trace-count').textContent).toBe('3')

    fireEvent.click(screen.getByRole('button', { name: /^5xx$/i }))
    expect(setMetric).toHaveBeenCalledWith('5xx')

    fireEvent.click(screen.getByRole('button', { name: /edge/i }))
    expect(toggleCategory).toHaveBeenCalledWith('edge')

    fireEvent.click(screen.getAllByRole('button', { name: /^1m$/ })[0])
    expect(setTrend).toHaveBeenCalledWith('1m')
  })
})

const sampleAggregates = {
  data: {
    status: { total: 100, top: [{ value: 200, label: '200', count: 90 }] },
    ip: { total: 100, top: [{ value: '1.1.1.1', label: '1.1.1.1', count: 10 }] },
  },
}

describe('CardGrid', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockTopTenTable = true
  })

  it('renders skeleton when visibleCardList is empty, and categorized/custom cards once ready', () => {
    const { unmount } = render(
      <CardGrid
        visibleCardList={[]}
        isReady={false}
        isFetchingAggs={false}
        aggregates={undefined}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    for (const cat of CARD_CATEGORIES) expect(screen.getByText(cat.label)).toBeInTheDocument()
    expect(screen.queryByTestId(/^card-/)).toBeNull()
    unmount()

    const toggle = vi.fn()
    const { rerender } = render(
      <CardGrid
        visibleCardList={[
          { id: 'ip', label: 'Client IP', inActiveFormat: false },
          { id: 'status', label: 'HTTP Status', inActiveFormat: false },
          { id: 'custom_field_42', label: 'My Custom Field', inActiveFormat: false },
        ]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={{ data: { ...sampleAggregates.data, custom_field_42: { total: 1, top: [] } } }}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={toggle}
        onRowClick={vi.fn()}
      />,
    )
    expect(screen.getByTestId('card-ip')).toBeInTheDocument()
    expect(screen.getByTestId('card-status')).toBeInTheDocument()
    expect(screen.getByTestId('card-custom_field_42')).toBeInTheDocument()
    expect(screen.getByText('Custom')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /request/i }))
    expect(toggle).toHaveBeenCalledWith('request')

    rerender(
      <CardGrid
        visibleCardList={[{ id: 'ip', label: 'Client IP', inActiveFormat: false }]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={sampleAggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set(['request'])}
        toggleSectionCollapsed={toggle}
        onRowClick={vi.fn()}
      />,
    )
    expect(screen.queryByTestId('card-ip')).toBeNull()
  })

  it('renders CardErrorState + Retry for a bot card when top-bots errors', () => {
    render(
      <CardGrid
        visibleCardList={[{ id: '_bot_name', label: 'Top Bots', inActiveFormat: false }]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={{ data: {} }}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        topBotsError={new Error('top-bots boom')}
        onRetryTopBots={vi.fn()}
        collapsedSections={new Set()}
        toggleSectionCollapsed={() => {}}
        onRowClick={() => {}}
      />,
    )
    expect(screen.getByText('Failed to load bot data.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  })
})

function seedFilters(filters: { column: string; value: string; mode?: 'include' | 'exclude' }[]) {
  useFilterStore.setState({
    filters: filters.map((f, i) => ({
      id: `pill-${i}`,
      column: f.column,
      value: f.value,
      mode: f.mode ?? 'include',
    })),
  })
}

describe('FieldSearchDialog', () => {
  beforeEach(() => {
    useFilterStore.setState({ filters: [] })
    usePopGeoStore.setState({ map: {} })
    __fieldValues = {
      values: [
        { value: 'jfk', count: 500 },
        { value: 'lhr', count: 300 },
        { value: 'den', count: 200 },
        { value: 'syd', count: 100 },
      ],
    }
    __isLoading = false
  })

  it('pins selected values for its own field (including unfetched/exclude/geo), filters by search, and removes on click', async () => {
    usePopGeoStore.setState({ map: { DEN: { city: 'Denver', region: 'CO', country: 'us' } } })
    seedFilters([
      { column: 'country', value: 'US' },
      { column: 'pop', value: 'den' },
      { column: 'pop', value: 'ams' },
      { column: 'pop', value: 'lhr', mode: 'exclude' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    await user.click(screen.getByRole('button', { name: 'Search Edge PoP' }))
    const dialog = await screen.findByRole('dialog')

    expect(within(dialog).getByText('Selected')).toBeInTheDocument()
    expect(within(dialog).queryByRole('button', { name: /filter US/ })).not.toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: 'Remove include filter ams' })).toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: 'Remove exclude filter lhr' })).toBeInTheDocument()

    const pinnedDen = within(dialog).getByRole('button', { name: 'Remove include filter den' })
    expect(pinnedDen.textContent).toContain('Denver, CO - USA')
    expect(pinnedDen.textContent).toContain('200')

    await user.type(within(dialog).getByPlaceholderText('Search for a Edge PoP...'), 'de')
    expect(within(dialog).getByRole('button', { name: 'Remove include filter den' })).toBeInTheDocument()
    expect(within(dialog).queryByRole('button', { name: 'Remove include filter ams' })).not.toBeInTheDocument()

    await user.clear(within(dialog).getByPlaceholderText('Search for a Edge PoP...'))
    await user.click(within(dialog).getByRole('button', { name: 'Remove include filter den' }))
    expect(useFilterStore.getState().filters.map(f => f.value)).not.toContain('den')
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('clicking an unpinned value adds an include filter and closes', async () => {
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    await user.click(screen.getByRole('button', { name: 'Search Edge PoP' }))
    const dialog = await screen.findByRole('dialog')

    await user.click(within(dialog).getByRole('button', { name: /JFK/ }))
    expect(useFilterStore.getState().filters).toEqual([
      expect.objectContaining({ column: 'pop', value: 'jfk', mode: 'include' }),
    ])
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  })
})

const SAMPLE_CATALOG = {
  fields: [
    { id: 'ngwaf_action', group: 'ngwaf' },
    { id: 'cache_status', group: 'cache' },
  ],
  groups: [
    { id: 'ngwaf', label: 'NGWAF' },
    { id: 'cache', label: 'Cache' },
  ],
}

describe('TopTenTable', () => {
  beforeEach(() => {
    __fieldValues = undefined
    useLogFieldsCatalogMock.mockReset()
    useLogFieldsCatalogMock.mockReturnValue({ data: undefined })
    useMaskIpsMock.mockReturnValue(false)
  })

  it.each([
    ['bare empty state when no field is supplied', undefined, false, null],
    ['catalog group hint for disabled group', 'ngwaf_action', true, 'Requires NGWAF fields to be enabled in Fastly logging.'],
    ['virtual NGWAF hint for _ngwaf_bot_name', '_ngwaf_bot_name', true, 'Requires NGWAF fields to be enabled in Fastly logging.'],
    ['User-Agent hint for _bot_name', '_bot_name', true, 'Requires User-Agent field to be enabled in Fastly logging.'],
  ])('renders %s', (_label, field, withCatalog, expectedHint) => {
    if (withCatalog) useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })
    render(<TopTenTable title="Card" field={field} data={{ top: [], total: 0 }} inActiveFormat={false} />)
    expect(screen.getByText('No data available')).toBeInTheDocument()
    if (expectedHint) expect(screen.getByText(expectedHint)).toBeInTheDocument()
    else expect(screen.queryByText(/Requires/i)).toBeNull()
  })

  it('renders at most 10 populated rows, supports click/keyboard activation, and preserves long URL styling', async () => {
    const onRowClick = vi.fn()
    const user = userEvent.setup()
    const top = Array.from({ length: 12 }, (_, idx) => ({
      value: `value-${idx + 1}`,
      label: idx === 0 ? 'Alpha' : undefined,
      count: 12 - idx,
    }))
    const { unmount } = render(<TopTenTable title="Bounded" field="status" data={{ total: 78, top }} onRowClick={onRowClick} />)
    expect(screen.getAllByRole('button', { name: /filter to/i })).toHaveLength(10)
    expect(screen.queryByText('value-11')).toBeNull()

    const alphaRow = screen.getByRole('button', { name: /filter to alpha/i })
    await user.click(alphaRow)
    expect(onRowClick).toHaveBeenCalledWith('status', 'value-1')

    alphaRow.focus()
    await user.keyboard('{Enter}')
    expect(onRowClick).toHaveBeenCalledTimes(2)
    unmount()

    const longUrl = 'https://example.com/very/long/path/with/query/parameters?session=synthetic&region=us'
    render(<TopTenTable title="Top URLs" field="url" data={{ total: 1, top: [{ value: longUrl, count: 1 }] }} />)
    expect(screen.getByText(longUrl).className).toMatch(/flex-1/)
    expect(screen.getByRole('button', { name: `Filter to ${longUrl}` })).toHaveAttribute('title', longUrl)
  })

  it('disables drill-down on `ip` when masking while keeping `oip` clickable', () => {
    useMaskIpsMock.mockReturnValue(true)
    const onRowClick = vi.fn()
    const { unmount } = render(
      <TopTenTable title="Top IPs" field="ip" data={{ total: 2, top: [{ value: '73.217.41.xxx', count: 9 }] }} onRowClick={onRowClick} />,
    )
    expect(screen.getByText('73.217.41.xxx')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /filter to/i })).toBeNull()
    unmount()

    render(<TopTenTable title="Top Origin IPs" field="oip" data={{ total: 1, top: [{ value: '151.101.1.51', count: 9 }] }} onRowClick={onRowClick} />)
    fireEvent.click(screen.getByRole('button', { name: /filter to 151.101.1.51/i }))
    expect(onRowClick).toHaveBeenCalledWith('oip', '151.101.1.51')
  })

  it('copies CSV to clipboard, escapes formula-injection payloads, mounts NotLoggedIndicator, and colors deltas', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })

    const { container } = render(
      <TopTenTable
        title="CSV & Deltas"
        field="ua"
        inActiveFormat={false}
        data={{
          total: 6,
          top: [
            { value: '=1+1', count: 20 },
            { value: 'a"b', count: 5 },
            { value: 'flat', label: 'Flat', count: 10 },
          ],
        }}
        compareData={{
          total: 6,
          top: [
            { value: '=1+1', count: 10 },
            { value: 'a"b', count: 10 },
            { value: 'flat', label: 'Flat', count: 10 },
          ],
        }}
      />,
    )
    expect(container.querySelector('svg.lucide-eye-off')).not.toBeNull()

    fireEvent.click(screen.getByRole('button', { name: /copy table as csv/i }))
    expect(writeText).toHaveBeenCalledWith('ua,count\n"\'=1+1",20\n"a""b",5\n"Flat",10')
    expect(await screen.findByRole('button', { name: /copied!/i })).toBeInTheDocument()

    expect(screen.getByLabelText(/increased by 100%/i).className).toMatch(/text-red-500/)
    expect(screen.getByLabelText(/decreased by 50%/i).className).toMatch(/text-green-500/)
    expect(screen.getByLabelText(/no change/i).className).toMatch(/text-muted-foreground/)
    expect(container.querySelector('svg.lucide-trending-up')).not.toBeNull()
    expect(container.querySelector('svg.lucide-trending-down')).not.toBeNull()
  })
})

const geoCatalog = {
  fields: [{ id: 'country', label: 'Country', group: 'GEO' }],
  groups: [{ id: 'GEO', label: 'Geolocation' }],
}

describe('GeoMap', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    activeFields.clear()
  })

  it('renders loading skeleton, empty states (active vs inactive country field), and interactive ChoroplethMap', async () => {
    const onCountryClick = vi.fn()
    const { rerender } = render(
      <GeoMap isReady={false} isFetchingAggs={false} aggregates={undefined} catalog={geoCatalog} onCountryClick={onCountryClick} />,
    )
    expect(screen.getByText('Initializing...')).toBeInTheDocument()

    rerender(<GeoMap isReady={true} isFetchingAggs={false} aggregates={{ map_data: [] }} catalog={undefined} onCountryClick={onCountryClick} />)
    expect(screen.getByText('Requires Geolocation fields to be enabled in Fastly logging.')).toBeInTheDocument()

    activeFields.add('country')
    rerender(<GeoMap isReady={true} isFetchingAggs={false} aggregates={{ map_data: [] }} catalog={geoCatalog} onCountryClick={onCountryClick} />)
    expect(screen.getByText('No country data in this time range.')).toBeInTheDocument()

    rerender(
      <GeoMap
        isReady={true}
        isFetchingAggs={false}
        aggregates={{ map_data: [{ country: 'US', count: 100 }, { country: 'DE', count: 40 }] }}
        catalog={geoCatalog}
        onCountryClick={onCountryClick}
      />,
    )
    await waitFor(() => expect(screen.getByTestId('choropleth-map')).toBeInTheDocument())
    expect(screen.getByTestId('marker-count').textContent).toBe('2')
    fireEvent.click(screen.getByTestId('country-US'))
    expect(onCountryClick).toHaveBeenCalledWith('US')
  })
})
