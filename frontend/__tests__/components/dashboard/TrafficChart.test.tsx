/**
 * Consolidated tests for TrafficChart, CardGrid, and FieldSearchDialog.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent, cleanup, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'
import type { PlotlyTrace } from '@/types/api'

vi.mock('@/components/AnalyticsCard', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/AnalyticsCard')>()),
  TimeSeriesChart: ({ data }: { data: PlotlyTrace[] }) => (
    <div data-testid="timeseries-chart">
      <span data-testid="trace-count">{data?.length ?? 0}</span>
    </div>
  ),
  LazyMount: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

vi.mock('@/components/PlotlyChart', async () => (await import('@/__tests__/helpers/page-smoke')).plotlyChartModuleMock())

vi.mock('@/components/Dashboard/TopTenTable', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Dashboard/TopTenTable')>()),
  TopTenTable: ({ title, field }: { title: string; field?: string }) => (
    <div data-testid={`card-${field ?? title}`}>{title}</div>
  ),
}))

let __fieldValues: { values: { value: string; count: number; label?: string }[] } | undefined = {
  values: [
    { value: 'jfk', count: 500 },
    { value: 'lhr', count: 300 },
    { value: 'den', count: 200 },
    { value: 'syd', count: 100 },
  ],
}
let __isLoading = false

const { activeFields } = vi.hoisted(() => ({ activeFields: new Set<string>() }))
vi.mock('@/hooks/useBootstrap', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useBootstrap')>()
  const base = (await import('@/__tests__/helpers/page-smoke')).activeLogFieldsSetModuleMock(activeFields)
  return {
    ...actual,
    ...base,
    useFieldValues: () => ({ data: __fieldValues, isLoading: __isLoading, isFetching: false }),
  }
})

import { TrafficChart } from '@/app/dashboard/_sections/DashboardClient'
import { CardGrid, CARD_CATEGORIES } from '@/app/dashboard/_sections/DashboardClient'
import { FieldSearchDialog } from '@/components/Dashboard/TopTenTable'
import { useFilterStore } from '@/stores/filterStore'
import { usePopGeoStore } from '@/stores/filterStore'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'
import { partial } from '../../helpers/page-smoke'

afterEach(() => {
  cleanup()
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
    activeFields.clear()
  })

  it('renders the loading skeleton when aggregates are undefined', () => {
    renderChart({ isReady: false, aggregates: undefined })
    expect(screen.getByText('Initializing...')).toBeInTheDocument()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('renders the empty state when trafficData is empty but aggregates are ready', () => {
    renderChart({ aggregates: { time_series: [] }, trafficData: [] })
    expect(screen.getByText('No data available')).toBeInTheDocument()
    expect(screen.getByText('No logs found for this period.')).toBeInTheDocument()
    expect(screen.queryByTestId('timeseries-chart')).toBeNull()
  })

  it('shows the "Requires Infrastructure (Group C)" hint when ttfb is NOT active', () => {
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
    fireEvent.click(screen.getByRole('button', { name: /edge/i }))
    expect(toggleCategory).toHaveBeenCalledWith('edge')
  })

  it('forwards trend-row clicks to setTrend', () => {
    const setTrend = vi.fn()
    renderChart({ setTrend })
    const oneMin = screen.getAllByRole('button', { name: /^1m$/ })[0]
    fireEvent.click(oneMin)
    expect(setTrend).toHaveBeenCalledWith('1m')
  })
})

const aggregates = {
  data: {
    status: { total: 100, top: [{ value: 200, label: '200', count: 90 }] },
    ip: { total: 100, top: [{ value: '1.1.1.1', label: '1.1.1.1', count: 10 }] },
  },
}

describe('CardGrid', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders the skeleton (one section per CARD_CATEGORIES) when visibleCardList is empty', () => {
    render(
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
    for (const cat of CARD_CATEGORIES) {
      expect(screen.getByText(cat.label)).toBeInTheDocument()
    }
    expect(screen.getAllByText('Initializing...').length).toBeGreaterThan(0)
    expect(screen.queryByTestId(/^card-/)).toBeNull()
  })

  it('renders categorized sections + the expected cards once aggregates arrive', () => {
    const visibleCardList = [
      { id: 'ip', label: 'Client IP', inActiveFormat: false },
      { id: 'status', label: 'HTTP Status', inActiveFormat: false },
    ]
    render(
      <CardGrid
        visibleCardList={visibleCardList}
        isReady={true}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    expect(screen.getByTestId('card-ip')).toBeInTheDocument()
    expect(screen.getByTestId('card-status')).toBeInTheDocument()
    expect(screen.getByText('Request')).toBeInTheDocument()
    expect(screen.queryByText('Cache')).toBeNull()
    expect(screen.queryByText('Geography')).toBeNull()
  })

  it('invokes toggleSectionCollapsed with the section id when the header is clicked', () => {
    const toggle = vi.fn()
    render(
      <CardGrid
        visibleCardList={[{ id: 'ip', label: 'Client IP', inActiveFormat: false }]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={toggle}
        onRowClick={vi.fn()}
      />,
    )
    const header = screen.getByRole('button', { name: /request/i })
    fireEvent.click(header)
    expect(toggle).toHaveBeenCalledWith('request')
  })

  it('hides the cards grid when the section is collapsed', () => {
    render(
      <CardGrid
        visibleCardList={[{ id: 'ip', label: 'Client IP', inActiveFormat: false }]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set(['request'])}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    const header = screen.getByRole('button', { name: /request/i })
    expect(header).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByTestId('card-ip')).toBeNull()
  })

  it('groups uncategorized cards under a Custom section', () => {
    render(
      <CardGrid
        visibleCardList={[{ id: 'custom_field_42', label: 'My Custom Field', inActiveFormat: false }]}
        isReady={true}
        isFetchingAggs={false}
        aggregates={{ data: { custom_field_42: { total: 1, top: [] } } }}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    expect(screen.getByText('Custom')).toBeInTheDocument()
    expect(screen.getByTestId('card-custom_field_42')).toBeInTheDocument()
  })

  it('renders CardErrorState + Retry for a bot card when top-bots errors (not a forever spinner)', () => {
    const onRetryTopBots = vi.fn()
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
        onRetryTopBots={onRetryTopBots}
        collapsedSections={new Set()}
        toggleSectionCollapsed={() => {}}
        onRowClick={() => {}}
      />,
    )
    expect(screen.getByText('Failed to load bot data.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
    expect(screen.queryByText('Loading...')).toBeNull()
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

async function openDialog(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Search Edge PoP' }))
  return screen.findByRole('dialog')
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

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('pins currently selected values at the top, above the fetched list', async () => {
    seedFilters([{ column: 'pop', value: 'den' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(within(dialog).getByText('Selected')).toBeInTheDocument()

    const pinned = within(dialog).getByRole('button', { name: 'Remove include filter den' })
    const rows = within(dialog).getAllByRole('button').filter(b =>
      /DEN|JFK|LHR|SYD/.test(b.textContent ?? '')
    )
    expect(rows[0]).toBe(pinned)
    expect(rows.filter(b => (b.textContent ?? '').includes('DEN'))).toHaveLength(1)
    expect(pinned.textContent).toContain('200')
  })

  it('pins selected values even when they are not in the fetched values', async () => {
    seedFilters([{ column: 'pop', value: 'ams' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(
      within(dialog).getByRole('button', { name: 'Remove include filter ams' })
    ).toBeInTheDocument()
  })

  it('only pins filters for its own field', async () => {
    seedFilters([
      { column: 'country', value: 'US' },
      { column: 'pop', value: 'lhr' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(within(dialog).getByRole('button', { name: 'Remove include filter lhr' })).toBeInTheDocument()
    expect(within(dialog).queryByRole('button', { name: /filter US/ })).not.toBeInTheDocument()
  })

  it('clicking a pinned value removes the filter and keeps the dialog open', async () => {
    seedFilters([
      { column: 'pop', value: 'den' },
      { column: 'pop', value: 'syd' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.click(within(dialog).getByRole('button', { name: 'Remove include filter den' }))

    expect(useFilterStore.getState().filters.map(f => f.value)).toEqual(['syd'])
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: /Remove include filter syd/ })).toBeInTheDocument()
  })

  it('marks exclude filters distinctly in the pinned section', async () => {
    seedFilters([{ column: 'pop', value: 'lhr', mode: 'exclude' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(
      within(dialog).getByRole('button', { name: 'Remove exclude filter lhr' })
    ).toBeInTheDocument()
  })

  it('narrows pinned values by the search text', async () => {
    seedFilters([
      { column: 'pop', value: 'den' },
      { column: 'pop', value: 'syd' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.type(within(dialog).getByPlaceholderText('Search for a Edge PoP...'), 'sy')

    expect(within(dialog).getByRole('button', { name: 'Remove include filter syd' })).toBeInTheDocument()
    expect(
      within(dialog).queryByRole('button', { name: 'Remove include filter den' })
    ).not.toBeInTheDocument()
  })

  it('renders pinned pop values through PopLabel (code + geo)', async () => {
    usePopGeoStore.setState({ map: { DEN: { city: 'Denver', region: 'CO', country: 'us' } } })
    seedFilters([{ column: 'pop', value: 'den' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    const pinned = within(dialog).getByRole('button', { name: 'Remove include filter den' })
    expect(pinned.textContent).toContain('DEN')
    expect(pinned.textContent).toContain('Denver, CO - USA')
  })

  it('clicking an unpinned value still adds an include filter and closes', async () => {
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.click(within(dialog).getByRole('button', { name: /JFK/ }))

    const pills = useFilterStore.getState().filters
    expect(pills).toHaveLength(1)
    expect(pills[0]).toMatchObject({ column: 'pop', value: 'jfk', mode: 'include' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})
