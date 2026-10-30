/** @vitest-environment jsdom */
import {
  createTestQueryClient, createUseSSEMock, makeQueryWrapper, partial, renderWithQuery, selectMockFactory,
  setupNextHeadersMock, spyOnConsoleError, stubIntersectionObserver, stubPointerCaptureAndScroll, stubResizeObserver, stubSystemTimeZone,
} from '@/__tests__/helpers/page-smoke'
import { render, screen, fireEvent, cleanup, act, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, test, expect, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { QueryClient, type QueryCacheNotifyEvent } from '@tanstack/react-query'
import { axe } from 'vitest-axe'
import React, { type ComponentProps, type ReactNode } from 'react'
import { renderToString } from 'react-dom/server'
import * as bootstrapHooks from '@/hooks/useBootstrap'
import type { CellContext, ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import type { PlotlyConfig, PlotlyGraphDiv, PlotlyLayout, PlotlyRelayoutEvent, PlotlyTrace } from '@/types/api'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Loose = { [k: string]: any }
if (typeof Element !== 'undefined') {
  Element.prototype.getAnimations ??= () => []
}

type PlotStubProps = {
  layout?: PlotlyLayout
  config?: PlotlyConfig
  onUpdate?: unknown
  onInitialized: (figure: unknown, graphDiv: PlotlyGraphDiv) => void
  data?: Array<{ country?: string; name?: string }>
  onCountryClick?: (c: string) => void
}
const plotCalls: PlotStubProps[] = []
type Captured = { data: PlotlyTrace[]; layout?: PlotlyLayout }
let captured: Captured | null = null

const { setHeaders } = setupNextHeadersMock()
const { state: sseState, resetState: resetSSE } = createUseSSEMock()
const mocks = vi.hoisted(() => ({
  useNowMs: vi.fn(() => 0), status: vi.fn(), lastSync: vi.fn(), analyst: vi.fn(() => false),
  bootstrap: vi.fn((): Record<string, unknown> => ({ data: { debug_state: { debug_responses_enabled: true } } })),
  formatTimeAgo: vi.fn((ts: string) => `ago:${ts}`), showToast: vi.fn(), useRealSelect: false,
  mockReactQueryForFilterBar: false,
  capturedPredicate: null as ((q: { queryKey?: readonly unknown[] }) => boolean) | null,
  logExtentsData: undefined as unknown,
  maskIps: vi.fn(() => false),
  fieldValues: undefined as { values: { value: string | null; count: number; label?: string }[] } | undefined,
  fieldValuesLoading: false,
  activeFields: new Set<string>(),
  logFieldsCatalog: vi.fn(() => ({ data: undefined as unknown })),
  useRealPlotly: false,
  mockTimeSeriesChart: false,
  mockTopTenTable: false,
  timeseries: undefined as unknown,
}))

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/network'))
vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>()
  return {
    ...actual,
    useQuery: (...args: Parameters<typeof actual.useQuery>) =>
      mocks.timeseries !== undefined
        ? ({ data: mocks.timeseries, isLoading: false, isFetching: false, isError: false, error: null } as unknown as ReturnType<typeof actual.useQuery>)
        : mocks.mockReactQueryForFilterBar
          ? ({ data: mocks.logExtentsData, isLoading: false, error: null } as ReturnType<typeof actual.useQuery>)
          : actual.useQuery(...args),
    useMutation: (...args: Parameters<typeof actual.useMutation>) =>
      mocks.mockReactQueryForFilterBar
        ? ({ mutate: vi.fn(), isPending: false } as unknown as ReturnType<typeof actual.useMutation>)
        : actual.useMutation(...args),
    useQueryClient: (...args: Parameters<typeof actual.useQueryClient>) =>
      mocks.mockReactQueryForFilterBar
        ? ({ getQueryState: () => undefined, invalidateQueries: vi.fn() } as unknown as ReturnType<typeof actual.useQueryClient>)
        : actual.useQueryClient(...args),
    useIsFetching: (opts?: { predicate?: (q: { queryKey?: readonly unknown[] }) => boolean }) => {
      if (mocks.mockReactQueryForFilterBar) {
        if (opts?.predicate) mocks.capturedPredicate = opts.predicate
        return 0
      }
      return actual.useIsFetching(opts as Parameters<typeof actual.useIsFetching>[0])
    },
  }
})
vi.mock('@/components/ui/select', async (importOriginal) => {
  const real = await importOriginal<typeof import('@/components/ui/select')>()
  const mock = selectMockFactory() as unknown as Record<string, React.ComponentType<Record<string, unknown>>>
  const wrap = (k: keyof typeof real) => function SelectWrap(props: Record<string, unknown>) {
    const Comp = (mocks.useRealSelect ? real[k] : mock[k]) as React.ComponentType<Record<string, unknown>>
    return <Comp {...props} />
  }
  return { ...real, Select: wrap('Select'), SelectTrigger: wrap('SelectTrigger'), SelectValue: wrap('SelectValue'), SelectContent: wrap('SelectContent'), SelectSeparator: wrap('SelectSeparator'), SelectItem: wrap('SelectItem') }
})
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))
vi.mock('@/hooks/useDashboardCards', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useDashboardCards')>()),
  useDebounce: (v: string) => v,
}))
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
      mocks.useRealPlotly ? <actual.PlotlyChart {...props} /> : renderMockPlotly(props),
  }
})
vi.mock('@/components/AnalyticsCard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/AnalyticsCard')>()
  return {
    ...actual,
    TimeSeriesChart: (props: ComponentProps<typeof actual.TimeSeriesChart>) =>
      mocks.mockTimeSeriesChart ? (
        <div data-testid="timeseries-chart">
          <span data-testid="trace-count">{props.data?.length ?? 0}</span>
        </div>
      ) : (
        <actual.TimeSeriesChart {...props} />
      ),
    LazyMount: (props: ComponentProps<typeof actual.LazyMount>) =>
      mocks.mockTopTenTable ? <>{props.children}</> : <actual.LazyMount {...props} />,
  }
})
vi.mock('@/components/Dashboard/TopTenTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Dashboard/TopTenTable')>()
  return {
    ...actual,
    TopTenTable: (props: ComponentProps<typeof actual.TopTenTable>) =>
      mocks.mockTopTenTable ? (
        <div data-testid={`card-${props.field ?? props.title}`}>{props.title}</div>
      ) : (
        <actual.TopTenTable {...props} />
      ),
  }
})
vi.mock('@/components/SessionScoring/ScoringAdminPanels', async (orig) => {
  const actual = await orig<typeof import('@/components/SessionScoring/ScoringAdminPanels')>()
  return {
    ...actual,
    useScorerTimeseries: () => ({
      data: mocks.timeseries,
      isLoading: false,
      isFetching: false,
      isError: false,
      error: null,
    }),
  }
})
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsAnalyst: () => mocks.analyst(), useBootstrap: () => mocks.bootstrap(),
  useMounted: () => true, useMaskIps: () => mocks.maskIps(), useEnforceMaskedFilters: () => {},
  useLogFieldsCatalog: () => mocks.logFieldsCatalog(),
  useFieldLabel: () => (id: string) => id,
  useFieldValues: () => ({ data: mocks.fieldValues, isLoading: mocks.fieldValuesLoading, isFetching: false }),
  useActiveLogFields: () => ({
    ready: true,
    isFieldActive: (id: string) => mocks.activeFields.has(id),
    isGroupActive: () => false,
  }),
}))
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState, useNowMs: () => mocks.useNowMs(), useSyncStatus: () => mocks.status(),
  useLastSync: () => mocks.lastSync(), useHeaderBadgeStream: () => ({ state: 'idle' }),
  useAdminEventStream: () => ({ state: 'idle' }),
  useDateFormat: () => ({ full: (d: string) => `FULL(${d})`, abbr: () => 'TZ' }), cronCacheBust: vi.fn(),
}))
vi.mock('@/lib/utils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/utils')>()),
  formatTimeAgo: (ts: string) => mocks.formatTimeAgo(ts), showToast: mocks.showToast,
  TIME_HOVER_LAYOUT: { hovermode: 'x unified' },
  makeTimeXAxis: (start: string | null | undefined, end: string | null | undefined) => ({
    type: 'date',
    range: [start, end],
  }),
}))
vi.mock('@/components/SSEModal', () => ({ ControlledSSEModal: () => null, SSEModal: () => null, SSEProgressView: () => null, SSEExecutionSection: () => null, SSEDialogFooter: () => null }))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()), BackfillRangeModal: () => null }))
vi.mock('@/components/ReportLayout', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/components/ReportLayout')>()), NoServiceSelected: () => null }))

import { DeltaIndicator } from '@/components/Insights/InsightCard'
import { ErrorBoundary, LazyMount, TimeSeriesChart } from '@/components/AnalyticsCard'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { ServerFooter } from '@/components/ServerFooter'
import { Sparkline, TrafficChart, CardGrid, CARD_CATEGORIES, GeoMap } from '@/app/dashboard/_sections/DashboardClient'
import { ServiceSwitcher, SyncStatusBadge, TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { TimezoneSwitcher } from '@/components/AppLayout'
import { useDebugStore, useFilterStore, usePopGeoStore, useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { getTimezoneAbbr, DEBUG_RESPONSES_COOKIE } from '@/lib/utils'
import { DebugPanel } from '@/components/DebugPanel'
import { DiagnosticsPanel } from '@/app/admin/_sections/OperationsOverview'
import { server } from '@/__tests__/helpers/msw/handlers'
import { client, getApiBase } from '@/lib/api'
import { LogSyncSection } from '@/components/CronSettingsModal/CronSettingsModal'
import LogsClient, { CronLiveLog, QuickActionsBar, SchemaTab, CRON_EXPLANATIONS, CRON_DISPLAY_NAMES, CRON_GROUPS } from '@/app/logs/_sections/LogsClient'
import { DataTable, DataTableReadonly, DateTimeCell, FilterValueCell, buildDashboardFilterUrl, makeLatencyColumns, type LatencyRow } from '@/components/DataTable'
import { BotSourcesPanel } from '@/app/control-room/_sections/ControlRoomClient'
import { RealtimeChart } from '@/app/control-room/_sections/ControlRoomWidgets'
import {
  GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp,
  AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp,
  HealthBadge, SHIELDING_COLUMNS, getShieldingLabels, adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT, SHIELDING_MIN_REQUESTS_OPTIONS,
} from '@/app/network/page'
import { FilterBar, ActiveFiltersBanner, AddFilterDialogContent } from '@/components/FilterBar/FilterBar'
import { Dialog } from '@/components/ui/dialog'
import { usePathname } from 'next/navigation'
import { PlotlyChart, ChartA11yTable, tracesToTable } from '@/components/PlotlyChart'
import { ScorerLatencyChart, ScorerErrorsChart } from '@/components/SessionScoring/StatusPanel'
import { TopTenTable, FieldSearchDialog } from '@/components/Dashboard/TopTenTable'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})
beforeEach(() => {
  stubResizeObserver()
  mocks.useRealSelect = false
  mocks.mockReactQueryForFilterBar = false
  mocks.useRealPlotly = false
  mocks.mockTimeSeriesChart = false
  mocks.mockTopTenTable = false
  mocks.maskIps.mockReturnValue(false)
  mocks.logFieldsCatalog.mockReturnValue({ data: undefined })
  mocks.fieldValues = undefined
  mocks.fieldValuesLoading = false
  mocks.activeFields.clear()
  mocks.analyst.mockReturnValue(false)
  mocks.bootstrap.mockReturnValue({ data: { debug_state: { debug_responses_enabled: true } } })
  mocks.formatTimeAgo.mockImplementation((ts: string) => `ago:${ts}`)
  mocks.useNowMs.mockReturnValue(0)
})
afterEach(() => { cleanup(); mocks.useNowMs.mockClear() })

describe('DeltaIndicator', () => {
  it('renders nothing when baseline is null or zero', () => {
    expect(render(<DeltaIndicator current={100} baseline={null} />).container.firstChild).toBeNull()
    expect(render(<DeltaIndicator current={100} baseline={0} />).container.firstChild).toBeNull()
  })
  it('renders red TrendingUp when current > baseline, green TrendingDown when <, and neutral Minus when |pct| < 1', () => {
    const { container: up } = render(<DeltaIndicator current={200} baseline={100} />)
    expect(up.querySelector('span')?.className).toMatch(/text-red-600/)
    expect(up.querySelector('span')?.textContent).toMatch(/\+100%/)
    const { container: down } = render(<DeltaIndicator current={50} baseline={100} />)
    expect(down.querySelector('span')?.className).toMatch(/text-green-700/)
    expect(down.querySelector('span')?.textContent).toMatch(/-50%/)
    const { container: neutral } = render(<DeltaIndicator current={1000.5} baseline={1000} />)
    expect(neutral.querySelector('span')).toBeNull()
    expect(neutral.querySelector('svg')).not.toBeNull()
    expect(render(<DeltaIndicator current={1234} baseline={1000} />).container.textContent).toMatch(/\+234/)
  })
})

function Boom(): React.ReactElement { throw new Error('kaboom') }

describe('ErrorBoundary', () => {
  let errSpy: ReturnType<typeof vi.spyOn>
  beforeEach(() => { errSpy = vi.spyOn(console, 'error').mockImplementation(() => {}) })
  afterEach(() => { errSpy.mockRestore() })
  it('renders default fallback when a child throws, custom fallback when provided, and children when ok', () => {
    const { unmount: u1 } = render(<ErrorBoundary><Boom /></ErrorBoundary>)
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('kaboom')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument(); u1()
    const { unmount: u2 } = render(<ErrorBoundary><div data-testid="ok">all good</div></ErrorBoundary>)
    expect(screen.getByTestId('ok')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).toBeNull(); u2()
    render(<ErrorBoundary fallback={(err) => <div data-testid="custom">caught: {err.message}</div>}><Boom /></ErrorBoundary>)
    expect(screen.getByTestId('custom')).toHaveTextContent('caught: kaboom')
  })
})

describe('HelpDialog', () => {
  it('renders title, icon, and children when open, nothing when closed, and respects size prop', () => {
    const { unmount: u1 } = render(<HelpDialog open onOpenChange={vi.fn()} title="My Title" icon={<span data-testid="help-icon">★</span>}><p>My content</p></HelpDialog>)
    expect(screen.getByText('My Title')).toBeInTheDocument()
    expect(screen.getByText('My content')).toBeInTheDocument()
    expect(screen.getByTestId('help-icon')).toBeInTheDocument()
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-lg'))).toBe(true); u1()
    const { unmount: u2 } = render(<HelpDialog open={false} onOpenChange={vi.fn()} title="Hidden"><p>Hidden body</p></HelpDialog>)
    expect(screen.queryByText('Hidden body')).not.toBeInTheDocument(); u2()
    const { unmount: u3 } = render(<HelpDialog open onOpenChange={vi.fn()} title="Large-only" size="xl">body</HelpDialog>)
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-xl'))).toBe(true); u3()
  })
})

describe('LazyMount', () => {
  afterEach(() => { vi.unstubAllGlobals() })
  it('mounts children once IntersectionObserver fires isIntersecting or immediately when unavailable', () => {
    const io = stubIntersectionObserver()
    const { unmount } = render(<LazyMount><div data-testid="child">payload</div></LazyMount>)
    expect(screen.queryByTestId('child')).toBeNull()
    act(() => { io.fire(true) })
    expect(screen.getByTestId('child')).toBeInTheDocument(); unmount()
    vi.stubGlobal('IntersectionObserver', undefined)
    render(<LazyMount><div data-testid="child">payload</div></LazyMount>)
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })
})

describe('ServerFooter', () => {
  beforeEach(() => {
    setHeaders({})
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, text: async () => 'secret-instance' }))
    process.env.COMMIT_HASH = 'abc1234'
  })
  afterEach(() => { vi.unstubAllGlobals() })
  it('renders infrastructure details for admin requests and nothing for Caddy-proxied requests', async () => {
    const el = await ServerFooter()
    expect(el).not.toBeNull()
    expect(JSON.stringify(el)).toContain('abc1234')
    const fetchSpy = vi.fn()
    vi.stubGlobal('fetch', fetchSpy)
    setHeaders({ 'x-proxied-by-caddy': 'true' })
    expect(await ServerFooter()).toBeNull()
    expect(fetchSpy).not.toHaveBeenCalled()
  })
})

const pts = (vals: number[]) => vals.map((value, i) => ({ ts: new Date(2026, 0, 1, 0, i).toISOString(), value }))

describe('Sparkline', () => {
  it('renders nothing for < 2 points and an svg polyline with custom height for >= 2 points', () => {
    expect(render(<Sparkline points={[]} />).container.firstChild).toBeNull()
    expect(render(<Sparkline points={pts([1])} />).container.firstChild).toBeNull()
    const { container: c1 } = render(<Sparkline points={pts([0, 5, 10])} />)
    const poly1 = c1.querySelector('polyline')!
    expect(poly1.getAttribute('points')!.trim().split(/\s+/)).toHaveLength(3)
    const { container: c2 } = render(<Sparkline points={pts([10, 20, 30, 40])} height={64} />)
    const svg2 = c2.querySelector('svg')!
    expect(svg2.getAttribute('height')).toBe('64')
    expect(svg2.querySelector('polyline')!.getAttribute('points')).not.toEqual(poly1.getAttribute('points'))
  })
})

describe('TimeAgo', () => {
  it('renders formatTimeAgo output, subscribes to 1 Hz tick, and handles null/missing timestamp', () => {
    const { unmount } = render(<TimeAgo timestamp="2026-06-15T12:00:00Z" />)
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
    expect(mocks.useNowMs).toHaveBeenCalled(); unmount()
    expect(render(<TimeAgo timestamp={null} />).container.textContent).toBe('')
    render(<TimeAgo timestamp={undefined} fallback={<span>Never</span>} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
  })
})

describe('BackfillRangeModal', () => {
  async function renderModal() {
    const { BackfillRangeModal } = await vi.importActual<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>('@/components/SyncStatusBadge/SyncStatusBadge')
    useTimezoneStore.setState({ mode: 'manual', timezone: 'UTC' })
    const onStartSync = vi.fn(), onOpenChange = vi.fn()
    render(<BackfillRangeModal open onOpenChange={onOpenChange} onStartSync={onStartSync} />)
    return { onStartSync, onOpenChange }
  }

  test('start is disabled until both times are set and start precedes end', async () => {
    await renderModal()
    const start = screen.getByLabelText(/start time/i), end = screen.getByLabelText(/end time/i)
    const button = screen.getByRole('button', { name: /start backfill/i })
    expect(button).toBeDisabled()
    fireEvent.change(start, { target: { value: '2026-01-02T00:00:00' } })
    expect(button).toBeDisabled()
    fireEvent.change(end, { target: { value: '2026-01-01T00:00:00' } })
    expect(button).toBeDisabled()
    fireEvent.change(end, { target: { value: '2026-01-03T00:00:00' } })
    expect(button).toBeEnabled()
  })

  test('starting passes the chosen range and closes the dialog', async () => {
    const { onStartSync, onOpenChange } = await renderModal()
    fireEvent.change(screen.getByLabelText(/start time/i), { target: { value: '2026-01-01T00:00:00' } })
    fireEvent.change(screen.getByLabelText(/end time/i), { target: { value: '2026-01-02T00:00:00' } })
    await userEvent.click(screen.getByRole('button', { name: /start backfill/i }))
    expect(onStartSync).toHaveBeenCalledWith({ start: '2026-01-01T00:00:00.000Z', end: '2026-01-02T00:00:00.000Z' })
    expect(onOpenChange).toHaveBeenCalledWith(false)
  })
})

describe('HealthBadge', () => {
  test('null → em-dash; ≥80 → green; 50-79 → yellow; <50 → destructive (no color leak)', () => {
    render(<HealthBadge score={null} />)
    expect(screen.getByText('—')).toBeInTheDocument()
    const hi = render(<HealthBadge score={92} />)
    expect(hi.getByText('92/100')).toBeInTheDocument()
    expect(hi.container.querySelector('[class*="text-green"]')).not.toBeNull(); hi.unmount()
    const mid = render(<HealthBadge score={65} />)
    expect(mid.getByText('65/100')).toBeInTheDocument()
    expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull(); mid.unmount()
    const lo = render(<HealthBadge score={20} />)
    expect(lo.getByText('20/100')).toBeInTheDocument()
    expect(lo.container.querySelector('[class*="text-green"]')).toBeNull()
    expect(lo.container.querySelector('[class*="text-yellow"]')).toBeNull(); lo.unmount()
  })
})

describe('SHIELDING_COLUMNS', () => {
  const makeInfo = (value: unknown, original: Record<string, unknown> = {}) => ({ getValue: () => value, row: { original } })
  const cellOf = (id: string) => SHIELDING_COLUMNS.find((c) => c.id === id)!.cell as (info: ReturnType<typeof makeInfo>) => React.ReactNode

  test('every column exposes id + accessorKey + meta.label + header + cell', () => {
    expect(SHIELDING_COLUMNS.length).toBeGreaterThanOrEqual(8)
    for (const col of SHIELDING_COLUMNS) {
      expect(col.id).toBeTruthy()
      expect((col as { accessorKey?: string }).accessorKey).toBe(col.id)
      expect((col.meta as { label?: string } | undefined)?.label).toBeTruthy()
      expect(typeof col.header).toBe('function')
      expect(typeof col.cell).toBe('function')
    }
  })

  test('edge_pop cell renders value + destructive class when anomaly_static', () => {
    const { container } = render(<>{cellOf('edge_pop')(makeInfo('SFO', { anomaly_static: true }))}</>)
    expect(container.textContent).toMatch(/SFO/)
    expect(container.querySelector('[class*="text-destructive"]')).not.toBeNull()
  })

  test('requests cell uses locale separators; p50/p95/p99 use fixed-point ms', () => {
    const reqs = render(<>{cellOf('requests')(makeInfo(1234567))}</>)
    expect(reqs.container.textContent).toMatch(/1[.,]234[.,]567/); reqs.unmount()
    for (const id of ['p50_ms', 'p95_ms', 'p99_ms'] as const) {
      const r = render(<>{cellOf(id)(makeInfo(12.345))}</>)
      expect(r.container.textContent).toMatch(/12\.3ms/); r.unmount()
    }
  })

  test('light_speed_rtt_ms: em-dash when null, fixed-point ms when present', () => {
    const a = render(<>{cellOf('light_speed_rtt_ms')(makeInfo(null))}</>)
    expect(a.container.textContent).toMatch(/—/); a.unmount()
    const b = render(<>{cellOf('light_speed_rtt_ms')(makeInfo(4.2))}</>)
    expect(b.container.textContent).toMatch(/4\.2ms/); b.unmount()
  })

  test('efficiency_ratio: null → em-dash; <1.5 → green; 1.5-3 → yellow; ≥3 → destructive', () => {
    const cell = cellOf('efficiency_ratio')
    const nul = render(<>{cell(makeInfo(null))}</>); expect(nul.container.textContent).toMatch(/—/); nul.unmount()
    const lo = render(<>{cell(makeInfo(1.2))}</>); expect(lo.container.querySelector('[class*="text-green"]')).not.toBeNull(); lo.unmount()
    const mid = render(<>{cell(makeInfo(2.4))}</>); expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull(); mid.unmount()
    const hi = render(<>{cell(makeInfo(5.0))}</>); expect(hi.container.querySelector('[class*="text-destructive"]')).not.toBeNull(); hi.unmount()
  })
})

describe('getShieldingLabels & stateless help components', () => {
  test('getShieldingLabels maps known ids, falls back to id for unknown, preserves order', () => {
    expect(getShieldingLabels(['edge_pop', 'requests'])).toEqual([{ id: 'edge_pop', label: 'Edge POP' }, { id: 'requests', label: 'Requests' }])
    expect(getShieldingLabels(['mystery_field'])).toEqual([{ id: 'mystery_field', label: 'mystery_field' }])
    expect(getShieldingLabels(['p99_ms', 'p50_ms', 'p95_ms']).map((o) => o.id)).toEqual(['p99_ms', 'p50_ms', 'p95_ms'])
  })

  test.each([
    ['GlobalHealthHelp', GlobalHealthHelp, [/0[–-]100 score/i]],
    ['AvgRttHelp', AvgRttHelp, [/TCP Round Trip Time/i]],
    ['WorstAsnHelp', WorstAsnHelp, [/Autonomous System Number/i]],
    ['WorstRegionHelp', WorstRegionHelp, [/country experiencing the lowest/i]],
    ['HeatmapHelp', HeatmapHelp, [/row is an ISP/i, /column is a time bucket/i]],
    ['AsnLeaderboardHelp', AsnLeaderboardHelp, [/P95 \/ P99 RTT/i]],
    ['MetroLeaderboardHelp', MetroLeaderboardHelp, [/city or metro area/i]],
    ['ShieldingHelp', ShieldingHelp, [/Excellent/i, /Moderate/i, /Investigate/i]],
  ] as const)('%s renders expected help copy', (_name, Comp, patterns) => {
    const { container } = render(<Comp />)
    for (const pat of patterns) expect(container.textContent).toMatch(pat)
  })
})

describe('adjustShieldingRows', () => {
  const eligible = (requests: number) => ({ edge_pop: 'LAX', shield_pop: 'SJC', requests, efficiency_ratio: 20, anomaly_eligible: true })

  test.each([
    ['below floor', 5, 30, { low_sample: true, anomaly_static: false }],
    ['above floor', 200, 30, { low_sample: false, anomaly_static: true }],
    ['at boundary (requests === floor)', 30, 30, { low_sample: false, anomaly_static: true }],
    ['no minimum (floor = 0)', 1, 0, { low_sample: false, anomaly_static: true }],
  ])('classifies eligible route %s', (_label, reqs, floor, expected) => {
    expect(adjustShieldingRows([eligible(reqs)], floor)[0]).toMatchObject(expected)
  })

  test('raising the floor reclassifies a previously-flagged route as low_sample, and default floor works', () => {
    expect(adjustShieldingRows([eligible(40)], 30)[0].anomaly_static).toBe(true)
    expect(adjustShieldingRows([eligible(40)], 100)[0]).toMatchObject({ anomaly_static: false, low_sample: true })
    const rows = adjustShieldingRows([eligible(SHIELDING_MIN_REQUESTS_DEFAULT - 1), eligible(SHIELDING_MIN_REQUESTS_DEFAULT)], SHIELDING_MIN_REQUESTS_DEFAULT)
    expect(rows[0]).toMatchObject({ low_sample: true, anomaly_static: false })
    expect(rows[1]).toMatchObject({ low_sample: false, anomaly_static: true })
  })

  test('never flags latency-ineligible routes, preserves passthrough fields, is null-safe, and includes default in options', () => {
    const notEligible = { ...eligible(500), anomaly_eligible: false }
    for (const floor of [0, 10, 30, 100]) expect(adjustShieldingRows([notEligible], floor)[0].anomaly_static).toBe(false)
    expect(adjustShieldingRows([{ edge_pop: 'CDG', shield_pop: 'FRA', p50_ms: 12.3, anomaly_eligible: false }], 30)[0]).toMatchObject({
      edge_pop: 'CDG', p50_ms: 12.3, low_sample: true,
    })
    for (const input of [null, undefined, []] as const) expect(adjustShieldingRows(input, 30)).toEqual([])
    expect(SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => o.value)).toContain(String(SHIELDING_MIN_REQUESTS_DEFAULT))
  })
})

const switcherQueryClient = createTestQueryClient()
const renderSwitcher = () => render(<ServiceSwitcher />, { wrapper: makeQueryWrapper(switcherQueryClient) })
const PREFS_KEY = 'fla-service-switcher-prefs'

describe('ServiceSwitcher MRU + pinning', () => {
  beforeEach(() => {
    window.localStorage.clear()
    switcherQueryClient.clear()
    useServiceStore.setState({
      services: [
        { id: 'svc-alpha', name: 'Alpha Service' }, { id: 'svc-beta', name: 'Beta Service' },
        { id: 'svc-gamma', name: 'Gamma Service' }, { id: 'svc-delta', name: 'Delta Service' },
      ],
      activeServiceId: 'svc-alpha',
    })
  })

  it('opens to show every service when no prefs exist and writes active service to MRU', async () => {
    const user = userEvent.setup()
    renderSwitcher()
    expect(JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}').mru[0]).toBe('svc-alpha')
    await user.click(screen.getByRole('combobox'))
    for (const svc of ['alpha', 'beta', 'gamma', 'delta']) {
      expect(screen.getByRole('button', { name: new RegExp(`pin ${svc} service to top`, 'i') })).toBeInTheDocument()
    }
  })

  it('clicking the pin button toggles persisted prefs without changing the active service', async () => {
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('button', { name: /pin beta service to top/i }))
    expect(JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}').pinned).toEqual(['svc-beta'])
    expect(useServiceStore.getState().activeServiceId).toBe('svc-alpha')
    expect(screen.getByRole('button', { name: /unpin beta service/i })).toBeInTheDocument()
  })

  it('hydrates Pinned and Recent buckets from localStorage on mount', async () => {
    window.localStorage.setItem(PREFS_KEY, JSON.stringify({ pinned: ['svc-gamma'], mru: ['svc-beta', 'svc-delta'] }))
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))
    expect(screen.getByRole('button', { name: /unpin gamma service/i })).toBeInTheDocument()
    for (const svc of ['beta', 'delta', 'alpha']) {
      expect(screen.getByRole('button', { name: new RegExp(`pin ${svc} service to top`, 'i') })).toBeInTheDocument()
    }
    expect(JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}').mru).toEqual(['svc-alpha', 'svc-beta', 'svc-delta'])
  })
})

describe('TimezoneSwitcher', () => {
  beforeEach(() => {
    window.localStorage.clear()
    useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('renders "System Time" first, then a separator, then the common zones in order, and formats trigger', async () => {
    useTimezoneStore.setState({ mode: 'system', timezone: 'America/Chicago' })
    render(<TimezoneSwitcher />)
    const content = await waitFor(() => screen.getByTestId('mock-select-content'))
    const entries = Array.from(content.querySelectorAll('[data-select-item-value], [data-testid="mock-select-separator"]')).map(
      (el) => el.getAttribute('data-select-item-value') ?? 'SEPARATOR',
    )
    expect(entries).toEqual([
      '__system__', 'SEPARATOR', 'UTC', 'America/New_York', 'America/Chicago',
      'America/Denver', 'America/Los_Angeles', 'Europe/London', 'Europe/Berlin', 'Asia/Tokyo', 'Australia/Sydney',
    ])
    const abbr = getTimezoneAbbr(new Date(), 'America/Chicago')
    await waitFor(() => expect(screen.getByTestId('mock-select-value')).toHaveTextContent(`System Time (${abbr})`))
  })

  it('switches between manual concrete zones and live browser System Time, hiding manual fallback in system mode', async () => {
    const user = userEvent.setup()
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Asia/Kolkata' })
    const { rerender } = render(<TimezoneSwitcher />)
    await waitFor(() => expect(screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata')).toBe(true))
    await user.click(screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'Asia/Tokyo')!)
    expect(useTimezoneStore.getState()).toMatchObject({ mode: 'manual', timezone: 'Asia/Tokyo' })
    stubSystemTimeZone('Australia/Sydney')
    await user.click(screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === '__system__')!)
    expect(useTimezoneStore.getState()).toMatchObject({ mode: 'system', timezone: 'Australia/Sydney' })
    act(() => { useTimezoneStore.setState({ mode: 'system', timezone: 'Asia/Kolkata' }) })
    rerender(<TimezoneSwitcher />)
    expect(screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata')).toBe(false)
  })
})

const oldTimestamp = '2026-09-06T17:19:00Z', requestTimestamp = '2026-09-07T19:31:22Z', syncTimestamp = '2026-09-08T02:33:23Z'
const mountBadge = () => render(<SyncStatusBadge />, { wrapper: makeQueryWrapper(createTestQueryClient()) })

describe('request header timestamp', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service' })
    mocks.formatTimeAgo.mockImplementation((ts: string) => ts)
    mocks.useNowMs.mockReturnValue(Date.parse('2026-09-08T18:00:00Z'))
    mocks.status.mockReturnValue({ data: undefined })
    mocks.analyst.mockReturnValue(false)
    mocks.lastSync.mockReturnValue({ data: { started_at: syncTimestamp, status: 'success' } })
    mocks.bootstrap.mockReturnValue({
      data: { services: [{ service_id: 'test-service', rum_enabled: false }], header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: oldTimestamp } } },
    })
  })

  it.each([[oldTimestamp, requestTimestamp], [requestTimestamp, oldTimestamp]])('uses the request extent rather than flat summary %s', async (flat, canonical) => {
    mocks.status.mockReturnValue({ data: { latest_log_at: flat, request: { latest_log_at: canonical } } })
    mountBadge()
    await waitFor(() => expect(screen.getByText(canonical)).toBeInTheDocument())
    expect(screen.queryByText(flat)).not.toBeInTheDocument()
    expect(screen.getByText(syncTimestamp)).toBeInTheDocument()
  })

  it('uses the analyst request extent from bootstrap and omits stale-log dot', async () => {
    mocks.analyst.mockReturnValue(true)
    mocks.bootstrap.mockReturnValue({ data: { header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: requestTimestamp } } } })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
    expect(screen.queryByRole('img', { name: /Latest log is/ })).not.toBeInTheDocument()
  })

  it('accepts an older canonical extent from a replacement snapshot', async () => {
    mocks.status.mockReturnValue({ data: { request: { latest_log_at: requestTimestamp } } })
    const { rerender } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    mocks.status.mockReturnValue({ data: { latest_log_at: requestTimestamp, request: { latest_log_at: oldTimestamp } } })
    rerender(<SyncStatusBadge />)
    await waitFor(() => expect(screen.getByText(oldTimestamp)).toBeInTheDocument())
    expect(screen.queryByText(requestTimestamp)).not.toBeInTheDocument()
  })

  it.each([null, { latest_log_at: null }])('honors a cleared request metric %s after an update', async (request) => {
    mocks.status.mockReturnValue({ data: { request: { latest_log_at: requestTimestamp } } })
    const { rerender } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    mocks.status.mockReturnValue({ data: { latest_log_at: oldTimestamp, request } })
    rerender(<SyncStatusBadge />)
    await waitFor(() => expect(screen.getByText('Never')).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
    expect(screen.queryByText(requestTimestamp)).not.toBeInTheDocument()
  })

  it('handles cleared flat extent, flat-only payloads, separate RUM timestamps, and request total rows', async () => {
    mocks.status.mockReturnValue({ data: { latest_log_at: null } })
    const { unmount: u1 } = mountBadge()
    await waitFor(() => expect(screen.getByText('Never')).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument(); u1()
    mocks.status.mockReturnValue({ data: { latest_log_at: requestTimestamp } })
    const { unmount: u2 } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument()); u2()
    const rumTimestamp = '2026-09-08T17:00:00Z'
    mocks.bootstrap.mockReturnValue({ data: { services: [{ service_id: 'test-service', rum_enabled: true }] } })
    mocks.status.mockReturnValue({
      data: { local_rows: 2700, latest_log_at: rumTimestamp, request: { latest_log_at: requestTimestamp, total_rows: 2700 }, rum: { latest_log_at: rumTimestamp, total_rows: 4430 } },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    for (const txt of [rumTimestamp, 'total: 2,700', 'total: 4,430']) expect(screen.getByText(txt)).toBeInTheDocument()
  })
})

const API_BASE = 'http://127.0.0.1:8000'
const makeDebugClient = () => createTestQueryClient({ queries: { gcTime: Infinity, staleTime: Infinity } })
const renderDebugWith = (qc: QueryClient) => renderWithQuery(<DebugPanel />, { queryClient: qc })

describe('DebugPanel & DiagnosticsPanel', () => {
  beforeEach(() => {
    act(() => { useDebugStore.setState({ enabled: false, apiCallsEnabled: false }) })
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/`
    server.use(http.get(`${API_BASE}/api/debug/recent-sqlite`, () => HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 })))
  })

  test('cookie-heal shim writes missing cookie + invalidates cache, and skips when cookie present', async () => {
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=; path=/; max-age=0`
    act(() => { useDebugStore.setState({ enabled: true, apiCallsEnabled: false }) })
    const qc1 = makeDebugClient(), spy1 = vi.spyOn(qc1, 'invalidateQueries')
    const { unmount } = renderDebugWith(qc1)
    await waitFor(() => expect(document.cookie).toContain(`${DEBUG_RESPONSES_COOKIE}=1`))
    expect(spy1).toHaveBeenCalled(); unmount()
    const qc2 = makeDebugClient(), spy2 = vi.spyOn(qc2, 'invalidateQueries')
    renderDebugWith(qc2)
    await new Promise((r) => setTimeout(r, 20))
    expect(spy2).not.toHaveBeenCalled()
  })

  test('renders nothing while both debug toggles are off, and polls SQLite ring-buffer only in Process-wide scope', async () => {
    const { container, unmount } = renderDebugWith(makeDebugClient())
    expect(container).toBeEmptyDOMElement(); unmount()
    let calls = 0
    server.use(http.get(`${API_BASE}/api/debug/recent-sqlite`, () => { calls += 1; return HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 }) }))
    act(() => { useDebugStore.setState({ enabled: true, apiCallsEnabled: false }) })
    renderDebugWith(makeDebugClient())
    const user = userEvent.setup()
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(0)
    await user.click(screen.getByRole('button', { name: /Process-wide/i }))
    await waitFor(() => expect(calls).toBeGreaterThanOrEqual(1))
    const bufferScopeCalls = calls
    await user.click(screen.getByRole('button', { name: /This page/i }))
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(bufferScopeCalls)
    act(() => { useDebugStore.setState({ enabled: false, apiCallsEnabled: false }) })
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(bufferScopeCalls)
  })

  test('page scope lists _debug_sqlite statements and expand/collapse of each section is independent', async () => {
    act(() => { useDebugStore.setState({ enabled: true, apiCallsEnabled: true }) })
    const qc = makeDebugClient()
    const { useQuery } = await import('@tanstack/react-query')
    function Seeder() {
      useQuery({
        queryKey: ['debug-fixture', 'sqlite'],
        initialData: {
          _debug_sqlite: [
            { seq: 5, ts: '2026-07-07T12:00:00.000000Z', sql: 'SELECT a FROM t', params_kind: 'none', time_ms: 3.25, rows: -1, op: 'execute' },
            { seq: 6, ts: '2026-07-07T12:00:00.100000Z', sql: 'INSERT INTO t VALUES (?)', params_kind: 'seq[1]', time_ms: 1.75, rows: 1, op: 'execute' },
          ],
        },
        queryFn: async () => null, staleTime: Infinity,
      })
      return null
    }
    renderWithQuery(<><Seeder /><DebugPanel /></>, { queryClient: qc })
    await waitFor(() => expect(screen.getByRole('button', { name: /Show 2 statements/i })).toBeInTheDocument())
    expect(screen.getByText(/5\.00ms/)).toBeInTheDocument()
    const user = userEvent.setup()
    const duckBtn = screen.getByRole('button', { name: /Show 0 queries/i })
    const callsBtn = screen.getByRole('button', { name: /Show 0 calls/i })
    await user.click(screen.getByRole('button', { name: /Show 2 statements/i }))
    expect(screen.getByText('SELECT a FROM t')).toBeInTheDocument()
    expect(screen.getByText('INSERT INTO t VALUES (?)')).toBeInTheDocument()
    expect(duckBtn).toHaveTextContent(/Show 0 queries/i)
    expect(callsBtn).toHaveTextContent(/Show 0 calls/i)
    await user.click(callsBtn)
    expect(screen.getByRole('button', { name: /Hide 0 calls/i })).toBeInTheDocument()
  })

  test('subscription picks up _debug_queries / _debug_calls, deduplicates identical notifications, and labels engine dynamically', async () => {
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: true })
      useServiceStore.setState({ activeServiceId: 'std-svc', services: [{ id: 'std-svc', name: 'Standard Service', is_high_scale: false } as never] })
    })
    const qc = makeDebugClient()
    const { useQuery } = await import('@tanstack/react-query')
    function Seeder() {
      useQuery({
        queryKey: ['debug-fixture', 'one'],
        initialData: { _debug_queries: [{ sql: 'SELECT 1', time_ms: 12.5 }], _debug_calls: [{ service: 'FOS', method: 'GET', path: '/x', time_ms: 8, status: 'OK' }] },
        queryFn: async () => null, staleTime: Infinity,
      })
      return null
    }
    renderWithQuery(<><Seeder /><DebugPanel /></>, { queryClient: qc })
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Show 1 queries/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /Show 1 calls/i })).toBeInTheDocument()
    })
    expect(screen.getByText(/12\.50ms/)).toBeInTheDocument()
    expect(screen.getByText('Data Queries (DuckDB)')).toBeInTheDocument()
    await act(async () => {
      const q = qc.getQueryCache().find({ queryKey: ['debug-fixture', 'one'] })!
      qc.getQueryCache().notify({ type: 'updated', query: q } as QueryCacheNotifyEvent)
    })
    await new Promise((r) => setTimeout(r, 10))
    expect(screen.getByRole('button', { name: /Show 1 queries/i })).toBeInTheDocument()
    await act(async () => {
      qc.setQueryData(['debug-fixture', 'one'], {
        _debug_queries: [{ sql: 'SELECT 1', time_ms: 99 }],
        _debug_calls: [{ service: 'FOS', method: 'GET', path: '/x', time_ms: 8, status: 'OK' }],
      })
      useServiceStore.setState({ activeServiceId: 'hs-svc', services: [{ id: 'hs-svc', name: 'High-Scale Service', is_high_scale: true } as never] })
    })
    await waitFor(() => {
      expect(screen.getByText(/99\.00ms/)).toBeInTheDocument()
      expect(screen.getByText('Data Queries (ClickHouse)')).toBeInTheDocument()
    })
    act(() => { useDebugStore.setState({ enabled: false, apiCallsEnabled: false }) })
    await waitFor(() => expect(screen.queryByRole('button', { name: /queries/i })).not.toBeInTheDocument())
  })

  test('DiagnosticsPanel renders initial settings and triggers mutations on query-debug and api-calls changes', async () => {
    useServiceStore.setState({ activeServiceId: 'service-1' })
    const user = userEvent.setup()
    const qc = createTestQueryClient(), invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    renderWithQuery(<DiagnosticsPanel />, { queryClient: qc })
    await screen.findByText('Query debugging panel')
    const selectContainers = screen.getAllByTestId('mock-select')
    expect(selectContainers).toHaveLength(2)
    expect(selectContainers[0].getAttribute('data-select-value')).toBe('disabled')
    expect(selectContainers[1].getAttribute('data-select-value')).toBe('disabled')
    await user.click(screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'admins')!)
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalled())
    invalidateSpy.mockClear()
    const apiBothItems = screen.getAllByRole('button').filter((b) => b.getAttribute('data-select-item-value') === 'both')
    expect(apiBothItems).toHaveLength(2)
    await user.click(apiBothItems[1])
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalled())
  })
})

describe('CronLiveLog, LogSyncSection & LogsClient', () => {
  let errorSpy: ReturnType<typeof spyOnConsoleError>
  beforeEach(() => { resetSSE(); mocks.showToast.mockClear(); errorSpy = spyOnConsoleError() })
  afterEach(() => { errorSpy.mockRestore() })

  it('CronLiveLog renders each line type, accumulates lines & SESSION INITIATED header in terminalMode, and truncates in singleLine mode', () => {
    sseState.status = 'streaming'
    sseState.lines = [
      { _id: 1, type: 'status', message: 'Starting ingest' }, { _id: 2, type: 'file_done', file_name: 'a.gz' },
      { _id: 3, type: 'error', message: 'Boom' }, { _id: 4, type: 'done', message: 'All clean' },
    ]
    const { unmount: u1 } = render(<CronLiveLog runId={1} terminalMode startedAt="2026-06-16T00:00:00Z" />)
    for (const pat of ['Starting ingest', /Processed a\.gz/, 'Boom', 'All clean', /SESSION INITIATED AT: FULL\(2026-06-16T00:00:00Z\) TZ/]) {
      expect(screen.getByText(pat)).toBeInTheDocument()
    }
    u1()
    sseState.lines = [{ _id: 1, type: 'status', message: 'old-one' }, { _id: 2, type: 'status', message: 'x'.repeat(120) }]
    render(<CronLiveLog runId={4} singleLine />)
    expect(screen.queryByText('old-one')).toBeNull()
    expect(screen.getByText('x'.repeat(80) + '...')).toBeInTheDocument()
  })

  it('CronLiveLog fires onDone once on streaming → done/error without double-firing', () => {
    const onDone = vi.fn()
    sseState.status = 'streaming'
    sseState.lines = [{ _id: 1, type: 'status', message: 'go' }]
    const { rerender } = render(<CronLiveLog runId={1} onDone={onDone} />)
    expect(onDone).not.toHaveBeenCalled()
    sseState.status = 'done'
    sseState.lines = [...sseState.lines, { _id: 2, type: 'done', message: 'ok' }]
    rerender(<CronLiveLog runId={1} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)
    sseState.status = 'error'
    rerender(<CronLiveLog runId={1} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)
  })

  test('LogSyncSection polling mode selector, RUM disabled banner, and Enable RUM Beacon Sync switch', async () => {
    mocks.useRealSelect = true
    const user = userEvent.setup()
    const setPollingMode = vi.fn(), setRumSyncEnabled = vi.fn()
    const baseProps = {
      syncEnabled: true, setSyncEnabled: vi.fn(), pollingMode: 'regular' as const, setPollingMode,
      deleteAfter: false, setDeleteAfter: vi.fn(), dataRetention: '30', setDataRetention: vi.fn(),
      cacheRetention: '90', setCacheRetention: vi.fn(), rollupRetention: '12', setRollupRetention: vi.fn(),
      commitInterval: '5', setCommitInterval: vi.fn(), syncLogEnabled: true, setSyncLogEnabled: vi.fn(),
      syncRetention: '7', setSyncRetention: vi.fn(), syncFreqLabel: 'every 2 minutes', isAnalyst: false,
      syncIntervalNum: 120, adminSyncSeconds: 120, rumRetention: '30', setRumRetention: vi.fn(),
      rumEnabled: false, rumLogPeriod: '300', setRumLogPeriod: vi.fn(), rumDeleteAfter: false, setRumDeleteAfter: vi.fn(),
    }
    const { rerender } = render(<LogSyncSection {...baseProps} rumEnabled={false} rumSyncEnabled={false} setRumSyncEnabled={setRumSyncEnabled} serviceId="test-service-123" />)
    expect(screen.getByText(/Adaptive makes up to two short follow-up discovery passes/)).toBeInTheDocument()
    await user.click(screen.getByRole('combobox', { name: 'Discovery Polling Mode' }))
    await user.click(await screen.findByRole('option', { name: 'Adaptive' }))
    expect(setPollingMode).toHaveBeenCalledWith('adaptive')
    expect(screen.getByText(/Real User Monitoring is not currently enabled on Fastly edge/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Configure RUM' })).toHaveAttribute('href', '/admin/rum?service=test-service-123')
    rerender(<LogSyncSection {...baseProps} rumEnabled={true} rumSyncEnabled={false} setRumSyncEnabled={setRumSyncEnabled} serviceId="test-service-123" />)
    const rumSwitch = screen.getByRole('switch', { name: 'Enable RUM Beacon Sync' })
    expect(rumSwitch).toHaveAttribute('aria-checked', 'false')
    await user.click(rumSwitch)
    expect(setRumSyncEnabled).toHaveBeenCalledWith(true, expect.anything())
  })

  test('LogsClient mounts in cron mode, QuickActionsBar handles failures, and partial_hour_merge is registered', async () => {
    useServiceStore.setState({ activeServiceId: 'test-svc' })
    mocks.status.mockReturnValue({ data: { idle: true } })
    const { unmount } = renderWithQuery(<LogsClient initialTab="cron" />)
    expect(screen.getByText('Data Management')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled(); unmount()
    expect(CRON_DISPLAY_NAMES.partial_hour_merge).toBeTruthy()
    expect(CRON_EXPLANATIONS.partial_hour_merge).toBeTruthy()
    expect(CRON_GROUPS.find((g) => g.tasks.includes('rollup_hour_heal'))?.tasks).toContain('partial_hour_merge')
    const postSpy = vi.spyOn(client, 'POST'), start = vi.fn()
    renderWithQuery(
      <QuickActionsBar
        status={{}} activeServiceId="svc" recentCrons={{}} cronLogs={{}}
        setSseTitle={vi.fn()} setSseDescription={vi.fn()} setIsSSEModalOpen={vi.fn()} setIsSyncModalOpen={vi.fn()}
        reset={vi.fn()} start={start} setDisplayedJobs={vi.fn()} setSelectedConsoleJobId={vi.fn()} setConsoleOpen={vi.fn()}
      />,
    )
    postSpy.mockRejectedValueOnce(new Error('ingest boom'))
    await userEvent.click(screen.getByRole('button', { name: /import logs/i }))
    await waitFor(() => expect(mocks.showToast).toHaveBeenCalledWith('ingest boom'))
    expect(start).not.toHaveBeenCalled()
    postSpy.mockResolvedValueOnce({ data: {} } as never)
    await userEvent.click(screen.getByRole('button', { name: /import logs/i }))
    await waitFor(() => expect(mocks.showToast).toHaveBeenCalledWith('No run id returned'))
    expect(start).not.toHaveBeenCalled()
    postSpy.mockRestore()
  })
})

interface Row { id: number; name: string; status: number }
const COLUMNS: ColumnDef<Row>[] = [{ accessorKey: 'id', header: 'ID', id: 'id' }, { accessorKey: 'name', header: 'Name', id: 'name' }, { accessorKey: 'status', header: 'Status', id: 'status' }]
const DATA: Row[] = [{ id: 1, name: 'alpha', status: 200 }, { id: 2, name: 'beta', status: 404 }, { id: 3, name: 'gamma', status: 500 }]

describe('DataTable & DataTableReadonly', () => {
  it('renders headers, rows, empty state, loading skeletons, and ignores stale initialColumnOrder IDs', () => {
    const { unmount: u1 } = render(<DataTable columns={COLUMNS} data={DATA} hideToolbar initialColumnOrder={['name', 'id', 'status', 'phantom_column']} />)
    for (const h of ['ID', 'Name', 'Status', 'alpha', 'beta', 'gamma']) expect(screen.getByText(h)).toBeTruthy()
    u1()
    const { unmount: u2 } = render(<DataTable columns={COLUMNS} data={[]} hideToolbar emptyMessage="No results found." />)
    expect(screen.getByText('No results found.')).toBeTruthy(); u2()
    render(<DataTable columns={COLUMNS} data={[]} hideToolbar isLoading />)
    expect(screen.queryByText('No results.')).toBeNull()
  })

  it('survives column removal without resize-handle throws or phantom headers', () => {
    const { container, rerender } = render(<DataTable columns={COLUMNS} data={DATA} hideToolbar />)
    expect(screen.getByText('Status')).toBeTruthy()
    expect(container.querySelectorAll('[class*="cursor-col-resize"]').length).toBeGreaterThan(0)
    rerender(<DataTable columns={COLUMNS.slice(0, 2)} data={DATA} hideToolbar />)
    expect(screen.queryByText('Status')).toBeNull()
    expect(screen.getByText('ID')).toBeTruthy()
    expect(screen.getByText('Name')).toBeTruthy()
    for (const h of Array.from(container.querySelectorAll('[class*="cursor-col-resize"]'))) {
      expect(() => fireEvent.mouseDown(h)).not.toThrow()
      expect(() => fireEvent.touchStart(h)).not.toThrow()
    }
  })

  it('has no axe-detectable a11y violations', async () => {
    const { container } = render(<DataTable columns={COLUMNS} data={DATA} hideToolbar />)
    expect(await axe(container)).toHaveNoViolations()
  })

  it('hands every row to virtualizer when showPagination={false} and pages at 50 by default', () => {
    const MANY: Row[] = Array.from({ length: 120 }, (_, i) => ({ id: i, name: `row-${i}`, status: 200 }))
    for (const Table of [DataTable, DataTableReadonly]) {
      const { unmount } = render(<Table columns={COLUMNS} data={MANY} hideToolbar showPagination={false} />)
      expect(screen.getByText('row-119')).toBeTruthy(); unmount()
    }
    render(<DataTableReadonly columns={COLUMNS} data={MANY} hideToolbar />)
    expect(screen.queryByText('row-119')).toBeNull()
    expect(screen.getByText('row-49')).toBeTruthy()
  })
})

describe('DateTimeCell & makeLatencyColumns', () => {
  it('DateTimeCell renders default em-dash, custom emptyFallback, and live TimeAgo', () => {
    const { container, unmount: u1 } = render(<DateTimeCell iso={null} />)
    expect(container.querySelector('span.text-muted-foreground\\/40')?.textContent).toBe('—'); u1()
    const { unmount: u2 } = render(<DateTimeCell iso={undefined} emptyFallback={<span>N/A</span>} />)
    expect(screen.getByText('N/A')).toBeInTheDocument(); u2()
    render(<DateTimeCell iso="2026-06-15T12:00:00Z" />)
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
  })

  it('makeLatencyColumns formats labelField cell and numeric cells', () => {
    type UrlRow = LatencyRow & { url?: string | null }
    type CellStub = Pick<CellContext<UrlRow, unknown>, 'getValue'> & { row: Pick<CellContext<UrlRow, unknown>['row'], 'original'> }
    const ctx = (value: unknown, original: UrlRow): CellStub => ({ getValue: <T,>() => value as T, row: { original } })
    const cellRenderer = (col: ColumnDef<UrlRow>): ((info: CellStub) => ReactNode) => {
      if (typeof col.cell !== 'function') throw new Error('expected cell function')
      const fn = col.cell
      return (info) => fn(info as CellContext<UrlRow, unknown>)
    }
    const cols = makeLatencyColumns<UrlRow>('url', 'URL', 'url')
    const labelCell = cellRenderer(cols[0])
    const { container: c1, unmount } = render(labelCell(ctx('/api/data', { url: null })))
    expect(c1.querySelector('span.font-mono')?.textContent).toBe('/api/data')
    expect(c1.querySelector('button')).toBeNull(); unmount()
    const { container: c2, getByText } = render(labelCell(ctx('/api/data', { url: '/api/data' })))
    expect(getByText('/api/data')).toBeInTheDocument()
    expect(c2.querySelector('button')).not.toBeNull()
    const requestsCell = cellRenderer(cols[1])
    expect(requestsCell(ctx(null, {}))).toBe('0')
    expect(requestsCell(ctx(1234, {}))).toBe((1234).toLocaleString())
    for (const col of cols.slice(2, 6)) {
      const c = cellRenderer(col)
      expect(c(ctx(null, {}))).toBe('0.00')
      expect(c(ctx(12.3456, {}))).toBe('12.35')
    }
  })
})

type SchemaResponse = components['schemas']['SchemaResponse']

describe('SchemaTab & BotSourcesPanel', () => {
  beforeEach(() => { useServiceStore.setState({ activeServiceId: 'svc-tables', isInitialized: true }) })

  it('SchemaTab renders every column past 50 rows with formatted stats and em-dash for bare columns', () => {
    const schema = Array.from({ length: 80 }, (_, i) => ({ name: `col_${i}`, type: 'VARCHAR', null_percentage: i === 0 ? 25 : 0, approx_unique: 1234, min: 'a', max: 'z' }))
    const { unmount } = render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    for (const name of ['Column Name', 'DuckDB Type', 'Populated %', 'Approx Unique', 'Min Value', 'Max Value']) expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    expect(screen.getByText('col_79')).toBeTruthy()
    expect(screen.getByText('75.0%')).toBeTruthy(); unmount()
    render(<SchemaTab schemaData={{ schema: [{ name: 'bare', type: 'INTEGER' }] } as unknown as SchemaResponse} isLoadingSchema={false} />)
    const row = screen.getByText('bare').closest('tr') as HTMLElement
    expect(within(row).getAllByText('—').length).toBe(4)
  })

  it('BotSourcesPanel lists sources, refreshes chosen source, triggers rDNS enrich, and surfaces rDNS errors', async () => {
    const refreshed: string[] = []
    let enrichCalls = 0, listCalls = 0
    const base = getApiBase()
    server.use(
      http.get(`${base}/api/admin/bot-sources`, () => {
        listCalls += 1
        return HttpResponse.json({
          sources: [{ id: 'src-a', name: 'Alpha Bots', url: 'https://example.test/a', entry_count: 1500, last_updated: null }],
          rdns: { total: 10, pending: 2, last_enrichment_at: null },
        })
      }),
      http.post(`${base}/api/admin/bot-sources/:id/refresh`, ({ params }) => { refreshed.push(String(params.id)); return HttpResponse.json({}) }),
      http.post(`${base}/api/admin/rdns/enrich`, () => { enrichCalls += 1; return HttpResponse.json({ ok: true, summary: {} }) }),
    )
    const user = userEvent.setup()
    const { unmount } = renderWithQuery(<BotSourcesPanel />)
    expect(await screen.findByText('Alpha Bots')).toBeTruthy()
    expect(screen.getByText('not cached')).toBeTruthy()
    expect(screen.getByText('1,500')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /Refresh/ }))
    await waitFor(() => expect(refreshed).toEqual(['src-a']))
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    await waitFor(() => expect(enrichCalls).toBe(1))
    await waitFor(() => expect(listCalls).toBeGreaterThanOrEqual(2))
    unmount()
    server.use(http.post(`${base}/api/admin/rdns/enrich`, () => HttpResponse.json({ detail: { error: 'rdns_failed', message: 'enrichment exploded' } }, { status: 500 })))
    renderWithQuery(<BotSourcesPanel />)
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})

describe('FilterBar, ActiveFiltersBanner, AddFilterDialogContent & FilterValueCell', () => {
  beforeEach(() => {
    mocks.useRealSelect = true
    mocks.mockReactQueryForFilterBar = true
    mocks.logExtentsData = undefined
    mocks.maskIps.mockReturnValue(false)
    mocks.fieldValues = { values: [] }
    vi.mocked(usePathname).mockReturnValue('/dashboard')
    act(() => {
      useFilterStore.getState().resetAll()
      useFilterStore.setState({
        filters: [],
        edgeOnly: false,
        compareMode: false,
        isAutoRange: true,
      })
      useServiceStore.setState({ activeServiceId: 'test-svc' })
    })
  })

  describe('FilterBar — filter chip rendering', () => {
    it('preserves date drafts until the committed range changes', () => {
      act(() => {
        useFilterStore.getState().setRange('2026-01-02T01:00:00Z', '2026-01-02T02:00:00Z')
      })
      render(<FilterBar />)
      const start = screen.getByLabelText<HTMLInputElement>('Range start')
      const initial = start.value
      fireEvent.change(start, { target: { value: '2026-01-03T03:00' } })
      act(() => {
        useFilterStore.getState().toggleEdgeOnly()
      })
      expect(start).toHaveValue('2026-01-03T03:00')
      act(() => {
        useFilterStore.getState().setRange('2026-01-04T01:00:00Z', '2026-01-04T02:00:00Z')
      })
      expect(start).not.toHaveValue('2026-01-03T03:00')
      expect(start.value).not.toBe(initial)
    })

    it('keeps switches synchronized with external store resets', async () => {
      const user = userEvent.setup()
      render(<FilterBar />)
      const edge = screen.getByRole('switch', { name: /edge only/i })
      const compare = screen.getByRole('switch', { name: /compare/i })
      await user.click(edge)
      await user.click(compare)
      expect(edge).toBeChecked()
      expect(compare).toBeChecked()
      act(() => {
        useFilterStore.getState().resetAll()
      })
      expect(edge).not.toBeChecked()
      expect(compare).not.toBeChecked()
    })

    it('renders no chips when filters is empty', () => {
      render(<FilterBar />)
      expect(screen.getByTestId('add-filter-stub')).toBeTruthy()
      expect(screen.queryByRole('button', { name: /toggle include\/exclude/i })).toBeNull()
    })

    it('renders one chip per filter with column:value', () => {
      act(() => {
        useFilterStore.setState({
          filters: [
            { id: 'f1', column: 'country', value: 'US', mode: 'include' },
            { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
          ],
        })
      })
      render(<FilterBar />)

      expect(screen.getByText('country:')).toBeTruthy()
      expect(screen.getByText('status:')).toBeTruthy()
      expect(screen.getByText('US')).toBeTruthy()
      expect(screen.getByText('500')).toBeTruthy()
    })

    it('include filter shows "+" and exclude filter shows "-"', () => {
      act(() => {
        useFilterStore.setState({
          filters: [
            { id: 'inc', column: 'country', value: 'US', mode: 'include' },
            { id: 'exc', column: 'country', value: 'CN', mode: 'exclude' },
          ],
        })
      })
      render(<FilterBar />)
      const toggles = screen.getAllByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
      const glyphs = toggles.map((b) => b.textContent?.trim())
      expect(glyphs).toContain('+')
      expect(glyphs).toContain('-')
    })

    it('_ngwaf_bot_name filter renders "Fastly Verified Bot:" prefix and _bot_name renders "Bot:"', () => {
      act(() => {
        useFilterStore.setState({
          filters: [
            { id: 'b1', column: '_ngwaf_bot_name', value: 'GoogleBot', mode: 'include' },
            { id: 'b2', column: '_bot_name', value: 'googlebot', mode: 'include' },
          ],
        })
      })
      render(<FilterBar />)
      expect(screen.getByText('Fastly Verified Bot:')).toBeTruthy()
      expect(screen.getByText('GoogleBot')).toBeTruthy()
      expect(screen.queryByText('_ngwaf_bot_name:')).toBeNull()
      expect(screen.getByText('Bot:')).toBeTruthy()
      expect(screen.queryByText('_bot_name:')).toBeNull()
    })

    it('clicking the X on a chip removes that filter from the store', async () => {
      const user = userEvent.setup()
      act(() => {
        useFilterStore.setState({
          filters: [
            { id: 'f1', column: 'country', value: 'US', mode: 'include' },
            { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
          ],
        })
      })
      render(<FilterBar />)

      expect(useFilterStore.getState().filters).toHaveLength(2)
      const removeUS = screen.getByRole('button', { name: /remove filter country: US/i })
      await user.click(removeUS)
      expect(useFilterStore.getState().filters).toHaveLength(1)
      expect(useFilterStore.getState().filters[0].id).toBe('f2')
    })

    it('clicking the +/- toggle flips the filter mode in the store', async () => {
      const user = userEvent.setup()
      act(() => {
        useFilterStore.setState({
          filters: [{ id: 'f1', column: 'country', value: 'US', mode: 'include' }],
        })
      })
      render(<FilterBar />)

      const toggle = screen.getByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
      await user.click(toggle)
      expect(useFilterStore.getState().filters[0].mode).toBe('exclude')

      const toggleAfter = screen.getByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
      await user.click(toggleAfter)
      expect(useFilterStore.getState().filters[0].mode).toBe('include')
    })

    it('has no axe-detectable a11y violations when chips are present', async () => {
      act(() => {
        useFilterStore.setState({
          filters: [
            { id: 'f1', column: 'country', value: 'US', mode: 'include' },
            { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
          ],
        })
      })
      const { container } = render(<FilterBar />)
      const results = await axe(container, {
        rules: { 'color-contrast': { enabled: false } },
      })
      expect(results).toHaveNoViolations()
    })

    it('renders many chips without crashing (no key collisions)', () => {
      act(() => {
        useFilterStore.setState({
          filters: Array.from({ length: 20 }, (_, i) => ({
            id: `filter-${i}`,
            column: 'country',
            value: ['US', 'GB', 'DE'][i % 3],
            mode: 'include' as const,
          })),
        })
      })
      render(<FilterBar />)
      expect(screen.getAllByText('country:')).toHaveLength(20)
    })
  })

  describe('FilterBar — pill-flash predicate (time-bound allowlist)', () => {
    beforeEach(() => {
      mocks.capturedPredicate = null
    })

    it('returns true only for queries the date range affects', () => {
      render(<FilterBar />)
      expect(mocks.capturedPredicate).not.toBeNull()
      const p = mocks.capturedPredicate!

      expect(p({ queryKey: ['dashboard', 'aggregates', 'svc'] })).toBe(true)
      expect(p({ queryKey: ['sessions', 'list', 'svc'] })).toBe(true)
      expect(p({ queryKey: ['usage', 'bandwidth', 'svc'] })).toBe(true)
      expect(p({ queryKey: ['insights', 'svc', 24, 24] })).toBe(true)
      expect(p({ queryKey: ['usage-log', 'head', 'svc'] })).toBe(true)
    })

    it('returns false for background polls and empty/malformed query keys', () => {
      render(<FilterBar />)
      const p = mocks.capturedPredicate!

      expect(p({ queryKey: ['log-extents', 'svc'] })).toBe(false)
      expect(p({ queryKey: ['bootstrap'] })).toBe(false)
      expect(p({ queryKey: ['sync-status', 'svc'] })).toBe(false)
      expect(p({ queryKey: ['last-sync', 'svc'] })).toBe(false)
      expect(p({ queryKey: ['admin', 'health-snapshot'] })).toBe(false)
      expect(p({ queryKey: ['admin', 'overview', 'queries-summary'] })).toBe(false)
      expect(p({ queryKey: ['admin', 'storage', 'svc'] })).toBe(false)
      expect(p({ queryKey: ['admin', 'share', 'live'] })).toBe(false)
      expect(p({ queryKey: ['system-jobs'] })).toBe(false)
      expect(p({ queryKey: ['alerts', 'svc'] })).toBe(false)
      expect(p({ queryKey: undefined })).toBe(false)
      expect(p({ queryKey: [] })).toBe(false)
      expect(p({ queryKey: [123] })).toBe(false)
    })
  })

  describe('ActiveFiltersBanner', () => {
    it('renders nothing when no filters and edgeOnly is off', () => {
      const { container } = render(<ActiveFiltersBanner />)
      expect(container.firstChild).toBeNull()
    })

    it('renders a pill for each active filter and has region role', () => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
      useFilterStore.getState().addFilter('status', '500', 'exclude')

      render(<ActiveFiltersBanner />)
      expect(screen.getByText('country:')).toBeInTheDocument()
      expect(screen.getByText('US')).toBeInTheDocument()
      expect(screen.getByText('status:')).toBeInTheDocument()
      expect(screen.getByText('500')).toBeInTheDocument()
      expect(
        screen.getByRole('region', { name: /active filters from other pages/i }),
      ).toBeInTheDocument()
    })

    it('renders the "Edge only" badge and "Clear all" wipes filters + edgeOnly', () => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
      useFilterStore.getState().toggleEdgeOnly()

      render(<ActiveFiltersBanner />)
      expect(screen.getByText('Edge only')).toBeInTheDocument()
      fireEvent.click(screen.getByRole('button', { name: /clear all/i }))

      expect(useFilterStore.getState().filters).toHaveLength(0)
      expect(useFilterStore.getState().edgeOnly).toBe(false)
    })
  })

  function renderDialogContent(onClose: () => void = () => {}) {
    return render(
      <Dialog open>
        <AddFilterDialogContent onClose={onClose} />
      </Dialog>,
    )
  }

  describe('AddFilterDialogContent', () => {
    it('keeps Apply Filter disabled until a value is typed', async () => {
      const user = userEvent.setup()
      renderDialogContent()
      const apply = screen.getByRole('button', { name: /Apply Filter/ }) as HTMLButtonElement
      expect(apply.disabled).toBe(true)
      await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
      await waitFor(() => expect(apply.disabled).toBe(false))
    })

    it('adds an include filter for the default field and closes', async () => {
      const user = userEvent.setup()
      const onClose = vi.fn()
      renderDialogContent(onClose)
      await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
      await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
      await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
      const [filter] = useFilterStore.getState().filters
      expect(filter).toMatchObject({ column: 'ip', value: '1.2.3.4', mode: 'include' })
    })

    it('honours the Exclude tab and submits on Enter', async () => {
      const user = userEvent.setup()
      const onClose = vi.fn()
      renderDialogContent(onClose)
      await user.click(screen.getByRole('tab', { name: /Exclude/ }))
      await user.type(screen.getByPlaceholderText('Type a ip value...'), 'bad{Enter}')
      await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
      expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'ip', value: 'bad', mode: 'exclude' })
    })

    it('defaults a masking analyst to the url field', async () => {
      mocks.maskIps.mockReturnValue(true)
      const user = userEvent.setup()
      renderDialogContent()
      await user.type(screen.getByPlaceholderText('Type a url value...'), '/x')
      await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
      await waitFor(() => expect(useFilterStore.getState().filters.length).toBe(1))
      expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'url', value: '/x' })
    })

    it('fills the value from a suggestion without submitting', async () => {
      mocks.fieldValues = { values: [{ value: 'US', label: 'United States', count: 1200 }] }
      const user = userEvent.setup()
      const onClose = vi.fn()
      renderDialogContent(onClose)
      const input = screen.getByPlaceholderText('Type a ip value...') as HTMLInputElement
      await user.type(input, 'U')
      await user.click(await screen.findByRole('button', { name: /United States/ }))
      expect(input.value).toBe('US')
      expect(onClose).not.toHaveBeenCalled()
      expect(useFilterStore.getState().filters.length).toBe(0)
    })
  })

  describe('buildDashboardFilterUrl', () => {
    it('builds a single-filter URL', () => {
      expect(buildDashboardFilterUrl([{ column: 'url', value: '/api/data' }]))
        .toBe('/dashboard?filter_url=%2Fapi%2Fdata')
    })

    it('builds a multi-filter URL with ampersands', () => {
      expect(buildDashboardFilterUrl([
        { column: 'city', value: 'London' },
        { column: 'region', value: 'England' },
        { column: 'country', value: 'GB' },
      ])).toBe('/dashboard?filter_city=London&filter_region=England&filter_country=GB')
    })

    it('escapes the underscore-prefixed bot id columns', () => {
      expect(buildDashboardFilterUrl([{ column: '_wellknown_bot_id', value: 'bot-1' }]))
        .toBe('/dashboard?filter__wellknown_bot_id=bot-1')
    })
  })

  describe('FilterValueCell', () => {
    beforeEach(() => {
      useFilterStore.getState().clearFilters()
      vi.mocked(usePathname).mockReturnValue('/origin')
      mocks.maskIps.mockReturnValue(false)
    })

    afterEach(() => {
      useFilterStore.getState().clearFilters()
    })

    it('renders the display value', () => {
      render(<FilterValueCell filters={[{ column: 'url', value: '/api/data' }]} />)
      expect(screen.getByText('/api/data')).toBeInTheDocument()
    })

    it('prefers the explicit display prop over the filter value', () => {
      render(
        <FilterValueCell
          filters={[{ column: '_wellknown_bot_id', value: 'bot-1' }]}
          display="Googlebot"
        />,
      )
      expect(screen.getByText('Googlebot')).toBeInTheDocument()
      expect(screen.queryByText('bot-1')).not.toBeInTheDocument()
    })

    it('"Filter <page>" menu item calls addFilter for every entry', () => {
      const addFilterSpy = vi.spyOn(useFilterStore.getState(), 'addFilter')
      render(
        <FilterValueCell
          filters={[
            { column: 'city', value: 'London' },
            { column: 'country', value: 'GB' },
          ]}
          display="London"
        />,
      )
      fireEvent.click(screen.getByRole('button', { name: /Filter actions for London/i }))
      fireEvent.click(screen.getByText(/Filter origin page/i))
      expect(addFilterSpy).toHaveBeenCalledWith('city', 'London', 'include')
      expect(addFilterSpy).toHaveBeenCalledWith('country', 'GB', 'include')
    })

    it('"Copy value" writes the first filter value to the clipboard', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
      render(<FilterValueCell filters={[{ column: 'city', value: 'London' }]} display="London" />)
      fireEvent.click(screen.getByRole('button', { name: /Filter actions for London/i }))
      fireEvent.click(screen.getByText(/Copy value/i))
      await waitFor(() => expect(writeText).toHaveBeenCalledWith('London'))
    })

    it('hides "Open in dashboard" when already on /dashboard', () => {
      vi.mocked(usePathname).mockReturnValue('/dashboard')
      render(<FilterValueCell filters={[{ column: 'country', value: 'US' }]} />)
      fireEvent.click(screen.getByRole('button', { name: /Filter actions for US/i }))
      expect(screen.queryByText(/Open in dashboard/i)).not.toBeInTheDocument()
      expect(screen.getByText(/Filter dashboard page/i)).toBeInTheDocument()
    })

    it('opens dashboard URL in a new tab via "Open in dashboard"', () => {
      const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null)
      render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
      fireEvent.click(screen.getByRole('button', { name: /Filter actions for JFK/i }))
      fireEvent.click(screen.getByText(/Open in dashboard/i))
      expect(openSpy).toHaveBeenCalledWith(
        '/dashboard?filter_pop=JFK',
        '_blank',
        'noopener,noreferrer',
      )
      openSpy.mockRestore()
    })

    it('renders empty cell (no trigger) when filters list is empty', () => {
      const { container } = render(<FilterValueCell filters={[]} display="—" />)
      expect(screen.getByText('—')).toBeInTheDocument()
      expect(container.querySelector('button')).toBeNull()
    })

    it('cmd/ctrl-click on the cell bypasses the menu and calls addFilter directly', () => {
      const addFilterSpy = vi.spyOn(useFilterStore.getState(), 'addFilter')
      render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
      const trigger = screen.getByRole('button', { name: /Filter actions for JFK/i })
      fireEvent.mouseDown(trigger, { metaKey: true })
      expect(addFilterSpy).toHaveBeenCalledWith('pop', 'JFK', 'include')
      expect(screen.queryByText(/Open in dashboard/i)).not.toBeInTheDocument()
    })

    it('plain click on the cell still opens the menu', () => {
      render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
      const trigger = screen.getByRole('button', { name: /Filter actions for JFK/i })
      fireEvent.click(trigger)
      expect(screen.getByText(/Filter origin page/i)).toBeInTheDocument()
    })

    it('renders an IP-family value as plain text (no filter menu) when masking', () => {
      mocks.maskIps.mockReturnValue(true)
      const { container } = render(
        <FilterValueCell filters={[{ column: 'client_ip', value: '73.217.41.xxx' }]} />,
      )
      expect(screen.getByText('73.217.41.xxx')).toBeInTheDocument()
      expect(container.querySelector('button')).toBeNull()
    })

    it('still allows filtering by oip (origin IP) when masking', () => {
      mocks.maskIps.mockReturnValue(true)
      render(<FilterValueCell filters={[{ column: 'oip', value: '151.101.1.51' }]} />)
      expect(
        screen.getByRole('button', { name: /Filter actions for 151.101.1.51/i }),
      ).toBeInTheDocument()
    })
  })
})

describe('TimeSeriesChart, ScorerSeries, PlotlyChart, TrafficChart, CardGrid, FieldSearchDialog, TopTenTable & GeoMap', () => {
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
    mocks.useRealPlotly = false
    mocks.mockTimeSeriesChart = false
    mocks.mockTopTenTable = false
    mocks.timeseries = undefined
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
      mocks.timeseries = { has_latency: true, granularity: 'hour', rows: rows() }
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
      mocks.timeseries = {
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
      mocks.timeseries = { has_latency: false, granularity: 'hour', rows: rows() }
      render(<ScorerLatencyChart serviceId="s" sinceHours={24} />)
      expect(captured).toBeNull()
      expect(screen.getByText(/Waiting for traffic logged with the scorer latency fields/)).toBeInTheDocument()
    })
  })

  describe('PlotlyChart, ChartA11yTable & tracesToTable', () => {
    beforeEach(() => {
      mocks.useRealPlotly = true
    })

    it('hydrates realtime chart labels across a server/browser clock boundary', async () => {
      const actual = await vi.importActual<typeof import('@/hooks/useBootstrap')>('@/hooks/useBootstrap')
      const mounted = vi.spyOn(bootstrapHooks, 'useMounted').mockImplementation(actual.useMounted)
      const now = Date.UTC(2026, 9, 27, 12, 0, 0)
      const clock = vi.spyOn(Date, 'now').mockReturnValue(now)
      const onRecoverableError = vi.fn()
      const container = document.createElement('div')
      const chart = <RealtimeChart title="Realtime requests" traces={[{ name: 'Requests/s', y: [1, 2], color: '#123456' }]} />
      try {
        container.innerHTML = renderToString(chart)
        clock.mockReturnValue(now + 1000)
        const view = render(chart, { container, hydrate: true, onRecoverableError })
        expect(onRecoverableError).not.toHaveBeenCalled()
        const table = within(container).getByRole('table', { name: 'Realtime requests' })
        expect(within(table).getAllByRole('row')).toHaveLength(3)
        expect(table).toHaveTextContent('Requests/s')
        view.unmount()
      } finally {
        clock.mockRestore()
        mounted.mockRestore()
        container.remove()
      }
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
      mocks.mockTimeSeriesChart = true
      mocks.activeFields.clear()
    })

    it.each([
      ['loading skeleton when aggregates undefined', { isReady: false, aggregates: undefined }, 'Initializing...', false],
      ['worker transform pending skeleton', { aggregates: { time_series: [{ time: '2026-01-01T00:00:00Z', value: 1 }] }, trafficData: [], transformPending: true }, 'Crunching logs...', false],
      ['empty state when trafficData is empty', { aggregates: { time_series: [] }, trafficData: [] }, 'No logs found for this period.', false],
      ['Group C hint when ttfb is not active', { metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] }, 'Requires Infrastructure (Group C) fields to be enabled in Fastly logging.', false],
      ['neutral no-logs copy when ttfb IS active', { metric: 'ttfb_client', aggregates: { time_series: [] }, trafficData: [] }, 'No logs found for this period.', true],
    ])('renders %s', (_label, overrides, expectedText, activateTtfb) => {
      if (activateTtfb) mocks.activeFields.add('ttfb')
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
      mocks.mockTopTenTable = true
    })

    it('distinguishes loading from an empty card selection, and renders categorized/custom cards once ready', () => {
      const { container, rerender: rerenderEmpty, unmount } = render(
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
      rerenderEmpty(
        <CardGrid
          visibleCardList={[]}
          isReady={true}
          isFetchingAggs={false}
          aggregates={sampleAggregates}
          compareAggregates={undefined}
          compareMode={false}
          topBotsData={undefined}
          collapsedSections={new Set()}
          toggleSectionCollapsed={vi.fn()}
          onRowClick={vi.fn()}
        />,
      )
      expect(container).toBeEmptyDOMElement()
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
      mocks.fieldValues = {
        values: [
          { value: 'jfk', count: 500 },
          { value: 'lhr', count: 300 },
          { value: 'den', count: 200 },
          { value: 'syd', count: 100 },
        ],
      }
      mocks.fieldValuesLoading = false
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
      mocks.fieldValues = undefined
      mocks.logFieldsCatalog.mockReset()
      mocks.logFieldsCatalog.mockReturnValue({ data: undefined })
      mocks.maskIps.mockReturnValue(false)
    })

    it.each([
      ['bare empty state when no field is supplied', undefined, false, null],
      ['catalog group hint for disabled group', 'ngwaf_action', true, 'Requires NGWAF fields to be enabled in Fastly logging.'],
      ['virtual NGWAF hint for _ngwaf_bot_name', '_ngwaf_bot_name', true, 'Requires NGWAF fields to be enabled in Fastly logging.'],
      ['User-Agent hint for _bot_name', '_bot_name', true, 'Requires User-Agent field to be enabled in Fastly logging.'],
    ])('renders %s', (_label, field, withCatalog, expectedHint) => {
      if (withCatalog) mocks.logFieldsCatalog.mockReturnValue({ data: SAMPLE_CATALOG })
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
      mocks.maskIps.mockReturnValue(true)
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
      mocks.logFieldsCatalog.mockReturnValue({ data: SAMPLE_CATALOG })

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
      mocks.activeFields.clear()
    })

    it('renders loading skeleton, empty states (active vs inactive country field), and interactive ChoroplethMap', async () => {
      const onCountryClick = vi.fn()
      const { rerender } = render(
        <GeoMap isReady={false} isFetchingAggs={false} aggregates={undefined} catalog={geoCatalog} onCountryClick={onCountryClick} />,
      )
      expect(screen.getByText('Initializing...')).toBeInTheDocument()

      rerender(<GeoMap isReady={true} isFetchingAggs={false} aggregates={{ map_data: [] }} catalog={undefined} onCountryClick={onCountryClick} />)
      expect(screen.getByText('Requires Geolocation fields to be enabled in Fastly logging.')).toBeInTheDocument()

      mocks.activeFields.add('country')
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
})
