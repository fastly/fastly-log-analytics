/**
 * Consolidated widget & component tests:
 * - DeltaIndicator, ErrorBoundary, HelpDialog, LazyMount, ServerFooter, Sparkline, TimeAgo
 * - Network page helpers (HealthBadge, SHIELDING_COLUMNS, getShieldingLabels, stateless help, adjustShieldingRows)
 * - ServiceSwitcher, TimezoneSwitcher, SyncStatusBadge request header timestamp
 * - DebugPanel & DiagnosticsPanel
 * - CronLiveLog, CronSettingsModal (LogSyncSection), LogsClient, QuickActionsBar, CRON registry
 * - DataTable, DataTableReadonly, DateTimeCell, makeLatencyColumns, SchemaTab, BotSourcesPanel
 *
 * @vitest-environment jsdom
 */
import {
  createTestQueryClient,
  createUseSSEMock,
  makeQueryWrapper,
  renderWithQuery,
  selectMockFactory,
  setupNextHeadersMock,
  spyOnConsoleError,
  stubIntersectionObserver,
  stubResizeObserver,
  stubSystemTimeZone,
} from '@/__tests__/helpers/page-smoke'
import { render, screen, fireEvent, cleanup, act, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, test, expect, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { QueryClient, type QueryCacheNotifyEvent } from '@tanstack/react-query'
import { axe } from 'vitest-axe'
import React, { type ReactNode } from 'react'
import type { CellContext, ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'

const { setHeaders } = setupNextHeadersMock()
const { state: sseState, resetState: resetSSE } = createUseSSEMock()

const mocks = vi.hoisted(() => ({
  useNowMs: vi.fn(() => 0),
  status: vi.fn(),
  bootstrap: vi.fn((): Record<string, unknown> => ({ data: { debug_state: { debug_responses_enabled: true } } })),
  lastSync: vi.fn(),
  analyst: vi.fn(() => false),
  formatTimeAgo: vi.fn((ts: string) => `ago:${ts}`),
  showToast: vi.fn(),
  useRealSelect: false,
}))

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/network'))
vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/components/ui/select', async (importOriginal) => {
  const real = await importOriginal<typeof import('@/components/ui/select')>()
  const mock = selectMockFactory() as unknown as typeof real
  return {
    ...real,
    Select: (props: React.ComponentProps<typeof real.Select>) =>
      mocks.useRealSelect ? <real.Select {...props} /> : <mock.Select {...props} />,
    SelectTrigger: (props: React.ComponentProps<typeof real.SelectTrigger>) =>
      mocks.useRealSelect ? <real.SelectTrigger {...props} /> : <mock.SelectTrigger {...props} />,
    SelectValue: (props: React.ComponentProps<typeof real.SelectValue>) =>
      mocks.useRealSelect ? <real.SelectValue {...props} /> : <mock.SelectValue {...props} />,
    SelectContent: (props: React.ComponentProps<typeof real.SelectContent>) =>
      mocks.useRealSelect ? <real.SelectContent {...props} /> : <mock.SelectContent {...props} />,
    SelectSeparator: (props: React.ComponentProps<typeof real.SelectSeparator>) =>
      mocks.useRealSelect ? <real.SelectSeparator {...props} /> : <mock.SelectSeparator {...props} />,
    SelectItem: (props: React.ComponentProps<typeof real.SelectItem>) =>
      mocks.useRealSelect ? <real.SelectItem {...props} /> : <mock.SelectItem {...props} />,
  }
})

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useFilterStore: vi.fn((selector?: (s: object) => unknown) => {
    const state = { filters: [], addFilter: vi.fn(), removeFilter: vi.fn(), clearFilters: vi.fn() }
    return selector ? selector(state) : state
  }),
}))

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsAnalyst: () => mocks.analyst(),
  useBootstrap: () => mocks.bootstrap(),
  useMounted: () => true,
  useMaskIps: () => false,
  useEnforceMaskedFilters: () => {},
}))

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
  useNowMs: () => mocks.useNowMs(),
  useSyncStatus: () => mocks.status(),
  useLastSync: () => mocks.lastSync(),
  useHeaderBadgeStream: () => ({ state: 'idle' }),
  useAdminEventStream: () => ({ state: 'idle' }),
  useDateFormat: () => ({
    full: (d: string) => `FULL(${d})`,
    abbr: () => 'TZ',
  }),
  cronCacheBust: vi.fn(),
}))

vi.mock('@/lib/utils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/utils')>()),
  formatTimeAgo: (ts: string) => mocks.formatTimeAgo(ts),
  showToast: mocks.showToast,
}))

vi.mock('@/components/SSEModal', () => ({
  ControlledSSEModal: () => null,
  SSEModal: () => null,
  SSEProgressView: () => null,
  SSEExecutionSection: () => null,
  SSEDialogFooter: () => null,
}))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()),
  SyncFromCloudModal: () => null,
}))
vi.mock('@/components/ReportLayout', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/ReportLayout')>()),
  NoServiceSelected: () => null,
}))

import { DeltaIndicator } from '@/components/Insights/InsightCard'
import { ErrorBoundary, LazyMount } from '@/components/AnalyticsCard'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { ServerFooter } from '@/components/ServerFooter'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { ServiceSwitcher, SyncStatusBadge, TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { TimezoneSwitcher } from '@/components/AppLayout'
import { useDebugStore, useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { getTimezoneAbbr, DEBUG_RESPONSES_COOKIE } from "@/lib/utils";
import { DebugPanel } from '@/components/DebugPanel'
import { DiagnosticsPanel } from '@/app/admin/_sections/OperationsOverview'
import { server } from '@/__tests__/helpers/msw/handlers'
import { client, getApiBase } from '@/lib/api'
import { LogSyncSection } from '@/components/CronSettingsModal/CronSettingsModal'
import LogsClient, { CronLiveLog, QuickActionsBar, SchemaTab, CRON_EXPLANATIONS, CRON_DISPLAY_NAMES, CRON_GROUPS } from '@/app/logs/_sections/LogsClient'
import { DataTable, DataTableReadonly, DateTimeCell, makeLatencyColumns, type LatencyRow } from '@/components/DataTable'
import { BotSourcesPanel } from '@/app/control-room/_sections/ControlRoomClient'
import {
  GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp,
  AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp,
  HealthBadge, SHIELDING_COLUMNS, getShieldingLabels,
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
} from '@/app/network/page'

beforeAll(() => {
  stubResizeObserver()
})

beforeEach(() => {
  stubResizeObserver()
  mocks.useRealSelect = false
  mocks.analyst.mockReturnValue(false)
  mocks.bootstrap.mockReturnValue({ data: { debug_state: { debug_responses_enabled: true } } })
  mocks.formatTimeAgo.mockImplementation((ts: string) => `ago:${ts}`)
  mocks.useNowMs.mockReturnValue(0)
})

afterEach(() => {
  cleanup()
  mocks.useNowMs.mockClear()
})

// ── Small UI Primitives ─────────────────────────────────────────────────────

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

    const { container: diff } = render(<DeltaIndicator current={1234} baseline={1000} />)
    expect(diff.textContent).toMatch(/\+234/)
  })
})

function Boom(): React.ReactElement {
  throw new Error('kaboom')
}

describe('ErrorBoundary', () => {
  let errSpy: ReturnType<typeof vi.spyOn>
  beforeEach(() => { errSpy = vi.spyOn(console, 'error').mockImplementation(() => {}) })
  afterEach(() => { errSpy.mockRestore() })

  it('renders default fallback when a child throws, custom fallback when provided, and children when ok', () => {
    const { unmount: u1 } = render(<ErrorBoundary><Boom /></ErrorBoundary>)
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('kaboom')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
    u1()

    const { unmount: u2 } = render(<ErrorBoundary><div data-testid="ok">all good</div></ErrorBoundary>)
    expect(screen.getByTestId('ok')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).toBeNull()
    u2()

    render(<ErrorBoundary fallback={(err) => <div data-testid="custom">caught: {err.message}</div>}><Boom /></ErrorBoundary>)
    expect(screen.getByTestId('custom')).toHaveTextContent('caught: kaboom')
  })
})

describe('HelpDialog', () => {
  it('renders title, icon, and children when open, nothing when closed, and respects size prop', () => {
    const { unmount: u1 } = render(
      <HelpDialog open onOpenChange={vi.fn()} title="My Title" icon={<span data-testid="help-icon">★</span>}>
        <p>My content</p>
      </HelpDialog>,
    )
    expect(screen.getByText('My Title')).toBeInTheDocument()
    expect(screen.getByText('My content')).toBeInTheDocument()
    expect(screen.getByTestId('help-icon')).toBeInTheDocument()
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-lg'))).toBe(true)
    u1()

    const { unmount: u2 } = render(<HelpDialog open={false} onOpenChange={vi.fn()} title="Hidden"><p>Hidden body</p></HelpDialog>)
    expect(screen.queryByText('Hidden body')).not.toBeInTheDocument()
    u2()

    const { unmount: u3 } = render(<HelpDialog open onOpenChange={vi.fn()} title="Large-only" size="xl">body</HelpDialog>)
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-xl'))).toBe(true)
    u3()
  })
})

describe('LazyMount', () => {
  afterEach(() => { vi.unstubAllGlobals() })

  it('mounts children once IntersectionObserver fires isIntersecting or immediately when unavailable', () => {
    const io = stubIntersectionObserver()
    const { unmount } = render(<LazyMount><div data-testid="child">payload</div></LazyMount>)
    expect(screen.queryByTestId('child')).toBeNull()
    act(() => { io.fire(true) })
    expect(screen.getByTestId('child')).toBeInTheDocument()
    unmount()

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
    const el2 = await ServerFooter()
    expect(el2).toBeNull()
    expect(fetchSpy).not.toHaveBeenCalled()
  })
})

const pts = (vals: number[]) =>
  vals.map((value, i) => ({ ts: new Date(2026, 0, 1, 0, i).toISOString(), value }))

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
    expect(mocks.useNowMs).toHaveBeenCalled()
    unmount()

    expect(render(<TimeAgo timestamp={null} />).container.textContent).toBe('')
    render(<TimeAgo timestamp={undefined} fallback={<span>Never</span>} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
  })
})

// ── Network Page Helpers ────────────────────────────────────────────────────

describe('HealthBadge', () => {
  test('null → em-dash; ≥80 → green; 50-79 → yellow; <50 → destructive (no color leak)', () => {
    render(<HealthBadge score={null} />)
    expect(screen.getByText('—')).toBeInTheDocument()
    const hi = render(<HealthBadge score={92} />)
    expect(hi.getByText('92/100')).toBeInTheDocument()
    expect(hi.container.querySelector('[class*="text-green"]')).not.toBeNull()
    hi.unmount()
    const mid = render(<HealthBadge score={65} />)
    expect(mid.getByText('65/100')).toBeInTheDocument()
    expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull()
    mid.unmount()
    const lo = render(<HealthBadge score={20} />)
    expect(lo.getByText('20/100')).toBeInTheDocument()
    expect(lo.container.querySelector('[class*="text-green"]')).toBeNull()
    expect(lo.container.querySelector('[class*="text-yellow"]')).toBeNull()
    lo.unmount()
  })
})

describe('SHIELDING_COLUMNS', () => {
  const makeInfo = (value: unknown, original: Record<string, unknown> = {}) => ({ getValue: () => value, row: { original } })
  const colById = (id: string) => SHIELDING_COLUMNS.find((c) => c.id === id)!
  const cellOf = (id: string) => colById(id).cell as (info: ReturnType<typeof makeInfo>) => React.ReactNode

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
    expect(reqs.container.textContent).toMatch(/1[.,]234[.,]567/)
    reqs.unmount()
    for (const id of ['p50_ms', 'p95_ms', 'p99_ms'] as const) {
      const r = render(<>{cellOf(id)(makeInfo(12.345))}</>)
      expect(r.container.textContent).toMatch(/12\.3ms/)
      r.unmount()
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
    const nul = render(<>{cell(makeInfo(null))}</>)
    expect(nul.container.textContent).toMatch(/—/); nul.unmount()
    const lo = render(<>{cell(makeInfo(1.2))}</>)
    expect(lo.container.querySelector('[class*="text-green"]')).not.toBeNull(); lo.unmount()
    const mid = render(<>{cell(makeInfo(2.4))}</>)
    expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull(); mid.unmount()
    const hi = render(<>{cell(makeInfo(5.0))}</>)
    expect(hi.container.querySelector('[class*="text-destructive"]')).not.toBeNull(); hi.unmount()
  })
})

describe('getShieldingLabels & stateless help components', () => {
  test('getShieldingLabels maps known ids, falls back to id for unknown, preserves order', () => {
    expect(getShieldingLabels(['edge_pop', 'requests'])).toEqual([
      { id: 'edge_pop', label: 'Edge POP' },
      { id: 'requests', label: 'Requests' },
    ])
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
  const eligible = (requests: number) => ({
    edge_pop: 'LAX', shield_pop: 'SJC', requests, efficiency_ratio: 20, anomaly_eligible: true,
  })

  test.each([
    ['below floor', 5, 30, { low_sample: true, anomaly_static: false }],
    ['above floor', 200, 30, { low_sample: false, anomaly_static: true }],
    ['at boundary (requests === floor)', 30, 30, { low_sample: false, anomaly_static: true }],
    ['no minimum (floor = 0)', 1, 0, { low_sample: false, anomaly_static: true }],
  ])('classifies eligible route %s', (_label, reqs, floor, expected) => {
    const [row] = adjustShieldingRows([eligible(reqs)], floor)
    expect(row).toMatchObject(expected)
  })

  test('raising the floor reclassifies a previously-flagged route as low_sample, and default floor works', () => {
    const [at30] = adjustShieldingRows([eligible(40)], 30)
    const [at100] = adjustShieldingRows([eligible(40)], 100)
    expect(at30.anomaly_static).toBe(true)
    expect(at100).toMatchObject({ anomaly_static: false, low_sample: true })

    const rows = adjustShieldingRows(
      [eligible(SHIELDING_MIN_REQUESTS_DEFAULT - 1), eligible(SHIELDING_MIN_REQUESTS_DEFAULT)],
      SHIELDING_MIN_REQUESTS_DEFAULT,
    )
    expect(rows[0]).toMatchObject({ low_sample: true, anomaly_static: false })
    expect(rows[1]).toMatchObject({ low_sample: false, anomaly_static: true })
  })

  test('never flags latency-ineligible routes, preserves passthrough fields, is null-safe, and includes default in options', () => {
    const notEligible = { ...eligible(500), anomaly_eligible: false }
    for (const floor of [0, 10, 30, 100]) {
      expect(adjustShieldingRows([notEligible], floor)[0].anomaly_static).toBe(false)
    }

    const [row] = adjustShieldingRows([{ edge_pop: 'CDG', shield_pop: 'FRA', p50_ms: 12.3, anomaly_eligible: false }], 30)
    expect(row).toMatchObject({ edge_pop: 'CDG', p50_ms: 12.3, low_sample: true })

    expect(adjustShieldingRows(null, 30)).toEqual([])
    expect(adjustShieldingRows(undefined, 30)).toEqual([])
    expect(adjustShieldingRows([], 30)).toEqual([])
    expect(SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => o.value)).toContain(String(SHIELDING_MIN_REQUESTS_DEFAULT))
  })
})

// ── ServiceSwitcher, TimezoneSwitcher & SyncStatusBadge ─────────────────────

const switcherQueryClient = createTestQueryClient()
const renderSwitcher = () => render(<ServiceSwitcher />, { wrapper: makeQueryWrapper(switcherQueryClient) })
const PREFS_KEY = 'fla-service-switcher-prefs'

describe('ServiceSwitcher MRU + pinning', () => {
  beforeEach(() => {
    window.localStorage.clear()
    switcherQueryClient.clear()
    useServiceStore.setState({
      services: [
        { id: 'svc-alpha', name: 'Alpha Service' },
        { id: 'svc-beta', name: 'Beta Service' },
        { id: 'svc-gamma', name: 'Gamma Service' },
        { id: 'svc-delta', name: 'Delta Service' },
      ],
      activeServiceId: 'svc-alpha',
    })
  })

  it('opens to show every service when no prefs exist and writes active service to MRU', async () => {
    const user = userEvent.setup()
    renderSwitcher()
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru[0]).toBe('svc-alpha')

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
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.pinned).toEqual(['svc-beta'])
    expect(useServiceStore.getState().activeServiceId).toBe('svc-alpha')
    expect(screen.getByRole('button', { name: /unpin beta service/i })).toBeInTheDocument()
  })

  it('hydrates Pinned and Recent buckets from localStorage on mount', async () => {
    window.localStorage.setItem(
      PREFS_KEY,
      JSON.stringify({ pinned: ['svc-gamma'], mru: ['svc-beta', 'svc-delta'] }),
    )
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))

    expect(screen.getByRole('button', { name: /unpin gamma service/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin beta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin delta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin alpha service to top/i })).toBeInTheDocument()

    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru).toEqual(['svc-alpha', 'svc-beta', 'svc-delta'])
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
    const entries = Array.from(
      content.querySelectorAll('[data-select-item-value], [data-testid="mock-select-separator"]'),
    ).map((el) => el.getAttribute('data-select-item-value') ?? 'SEPARATOR')

    expect(entries).toEqual([
      '__system__', 'SEPARATOR', 'UTC', 'America/New_York', 'America/Chicago',
      'America/Denver', 'America/Los_Angeles', 'Europe/London', 'Europe/Berlin',
      'Asia/Tokyo', 'Australia/Sydney',
    ])

    const abbr = getTimezoneAbbr(new Date(), 'America/Chicago')
    await waitFor(() => {
      expect(screen.getByTestId('mock-select-value')).toHaveTextContent(`System Time (${abbr})`)
    })
  })

  it('switches between manual concrete zones and live browser System Time, hiding manual fallback in system mode', async () => {
    const user = userEvent.setup()
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Asia/Kolkata' })
    const { rerender } = render(<TimezoneSwitcher />)

    await waitFor(() => {
      expect(screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata')).toBe(true)
    })

    const tokyoItem = screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'Asia/Tokyo')!
    await user.click(tokyoItem)
    expect(useTimezoneStore.getState()).toMatchObject({ mode: 'manual', timezone: 'Asia/Tokyo' })

    stubSystemTimeZone('Australia/Sydney')
    const systemItem = screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === '__system__')!
    await user.click(systemItem)
    expect(useTimezoneStore.getState()).toMatchObject({ mode: 'system', timezone: 'Australia/Sydney' })

    useTimezoneStore.setState({ mode: 'system', timezone: 'Asia/Kolkata' })
    rerender(<TimezoneSwitcher />)
    expect(screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata')).toBe(false)
  })
})

const oldTimestamp = '2026-09-06T17:19:00Z'
const requestTimestamp = '2026-09-07T19:31:22Z'
const syncTimestamp = '2026-09-08T02:33:23Z'

function mountBadge() {
  return render(<SyncStatusBadge />, { wrapper: makeQueryWrapper(createTestQueryClient()) })
}

describe('request header timestamp', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service' })
    mocks.formatTimeAgo.mockImplementation((ts: string) => ts)
    mocks.useNowMs.mockReturnValue(Date.parse('2026-09-08T18:00:00Z'))
    mocks.status.mockReturnValue({ data: undefined })
    mocks.analyst.mockReturnValue(false)
    mocks.lastSync.mockReturnValue({ data: { started_at: syncTimestamp, status: 'success' } })
    mocks.bootstrap.mockReturnValue({
      data: {
        services: [{ service_id: 'test-service', rum_enabled: false }],
        header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: oldTimestamp } },
      },
    })
  })

  it.each([
    [oldTimestamp, requestTimestamp],
    [requestTimestamp, oldTimestamp],
  ])('uses the request extent rather than flat summary %s', async (flat, canonical) => {
    mocks.status.mockReturnValue({ data: { latest_log_at: flat, request: { latest_log_at: canonical } } })
    mountBadge()
    await waitFor(() => expect(screen.getByText(canonical)).toBeInTheDocument())
    expect(screen.queryByText(flat)).not.toBeInTheDocument()
    expect(screen.getByText(syncTimestamp)).toBeInTheDocument()
  })

  it('uses the analyst request extent from bootstrap and omits stale-log dot', async () => {
    mocks.analyst.mockReturnValue(true)
    mocks.bootstrap.mockReturnValue({
      data: { header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: requestTimestamp } } },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
    expect(screen.queryByRole('img', { name: /Latest log is/ })).not.toBeInTheDocument()
  })

  it('accepts an older canonical extent from a replacement snapshot', async () => {
    mocks.status.mockReturnValue({ data: { request: { latest_log_at: requestTimestamp } } })
    const { rerender } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    mocks.status.mockReturnValue({
      data: { latest_log_at: requestTimestamp, request: { latest_log_at: oldTimestamp } },
    })
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
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
    u1()

    mocks.status.mockReturnValue({ data: { latest_log_at: requestTimestamp } })
    const { unmount: u2 } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    u2()

    const rumTimestamp = '2026-09-08T17:00:00Z'
    mocks.bootstrap.mockReturnValue({ data: { services: [{ service_id: 'test-service', rum_enabled: true }] } })
    mocks.status.mockReturnValue({
      data: {
        local_rows: 2700,
        latest_log_at: rumTimestamp,
        request: { latest_log_at: requestTimestamp, total_rows: 2700 },
        rum: { latest_log_at: rumTimestamp, total_rows: 4430 },
      },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    expect(screen.getByText(rumTimestamp)).toBeInTheDocument()
    expect(screen.getByText('total: 2,700')).toBeInTheDocument()
    expect(screen.getByText('total: 4,430')).toBeInTheDocument()
  })
})

// ── DebugPanel & DiagnosticsPanel ───────────────────────────────────────────

const API_BASE = 'http://127.0.0.1:8000'
const makeDebugClient = () => createTestQueryClient({ queries: { gcTime: Infinity, staleTime: Infinity } })
const renderDebugWith = (qc: QueryClient) => renderWithQuery(<DebugPanel />, { queryClient: qc })

describe('DebugPanel & DiagnosticsPanel', () => {
  beforeEach(() => {
    act(() => {
      useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
    })
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/`
    server.use(
      http.get(`${API_BASE}/api/debug/recent-sqlite`, () =>
        HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 }),
      ),
    )
  })

  test('cookie-heal shim writes missing cookie + invalidates cache, and skips when cookie present', async () => {
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=; path=/; max-age=0`
    act(() => { useDebugStore.setState({ enabled: true, apiCallsEnabled: false }) })
    const qc1 = makeDebugClient()
    const spy1 = vi.spyOn(qc1, 'invalidateQueries')
    const { unmount } = renderDebugWith(qc1)
    await waitFor(() => expect(document.cookie).toContain(`${DEBUG_RESPONSES_COOKIE}=1`))
    expect(spy1).toHaveBeenCalled()
    unmount()

    const qc2 = makeDebugClient()
    const spy2 = vi.spyOn(qc2, 'invalidateQueries')
    renderDebugWith(qc2)
    await new Promise((r) => setTimeout(r, 20))
    expect(spy2).not.toHaveBeenCalled()
  })

  test('renders nothing while both debug toggles are off, and polls SQLite ring-buffer only in Process-wide scope', async () => {
    const { container, unmount } = renderDebugWith(makeDebugClient())
    expect(container).toBeEmptyDOMElement()
    unmount()

    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/debug/recent-sqlite`, () => {
        calls += 1
        return HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 })
      }),
    )

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

  test("page scope lists _debug_sqlite statements and expand/collapse of each section is independent", async () => {
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
        queryFn: async () => null,
        staleTime: Infinity,
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
      useServiceStore.setState({
        activeServiceId: 'std-svc',
        services: [{ id: 'std-svc', name: 'Standard Service', is_high_scale: false } as never],
      })
    })
    const qc = makeDebugClient()
    const { useQuery } = await import('@tanstack/react-query')
    function Seeder() {
      useQuery({
        queryKey: ['debug-fixture', 'one'],
        initialData: {
          _debug_queries: [{ sql: 'SELECT 1', time_ms: 12.5 }],
          _debug_calls: [{ service: 'FOS', method: 'GET', path: '/x', time_ms: 8, status: 'OK' }],
        },
        queryFn: async () => null,
        staleTime: Infinity,
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
      useServiceStore.setState({
        activeServiceId: 'hs-svc',
        services: [{ id: 'hs-svc', name: 'High-Scale Service', is_high_scale: true } as never],
      })
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
    const qc = createTestQueryClient()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')

    renderWithQuery(<DiagnosticsPanel />, { queryClient: qc })
    await screen.findByText('Query debugging panel')

    const selectContainers = screen.getAllByTestId('mock-select')
    expect(selectContainers).toHaveLength(2)
    expect(selectContainers[0].getAttribute('data-select-value')).toBe('disabled')
    expect(selectContainers[1].getAttribute('data-select-value')).toBe('disabled')

    const queryAdminsItem = screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'admins')!
    await user.click(queryAdminsItem)
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalled())

    invalidateSpy.mockClear()
    const apiBothItems = screen.getAllByRole('button').filter((b) => b.getAttribute('data-select-item-value') === 'both')
    expect(apiBothItems).toHaveLength(2)
    await user.click(apiBothItems[1])
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalled())
  })
})

// ── CronLiveLog, LogSyncSection, LogsClient & QuickActionsBar ───────────────

describe('CronLiveLog, LogSyncSection & LogsClient', () => {
  let errorSpy: ReturnType<typeof spyOnConsoleError>

  beforeEach(() => {
    resetSSE()
    mocks.showToast.mockClear()
    errorSpy = spyOnConsoleError()
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

  it('CronLiveLog renders each line type, accumulates lines & SESSION INITIATED header in terminalMode, and truncates in singleLine mode', () => {
    sseState.status = 'streaming'
    sseState.lines = [
      { _id: 1, type: 'status', message: 'Starting ingest' },
      { _id: 2, type: 'file_done', file_name: 'a.gz' },
      { _id: 3, type: 'error', message: 'Boom' },
      { _id: 4, type: 'done', message: 'All clean' },
    ]
    const { unmount: u1 } = render(<CronLiveLog runId={1} terminalMode startedAt="2026-06-16T00:00:00Z" />)
    expect(screen.getByText('Starting ingest')).toBeInTheDocument()
    expect(screen.getByText(/Processed a\.gz/)).toBeInTheDocument()
    expect(screen.getByText('Boom')).toBeInTheDocument()
    expect(screen.getByText('All clean')).toBeInTheDocument()
    expect(screen.getByText(/SESSION INITIATED AT: FULL\(2026-06-16T00:00:00Z\) TZ/)).toBeInTheDocument()
    u1()

    const long = 'x'.repeat(120)
    sseState.lines = [
      { _id: 1, type: 'status', message: 'old-one' },
      { _id: 2, type: 'status', message: long },
    ]
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
    const setPollingMode = vi.fn()
    const setRumSyncEnabled = vi.fn()
    const baseProps = {
      syncEnabled: true, setSyncEnabled: vi.fn(), pollingMode: 'regular' as const, setPollingMode,
      deleteAfter: false, setDeleteAfter: vi.fn(), dataRetention: '30', setDataRetention: vi.fn(),
      cacheRetention: '90', setCacheRetention: vi.fn(), rollupRetention: '12', setRollupRetention: vi.fn(),
      commitInterval: '5', setCommitInterval: vi.fn(), syncLogEnabled: true, setSyncLogEnabled: vi.fn(),
      syncRetention: '7', setSyncRetention: vi.fn(), syncFreqLabel: 'every 2 minutes', isAnalyst: false,
      syncIntervalNum: 120, adminSyncSeconds: 120, rumRetention: '30', setRumRetention: vi.fn(),
      rumEnabled: false, rumLogPeriod: '300', setRumLogPeriod: vi.fn(), rumDeleteAfter: false, setRumDeleteAfter: vi.fn(),
    }

    const { rerender } = render(
      <LogSyncSection {...baseProps} rumEnabled={false} rumSyncEnabled={false} setRumSyncEnabled={setRumSyncEnabled} serviceId="test-service-123" />,
    )
    expect(screen.getByText(/Adaptive makes up to two short follow-up discovery passes/)).toBeInTheDocument()
    await user.click(screen.getByRole('combobox', { name: 'Discovery Polling Mode' }))
    await user.click(await screen.findByRole('option', { name: 'Adaptive' }))
    expect(setPollingMode).toHaveBeenCalledWith('adaptive')

    expect(screen.getByText(/Real User Monitoring is not currently enabled on Fastly edge/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Configure RUM' })).toHaveAttribute('href', '/admin/rum?service=test-service-123')

    rerender(
      <LogSyncSection {...baseProps} rumEnabled={true} rumSyncEnabled={false} setRumSyncEnabled={setRumSyncEnabled} serviceId="test-service-123" />,
    )
    const rumSwitch = screen.getByRole('switch', { name: 'Enable RUM Beacon Sync' })
    expect(rumSwitch).toHaveAttribute('aria-checked', 'false')
    await user.click(rumSwitch)
    expect(setRumSyncEnabled).toHaveBeenCalledWith(true, expect.anything())
  })

  test('LogsClient mounts in cron mode, QuickActionsBar handles failures, and partial_hour_merge is registered', async () => {
    useServiceStore.setState({ activeServiceId: 'test-svc' })
    mocks.status.mockReturnValue({ data: { idle: true, access_level: 'read_write' } })
    const { unmount } = renderWithQuery(<LogsClient initialTab="cron" />)
    expect(screen.getByText('Data Management')).toBeInTheDocument()
    expect(errorSpy).not.toHaveBeenCalled()
    unmount()

    expect(CRON_DISPLAY_NAMES.partial_hour_merge).toBeTruthy()
    expect(CRON_EXPLANATIONS.partial_hour_merge).toBeTruthy()
    expect(CRON_GROUPS.find((g) => g.tasks.includes('rollup_hour_heal'))?.tasks).toContain('partial_hour_merge')

    const postSpy = vi.spyOn(client, 'POST')
    const start = vi.fn()
    const barProps: React.ComponentProps<typeof QuickActionsBar> = {
      isAnalyst: false, status: { access_level: 'read_write' }, activeServiceId: 'svc',
      recentCrons: {}, cronLogs: {}, setSseTitle: vi.fn(), setSseDescription: vi.fn(),
      setIsSSEModalOpen: vi.fn(), setIsSyncModalOpen: vi.fn(), reset: vi.fn(), start,
      setDisplayedJobs: vi.fn(), setSelectedConsoleJobId: vi.fn(), setConsoleOpen: vi.fn(),
    }
    renderWithQuery(<QuickActionsBar {...barProps} />)

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

// ── DataTable, DateTimeCell, makeLatencyColumns, SchemaTab & BotSourcesPanel ─

interface Row {
  id: number
  name: string
  status: number
}

const COLUMNS: ColumnDef<Row>[] = [
  { accessorKey: 'id', header: 'ID', id: 'id' },
  { accessorKey: 'name', header: 'Name', id: 'name' },
  { accessorKey: 'status', header: 'Status', id: 'status' },
]

const DATA: Row[] = [
  { id: 1, name: 'alpha', status: 200 },
  { id: 2, name: 'beta', status: 404 },
  { id: 3, name: 'gamma', status: 500 },
]

describe('DataTable & DataTableReadonly', () => {
  it('renders headers, rows, empty state, loading skeletons, and ignores stale initialColumnOrder IDs', () => {
    const { unmount: u1 } = render(
      <DataTable columns={COLUMNS} data={DATA} hideToolbar initialColumnOrder={['name', 'id', 'status', 'phantom_column']} />,
    )
    for (const h of ['ID', 'Name', 'Status', 'alpha', 'beta', 'gamma']) expect(screen.getByText(h)).toBeTruthy()
    u1()

    const { unmount: u2 } = render(<DataTable columns={COLUMNS} data={[]} hideToolbar emptyMessage="No results found." />)
    expect(screen.getByText('No results found.')).toBeTruthy()
    u2()

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
      expect(screen.getByText('row-119')).toBeTruthy()
      unmount()
    }
    render(<DataTableReadonly columns={COLUMNS} data={MANY} hideToolbar />)
    expect(screen.queryByText('row-119')).toBeNull()
    expect(screen.getByText('row-49')).toBeTruthy()
  })
})

describe('DateTimeCell & makeLatencyColumns', () => {
  it('DateTimeCell renders default em-dash, custom emptyFallback, and live TimeAgo', () => {
    const { container, unmount: u1 } = render(<DateTimeCell iso={null} />)
    expect(container.querySelector('span.text-muted-foreground\\/40')?.textContent).toBe('—')
    u1()

    const { unmount: u2 } = render(<DateTimeCell iso={undefined} emptyFallback={<span>N/A</span>} />)
    expect(screen.getByText('N/A')).toBeInTheDocument()
    u2()

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
    expect(c1.querySelector('button')).toBeNull()
    unmount()

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
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'svc-tables', isInitialized: true })
  })

  it('SchemaTab renders every column past 50 rows with formatted stats and em-dash for bare columns', () => {
    const schema = Array.from({ length: 80 }, (_, i) => ({
      name: `col_${i}`, type: 'VARCHAR', null_percentage: i === 0 ? 25 : 0, approx_unique: 1234, min: 'a', max: 'z',
    }))
    const { unmount } = render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    for (const name of ['Column Name', 'DuckDB Type', 'Populated %', 'Approx Unique', 'Min Value', 'Max Value']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('col_79')).toBeTruthy()
    expect(screen.getByText('75.0%')).toBeTruthy()
    unmount()

    render(<SchemaTab schemaData={{ schema: [{ name: 'bare', type: 'INTEGER' }] } as unknown as SchemaResponse} isLoadingSchema={false} />)
    const row = screen.getByText('bare').closest('tr') as HTMLElement
    expect(within(row).getAllByText('—').length).toBe(4)
  })

  it('BotSourcesPanel lists sources, refreshes chosen source, triggers rDNS enrich, and surfaces rDNS errors', async () => {
    const refreshed: string[] = []
    let enrichCalls = 0
    let listCalls = 0
    const base = getApiBase()
    server.use(
      http.get(`${base}/api/admin/bot-sources`, () => {
        listCalls += 1
        return HttpResponse.json({
          sources: [{ id: 'src-a', name: 'Alpha Bots', url: 'https://example.test/a', entry_count: 1500, last_updated: null }],
          rdns: { total: 10, pending: 2, last_enrichment_at: null },
        })
      }),
      http.post(`${base}/api/admin/bot-sources/:id/refresh`, ({ params }) => {
        refreshed.push(String(params.id))
        return HttpResponse.json({})
      }),
      http.post(`${base}/api/admin/rdns/enrich`, () => {
        enrichCalls += 1
        return HttpResponse.json({ ok: true, summary: {} })
      }),
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

    server.use(
      http.post(`${base}/api/admin/rdns/enrich`, () =>
        HttpResponse.json({ detail: { error: 'rdns_failed', message: 'enrichment exploded' } }, { status: 500 }),
      ),
    )
    renderWithQuery(<BotSourcesPanel />)
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})
