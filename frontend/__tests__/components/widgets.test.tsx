/**
 * Consolidated widget tests: DeltaIndicator, ErrorBoundary, HelpDialog,
 * LazyMount, ServerFooter, Sparkline, and TimeAgo.
 *
 * @vitest-environment jsdom
 */
import { setupNextHeadersMock, stubIntersectionObserver } from '@/__tests__/helpers/page-smoke'
import { render, screen, cleanup, act } from '@testing-library/react'
import { describe, it, test, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'

const { setHeaders } = setupNextHeadersMock()

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/network'))
vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useFilterStore: vi.fn((selector?: (s: object) => unknown) => {
    const state = { filters: [], addFilter: vi.fn(), removeFilter: vi.fn(), clearFilters: vi.fn() }
    return selector ? selector(state) : state
  }),
}))
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsAnalyst: () => false,
  useMaskIps: () => false,
  useEnforceMaskedFilters: () => {},
}))

const { useNowMsMock } = vi.hoisted(() => ({ useNowMsMock: vi.fn(() => 0) }))
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()), useNowMs: useNowMsMock }))
vi.mock('@/lib/format', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/format')>()),
  formatTimeAgo: (ts: string) => `ago:${ts}`,
}))

import { DeltaIndicator } from '@/components/Insights/InsightCard'
import { ErrorBoundary } from '@/components/AnalyticsCard'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { LazyMount } from '@/components/AnalyticsCard'
import { ServerFooter } from '@/components/ServerFooter'
import { Sparkline } from '@/components/Sparkline'
import { TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import {
  GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp,
  AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp,
  HealthBadge, SHIELDING_COLUMNS, getShieldingLabels,
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
} from '@/app/network/help-content'

afterEach(() => {
  cleanup()
  useNowMsMock.mockClear()
})

describe('DeltaIndicator', () => {
  it('renders nothing when baseline is null or zero', () => {
    const { container: c1 } = render(<DeltaIndicator current={100} baseline={null} />)
    expect(c1.firstChild).toBeNull()
    const { container: c2 } = render(<DeltaIndicator current={100} baseline={0} />)
    expect(c2.firstChild).toBeNull()
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
  beforeEach(() => {
    errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    errSpy.mockRestore()
  })

  it('renders default fallback when a child throws, custom fallback when provided, and children when ok', () => {
    const { unmount: u1 } = render(
      <ErrorBoundary>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('kaboom')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
    u1()

    const { unmount: u2 } = render(
      <ErrorBoundary>
        <div data-testid="ok">all good</div>
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('ok')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).toBeNull()
    u2()

    render(
      <ErrorBoundary fallback={(err) => <div data-testid="custom">caught: {err.message}</div>}>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('custom')).toHaveTextContent('caught: kaboom')
  })
})

describe('HelpDialog', () => {
  it('renders title, icon, and children when open, nothing when closed, and respects size prop', () => {
    const { unmount: u1 } = render(
      <HelpDialog
        open
        onOpenChange={vi.fn()}
        title="My Title"
        icon={<span data-testid="help-icon">★</span>}
      >
        <p>My content</p>
      </HelpDialog>,
    )
    expect(screen.getByText('My Title')).toBeInTheDocument()
    expect(screen.getByText('My content')).toBeInTheDocument()
    expect(screen.getByTestId('help-icon')).toBeInTheDocument()
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-lg'))).toBe(true)
    u1()

    const { unmount: u2 } = render(
      <HelpDialog open={false} onOpenChange={vi.fn()} title="Hidden">
        <p>Hidden body</p>
      </HelpDialog>,
    )
    expect(screen.queryByText('Hidden body')).not.toBeInTheDocument()
    u2()

    const { unmount: u3 } = render(
      <HelpDialog open onOpenChange={vi.fn()} title="Large-only" size="xl">
        body
      </HelpDialog>,
    )
    expect(Array.from(document.querySelectorAll('[role="dialog"]')).some((d) => d.className.includes('max-w-xl'))).toBe(true)
    u3()
  })
})

describe('LazyMount', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('mounts children once IntersectionObserver fires isIntersecting or immediately when unavailable', () => {
    const io = stubIntersectionObserver()
    const { unmount } = render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    expect(screen.queryByTestId('child')).toBeNull()
    act(() => {
      io.fire(true)
    })
    expect(screen.getByTestId('child')).toBeInTheDocument()
    unmount()

    vi.stubGlobal('IntersectionObserver', undefined)
    render(
      <LazyMount>
        <div data-testid="child">payload</div>
      </LazyMount>,
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })
})

describe('ServerFooter', () => {
  beforeEach(() => {
    setHeaders({})
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, text: async () => 'secret-instance' }))
    process.env.COMMIT_HASH = 'abc1234'
  })
  afterEach(() => {
    vi.unstubAllGlobals()
  })

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
    expect(useNowMsMock).toHaveBeenCalled()
    unmount()

    expect(render(<TimeAgo timestamp={null} />).container.textContent).toBe('')
    render(<TimeAgo timestamp={undefined} fallback={<span>Never</span>} />)
    expect(screen.getByText('Never')).toBeInTheDocument()
  })
})

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

describe('getShieldingLabels', () => {
  test('maps known ids, falls back to id for unknown, preserves order', () => {
    expect(getShieldingLabels(['edge_pop', 'requests'])).toEqual([
      { id: 'edge_pop', label: 'Edge POP' },
      { id: 'requests', label: 'Requests' },
    ])
    expect(getShieldingLabels(['mystery_field'])).toEqual([
      { id: 'mystery_field', label: 'mystery_field' },
    ])
    expect(getShieldingLabels(['p99_ms', 'p50_ms', 'p95_ms']).map((o) => o.id))
      .toEqual(['p99_ms', 'p50_ms', 'p95_ms'])
  })
})

describe('stateless help components', () => {
  test('GlobalHealthHelp mentions the 0–100 composite score', () => {
    render(<GlobalHealthHelp />)
    expect(screen.getByText(/0[–-]100 score/i)).toBeInTheDocument()
  })
  test('AvgRttHelp mentions TCP Round Trip Time', () => {
    render(<AvgRttHelp />)
    expect(screen.getByText(/TCP Round Trip Time/i)).toBeInTheDocument()
  })
  test('WorstAsnHelp mentions Autonomous System Number', () => {
    render(<WorstAsnHelp />)
    expect(screen.getByText(/Autonomous System Number/i)).toBeInTheDocument()
  })
  test('WorstRegionHelp mentions country-level network health', () => {
    const { container } = render(<WorstRegionHelp />)
    expect(container.textContent).toMatch(/country experiencing the lowest/i)
  })
  test('HeatmapHelp explains ISP rows + time-bucket columns', () => {
    const { container } = render(<HeatmapHelp />)
    expect(container.textContent).toMatch(/row is an ISP/i)
    expect(container.textContent).toMatch(/column is a time bucket/i)
  })
  test('AsnLeaderboardHelp surfaces P95/P99 tail-latency context', () => {
    const { container } = render(<AsnLeaderboardHelp />)
    expect(container.textContent).toMatch(/P95 \/ P99 RTT/i)
  })
  test('MetroLeaderboardHelp mentions city / metro area segmentation', () => {
    const { container } = render(<MetroLeaderboardHelp />)
    expect(container.textContent).toMatch(/city or metro area/i)
  })
  test('ShieldingHelp includes the efficiency legend buckets', () => {
    const { container } = render(<ShieldingHelp />)
    expect(container.textContent).toMatch(/Excellent/i)
    expect(container.textContent).toMatch(/Moderate/i)
    expect(container.textContent).toMatch(/Investigate/i)
  })
})

const eligible = (requests: number) => ({
  edge_pop: 'LAX',
  shield_pop: 'SJC',
  requests,
  efficiency_ratio: 20,
  anomaly_eligible: true,
})

describe('adjustShieldingRows', () => {
  test('marks a route below the floor low_sample and never flags it (even if eligible)', () => {
    const [row] = adjustShieldingRows([eligible(5)], 30)
    expect(row.low_sample).toBe(true)
    expect(row.anomaly_static).toBe(false)
  })

  test('flags an eligible route at/above the floor', () => {
    const [row] = adjustShieldingRows([eligible(200)], 30)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  test('treats the boundary value (requests === floor) as enough samples', () => {
    const [row] = adjustShieldingRows([eligible(30)], 30)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  test('raising the floor reclassifies a previously-flagged route as low_sample', () => {
    const [at30] = adjustShieldingRows([eligible(40)], 30)
    const [at100] = adjustShieldingRows([eligible(40)], 100)
    expect(at30.anomaly_static).toBe(true)
    expect(at100.anomaly_static).toBe(false)
    expect(at100.low_sample).toBe(true)
  })

  test('"No minimum" (0) disables the floor: nothing is low_sample, eligible routes flag', () => {
    const [row] = adjustShieldingRows([eligible(1)], 0)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  test('never flags a route the backend deemed latency-ineligible, at any floor', () => {
    const notEligible = { ...eligible(500), anomaly_eligible: false }
    for (const floor of [0, 10, 30, 100]) {
      const [row] = adjustShieldingRows([notEligible], floor)
      expect(row.anomaly_static).toBe(false)
    }
  })

  test('default floor greys eligible routes below it and flags them at/above', () => {
    const rows = adjustShieldingRows(
      [eligible(SHIELDING_MIN_REQUESTS_DEFAULT - 1), eligible(SHIELDING_MIN_REQUESTS_DEFAULT)],
      SHIELDING_MIN_REQUESTS_DEFAULT,
    )
    expect(rows[0]).toMatchObject({ low_sample: true, anomaly_static: false })
    expect(rows[1]).toMatchObject({ low_sample: false, anomaly_static: true })
  })

  test('passes other fields through untouched and treats missing requests as 0', () => {
    const [row] = adjustShieldingRows(
      [{ edge_pop: 'CDG', shield_pop: 'FRA', p50_ms: 12.3, anomaly_eligible: false }],
      30,
    )
    expect(row.edge_pop).toBe('CDG')
    expect(row.p50_ms).toBe(12.3)
    expect(row.low_sample).toBe(true)
  })

  test('is null/undefined-safe', () => {
    expect(adjustShieldingRows(null, 30)).toEqual([])
    expect(adjustShieldingRows(undefined, 30)).toEqual([])
    expect(adjustShieldingRows([], 30)).toEqual([])
  })

  test('the default is one of the offered options', () => {
    const values = SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => o.value)
    expect(values).toContain(String(SHIELDING_MIN_REQUESTS_DEFAULT))
  })
})
