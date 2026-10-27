import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import type { components } from '@/types/api.generated'
import { useServiceStore, useFilterStore } from '@/stores/filterStore'
import {
  createTestQueryClient, renderWithQuery,
  spyOnConsoleError, stubResizeObserver, stubPointerCaptureAndScroll,
  createUseSSEMock, partial, type ReportLayoutChildArgs, type InteractiveDataTableMockProps,
} from '../helpers/page-smoke'
import SecurityClient, {
  BotsSection,
  HeaderAnomaliesSection,
  NetworkSection,
  ProxyWatchdogSection,
} from '@/app/security/_sections/SecurityClient'
import type { PlotlyChart } from '@/components/PlotlyChart'
import React, { type ComponentProps } from 'react'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

const { state: sseState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
}))

vi.mock('../../playwright.config', () => ({ E2E_BACKEND_PORT: 18004, E2E_FRONTEND_PORT: 13004 }))
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('next-themes', () => ({ useTheme: vi.fn(() => ({ theme: 'light' })) }))

let mockPathname = '/dashboard'
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), prefetch: vi.fn(), back: vi.fn() }),
  useSearchParams: () => new URLSearchParams(window.location.search),
  usePathname: () => mockPathname,
}))

vi.mock('@uiw/react-codemirror', async () => {
  const R = await import('react')
  return {
    default: R.forwardRef(function MockCodeMirror(
      { value, onChange }: { value: string; onChange: (v: string) => void },
      ref: React.Ref<HTMLTextAreaElement>,
    ) {
      return <textarea ref={ref} data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
    }),
  }
})

let useRealApiClient = false
let useMockAdminFetch = false
const {
  mockGet, mockPost, mockDelete,
  mockAdminFetch, realAdminFetchRef, plotlyMock, dataTableMock, securityActiveFields,
} = vi.hoisted(() => ({
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockGet: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockPost: vi.fn<(...args: any[]) => any>(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mockDelete: vi.fn<(...args: any[]) => any>(),
  useFactsMock: vi.fn(),
  mockAdminFetch: vi.fn(),
  realAdminFetchRef: { current: null as ((...args: unknown[]) => Promise<Response>) | null },
  plotlyMock: vi.fn<(props: ComponentProps<typeof PlotlyChart>) => void>(),
  dataTableMock: vi.fn<(props: InteractiveDataTableMockProps) => void>(),
  securityActiveFields: new Set<string>(),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  realAdminFetchRef.current = actual.adminFetch as (...args: unknown[]) => Promise<Response>
  const wrap = (fn: unknown, mock: (...a: unknown[]) => unknown) =>
    vi.fn((...args: unknown[]) => (useRealApiClient ? (fn as (...a: unknown[]) => unknown)(...args) : mock(...args)))
  return {
    ...actual,
    adminFetch: (...args: unknown[]) => (useMockAdminFetch ? mockAdminFetch(...args) : realAdminFetchRef.current!(...args)),
    client: {
      GET: wrap(actual.client.GET, mockGet),
      POST: wrap(actual.client.POST, mockPost),
      DELETE: wrap(actual.client.DELETE, mockDelete),
      use: actual.client.use,
    },
    extractApiError: vi.fn((...args: Parameters<typeof actual.extractApiError>) =>
      useRealApiClient ? actual.extractApiError(...args) : String(args[0]),
    ),
    getApiBase: vi.fn((...args: Parameters<typeof actual.getApiBase>) =>
      useRealApiClient ? actual.getApiBase(...args) : 'http://test',
    ),
  }
})

let reportLayoutChildArgs: ReportLayoutChildArgs | null = null
const useMockDataTable = false
let useInteractiveDataTableMock = false
let useMockAnalyticsCard = false
let useMockActiveLogFields = false
let useSecurityActiveFields = false
let mockIsDataReady: boolean | null = null
let useMockHighScaleBootstrap = false

vi.mock('@/hooks/useBootstrap', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useBootstrap')>()
  return {
    ...actual,
    useActiveLogFields: (...args: Parameters<typeof actual.useActiveLogFields>) =>
      useSecurityActiveFields
        ? {
            ready: true,
            isFieldActive: (id: string) => securityActiveFields.has(id),
            isGroupActive: () => false,
            activeFields: securityActiveFields,
          }
        : useMockActiveLogFields
          ? { ready: true, isFieldActive: vi.fn(() => true), isGroupActive: () => true, activeFields: new Set(['ottfb', 'pop', 'oip']) }
          : actual.useActiveLogFields(...args),
    useIsDataReady: (...args: Parameters<typeof actual.useIsDataReady>) =>
      mockIsDataReady !== null ? mockIsDataReady : actual.useIsDataReady(...args),
    useEffectiveServiceId: (...args: Parameters<typeof actual.useEffectiveServiceId>) =>
      useMockHighScaleBootstrap ? 'service-1' : actual.useEffectiveServiceId(...args),
    useBootstrapResolved: (...args: Parameters<typeof actual.useBootstrapResolved>) =>
      useMockHighScaleBootstrap ? true : actual.useBootstrapResolved(...args),
  }
})

vi.mock('@/hooks/useDashboardCards', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useDashboardCards')>()
  return {
    ...actual,
    useTimeRange: (...args: Parameters<typeof actual.useTimeRange>) =>
      useMockHighScaleBootstrap
        ? { startTime: '2026-09-11T00:00:00Z', endTime: '2026-09-11T01:00:00Z' }
        : actual.useTimeRange(...args),
  }
})

vi.mock('@/components/CostCalculator/CostCalculator', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/CostCalculator/CostCalculator')>()),
  CostCalculator: () => <div data-testid="cost-calculator" />,
}))

function resetDefaultStores() {
  useRealApiClient = false
  useMockAdminFetch = false
  useInteractiveDataTableMock = false
  useMockActiveLogFields = false
  useSecurityActiveFields = false
  mockIsDataReady = null
  useMockHighScaleBootstrap = false
  mockPathname = '/dashboard'
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true, services: [{ id: 'test-svc', name: 'Test' }] })
  useFilterStore.getState().resetAll()
  useFilterStore.setState({
    startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z',
    filters: [], isAutoRange: false, compareMode: false,
  })
}

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const WrappedReportLayout = (props: any) =>
    !reportLayoutChildArgs ? (
      <actual.ReportLayout {...props} />
    ) : (
      <div>
        <h1>{props.title}</h1>
        <div data-testid="header-actions">{props.headerActions}</div>
        {props.children(reportLayoutChildArgs)}
      </div>
    )
  return { ...actual, default: WrappedReportLayout, ReportLayout: WrappedReportLayout }
})

vi.mock('@/components/DataTable', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/DataTable')>()
  const interactive = await (await import('../helpers/page-smoke')).interactiveDataTableModuleMock(dataTableMock)
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    DataTable: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.DataTable(props)
        : useMockDataTable
          ? <div data-testid="data-table" />
          : <actual.DataTable {...props} />,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    FilterValueCell: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.FilterValueCell(props)
        : useMockDataTable
          ? <span>{props.display ?? props.filters?.[0]?.value}</span>
          : <actual.FilterValueCell {...props} />,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ColumnVisibilityDropdown: (props: any) =>
      useInteractiveDataTableMock
        ? interactive.ColumnVisibilityDropdown(props)
        : useMockDataTable
          ? null
          : <actual.ColumnVisibilityDropdown {...props} />,
  }
})

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock(plotlyMock))
vi.mock('@/components/AnalyticsCard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/AnalyticsCard')>()
  return {
    ...actual,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    AnalyticsCard: (props: any) =>
      !useMockAnalyticsCard ? (
        <actual.AnalyticsCard {...props} />
      ) : (
        <div data-testid="analytics-card">
          <h3>{props.title}</h3>
          {props.error ? <div role="alert">card error</div> : null}
          {props.children}
        </div>
      ),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    LazyMount: (props: any) => (useMockAnalyticsCard ? <>{props.children}</> : <actual.LazyMount {...props} />),
    TimeSeriesChart: () => <div data-testid="timeseries-chart">TimeSeriesChart</div>,
  }
})
vi.mock('@/components/Map/ChoroplethMap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Map/ChoroplethMap')>()),
  ChoroplethMap: () => <div data-testid="choropleth-map">ChoroplethMap</div>,
}))
vi.mock('@/components/Map/NetworkMap', () => ({ NetworkMap: () => <div data-testid="network-map" /> }))
vi.mock('@/components/Map/ShieldingMap', () => ({ ShieldingMap: () => <div data-testid="shielding-map" /> }))

const queryClient = createTestQueryClient({ queries: { retry: false, staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>


type SecurityData = components['schemas']['SecurityAggregatesResponse']
type BotsData = NonNullable<React.ComponentProps<typeof BotsSection>['data']>
type HeaderData = NonNullable<React.ComponentProps<typeof HeaderAnomaliesSection>['data']>

describe('SecurityClient, BotsSection, HeaderAnomaliesSection, NetworkSection & ProxyWatchdogSection', () => {
  beforeEach(() => {
    resetDefaultStores()
    mockPathname = '/security'
    useInteractiveDataTableMock = true
    useSecurityActiveFields = true
    useMockAnalyticsCard = false
    reportLayoutChildArgs = {
      data: { security_signals: [], top_bots: [], ngwaf_bots: [] },
      isLoading: false,
      isFetching: false,
      intervalButtons: null,
      bucketSeconds: 3600,
    } as unknown as ReportLayoutChildArgs
    queryClient.clear()
    plotlyMock.mockClear()
    dataTableMock.mockClear()
    securityActiveFields.clear()
    errorSpy = spyOnConsoleError()
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

  test('security page mounts and renders title and tab navigation', async () => {
    renderWithQuery(<SecurityClient />, { queryClient })

    expect(screen.getByText('Security')).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /waf activity/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /threat intelligence/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /vpn & proxy watchdog/i })).toBeInTheDocument()

    await waitFor(() => {
      expect(
        screen.getByText('Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)'),
      ).toBeInTheDocument()
    })

    expect(errorSpy).not.toHaveBeenCalled()
  })

  test('BotsSection renders Bot & Crawler Value vs. Cost table with bandwidth and referral metrics', async () => {
    renderWithQuery(
      <BotsSection
        data={{
          _is_cached: false,
          tls_fingerprints: [],
          fingerprint_coverage: {},
          req_size_dist: [],
          top_ips_header: [],
          ipv6_adoption: [],
          proxy_dist: [],
          conn_reuse_dist: [],
          verified_bots_ts: [],
          ngwaf_configured: false,
          ngwaf_verified_bots: [],
          ngwaf_verified_bots_ts: [],
          wellknown_bots: [],
          bot_efficiency: [
            {
              bot_name: 'Anthropic Crawler Search',
              category: 'AI Crawler',
              request_count: 1500,
              resp_bytes: 52_428_800,
              bandwidth_pct: 32.5,
              referrals: 0,
              requests_per_referral: 1500,
              bytes_per_referral: 52_428_800,
            },
            {
              bot_name: 'Google Crawler',
              category: 'Search Engine Crawler',
              request_count: 800,
              resp_bytes: 10_485_760,
              bandwidth_pct: 6.5,
              referrals: 400,
              requests_per_referral: 2.0,
              bytes_per_referral: 26_214.4,
            },
          ],
          bots: [],
          ngwaf_bots: [],
          active_proxies_count: 0,
          tunnel_requests_count: 0,
          distance_mismatches_count: 0,
          traffic_quality: [],
          suspicious_isps: [],
          active_clients: [],
        }}
        bucketSeconds={3600}
        isLoading={false}
        isFetching={false}
        error={null}
        intervalButtons={null}
        timezone="UTC"
        commonTimeLayout={{}}
        getFieldLabel={(id: string) => id}
        ngwafBotVisibility={{}}
        setNgwafBotVisibility={vi.fn()}
        onNgwafBotVisChange={vi.fn()}
        botVisibility={{}}
        setBotVisibility={vi.fn()}
        onBotVisChange={vi.fn()}
        botEfficiencyVisibility={{}}
        setBotEfficiencyVisibility={vi.fn()}
        onBotEfficiencyVisChange={vi.fn()}
        fingerprintVisibility={{}}
        setFingerprintVisibility={vi.fn()}
        onFingerprintVisChange={vi.fn()}
      />,
      { queryClient },
    )

    expect(
      screen.getByText('Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)'),
    ).toBeInTheDocument()
    expect(screen.getByText('Anthropic Crawler Search')).toBeInTheDocument()
    expect(screen.getByText('Google Crawler')).toBeInTheDocument()
    const efficiencyRows = dataTableMock.mock.calls.at(-1)?.[0].data as Array<{ category?: string }> | undefined
    expect(efficiencyRows?.map((r) => r.category)).toEqual(['AI Crawler', 'Search Engine Crawler'])
  })

  function baseProps(overrides: Partial<React.ComponentProps<typeof BotsSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      intervalButtons: <div data-testid="interval-buttons" />,
      bucketSeconds: 3600,
      timezone: 'UTC',
      commonTimeLayout: {},
      getFieldLabel: (id: string) => id,
      ngwafBotVisibility: {},
      setNgwafBotVisibility: vi.fn(),
      onNgwafBotVisChange: vi.fn(),
      botVisibility: {},
      setBotVisibility: vi.fn(),
      onBotVisChange: vi.fn(),
      fingerprintVisibility: {},
      setFingerprintVisibility: vi.fn(),
      onFingerprintVisChange: vi.fn(),
      ...overrides,
    }
  }

  describe('BotsSection', () => {
    it('shows loading skeleton (AnalyticsCard overlay) when data is undefined and isLoading=true', () => {
      render(<BotsSection {...baseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading NGWAF verified bots...')).toBeInTheDocument()
      expect(screen.getByText('Loading NGWAF bot names...')).toBeInTheDocument()
      expect(screen.getByText('Loading well-known bots...')).toBeInTheDocument()
      expect(screen.getByText('Loading TLS fingerprints...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<BotsSection {...baseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\], .\\[contain-intrinsic-size\\:300px\\]')
      expect(containedElements.length).toBeGreaterThanOrEqual(4)
    })

    it('renders empty-state messages when arrays are empty', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
          })}
        />,
      )
      expect(screen.getAllByText(/no ngwaf bot detections/i).length).toBe(2)
      expect(screen.getAllByTestId('data-table-0').length).toBeGreaterThanOrEqual(3)
    })

    it('hides NGWAF cards entirely when ngwaf_configured=false', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: false,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
          })}
        />,
      )
      expect(screen.queryByText(/Verified Bots \(NGWAF\)/i)).not.toBeInTheDocument()
      expect(screen.queryByText(/Verified Bot Names \(NGWAF\)/i)).not.toBeInTheDocument()
    })

    it('routes the TLS-fingerprint empty copy on field-active state', () => {
      const emptyData = partial<BotsData>({
        ngwaf_configured: true,
        ngwaf_verified_bots_ts: [],
        ngwaf_verified_bots: [],
        wellknown_bots: [],
        tls_fingerprints: [],
      })

      const { unmount } = render(<BotsSection {...baseProps({ data: emptyData })} />)
      expect(
        screen.getByText(
          'Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.',
        ),
      ).toBeInTheDocument()
      unmount()
      dataTableMock.mockClear()

      securityActiveFields.add('ja3')
      render(<BotsSection {...baseProps({ data: emptyData })} />)
      expect(screen.getByText('No TLS fingerprints in this time range.')).toBeInTheDocument()
      expect(
        screen.queryByText(/Requires Security: TLS Fingerprinting/),
      ).not.toBeInTheDocument()
    })

    it('passes populated rows through to DataTable for each section', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [
                { time: '2026-01-01T00:00:00Z', bot_name: 'OpenAI', count: 12 },
                { time: '2026-01-01T01:00:00Z', bot_name: 'OpenAI', count: 8 },
                { time: '2026-01-01T01:00:00Z', bot_name: 'Anthropic', count: 3 },
              ],
              ngwaf_verified_bots: [
                { bot_name: 'OpenAI', category: 'search-engine', request_count: 20 },
              ],
              wellknown_bots: [
                { id: 'gb', name: 'Googlebot', category: 'search-engine', request_count: 100, verified_count: 90, impersonator_count: 5, unverified_count: 3, pending_count: 2 },
              ],
              tls_fingerprints: [{ fingerprint: 'sha-abc', ip_count: 4, request_count: 50 }],
              bot_efficiency: [
                {
                  bot_name: 'Commoncrawl Crawler',
                  category: 'ai-scraper',
                  request_count: 40,
                  resp_bytes: 80000,
                  bandwidth_pct: 12.5,
                  referrals: 0,
                  requests_per_referral: 40,
                  bytes_per_referral: 80000,
                },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalled()
      const traces = plotlyMock.mock.calls[0]![0].data
      expect(Array.isArray(traces)).toBe(true)
      expect(traces.length).toBe(2)
      const names = traces.map((t) => t.name).sort()
      expect(names).toEqual(['Anthropic', 'OpenAI'])

      const tableProps = dataTableMock.mock.calls.map((c) => c[0])
      expect(tableProps).toHaveLength(4)
      expect(tableProps[0]?.data).toHaveLength(1)
      expect(tableProps[1]?.data).toHaveLength(1)
      expect(tableProps[2]?.data).toHaveLength(1)
      expect(tableProps[3]?.data).toHaveLength(1)

      expect(screen.getByText('Googlebot')).toBeInTheDocument()
      expect(screen.getByText('OpenAI')).toBeInTheDocument()
    })

    it('renders low-coverage hint only when fingerprint_coverage < 1% for that field', () => {
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
              fingerprint_coverage: { tls_ciphers_sha: 0.005 },
            }),
          })}
        />,
      )
      expect(screen.getByText(/low coverage/i)).toBeInTheDocument()
      expect(screen.getByText(/TLS fingerprints are only captured/i)).toBeInTheDocument()
    })

    it('invokes the column-visibility change callback when the user toggles a column off', async () => {
      const onNgwafBotVisChange = vi.fn()
      const user = userEvent.setup()
      render(
        <BotsSection
          {...baseProps({
            data: partial<BotsData>({
              ngwaf_configured: true,
              ngwaf_verified_bots_ts: [],
              ngwaf_verified_bots: [],
              wellknown_bots: [],
              tls_fingerprints: [],
            }),
            onNgwafBotVisChange,
          })}
        />,
      )

      const buttons = screen.getAllByTestId('col-vis')
      expect(buttons.length).toBeGreaterThanOrEqual(3)
      await user.click(buttons[0]!)
      expect(onNgwafBotVisChange).toHaveBeenCalledTimes(1)
      expect(onNgwafBotVisChange).toHaveBeenCalledWith('bot_name', false)
    })
  })

  function headerBaseProps(overrides: Partial<React.ComponentProps<typeof HeaderAnomaliesSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      getFieldLabel: (id: string) => id,
      topIpVisibility: {},
      setTopIpVisibility: vi.fn(),
      onTopIpVisChange: vi.fn(),
      ...overrides,
    }
  }

  describe('HeaderAnomaliesSection', () => {
    it('shows loading skeleton overlay across both cards when isLoading', () => {
      render(<HeaderAnomaliesSection {...headerBaseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading request header size distribution...')).toBeInTheDocument()
      expect(screen.getByText('Loading oversized request headers...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<HeaderAnomaliesSection {...headerBaseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\], .\\[contain-intrinsic-size\\:300px\\]')
      expect(containedElements.length).toBe(2)
    })

    it('renders the not-enabled "Requires Group A" copy when req_header_bytes is inactive', () => {
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
          })}
        />,
      )
      expect(screen.getByText(/no data available/i)).toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
      expect(screen.getByTestId('empty')).toBeInTheDocument()
      expect(
        screen.getByText(
          'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.',
        ),
      ).toBeInTheDocument()
    })

    it('renders the neutral "no data yet" copy when req_header_bytes IS active', () => {
      securityActiveFields.add('req_header_bytes')
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
          })}
        />,
      )
      expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(2)
      expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
    })

    it('passes populated rows to Plotly and DataTable', () => {
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({
              req_size_dist: [
                { bucket: '0-512', count: 100 },
                { bucket: '512-1k', count: 80 },
                { bucket: '8k+', count: 5 },
              ],
              top_ips_header: [
                { ip: '203.0.113.5', max_header: 16384 },
                { ip: '198.51.100.7', max_header: 12000 },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalledTimes(1)
      const trace = plotlyMock.mock.calls[0]![0].data[0]
      expect(trace?.type).toBe('bar')
      expect(trace?.x).toEqual(['0-512', '512-1k', '8k+'])
      expect(trace?.y).toEqual([100, 80, 5])

      expect(dataTableMock).toHaveBeenCalledTimes(1)
      expect(dataTableMock.mock.calls[0]![0].data).toHaveLength(2)
      expect(screen.getByText('203.0.113.5')).toBeInTheDocument()
      expect(screen.getByText('198.51.100.7')).toBeInTheDocument()
    })

    it('forwards column-visibility toggles to the parent callback', async () => {
      const onTopIpVisChange = vi.fn()
      const user = userEvent.setup()
      render(
        <HeaderAnomaliesSection
          {...headerBaseProps({
            data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
            onTopIpVisChange,
          })}
        />,
      )
      const btn = screen.getByTestId('col-vis')
      await user.click(btn)
      expect(onTopIpVisChange).toHaveBeenCalledWith('ip', false)
    })
  })

  function securityData(overrides: Partial<SecurityData> = {}): SecurityData {
    return {
      _is_cached: false,
      tls_fingerprints: [],
      fingerprint_coverage: {},
      req_size_dist: [],
      top_ips_header: [],
      ipv6_adoption: [],
      proxy_dist: [],
      conn_reuse_dist: [],
      verified_bots_ts: [],
      ngwaf_configured: false,
      ngwaf_verified_bots: [],
      ngwaf_verified_bots_ts: [],
      wellknown_bots: [],
      bot_efficiency: [],
      bots: [],
      ngwaf_bots: [],
      active_proxies_count: 10,
      tunnel_requests_count: 500,
      distance_mismatches_count: 2,
      traffic_quality: [],
      suspicious_isps: [],
      active_clients: [],
      ...overrides,
    }
  }

  function networkBaseProps(overrides: Partial<React.ComponentProps<typeof NetworkSection>> = {}) {
    return {
      data: undefined,
      isLoading: false,
      isFetching: false,
      error: null,
      timezone: 'UTC',
      commonTimeLayout: {},
      ...overrides,
    }
  }

  describe('NetworkSection', () => {
    it('shows loading skeleton overlay across all three cards when isLoading', () => {
      render(<NetworkSection {...networkBaseProps({ isLoading: true })} />)
      expect(screen.getByText('Loading IPv6 adoption data...')).toBeInTheDocument()
      expect(screen.getByText('Loading proxy/anonymizer breakdown...')).toBeInTheDocument()
      expect(screen.getByText('Loading connection reuse distribution...')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on all cards', () => {
      const { container } = render(<NetworkSection {...networkBaseProps()} />)
      const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\]')
      expect(containedElements.length).toBe(3)
    })

    it('renders "No data available" empty card for each chart when arrays are empty', () => {
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      expect(screen.getAllByText(/no data available/i).length).toBe(3)
      expect(screen.getAllByText(/Infrastructure \(Group C\) fields/i).length).toBe(2)
      expect(screen.getByText(/Security: Proxy & Anonymization \(Group I\)/i)).toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
    })

    it('shows a neutral "no data yet" state (no Requires hint) when the field group IS enabled', () => {
      securityActiveFields.add('is_ipv6')
      securityActiveFields.add('p_type')
      securityActiveFields.add('conn_requests')
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(3)
      expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
      expect(plotlyMock).not.toHaveBeenCalled()
    })

    it('forwards populated data into Plotly traces (IPv6 line, proxy pie, conn-reuse bar)', () => {
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({
              ipv6_adoption: [
                { time: '2026-01-01T00:00:00Z', pct: 42 },
                { time: '2026-01-01T01:00:00Z', pct: 51 },
              ],
              proxy_dist: [
                { type: 'residential', count: 800 },
                { type: 'hosting', count: 150 },
                { type: 'tor', count: 5 },
              ],
              conn_reuse_dist: [
                { bucket: '1', count: 300 },
                { bucket: '2-5', count: 1200 },
                { bucket: '6-10', count: 900 },
              ],
            }),
          })}
        />,
      )

      expect(plotlyMock).toHaveBeenCalledTimes(3)
      const [ipv6Call, proxyCall, reuseCall] = plotlyMock.mock.calls.map((c) => c[0])

      expect(ipv6Call.data[0]?.name).toBe('IPv6 %')
      expect(ipv6Call.data[0]?.y).toEqual([42, 51])

      expect(proxyCall.data[0]?.type).toBe('pie')
      expect(proxyCall.data[0]?.labels).toEqual(['residential', 'hosting', 'tor'])
      expect(proxyCall.data[0]?.values).toEqual([800, 150, 5])

      expect(reuseCall.data[0]?.type).toBe('bar')
      expect(reuseCall.data[0]?.x).toEqual(['1', '2-5', '6-10'])
      expect(reuseCall.data[0]?.y).toEqual([300, 1200, 900])
    })

    it('opens the help dialog when the user clicks an AnalyticsCard help button', async () => {
      const user = userEvent.setup()
      render(
        <NetworkSection
          {...networkBaseProps({
            data: securityData({}),
          })}
        />,
      )
      const helpButtons = screen.getAllByRole('button', { name: /about this chart/i })
      expect(helpButtons.length).toBe(3)
      await user.click(helpButtons[0]!)

      const dialog = await screen.findByRole('dialog')
      expect(dialog).toBeInTheDocument()
      expect(within(dialog).getByText(/Tracks the percentage of requests/i)).toBeInTheDocument()
    })
  })

  describe('ProxyWatchdogSection', () => {
    it('renders stats and all card headers correctly', () => {
      const mockData = securityData()
      render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
      expect(screen.getByText('Active VPN & Proxy Users')).toBeInTheDocument()
      expect(screen.getByText('10')).toBeInTheDocument()
      expect(screen.getByText('Tunnel Requests')).toBeInTheDocument()
      expect(screen.getByText('500')).toBeInTheDocument()
      expect(screen.getByText('Impossible Distance Mismatches')).toBeInTheDocument()
      expect(screen.getByText('2')).toBeInTheDocument()

      expect(screen.getByText('Traffic Quality Breakdown')).toBeInTheDocument()
      expect(screen.getByText('Suspicious ISPs & Providers')).toBeInTheDocument()
      expect(screen.getByText('VPN/Proxy Export Center')).toBeInTheDocument()
      expect(screen.getByText('Active Suspicious Clients')).toBeInTheDocument()
    })

    it('enforces zero-CLS layout reservations on stat cards and card containers', () => {
      const mockData = securityData()
      const { container } = render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:120px\\]').length).toBeGreaterThanOrEqual(3)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:320px\\]').length).toBeGreaterThanOrEqual(2)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:140px\\]').length).toBeGreaterThanOrEqual(1)
      expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:600px\\]').length).toBeGreaterThanOrEqual(1)
    })
  })
})
