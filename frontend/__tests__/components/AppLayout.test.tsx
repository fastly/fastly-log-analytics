/**
 * Consolidated tests for AppLayout, ReportShell, ChartIntervalButtons,
 * NoServiceSelected, UpdatingBadge, StoreHydrator, and ReloadLoopGuard.
 *
 * @vitest-environment jsdom
 */
import { stubSystemTimeZone, stubResizeObserver, stubPointerCaptureAndScroll, renderWithQuery, renderWithQueryClient, partial } from '@/__tests__/helpers/page-smoke'
import { render, screen, act, cleanup, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { axe } from 'vitest-axe'
import { Server } from 'lucide-react'
import { AppLayout } from '@/components/AppLayout'
import { ReportShell, ChartIntervalButtons, NoServiceSelected } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/AnalyticsCard'
import { StoreHydrator, ReloadLoopGuard } from '@/components/QueryProvider'
import { InsightCard, InsightItemRow, ScriptedTrafficModal, type ScriptedTrafficData } from '@/components/Insights/InsightCard'
import { CacheCollapseModal } from '@/components/Insights/InsightDetailModal'
import { getSecurityContent } from '@/components/Insights/InsightHelpModal/sections/security'
import { getPerformanceContent, getTrafficContent } from '@/components/Insights/InsightHelpModal'
import { INTERVALS } from '@/lib/utils'
import { useServiceStore, useTimezoneStore, useDebugStore } from '@/stores/filterStore'
import { useBootstrap } from '@/hooks/useBootstrap'
import type { BootstrapResponse, InsightCardData } from '@/types/api'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { usePathname, useRouter } from 'next/navigation'
import React, { isValidElement } from 'react'

const { realInsightSubcomponentsRef } = vi.hoisted(() => ({ realInsightSubcomponentsRef: { value: false } }))

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('@/components/AnalyticsCard', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/AnalyticsCard')>()),
  TimeSeriesChart: () => <div data-testid="timeseries" />,
}))
vi.mock('@/components/Insights/InsightHelpModal', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Insights/InsightHelpModal')>()),
  InsightHelpModal: ({ isOpen }: { isOpen: boolean }) => (isOpen ? <div data-testid="help-modal-open" /> : null),
}))
vi.mock('@/components/Insights/InsightDetailModal', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Insights/InsightDetailModal')>()
  return {
    ...actual,
    ImpossibleDistanceModal: ({ isOpen }: { isOpen: boolean }) => (isOpen ? <div data-testid="distance-modal-open" /> : null),
    CacheCollapseModal: (props: React.ComponentProps<typeof actual.CacheCollapseModal>) =>
      realInsightSubcomponentsRef.value ? <actual.CacheCollapseModal {...props} /> : props.isOpen ? <div data-testid="collapse-modal-open" /> : null,
  }
})

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  if (typeof Element !== 'undefined' && !Element.prototype.getAnimations) {
    Element.prototype.getAnimations = () => []
  }
})

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

let mockActiveServiceId: string | null = 'svc-1'
let mockIsDataReady = true
let mockBootstrapResolved = true

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: vi.fn(),
  useUrlServiceSync: vi.fn(),
  useIsAnalyst: vi.fn(),
  useMaskIps: () => false,
  useEnforceMaskedFilters: vi.fn(),
  useEffectiveServiceId: () => mockActiveServiceId,
  useIsDataReady: () => mockIsDataReady,
  useBootstrapResolved: () => mockBootstrapResolved,
}))

vi.mock('@/components/ReportLayout', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/ReportLayout')>()),
  DashboardSkeleton: () => <div data-testid="skeleton" />,
}))

vi.mock('@/components/FilterBar/FilterBar', () => ({ FilterBar: () => <div>FilterBar</div> }))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()),
  ServiceSwitcher: () => <div>ServiceSwitcher</div>,
  SyncStatusBadge: () => <div>SyncStatusBadge</div>,
}))
vi.mock('@/components/DebugPanel', () => ({ DebugPanel: () => <div>DebugPanel</div> }))
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

type BootstrapData = BootstrapResponse

afterEach(() => {
  realInsightSubcomponentsRef.value = false
  cleanup()
  vi.unstubAllGlobals()
})

beforeEach(async () => {
  vi.clearAllMocks()
  vi.mocked(usePathname).mockReturnValue('/dashboard')
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: partial<BootstrapData>({ services: [{ service_id: 'test-svc', name: 'Test Service' }] }),
    isSuccess: true,
    isLoading: false,
  }))
  const { useIsAnalyst } = await import('@/hooks/useBootstrap')
  vi.mocked(useIsAnalyst).mockImplementation(() => {
    const { activeServiceId, services } = useServiceStore.getState()
    return services.find((s) => s.id === activeServiceId)?.accessLevel === 'read_only'
  })
  act(() => {
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      services: [{ id: 'test-svc', name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true,
    })
  })
})

test('renders AppLayout with standard navigation', () => {
  renderWithQueryClient(<AppLayout><div>Content</div></AppLayout>)
  expect(screen.getAllByText('Dashboard').length).toBeGreaterThan(0)
  expect(screen.getByText('Usage & Cost')).toBeInTheDocument()
})

test('hides restricted items for analysts', () => {
  act(() => {
    useServiceStore.setState({
      activeServiceId: 'analyst-svc',
      services: [{ id: 'analyst-svc', name: 'Analyst Service', accessLevel: 'read_only' }],
    })
  })
  renderWithQueryClient(<AppLayout><div>Content</div></AppLayout>)
  expect(screen.getAllByText('Dashboard').length).toBeGreaterThan(0)
  expect(screen.queryByText('Usage & Cost')).not.toBeInTheDocument()
})

test('renders analyst watermark when bootstrap reports remote analyst', async () => {
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: partial<BootstrapData>({
      services: [{ service_id: 'test-svc', name: 'Test Service' }],
      settings: { is_remote_analyst: true, analyst_email: 'jane@example.com', analyst_name: 'Jane Doe' },
    }),
    isSuccess: true,
    isLoading: false,
  }))
  const { useIsAnalyst } = await import('@/hooks/useBootstrap')
  vi.mocked(useIsAnalyst).mockReturnValue(true)

  renderWithQueryClient(<AppLayout><div>Content</div></AppLayout>)
  const watermark = screen.getByTestId('analyst-watermark')
  expect(watermark).toBeInTheDocument()
  expect(watermark.getAttribute('data-analyst-email')).toBe('jane@example.com')
  expect(watermark).toHaveTextContent('Jane Doe')
})

test('does not render watermark for non-analyst users', () => {
  renderWithQueryClient(<AppLayout><div>Content</div></AppLayout>)
  expect(screen.queryByTestId('analyst-watermark')).not.toBeInTheDocument()
})

test('keeps anonymous visitors on sign-in before the analyst role hydrates', async () => {
  vi.mocked(usePathname).mockReturnValue('/share-login')
  const replace = vi.fn()
  vi.mocked(useRouter).mockReturnValue({ ...useRouter(), replace })
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: partial<BootstrapData>({ services: [], settings: { is_remote_analyst: true, needs_login: true } }),
    isSuccess: true,
    isLoading: false,
  }))
  const { useIsAnalyst } = await import('@/hooks/useBootstrap')
  vi.mocked(useIsAnalyst).mockReturnValue(false)

  renderWithQueryClient(<AppLayout><div>Analyst sign-in</div></AppLayout>)
  expect(screen.getByText('Analyst sign-in')).toBeInTheDocument()
  expect(replace).not.toHaveBeenCalled()
  expect(screen.queryByText('No services configured yet. Redirecting to the admin setup page.')).not.toBeInTheDocument()
})

describe('ReportShell', () => {
  beforeEach(() => {
    mockActiveServiceId = 'svc-1'
    mockIsDataReady = true
    mockBootstrapResolved = true
  })

  it('renders title + children once the service is selected and data is ready', () => {
    render(<ReportShell title="My Report" description="A descriptive line" icon={Server}><div data-testid="content">body</div></ReportShell>)
    expect(screen.getByText('My Report')).toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })

  it('falls back to NoServiceSelected when no active service id is set (requireService default)', () => {
    mockActiveServiceId = null
    render(<ReportShell title="Security" icon={Server}><div data-testid="content">body</div></ReportShell>)
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText(/select a service/i)).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
  })

  it('shows the dashboard skeleton when isDataReady is false and honours isReadyOverride', () => {
    mockIsDataReady = false
    const { unmount } = render(<ReportShell title="Logs" icon={Server}><div data-testid="content">body</div></ReportShell>)
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
    unmount()

    render(<ReportShell title="Logs" icon={Server} isReadyOverride><div data-testid="content">body</div></ReportShell>)
    expect(screen.getByTestId('content')).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })

  it('flash defense: bootstrap unresolved AND no active service → skeleton, not NoServiceSelected', () => {
    mockActiveServiceId = null
    mockBootstrapResolved = false
    mockIsDataReady = false
    render(<ReportShell title="Dashboard" icon={Server}><div data-testid="content">body</div></ReportShell>)
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
  })

  it('requireService=false bypasses the NoServiceSelected gate', () => {
    mockActiveServiceId = null
    render(<ReportShell title="Insights" icon={Server} requireService={false}><div data-testid="content">body</div></ReportShell>)
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })
})

const ALL_VALID = new Set(INTERVALS.map((i) => i.value))

describe('ChartIntervalButtons', () => {
  it('renders one button per INTERVALS entry and marks effective interval with aria-pressed=true', () => {
    render(<ChartIntervalButtons effectiveInterval="1 hour" validIntervals={ALL_VALID} onIntervalChange={() => {}} />)
    for (const i of INTERVALS) {
      expect(screen.getByRole('button', { name: `Chart bucket size: ${i.label}` })).toBeInTheDocument()
    }
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1h' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1m' })).toHaveAttribute('aria-pressed', 'false')
  })

  it('fires onIntervalChange for valid buttons and disables invalid ones', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    const { unmount } = render(<ChartIntervalButtons effectiveInterval="1 minute" validIntervals={ALL_VALID} onIntervalChange={onChange} />)
    await user.click(screen.getByRole('button', { name: 'Chart bucket size: 1h' }))
    expect(onChange).toHaveBeenCalledWith('1 hour')
    unmount()

    const onChangeDisabled = vi.fn()
    render(<ChartIntervalButtons effectiveInterval="1 minute" validIntervals={new Set(['1 minute'])} onIntervalChange={onChangeDisabled} />)
    const hourBtn = screen.getByRole('button', { name: 'Chart bucket size: 1h' })
    expect(hourBtn).toBeDisabled()
    await user.click(hourBtn)
    expect(onChangeDisabled).not.toHaveBeenCalled()
  })
})

describe('NoServiceSelected', () => {
  it('renders the provided message, default/explicit title, and lucide icon', () => {
    const { container, unmount } = render(<NoServiceSelected icon={Server} message="Pick one from the picker above" />)
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText('Pick one from the picker above')).toBeInTheDocument()
    expect(container.querySelector('svg')?.classList.contains('text-muted-foreground')).toBe(true)
    unmount()

    render(<NoServiceSelected icon={Server} message="msg" title="Nothing here yet" />)
    expect(screen.getByRole('heading', { name: 'Nothing here yet' })).toBeInTheDocument()
  })
})

describe('UpdatingBadge', () => {
  it('renders the "Updating" label text and pulses the decorative dot only', () => {
    const { container } = render(<UpdatingBadge />)
    expect(screen.getByText(/updating/i)).toBeInTheDocument()
    expect(container.children).toHaveLength(1)
    const root = container.firstChild as HTMLElement
    expect(root.className).not.toMatch(/animate-pulse/)
    expect(root.querySelector('span')!.className).toMatch(/animate-pulse/)
  })
})

describe('StoreHydrator', () => {
  beforeEach(() => {
    window.localStorage.clear()
    sessionStorage.clear()
    act(() => {
      useServiceStore.setState({ activeServiceId: null, services: [], isInitialized: false })
      useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
      useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
    })
  })

  it('renders nothing (null)', () => {
    const { container } = render(<StoreHydrator />)
    expect(container.innerHTML).toBe('')
  })

  it('rehydrates serviceStore from localStorage after mount', async () => {
    window.localStorage.setItem(
      'service-storage',
      JSON.stringify({ state: { activeServiceId: 'svc-persisted', services: [{ id: 'svc-persisted', name: 'Persisted' }], isInitialized: true }, version: 0 }),
    )
    render(<StoreHydrator />)
    await waitFor(() => expect(useServiceStore.getState().activeServiceId).toBe('svc-persisted'))
  })

  it('rehydrates debugStore from localStorage after mount', async () => {
    window.localStorage.setItem('fastly-debug-settings', JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }))
    render(<StoreHydrator />)
    await waitFor(() => {
      expect(useDebugStore.getState().enabled).toBe(true)
      expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    })
  })

  it.each([
    ['no persisted zone (default system)', null, 'Asia/Tokyo', 'system', 'Asia/Tokyo'],
    ['persisted system-mode blob', { state: { timezone: 'UTC', mode: 'system' }, version: 1 }, 'Europe/Berlin', 'system', 'Europe/Berlin'],
    ['persisted manual-mode timezone', { state: { timezone: 'America/New_York', mode: 'manual' }, version: 1 }, 'Asia/Tokyo', 'manual', 'America/New_York'],
    ['pre-v1 persisted blob (no mode)', { state: { timezone: 'America/New_York' }, version: 0 }, 'Asia/Tokyo', 'manual', 'America/New_York'],
  ] as const)('hydrates timezone correctly for %s', async (_label, persisted, sysZone, expectedMode, expectedTz) => {
    if (persisted) window.localStorage.setItem('timezone-storage', JSON.stringify(persisted))
    stubSystemTimeZone(sysZone)
    render(<StoreHydrator />)
    await waitFor(() => {
      expect(useTimezoneStore.getState().mode).toBe(expectedMode)
      expect(useTimezoneStore.getState().timezone).toBe(expectedTz)
    })
  })
})

const CHILD = 'app-subtree-rendered'
const loadGuard = () => render(<ReloadLoopGuard><div>{CHILD}</div></ReloadLoopGuard>)
function tripGuard(times = 6) {
  let last: ReturnType<typeof loadGuard> | null = null
  for (let i = 0; i < times; i++) {
    last?.unmount()
    last = loadGuard()
  }
  return last
}

describe('ReloadLoopGuard', () => {
  beforeEach(() => {
    window.localStorage.clear()
    sessionStorage.clear()
  })

  test('renders the app subtree under the reload threshold', () => {
    for (let i = 0; i < 5; i++) {
      const { unmount } = loadGuard()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
      unmount()
    }
  })

  test('trips after 6 same-path loads in the window — shows recovery prompt, hides app', () => {
    tripGuard()
    expect(screen.queryByText(CHILD)).not.toBeInTheDocument()
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh now/i })).toBeInTheDocument()
  })

  test('"Continue anyway" dismisses the prompt and renders the app', () => {
    tripGuard()
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /continue anyway/i }))
    expect(screen.getByText(CHILD)).toBeInTheDocument()
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
  })

  test('"Refresh now" clears history and reloads', () => {
    const reloadSpy = vi.spyOn(window.location, 'reload').mockImplementation(() => {})
    tripGuard()
    fireEvent.click(screen.getByRole('button', { name: /refresh now/i }))
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
    expect(reloadSpy).toHaveBeenCalledTimes(1)
    reloadSpy.mockRestore()
  })

  test('does not trip when loads are spread across different paths', () => {
    const spy = vi.spyOn(window.location, 'pathname', 'get')
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 8; i++) {
      spy.mockReturnValue(`/page-${i}`)
      last?.unmount()
      last = loadGuard()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
    }
    spy.mockRestore()
  })
})

type ErrorSpikesCard = Extract<InsightCardData, { id: 'error_spikes' }>

const errorItem = (label: string, current_val: number, severity: string): ErrorSpikesCard['items'][number] => ({
  label,
  current_val,
  severity,
  baseline_label: 'baseline',
  meta: { requests: 100, errors: 40, filters: { url: label } },
})

const _BASE_INSIGHT: ErrorSpikesCard = {
  id: 'error_spikes',
  title: 'Error Spikes',
  description: 'URLs with abnormally elevated 5xx error rates',
  severity: 'critical',
  summary: '3 URLs with elevated server error rates',
  items: [
    errorItem('/api/users', 0.45, 'critical'),
    errorItem('/api/orders', 0.35, 'warning'),
    errorItem('/api/auth', 0.25, 'warning'),
  ],
}

const manyItems = () => Array.from({ length: 8 }, (_, i) => errorItem(`/path/${i}`, 0.1, 'warning'))

describe('InsightCard', () => {
  it('renders title, summary, and severity badge', () => {
    renderWithQueryClient(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('3 URLs with elevated server error rates')).toBeTruthy()
    expect(screen.getByText(/^critical$/i)).toBeTruthy()
  })

  it('renders one row per item up to 5', () => {
    renderWithQueryClient(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(3)
  })

  it('shows "Show N more" button when items > 5', () => {
    renderWithQueryClient(<InsightCard insight={{ ..._BASE_INSIGHT, items: manyItems() }} windowHours="24" baselineHours="168" />)
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(5)
    expect(screen.getByText(/Show 3 more/i)).toBeTruthy()
  })

  it('does not show "Show more" when items <= 5', () => {
    renderWithQueryClient(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.queryByText(/Show \d+ more/i)).toBeNull()
  })

  it('renders the help modal when the help button is clicked', async () => {
    const user = userEvent.setup()
    renderWithQueryClient(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.queryByTestId('help-modal-open')).toBeNull()
    await user.click(screen.getByTitle('How this works'))
    expect(await screen.findByTestId('help-modal-open')).toBeTruthy()
  })

  it('renders the data modal when "Show N more" is clicked', async () => {
    const user = userEvent.setup()
    renderWithQueryClient(<InsightCard insight={{ ..._BASE_INSIGHT, items: manyItems() }} windowHours="24" baselineHours="168" />)
    expect(screen.queryByTestId('data-modal-open')).toBeNull()
    await user.click(screen.getByRole('button', { name: /show 3 more/i }))
    expect(screen.getByTestId('data-modal-open')).toBeTruthy()
  })

  it('falls back gracefully on unknown severity (no crash, default icon)', () => {
    renderWithQueryClient(<InsightCard insight={{ ..._BASE_INSIGHT, severity: 'urgent', items: [] }} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
  })

  it('renders empty-items state without crashing', () => {
    renderWithQueryClient(<InsightCard insight={{ ..._BASE_INSIGHT, severity: 'clean', summary: 'No error spikes detected', items: [] }} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('No error spikes detected')).toBeTruthy()
    expect(screen.queryAllByTestId('insight-item-row')).toHaveLength(0)
  })

  it('has no axe-detectable a11y violations', async () => {
    const { container } = renderWithQueryClient(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(await axe(container, { rules: { 'color-contrast': { enabled: false } } })).toHaveNoViolations()
  })

  it('multiple InsightCards with distinct ids render distinct elements', () => {
    const a: InsightCardData = { ..._BASE_INSIGHT, title: 'Error Spikes' }
    const b: InsightCardData = { ..._BASE_INSIGHT, id: 'botnet_grouping', title: 'Botnet Grouping', items: [] }
    renderWithQueryClient(<><InsightCard insight={a} windowHours="24" baselineHours="168" /><InsightCard insight={b} windowHours="24" baselineHours="168" /></>)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('Botnet Grouping')).toBeTruthy()
  })
})

const PERFORMANCE_IDS = [
  'city_latency_regressions', 'asn_metro_performance', 'latency_regression', 'network_asn_health',
  'pop_latency_regression', 'metro_delivery_degradation', 'connection_type_mix', 'http3_fallback',
  'payload_compression_regression', 'timeout_split',
] as const

const SECURITY_IDS = [
  'impossible_distance', 'ua_monoculture', 'new_probe_urls', 'waf_signal_spikes', 'proxy_surge',
  'botnet_grouping', 'low_and_slow', 'credential_enumeration', 'content_discovery',
  'session_harvesting', 'repeated_patterns',
] as const

const ARMS_WITH_DIAGRAM = new Set<string>(['impossible_distance', 'repeated_patterns'])

const TRAFFIC_IDS = [
  'city_surges', 'new_city_traffic', 'new_country_traffic', 'asn_concentration',
  'referer_monoculture', 'method_drift', 'new_asn_traffic', 'asn_hosting_shift',
] as const

function assertInsightContent(getter: (id: string) => ReturnType<typeof getPerformanceContent>, id: string, checkUniqueFields = true) {
  const content = getter(id)
  expect(content).not.toBeNull()
  if (!content) return
  expect(typeof content.title).toBe('string')
  expect(content.title.length).toBeGreaterThan(0)
  expect(isValidElement(content.icon)).toBe(true)
  expect(Array.isArray(content.fields)).toBe(true)
  expect(content.fields.length).toBeGreaterThan(0)
  for (const f of content.fields) expect(typeof f).toBe('string')
  if (checkUniqueFields) expect(new Set(content.fields).size).toBe(content.fields.length)
  expect(content.description).toBeTruthy()
  const { container, unmount } = render(<>{content.description}</>)
  expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
  unmount()
}

describe('getPerformanceContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getPerformanceContent('nope_not_a_real_insight')).toBeNull()
    expect(getPerformanceContent('')).toBeNull()
  })

  test.each(PERFORMANCE_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    assertInsightContent(getPerformanceContent, id)
  })

  test('performance arms do not require a diagram and expose distinct titles', () => {
    for (const id of PERFORMANCE_IDS) expect(getPerformanceContent(id)?.diagram).toBeUndefined()
    const titles = PERFORMANCE_IDS.map((id) => getPerformanceContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })

  test('asn_metro_performance lists ASN/metro fields and network_asn_health surfaces kernel metrics', () => {
    const content = getPerformanceContent('asn_metro_performance')!
    expect(content.fields).toEqual(expect.arrayContaining(['asn', 'metro', 'tcp_rtt']))
    const health = getPerformanceContent('network_asn_health')!
    const { container, unmount } = render(<>{health.description}</>)
    expect(container.textContent).toMatch(/Packet Loss/i)
    expect(container.textContent).toMatch(/Jitter/i)
    unmount()
  })
})

describe('getSecurityContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getSecurityContent('nope_not_a_real_insight')).toBeNull()
    expect(getSecurityContent('')).toBeNull()
  })

  test.each(SECURITY_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    assertInsightContent(getSecurityContent, id)
  })

  test('impossible_distance and repeated_patterns include diagrams; only documented arms ship one', () => {
    const idist = getSecurityContent('impossible_distance')
    expect(idist?.diagram).toBeTruthy()
    const { container: c1, unmount: u1 } = render(<>{idist!.diagram}</>)
    expect(c1.textContent).toMatch(/Speed of Light/i)
    u1()

    const rpat = getSecurityContent('repeated_patterns')
    expect(rpat?.diagram).toBeTruthy()
    expect(rpat?.fields).toEqual(['ip', 'timestamp'])
    const { container: c2, unmount: u2 } = render(<>{rpat!.diagram}</>)
    expect(c2.textContent).toMatch(/Automated script/i)
    expect(c2.textContent).toMatch(/Human browsing/i)
    u2()

    for (const id of SECURITY_IDS) {
      const diagram = getSecurityContent(id)?.diagram
      if (ARMS_WITH_DIAGRAM.has(id)) expect(diagram).toBeTruthy()
      else expect(diagram).toBeUndefined()
    }
    const titles = SECURITY_IDS.map((id) => getSecurityContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })
})

describe('getTrafficContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getTrafficContent('nope_not_a_real_insight')).toBeNull()
    expect(getTrafficContent('')).toBeNull()
  })

  test.each(TRAFFIC_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    assertInsightContent(getTrafficContent, id, false)
  })

  test('traffic arms do not require a diagram and branch new_city_traffic vs new_country_traffic', () => {
    for (const id of TRAFFIC_IDS) expect(getTrafficContent(id)?.diagram).toBeUndefined()
    const city = getTrafficContent('new_city_traffic')!
    const country = getTrafficContent('new_country_traffic')!
    expect(city.title).toMatch(/City/i)
    expect(country.title).toMatch(/Country/i)
    expect(city.title).not.toBe(country.title)
    expect(city.fields).toContain('city')
    expect(country.fields).toContain('country')
    expect(getTrafficContent('city_surges')!.fields).toEqual(['city'])
    expect(getTrafficContent('asn_concentration')!.fields).toEqual(['asn'])
  })
})

const SCRIPTED_DATA: ScriptedTrafficData = {
  label: '203.0.113.x', score: 92, cv: 0.04, modal_frac: 0.94, mean_interval_s: 60,
  stddev_s: 1.2, mode_gap_s: 60, n_gaps: 1439, n_events: 1440, span_s: 86400, rps: 0.0167, distinct_ua: 3,
}

const REPEATED_CARD: Extract<InsightCardData, { id: 'repeated_patterns' }> = {
  id: 'repeated_patterns',
  title: 'Scripted Traffic Patterns',
  description: 'IPs sending requests on a highly regular cadence',
  severity: 'critical',
  summary: '1 anomalies detected',
  items: [{
    label: '203.0.113.x', current_val: 60, baseline_val: 1.2, baseline_label: 'jitter (σ)',
    unit: 's interval', severity: 'critical',
    meta: { ...SCRIPTED_DATA, filters: { ip: '203.0.113.x' } },
  }],
}

describe('InsightItemRow — Scripted Traffic trigger', () => {
  beforeEach(() => {
    realInsightSubcomponentsRef.value = true
    useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
  })

  it('shows the evidence trigger for repeated_patterns and maps meta on click', async () => {
    const user = userEvent.setup()
    const onScriptedTrafficClick = vi.fn()
    render(<InsightItemRow insight={REPEATED_CARD} index={0} onScriptedTrafficClick={onScriptedTrafficClick} />)
    await user.click(screen.getByRole('button', { name: /why 203\.0\.113\.x was flagged/i }))
    expect(onScriptedTrafficClick).toHaveBeenCalledTimes(1)
    const payload = onScriptedTrafficClick.mock.calls[0][0] as ScriptedTrafficData
    expect(payload).toMatchObject({ label: '203.0.113.x', score: 92, modal_frac: 0.94, mode_gap_s: 60 })
  })

  it('does NOT show the trigger for a different insight id', () => {
    render(
      <InsightItemRow
        insight={{
          id: 'error_spikes', title: 'Error Spikes', description: '', severity: 'critical', summary: '1 anomalies detected',
          items: [{ label: '203.0.113.x', baseline_label: 'baseline', severity: 'critical', meta: { requests: 1, errors: 1, filters: {} } }],
        }}
        index={0}
        onScriptedTrafficClick={vi.fn()}
      />,
    )
    expect(screen.queryByRole('button', { name: /was flagged/i })).toBeNull()
  })
})

describe('ScriptedTrafficModal', () => {
  beforeEach(() => { realInsightSubcomponentsRef.value = true })

  it('renders nothing when data is null', () => {
    const { container } = render(<ScriptedTrafficModal isOpen onOpenChange={() => {}} data={null} />)
    expect(container.querySelector('[role="dialog"]')).toBeNull()
  })

  it('surfaces the evidence when open', () => {
    render(<ScriptedTrafficModal isOpen onOpenChange={() => {}} data={SCRIPTED_DATA} />)
    expect(screen.getByText(/Why we flagged this/i)).toBeTruthy()
    expect(screen.getAllByText(/92\/100/).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/94%/).length).toBeGreaterThan(0)
    expect(screen.getByText(/CV 0\.04/)).toBeTruthy()
    expect(screen.getAllByText(/\b1d\b/).length).toBeGreaterThan(0)
  })
})

describe('CacheCollapseModal recent misses', () => {
  beforeEach(() => {
    realInsightSubcomponentsRef.value = true
    useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
  })

  it('lists recent misses with cache and status badges', async () => {
    server.use(
      http.post(`${getApiBase()}/api/insights/cache-collapse-detail`, () =>
        HttpResponse.json({
          timeline: [],
          recent_misses: [
            { timestamp: '2026-10-01T10:00:00Z', cache: 'MISS', pop: 'SJC', ip: '203.0.113.9', status: 200 },
            { timestamp: '2026-10-01T10:01:00Z', cache: 'PASS', pop: null, ip: null, status: 503 },
          ],
          baseline_hit_rate: 0.9, window_hit_rate: 0.1, baseline_pass_rate: 0, window_pass_rate: 0,
        }),
      ),
    )
    renderWithQuery(<CacheCollapseModal isOpen onOpenChange={() => {}} url="/hot" windowHours="1" baselineHours="24" />)
    expect(await screen.findByText('SJC')).toBeTruthy()
    expect(screen.getByRole('columnheader', { name: /^Timestamp \(/ })).toBeTruthy()
    for (const name of ['Cache Status', 'POP', 'Client IP', 'HTTP Status']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    for (const text of ['MISS', 'PASS', '503', '203.0.113.9']) {
      expect(screen.getByText(text)).toBeTruthy()
    }
  })
})
