/**
 * Consolidated tests for AppLayout, ReportShell, ChartIntervalButtons,
 * NoServiceSelected, UpdatingBadge, StoreHydrator, and ReloadLoopGuard.
 *
 * @vitest-environment jsdom
 */
import { stubSystemTimeZone } from '@/__tests__/helpers/page-smoke'
import { render, screen, act, cleanup, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
import { Server } from 'lucide-react'
import { renderWithQueryClient } from '../helpers/page-smoke'
import { AppLayout } from '@/components/AppLayout'
import { ReportShell } from '@/components/ReportLayout'
import { ChartIntervalButtons } from '@/components/ReportLayout'
import { NoServiceSelected } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/AnalyticsCard'
import { StoreHydrator, ReloadLoopGuard } from '@/components/QueryProvider'
import { INTERVALS } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/filterStore'
import { useDebugStore } from '@/stores/filterStore'
import { useBootstrap } from '@/hooks/useBootstrap'
import { partial } from '../helpers/page-smoke'
import type { BootstrapResponse } from '@/types/api'
import { usePathname, useRouter } from 'next/navigation'
import React from 'react'

// Mock next/navigation
// (navigationModuleMock also stubs useSearchParams, which AppLayout's
// <RawQueryModeProbe> consumes inside its Suspense boundary.)
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

let mockActiveServiceId: string | null = 'svc-1'
let mockIsDataReady = true
let mockBootstrapResolved = true

// Mock custom hooks
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

// Mock components
vi.mock('@/components/FilterBar/FilterBar', () => ({ FilterBar: () => <div>FilterBar</div> }))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()),
  ServiceSwitcher: () => <div>ServiceSwitcher</div>,
  SyncStatusBadge: () => <div>SyncStatusBadge</div>,
}))
vi.mock('@/components/DebugPanel', () => ({ DebugPanel: () => <div>DebugPanel</div> }))

// ScrollArea (base-ui) sets up internal layout via ResizeObserver and
// schedules a state update after mount; in jsdom that fires outside the
// test's render and triggers a React 19 act() warning. Stub it to a bare
// div — the navigation assertions don't care about scroll behaviour.
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

type BootstrapData = BootstrapResponse

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

// Real store for the test
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
    const active = services.find((s) => s.id === activeServiceId)
    return active?.accessLevel === 'read_only'
  })

  act(() => {
    useServiceStore.setState({
      activeServiceId: 'test-svc',
      services: [{ id: 'test-svc', name: 'Test Service', accessLevel: 'read_write' }],
      isInitialized: true
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
      settings: {
        is_remote_analyst: true,
        analyst_email: 'jane@example.com',
        analyst_name: 'Jane Doe',
      },
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
    data: partial<BootstrapData>({
      services: [],
      settings: { is_remote_analyst: true, needs_login: true },
    }),
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
    render(
      <ReportShell title="My Report" description="A descriptive line" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByText('My Report')).toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })

  it('falls back to NoServiceSelected when no active service id is set (requireService default)', () => {
    mockActiveServiceId = null
    render(
      <ReportShell title="Security" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText(/select a service/i)).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
  })

  it('shows the dashboard skeleton when isDataReady is false and honours isReadyOverride', () => {
    mockIsDataReady = false
    const { unmount } = render(
      <ReportShell title="Logs" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
    unmount()

    render(
      <ReportShell title="Logs" icon={Server} isReadyOverride>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('content')).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })

  it('flash defense: bootstrap unresolved AND no active service → skeleton, not NoServiceSelected', () => {
    mockActiveServiceId = null
    mockBootstrapResolved = false
    mockIsDataReady = false
    render(
      <ReportShell title="Dashboard" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
  })

  it('requireService=false bypasses the NoServiceSelected gate', () => {
    mockActiveServiceId = null
    render(
      <ReportShell title="Insights" icon={Server} requireService={false}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })
})

const ALL_VALID = new Set(INTERVALS.map((i) => i.value))

describe('ChartIntervalButtons', () => {
  it('renders one button per INTERVALS entry and marks effective interval with aria-pressed=true', () => {
    render(
      <ChartIntervalButtons
        effectiveInterval="1 hour"
        validIntervals={ALL_VALID}
        onIntervalChange={() => {}}
      />,
    )
    for (const i of INTERVALS) {
      expect(screen.getByRole('button', { name: `Chart bucket size: ${i.label}` })).toBeInTheDocument()
    }
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1h' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1m' })).toHaveAttribute('aria-pressed', 'false')
  })

  it('fires onIntervalChange for valid buttons and disables invalid ones', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    const { unmount } = render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={ALL_VALID}
        onIntervalChange={onChange}
      />,
    )
    await user.click(screen.getByRole('button', { name: 'Chart bucket size: 1h' }))
    expect(onChange).toHaveBeenCalledWith('1 hour')
    unmount()

    const onChangeDisabled = vi.fn()
    render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={new Set(['1 minute'])}
        onIntervalChange={onChangeDisabled}
      />,
    )
    const hourBtn = screen.getByRole('button', { name: 'Chart bucket size: 1h' })
    expect(hourBtn).toBeDisabled()
    await user.click(hourBtn)
    expect(onChangeDisabled).not.toHaveBeenCalled()
  })
})

describe('NoServiceSelected', () => {
  it('renders the provided message, default/explicit title, and lucide icon', () => {
    const { container, unmount } = render(
      <NoServiceSelected icon={Server} message="Pick one from the picker above" />,
    )
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText('Pick one from the picker above')).toBeInTheDocument()
    const svg = container.querySelector('svg')
    expect(svg?.classList.contains('text-muted-foreground')).toBe(true)
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
      useServiceStore.setState({
        activeServiceId: null,
        services: [],
        isInitialized: false,
      })
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
      JSON.stringify({
        state: { activeServiceId: 'svc-persisted', services: [{ id: 'svc-persisted', name: 'Persisted' }], isInitialized: true },
        version: 0,
      }),
    )

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useServiceStore.getState().activeServiceId).toBe('svc-persisted')
    })
  })

  it('rehydrates debugStore from localStorage after mount', async () => {
    window.localStorage.setItem(
      'fastly-debug-settings',
      JSON.stringify({
        state: { enabled: true, apiCallsEnabled: true },
        version: 0,
      }),
    )

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useDebugStore.getState().enabled).toBe(true)
      expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    })
  })

  it('resolves the live system zone on mount when no zone was ever persisted (default mode: system)', async () => {
    stubSystemTimeZone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
    })
    expect(useTimezoneStore.getState().mode).toBe('system')
  })

  it('re-resolves the live system zone on mount for a persisted system-mode blob', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'UTC', mode: 'system' }, version: 1 }),
    )
    stubSystemTimeZone('Europe/Berlin')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().timezone).toBe('Europe/Berlin')
    })
  })

  it('keeps a persisted manual-mode timezone untouched on mount, even if the system zone differs', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/New_York', mode: 'manual' }, version: 1 }),
    )
    stubSystemTimeZone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useServiceStore.getState().isInitialized).toBe(false)
    })
    expect(useTimezoneStore.getState().mode).toBe('manual')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })

  it('migrates a pre-v1 persisted blob (no mode field) to manual and keeps its zone', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }),
    )
    stubSystemTimeZone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().mode).toBe('manual')
    })
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })
})

const CHILD = 'app-subtree-rendered'

function loadGuard() {
  return render(
    <ReloadLoopGuard>
      <div>{CHILD}</div>
    </ReloadLoopGuard>,
  )
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
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
    expect(screen.queryByText(CHILD)).not.toBeInTheDocument()
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh now/i })).toBeInTheDocument()
  })

  test('"Continue anyway" dismisses the prompt and renders the app', () => {
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /continue anyway/i }))
    expect(screen.getByText(CHILD)).toBeInTheDocument()
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
  })

  test('"Refresh now" clears history and reloads', () => {
    const reloadSpy = vi.spyOn(window.location, 'reload').mockImplementation(() => {})
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
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
