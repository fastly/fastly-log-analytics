/**
 * R-16 audit (testing_suite_audit_2026-06-14.md): every assertion in
 * this file is pure-logic conditional rendering driven by the
 * useServiceStore + useBootstrap mocks (access-level gating, remote-
 * analyst watermark). The Phase 3 Playwright journeys (plotly-chart,
 * maplibre-country-filter, dashboard-card-drag-drop) exercise the
 * mounted DOM of the layout but NOT the prop-driven branching pinned
 * here, so there is no overlap to prune — these tests stay.
 *
 * If a future Playwright spec starts asserting on the same conditional
 * branches (e.g. that an analyst session hides "Usage & Cost"), revisit
 * this file and trim the now-redundant JSDOM coverage.
 */
import { screen, act } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import { renderWithQueryClient } from '../helpers/query'
import { AppLayout } from '@/components/AppLayout'
import { useServiceStore } from '@/stores/serviceStore'
import { useBootstrap } from '@/hooks/useBootstrap'
import { partial } from '../helpers/partial'
import type { BootstrapResponse } from '@/types/api'
import { usePathname, useRouter } from 'next/navigation'
import React from 'react'

// Mock next/navigation
// (navigationModuleMock also stubs useSearchParams, which AppLayout's
// <RawQueryModeProbe> consumes inside its Suspense boundary.)
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

// Mock custom hooks
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: vi.fn(),
}))

vi.mock('@/hooks/useUrlServiceSync', () => ({
  useUrlServiceSync: vi.fn(),
}))

// useIsAnalyst reads directly from the QueryClient cache (NOT the
// mocked useBootstrap), so mocking useBootstrap above doesn't reach it.
// The default implementation honours serviceStore.accessLevel so the
// "hides restricted items for analysts" test continues to work; the
// watermark test overrides via vi.mocked(useIsAnalyst).mockReturnValue.
vi.mock('@/hooks/useIsAnalyst', () => ({
  useIsAnalyst: vi.fn(),
}))

// Mock components
vi.mock('@/components/ServiceSwitcher/ServiceSwitcher', () => ({ ServiceSwitcher: () => <div>ServiceSwitcher</div> }))
vi.mock('@/components/TimezoneSwitcher/TimezoneSwitcher', () => ({ TimezoneSwitcher: () => <div>TimezoneSwitcher</div> }))
vi.mock('@/components/ThemeToggle/ThemeToggle', () => ({ ThemeToggle: () => <div>ThemeToggle</div> }))
vi.mock('@/components/FilterBar/FilterBar', () => ({ FilterBar: () => <div>FilterBar</div> }))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', () => ({ SyncStatusBadge: () => <div>SyncStatusBadge</div> }))
vi.mock('@/components/DebugPanel', () => ({ DebugPanel: () => <div>DebugPanel</div> }))

// ScrollArea (base-ui) sets up internal layout via ResizeObserver and
// schedules a state update after mount; in jsdom that fires outside the
// test's render and triggers a React 19 act() warning. Stub it to a bare
// div — the navigation assertions don't care about scroll behaviour.
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

type BootstrapData = BootstrapResponse

// Real store for the test
beforeEach(async () => {
  vi.clearAllMocks()
  vi.mocked(usePathname).mockReturnValue('/dashboard')

  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: partial<BootstrapData>({ services: [{ service_id: 'test-svc', name: 'Test Service' }] }),
    isSuccess: true,
    isLoading: false,
  }))

  // Default useIsAnalyst implementation: mirror the prod hook's
  // serviceStore-accessLevel branch (the bootstrap-settings branch
  // requires QueryClient cache seeding that the test isn't doing).
  // Watermark test overrides via mockReturnValue(true).
  const { useIsAnalyst } = await import('@/hooks/useIsAnalyst')
  vi.mocked(useIsAnalyst).mockImplementation(() => {
    const { activeServiceId, services } = useServiceStore.getState()
    const active = services.find((s) => s.id === activeServiceId)
    return active?.accessLevel === 'read_only'
  })

  // Wrap in act() — the previous test's component may still have a live
  // store subscription mid-cleanup, and React 19 warns when a subscriber
  // is notified outside act().
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
  // Wrap the store mutation in act() — subscribers (the live AppLayout
  // component the next render mounts) are notified synchronously, and
  // React 19 warns when that happens outside act().
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
  // useIsAnalyst reads its own QueryClient cache; flip it true for
  // this assertion so the watermark render gate (isAnalyst &&
  // (analystEmail || analystName)) opens.
  const { useIsAnalyst } = await import('@/hooks/useIsAnalyst')
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
  const { useIsAnalyst } = await import('@/hooks/useIsAnalyst')
  vi.mocked(useIsAnalyst).mockReturnValue(false)

  renderWithQueryClient(<AppLayout><div>Analyst sign-in</div></AppLayout>)

  expect(screen.getByText('Analyst sign-in')).toBeInTheDocument()
  expect(replace).not.toHaveBeenCalled()
  expect(screen.queryByText('No services configured yet. Redirecting to the admin setup page.')).not.toBeInTheDocument()
})
