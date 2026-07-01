import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// app/security/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <SecurityClient />; RTL can't render an async component, so
// the smoke test targets the client component that owns the title + sections.
import SecurityClient from '@/app/security/_sections/SecurityClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'

// R-6 (testing_suite_audit_2026-06-14.md). Render-smoke only: prove the
// page mounts, the title renders, and no console.error is emitted. The
// page-internal sections (BotsSection / HeaderAnomaliesSection /
// NetworkSection) are exercised by their own component tests + the
// Playwright journey in Phase 3.

vi.mock('@/stores/serviceStore', () => ({
  useServiceStore: vi.fn((selector) => {
    const state = {
      activeServiceId: 'test-svc',
      isInitialized: true,
      services: [{ id: 'test-svc', name: 'Test Service' }],
      setServices: vi.fn(),
      setInitialized: vi.fn(),
      setActiveServiceId: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
}))

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector) => {
    const state = {
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-01T01:00:00Z',
      filters: [],
      isAutoRange: false,
      hasSyncedExtents: true,
      compareMode: false,
      autoSetRange: vi.fn(),
      setRange: vi.fn(),
      clearFilters: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
}))

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/security'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

// Mock heavy chart/section components so jsdom doesn't choke on plotly /
// d3-driven layout. The smoke test only cares that the page assembles
// — the chart bodies are tested in their own specs.
vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: () => <div data-testid="plotly-chart" />,
}))
vi.mock('@/app/security/_sections/BotsSection', () => ({
  BotsSection: () => <div data-testid="bots-section">Bots</div>,
}))
vi.mock('@/app/security/_sections/HeaderAnomaliesSection', () => ({
  HeaderAnomaliesSection: () => <div data-testid="header-anomalies-section">Header Anomalies</div>,
}))
vi.mock('@/app/security/_sections/NetworkSection', () => ({
  NetworkSection: () => <div data-testid="network-section">Network</div>,
}))

vi.mock('@/components/ReportLayout', () => ({
  ReportLayout: ({ children, title }: any) => (
    <div>
      <h1>{title}</h1>
      {children({
        data: { security_signals: [], top_bots: [], ngwaf_bots: [] },
        isLoading: false,
        isFetching: false,
        intervalButtons: null,
        bucketSeconds: 3600,
      })}
    </div>
  ),
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  queryClient.clear()
  errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('security page mounts and renders title', async () => {
  render(
    <QueryClientProvider client={queryClient}>
      <SecurityClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Security')).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.getByTestId('bots-section')).toBeInTheDocument()
  })

  expect(errorSpy).not.toHaveBeenCalled()
})
