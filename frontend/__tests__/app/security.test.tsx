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

vi.mock('@/stores/serviceStore', () => {
  const state = {
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [{ id: 'test-svc', name: 'Test Service' }],
    setServices: vi.fn(),
    setInitialized: vi.fn(),
    setActiveServiceId: vi.fn(),
  }
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state)),
    { getState: () => state },
  )
  return { useServiceStore }
})

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector) => {
    const state = {
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-01T01:00:00Z',
      filters: [],
      isAutoRange: false,
      compareMode: false,
      setRange: vi.fn(),
      clearFilters: vi.fn(),
    }
    return selector ? selector(state) : state
  }),
}))

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/security'))

// Mock heavy chart/section components so jsdom doesn't choke on plotly /
// d3-driven layout. The smoke test only cares that the page assembles
// — the chart bodies are tested in their own specs.
vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: () => <div data-testid="plotly-chart" />,
}))
vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 40,
        end: (index + 1) * 40,
        size: 40,
      })),
    getTotalSize: () => count * 40,
  }),
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
  ReportLayout: ({ children, title }: { children: (args: Record<string, unknown>) => React.ReactNode; title: string }) => (
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

test('security page mounts and renders title and tab navigation', async () => {
  render(
    <QueryClientProvider client={queryClient}>
      <SecurityClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Security')).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /waf activity/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /threat intelligence/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /vpn & proxy watchdog/i })).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.getByTestId('bots-section')).toBeInTheDocument()
  })

  expect(errorSpy).not.toHaveBeenCalled()
})

test('BotsSection renders Bot & Crawler Value vs. Cost table with bandwidth and referral metrics', async () => {
  const { BotsSection: ActualBotsSection } = await vi.importActual<
    typeof import('@/app/security/_sections/BotsSection')
  >('@/app/security/_sections/BotsSection')

  render(
    <QueryClientProvider client={queryClient}>
      <ActualBotsSection
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
      />
    </QueryClientProvider>,
  )

  expect(
    screen.getByText('Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)'),
  ).toBeInTheDocument()
  expect(screen.getByText('Anthropic Crawler Search')).toBeInTheDocument()
  expect(screen.getByText('AI Crawler')).toBeInTheDocument()
  expect(screen.getByText('Google Crawler')).toBeInTheDocument()
  expect(screen.getByText('Search Engine Crawler')).toBeInTheDocument()
})
