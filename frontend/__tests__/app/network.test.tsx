import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import NetworkPage from '@/app/network/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import type { ReactNode } from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6: render-smoke for the network analytics page.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/network'))

type ApiCall = (url: string, opts?: unknown) => Promise<{ data: unknown }>
const mockPost = vi.fn<ApiCall>()
const mockGet = vi.fn<ApiCall>()

vi.mock('@/lib/api', () => ({
  client: {
    GET: (url: string, opts?: unknown) => mockGet(url, opts),
    POST: (url: string, opts?: unknown) => mockPost(url, opts),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e: unknown) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/Map/NetworkMap', () => ({
  NetworkMap: () => <div data-testid="network-map" />,
}))
vi.mock('@/components/Map/ShieldingMap', () => ({
  ShieldingMap: () => <div data-testid="shielding-map" />,
}))
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
vi.mock('@/components/AnalyticsCard', () => ({
  AnalyticsCard: ({ title, children }: { title: ReactNode; children?: ReactNode }) => (
    <div data-testid="analytics-card">
      <h3>{title}</h3>
      {children}
    </div>
  ),
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 3600,
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
  mockPost.mockReset()
  mockGet.mockReset()
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('network page mounts and renders unavailable state when fields not active', async () => {
  mockPost.mockResolvedValue({ data: { available: false, reason: 'Network fields not enabled' } })
  mockGet.mockResolvedValue({ data: { data: [] } })

  render(
    <QueryClientProvider client={queryClient}>
      <NetworkPage />
    </QueryClientProvider>,
  )
  expect(screen.getByText('Network & ASN Health')).toBeInTheDocument()
  expect(await screen.findByText('Network Metrics Unavailable')).toBeInTheDocument()
  expect(screen.getByText('Network fields not enabled')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('network page renders full dashboard with transport metrics and leaderboards', async () => {
  mockPost.mockImplementation(async (url: string) => {
    if (url.includes('/api/network/bundle')) {
      return {
        data: {
          transport: {
            tcp_rtt_p50_ms: 24.5,
            tcp_rtt_p90_ms: 45.2,
            tcp_rtt_p95_ms: 62.1,
            tcp_rtt_p99_ms: 98.4,
            retrans_rate_pct: 0.15,
            packet_loss_pct: 0.05,
            delivery_rate_mbps: 42.8,
            protocol_distribution: [
              { protocol: 'HTTP/3', share_pct: 65.4, requests: 6540 },
              { protocol: 'HTTP/2', share_pct: 30.1, requests: 3010 },
              { protocol: 'HTTP/1.1', share_pct: 4.5, requests: 450 },
            ],
            missing_fields: [],
          },
        },
      }
    }
    if (url.includes('/api/network-health')) {
      return {
        data: {
          available: true,
          summary: {
            global_health_score: 94.2,
            avg_rtt_ms: 28.4,
            worst_asn: { label: 'Comcast (7922)', score: 78.5 },
            worst_country: { label: 'Australia (AU)', score: 81.2 },
          },
          leaderboard: [
            {
              asn: 7922,
              label: 'Comcast (7922)',
              total_reqs: 50000,
              health_score_now: 88.5,
              health_score_1h_ago: 87.0,
              p95_rtt_us: 45000,
              p99_rtt_us: 75000,
              trend: 'improving',
            },
          ],
          has_metro: true,
          metro_leaderboard: [
            {
              city: 'San Jose',
              raw_city: 'San Jose',
              region: 'CA',
              country: 'US',
              total_reqs: 25000,
              health_score: 95.0,
            },
          ],
        },
      }
    }
    if (url.includes('/api/network-quality')) {
      return {
        data: {
          available: true,
          by_country: [{ label: 'United States', rtt_ms: 22.1, reqs: 10000 }],
          by_asn: [{ label: 'Comcast (7922)', rtt_ms: 24.5, reqs: 5000 }],
          by_region: [{ label: 'California', rtt_ms: 18.2, reqs: 4000 }],
          by_pop: [{ label: 'SJC', rtt_ms: 15.1, reqs: 8000 }],
          countries: ['US'],
          scatter: [{ rtt_ms: 20, ttfb_ms: 45 }],
        },
      }
    }
    return { data: {} }
  })

  mockGet.mockResolvedValue({
    data: {
      data: [
        {
          pop: 'SJC',
          requests: 50000,
          errors: 10,
          error_rate: 0.02,
          p50_rtt_us: 15000,
          p95_ttfb_ms: 45.0,
          cache_hit_rate: 92.5,
          bandwidth_bytes: 104857600,
        },
      ],
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <NetworkPage />
    </QueryClientProvider>,
  )

  // Verify shell and summary cards
  expect(screen.getByText('Network & ASN Health')).toBeInTheDocument()
  expect(await screen.findByText('94.2/100')).toBeInTheDocument()
  expect(screen.getByText('28.4ms')).toBeInTheDocument()
  expect(screen.getAllByText('Comcast (7922)').length).toBeGreaterThan(0)

  // Verify Transport Section
  expect(await screen.findByText('Edge Transport & TCP Performance')).toBeInTheDocument()
  expect(await screen.findByText('24.5')).toBeInTheDocument()
  expect(screen.getByText('/ 62.1ms')).toBeInTheDocument()
  expect(screen.getByText('42.8')).toBeInTheDocument()
  expect(screen.getByText('HTTP/3')).toBeInTheDocument()
  expect(screen.getByText('65.4%')).toBeInTheDocument()

  // Verify ASN Leaderboard
  expect(screen.getByText('ASN Performance Leaderboard')).toBeInTheDocument()
  expect(screen.getByText('IMPROVING')).toBeInTheDocument()

  // Verify PoP Heatmap
  expect(await screen.findByText('Fastly Edge PoP Health Heatmap')).toBeInTheDocument()
  expect(screen.getByText('1 Active PoPs')).toBeInTheDocument()

  expect(errorSpy).not.toHaveBeenCalled()
})

test('transport section displays missing fields banner when optional fields not logged', async () => {
  mockPost.mockImplementation(async (url: string) => {
    if (url.includes('/api/network/bundle')) {
      return {
        data: {
          transport: {
            tcp_rtt_p50_ms: null,
            tcp_rtt_p90_ms: null,
            tcp_rtt_p95_ms: null,
            tcp_rtt_p99_ms: null,
            retrans_rate_pct: null,
            packet_loss_pct: null,
            delivery_rate_mbps: null,
            protocol_distribution: [],
            missing_fields: ['tcp_rtt', 'retrans', 'ploss'],
          },
        },
      }
    }
    return {
      data: {
        available: true,
        summary: { global_health_score: 90 },
      },
    }
  })
  mockGet.mockResolvedValue({ data: { data: [] } })

  render(
    <QueryClientProvider client={queryClient}>
      <NetworkPage />
    </QueryClientProvider>,
  )

  expect(await screen.findByTestId('missing-transport-fields-banner')).toBeInTheDocument()
  expect(screen.getByText('tcp_rtt, retrans, ploss')).toBeInTheDocument()
  expect(screen.getByText('Service Configuration')).toHaveAttribute('href', '/service-config')
})
