import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import InsightsClient from '@/app/insights/_sections/InsightsClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { useServiceStore } from '@/stores/serviceStore'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'
import type { ReactNode } from 'react'

// Mock complicated components. SEVERITY_BADGE_CLASS is a real named export of
// this module (the section-header severity chips reuse it), so the mock must
// provide it too or SeverityChips throws on undefined-map indexing.
vi.mock('@/components/Insights/InsightCard', () => ({
  InsightCard: ({ insight }: { insight: { name?: string; title?: string } }) => <div data-testid="insight-card">{insight.name || insight.title}</div>,
  SEVERITY_BADGE_CLASS: { clean: '', info: '', warning: '', critical: '', error: '' },
}))
vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    activeServiceId: 'test-svc',
    startTime: null,
    endTime: null,
    filterPayload: {},
  }),
)

type ApiResult = { data: unknown }
type PostOpts = { body?: { window_size_hrs?: number; baseline_hours?: number; force_refresh?: boolean } }

const { apiGet, apiPost } = vi.hoisted(() => ({
  apiGet: vi.fn<(url: string, opts?: unknown) => Promise<ApiResult>>(),
  apiPost: vi.fn<(url: string, opts?: PostOpts) => Promise<ApiResult>>(),
}))

vi.mock('@/lib/api', () => ({
  client: {
    GET: apiGet,
    POST: apiPost,
    use: vi.fn(),
  },
  extractApiError: vi.fn((e: unknown) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
  queryClient.clear()
})

test('renders insights page and displays insight cards', async () => {
  apiGet.mockResolvedValue({ data: { unavailable: [] } })
  apiPost.mockResolvedValue({
    data: {
      insights: [
        {
          id: 'insight-1',
          name: 'Global Latency Spike',
          title: 'Global Latency Spike',
          description: 'Latency is higher than usual',
          category: 'edge',
          severity: 'warning',
          current_value: 150,
          baseline_value: 100,
          delta_percent: 50,
          unit: 'ms',
        },
      ],
      computed_at: new Date().toISOString(),
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Anomaly Detection')).toBeInTheDocument()

  // Wait for insights to load
  await waitFor(() => {
    expect(screen.getByTestId('insight-card')).toBeInTheDocument()
    expect(screen.getByText('Global Latency Spike')).toBeInTheDocument()
  })
})

test('adapts the default window/baseline to a service with ~2h of history', async () => {
  // /api/log-extents drives the adaptive default; everything else returns empty.
  apiGet.mockImplementation((url) =>
    url === '/api/log-extents'
      ? Promise.resolve({
          data: {
            earliest_log_at: new Date(Date.now() - 2 * 3600_000).toISOString(),
            latest_log_at: new Date().toISOString(),
          },
        })
      : Promise.resolve({ data: { unavailable: [] } }))
  apiPost.mockResolvedValue({
    data: { insights: [], computed_at: new Date().toISOString() },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  // ~2h history → window 1h / baseline 1h reaches the /api/insights request
  // (instead of the static 7-day baseline that would just say "not enough data").
  await waitFor(
    () => {
      const sawAdaptive = apiPost.mock.calls.some(
          ([, opts]) =>
            opts?.body?.window_size_hrs === 1 && opts?.body?.baseline_hours === 1,
        )
      expect(sawAdaptive).toBe(true)
    },
    { timeout: 3000 },
  )
})

test('renders all 6 category tabs and enforces zero-CLS layout reservation classes', async () => {
  apiGet.mockResolvedValue({ data: { unavailable: [] } })
  apiPost.mockResolvedValue({
    data: {
      insights: [
        {
          id: 'insight-sec-1',
          name: 'SQL Injection Spike',
          title: 'SQL Injection Spike',
          category: 'security',
          severity: 'critical',
        },
      ],
      computed_at: new Date().toISOString(),
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  // Verify all 6 tabs are rendered
  await waitFor(() => {
    expect(screen.getByRole('tab', { name: /all categories/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /security & threat detection/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /origin health & stability/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /edge & delivery performance/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /network & routing path/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /traffic & volumetrics/i })).toBeInTheDocument()
    expect(screen.getByTestId('insights-grid')).toBeInTheDocument()
  })

  // Verify zero-CLS container class on card grid
  const grid = screen.getByTestId('insights-grid')
  expect(grid.className).toContain('min-h-[300px]')
  expect(grid.className).toContain('[contain-intrinsic-size:300px]')
  expect(grid.className).toContain('[content-visibility:auto]')
})

test('displays unavailable insights alert with link to service-config', async () => {
  apiGet.mockResolvedValue({
    data: {
      unavailable: [
        { id: 'ja3_anomaly', name: 'JA3 Fingerprint Anomaly', missing_fields: ['ja3_md5'] },
        { id: 'bot_spike', name: 'Bot Cluster Surge', missing_fields: ['bot_score'] },
      ],
    },
  })
  apiPost.mockResolvedValue({
    data: { insights: [], computed_at: new Date().toISOString() },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(screen.getByText(/2 insights are unavailable/i)).toBeInTheDocument()
    expect(screen.getByText(/Configure Service Log Fields/i)).toBeInTheDocument()
  })

  const link = screen.getByRole('link', { name: /configure service log fields/i })
  expect(link).toHaveAttribute('href', '/service-config?service=test-svc')
})

test('renders Re-scan button for Admin and handles force_refresh', async () => {
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
  apiGet.mockResolvedValue({ data: { unavailable: [] } })
  apiPost.mockResolvedValue({
    data: { insights: [], computed_at: new Date().toISOString() },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(screen.getByRole('button', { name: /re-scan/i })).toBeInTheDocument()
  })

  const rescanBtn = screen.getByRole('button', { name: /re-scan/i })
  fireEvent.click(rescanBtn)

  await waitFor(() => {
    const hasForceRefresh = apiPost.mock.calls.some(([, opts]) => opts?.body?.force_refresh === true)
    expect(hasForceRefresh).toBe(true)
  })
})

test('hides Re-scan button for Remote Analyst', async () => {
  useSessionRoleStore.setState({ isRemoteAnalyst: true })
  apiGet.mockResolvedValue({ data: { unavailable: [] } })
  apiPost.mockResolvedValue({
    data: { insights: [], computed_at: new Date().toISOString() },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(screen.getByText('Anomaly Detection')).toBeInTheDocument()
  })

  expect(screen.queryByRole('button', { name: /re-scan/i })).not.toBeInTheDocument()
})

test('displays clean pass state when a category has no anomalies', async () => {
  apiGet.mockResolvedValue({ data: { unavailable: [] } })
  apiPost.mockResolvedValue({
    data: {
      insights: [],
      computed_at: new Date().toISOString(),
    },
  })

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient initialTab="security" />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(screen.getByTestId('insights-empty-pass')).toBeInTheDocument()
    expect(screen.getByText(/no anomalies detected in security & threat detection/i)).toBeInTheDocument()
  })

  const emptyPass = screen.getByTestId('insights-empty-pass')
  expect(emptyPass.className).toContain('min-h-[300px]')
  expect(emptyPass.className).toContain('[contain-intrinsic-size:300px]')
  expect(emptyPass.className).toContain('[content-visibility:auto]')
})
