import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
// app/insights/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <InsightsClient />; RTL can't render an async component, so
// the unit tests target the client component that owns the title + fetch logic.
import InsightsClient from '@/app/insights/_sections/InsightsClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import React from 'react'


// Mock complicated components
vi.mock('@/components/Insights/InsightCard', () => ({ InsightCard: ({ insight }: any) => <div data-testid="insight-card">{insight.name}</div> }))
vi.mock('@/components/ReportLayout', () => ({
  ReportLayout: ({ children, title }: any) => <div><h1>{title}</h1>{children({ activeServiceId: 'test-svc', startTime: null, endTime: null, filterPayload: {} })}</div>
}))

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
  queryClient.clear()
})

test('renders insights page and displays insight cards', async () => {
  vi.mocked(client.GET).mockResolvedValue({ data: { unavailable: [] } } as any)
  vi.mocked(client.POST).mockResolvedValue({
    data: {
      insights: [
        {
          id: 'insight-1',
          name: 'Global Latency Spike',
          description: 'Latency is higher than usual',
          severity: 'warning',
          current_value: 150,
          baseline_value: 100,
          delta_percent: 50,
          unit: 'ms'
        }
      ],
      computed_at: new Date().toISOString()
    }
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>
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
  vi.mocked(client.GET).mockImplementation(((url: string) =>
    url === '/api/log-extents'
      ? Promise.resolve({
          data: {
            earliest_log_at: new Date(Date.now() - 2 * 3600_000).toISOString(),
            latest_log_at: new Date().toISOString(),
          },
        })
      : Promise.resolve({ data: { unavailable: [] } })) as any)
  vi.mocked(client.POST).mockResolvedValue({
    data: { insights: [], computed_at: new Date().toISOString() },
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <InsightsClient />
    </QueryClientProvider>
  )

  // ~2h history → window 1h / baseline 1h reaches the /api/insights request
  // (instead of the static 7-day baseline that would just say "not enough data").
  await waitFor(
    () => {
      const sawAdaptive = vi
        .mocked(client.POST)
        .mock.calls.some(
          ([, opts]: any) =>
            opts?.body?.window_size_hrs === 1 && opts?.body?.baseline_hours === 1
        )
      expect(sawAdaptive).toBe(true)
    },
    { timeout: 3000 }
  )
})
