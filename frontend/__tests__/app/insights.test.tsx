import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import InsightsPage from '@/app/insights/page'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import React from 'react'


// Mock complicated components
vi.mock('@/components/Insights/InsightCard', () => ({ InsightCard: ({ insight }: any) => <div data-testid="insight-card">{insight.name}</div> }))
vi.mock('@/components/ReportLayout', () => ({
  ReportLayout: ({ children, title }: any) => <div><h1>{title}</h1>{children({ activeServiceId: 'test-svc', startTime: null, endTime: null, filterPayload: {} })}</div>
}))

// Mock the API client
vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      staleTime: 0
    }
  }
})

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
  queryClient.clear()
})

test('renders insights page and displays insight cards', async () => {
  // Mock API responses
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
      <InsightsPage />
    </QueryClientProvider>
  )

  // Verify header
  expect(screen.getByText('Anomaly Detection')).toBeInTheDocument()

  // Wait for insights to load
  await waitFor(() => {
    expect(screen.getByTestId('insight-card')).toBeInTheDocument()
    expect(screen.getByText('Global Latency Spike')).toBeInTheDocument()
  })
})
