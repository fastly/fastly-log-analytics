import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import UsagePage from '@/app/usage/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// UX-6: the Storage StatCard read `(storage?.total_billed_gb_hours ?? 0)`, so a
// /api/usage/current-storage 5xx rendered a confident "0.00 GB-hrs" and the
// cost card silently dropped storage cost. The fix gates the value on
// errorStorage (shows "—" + an inline error) and flags the cost as partial.

vi.mock('@/stores/serviceStore', async () =>
  (await import('../helpers/page-smoke')).serviceStoreModuleMock({ accessLevel: 'read_write' }),
)
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/usage'))
vi.mock('next-themes', () => ({ useTheme: vi.fn(() => ({ theme: 'light' })) }))
vi.mock('@/hooks/useIsDataReady', () => ({ useIsDataReady: vi.fn(() => true) }))

vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(async (path: string) => {
      if (path === '/api/usage/current-storage') throw new Error('storage boom')
      // Other GETs resolve undefined (like the smoke test): every read on the
      // page is guarded with optional chaining + `?? []`, so undefined data is
      // safe and keeps this test focused on the storage card.
      return undefined
    }),
    POST: vi.fn().mockResolvedValue({ data: {} }),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/CostCalculator/CostCalculator', () => ({ CostCalculator: () => <div data-testid="cost-calculator" /> }))
vi.mock('@/components/ReportLayout', () => ({
  ReportLayout: ({ children, title }: any) => (
    <div>
      <h1>{title}</h1>
      {children({
        startTime: '2026-01-01T00:00:00Z',
        endTime: '2026-01-01T01:00:00Z',
        activeServiceId: 'test-svc',
        filterPayload: {},
        intervalButtons: null,
        bucketSeconds: 3600,
        config: { effectiveInterval: '1 hour', interval: '1 hour' },
        setChartInterval: vi.fn(),
      })}
    </div>
  ),
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>
beforeEach(() => { queryClient.clear(); errorSpy = spyOnConsoleError() })
afterEach(() => errorSpy.mockRestore())

test('UX-6: storage card shows an error (not "0.00 GB-hrs") and the cost is flagged partial on a storage 5xx', async () => {
  render(
    <QueryClientProvider client={queryClient}>
      <UsagePage />
    </QueryClientProvider>,
  )
  await waitFor(() => {
    expect(screen.getByText('Failed to load storage usage.')).toBeInTheDocument()
  })
  // The fabricated "0.00 GB-hrs" value is gone…
  expect(screen.queryByText(/GB-hrs/)).toBeNull()
  // …and the cost estimate is explicitly flagged as partial.
  expect(screen.getByText(/Partial estimate — storage cost excluded\./)).toBeInTheDocument()
})
