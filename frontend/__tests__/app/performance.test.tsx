import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// app/performance/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <PerformanceClient />; RTL can't render an async component, so
// the smoke test targets the client component that owns the title + sections.
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6 (testing_suite_audit_2026-06-14.md): smoke test only — verify
// the page mounts and renders its title without console errors.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/performance'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
vi.mock('@/components/AnalyticsCard', () => ({
  AnalyticsCard: ({ title, children }: any) => (
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
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('performance page mounts and renders title', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <PerformanceClient />
    </QueryClientProvider>,
  )
  expect(screen.getByText('Performance')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})
