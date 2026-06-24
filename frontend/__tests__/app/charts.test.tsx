import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import ChartsPage from '@/app/charts/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6 (optional charts test): render-smoke for the distribution charts page.
// Uses ReportShell (not ReportLayout) so the mock surface is slightly
// different from the other analytics page tests.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/charts'))

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('@/hooks/useUrlFilterSync', () => ({
  useUrlFilterSync: vi.fn(),
}))

vi.mock('@/hooks/useFilterPayload', () => ({
  useFilterPayload: vi.fn(() => ({})),
  useDebouncedFilterPayload: vi.fn(() => ({})),
}))

vi.mock('@/hooks/useDashboardCards', () => ({
  useDashboardCards: vi.fn(() => []),
}))

vi.mock('@/hooks/useLogFieldsCatalog', () => ({
  useLogFieldsCatalog: vi.fn(() => ({ data: { fields: [] } })),
}))

vi.mock('@/hooks/useServiceQuery', () => ({
  useServiceQuery: vi.fn(() => ({ data: { rows: [] }, isLoading: false, isFetching: false })),
}))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())

vi.mock('@/components/ReportShell', () => ({
  ReportShell: ({ children, title }: any) => (
    <div>
      <h1>{title}</h1>
      {children}
    </div>
  ),
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('charts page mounts and renders title', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <ChartsPage />
    </QueryClientProvider>,
  )
  expect(screen.getByText('Distribution Charts')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})
