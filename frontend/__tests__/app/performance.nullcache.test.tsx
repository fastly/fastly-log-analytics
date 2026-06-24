/**
 * Repro for UX pre-release finding "UX-2 — /performance scatter useMemo throws
 * on a null `cache` row → crashes the WHOLE route body to the error boundary".
 *
 * app/performance/page.tsx:114-117 computes the cache-hit/miss scatter inside a
 * render-time useMemo whose only guard is `!coreData?.scatter?.length`. Each row
 * is read as `(d.cache as string).startsWith('HIT')`. `scatter` rows are typed
 * `{ [key: string]: unknown }` in the generated schema, so the `as string` is the
 * sole reason this compiles — and the backend can legitimately emit `cache: null`.
 * `null.startsWith` throws DURING render, unmounting the entire route to the
 * segment error boundary (same blast radius as the prior ShieldingMap crash).
 *
 * Regression guard: the page must SURVIVE a dirty scatter row. Before the UX-2
 * fix this failed (the boundary caught the TypeError); it now passes because the
 * scatter useMemo narrows `cache` at runtime instead of asserting `as string`.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'
import { QueryClientProvider } from '@tanstack/react-query'

import PerformancePage from '@/app/performance/page'
import { createTestQueryClient } from '../helpers/query'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/performance'))

// Core query (sections include 'scatter') resolves a scatter row with cache=null
// — a valid producer payload that the `as string` cast hides from the compiler.
vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(async (_path: string, opts: any) => {
      const sections: string[] = opts?.body?.sections ?? []
      if (sections.includes('scatter')) {
        return {
          data: {
            available: true,
            scatter: [{ cache: null, origin: 10, edge: 5 }],
            waterfall: { avg: { edge_processing: 1, origin_wait: 2 } },
            top_urls: [],
            top_asns: [],
          },
        }
      }
      return { data: { available: true, ttl_dist: [] } }
    }),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/DataTable', () => ({
  DataTable: () => <div data-testid="data-table" />,
  ColumnVisibilityDropdown: () => null,
}))
vi.mock('@/components/AnalyticsCard', () => ({
  AnalyticsCard: ({ title, isLoading, children }: any) => (
    <div data-testid="analytics-card" data-loading={String(!!isLoading)}>
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

class Boundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null }
  static getDerivedStateFromError(error: Error) {
    return { error }
  }
  render() {
    if (this.state.error) {
      return <div data-testid="route-crashed">{this.state.error.message}</div>
    }
    return this.props.children
  }
}

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

beforeEach(() => queryClient.clear())
afterEach(() => vi.clearAllMocks())

describe('/performance survives a dirty scatter row (UX-2)', () => {
  it('does not crash the route when a scatter row has cache=null', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <Boundary>
          <PerformancePage />
        </Boundary>
      </QueryClientProvider>,
    )
    // Wait for the core query to resolve — the waterfall card flips out of its
    // loading state. The dirty-scatter useMemo runs on that same render, so once
    // the card is no longer loading the crashing code path has executed.
    await waitFor(() => {
      const card = screen
        .getByText('End-to-End Latency Waterfall (Average)')
        .closest('[data-testid="analytics-card"]')
      expect(card?.getAttribute('data-loading')).toBe('false')
    })
    // The route body survived (not replaced by the error boundary).
    expect(screen.queryByTestId('route-crashed')).toBeNull()
  })
})
