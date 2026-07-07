/**
 * C-3: ReportLayout wires ReportShell + per-page apiCall through the
 * shared time/service/filter/interval hooks and exposes a render-prop
 * callback to the page body. Pin the contract: the render-prop
 * receives `data`, `isLoading`, `bucketSeconds`, `intervalButtons` etc.,
 * and `apiCall` runs through `useServiceQuery`.
 *
 * @vitest-environment jsdom
 */
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Server } from 'lucide-react'
import React from 'react'
import { ReportLayout } from '@/components/ReportLayout'

vi.mock('@/hooks/useTimeRange', () => ({
  useTimeRange: () => ({ startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z' }),
}))
vi.mock('@/hooks/useActiveService', () => ({
  useActiveService: () => ({ activeServiceId: 'svc-1', services: [{ id: 'svc-1' }] }),
}))
vi.mock('@/hooks/useTimezone', () => ({ useTimezone: () => 'UTC' }))
vi.mock('@/hooks/useFilterPayload', () => ({
  useFilterPayload: () => ({ filters: [] }),
  useDebouncedFilterPayload: () => ({ filters: [] }),
}))
vi.mock('@/hooks/useViewMetricUrlSync', () => ({ useViewMetricUrlSync: vi.fn() }))
vi.mock('@/hooks/useReportConfig', () => ({
  useReportConfig: () => ({
    config: { effectiveInterval: '1 hour', validIntervals: ['1 hour'] },
    setChartInterval: vi.fn(),
    trend: 'flat',
    setTrend: vi.fn(),
  }),
}))

vi.mock('@/components/ReportShell', () => ({
  ReportShell: ({ children, title }: any) => (
    <div>
      <h1>{title}</h1>
      {children}
    </div>
  ),
}))

vi.mock('@/components/ChartIntervalButtons', () => ({
  ChartIntervalButtons: () => <div data-testid="interval-buttons" />,
}))

function wrapper() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0, staleTime: 0 } },
  })
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children)
}

describe('ReportLayout', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('invokes apiCall with the resolved time range + filters + bucket', async () => {
    const apiCall = vi.fn().mockResolvedValue({ rows: ['row-a'] })
    const Wrapper = wrapper()
    render(
      <Wrapper>
        <ReportLayout title="Security" description="" icon={Server} apiCall={apiCall}>
          {({ data, isLoading }) => (
            <div>
              <div data-testid="loading">{String(isLoading)}</div>
              <div data-testid="data">{data ? JSON.stringify(data) : 'no-data'}</div>
            </div>
          )}
        </ReportLayout>
      </Wrapper>,
    )

    await waitFor(() => expect(apiCall).toHaveBeenCalled())
    expect(apiCall).toHaveBeenCalledWith({
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-01T01:00:00Z',
      filters: { filters: [] },
      bucketSeconds: 3600,
    })

    await waitFor(() => expect(screen.getByTestId('data').textContent).toContain('row-a'))
  })

  it('skips the query when no apiCall is provided', async () => {
    const Wrapper = wrapper()
    render(
      <Wrapper>
        <ReportLayout title="Charts" description="" icon={Server}>
          {({ data, isLoading }) => (
            <div>
              <div data-testid="loading">{String(isLoading)}</div>
              <div data-testid="data">{data === undefined ? 'undef' : 'has-data'}</div>
            </div>
          )}
        </ReportLayout>
      </Wrapper>,
    )

    await waitFor(() => expect(screen.getByTestId('data').textContent).toBe('undef'))
    expect(screen.getByTestId('loading').textContent).toBe('false')
  })

  it('renders the interval buttons in the render-prop arg', () => {
    const Wrapper = wrapper()
    render(
      <Wrapper>
        <ReportLayout title="Logs" description="" icon={Server}>
          {({ intervalButtons }) => <div>{intervalButtons}</div>}
        </ReportLayout>
      </Wrapper>,
    )
    expect(screen.getByTestId('interval-buttons')).toBeInTheDocument()
  })
})
