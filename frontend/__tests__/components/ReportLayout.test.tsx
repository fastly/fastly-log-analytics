/**
 * C-3: ReportLayout wires ReportShell through the shared
 * time/service/filter/interval hooks and exposes a render-prop callback to
 * the page body. It no longer owns a query (the `apiCall` opt-out was removed
 * so there is ONE query architecture — pages run their data fetch through
 * `useAnalyticsReportQuery`). Pin the contract: the render-prop receives
 * `startTime`, `endTime`, `activeServiceId`, `filterPayload`, `bucketSeconds`,
 * `config`, `intervalButtons` etc. — and does NOT expose `data`/`isLoading`.
 *
 * @vitest-environment jsdom
 */
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../helpers/query'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Server } from 'lucide-react'
import React from 'react'
import { ReportLayout } from '@/components/ReportLayout'

vi.mock('@/hooks/useTimeRange', () => ({
  useTimeRange: () => ({ startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z' }),
}))
vi.mock('@/hooks/useIsDataReady', () => ({
  useEffectiveServiceId: () => 'svc-1',
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
  ReportShell: ({ children, title }: { children?: React.ReactNode; title?: React.ReactNode }) => (
    <div>
      <h1>{title}</h1>
      {children}
    </div>
  ),
}))

vi.mock('@/components/ChartIntervalButtons', () => ({
  ChartIntervalButtons: () => <div data-testid="interval-buttons" />,
}))

describe('ReportLayout', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('provides the resolved time range, service, filter payload + bucket to the render-prop', () => {
    renderWithQuery(
      <ReportLayout title="Security" description="" icon={Server}>
        {({ startTime, endTime, activeServiceId, filterPayload, bucketSeconds }) => (
          <div>
            <div data-testid="start">{startTime}</div>
            <div data-testid="end">{endTime}</div>
            <div data-testid="service">{activeServiceId}</div>
            <div data-testid="filter">{JSON.stringify(filterPayload)}</div>
            <div data-testid="bucket">{String(bucketSeconds)}</div>
          </div>
        )}
      </ReportLayout>,
      { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } },
    )

    expect(screen.getByTestId('start').textContent).toBe('2026-01-01T00:00:00Z')
    expect(screen.getByTestId('end').textContent).toBe('2026-01-01T01:00:00Z')
    expect(screen.getByTestId('service').textContent).toBe('svc-1')
    expect(screen.getByTestId('filter').textContent).toBe(JSON.stringify({ filters: [] }))
    expect(screen.getByTestId('bucket').textContent).toBe('3600')
  })

  it('does not own a query — no data/isLoading leak into the render-prop', () => {
    renderWithQuery(
      <ReportLayout title="Charts" description="" icon={Server}>
        {(ctx) => (
          <div>
            <div data-testid="has-data-key">{String('data' in ctx)}</div>
            <div data-testid="has-loading-key">{String('isLoading' in ctx)}</div>
          </div>
        )}
      </ReportLayout>,
      { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } },
    )

    expect(screen.getByTestId('has-data-key').textContent).toBe('false')
    expect(screen.getByTestId('has-loading-key').textContent).toBe('false')
  })

  it('renders the interval buttons in the render-prop arg', () => {
    renderWithQuery(
      <ReportLayout title="Logs" description="" icon={Server}>
        {({ intervalButtons }) => <div>{intervalButtons}</div>}
      </ReportLayout>,
      { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } },
    )
    expect(screen.getByTestId('interval-buttons')).toBeInTheDocument()
  })
})
