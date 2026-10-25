/**
 * @vitest-environment jsdom
 */
import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import HighScaleRequestFactsPage from '@/app/high-scale/request-facts/page'
import HighScaleRequestFactsLoading from '@/app/high-scale/request-facts/loading'
import { highScaleRequestFactsHooks } from '@/components/high-scale/HighScaleRequestFactsPanel'

const { useFactsMock } = vi.hoisted(() => ({
  useFactsMock: vi.fn(),
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/browser-stubs')).reactVirtualModuleMock())

vi.mock('@/hooks/useIsDataReady', () => ({
  useEffectiveServiceId: () => 'service-1',
  useBootstrapResolved: () => true,
}))

vi.mock('@/hooks/useReportRangeAnchor', () => ({
  useTimeRange: () => ({
    startTime: '2026-09-11T00:00:00Z',
    endTime: '2026-09-11T01:00:00Z',
  }),
}))

const baseMetadata = {
  status: 'complete' as const,
  exact: true,
  coverage: 1,
  freshness_lag_seconds: 18,
  mode: 'clickhouse' as const,
  watermark: {
    service_id: 'service-1',
    domain: 'request',
    owner_epoch: 1,
    coverage_start: '2026-09-10T00:00:00Z',
    coverage_end: '2026-09-11T01:00:00Z',
    last_accepted_cursor: null,
    last_archived_event_id: null,
    last_visible_event_id: null,
    exact: true,
  },
  approximation_error: null,
  error: null,
}

beforeEach(() => {
  useFactsMock.mockReset()
  vi.spyOn(highScaleRequestFactsHooks, 'useHighScaleRequestFacts').mockImplementation(useFactsMock)
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('HighScaleRequestFactsPage (/high-scale/request-facts)', () => {
  it('renders route loading skeleton with zero-CLS layout reservations', () => {
    const { container } = render(<HighScaleRequestFactsLoading />)
    expect(screen.getByText('High-scale request facts')).toBeInTheDocument()
    expect(container.querySelector('[class*="contain-intrinsic-size"]')).toBeTruthy()
  })

  it('renders contextual distributed cluster shard skeleton while loading', () => {
    useFactsMock.mockReturnValue({
      isLoading: true,
      isError: false,
      data: undefined,
      error: null,
      isFetching: true,
      pageIndex: 0,
      canNextPage: false,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })

    render(<HighScaleRequestFactsPage />)
    expect(screen.getByText('Loading request facts…')).toBeInTheDocument()
    expect(screen.getByText('Querying distributed cluster shards...')).toBeInTheDocument()
    expect(screen.getByTestId('high-scale-loading-card').className).toContain('contain-intrinsic-size')
  })

  it('renders DuckDB Standard Mode Emulation badge and supports filter controls', () => {
    useFactsMock.mockReturnValue({
      isLoading: false,
      isError: false,
      data: {
        rows: [
          {
            event_id: 'event-emul-1',
            timestamp: '2026-09-11T00:00:01Z',
            service_id: 'service-1',
            status: 200,
            host: 'www.example.com',
            pop: 'IAD',
            country: 'US',
            client_ip: '203.0.113.xxx',
            url: '/healthz',
          },
        ],
        metadata: {
          ...baseMetadata,
          mode: 'duckdb_emulation',
        },
        next_cursor: null,
        _is_cached: false,
      },
      error: null,
      isFetching: false,
      pageIndex: 0,
      canNextPage: false,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })

    render(<HighScaleRequestFactsPage />)

    expect(screen.getByTestId('high-scale-mode-badge')).toHaveTextContent(
      'DuckDB Standard Mode Emulation'
    )
    expect(screen.getByText('/healthz')).toBeInTheDocument()
    expect(screen.getByText('IAD')).toBeInTheDocument()
    expect(screen.getByText('203.0.113.xxx')).toBeInTheDocument()

    // Filter controls pass updated predicates to useHighScaleRequestFacts
    fireEvent.change(screen.getByTestId('filter-status'), { target: { value: '503' } })
    fireEvent.change(screen.getByTestId('filter-domain'), { target: { value: 'api.example.com' } })
    fireEvent.change(screen.getByTestId('filter-pop'), { target: { value: 'LHR' } })
    fireEvent.change(screen.getByTestId('filter-country'), { target: { value: 'GB' } })
    fireEvent.change(screen.getByTestId('filter-batch-id'), { target: { value: 'batch-9' } })

    expect(useFactsMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        serviceId: 'service-1',
        status: 503,
        domain: 'api.example.com',
        pop: 'LHR',
        country: 'GB',
        batchId: 'batch-9',
        emulateStandard: true,
      })
    )

    // Clear filters button resets all filter inputs
    fireEvent.click(screen.getByTestId('filter-clear'))
    expect(useFactsMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        status: null,
        domain: null,
        pop: null,
        country: null,
        batchId: null,
      })
    )
  })

  it('applies dimmed opacity-40 transition during background refetch and renders empty state', () => {
    useFactsMock.mockReturnValue({
      isLoading: false,
      isError: false,
      data: {
        rows: [],
        metadata: baseMetadata,
        next_cursor: null,
        _is_cached: false,
      },
      error: null,
      isFetching: true,
      pageIndex: 0,
      canNextPage: false,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })

    render(<HighScaleRequestFactsPage />)
    expect(screen.getByText('No request facts in this time range.')).toBeInTheDocument()
    expect(screen.getByTestId('high-scale-table-wrapper').className).toContain('opacity-40')
    expect(screen.getByTestId('high-scale-table-wrapper').className).toContain('pointer-events-none')
  })

  it('renders error alert and server metadata with column headers', () => {
    useFactsMock.mockReturnValue({
      isLoading: false,
      isError: true,
      data: undefined,
      error: new Error('high-scale unavailable'),
      isFetching: false,
      pageIndex: 0,
      canNextPage: false,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })
    const errView = render(<HighScaleRequestFactsPage />)
    expect(screen.getByRole('alert')).toHaveTextContent('high-scale unavailable')
    errView.unmount()

    useFactsMock.mockReturnValue({
      isLoading: false,
      isError: false,
      data: {
        rows: [
          {
            event_id: 'event-1',
            timestamp: '2026-09-11T00:00:01Z',
            service_id: 'service-1',
            country: 'US',
            client_ip: '1.2.3.xxx',
            url: '/first',
          },
        ],
        metadata: {
          ...baseMetadata,
          freshness_lag_seconds: 42,
        },
        next_cursor: 'cursor-1',
        _is_cached: false,
      },
      error: null,
      isFetching: false,
      pageIndex: 0,
      canNextPage: true,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })
    render(<HighScaleRequestFactsPage />)
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('complete')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('42s lag')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('100%')
    expect(screen.getByText('1.2.3.xxx')).toBeInTheDocument()
    expect(screen.queryByText('1.2.3.4')).not.toBeInTheDocument()
    for (const name of ['Timestamp', 'Status', 'Host', 'POP', 'Country', 'Client IP', 'URL']) {
      expect(screen.getByRole('columnheader', { name })).toBeInTheDocument()
    }
  })
})
