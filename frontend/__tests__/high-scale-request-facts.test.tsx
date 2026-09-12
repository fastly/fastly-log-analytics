/**
 * @vitest-environment jsdom
 */
import React from 'react'
import { render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { HighScaleRequestFactsPanel } from '@/components/high-scale/HighScaleRequestFactsPanel'

const { useFactsMock } = vi.hoisted(() => ({
  useFactsMock: vi.fn(),
}))

vi.mock('@/hooks/useHighScaleRequestFacts', () => ({
  useHighScaleRequestFacts: useFactsMock,
}))

vi.mock('@/hooks/useIsDataReady', () => ({
  useEffectiveServiceId: () => 'service-1',
  useBootstrapResolved: () => true,
}))

vi.mock('@/hooks/useTimeRange', () => ({
  useTimeRange: () => ({
    startTime: '2026-09-11T00:00:00Z',
    endTime: '2026-09-11T01:00:00Z',
  }),
}))

const metadata = {
  status: 'complete' as const,
  exact: true,
  coverage: 1,
  freshness_lag_seconds: 42,
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

const pageOne = {
  rows: [{
    event_id: 'event-1',
    timestamp: '2026-09-11T00:00:01Z',
    service_id: 'service-1',
    country: 'US',
    client_ip: '1.2.3.xxx',
    url: '/first',
  }],
  metadata,
  next_cursor: 'cursor-1',
  _is_cached: false,
}

beforeEach(() => {
  useFactsMock.mockReset()
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('HighScaleRequestFactsPanel', () => {
  it('renders loading and error states', () => {
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

    const loading = render(<HighScaleRequestFactsPanel />)
    expect(screen.getByText('Loading request facts…')).toBeInTheDocument()
    loading.unmount()

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
    render(<HighScaleRequestFactsPanel />)
    expect(screen.getByRole('alert')).toHaveTextContent('high-scale unavailable')
  })

  it('renders server metadata and masked row values', () => {
    useFactsMock.mockReturnValue({
      isLoading: false,
      isError: false,
      data: pageOne,
      error: null,
      isFetching: false,
      pageIndex: 0,
      canNextPage: true,
      canPreviousPage: false,
      nextPage: vi.fn(),
      previousPage: vi.fn(),
    })
    render(<HighScaleRequestFactsPanel />)
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('complete')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('42s lag')
    expect(screen.getByTestId('high-scale-metadata')).toHaveTextContent('100%')
    expect(screen.getByText('1.2.3.xxx')).toBeInTheDocument()
    expect(screen.queryByText('1.2.3.4')).not.toBeInTheDocument()
  })
})
