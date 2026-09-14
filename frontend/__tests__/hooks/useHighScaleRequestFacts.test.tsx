/**
 * @vitest-environment jsdom
 */
import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderHook, waitFor } from '@testing-library/react'
import { act } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { client } from '@/lib/api'
import { useHighScaleRequestFacts } from '@/hooks/useHighScaleRequestFacts'

const metadata = {
  status: 'complete' as const,
  exact: true,
  coverage: 1,
  freshness_lag_seconds: 0,
  watermark: {
    service_id: 'service-1',
    domain: 'request',
    owner_epoch: 1,
    coverage_start: null,
    coverage_end: null,
    last_accepted_cursor: null,
    last_archived_event_id: null,
    last_visible_event_id: null,
    exact: true,
  },
  approximation_error: null,
  error: null,
}

const pageOne = {
  rows: [{ event_id: 'event-1', timestamp: '2026-09-11T00:00:01Z' }],
  metadata,
  next_cursor: 'cursor-1',
  _is_cached: false,
}

const pageTwo = {
  rows: [{ event_id: 'event-2', timestamp: '2026-09-11T00:00:02Z' }],
  metadata,
  next_cursor: null,
  _is_cached: false,
}

function wrapper({ children }: { children: React.ReactNode }) {
  return (
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      {children}
    </QueryClientProvider>
  )
}

beforeEach(() => {
  vi.spyOn(client, 'POST').mockReset()
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('useHighScaleRequestFacts', () => {
  it('advances and rewinds with server-issued cursors', async () => {
    vi.mocked(client.POST)
      .mockResolvedValueOnce({ data: pageOne, error: undefined } as never)
      .mockResolvedValueOnce({ data: pageTwo, error: undefined } as never)

    const { result } = renderHook(
      () => useHighScaleRequestFacts({
        serviceId: 'service-1',
        startTime: '2026-09-11T00:00:00Z',
        endTime: '2026-09-11T01:00:00Z',
        pageSize: 1,
      }),
      { wrapper },
    )

    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-1'))
    expect(result.current.pageIndex).toBe(0)
    expect(result.current.canNextPage).toBe(true)

    await act(async () => {
      result.current.nextPage()
    })
    await waitFor(() => expect(result.current.data?.rows[0]?.event_id).toBe('event-2'))
    expect(result.current.pageIndex).toBe(1)
    expect(result.current.canPreviousPage).toBe(true)

    const calls = vi.mocked(client.POST).mock.calls
    expect((calls[0]?.[1] as { body: { cursor: string | null } }).body.cursor).toBeNull()
    expect((calls[1]?.[1] as { body: { cursor: string | null } }).body.cursor).toBe('cursor-1')

    await act(async () => {
      result.current.previousPage()
    })
    expect(result.current.pageIndex).toBe(0)
  })
})
