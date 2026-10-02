import { render } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import QualityTab from '@/app/streaming/_sections/tabs/QualityTab'
import { client } from '@/lib/api'
import { createTestQueryClient } from '../../helpers/query'
import { spyOnConsoleError } from '../../helpers/page-smoke'

vi.mock('@/stores/serviceStore', async () => (await import('../../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../../helpers/page-smoke')).navigationModuleMock('/streaming'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn(), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => null }))
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => null }))

const postMock = vi.mocked(client.POST)
let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  postMock.mockReset()
  // The aggregates request never settles, so it is still in flight at unmount.
  postMock.mockImplementation(() => new Promise(() => {}) as never)
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

const PROPS = {
  activeServiceId: 'test-svc',
  filterPayload: {},
  startTime: '2026-01-01T00:00:00Z',
  endTime: '2026-01-01T01:00:00Z',
  relativeRange: null,
  isAutoRange: false,
  anchor: '2026-01-01T01:00:00Z',
  timezone: 'UTC',
}

test('leaving the Quality tab does not cancel the Content Security fetch', async () => {
  // REGRESSION: the unmount cleanup cancelled every ['cmcd'] query, which
  // includes ['cmcd', 'content-security', ...] — in flight at the moment the
  // user switches tabs. A cancelled first fetch reverts to "no data, not
  // loading", so Content Security showed empty cards until its 30 s refetch.
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  const view = render(
    <QueryClientProvider client={queryClient}>
      <QualityTab {...PROPS} />
    </QueryClientProvider>,
  )

  let resolveSecurity!: (v: { ok: true }) => void
  const securityKey = ['cmcd', 'content-security', 'test-svc']
  const securityFetch = queryClient.fetchQuery({
    queryKey: securityKey,
    queryFn: ({ signal }) =>
      new Promise<{ ok: true }>((resolve, reject) => {
        resolveSecurity = resolve
        signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')))
      }),
  })

  view.unmount() // user switched to Content Security
  resolveSecurity({ ok: true })

  await expect(securityFetch).resolves.toEqual({ ok: true })
  expect(queryClient.getQueryData(securityKey)).toEqual({ ok: true })
})

test('leaving the Quality tab still cancels its own aggregates request', async () => {
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  const cancelSpy = vi.spyOn(queryClient, 'cancelQueries')
  const view = render(
    <QueryClientProvider client={queryClient}>
      <QualityTab {...PROPS} />
    </QueryClientProvider>,
  )

  view.unmount()

  expect(cancelSpy).toHaveBeenCalledWith({ queryKey: ['cmcd', 'aggregates'] })
  expect(cancelSpy).not.toHaveBeenCalledWith({ queryKey: ['cmcd'] })
})
