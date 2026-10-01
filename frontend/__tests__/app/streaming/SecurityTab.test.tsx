import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import React from 'react'
import SecurityTab from '@/app/streaming/_sections/tabs/SecurityTab'
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

vi.mock('@/components/charts/TimeSeriesChart', () => ({
  TimeSeriesChart: ({ data }: { data: { name: string; y: number[] }[] }) => (
    <div data-testid="bandwidth-chart">
      {data.map((t) => (
        <span key={t.name}>{`${t.name}: ${t.y.join(',')}`}</span>
      ))}
    </div>
  ),
}))

const FULL_RESPONSE = {
  available: true,
  fields: { country: true, referer: true, host: true, resp_bytes: true, edge: true, cmcd_cid: true },
  content_id: null,
  has_shield_split: true,
  content_ids: [
    { content_id: 'movie-1', requests: 3 },
    { content_id: 'movie-2', requests: 2 },
  ],
  top_countries: [{ value: 'US', requests: 3, bytes: 3072 }],
  top_referers: [{ value: 'https://pirate.example/watch', requests: 3, bytes: 3072 }],
  top_hosts: [{ value: 'pirate.example', requests: 1234, bytes: 1048576 }],
  bandwidth_ts: [
    { bucket: '2026-01-01 00:00:00', edge_bytes: 3000, shield_bytes: 300 },
    { bucket: '2026-01-01 00:05:00', edge_bytes: 6000, shield_bytes: 0 },
  ],
}

const postMock = vi.mocked(client.POST)
const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  // cmdk (the content picker's search list) observes its own size.
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  queryClient.clear()
  postMock.mockReset()
  postMock.mockResolvedValue({ data: FULL_RESPONSE } as never)
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  vi.unstubAllGlobals()
  errorSpy.mockRestore()
})

function renderTab(searchParams = '', onUrlUpdate?: (e: UrlUpdateEvent) => void) {
  return render(
    <NuqsTestingAdapter searchParams={searchParams} onUrlUpdate={onUrlUpdate} hasMemory>
      <QueryClientProvider client={queryClient}>
        <SecurityTab
          activeServiceId="test-svc"
          filterPayload={{}}
          startTime="2026-01-01T00:00:00Z"
          endTime="2026-01-01T01:00:00Z"
          relativeRange={null}
          isAutoRange={false}
          anchor="2026-01-01T01:00:00Z"
          timezone="UTC"
          bucketSeconds={300}
        />
      </QueryClientProvider>
    </NuqsTestingAdapter>,
  )
}

function lastBody() {
  const call = postMock.mock.calls.at(-1)
  return (call?.[1] as unknown as { body: Record<string, unknown> }).body
}

test('renders the three leaderboards and the edge vs shield chart', async () => {
  renderTab()

  expect(await screen.findByText('pirate.example')).toBeInTheDocument()
  expect(screen.getByText('1,234')).toBeInTheDocument()
  expect(screen.getByText('1 MB')).toBeInTheDocument()
  expect(screen.getByText('https://pirate.example/watch')).toBeInTheDocument()
  expect(screen.getByText(/United States \(US\)/)).toBeInTheDocument()
  // Card titles follow the Overview page's "Name (Top 10)" style.
  for (const name of ['Countries', 'Referers', 'Hosts']) {
    expect(screen.getByText((_, el) => el?.getAttribute('data-slot') === 'card-title' && el.textContent === `${name} (Top 10)`)).toBeInTheDocument()
  }

  // bytes per 300 s bucket → bits per second.
  const chart = screen.getByTestId('bandwidth-chart')
  expect(within(chart).getByText('Edge (to viewers): 80,160')).toBeInTheDocument()
  expect(within(chart).getByText('Shield (to edge): 8,0')).toBeInTheDocument()

  expect(postMock).toHaveBeenCalledWith('/api/cmcd/content-security', expect.anything())
  expect(lastBody()).toMatchObject({ content_id: null, bucket_seconds: 300, top_n: 10 })
})

test('?content= deep-links to one piece of content', async () => {
  renderTab('?content=movie-2')

  await screen.findByText('pirate.example')
  expect(lastBody()).toMatchObject({ content_id: 'movie-2' })
  expect(screen.getByRole('combobox', { name: 'Content ID' })).toHaveTextContent('movie-2')
})

test('picking a content ID writes ?content= and refetches for it', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderTab('', onUrlUpdate)
  await screen.findByText('pirate.example')

  await userEvent.click(screen.getByRole('combobox', { name: 'Content ID' }))
  await userEvent.click(await screen.findByRole('option', { name: /movie-1/ }))

  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?content=movie-1' }))
  await waitFor(() => expect(lastBody()).toMatchObject({ content_id: 'movie-1' }))

  await userEvent.click(screen.getByRole('button', { name: /clear/i }))
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '' }))
})

test('explains which log fields are missing', async () => {
  postMock.mockResolvedValue({
    data: {
      ...FULL_RESPONSE,
      fields: { ...FULL_RESPONSE.fields, host: false, cmcd_cid: false },
      content_ids: [],
      top_hosts: [],
    },
  } as never)
  renderTab()

  expect(await screen.findByText(/Host field \(Group A\)/)).toBeInTheDocument()
  expect(screen.getByText(/content ID \(cid\)/)).toBeInTheDocument()
  expect(screen.getByRole('combobox', { name: 'Content ID' })).toBeDisabled()
})

test('shows the reason when the service is not supported', async () => {
  postMock.mockResolvedValue({
    data: { available: false, reason: 'Content security metrics are not yet supported for high-scale services.' },
  } as never)
  renderTab()

  expect(await screen.findByText(/not yet supported for high-scale services/)).toBeInTheDocument()
  expect(screen.queryByTestId('bandwidth-chart')).not.toBeInTheDocument()
})
