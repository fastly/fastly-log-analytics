import { fireEvent, render, screen, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter } from 'nuqs/adapters/testing'
import { afterEach, expect, it, vi } from 'vitest'
import SessionDetailPage from '@/app/sessions/detail/page'
import SessionDetailModal from '@/components/SessionDetailModal'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'

const { post, routerPush } = vi.hoisted(() => ({ post: vi.fn(), routerPush: vi.fn() }))
let search = 'token=session-key&service=selected-service'

vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams(search),
  usePathname: () => '/sessions/detail',
  useRouter: () => ({ push: routerPush }),
}))
vi.mock('@/hooks/useBootstrap', async (original) => ({
  ...await original<typeof import('@/hooks/useBootstrap')>(),
  useEffectiveServiceId: () => 'fallback-service',
  useBootstrapResolved: () => true,
}))
vi.mock('@/lib/api', async (original) => ({
  ...await original<typeof import('@/lib/api')>(),
  client: { POST: post },
}))
vi.mock('@/components/DataTable', () => ({
  DataTable: ({ data }: { data: { timestamp: string; status?: number }[] }) => (
    <ol aria-label="Request records">
      {data.filter((row) => row.status !== undefined).map((row) => <li key={`${row.timestamp}:${row.status}`}>{row.status}</li>)}
    </ol>
  ),
}))
vi.mock('@/app/streaming/_sections/StreamingClient', () => ({}))

afterEach(() => {
  search = 'token=session-key&service=selected-service'
  post.mockReset()
  setCloudModeRuntime(false)
  sessionStorage.clear()
})

function openPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(<QueryClientProvider client={client}><SessionDetailPage /></QueryClientProvider>)
}

function request(at: number, status: number, overrides = {}) {
  return {
    timestamp: new Date(Date.UTC(2026, 10, 18, 19, 0, at)).toISOString(),
    status, url: '/checkout', host: 'store.example', method: 'GET', edge: '1',
    ...overrides,
  }
}

it('uses the link service and filters evidence without confusing shield success with recovery', async () => {
  post.mockResolvedValue({
    data: {
      columns: ['timestamp', 'status', 'url', 'host', 'method', 'edge'],
      data: [
        request(29, 200, { elapsed: 100_000 }),
        request(13, 200, { edge: '0' }),
        request(12, 503, { elapsed: 18_000_000 }),
        request(15, 200, { host: 'other.example' }),
        request(18, 502),
      ],
    },
  })
  openPage()
  const recovery = await screen.findByRole('button', { name: /later 2xx started at \+17s/i })
  expect(post).toHaveBeenCalledWith('/api/sessions/detail', expect.objectContaining({
    body: { session_token: 'session-key' },
    headers: { 'x-service-id': 'selected-service' },
  }))
  fireEvent.click(recovery)
  expect(within(screen.getByRole('list', { name: 'Request records' })).getAllByRole('listitem')
    .map((item) => item.textContent)).toEqual(['503', '502', '200'])
})

it('does not request data without a token', () => {
  search = ''
  openPage()
  expect(screen.getByRole('status')).toHaveTextContent('No session token')
  expect(post).not.toHaveBeenCalled()
})

it('cannot report a clean session from capped rows with no logged status', async () => {
  post.mockResolvedValue({
    data: { columns: ['timestamp'], data: Array.from({ length: 500 }, (_, i) => ({ timestamp: request(i, 0).timestamp })) },
  })
  openPage()
  expect(await screen.findByText('Partial data')).toBeInTheDocument()
  expect(screen.getByText('No HTTP statuses were logged.')).toBeInTheDocument()
  expect(screen.queryByText(/no HTTP errors observed/i)).not.toBeInTheDocument()
})

it.each([
  [false, 'Session Details', '/sessions/detail?token=key%2B%2F&service=selected-service'],
  [true, 'Stream Details', '/sessions/stream?token=key%2B%2F&service=selected-service'],
])('uses the right modal link for is_streaming=%s', (isStreaming, label, href) => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  post.mockResolvedValue({ data: { columns: [], data: [] } })
  render(
    <QueryClientProvider client={client}>
      <NuqsTestingAdapter>
        <SessionDetailModal
          selectedSession={{
            ip: '192.0.2.1', session_token: 'key+/', session_start: request(12, 200).timestamp,
            session_end: request(29, 200).timestamp, is_streaming: isStreaming, req_count: 2, flagged: false,
          }}
          activeServiceId="selected-service" setSelectedSession={vi.fn()} data={undefined}
          labels={[]} labelBySid={new Map()} onFlagged={vi.fn()}
        />
      </NuqsTestingAdapter>
    </QueryClientProvider>,
  )
  expect(screen.getByRole('link', { name: label })).toHaveAttribute('href', href)
  expect(screen.queryByRole('link', { name: isStreaming ? 'Session Details' : 'Stream Details' })).not.toBeInTheDocument()
})

it('keeps the Cloud token out of the link but resolves it in the tab that opened it', async () => {
  setCloudModeRuntime(true)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const token = 'cloud-token-containing-client-identifiers'
  post.mockResolvedValue({ data: { columns: ['status'], data: [request(12, 200)] } })
  const modal = render(
    <QueryClientProvider client={client}>
      <NuqsTestingAdapter>
        <SessionDetailModal
          selectedSession={{
            ip: '192.0.2.1', session_token: token, session_start: request(12, 200).timestamp,
            session_end: request(12, 200).timestamp, is_streaming: false, req_count: 1, flagged: false,
          }}
          activeServiceId="selected-service" setSelectedSession={vi.fn()} data={undefined}
          labels={[]} labelBySid={new Map()} onFlagged={vi.fn()}
        />
      </NuqsTestingAdapter>
    </QueryClientProvider>,
  )
  const link = screen.getByRole('link', { name: 'Session Details' })
  const href = link.getAttribute('href') ?? ''
  expect(href).toMatch(/^\/sessions\/detail\?ref=[a-f0-9-]+&service=selected-service$/)
  expect(href).not.toContain(token)
  link.parentElement?.addEventListener('click', (event) => event.preventDefault())
  fireEvent.click(link)
  modal.unmount()
  post.mockClear()
  search = href.split('?')[1]
  openPage()
  expect(await screen.findByRole('heading', { name: 'Request trace' })).toBeInTheDocument()
  expect(post).toHaveBeenCalledWith('/api/sessions/detail', expect.objectContaining({ body: { session_token: token } }))
})
