import { render, screen, fireEvent, act } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { LiveStreamingClient } from '@/app/streaming/_sections/LiveStreamingClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// Mocks for streaming page
vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/streaming'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

// Mock useEventStream so we can control connection states and emit mock events
let mockOnEvent: ((raw: string) => void) | null = null
let mockConnectionState: 'idle' | 'connecting' | 'open' | 'reconnecting' = 'open'

vi.mock('@/hooks/useEventStream', () => ({
  useEventStream: vi.fn((enabled: boolean, req: unknown, onEvent: (raw: string) => void) => {
    mockOnEvent = onEvent
    return { state: mockConnectionState }
  }),
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    activeServiceId: 'test-svc',
    timezone: 'UTC',
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
  mockConnectionState = 'open'
  mockOnEvent = null
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('streaming page mounts and renders title, toolbar controls, and terminal layout', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Live Log Streaming')).toBeInTheDocument()
  expect(screen.getByTestId('stream-controls-toolbar')).toBeInTheDocument()
  expect(screen.getByTestId('stream-terminal-container')).toBeInTheDocument()
  expect(screen.getByTestId('stream-pause-toggle')).toBeInTheDocument()
  expect(screen.getByTestId('stream-search-input')).toBeInTheDocument()
  expect(screen.getByTestId('stream-autoscroll-toggle')).toBeInTheDocument()
  expect(screen.getByTestId('stream-clear-logs')).toBeInTheDocument()
  expect(screen.getByText('TIMESTAMP')).toBeInTheDocument()
  expect(screen.getByText('CLIENT IP')).toBeInTheDocument()
  expect(screen.getByText('URL PATH')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('renders connecting skeleton state when connection is pending', () => {
  mockConnectionState = 'connecting'
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  expect(screen.getByTestId('stream-connecting-skeleton')).toBeInTheDocument()
  expect(screen.getByText(/Connecting to live Fastly log stream/i)).toBeInTheDocument()
})

test('receives and renders SSE log events in real time', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  const sampleEvent1 = {
    event_id: 'ev-1',
    timestamp: '2026-10-16T12:00:00Z',
    client_ip: '198.51.100.10',
    method: 'GET',
    url: '/api/v1/health',
    status: 200,
    pop: 'SJC',
    time_elapsed: 0.015,
    attributes: { service_id: 'test-svc', resp_bytes: 420 },
  }

  const sampleEvent2 = {
    event_id: 'ev-2',
    timestamp: '2026-10-16T12:00:01Z',
    client_ip: '203.0.113.5',
    method: 'POST',
    url: '/checkout/pay',
    status: 503,
    pop: 'LCY',
    time_elapsed: 0.25,
    attributes: { service_id: 'test-svc', error: 'backend_timeout' },
  }

  act(() => {
    mockOnEvent?.(JSON.stringify(sampleEvent1))
    mockOnEvent?.(JSON.stringify(sampleEvent2))
  })

  const rows = screen.getAllByTestId('stream-log-row')
  expect(rows.length).toBe(2)
  expect(screen.getByText('/api/v1/health')).toBeInTheDocument()
  expect(screen.getByText('/checkout/pay')).toBeInTheDocument()
  expect(screen.getByText('503')).toBeInTheDocument()
  expect(screen.getByText('SJC')).toBeInTheDocument()
  expect(screen.getByText('LCY')).toBeInTheDocument()
})

test('filters streamed logs by live search regex / string and status code', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  act(() => {
    mockOnEvent?.(
      JSON.stringify({
        event_id: 'ev-1',
        timestamp: '2026-10-16T12:00:00Z',
        client_ip: '198.51.100.10',
        method: 'GET',
        url: '/assets/logo.png',
        status: 200,
        pop: 'SJC',
        time_elapsed: 0.005,
        attributes: {},
      }),
    )
    mockOnEvent?.(
      JSON.stringify({
        event_id: 'ev-2',
        timestamp: '2026-10-16T12:00:01Z',
        client_ip: '198.51.100.20',
        method: 'POST',
        url: '/login',
        status: 401,
        pop: 'SJC',
        time_elapsed: 0.045,
        attributes: {},
      }),
    )
  })

  expect(screen.getAllByTestId('stream-log-row').length).toBe(2)

  // Filter by string query
  const searchInput = screen.getByTestId('stream-search-input')
  fireEvent.change(searchInput, { target: { value: 'logo.png' } })

  expect(screen.getAllByTestId('stream-log-row').length).toBe(1)
  expect(screen.getByText('/assets/logo.png')).toBeInTheDocument()
  expect(screen.queryByText('/login')).not.toBeInTheDocument()

  // Clear search filter
  fireEvent.change(searchInput, { target: { value: '' } })
  expect(screen.getAllByTestId('stream-log-row').length).toBe(2)

  // Filter by status 4xx
  const status4xxButton = screen.getByRole('button', { name: '4XX' })
  fireEvent.click(status4xxButton)

  expect(screen.getAllByTestId('stream-log-row').length).toBe(1)
  expect(screen.getByText('/login')).toBeInTheDocument()
  expect(screen.queryByText('/assets/logo.png')).not.toBeInTheDocument()
})

test('clicking a log row opens the Deep JSON Row Inspector modal with attributes', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  const eventPayload = {
    event_id: 'ev-inspect-1',
    timestamp: '2026-10-16T12:00:00Z',
    client_ip: '198.51.100.99',
    method: 'GET',
    url: '/catalog/products',
    status: 200,
    pop: 'HND',
    time_elapsed: 0.032,
    attributes: {
      req_header_host: 'api.example.com',
      resp_header_cache: 'HIT',
      shield: 'iad',
    },
  }

  act(() => {
    mockOnEvent?.(JSON.stringify(eventPayload))
  })

  const row = screen.getByTestId('stream-log-row')
  fireEvent.click(row)

  expect(screen.getByTestId('stream-row-inspector-modal')).toBeInTheDocument()
  expect(screen.getByText('req_header_host')).toBeInTheDocument()
  expect(screen.getByText('api.example.com')).toBeInTheDocument()
  expect(screen.getByText('resp_header_cache')).toBeInTheDocument()
  expect(screen.getByText('HIT')).toBeInTheDocument()
  expect(screen.getByTestId('stream-copy-json')).toBeInTheDocument()
})

test('pause toggle buffers incoming logs and flushes when resumed', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  const pauseButton = screen.getByTestId('stream-pause-toggle')
  expect(pauseButton).toHaveTextContent(/Pause/i)

  // Pause stream
  fireEvent.click(pauseButton)
  expect(pauseButton).toHaveTextContent(/Resume/i)

  // Emit event while paused
  act(() => {
    mockOnEvent?.(
      JSON.stringify({
        event_id: 'ev-paused-1',
        timestamp: '2026-10-16T12:00:00Z',
        client_ip: '1.2.3.4',
        method: 'GET',
        url: '/queued-while-paused',
        status: 200,
        pop: 'SJC',
        time_elapsed: 0.01,
        attributes: {},
      }),
    )
  })

  // In paused state, log row is not yet rendered to terminal
  expect(screen.queryByText('/queued-while-paused')).not.toBeInTheDocument()

  // Resume stream
  fireEvent.click(pauseButton)
  expect(pauseButton).toHaveTextContent(/Pause/i)

  // Now the buffered log row is flushed and rendered
  expect(screen.getByText('/queued-while-paused')).toBeInTheDocument()
})

test('clear logs button empties the terminal display', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )

  act(() => {
    mockOnEvent?.(
      JSON.stringify({
        event_id: 'ev-clear-1',
        timestamp: '2026-10-16T12:00:00Z',
        client_ip: '1.1.1.1',
        method: 'GET',
        url: '/to-be-cleared',
        status: 200,
        pop: 'SJC',
        time_elapsed: 0.01,
        attributes: {},
      }),
    )
  })

  expect(screen.getByText('/to-be-cleared')).toBeInTheDocument()

  const clearButton = screen.getByTestId('stream-clear-logs')
  fireEvent.click(clearButton)

  expect(screen.queryByText('/to-be-cleared')).not.toBeInTheDocument()
})

const openInspector = () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LiveStreamingClient />
    </QueryClientProvider>,
  )
  act(() => {
    mockOnEvent?.(
      JSON.stringify({ event_id: 'ev-copy-1', timestamp: '2026-10-16T12:00:00Z', method: 'GET', url: '/copy', status: 200, pop: 'HND', time_elapsed: 0.01, attributes: {} }),
    )
  })
  fireEvent.click(screen.getByTestId('stream-log-row'))
}

test('Copy JSON flips to Copied after a successful write and resets', async () => {
  const writeText = vi.fn().mockResolvedValue(undefined)
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  openInspector()
  vi.useFakeTimers()
  try {
    await act(async () => {
      fireEvent.click(screen.getByTestId('stream-copy-json'))
    })
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(screen.getByTestId('stream-copy-json')).toHaveTextContent('Copied')
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(screen.getByTestId('stream-copy-json')).toHaveTextContent('Copy JSON')
  } finally {
    vi.useRealTimers()
  }
})

test('Copy JSON does not claim success when the clipboard write is rejected', async () => {
  Object.defineProperty(navigator, 'clipboard', { value: { writeText: vi.fn().mockRejectedValue(new Error('denied')) }, configurable: true })
  openInspector()
  await act(async () => {
    fireEvent.click(screen.getByTestId('stream-copy-json'))
  })
  expect(screen.getByTestId('stream-copy-json')).toHaveTextContent('Copy JSON')
})
