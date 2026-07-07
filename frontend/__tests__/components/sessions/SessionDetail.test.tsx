/**
 * @vitest-environment jsdom
 *
 * SessionDetail is the right-side dialog that appears when the analyst
 * clicks a row in the sessions table. It:
 *   - is wired to a controlled `selectedSession` prop (open = !!selected)
 *   - fetches /api/sessions/detail via useQuery (POST) once a session is
 *     selected, and renders the returned columns/data as a timeline table
 *   - shows session metadata (start/end/country/asn/requests/etc.)
 *   - exposes an "Edge only" toggle when data.has_edge=true
 *
 * The fetch path is the only async surface — we mock `client.POST` to
 * resolve with a deterministic SessionDetailResponse shape and wait for
 * the timeline to render.
 */
import { render, screen, fireEvent, cleanup, waitFor, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import React from 'react'

import { SessionDetail } from '@/app/sessions/_sections/SessionDetail'
import type { components } from '@/types/api.generated'

type SessionRow = components['schemas']['Session']
type SessionsResponse = components['schemas']['SessionsResponse']

// --- mocks --------------------------------------------------------------

const postMock = vi.fn()

vi.mock('@/lib/api', () => ({
  client: {
    POST: (...args: any[]) => postMock(...args),
    GET: vi.fn(),
    use: vi.fn(),
  },
  extractApiError: (e: unknown) => String(e),
}))

// useFieldLabel pulls a catalog over the network; replace with an
// identity mapper so the timeline columns render predictably.
vi.mock('@/hooks/useFieldLabel', () => ({
  useFieldLabel: () => (id: string) => id,
}))

// Virtualizer needs to enumerate every row for jsdom layout-free tests.
vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: (options: any) => ({
    getVirtualItems: () =>
      Array.from({ length: options.count }).map((_, i) => ({
        index: i,
        start: i * 40,
        end: (i + 1) * 40,
      })),
    getTotalSize: () => options.count * 40,
  }),
}))

vi.mock('@/components/SessionScoring/FlagSessionPopover', () => ({
  FlagSessionPopover: () => <span data-testid="flag-popover" />,
}))

// --- helpers ------------------------------------------------------------

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

afterEach(() => {
  cleanup()
  postMock.mockReset()
})

function makeSession(overrides: Partial<SessionRow> = {}): SessionRow {
  return {
    ip: '10.0.0.1',
    country: 'US',
    asn: 7922,
    asn_label: null,
    session_start: '2026-06-15T00:00:00Z',
    session_end: '2026-06-15T00:30:00Z',
    req_count: 100,
    edge_count: 80,
    shield_count: 20,
    unique_urls: 12,
    reqs_4xx: 5,
    reqs_5xx: 0,
    total_bytes: 50000,
    median_rtt_ms: 23.5,
    ja4: 't13d_5b57_3d54',
    ua: 'curl/7.85.0',
    edge_sid: null,
    flagged: false,
    ...overrides,
  }
}

function makeListResponse(
  flags: Partial<Pick<SessionsResponse, 'has_rtt' | 'has_ja4' | 'has_edge' | 'has_edge_sid'>> = {},
): SessionsResponse {
  return {
    sessions: [],
    asn_names: {},
    total: 0,
    page: 1,
    limit: 100,
    has_rtt: false,
    has_ja4: false,
    has_edge: false,
    has_edge_sid: false,
    min_reqs_flag: 0,
    min_4xx_pct_flag: 0,
    _is_cached: false,
    ...flags,
  }
}

function withProviders(ui: React.ReactNode) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0, staleTime: 0 } },
  })
  return <QueryClientProvider client={qc}>{ui}</QueryClientProvider>
}

// --- tests --------------------------------------------------------------

describe('SessionDetail', () => {
  it('does not render the dialog when selectedSession is null', () => {
    render(
      withProviders(
        <SessionDetail
          selectedSession={null}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    // The dialog title is only rendered when open=true.
    expect(screen.queryByText(/^Session:/)).toBeNull()
    expect(postMock).not.toHaveBeenCalled()
  })

  it('renders the session header + metadata when a session is selected', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    render(
      withProviders(
        <SessionDetail
          selectedSession={makeSession({ ip: '10.0.0.42', country: 'CA' })}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    expect(screen.getByText(/Session:/)).toBeInTheDocument()
    expect(screen.getByText('10.0.0.42')).toBeInTheDocument()
    expect(screen.getByText('CA')).toBeInTheDocument()
    expect(screen.getByText('AS7922')).toBeInTheDocument()
    // The request count gets toLocaleString'd
    expect(screen.getByText('100')).toBeInTheDocument()
  })

  it('issues a POST to /api/sessions/detail with the session window', async () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const session = makeSession({
      ip: '10.0.0.50',
      ja4: 'abc_def_ghi',
      session_start: '2026-06-15T00:00:00Z',
      session_end: '2026-06-15T01:00:00Z',
    })
    render(
      withProviders(
        <SessionDetail
          selectedSession={session}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    await waitFor(() => expect(postMock).toHaveBeenCalled())
    const [path, opts] = postMock.mock.calls[0]
    expect(path).toBe('/api/sessions/detail')
    expect(opts.body).toMatchObject({
      ip: '10.0.0.50',
      ja4: 'abc_def_ghi',
      start_time: '2026-06-15T00:00:00Z',
      end_time: '2026-06-15T01:00:00Z',
    })
  })

  it('skips the fetch when activeServiceId is null (useQuery enabled=false)', async () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    render(
      withProviders(
        <SessionDetail
          selectedSession={makeSession()}
          setSelectedSession={vi.fn()}
          activeServiceId={null}
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    // Give react-query a tick — should still NOT have called POST.
    // Wrap the settle window in act() so the dialog's deferred Radix
    // focus / Next <Link> state update lands inside act (otherwise React
    // 19 logs a "not wrapped in act(...)" warning for that late update).
    await act(async () => {
      await new Promise((r) => setTimeout(r, 30))
    })
    expect(postMock).not.toHaveBeenCalled()
  })

  it('renders the timeline columns + rows once the detail fetch resolves', async () => {
    postMock.mockResolvedValue({
      data: {
        _is_cached: false,
        columns: ['timestamp', 'host', 'url', 'method', 'status'],
        data: [
          { timestamp: '2026-06-15T00:00:00Z', host: 'example.com', url: '/a', method: 'GET', status: 200 },
          { timestamp: '2026-06-15T00:00:05Z', host: 'example.com', url: '/b', method: 'GET', status: 404 },
        ],
      },
    })
    render(
      withProviders(
        <SessionDetail
          selectedSession={makeSession()}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    expect(screen.getByText(/Request Timeline/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.getAllByText(/example\.com/).length).toBeGreaterThan(0))
    expect(screen.getByText('/a')).toBeInTheDocument()
    expect(screen.getByText('/b')).toBeInTheDocument()
    // status column renders the numeric code in a badge.
    expect(screen.getByText('200')).toBeInTheDocument()
    expect(screen.getByText('404')).toBeInTheDocument()
  })

  it('shows the Edge-only switch only when data.has_edge=true', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const { rerender } = render(
      withProviders(
        <SessionDetail
          selectedSession={makeSession()}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse({ has_edge: false })}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    expect(screen.queryByText(/Edge only/i)).toBeNull()

    rerender(
      withProviders(
        <SessionDetail
          selectedSession={makeSession()}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse({ has_edge: true })}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    expect(screen.getByText(/Edge only/i)).toBeInTheDocument()
  })

  it('renders the FlagSessionPopover in the header when edge_sid is present', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    render(
      withProviders(
        <SessionDetail
          selectedSession={makeSession({ edge_sid: 'sid-abc' })}
          setSelectedSession={vi.fn()}
          activeServiceId="svc-1"
          data={makeListResponse()}
          labels={[]}
          labelBySid={new Map()}
          onFlagged={vi.fn()}
        />,
      ),
    )
    expect(screen.getByTestId('flag-popover')).toBeInTheDocument()
  })
})
