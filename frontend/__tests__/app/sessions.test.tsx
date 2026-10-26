/**
 * @vitest-environment jsdom
 *
 * Consolidated Sessions test suite: SessionDetail, SessionsTable, and ScoringControls.
 */
import { stubResizeObserver, renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { render, screen, fireEvent, cleanup, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import React from 'react'

import { SessionDetail, SessionsTable, ScoringControls } from '@/app/sessions/page'
import type { components } from '@/types/api.generated'
import type { SessionsResponse } from '@/types/api'

type SessionRow = components['schemas']['Session']

const postMock = vi.fn()

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    POST: (...args: unknown[]) => postMock(...args),
    GET: vi.fn(),
    use: vi.fn(),
  },
  extractApiError: (e: unknown) => String(e),
}))

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useFieldLabel: () => (id: string) => id,
}))

vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/components/SessionScoring/TopFlaggedTable', () => ({
  FlagSessionPopover: () => <span data-testid="flag-popover" />,
}))

beforeAll(() => {
  stubResizeObserver()
})

afterEach(() => {
  cleanup()
  postMock.mockReset()
})

function makeRow(overrides: Partial<SessionRow> = {}): SessionRow {
  return {
    ip: '10.0.0.1',
    country: 'US',
    asn: 7922,
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
    ja4: 't13d1715h2_5b57614c22b0_3d5424432f57',
    ua: 'curl/7.85.0',
    edge_sid: null,
    flagged: false,
    ...overrides,
  }
}

function makeResponse(
  rows: SessionRow[] = [],
  flags: Partial<Pick<SessionsResponse, 'has_rtt' | 'has_ja4' | 'has_edge' | 'has_edge_sid' | 'has_cmcd'>> = {},
): SessionsResponse {
  return {
    sessions: rows,
    asn_names: {},
    total: rows.length,
    page: 1,
    limit: 100,
    has_rtt: false,
    has_ja4: false,
    has_edge: false,
    has_edge_sid: false,
    has_cmcd: false,
    min_reqs_flag: 0,
    min_4xx_pct_flag: 0,
    _is_cached: false,
    ...flags,
  }
}

function renderSessionDetail(ui: React.ReactElement) {
  return renderWithQuery(ui, { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } })
}

describe('SessionDetail', () => {
  it('does not render the dialog when selectedSession is null', () => {
    renderSessionDetail(
      <SessionDetail
        selectedSession={null}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.queryByText(/^Session:/)).toBeNull()
    expect(postMock).not.toHaveBeenCalled()
  })

  it('renders the session header + metadata when a session is selected', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow({ ip: '10.0.0.42', country: 'CA' })}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.getByText(/Session:/)).toBeInTheDocument()
    expect(screen.getByText('10.0.0.42')).toBeInTheDocument()
    expect(screen.getByText('CA')).toBeInTheDocument()
    expect(screen.getByText('AS7922')).toBeInTheDocument()
    expect(screen.getByText('100')).toBeInTheDocument()
  })

  it('issues a POST to /api/sessions/detail with the session window', async () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const session = makeRow({
      ip: '10.0.0.50',
      ja4: 'abc_def_ghi',
      session_start: '2026-06-15T00:00:00Z',
      session_end: '2026-06-15T01:00:00Z',
    })
    renderSessionDetail(
      <SessionDetail
        selectedSession={session}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
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
    renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow()}
        setSelectedSession={vi.fn()}
        activeServiceId={null}
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
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
    renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow()}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.getByText(/Request Timeline/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.getAllByText(/example\.com/).length).toBeGreaterThan(0))
    expect(screen.getByText('/a')).toBeInTheDocument()
    expect(screen.getByText('/b')).toBeInTheDocument()
    expect(screen.getByText('200')).toBeInTheDocument()
    expect(screen.getByText('404')).toBeInTheDocument()
  })

  it('shows the Edge-only switch only when data.has_edge=true', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const { rerender } = renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow()}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse([], { has_edge: false })}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.queryByText(/Edge only/i)).toBeNull()

    rerender(
      <SessionDetail
        selectedSession={makeRow()}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse([], { has_edge: true })}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.getByText(/Edge only/i)).toBeInTheDocument()
  })

  it('renders the FlagSessionPopover in the header when edge_sid is present', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow({ edge_sid: 'sid-abc' })}
        setSelectedSession={vi.fn()}
        activeServiceId="svc-1"
        data={makeResponse()}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    expect(screen.getByTestId('flag-popover')).toBeInTheDocument()
  })

  it('UX-5: shows an inline error + Retry (not a bare empty table) when the detail query 5xxs', async () => {
    postMock.mockRejectedValueOnce(new Error('detail boom'))
    renderSessionDetail(
      <SessionDetail
        selectedSession={makeRow({ ip: '1.2.3.4' })}
        setSelectedSession={vi.fn()}
        activeServiceId="test-svc"
        data={undefined}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={vi.fn()}
      />,
    )
    await waitFor(
      () => {
        expect(screen.getByRole('alert')).toHaveTextContent(/failed to load session detail/i)
      },
      { timeout: 10000 },
    )
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  }, 15000)
})

function baseProps(
  overrides: Partial<React.ComponentProps<typeof SessionsTable>> = {},
): React.ComponentProps<typeof SessionsTable> {
  return {
    data: makeResponse([makeRow()]),
    activeServiceId: 'svc-1',
    isLoadingInitial: false,
    isFetching: false,
    labels: [],
    labelBySid: new Map(),
    idBySid: new Map(),
    onFlagged: vi.fn(),
    onRowClick: vi.fn(),
    ...overrides,
  }
}

describe('SessionsTable', () => {
  it('renders the canonical column headers', () => {
    render(<SessionsTable {...baseProps()} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
    expect(screen.getByText('Country')).toBeInTheDocument()
    expect(screen.getByText('Requests')).toBeInTheDocument()
    expect(screen.getByText('4xx%')).toBeInTheDocument()
    expect(screen.getByText('User-Agent')).toBeInTheDocument()
  })

  it('renders one row per session in data.sessions', () => {
    const rows = [
      makeRow({ ip: '10.0.0.1' }),
      makeRow({ ip: '10.0.0.2', country: 'CA' }),
      makeRow({ ip: '10.0.0.3', country: 'GB' }),
    ]
    render(<SessionsTable {...baseProps({ data: makeResponse(rows) })} />)
    expect(screen.getByText('10.0.0.1')).toBeInTheDocument()
    expect(screen.getByText('10.0.0.2')).toBeInTheDocument()
    expect(screen.getByText('10.0.0.3')).toBeInTheDocument()
  })

  it('clicking the row-level Details button opens the detail panel via onRowClick', () => {
    const onRowClick = vi.fn()
    const row = makeRow({ ip: '10.0.0.42' })
    render(<SessionsTable {...baseProps({ data: makeResponse([row]), onRowClick })} />)
    const detailsBtn = screen.getByRole('button', { name: /details/i })
    fireEvent.click(detailsBtn)
    expect(onRowClick).toHaveBeenCalled()
    expect(onRowClick.mock.calls[0][0]).toMatchObject({ ip: '10.0.0.42' })
  })

  it('renders the flagged-icon when row.flagged=true', () => {
    const row = makeRow({ ip: '10.0.0.7', flagged: true })
    const { container } = render(
      <SessionsTable {...baseProps({ data: makeResponse([row]) })} />,
    )
    expect(container.querySelector('[title="Flagged Session"]')).toBeInTheDocument()
  })

  it('omits RTT / JA4 / Edge columns when their has_* flags are false', () => {
    render(<SessionsTable {...baseProps()} />)
    expect(screen.queryByText('Med. RTT')).toBeNull()
    expect(screen.queryByText('JA4')).toBeNull()
    expect(screen.queryByText('Edge / Shield')).toBeNull()
    expect(screen.queryByText('Flag')).toBeNull()
  })

  it('adds RTT / JA4 / Edge columns when their has_* flags are true', () => {
    const row = makeRow({ edge_sid: 'sid-abc' })
    render(
      <SessionsTable
        {...baseProps({
          data: makeResponse([row], {
            has_rtt: true,
            has_ja4: true,
            has_edge: true,
            has_edge_sid: true,
          }),
        })}
      />,
    )
    expect(screen.getByText('Med. RTT')).toBeInTheDocument()
    expect(screen.getByText('JA4')).toBeInTheDocument()
    expect(screen.getByText('Edge / Shield')).toBeInTheDocument()
    expect(screen.getByText('Flag')).toBeInTheDocument()
    expect(screen.getAllByTestId('flag-popover').length).toBeGreaterThan(0)
  })

  it('renders the empty state when data has no sessions', () => {
    render(<SessionsTable {...baseProps({ data: makeResponse([]) })} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })

  it('renders even when data is undefined (initial fetch)', () => {
    render(<SessionsTable {...baseProps({ data: undefined })} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })
})

function controlsBaseProps(overrides: Partial<React.ComponentProps<typeof ScoringControls>> = {}) {
  return {
    cmcdEnabled: false,
    flaggedOnly: false,
    setFlaggedOnly: vi.fn(),
    streamingOnly: false,
    setStreamingOnly: vi.fn(),
    minReqs: '' as number | '',
    setMinReqs: vi.fn(),
    min4xxPct: '' as number | '',
    setMin4xxPct: vi.fn(),
    data: { min_reqs_flag: 1000, min_4xx_pct_flag: 20 },
    isFetching: false,
    isLoadingInitial: false,
    refetch: vi.fn(),
    ...overrides,
  }
}

describe('ScoringControls', () => {
  it('renders the flagged-only switch label and the threshold inputs', () => {
    render(<ScoringControls {...controlsBaseProps()} />)
    expect(screen.getByText(/flagged only/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. requests/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. 4xx%/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument()
  })

  it('uses data.min_reqs_flag / data.min_4xx_pct_flag as placeholder hints', () => {
    render(
      <ScoringControls
        {...controlsBaseProps({ data: { min_reqs_flag: 5000, min_4xx_pct_flag: 35 } })}
      />,
    )
    expect(screen.getByPlaceholderText(/≥\s*5000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*35/)).toBeInTheDocument()
  })

  it('falls back to default placeholders when data is undefined', () => {
    render(<ScoringControls {...controlsBaseProps({ data: undefined })} />)
    expect(screen.getByPlaceholderText(/≥\s*1000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*20/)).toBeInTheDocument()
  })

  it('fires setMinReqs with a number when the requests input changes', () => {
    const setMinReqs = vi.fn()
    render(<ScoringControls {...controlsBaseProps({ setMinReqs })} />)
    const reqInput = screen.getByPlaceholderText(/≥\s*1000/) as HTMLInputElement
    fireEvent.change(reqInput, { target: { value: '250' } })
    expect(setMinReqs).toHaveBeenCalledWith(250)
  })

  it('fires setMinReqs("") when the requests input is cleared', () => {
    const setMinReqs = vi.fn()
    render(<ScoringControls {...controlsBaseProps({ setMinReqs, minReqs: 250 })} />)
    const reqInput = screen.getByPlaceholderText(/≥\s*1000/) as HTMLInputElement
    fireEvent.change(reqInput, { target: { value: '' } })
    expect(setMinReqs).toHaveBeenCalledWith('')
  })

  it('fires setMin4xxPct with a number when the 4xx input changes', () => {
    const setMin4xxPct = vi.fn()
    render(<ScoringControls {...controlsBaseProps({ setMin4xxPct })} />)
    const pctInput = screen.getByPlaceholderText(/≥\s*20/) as HTMLInputElement
    fireEvent.change(pctInput, { target: { value: '40' } })
    expect(setMin4xxPct).toHaveBeenCalledWith(40)
  })

  it('fires setFlaggedOnly when the switch is toggled', async () => {
    const setFlaggedOnly = vi.fn()
    render(<ScoringControls {...controlsBaseProps({ setFlaggedOnly })} />)
    const sw = screen.getByRole('switch')
    const user = userEvent.setup()
    await user.click(sw)
    expect(setFlaggedOnly).toHaveBeenCalledTimes(1)
    expect(setFlaggedOnly.mock.calls[0][0]).toBe(true)
  })

  it('does not show "Clear filters" when no filter is active', () => {
    render(<ScoringControls {...controlsBaseProps()} />)
    expect(screen.queryByRole('button', { name: /clear filters/i })).toBeNull()
  })

  it('shows "Clear filters" once any threshold is set and resets all three on click', () => {
    const setFlaggedOnly = vi.fn()
    const setMinReqs = vi.fn()
    const setMin4xxPct = vi.fn()
    render(
      <ScoringControls
        {...controlsBaseProps({
          flaggedOnly: true,
          minReqs: 250,
          min4xxPct: 40,
          setFlaggedOnly,
          setMinReqs,
          setMin4xxPct,
        })}
      />,
    )
    const clearBtn = screen.getByRole('button', { name: /clear filters/i })
    fireEvent.click(clearBtn)
    expect(setFlaggedOnly).toHaveBeenCalledWith(false)
    expect(setMinReqs).toHaveBeenCalledWith('')
    expect(setMin4xxPct).toHaveBeenCalledWith('')
  })

  it('calls refetch when the Refresh button is clicked', () => {
    const refetch = vi.fn()
    render(<ScoringControls {...controlsBaseProps({ refetch })} />)
    fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    expect(refetch).toHaveBeenCalledTimes(1)
  })

  it('disables the Refresh button while isFetching=true', () => {
    render(<ScoringControls {...controlsBaseProps({ isFetching: true })} />)
    expect(screen.getByRole('button', { name: /refresh/i })).toBeDisabled()
  })

  it('dims the control bar while a background refetch is running', () => {
    const { container } = render(
      <ScoringControls {...controlsBaseProps({ isFetching: true, isLoadingInitial: false })} />,
    )
    expect(container.querySelector('.opacity-40')).toBeInTheDocument()
  })

  it('renders the streaming sessions switch when cmcdEnabled is true', () => {
    render(<ScoringControls {...controlsBaseProps({ cmcdEnabled: true })} />)
    expect(screen.getByText(/streaming sessions/i)).toBeInTheDocument()
  })

  it('does not render the streaming sessions switch when cmcdEnabled is false', () => {
    render(<ScoringControls {...controlsBaseProps({ cmcdEnabled: false })} />)
    expect(screen.queryByText(/streaming sessions/i)).toBeNull()
  })
})
