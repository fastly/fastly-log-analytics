/**
 * @vitest-environment jsdom
 *
 * SessionsTable renders the sessions DataTable with a column-set that
 * depends on the SessionsResponse capability flags (has_rtt, has_ja4,
 * has_edge, has_edge_sid). It's a presentation component — the parent
 * supplies the data and the row-click callback. Tests:
 *   - presence of canonical columns (IP / Country / Requests / 4xx%)
 *   - row count matches data.sessions
 *   - row click fires onRowClick with the SessionRow object
 *   - conditional columns appear only when the matching has_* flag is set
 *
 * We mock @tanstack/react-virtual the same way DataTable.test.tsx does so
 * the virtualizer enumerates every row (jsdom has no layout, so the
 * default measureElement returns 0 and the virtualizer would render
 * nothing).
 */
import { render, screen, fireEvent, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import React from 'react'

import { SessionsTable } from '@/app/sessions/_sections/SessionsTable'
import type { components } from '@/types/api.generated'

type SessionRow = components['schemas']['Session']
type SessionsResponse = components['schemas']['SessionsResponse']

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

// FlagSessionPopover ultimately POSTs to the labels endpoint; stub it so
// the table doesn't try to set up popover state for each row. We
// deliberately avoid the literal text "Flag" here so it doesn't collide
// with the column header of the same name.
vi.mock('@/components/SessionScoring/FlagSessionPopover', () => ({
  FlagSessionPopover: () => <span data-testid="flag-popover" />,
}))

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

afterEach(() => cleanup())

function makeRow(overrides: Partial<SessionRow> = {}): SessionRow {
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
    ja4: 't13d1715h2_5b57614c22b0_3d5424432f57',
    ua: 'curl/7.85.0',
    edge_sid: null,
    flagged: false,
    ...overrides,
  }
}

function makeResponse(
  rows: SessionRow[],
  flags: Partial<Pick<SessionsResponse, 'has_rtt' | 'has_ja4' | 'has_edge' | 'has_edge_sid'>> = {},
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
    min_reqs_flag: 0,
    min_4xx_pct_flag: 0,
    _is_cached: false,
    ...flags,
  }
}

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
    // The Details button passes the row to onRowClick AND the click
    // event bubbles to DataTable's row-level handler — both fire with
    // the same row object. We assert at least one invocation with the
    // correct row rather than pinning the exact count to whichever
    // handler order DataTable picks.
    expect(onRowClick).toHaveBeenCalled()
    expect(onRowClick.mock.calls[0][0]).toMatchObject({ ip: '10.0.0.42' })
  })

  it('renders the flagged-icon when row.flagged=true', () => {
    const row = makeRow({ ip: '10.0.0.7', flagged: true })
    const { container } = render(
      <SessionsTable {...baseProps({ data: makeResponse([row]) })} />,
    )
    // The flagged span has title="Flagged Session"
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
    // The FlagSessionPopover stub renders once per edge_sid row.
    expect(screen.getAllByTestId('flag-popover').length).toBeGreaterThan(0)
  })

  it('renders the empty state when data has no sessions', () => {
    render(<SessionsTable {...baseProps({ data: makeResponse([]) })} />)
    // DataTable's default empty-state copy; the table still renders headers.
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })

  it('renders even when data is undefined (initial fetch)', () => {
    render(<SessionsTable {...baseProps({ data: undefined })} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })
})
