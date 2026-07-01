/**
 * HeaderAnomaliesSection wraps the request-header-size histogram and the
 * "oversized request headers by IP" table. PlotlyChart and DataTable are
 * mocked so jsdom can render without plotly/d3, and we assert loading,
 * empty, populated, and column-visibility interaction paths.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

const plotlyMock = vi.fn((_props: any) => <div data-testid="plotly-chart" />)
vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: any) => plotlyMock(props),
}))

const dataTableMock = vi.fn((_props: any) => null)
vi.mock('@/components/DataTable', () => ({
  DataTable: (props: any) => {
    dataTableMock(props)
    return (
      <div data-testid={`data-table-${props.data?.length ?? 0}`}>
        {props.data?.length === 0
          ? <span data-testid="empty">{props.emptyMessage}</span>
          : props.data?.map((row: any, i: number) => (
              <div key={i} data-testid="row">{row.ip}</div>
            ))}
      </div>
    )
  },
  ColumnVisibilityDropdown: (props: any) => (
    <button
      type="button"
      data-testid="col-vis"
      onClick={() => props.onChange?.(props.columns[0]?.id, false)}
    >
      cols
    </button>
  ),
}))

vi.mock('@/components/FilterValueCell', () => ({
  FilterValueCell: (props: any) => <span>{props.display ?? props.filters?.[0]?.value}</span>,
}))

// useActiveLogFields chains useBootstrap + useLogFieldsCatalog (both react-query),
// which would need a QueryClientProvider. Mock it instead — both cards here gate
// their empty copy on `req_header_bytes`: inactive → "Requires Request Identity
// (Group A) …", active → neutral "No data in this time range yet." Default to no
// active fields so the not-enabled path is exercised; the neutral test adds the id.
const { activeFields } = vi.hoisted(() => ({ activeFields: new Set<string>() }))
vi.mock('@/hooks/useActiveLogFields', () => ({
  useActiveLogFields: () => ({
    ready: true,
    isFieldActive: (id: string) => activeFields.has(id),
    isGroupActive: () => false,
  }),
}))

import { HeaderAnomaliesSection } from '@/app/security/_sections/HeaderAnomaliesSection'

function baseProps(overrides: Partial<React.ComponentProps<typeof HeaderAnomaliesSection>> = {}) {
  return {
    data: undefined,
    isLoading: false,
    isFetching: false,
    error: null,
    getFieldLabel: (id: string) => id,
    topIpVisibility: {},
    setTopIpVisibility: vi.fn(),
    onTopIpVisChange: vi.fn(),
    ...overrides,
  }
}

describe('HeaderAnomaliesSection', () => {
  beforeEach(() => {
    plotlyMock.mockClear()
    dataTableMock.mockClear()
    activeFields.clear()
  })

  it('shows loading skeleton overlay across both cards when isLoading', () => {
    render(<HeaderAnomaliesSection {...baseProps({ isLoading: true })} />)
    expect(screen.getAllByText(/loading data/i).length).toBe(2)
  })

  it('renders the not-enabled "Requires Group A" copy when req_header_bytes is inactive', () => {
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: { req_size_dist: [], top_ips_header: [] } as any,
        })}
      />
    )
    // Chart empty card => "No data available" (ChartEmptyState with `requires`).
    expect(screen.getByText(/no data available/i)).toBeInTheDocument()
    // PlotlyChart should NOT be rendered in this branch.
    expect(plotlyMock).not.toHaveBeenCalled()
    // DataTable receives data=[] and our mock emits the gated empty message.
    expect(screen.getByTestId('empty')).toBeInTheDocument()
    expect(
      screen.getByText(
        'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.',
      ),
    ).toBeInTheDocument()
  })

  it('renders the neutral "no data yet" copy when req_header_bytes IS active', () => {
    // Enabled-but-empty: the field is in the active log format, so empty means
    // "no data in this window", NOT misconfigured — both cards drop "Requires".
    activeFields.add('req_header_bytes')
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: { req_size_dist: [], top_ips_header: [] } as any,
        })}
      />
    )
    // Chart card (ChartEmptyState, `requires` omitted) AND the DataTable empty
    // message both render the neutral copy → two occurrences.
    expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(2)
    expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
  })

  it('passes populated rows to Plotly and DataTable', () => {
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: {
            req_size_dist: [
              { bucket: '0-512', count: 100 },
              { bucket: '512-1k', count: 80 },
              { bucket: '8k+', count: 5 },
            ],
            top_ips_header: [
              { ip: '203.0.113.5', max_header: 16384 },
              { ip: '198.51.100.7', max_header: 12000 },
            ],
          } as any,
        })}
      />
    )

    // Plotly receives a single bar trace with x = buckets, y = counts.
    expect(plotlyMock).toHaveBeenCalledTimes(1)
    const trace = plotlyMock.mock.calls[0]![0].data[0]
    expect(trace.type).toBe('bar')
    expect(trace.x).toEqual(['0-512', '512-1k', '8k+'])
    expect(trace.y).toEqual([100, 80, 5])

    // DataTable receives two rows.
    expect(dataTableMock).toHaveBeenCalledTimes(1)
    expect(dataTableMock.mock.calls[0]![0].data).toHaveLength(2)
    // And our mock renders the IP strings inline.
    expect(screen.getByText('203.0.113.5')).toBeInTheDocument()
    expect(screen.getByText('198.51.100.7')).toBeInTheDocument()
  })

  it('forwards column-visibility toggles to the parent callback', async () => {
    const onTopIpVisChange = vi.fn()
    const user = userEvent.setup()
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: { req_size_dist: [], top_ips_header: [] } as any,
          onTopIpVisChange,
        })}
      />
    )
    const btn = screen.getByTestId('col-vis')
    await user.click(btn)
    // TOP_IP_COLUMN_IDS[0] = 'ip'
    expect(onTopIpVisChange).toHaveBeenCalledWith('ip', false)
  })
})
