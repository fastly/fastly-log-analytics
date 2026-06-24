/**
 * NetworkSection wraps three Plotly visualisations (IPv6 adoption line,
 * proxy/anonymizer donut, connection-reuse histogram). It has no
 * DataTable. PlotlyChart is mocked so jsdom can render; useTimeseriesToTraces
 * runs unmocked since it's pure-JS. Interaction surface: AnalyticsCard help
 * buttons open a Radix dialog — clicking one is our user interaction.
 *
 * @vitest-environment jsdom
 */
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

const plotlyMock = vi.fn((_props: any) => <div data-testid="plotly-chart" />)
vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: any) => plotlyMock(props),
}))

vi.mock('@/lib/date', () => ({
  formatDate: (t: string) => t,
}))

import { NetworkSection } from '@/app/security/_sections/NetworkSection'

function baseProps(overrides: Partial<React.ComponentProps<typeof NetworkSection>> = {}) {
  return {
    data: undefined,
    isLoading: false,
    isFetching: false,
    error: null,
    timezone: 'UTC',
    commonTimeLayout: {},
    ...overrides,
  }
}

describe('NetworkSection', () => {
  beforeEach(() => {
    plotlyMock.mockClear()
  })

  it('shows loading skeleton overlay across all three cards when isLoading', () => {
    render(<NetworkSection {...baseProps({ isLoading: true })} />)
    // AnalyticsCard overlays "Loading data..." on top of each card.
    expect(screen.getAllByText(/loading data/i).length).toBe(3)
  })

  it('renders "No data available" empty card for each chart when arrays are empty', () => {
    render(
      <NetworkSection
        {...baseProps({
          data: { ipv6_adoption: [], proxy_dist: [], conn_reuse_dist: [] } as any,
        })}
      />
    )
    expect(screen.getAllByText(/no data available/i).length).toBe(3)
    // IPv6 + conn_reuse both cite Group C — assert it appears twice
    // (one per card) and proxy cites Group I once.
    expect(screen.getAllByText(/Infrastructure \(Group C\) fields/i).length).toBe(2)
    expect(screen.getByText(/Security: Proxy & Anonymization \(Group I\)/i)).toBeInTheDocument()
    expect(plotlyMock).not.toHaveBeenCalled()
  })

  it('forwards populated data into Plotly traces (IPv6 line, proxy pie, conn-reuse bar)', () => {
    render(
      <NetworkSection
        {...baseProps({
          data: {
            ipv6_adoption: [
              { time: '2026-01-01T00:00:00Z', pct: 42 },
              { time: '2026-01-01T01:00:00Z', pct: 51 },
            ],
            proxy_dist: [
              { type: 'residential', count: 800 },
              { type: 'hosting', count: 150 },
              { type: 'tor', count: 5 },
            ],
            conn_reuse_dist: [
              { bucket: '1', count: 300 },
              { bucket: '2-5', count: 1200 },
              { bucket: '6-10', count: 900 },
            ],
          } as any,
        })}
      />
    )

    expect(plotlyMock).toHaveBeenCalledTimes(3)
    const [ipv6Call, proxyCall, reuseCall] = plotlyMock.mock.calls.map((c) => c[0])

    // IPv6 trace: useTimeseriesToTraces config = pct → "IPv6 %"
    expect(ipv6Call.data[0].name).toBe('IPv6 %')
    expect(ipv6Call.data[0].y).toEqual([42, 51])

    // Proxy donut: pie with hole=0.4, labels = types, values = counts
    expect(proxyCall.data[0].type).toBe('pie')
    expect(proxyCall.data[0].labels).toEqual(['residential', 'hosting', 'tor'])
    expect(proxyCall.data[0].values).toEqual([800, 150, 5])

    // Connection reuse: bar trace
    expect(reuseCall.data[0].type).toBe('bar')
    expect(reuseCall.data[0].x).toEqual(['1', '2-5', '6-10'])
    expect(reuseCall.data[0].y).toEqual([300, 1200, 900])
  })

  it('opens the help dialog when the user clicks an AnalyticsCard help button', async () => {
    const user = userEvent.setup()
    render(
      <NetworkSection
        {...baseProps({
          data: { ipv6_adoption: [], proxy_dist: [], conn_reuse_dist: [] } as any,
        })}
      />
    )
    // Each card exposes an "About this chart" trigger; click the first
    // (IPv6 Adoption) and assert the Radix dialog appears.
    const helpButtons = screen.getAllByRole('button', { name: /about this chart/i })
    expect(helpButtons.length).toBe(3)
    await user.click(helpButtons[0]!)

    const dialog = await screen.findByRole('dialog')
    expect(dialog).toBeInTheDocument()
    // Body content from SECURITY_INFO.ipv6
    expect(within(dialog).getByText(/Tracks the percentage of requests/i)).toBeInTheDocument()
  })
})
