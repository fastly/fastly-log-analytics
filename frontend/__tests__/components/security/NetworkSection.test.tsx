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
import type React from 'react'
import type { components } from '@/types/api.generated'
import type { PlotlyChart } from '@/components/PlotlyChart'

type SecurityData = components['schemas']['SecurityAggregatesResponse']

const { plotlyMock, activeFields } = vi.hoisted(() => ({
  plotlyMock: vi.fn<(props: React.ComponentProps<typeof PlotlyChart>) => void>(),
  activeFields: new Set<string>(),
}))
vi.mock('@/components/PlotlyChart', async () => (await import('../../helpers/page-smoke')).plotlyChartModuleMock(plotlyMock))
vi.mock('@/lib/date', () => ({ formatDate: (t: string) => t }))
vi.mock('@/hooks/useLogFieldsCatalog', async () => (await import('../../helpers/page-smoke')).activeLogFieldsSetModuleMock(activeFields))

import { NetworkSection } from '@/app/security/_sections/NetworkSection'

function securityData(overrides: Partial<SecurityData>): SecurityData {
  return {
    _is_cached: false,
    tls_fingerprints: [],
    fingerprint_coverage: {},
    req_size_dist: [],
    top_ips_header: [],
    ipv6_adoption: [],
    proxy_dist: [],
    conn_reuse_dist: [],
    verified_bots_ts: [],
    ngwaf_configured: false,
    ngwaf_verified_bots: [],
    ngwaf_verified_bots_ts: [],
    wellknown_bots: [],
    bot_efficiency: [],
    bots: [],
    ngwaf_bots: [],
    active_proxies_count: 0,
    tunnel_requests_count: 0,
    distance_mismatches_count: 0,
    traffic_quality: [],
    suspicious_isps: [],
    active_clients: [],
    ...overrides,
  }
}

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
    activeFields.clear()
  })

  it('shows loading skeleton overlay across all three cards when isLoading', () => {
    render(<NetworkSection {...baseProps({ isLoading: true })} />)
    expect(screen.getByText('Loading IPv6 adoption data...')).toBeInTheDocument()
    expect(screen.getByText('Loading proxy/anonymizer breakdown...')).toBeInTheDocument()
    expect(screen.getByText('Loading connection reuse distribution...')).toBeInTheDocument()
  })

  it('enforces zero-CLS layout reservations on all cards', () => {
    const { container } = render(<NetworkSection {...baseProps()} />)
    const containedElements = container.querySelectorAll('.\\\[contain-intrinsic-size\\:360px\\\]')
    expect(containedElements.length).toBe(3)
  })

  it('renders "No data available" empty card for each chart when arrays are empty', () => {
    render(
      <NetworkSection
        {...baseProps({
          data: securityData({}),
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

  it('shows a neutral "no data yet" state (no Requires hint) when the field group IS enabled', () => {
    // Enabled-but-empty: the fields driving each card are in the active log
    // format, so an empty result means "no data in this window", NOT
    // "misconfigured". ChartEmptyState should drop the "Requires …" subtext.
    activeFields.add('is_ipv6')
    activeFields.add('p_type')
    activeFields.add('conn_requests')
    render(
      <NetworkSection
        {...baseProps({
          data: securityData({}),
        })}
      />
    )
    expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(3)
    expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
    expect(plotlyMock).not.toHaveBeenCalled()
  })

  it('forwards populated data into Plotly traces (IPv6 line, proxy pie, conn-reuse bar)', () => {
    render(
      <NetworkSection
        {...baseProps({
          data: securityData({
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
          }),
        })}
      />
    )

    expect(plotlyMock).toHaveBeenCalledTimes(3)
    const [ipv6Call, proxyCall, reuseCall] = plotlyMock.mock.calls.map((c) => c[0])

    // IPv6 trace: useTimeseriesToTraces config = pct → "IPv6 %"
    expect(ipv6Call.data[0]?.name).toBe('IPv6 %')
    expect(ipv6Call.data[0]?.y).toEqual([42, 51])

    // Proxy donut: pie with hole=0.4, labels = types, values = counts
    expect(proxyCall.data[0]?.type).toBe('pie')
    expect(proxyCall.data[0]?.labels).toEqual(['residential', 'hosting', 'tor'])
    expect(proxyCall.data[0]?.values).toEqual([800, 150, 5])

    // Connection reuse: bar trace
    expect(reuseCall.data[0]?.type).toBe('bar')
    expect(reuseCall.data[0]?.x).toEqual(['1', '2-5', '6-10'])
    expect(reuseCall.data[0]?.y).toEqual([300, 1200, 900])
  })

  it('opens the help dialog when the user clicks an AnalyticsCard help button', async () => {
    const user = userEvent.setup()
    render(
      <NetworkSection
        {...baseProps({
          data: securityData({}),
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
