/**
 * Consolidated Security section tests: HeaderAnomaliesSection and ProxyWatchdogSection.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { ComponentProps } from 'react'
import React from 'react'
import { partial } from '../../helpers/partial'
import type { PlotlyChart } from '@/components/PlotlyChart'
import type { InteractiveDataTableMockProps } from '../../helpers/page-smoke'
import type { components } from '@/types/api.generated'

const { plotlyMock, dataTableMock, activeFields } = vi.hoisted(() => ({
  plotlyMock: vi.fn<(props: ComponentProps<typeof PlotlyChart>) => void>(),
  dataTableMock: vi.fn<(props: InteractiveDataTableMockProps) => void>(),
  activeFields: new Set<string>(),
}))
vi.mock('@/components/PlotlyChart', async () => (await import('../../helpers/page-smoke')).plotlyChartModuleMock(plotlyMock))
vi.mock('@/components/DataTable', async () => (await import('../../helpers/page-smoke')).interactiveDataTableModuleMock(dataTableMock))
vi.mock('@/components/FilterValueCell', async () => (await import('../../helpers/page-smoke')).filterValueCellModuleMock())
vi.mock('@/hooks/useLogFieldsCatalog', async () => (await import('../../helpers/page-smoke')).activeLogFieldsSetModuleMock(activeFields))

import { HeaderAnomaliesSection } from '@/app/security/_sections/HeaderAnomaliesSection'
import { ProxyWatchdogSection } from '@/app/security/_sections/ProxyWatchdogSection'

type HeaderData = NonNullable<React.ComponentProps<typeof HeaderAnomaliesSection>['data']>

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
    expect(screen.getByText('Loading request header size distribution...')).toBeInTheDocument()
    expect(screen.getByText('Loading oversized request headers...')).toBeInTheDocument()
  })

  it('enforces zero-CLS layout reservations on all cards', () => {
    const { container } = render(<HeaderAnomaliesSection {...baseProps()} />)
    const containedElements = container.querySelectorAll('.\\[contain-intrinsic-size\\:360px\\], .\\[contain-intrinsic-size\\:300px\\]')
    expect(containedElements.length).toBe(2)
  })

  it('renders the not-enabled "Requires Group A" copy when req_header_bytes is inactive', () => {
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
        })}
      />
    )
    expect(screen.getByText(/no data available/i)).toBeInTheDocument()
    expect(plotlyMock).not.toHaveBeenCalled()
    expect(screen.getByTestId('empty')).toBeInTheDocument()
    expect(
      screen.getByText(
        'Requires Request Identity (Group A) log fields to be enabled in Fastly logging.',
      ),
    ).toBeInTheDocument()
  })

  it('renders the neutral "no data yet" copy when req_header_bytes IS active', () => {
    activeFields.add('req_header_bytes')
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
        })}
      />
    )
    expect(screen.getAllByText(/no data in this time range yet/i).length).toBe(2)
    expect(screen.queryByText(/Requires/i)).not.toBeInTheDocument()
  })

  it('passes populated rows to Plotly and DataTable', () => {
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: partial<HeaderData>({
            req_size_dist: [
              { bucket: '0-512', count: 100 },
              { bucket: '512-1k', count: 80 },
              { bucket: '8k+', count: 5 },
            ],
            top_ips_header: [
              { ip: '203.0.113.5', max_header: 16384 },
              { ip: '198.51.100.7', max_header: 12000 },
            ],
          }),
        })}
      />
    )

    expect(plotlyMock).toHaveBeenCalledTimes(1)
    const trace = plotlyMock.mock.calls[0]![0].data[0]
    expect(trace?.type).toBe('bar')
    expect(trace?.x).toEqual(['0-512', '512-1k', '8k+'])
    expect(trace?.y).toEqual([100, 80, 5])

    expect(dataTableMock).toHaveBeenCalledTimes(1)
    expect(dataTableMock.mock.calls[0]![0].data).toHaveLength(2)
    expect(screen.getByText('203.0.113.5')).toBeInTheDocument()
    expect(screen.getByText('198.51.100.7')).toBeInTheDocument()
  })

  it('forwards column-visibility toggles to the parent callback', async () => {
    const onTopIpVisChange = vi.fn()
    const user = userEvent.setup()
    render(
      <HeaderAnomaliesSection
        {...baseProps({
          data: partial<HeaderData>({ req_size_dist: [], top_ips_header: [] }),
          onTopIpVisChange,
        })}
      />
    )
    const btn = screen.getByTestId('col-vis')
    await user.click(btn)
    expect(onTopIpVisChange).toHaveBeenCalledWith('ip', false)
  })
})

type SecurityData = components['schemas']['SecurityAggregatesResponse']

function securityData(): SecurityData {
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
    active_proxies_count: 10,
    tunnel_requests_count: 500,
    distance_mismatches_count: 2,
    traffic_quality: [],
    suspicious_isps: [],
    active_clients: [],
  }
}

describe('ProxyWatchdogSection', () => {
  it('renders stats and all card headers correctly', () => {
    const mockData = securityData()
    render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
    expect(screen.getByText('Active VPN & Proxy Users')).toBeInTheDocument()
    expect(screen.getByText('10')).toBeInTheDocument()
    expect(screen.getByText('Tunnel Requests')).toBeInTheDocument()
    expect(screen.getByText('500')).toBeInTheDocument()
    expect(screen.getByText('Impossible Distance Mismatches')).toBeInTheDocument()
    expect(screen.getByText('2')).toBeInTheDocument()

    expect(screen.getByText('Traffic Quality Breakdown')).toBeInTheDocument()
    expect(screen.getByText('Suspicious ISPs & Providers')).toBeInTheDocument()
    expect(screen.getByText('VPN/Proxy Export Center')).toBeInTheDocument()
    expect(screen.getByText('Active Suspicious Clients')).toBeInTheDocument()
  })

  it('enforces zero-CLS layout reservations on stat cards and card containers', () => {
    const mockData = securityData()
    const { container } = render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
    expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:120px\\]').length).toBeGreaterThanOrEqual(3)
    expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:320px\\]').length).toBeGreaterThanOrEqual(2)
    expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:140px\\]').length).toBeGreaterThanOrEqual(1)
    expect(container.querySelectorAll('.\\[contain-intrinsic-size\\:600px\\]').length).toBeGreaterThanOrEqual(1)
  })
})
