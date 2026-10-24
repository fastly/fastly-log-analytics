/**
 * BotsSection covers three data tables (ngwaf_verified_bots, wellknown_bots,
 * tls_fingerprints) plus a stacked-bar
 * NGWAF timeseries chart and per-field "low coverage" hints. We mock
 * PlotlyChart + DataTable so jsdom can render without plotly/d3, and assert
 * loading, empty, populated, and interaction (column-visibility toggle)
 * paths against the props each child receives.
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

const { plotlyMock, dataTableMock, activeFields } = vi.hoisted(() => ({
  plotlyMock: vi.fn<(props: ComponentProps<typeof PlotlyChart>) => void>(),
  dataTableMock: vi.fn<(props: InteractiveDataTableMockProps) => void>(),
  activeFields: new Set<string>(),
}))
vi.mock('@/components/PlotlyChart', async () => (await import('../../helpers/page-smoke')).plotlyChartModuleMock(plotlyMock))
vi.mock('@/components/DataTable', async () => (await import('../../helpers/page-smoke')).interactiveDataTableModuleMock(dataTableMock))
vi.mock('@/components/FilterValueCell', async () => (await import('../../helpers/page-smoke')).filterValueCellModuleMock())
vi.mock('@/lib/date', () => ({ formatDate: (t: string) => t }))
vi.mock('@/hooks/useLogFieldsCatalog', async () => (await import('../../helpers/page-smoke')).activeLogFieldsSetModuleMock(activeFields))

import { BotsSection } from '@/app/security/_sections/BotsSection'

type BotsData = NonNullable<React.ComponentProps<typeof BotsSection>['data']>

function baseProps(overrides: Partial<React.ComponentProps<typeof BotsSection>> = {}) {
  return {
    data: undefined,
    isLoading: false,
    isFetching: false,
    error: null,
    intervalButtons: <div data-testid="interval-buttons" />,
    bucketSeconds: 3600,
    timezone: 'UTC',
    commonTimeLayout: {},
    getFieldLabel: (id: string) => id,
    ngwafBotVisibility: {},
    setNgwafBotVisibility: vi.fn(),
    onNgwafBotVisChange: vi.fn(),
    botVisibility: {},
    setBotVisibility: vi.fn(),
    onBotVisChange: vi.fn(),
    fingerprintVisibility: {},
    setFingerprintVisibility: vi.fn(),
    onFingerprintVisChange: vi.fn(),
    ...overrides,
  }
}

describe('BotsSection', () => {
  beforeEach(() => {
    plotlyMock.mockClear()
    dataTableMock.mockClear()
    activeFields.clear()
  })

  it('shows loading skeleton (AnalyticsCard overlay) when data is undefined and isLoading=true', () => {
    render(<BotsSection {...baseProps({ isLoading: true })} />)
    expect(screen.getByText('Loading NGWAF verified bots...')).toBeInTheDocument()
    expect(screen.getByText('Loading NGWAF bot names...')).toBeInTheDocument()
    expect(screen.getByText('Loading well-known bots...')).toBeInTheDocument()
    expect(screen.getByText('Loading TLS fingerprints...')).toBeInTheDocument()
  })

  it('enforces zero-CLS layout reservations on all cards', () => {
    const { container } = render(<BotsSection {...baseProps()} />)
    const containedElements = container.querySelectorAll('.\\\[contain-intrinsic-size\\:360px\\\], .\\\[contain-intrinsic-size\\:300px\\\]')
    expect(containedElements.length).toBeGreaterThanOrEqual(4)
  })

  it('renders empty-state messages when arrays are empty', () => {
    render(
      <BotsSection
        {...baseProps({
          data: partial<BotsData>({
            ngwaf_configured: true,
            ngwaf_verified_bots_ts: [],
            ngwaf_verified_bots: [],
            wellknown_bots: [],
            tls_fingerprints: [],
          }),
        })}
      />
    )
    // NGWAF chart empty-card text + DataTable empty pass-through both
    // render the same copy ("No NGWAF bot detections..."), so it appears
    // twice. The two-occurrence count is the assertion we care about
    // because it pins both fall-through branches.
    expect(screen.getAllByText(/no ngwaf bot detections/i).length).toBe(2)
    // Tables resolve to data.length=0 -> empty wrappers from our DataTable mock
    expect(screen.getAllByTestId('data-table-0').length).toBeGreaterThanOrEqual(3)
  })

  it('hides NGWAF cards entirely when ngwaf_configured=false', () => {
    render(
      <BotsSection
        {...baseProps({
          data: partial<BotsData>({
            ngwaf_configured: false,
            ngwaf_verified_bots_ts: [],
            ngwaf_verified_bots: [],
            wellknown_bots: [],
            tls_fingerprints: [],
          }),
        })}
      />
    )
    expect(screen.queryByText(/Verified Bots \(NGWAF\)/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Verified Bot Names \(NGWAF\)/i)).not.toBeInTheDocument()
  })

  it('routes the TLS-fingerprint empty copy on field-active state', () => {
    const emptyData = partial<BotsData>({
      ngwaf_configured: true,
      ngwaf_verified_bots_ts: [],
      ngwaf_verified_bots: [],
      wellknown_bots: [],
      tls_fingerprints: [],
    })

    // Not enabled (no ja3/ja4 in the active format) → "Requires …" hint.
    const { unmount } = render(<BotsSection {...baseProps({ data: emptyData })} />)
    expect(
      screen.getByText(
        'Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.',
      ),
    ).toBeInTheDocument()
    unmount()
    dataTableMock.mockClear()

    // Enabled-but-empty (ja3 active) → neutral "no data in this range" copy.
    activeFields.add('ja3')
    render(<BotsSection {...baseProps({ data: emptyData })} />)
    expect(screen.getByText('No TLS fingerprints in this time range.')).toBeInTheDocument()
    expect(
      screen.queryByText(/Requires Security: TLS Fingerprinting/),
    ).not.toBeInTheDocument()
  })

  it('passes populated rows through to DataTable for each section', () => {
    render(
      <BotsSection
        {...baseProps({
          data: partial<BotsData>({
            ngwaf_configured: true,
            ngwaf_verified_bots_ts: [
              { time: '2026-01-01T00:00:00Z', bot_name: 'OpenAI', count: 12 },
              { time: '2026-01-01T01:00:00Z', bot_name: 'OpenAI', count: 8 },
              { time: '2026-01-01T01:00:00Z', bot_name: 'Anthropic', count: 3 },
            ],
            ngwaf_verified_bots: [
              { bot_name: 'OpenAI', category: 'search-engine', request_count: 20 },
            ],
            wellknown_bots: [
              { id: 'gb', name: 'Googlebot', category: 'search-engine', request_count: 100, verified_count: 90, impersonator_count: 5, unverified_count: 3, pending_count: 2 },
            ],
            tls_fingerprints: [{ fingerprint: 'sha-abc', ip_count: 4, request_count: 50 }],
            bot_efficiency: [
              {
                bot_name: 'Commoncrawl Crawler',
                category: 'ai-scraper',
                request_count: 40,
                resp_bytes: 80000,
                bandwidth_pct: 12.5,
                referrals: 0,
                requests_per_referral: 40,
                bytes_per_referral: 80000,
              },
            ],
          }),
        })}
      />
    )

    // Plotly call for NGWAF timeseries: 2 distinct bot names => 2 traces.
    expect(plotlyMock).toHaveBeenCalled()
    const traces = plotlyMock.mock.calls[0]![0].data
    expect(Array.isArray(traces)).toBe(true)
    expect(traces.length).toBe(2)
    const names = traces.map((t) => t.name).sort()
    expect(names).toEqual(['Anthropic', 'OpenAI'])

    // All four tables receive their respective rows.
    const tableProps = dataTableMock.mock.calls.map((c) => c[0])
    expect(tableProps).toHaveLength(4)
    expect(tableProps[0]?.data).toHaveLength(1) // ngwaf_verified_bots
    expect(tableProps[1]?.data).toHaveLength(1) // wellknown_bots
    expect(tableProps[2]?.data).toHaveLength(1) // tls_fingerprints
    expect(tableProps[3]?.data).toHaveLength(1) // bot_efficiency

    // Custom-mocked DataTable renders the row label inline so a populated
    // row is observable from the DOM, not just from mock-call inspection.
    expect(screen.getByText('Googlebot')).toBeInTheDocument()
    expect(screen.getByText('OpenAI')).toBeInTheDocument()
  })

  it('renders low-coverage hint only when fingerprint_coverage < 1% for that field', () => {
    render(
      <BotsSection
        {...baseProps({
          data: partial<BotsData>({
            ngwaf_configured: true,
            ngwaf_verified_bots_ts: [],
            ngwaf_verified_bots: [],
            wellknown_bots: [],
            tls_fingerprints: [],
            // tls 0.5% => hint (< 1% threshold)
            fingerprint_coverage: { tls_ciphers_sha: 0.005 },
          }),
        })}
      />
    )
    expect(screen.getByText(/low coverage/i)).toBeInTheDocument()
    expect(screen.getByText(/TLS fingerprints are only captured/i)).toBeInTheDocument()
  })

  it('invokes the column-visibility change callback when the user toggles a column off', async () => {
    const onNgwafBotVisChange = vi.fn()
    const user = userEvent.setup()
    render(
      <BotsSection
        {...baseProps({
          data: partial<BotsData>({
            ngwaf_configured: true,
            ngwaf_verified_bots_ts: [],
            ngwaf_verified_bots: [],
            wellknown_bots: [],
            tls_fingerprints: [],
          }),
          onNgwafBotVisChange,
        })}
      />
    )

    // First col-vis button = NGWAF verified bots dropdown.
    const buttons = screen.getAllByTestId('col-vis')
    expect(buttons.length).toBeGreaterThanOrEqual(3)
    await user.click(buttons[0]!)
    expect(onNgwafBotVisChange).toHaveBeenCalledTimes(1)
    // First col in NGWAF_BOT_COLUMN_IDS = 'bot_name'
    expect(onNgwafBotVisChange).toHaveBeenCalledWith('bot_name', false)
  })
})
