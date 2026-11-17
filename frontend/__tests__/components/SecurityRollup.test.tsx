/**
 * @vitest-environment jsdom
 */
import { renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { cleanup, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import SecurityClient from '@/app/security/_sections/SecurityClient'
import { useTimezoneStore } from '@/stores/filterStore'

const layout = vi.hoisted(() => ({ filterPayload: {} as Record<string, unknown> }))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: Record<string, unknown>) => React.ReactNode }) => (
    <div>{props.children({
      activeServiceId: 'svc-1', startTime: '2027-01-15T08:00:00Z', endTime: '2027-01-15T10:00:00Z',
      filterPayload: layout.filterPayload, timezone: 'UTC', intervalButtons: null, bucketSeconds: 3600,
    })}</div>
  )
  return { ...actual, default: ReportLayout, ReportLayout }
})

vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/lib/cloud-engine/adapter', () => ({ handleCloudAnalyticalRequest: async () => null }))

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: ({ data }: { data: Array<{ x?: unknown[]; y?: unknown[] }> }) => (
    <div data-testid="plot">{data.map((trace) => JSON.stringify([trace.x, trace.y])).join('|')}</div>
  ),
}))

const ALL_SECTIONS = [
  'ngwaf_verified_bots', 'ngwaf_verified_bots_ts', 'wellknown_bots', 'bot_efficiency', 'tls_fingerprints',
  'fingerprint_coverage', 'req_size_dist', 'top_ips_header', 'ipv6_adoption', 'proxy_dist', 'conn_reuse_dist', 'proxies',
]
const RAW_SECTIONS = ['wellknown_bots', 'bot_efficiency', 'top_ips_header', 'proxies']

const summaryAnswer = {
  ngwaf_configured: false, ngwaf_verified_bots: [], ngwaf_verified_bots_ts: [],
  tls_fingerprints: [{ fingerprint: 'abc123', ip_count: 12, request_count: 1234 }],
  fingerprint_coverage: { tls_ciphers_sha: 0.5 },
  req_size_dist: [{ bucket: '0-256B', count: 7 }],
  ipv6_adoption: [{ time: '2027-01-15T08:00:00.000Z', pct: 12.5 }],
  proxy_dist: [{ type: 'vpn', count: 9 }],
  conn_reuse_dist: [{ bucket: '2-5', count: 3 }],
}

const meta = {
  data_from: '2027-01-15T08:00:00.000Z', data_through: '2027-01-15T09:45:00.000Z',
  approximate: ['tls_fingerprints.ip_count'], lower_bound: ['tls_fingerprints.request_count', 'req_size_dist', 'proxy_dist'],
}

const never = () => new Promise<never>(() => {})

function recordPosts(answer: (sections: string[] | undefined) => Promise<unknown> | unknown) {
  const sections: (string[] | undefined)[] = []
  server.use(http.post(`${getApiBase()}/api/security/aggregates`, async ({ request }) => {
    const body = await request.json() as { sections?: string[] }
    sections.push(body.sections)
    return HttpResponse.json(await answer(body.sections) as Record<string, unknown>)
  }))
  return sections
}

beforeEach(() => {
  useTimezoneStore.setState({ timezone: 'UTC' })
  layout.filterPayload = {}
  window.history.replaceState(null, '', '/security')
})

afterEach(() => {
  setCloudModeRuntime(false)
  cleanup()
})

describe('SecurityClient on Cloud', () => {
  it('paints summary-served sections while the row-level ones load, with ≈ and ≥ marks', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts((asked) => asked?.includes('wellknown_bots') ? never() : { ...summaryAnswer, rollup: meta })
    renderWithQuery(<SecurityClient />, { withNuqs: true })

    const slot = screen.getByTestId('rollup-data-through-slot')
    expect(slot).toBeEmptyDOMElement()
    expect(await within(slot).findByText(/Data through 09:45/)).toBeInTheDocument()
    const row = (await screen.findByText('abc123')).closest('tr')!
    expect(within(row).getByText('≈12')).toBeInTheDocument()
    expect(within(row).getByText('≥1,234')).toBeInTheDocument()
    expect(screen.getByText('Request Header Size Distribution (≥ counts)')).toBeInTheDocument()
    expect(screen.getByText('Proxy/Anonymizer Breakdown (≥ counts)')).toBeInTheDocument()
    expect(screen.getByText('Connection Reuse (Requests per Connection)')).toBeInTheDocument()
    expect(screen.getAllByTestId('plot').map((plot) => plot.textContent)).toContain(JSON.stringify([['0-256B'], [7]]))
    expect(sections).toEqual(expect.arrayContaining([
      ALL_SECTIONS.filter((s) => !RAW_SECTIONS.includes(s)),
      RAW_SECTIONS,
    ]))
    expect(screen.getByText('Loading well-known bots...')).toBeInTheDocument()
    expect(screen.getByText('Loading oversized request headers...')).toBeInTheDocument()
    expect(screen.queryByText('Loading TLS fingerprints...')).not.toBeInTheDocument()
    expect(screen.queryByText('Loading IPv6 adoption data...')).not.toBeInTheDocument()
  })

  it('keeps one raw read, with no summary labels, when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordPosts(() => ({ ...summaryAnswer, wellknown_bots: [], bot_efficiency: [], top_ips_header: [] }))
    renderWithQuery(<SecurityClient />, { withNuqs: true })

    const row = (await screen.findByText('abc123')).closest('tr')!
    expect(within(row).getByText('12')).toBeInTheDocument()
    expect(within(row).getByText('1,234')).toBeInTheDocument()
    expect(screen.getByText('Request Header Size Distribution')).toBeInTheDocument()
    expect(sections).toEqual([ALL_SECTIONS])
    expect(screen.queryByText(/Data through/)).not.toBeInTheDocument()
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })
})
