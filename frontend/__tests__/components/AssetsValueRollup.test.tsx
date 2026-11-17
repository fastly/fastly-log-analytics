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
import AssetsClient from '@/app/assets-shield/_sections/AssetsClient'
import FastlyValueClient from '@/app/fastly-value/_sections/FastlyValueClient'
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'

const layout = vi.hoisted(() => ({ filterPayload: {} as Record<string, unknown> }))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: Record<string, unknown>) => React.ReactNode }) => (
    <div>{props.children({
      activeServiceId: 'svc-1', startTime: '2027-01-15T08:00:00Z', endTime: '2027-01-15T10:00:00Z',
      filterPayload: layout.filterPayload, timezone: 'UTC', trend: 'off', setTrend: () => {}, intervalButtons: null,
      config: { spanHours: 2, effectiveInterval: '1 hour', validIntervals: new Set(['1 hour']), validTrends: new Set(['off']) },
    })}</div>
  )
  return { ...actual, default: ReportLayout, ReportLayout }
})

vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/lib/cloud-engine/adapter', () => ({ handleCloudAnalyticalRequest: async () => null }))

vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plot" /> }))

const meta = {
  data_from: '2027-01-15T08:00:00.000Z', data_through: '2027-01-15T09:45:00.000Z', approximate: [], lower_bound: [],
}

const never = () => new Promise<never>(() => {})

function recordPosts(path: string, answer: (sections: string[] | undefined) => Promise<unknown> | unknown) {
  const sections: (string[] | undefined)[] = []
  server.use(http.post(`${getApiBase()}${path}`, async ({ request }) => {
    const body = await request.json() as { sections?: string[] }
    sections.push(body.sections)
    return HttpResponse.json(await answer(body.sections) as Record<string, unknown>)
  }))
  return sections
}

const imagesRow = {
  asset_type: 'Images', requests: 4321, bytes: 1000, total_bytes: 1000, egress_bytes: 1000, pct_requests: 100,
  cache_hit_ratio: 0.9, hit_ratio_pct: 90, compression_rate: 0, avg_ttfb_ms: 12,
}

const assetsSummary = {
  missing_fields: [],
  summary: { total_requests: 4321, total_bytes: 1000, overall_hit_ratio: 0.9, overall_hit_ratio_pct: 90, avg_ttfb_ms: 12, avg_origin_ms: 0 },
  asset_type_breakdown: [imagesRow], cache_performance: [], compression_performance: [],
}

beforeEach(() => {
  useTimezoneStore.setState({ timezone: 'UTC' })
  useServiceStore.setState({ activeServiceId: 'svc-1', isInitialized: true })
  layout.filterPayload = {}
  window.history.replaceState(null, '', '/')
})

afterEach(() => {
  setCloudModeRuntime(false)
  useServiceStore.setState({ activeServiceId: null, isInitialized: false })
  cleanup()
})

describe('AssetsClient on Cloud', () => {
  it('paints the summary-served breakdowns while the URL hotspots load raw', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? { ...assetsSummary, rollup: meta } : never())
    renderWithQuery(<AssetsClient />, { withNuqs: true })

    const slot = screen.getByTestId('rollup-data-through-slot')
    expect(await within(slot).findByText(/Data through 09:45/)).toBeInTheDocument()
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
    expect(sections).toHaveLength(2)
  })

  it('splits a cache-status filter too, but keeps one raw read for any other filter', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { cache: { mode: 'include', values: ['HIT'] } }
    const split = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? assetsSummary : never())
    const { unmount } = renderWithQuery(<AssetsClient />, { withNuqs: true })
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(split).toEqual(expect.arrayContaining([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
    unmount()

    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const one = recordPosts('/api/assets/aggregates', () => ({ ...assetsSummary, large_uncompressed_assets: [], low_ttl_assets: [] }))
    renderWithQuery(<AssetsClient />, { withNuqs: true })
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(one).toEqual([undefined])
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })
})

const overview = {
  total_requests: 4321, total_bandwidth_bytes: 1000, origin_offload_pct: 90, cache_acceleration_factor: 3,
  threats_blocked: null, edge_hit_requests: 3889, origin_requests: 432,
}
const network = { total_requests: 4321, http3_pct: 12.5, h2_pct: 80, tls_pct: null, ipv6_pct: null, protocol_time_series: [] }

describe('FastlyValueClient on Cloud', () => {
  it('paints the summary-served overview and network cards while the caching section loads raw', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/value/summary', (asked) => asked?.includes('caching') ? never() : { overview, network, rollup: meta })
    renderWithQuery(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(screen.getByText('12.5%')).toBeInTheDocument()
    expect(within(screen.getByTestId('rollup-data-through-slot')).getByText(/Data through 09:45/)).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['overview', 'network'], ['caching']]))
    expect(sections).toHaveLength(2)
  })

  it('keeps one raw read for every section when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordPosts('/api/value/summary', () => ({ overview, network, caching: null }))
    renderWithQuery(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(sections).toEqual([['overview', 'caching', 'network']])
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })
})
