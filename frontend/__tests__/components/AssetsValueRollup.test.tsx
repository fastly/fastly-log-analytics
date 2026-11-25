/**
 * @vitest-environment jsdom
 */
import { renderWithQuery, stubIntersectionObserver } from '@/__tests__/helpers/page-smoke'
import { act, cleanup, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase, queryKeys } from '@/lib/api'
import { createTestQueryClient } from '@/__tests__/helpers/page-smoke'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import AssetsClient from '@/app/assets-shield/_sections/AssetsClient'
import FastlyValueClient from '@/app/fastly-value/_sections/FastlyValueClient'
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'

const layout = vi.hoisted(() => ({
  filterPayload: {} as Record<string, unknown>,
  startTime: '2027-01-15T08:00:00Z' as string | null,
  endTime: '2027-01-15T10:00:00Z' as string | null,
}))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: Record<string, unknown>) => React.ReactNode }) => (
    <div>{props.children({
      activeServiceId: 'svc-1', startTime: layout.startTime, endTime: layout.endTime,
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

function renderBootstrapped(ui: React.ReactElement, _opts?: unknown, bootstrap: Record<string, unknown> = {}) {
  const queryClient = createTestQueryClient({ queries: { retry: false } })
  queryClient.setQueryData(queryKeys.bootstrap(), {
    runtime_mode: 'cloud',
    active_service_id: 'svc-1',
    services: [{ service_id: 'svc-1', name: 'Demo' }],
    settings: {},
    ...bootstrap,
  })
  return renderWithQuery(ui, { queryClient, withNuqs: true })
}

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
  layout.startTime = '2027-01-15T08:00:00Z'
  layout.endTime = '2027-01-15T10:00:00Z'
  window.history.replaceState(null, '', '/')
})

afterEach(() => {
  setCloudModeRuntime(false)
  vi.unstubAllGlobals()
  useServiceStore.setState({ activeServiceId: null, isInitialized: false })
  cleanup()
})

describe('AssetsClient on Cloud', () => {
  it('paints the summary-served breakdowns while the URL hotspots load raw', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? { ...assetsSummary, rollup: meta } : never())
    renderBootstrapped(<AssetsClient />, { withNuqs: true })

    const slot = screen.getByTestId('rollup-data-through-slot')
    expect(await within(slot).findByText(/Data through 09:45/)).toBeInTheDocument()
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
    expect(sections).toHaveLength(2)
  })

  it('reads the raw URL hotspots only once their panel nears the viewport', async () => {
    setCloudModeRuntime(true)
    const observer = stubIntersectionObserver()
    let answerTables: (body: unknown) => void = () => {}
    const tables = new Promise((resolve) => { answerTables = resolve })
    const sections = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? { ...assetsSummary, rollup: meta } : tables)
    const { container } = renderBootstrapped(<AssetsClient />, { withNuqs: true })

    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(sections).toEqual([['summary']])
    expect(screen.getByText('Scanning uncompressed static assets...')).toBeInTheDocument()
    expect(screen.queryByText('No uncompressed static assets detected! Nice work.')).not.toBeInTheDocument()
    act(() => observer.fire(true))
    await waitFor(() => expect(sections).toEqual([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
    expect(container.querySelector('.opacity-60')).toBeNull()
    answerTables({ large_uncompressed_assets: [], low_ttl_assets: [] })
    expect(await screen.findByText('No uncompressed static assets detected! Nice work.')).toBeInTheDocument()
  })

  it('observes hotspots against the nearest overflow-auto scroll container with 0px rootMargin', async () => {
    setCloudModeRuntime(true)
    const observed: Array<IntersectionObserverInit | undefined> = []
    class RecordingObserver {
      constructor(_cb: IntersectionObserverCallback, options?: IntersectionObserverInit) {
        observed.push(options)
      }
      observe() {}
      disconnect() {}
      unobserve() {}
    }
    vi.stubGlobal('IntersectionObserver', RecordingObserver)
    recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? { ...assetsSummary, rollup: meta } : never())

    const { container } = renderBootstrapped(
      <main id="main" style={{ overflow: 'auto', height: '600px' }}>
        <AssetsClient />
      </main>,
      { withNuqs: true },
    )
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    const mainEl = container.querySelector('main#main')
    expect(mainEl).not.toBeNull()
    expect(observed).toContainEqual(expect.objectContaining({ root: mainEl, rootMargin: '0px' }))
  })

  it('waits for the rollup summary to succeed before observing or reading raw URL hotspots', async () => {
    setCloudModeRuntime(true)
    const observer = stubIntersectionObserver()
    let answerSummary: (body: unknown) => void = () => {}
    const summary = new Promise((resolve) => { answerSummary = resolve })
    const sections = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? summary : never())
    renderBootstrapped(<AssetsClient />, { withNuqs: true })

    await waitFor(() => expect(sections).toEqual([['summary']]))
    // While summary is pending, useInViewOnce is disabled so skeleton layout cannot latch visibility.
    act(() => observer.fire(true))
    await Promise.resolve()
    expect(sections).toEqual([['summary']])

    act(() => answerSummary({ ...assetsSummary, rollup: meta }))
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(sections).toEqual([['summary']])

    // Once summary succeeds, the observer is attached; reporting in-view triggers the raw hotspots read.
    act(() => observer.fire(true))
    await waitFor(() => expect(sections).toEqual([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
  })

  it('names the asset fields the service log settings leave off, whatever the response says', async () => {
    setCloudModeRuntime(true)
    recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? { ...assetsSummary, rollup: meta } : never())
    const { unmount } = renderBootstrapped(<AssetsClient />, { withNuqs: true }, { active_log_field_ids: ['url', 'cache', 'status'] })
    const banner = await screen.findByTestId('missing-asset-fields-banner')
    expect(banner).toHaveTextContent('ttl, resp_header_content_encoding')
    unmount()

    renderBootstrapped(<AssetsClient />, { withNuqs: true }, { active_log_field_ids: ['url', 'ttl', 'resp_header_content_encoding'] })
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(screen.queryByTestId('missing-asset-fields-banner')).not.toBeInTheDocument()
  })

  it('splits a cache-status filter too, but keeps one raw read for any other filter', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { cache: { mode: 'include', values: ['HIT'] } }
    const split = recordPosts('/api/assets/aggregates', (asked) => asked?.includes('summary') ? assetsSummary : never())
    const { unmount } = renderBootstrapped(<AssetsClient />, { withNuqs: true })
    expect(await screen.findByText('4,321 reqs')).toBeInTheDocument()
    expect(split).toEqual(expect.arrayContaining([['summary'], ['large_uncompressed_assets', 'low_ttl_assets']]))
    unmount()

    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const one = recordPosts('/api/assets/aggregates', () => ({ ...assetsSummary, large_uncompressed_assets: [], low_ttl_assets: [] }))
    renderBootstrapped(<AssetsClient />, { withNuqs: true })
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
  it('reads every summary-tab section in one summary read, with no raw read', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/value/summary', () => ({ overview, network, caching: null, rollup: meta }))
    renderBootstrapped(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(screen.getByText('12.5%')).toBeInTheDocument()
    expect(within(screen.getByTestId('rollup-data-through-slot')).getByText(/Data through 09:45/)).toBeInTheDocument()
    expect(sections).toEqual([['overview', 'caching', 'network']])
  })

  it('marks an estimated P99 on the performance tab', async () => {
    setCloudModeRuntime(true)
    const performance = {
      cache_accel_factor: 3, avg_hit_latency_ms: 5, avg_miss_latency_ms: 15, p99_latency_ms: 120, latency_time_series: [],
    }
    recordPosts('/api/value/summary', () => ({
      overview, network, caching: null, performance,
      rollup: { ...meta, approximate: ['performance.p99_latency_ms'] },
    }))
    renderBootstrapped(<FastlyValueClient />, { withNuqs: true })
    await screen.findByText('4.3k')
    const user = (await import('@testing-library/user-event')).default.setup()
    await user.click(screen.getByRole('tab', { name: /Performance/ }))

    expect(await screen.findByText(/^≈120/)).toBeInTheDocument()
  })

  it('splits overview onto filtered_rollups for a single low-cardinality filter, and keeps one raw read for other filters', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const split = recordPosts('/api/value/summary', (asked) =>
      asked?.includes('overview') ? { overview, rollup: meta } : { network, caching: null },
    )
    const { unmount } = renderBootstrapped(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(split).toEqual(expect.arrayContaining([['overview'], ['caching', 'network']]))
    expect(split).toHaveLength(2)
    expect(within(screen.getByTestId('rollup-data-through-slot')).getByText(/Data through 09:45/)).toBeInTheDocument()
    unmount()

    layout.filterPayload = { url: { mode: 'include', values: ['/api/*'] } }
    const one = recordPosts('/api/value/summary', () => ({ overview, network, caching: null }))
    renderBootstrapped(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(one).toEqual([['overview', 'caching', 'network']])
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })

  it('keeps a single raw read for a single low-cardinality filter when the range is not hour-aligned or is auto', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { cache: { mode: 'include', values: ['HIT'] } }
    layout.startTime = '2027-01-15T08:15:00Z'
    layout.endTime = '2027-01-15T10:15:00Z'
    const unaligned = recordPosts('/api/value/summary', () => ({ overview, network, caching: null }))
    const { unmount } = renderBootstrapped(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(unaligned).toEqual([['overview', 'caching', 'network']])
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
    unmount()

    layout.startTime = null
    layout.endTime = null
    const autoRange = recordPosts('/api/value/summary', () => ({ overview, network, caching: null }))
    renderBootstrapped(<FastlyValueClient />, { withNuqs: true })

    expect(await screen.findByText('4.3k')).toBeInTheDocument()
    expect(autoRange).toEqual([['overview', 'caching', 'network']])
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })

  it('shows an error card instead of silent dashes when /api/value/summary fails', async () => {
    setCloudModeRuntime(true)
    server.use(
      http.post(`${getApiBase()}/api/value/summary`, () =>
        HttpResponse.json({ detail: 'boom' }, { status: 500 }),
      ),
    )
    renderBootstrapped(<FastlyValueClient />)
    expect(await screen.findByText('Service Summary unavailable')).toBeInTheDocument()
  })

  it('formats near-zero negative Fastly reported deltas as +0.0% rather than −0.0%', async () => {
    setCloudModeRuntime(true)
    recordPosts('/api/value/summary', () => ({
      overview,
      network,
      caching: null,
      fastly_reported: {
        window_start: '2027-01-15T08:00:00Z',
        window_end: '2027-01-15T09:00:00Z',
        requests: 100_000,
        logged_requests: 99_990,
        sample_rate: 100,
      },
      rollup: meta,
    }))
    renderBootstrapped(<FastlyValueClient />)
    expect(await screen.findByText(/logs \+0\.0%/)).toBeInTheDocument()
    expect(screen.queryByText(/−0\.0%/)).not.toBeInTheDocument()
  })
})
