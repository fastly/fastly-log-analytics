/**
 * @vitest-environment jsdom
 */
import { createTestQueryClient, renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { cleanup, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase, queryKeys } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import OriginClient from '@/app/origin/_sections/OriginClient'
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import { useTimezoneStore } from '@/stores/filterStore'

const layout = vi.hoisted(() => ({ filterPayload: {} as Record<string, unknown>, interval: '1 hour' }))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: Record<string, unknown>) => React.ReactNode }) => (
    <div>{props.children({
      activeServiceId: 'svc-1', startTime: '2027-01-15T08:00:00Z', endTime: '2027-01-15T10:00:00Z',
      filterPayload: layout.filterPayload, timezone: 'UTC', trend: 'off', setTrend: () => {}, intervalButtons: null,
      config: { spanHours: 2, effectiveInterval: layout.interval, validIntervals: new Set([layout.interval]), validTrends: new Set(['off']) },
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

const ORIGIN_SECTIONS = ['summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health']

const meta = (approximate: string[], lowerBound: string[]) => ({
  data_from: '2027-01-15T08:00:00.000Z', data_through: '2027-01-15T09:45:00.000Z',
  approximate, lower_bound: lowerBound,
})

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

const ipRow = { oip: '203.0.113.1', requests: 1234, p50_ms: 45, p95_ms: 120, error_pct: 0.5 }

const originRaw = {
  has_data: true,
  summary: { has_data: true, total_misses: 10, total_passes: 0, ottfb_p50_ms: 40, ottfb_p95_ms: 90 },
  timeseries: { has_data: false, series: [] },
  slow_urls: { has_data: false, rows: [] },
  status_codes: { has_data: false, rows: [] },
  path_breakdown: { has_data: false, shielding_detected: false, rows: [] },
  pop_latency: { has_data: false, requires_group_c: false, rows: [] },
  ip_health: { has_data: true, rows: [ipRow] },
}

beforeEach(() => {
  useTimezoneStore.setState({ timezone: 'UTC' })
  layout.filterPayload = {}
  layout.interval = '1 hour'
  window.history.replaceState(null, '', '/')
})

afterEach(() => {
  setCloudModeRuntime(false)
  cleanup()
})

describe('OriginClient on Cloud', () => {
  it('answers every unfiltered origin section from one summary read, with ≈ and ≥ marks', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/origin/aggregates', () => ({
      ...originRaw,
      slow_urls: { has_data: true, rows: [{ url: '/slow', requests: 77, p50_ms: 10, p95_ms: 300, p99_ms: 400 }] },
      rollup: meta(
        ['summary.ottfb_p50_ms', 'slow_urls.p95_ms', 'ip_health.p50_ms', 'ip_health.p95_ms', 'timeseries.value'],
        ['slow_urls.requests', 'ip_health.requests'],
      ),
    }))
    renderWithQuery(<OriginClient />, { withNuqs: true })

    const slot = screen.getByTestId('rollup-data-through-slot')
    expect(await within(slot).findByText(/Data through 09:45/)).toBeInTheDocument()
    const row = (await screen.findByText('203.0.113.1')).closest('tr')!
    expect(within(row).getByText('≥1,234')).toBeInTheDocument()
    expect(within(row).getByText('≈45.0ms')).toBeInTheDocument()
    expect(await screen.findByText('/slow')).toBeInTheDocument()
    expect(screen.getAllByText('Approximate').length).toBeGreaterThanOrEqual(2)
    expect(sections).toHaveLength(1)
    expect(sections[0]!.toSorted()).toEqual(ORIGIN_SECTIONS.toSorted())
  })

  it('reads the latency series raw when its bucket does not tile summary steps, and the rest from summaries', async () => {
    setCloudModeRuntime(true)
    layout.interval = '1 minute'
    const sections = recordPosts('/api/origin/aggregates', (asked) => asked?.includes('timeseries') ? never() : {
      ...originRaw,
      rollup: meta(['ip_health.p50_ms', 'ip_health.p95_ms'], []),
    })
    renderWithQuery(<OriginClient />, { withNuqs: true })

    expect(await screen.findByText('≈45.0ms')).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['timeseries'], ORIGIN_SECTIONS.filter((s) => s !== 'timeseries')]))
    expect(screen.getByText('Loading origin latency timeseries...')).toBeInTheDocument()
  })

  it('keeps one raw read, with no summary labels, when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordPosts('/api/origin/aggregates', () => originRaw)
    renderWithQuery(<OriginClient />, { withNuqs: true })

    const row = (await screen.findByText('203.0.113.1')).closest('tr')!
    expect(within(row).getByText('1,234')).toBeInTheDocument()
    expect(within(row).getByText('45.0ms')).toBeInTheDocument()
    expect(sections).toEqual([ORIGIN_SECTIONS])
    expect(screen.queryByText(/Data through/)).not.toBeInTheDocument()
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })
})

const waterfall = { avg: { edge_processing: 1.5, origin_wait: 40, origin_download: 3, client_download: 7 } }
const ttlDist = [{ bucket: '< 60s', count: 9 }, { bucket: '> 1d', count: 4 }]

function seededCloudQueryClient() {
  const queryClient = createTestQueryClient()
  queryClient.setQueryData(queryKeys.bootstrap(), {
    runtime_mode: 'cloud',
    active_service_id: 'svc-1',
    services: [{ service_id: 'svc-1', name: 'Test Service' }],
  })
  return queryClient
}

describe('PerformanceClient on Cloud', () => {
  it('paints the waterfall, TTL buckets, rankings and scatter sample from one summary read', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/performance/aggregates', () => ({
      ttl_dist: ttlDist, waterfall, _approx: true,
      top_urls: [{ url: '/slow/report', requests: 42, avg: 310, p50: 250, p95: 900, p99: 1200 }],
      top_asns: [{ asn: '64512', label: 'AS64512', requests: 77, avg: 120, p50: 100, p95: 300, p99: 450 }],
      scatter: [{ origin: 12.5, edge: 3.25, cache: 'MISS' }, { origin: 0.5, edge: 1, cache: 'HIT' }],
      rollup: meta(['top_urls.p99', 'top_asns.p99', 'scatter'], ['ttl_dist', 'top_urls.requests']),
    }))
    renderWithQuery(<PerformanceClient />, { withNuqs: true, queryClient: seededCloudQueryClient() })

    const slot = screen.getByTestId('rollup-data-through-slot')
    expect(slot).toBeEmptyDOMElement()
    expect(await within(slot).findByText(/Data through 09:45/)).toBeInTheDocument()
    expect(await screen.findByText('Cache TTL Distribution (≥ counts)')).toBeInTheDocument()
    const plots = screen.getAllByTestId('plot').map((plot) => plot.textContent)
    expect(plots).toContain(JSON.stringify([['< 60s', '> 1d'], [9, 4]]))
    expect(plots.some((plot) => plot?.includes(JSON.stringify([[40], ['Origin TTFB Wait']])))).toBe(true)
    expect(plots).toContain(`${JSON.stringify([[0.5], [1]])}|${JSON.stringify([[12.5], [3.25]])}`)
    expect(sections).toEqual([['ttl_dist', 'waterfall', 'top_urls', 'top_asns', 'scatter']])
    expect(await screen.findByText('/slow/report')).toBeInTheDocument()
    expect(screen.getByText('Slowest URLs (≥ counts)')).toBeInTheDocument()
    expect(screen.getByText('Slowest Networks')).toBeInTheDocument()
    expect(screen.getByText('Approximate')).toHaveAttribute('title', expect.stringMatching(/summary histograms/))
    expect(screen.queryByText('Profiling origin vs edge latency...')).not.toBeInTheDocument()
    expect(screen.getByText('Origin vs Edge Processing (ms, ≈ sample)')).toBeInTheDocument()
  })

  it('keeps one raw read, with no summary labels, when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordPosts('/api/performance/aggregates', () => ({
      ttl_dist: ttlDist, waterfall, top_urls: [], top_asns: [], scatter: [], _approx: false,
    }))
    renderWithQuery(<PerformanceClient />, { withNuqs: true, queryClient: seededCloudQueryClient() })

    expect(await screen.findByText(JSON.stringify([['< 60s', '> 1d'], [9, 4]]))).toBeInTheDocument()
    expect(screen.getByText('Cache TTL Distribution')).toBeInTheDocument()
    expect(sections).toEqual([undefined])
    expect(screen.getByText('Origin vs Edge Processing (ms)')).toBeInTheDocument()
    expect(screen.queryByText(/Data through/)).not.toBeInTheDocument()
    expect(screen.queryByTestId('rollup-data-through-slot')).not.toBeInTheDocument()
  })
})
