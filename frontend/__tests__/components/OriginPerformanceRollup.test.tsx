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
import OriginClient from '@/app/origin/_sections/OriginClient'
import PerformanceClient from '@/app/performance/_sections/PerformanceClient'
import { useTimezoneStore } from '@/stores/filterStore'

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
  window.history.replaceState(null, '', '/')
})

afterEach(() => {
  setCloudModeRuntime(false)
  cleanup()
})

describe('OriginClient on Cloud', () => {
  it('paints summary-served origin IP health while the raw sections load, with ≈ and ≥ marks', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/origin/aggregates', (asked) => asked?.includes('summary') ? never() : {
      ...originRaw,
      ip_health: { has_data: true, rows: [ipRow] },
      rollup: meta(['ip_health.p50_ms', 'ip_health.p95_ms'], ['ip_health.requests']),
    })
    renderWithQuery(<OriginClient />, { withNuqs: true })

    expect(await screen.findByText(/Data through 09:45/)).toBeInTheDocument()
    const row = (await screen.findByText('203.0.113.1')).closest('tr')!
    expect(within(row).getByText('≥1,234')).toBeInTheDocument()
    expect(within(row).getByText('≈45.0ms')).toBeInTheDocument()
    expect(within(row).getByText('≈120.0ms')).toBeInTheDocument()
    expect(within(row).getByText('0.5%')).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['ip_health'], ORIGIN_SECTIONS.filter((s) => s !== 'ip_health')]))
    expect(screen.getByText('Loading origin latency timeseries...')).toBeInTheDocument()
  })

  it('asks summaries for the OTTLB series too and marks it approximate', async () => {
    setCloudModeRuntime(true)
    window.history.replaceState(null, '', '/?metric=ttlb')
    const sections = recordPosts('/api/origin/aggregates', (asked) => asked?.includes('summary') ? originRaw : {
      ...originRaw,
      timeseries: { has_data: true, series: [{ time: '2027-01-15T08:00:00.000Z', miss_count: 5, value: 80 }] },
      rollup: meta(['ip_health.p50_ms', 'ip_health.p95_ms', 'timeseries.value'], []),
    })
    renderWithQuery(<OriginClient />, { withNuqs: true })

    expect(await screen.findByText('Approximate')).toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([['ip_health', 'timeseries']]))
    expect(sections.find((s) => s?.includes('summary'))).toEqual(
      ORIGIN_SECTIONS.filter((s) => s !== 'ip_health' && s !== 'timeseries'),
    )
    expect(within(screen.getByText('203.0.113.1').closest('tr')!).getByText('1,234')).toBeInTheDocument()
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
  })
})

const waterfall = { avg: { edge_processing: 1.5, origin_wait: 40, origin_download: 3, client_download: 7 } }
const ttlDist = [{ bucket: '< 60s', count: 9 }, { bucket: '> 1d', count: 4 }]

describe('PerformanceClient on Cloud', () => {
  it('paints the summary-served waterfall and TTL buckets while URL and network rankings load', async () => {
    setCloudModeRuntime(true)
    const sections = recordPosts('/api/performance/aggregates', (asked) => asked?.includes('top_urls') ? never() : {
      ttl_dist: ttlDist, waterfall, _approx: false, rollup: meta([], ['ttl_dist']),
    })
    renderWithQuery(<PerformanceClient />, { withNuqs: true })

    expect(await screen.findByText(/Data through 09:45/)).toBeInTheDocument()
    expect(await screen.findByText('Cache TTL Distribution (≥ counts)')).toBeInTheDocument()
    const plots = screen.getAllByTestId('plot').map((plot) => plot.textContent)
    expect(plots).toContain(JSON.stringify([['< 60s', '> 1d'], [9, 4]]))
    expect(plots.some((plot) => plot?.includes(JSON.stringify([[40], ['Origin TTFB Wait']])))).toBe(true)
    expect(sections).toEqual(expect.arrayContaining([['ttl_dist', 'waterfall'], ['top_urls', 'top_asns', 'scatter']]))
    expect(screen.getByText('Profiling endpoint latency...')).toBeInTheDocument()
  })

  it('keeps one raw read, with no summary labels, when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordPosts('/api/performance/aggregates', () => ({
      ttl_dist: ttlDist, waterfall, top_urls: [], top_asns: [], scatter: [], _approx: false,
    }))
    renderWithQuery(<PerformanceClient />, { withNuqs: true })

    expect(await screen.findByText('Cache TTL Distribution')).toBeInTheDocument()
    expect(sections).toEqual([undefined])
    expect(screen.queryByText(/Data through/)).not.toBeInTheDocument()
  })
})
