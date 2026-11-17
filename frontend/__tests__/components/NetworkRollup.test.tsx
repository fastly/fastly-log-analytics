/**
 * @vitest-environment jsdom
 */
import { renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { NetworkClient } from '@/app/network/_sections/NetworkClient'
import { PopHealthHeatmap } from '@/components/network/PopHealthHeatmap'
import { useTimezoneStore } from '@/stores/filterStore'

const layout = vi.hoisted(() => ({ filterPayload: {} as Record<string, unknown> }))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: Record<string, unknown>) => React.ReactNode }) => (
    <div>{props.children({
      activeServiceId: 'svc-1', startTime: '2027-01-15T08:00:00Z', endTime: '2027-01-15T10:00:00Z',
      filterPayload: layout.filterPayload,
    })}</div>
  )
  return { ...actual, default: ReportLayout, ReportLayout }
})

vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/lib/cloud-engine/adapter', () => ({ handleCloudAnalyticalRequest: async () => null }))

vi.mock('@/components/Map/NetworkMap', () => ({
  NetworkMap: ({ data }: { data?: { cities?: unknown[] } }) => <div data-testid="map">{data?.cities?.length ?? 0} cities</div>,
}))

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: ({ data }: { data: Array<{ hovertemplate?: string }> }) => (
    <div data-testid="plot">{data.map((trace) => trace.hovertemplate).join('|')}</div>
  ),
}))

const ROLLUP_SECTIONS = ['summary', 'leaderboard', 'metro_leaderboard', 'heatmap', 'buckets']
const MAP_SECTIONS = ['buckets', 'cities', 'map_buckets']
const BUCKET = '2027-01-15T08:00:00.000Z'

const rollup = (lowerBound: string[]) => ({
  data_from: '2027-01-15T08:00:00.000Z',
  data_through: '2027-01-15T09:45:00.000Z',
  approximate: ['health_score', 'global_health_score', 'health_score_now', 'worst_asn', 'worst_country'],
  lower_bound: lowerBound,
})

function coreResponse(meta?: ReturnType<typeof rollup>) {
  return {
    available: true, has_data: true, has_metro: true,
    summary: {
      total_requests: 1234, global_health_score: 87, avg_rtt_ms: 21.5,
      worst_asn: { label: 'AS64500', score: 40 }, worst_country: { label: 'BR', score: 55 },
    },
    leaderboard: [{ asn: 64500, label: 'AS64500', total_reqs: 1234, health_score_now: 40 }],
    metro_leaderboard: [{ city: 'Lisbon', country: 'PT', raw_city: 'Lisbon', total_reqs: 77, health_score: 60 }],
    heatmap: [{ asn: 64500, label: 'AS64500', total_reqs: 1234, buckets: [{ bucket: BUCKET, health_score: 40 }] }],
    buckets: [BUCKET], cities: [], map_buckets: [], by_country: [], by_asn: [], timeseries: [],
    ...meta ? { rollup: meta } : {},
  }
}

function recordNetworkHealth(core: ReturnType<typeof coreResponse>) {
  const sections: string[][] = []
  server.use(http.post(`${getApiBase()}/api/network-health`, async ({ request }) => {
    const body = await request.json() as { sections: string[] }
    sections.push(body.sections)
    return HttpResponse.json(body.sections.includes('cities')
      ? { ...coreResponse(), cities: [{ name: 'Lisbon', lat: 38.7, lon: -9.1 }] }
      : core)
  }))
  return sections
}

beforeEach(() => {
  useTimezoneStore.setState({ timezone: 'UTC' })
  layout.filterPayload = {}
  server.use(http.get(`${getApiBase()}/api/network/pop-health`, () => HttpResponse.json({ data: [] })))
})

afterEach(() => {
  setCloudModeRuntime(false)
  cleanup()
})

describe('NetworkClient on Cloud', () => {
  it('asks summaries for the unfiltered core, reads the map from raw rows apart, and labels what summaries answer', async () => {
    setCloudModeRuntime(true)
    const sections = recordNetworkHealth(coreResponse(rollup(['leaderboard', 'metro_leaderboard'])))
    renderWithQuery(<NetworkClient />, { withNuqs: true })

    expect(await screen.findByText(/Data through 09:45/)).toBeInTheDocument()
    // Summaries reach the newest raw window, so the note claims no unsummarized backlog.
    expect(screen.queryByText(/not summarized yet/)).not.toBeInTheDocument()
    expect(sections).toEqual(expect.arrayContaining([ROLLUP_SECTIONS, MAP_SECTIONS]))
    expect(await screen.findByText('1 cities')).toBeInTheDocument()
    expect(screen.getByText('≈87/100')).toBeInTheDocument()
    expect(screen.getByText('Score: ≈40')).toBeInTheDocument()
    expect(screen.getByText('≥1,234')).toBeInTheDocument()
    expect(screen.getByText('≥77')).toBeInTheDocument()
    expect(screen.getByText('Avg RTT').closest('div')?.parentElement?.textContent).not.toContain('≈')
    await waitFor(() => expect(screen.getAllByTestId('plot').some((plot) => plot.textContent?.includes('Score: ≈%{z}'))).toBe(true))
  })

  it('keeps one raw core read, with no summary labels, when a filter is applied', async () => {
    setCloudModeRuntime(true)
    layout.filterPayload = { pop: { mode: 'include', values: ['LIS'] } }
    const sections = recordNetworkHealth(coreResponse())
    renderWithQuery(<NetworkClient />, { withNuqs: true })

    expect(await screen.findByText('87/100')).toBeInTheDocument()
    expect(sections.filter((s) => !s.includes('shielding_analysis')))
      .toEqual([[...ROLLUP_SECTIONS, 'cities', 'map_buckets']])
    expect(screen.queryByText(/Data through/)).not.toBeInTheDocument()
    expect(screen.getByText('1,234')).toBeInTheDocument()
  })
})

describe('PopHealthHeatmap', () => {
  const pop = {
    pop: 'LIS', requests: 500, errors: 0, error_rate: 0, bandwidth_bytes: 1024,
    cache_hit_rate: 40, p50_rtt_us: 12_000, p95_ttfb_ms: 150,
  }
  const respond = (body: Record<string, unknown>) => server.use(
    http.get(`${getApiBase()}/api/network/pop-health`, () => HttpResponse.json(body)),
  )

  it('marks a summary-served hit rate as a lower bound and its latency approximate', async () => {
    respond({ data: [pop], rollup: { ...rollup([]), approximate: ['p50_rtt_us', 'p95_ttfb_ms'], lower_bound: ['cache_hit_rate'] } })
    renderWithQuery(<PopHealthHeatmap serviceId="svc-1" startTime="2027-01-15T08:00:00Z" endTime="2027-01-15T10:00:00Z" />)
    fireEvent.click(await screen.findByText('≥40.0% Hit'))
    expect(await screen.findByText('≈12.0 ms')).toBeInTheDocument()
    expect(screen.getByText('≈150.0 ms')).toBeInTheDocument()
    expect(screen.getByText('≥40.00%')).toBeInTheDocument()
  })

  it('shows raw-scan values unmarked', async () => {
    respond({ data: [pop] })
    renderWithQuery(<PopHealthHeatmap serviceId="svc-1" startTime="2027-01-15T08:00:00Z" endTime="2027-01-15T10:00:00Z" />)
    fireEvent.click(await screen.findByText('40.0% Hit'))
    expect(await screen.findByText('12.0 ms')).toBeInTheDocument()
  })
})
