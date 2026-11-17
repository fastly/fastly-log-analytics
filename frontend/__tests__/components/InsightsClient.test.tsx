/**
 * @vitest-environment jsdom
 */
import { renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { cleanup, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { delay } from 'msw/utils/delay'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { InsightsClient } from '@/components/Insights'
import { useServiceStore } from '@/stores/filterStore'

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useEffectiveServiceId: () => 'svc-1',
}))

vi.mock('@/components/ReportLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ReportLayout')>()
  const ReportLayout = (props: { children: (ctx: { activeServiceId: string }) => React.ReactNode }) => (
    <div>{props.children({ activeServiceId: 'svc-1' })}</div>
  )
  return { ...actual, default: ReportLayout, ReportLayout }
})

afterEach(cleanup)

describe('InsightsClient first load', () => {
  it('scans once with the history-adapted defaults instead of first scanning the static 24h/7d pair', async () => {
    const earliest = new Date(Date.now() - 72 * 3600_000).toISOString()
    const bodies: Array<{ window_size_hrs: number; baseline_hours: number }> = []
    server.use(
      http.get(`${getApiBase()}/api/log-extents`, async () => {
        await delay(50)
        return HttpResponse.json({ earliest_log_at: earliest, latest_log_at: new Date().toISOString(), count: 10 })
      }),
      http.post(`${getApiBase()}/api/insights`, async ({ request }) => {
        bodies.push((await request.json()) as { window_size_hrs: number; baseline_hours: number })
        return HttpResponse.json({ insights: [], next_cursor: null })
      }),
    )

    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })
    expect(screen.getAllByText(/Evaluating .*baseline comparisons/).length).toBeGreaterThan(0)

    await waitFor(() => expect(bodies.length).toBeGreaterThan(0), { timeout: 3000 })
    await new Promise((resolve) => setTimeout(resolve, 600))
    expect(bodies.map((b) => [b.window_size_hrs, b.baseline_hours])).toEqual([[24, 24]])
  })

  it('marks only the month-baseline prefetch so Cloud gives it its own smaller budget', async () => {
    const calls: Array<{ baseline: number; prefetch: string | null }> = []
    server.use(
      http.get(`${getApiBase()}/api/log-extents`, () => HttpResponse.json({
        earliest_log_at: new Date(Date.now() - 800 * 3600_000).toISOString(), latest_log_at: new Date().toISOString(), count: 10,
      })),
      http.post(`${getApiBase()}/api/insights`, async ({ request }) => {
        const body = (await request.json()) as { baseline_hours: number }
        calls.push({ baseline: body.baseline_hours, prefetch: request.headers.get('x-flp-cloud-prefetch') })
        return HttpResponse.json({ insights: [], next_cursor: null })
      }),
    )
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })
    await waitFor(() => expect(calls.some((call) => call.baseline === 720)).toBe(true), { timeout: 6000 })
    expect(calls.filter((call) => call.baseline === 720).every((call) => call.prefetch === '1')).toBe(true)
    expect(calls.filter((call) => call.baseline !== 720).map((call) => call.prefetch)).toEqual([null])
  }, 10_000)
})

function envelope(overrides: Record<string, unknown>) {
  return {
    insights: [], next_cursor: null, window_start: '', window_end: '', baseline_start: '', baseline_end: '',
    computed_at: '', window_hours: 24, baseline_hours: 24, ...overrides,
  }
}

function card(id: string, severity: string, summary: string, placeholder?: boolean) {
  return { id, title: `Title ${id}`, description: `About ${id}`, category: 'security', severity, summary, items: [], placeholder }
}

function serveInsights(responses: Array<Record<string, unknown>>) {
  let calls = 0
  server.use(
    http.get(`${getApiBase()}/api/log-extents`, () =>
      HttpResponse.json({ earliest_log_at: new Date(Date.now() - 72 * 3600_000).toISOString(), latest_log_at: new Date().toISOString() })),
    http.post(`${getApiBase()}/api/insights`, () => {
      const body = responses[Math.min(calls, responses.length - 1)]
      calls += 1
      return HttpResponse.json(body)
    }),
  )
  return () => calls
}

const tabCount = (key: string) => document.querySelector(`[data-tab="${key}"] .tabular-nums`)?.textContent

describe('InsightsClient background evaluation', () => {
  afterEach(() => { vi.useRealTimers() })

  it('polls while the first evaluation is pending without counting placeholder cards, then stops', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const calls = serveInsights([
      envelope({ pending: true, refreshing: false, insights: [
        card('ddos', 'info', 'Queued for evaluation', true),
        card('waf', 'info', 'Queued for evaluation', true),
        card('bots', 'info', 'Requires 24h of historical data (only 3h available)'),
        card('scan', 'info', 'Evaluating looks unusual', false),
      ] }),
      envelope({ pending: false, refreshing: false, computed_at: new Date().toISOString(), insights: [
        card('ddos', 'warning', 'Spike'),
        card('bots', 'info', 'Requires 24h of historical data (only 3h available)'),
      ] }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    await waitFor(() => expect(calls()).toBe(1))
    expect(await screen.findByText(/Evaluating/, { selector: '[role="status"] *, [role="status"]' })).toBeInTheDocument()
    expect(tabCount('all')).toBe('2')

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(calls()).toBe(2))
    await waitFor(() => expect(tabCount('all')).toBe('2'))
    expect(screen.queryByRole('status')).toBeNull()

    await vi.advanceTimersByTimeAsync(20000)
    expect(calls()).toBe(2)
  })

  it('shows a refreshing result with its computed time and refresh error, polling until the refresh lands', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const computed = new Date(Date.now() - 600_000).toISOString()
    const calls = serveInsights([
      envelope({ pending: false, refreshing: true, refresh_error: 'ClickHouse timeout', computed_at: computed, insights: [card('ddos', 'warning', 'Spike')] }),
      envelope({ pending: false, refreshing: false, computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    expect(await screen.findByRole('status')).toHaveTextContent(/Refreshing \(computed .+\)/)
    expect(screen.getByText(/ClickHouse timeout/)).toBeInTheDocument()

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(calls()).toBe(2))
    await waitFor(() => expect(screen.queryByRole('status')).toBeNull())
    expect(screen.queryByText(/ClickHouse timeout/)).toBeNull()
  })

  it('does not poll a response without the background fields (Cloud)', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const calls = serveInsights([envelope({ computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] })])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })
    await waitFor(() => expect(tabCount('all')).toBe('1'))
    await vi.advanceTimersByTimeAsync(20000)
    expect(calls()).toBe(1)
    expect(screen.queryByRole('status')).toBeNull()
  })
})

describe('InsightsClient queued evaluation', () => {
  afterEach(() => { vi.useRealTimers() })

  it('treats a pending response with no cards as still evaluating and keeps polling', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    server.use(http.get(`${getApiBase()}/api/insight-availability`, () => HttpResponse.json({
      insights: [
        { id: 'ddos', title: 'DDoS Detection', description: 'About ddos', category: 'security', available: true },
        { id: 'waf', title: 'WAF Activity', description: 'About waf', category: 'security', available: true },
      ],
      unavailable: [],
    })))
    const calls = serveInsights([
      envelope({ pending: true, refreshing: false, insights: [] }),
      envelope({ pending: false, refreshing: false, computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    await waitFor(() => expect(calls()).toBe(1))
    expect(await screen.findByRole('status')).toHaveTextContent(/Evaluating/)
    expect(screen.queryByText(/No anomalies detected/)).toBeNull()
    expect(screen.queryByTestId('insights-loading')).toBeNull()
    const grid = screen.getAllByTestId('insights-grid-loading')[0]
    expect(grid).toHaveTextContent('DDoS Detection')
    expect(grid).toHaveTextContent('WAF Activity')

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(tabCount('all')).toBe('1'))
    expect(calls()).toBe(2)
  })
})

describe('InsightsClient layout stability while evaluating', () => {
  afterEach(() => { vi.useRealTimers() })

  it('renders queued placeholders as skeleton cards in the grid instead of collapsing to a spinner', async () => {
    serveInsights([envelope({ pending: true, refreshing: false, insights: [
      card('ddos', 'info', 'Queued for evaluation', true),
      card('waf', 'info', 'Queued for evaluation', true),
    ] })])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    const grid = (await screen.findAllByTestId('insights-grid'))[0]
    expect(grid).toHaveTextContent('Title ddos')
    expect(grid).toHaveTextContent('Title waf')
    expect(screen.queryByTestId('insights-loading')).toBeNull()
    expect(tabCount('all')).toBe('0')
  })

  it('keeps one status slot mounted from first paint through pending, error and final results', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const calls = serveInsights([
      envelope({ pending: true, refreshing: false, insights: [card('ddos', 'info', 'Queued for evaluation', true)] }),
      envelope({ pending: false, refreshing: true, refresh_error: 'ClickHouse timeout', computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
      envelope({ pending: false, refreshing: false, computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    const slot = screen.getByTestId('insights-status-slot')
    await waitFor(() => expect(slot).toHaveTextContent(/Evaluating/))

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(calls()).toBe(2))
    await waitFor(() => expect(slot).toHaveTextContent(/ClickHouse timeout/))
    expect(screen.getByTestId('insights-status-slot')).toBe(slot)

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(calls()).toBe(3))
    await waitFor(() => expect(slot).toBeEmptyDOMElement())
    expect(screen.getByTestId('insights-status-slot')).toBe(slot)
  })
})

describe('InsightsClient deferred baseline follow-up', () => {
  const windowEnd = new Date(Date.now() - 3600_000).toISOString()
  const first = envelope({
    computed_at: new Date().toISOString(), window_end: windowEnd, deferred_detectors: ['slow'],
    rollup: { data_from: new Date(Date.now() - 48 * 3600_000).toISOString(), data_through: windowEnd, approximate: [], lower_bound: [] },
    insights: [card('fast', 'warning', 'Spike'), card('slow', 'info', 'Computing baseline…', true)],
  })
  type Body = { detectors?: string[]; end_time?: string; baseline_hours: number; window_size_hrs: number }

  function serveFollowUp(followUp: () => Promise<Response>) {
    const followUps: Array<{ body: Body; prefetch: string | null }> = []
    server.use(
      http.get(`${getApiBase()}/api/log-extents`, () =>
        HttpResponse.json({ earliest_log_at: new Date(Date.now() - 72 * 3600_000).toISOString(), latest_log_at: new Date().toISOString() })),
      http.post(`${getApiBase()}/api/insights`, async ({ request }) => {
        const body = (await request.json()) as Body
        if (!body.detectors) return HttpResponse.json(first)
        followUps.push({ body, prefetch: request.headers.get('x-flp-cloud-prefetch') })
        return followUp()
      }),
    )
    return followUps
  }

  it('renders the summary-served cards first and fills the computing-baseline cards when the follow-up lands', async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const followUps = serveFollowUp(async () => {
      await gate
      return HttpResponse.json(envelope({ window_end: windowEnd, insights: [card('slow', 'critical', 'Slow spike')] }))
    })
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    await waitFor(() => expect(followUps).toHaveLength(1))
    const grid = screen.getAllByTestId('insights-grid')[0]
    expect(grid).toHaveTextContent('Spike')
    expect(grid).toHaveTextContent('Computing baseline…')
    expect(grid).not.toHaveTextContent('Slow spike')
    expect(tabCount('all')).toBe('1')
    expect(followUps[0].body).toMatchObject({ detectors: ['slow'], end_time: windowEnd, window_size_hrs: 24, baseline_hours: 24 })
    expect(followUps[0].prefetch).toBeNull()

    release()
    await waitFor(() => expect(screen.getAllByTestId('insights-grid')[0]).toHaveTextContent('Slow spike'))
    expect(screen.getAllByTestId('insights-grid')[0]).not.toHaveTextContent('Computing baseline…')
    expect(tabCount('all')).toBe('2')
    expect(followUps).toHaveLength(1)
  })

  it('fills the computing-baseline cards in place instead of re-sorting the grid by their new severity', async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    serveFollowUp(async () => {
      await gate
      return HttpResponse.json(envelope({ window_end: windowEnd, insights: [card('slow', 'critical', 'Slow spike')] }))
    })
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })
    const order = () => within(screen.getAllByTestId('insights-grid')[0])
      .getAllByText(/^Title (fast|slow)$/).map((title) => title.textContent)

    await waitFor(() => expect(screen.getAllByTestId('insights-grid')[0]).toHaveTextContent('Computing baseline…'))
    expect(order()).toEqual(['Title fast', 'Title slow'])

    release()
    await waitFor(() => expect(screen.getAllByTestId('insights-grid')[0]).toHaveTextContent('Slow spike'))
    expect(order()).toEqual(['Title fast', 'Title slow'])
  })

  it('shows the computing-baseline cards in their normal error state when the follow-up fails', async () => {
    serveFollowUp(async () => HttpResponse.json({ detail: 'Local query failed' }, { status: 500 }))
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    await waitFor(() => expect(screen.getAllByTestId('insights-grid')[0]).toHaveTextContent(/Query failed: .*Local query failed/), { timeout: 5000 })
    const grid = screen.getAllByTestId('insights-grid')[0]
    expect(grid).toHaveTextContent('Title slow')
    expect(grid).not.toHaveTextContent('Computing baseline…')
    expect(grid).not.toHaveTextContent('Not evaluated')
    expect(screen.queryByText(/Error loading insights/)).toBeNull()
  })
})

describe('InsightsClient notices', () => {
  const unsupported = {
    id: 'botnet_grouping', title: 'Botnet Grouping', description: 'About botnets', category: 'security',
    required_fields: ['ja3'], required_groups: [], available: false, missing_fields: ['ja3'], reason: 'cloud_detector_unsupported',
  }

  it('shows the unavailable-detectors notice as status, not an error alert, beside a successful result', async () => {
    server.use(http.get(`${getApiBase()}/api/insight-availability`, () => HttpResponse.json({
      insights: [unsupported], unavailable: [unsupported], available: true,
    })))
    serveInsights([envelope({ computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] })])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    const notice = await screen.findByText(/1 insight is unavailable/)
    await waitFor(() => expect(tabCount('all')).toBe('1'))
    expect(notice.closest('[role="status"]')).not.toBeNull()
    expect(screen.queryAllByRole('alert')).toEqual([])
  })

  it('says when the baseline came from log summaries, through when, and what ≈ and ≥ mean', async () => {
    const dataThrough = new Date(Date.now() - 25 * 3600_000).toISOString()
    serveInsights([
      envelope({ computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
      envelope({
        computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')],
        rollup: { data_from: new Date(Date.now() - 48 * 3600_000).toISOString(), data_through: dataThrough, approximate: ['city_surges'], lower_bound: [] },
      }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    const { queryClient } = renderWithQuery(<InsightsClient />, { withNuqs: true })
    await waitFor(() => expect(tabCount('all')).toBe('1'))
    expect(screen.queryByText(/Baseline from log summaries/)).toBeNull()

    await queryClient.refetchQueries({ queryKey: ['insights'] })
    const note = await screen.findByText(/Baseline from log summaries through \d\d:\d\d/)
    expect(note.closest('[role="note"]')).not.toBeNull()
    expect(note).toHaveTextContent(/24h window is read from raw logs.*≈ are estimated.*≥ are lower bounds/)
  })

  it('still shows a failed requested window as an error alert', async () => {
    server.use(
      http.get(`${getApiBase()}/api/log-extents`, () =>
        HttpResponse.json({ earliest_log_at: new Date(Date.now() - 72 * 3600_000).toISOString(), latest_log_at: new Date().toISOString() })),
      http.post(`${getApiBase()}/api/insights`, () =>
        HttpResponse.json({ detail: 'Cloud query range exceeds the browser scan budget; choose a smaller range' }, { status: 413 })),
    )
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    expect(await screen.findByRole('alert', {}, { timeout: 5000 })).toHaveTextContent(/Error loading insights/)
  })
})
