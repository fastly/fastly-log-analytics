/**
 * @vitest-environment jsdom
 */
import { renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { cleanup, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { delay, http, HttpResponse } from 'msw'
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
    const calls = serveInsights([
      envelope({ pending: true, refreshing: false, insights: [] }),
      envelope({ pending: false, refreshing: false, computed_at: new Date().toISOString(), insights: [card('ddos', 'warning', 'Spike')] }),
    ])
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    renderWithQuery(<InsightsClient />, { withNuqs: true })

    await waitFor(() => expect(calls()).toBe(1))
    expect(await screen.findByRole('status')).toHaveTextContent(/Evaluating/)
    expect(screen.queryByText(/No anomalies detected/)).toBeNull()
    expect(screen.getAllByTestId('insights-loading').length).toBeGreaterThan(0)

    await vi.advanceTimersByTimeAsync(6000)
    await waitFor(() => expect(tabCount('all')).toBe('1'))
    expect(calls()).toBe(2)
  })
})
