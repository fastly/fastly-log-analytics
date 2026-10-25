/**
 * @vitest-environment jsdom
 *
 * Tests for InsightCard and InsightHelpModal content helpers.
 */
import { stubResizeObserver, stubPointerCaptureAndScroll, renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, test, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { axe } from 'vitest-axe'
import React, { isValidElement } from 'react'
import { InsightCard, ScriptedTrafficModal, type ScriptedTrafficData } from '@/components/Insights/InsightCard'
import { InsightItemRow } from '@/components/Insights/InsightItemRow'
import { CacheCollapseModal } from '@/components/Insights/CacheCollapseModal'
import { getPerformanceContent } from '@/components/Insights/InsightHelpModal/sections/performance'
import { getSecurityContent } from '@/components/Insights/InsightHelpModal/sections/security'
import { getTrafficContent } from '@/components/Insights/InsightHelpModal'
import type { InsightCardData } from '@/types/api'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'

const { realInsightSubcomponentsRef } = vi.hoisted(() => ({
  realInsightSubcomponentsRef: { value: false },
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())
vi.mock('@/components/AnalyticsCard', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/AnalyticsCard')>()),
  TimeSeriesChart: () => <div data-testid="timeseries" />,
}))

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  if (typeof Element !== 'undefined' && !Element.prototype.getAnimations) {
    Element.prototype.getAnimations = () => []
  }
})

// Project's vitest setup doesn't auto-cleanup between tests, so do it manually.
afterEach(() => {
  realInsightSubcomponentsRef.value = false
  cleanup()
})

// Stub the modal children — their internals (maplibre, query hooks) aren't
// relevant to InsightCard rendering tests, while preserving getTrafficContent.
vi.mock('@/components/Insights/InsightHelpModal', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Insights/InsightHelpModal')>()),
  InsightHelpModal: ({ isOpen }: { isOpen: boolean }) =>
    isOpen ? <div data-testid="help-modal-open" /> : null,
}))
vi.mock('@/components/Insights/ImpossibleDistanceModal', () => ({
  ImpossibleDistanceModal: ({ isOpen }: { isOpen: boolean }) =>
    isOpen ? <div data-testid="distance-modal-open" /> : null,
}))
vi.mock('@/components/Insights/CacheCollapseModal', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Insights/CacheCollapseModal')>()
  return {
    ...actual,
    CacheCollapseModal: (props: React.ComponentProps<typeof actual.CacheCollapseModal>) =>
      realInsightSubcomponentsRef.value ? (
        <actual.CacheCollapseModal {...props} />
      ) : props.isOpen ? (
        <div data-testid="collapse-modal-open" />
      ) : null,
  }
})
vi.mock('@/components/Insights/InsightItemRow', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Insights/InsightItemRow')>()
  return {
    ...actual,
    InsightItemRow: (props: React.ComponentProps<typeof actual.InsightItemRow>) =>
      realInsightSubcomponentsRef.value ? (
        <actual.InsightItemRow {...props} />
      ) : (
        <div data-testid="insight-item-row">{props.insight.items[props.index].label ?? 'no-label'}</div>
      ),
  }
})

type ErrorSpikesCard = Extract<InsightCardData, { id: 'error_spikes' }>

const errorItem = (label: string, current_val: number, severity: string): ErrorSpikesCard['items'][number] => ({
  label,
  current_val,
  severity,
  baseline_label: 'baseline',
  meta: { requests: 100, errors: 40, filters: { url: label } },
})

const _BASE_INSIGHT: ErrorSpikesCard = {
  id: 'error_spikes',
  title: 'Error Spikes',
  description: 'URLs with abnormally elevated 5xx error rates',
  severity: 'critical',
  summary: '3 URLs with elevated server error rates',
  items: [
    errorItem('/api/users', 0.45, 'critical'),
    errorItem('/api/orders', 0.35, 'warning'),
    errorItem('/api/auth', 0.25, 'warning'),
  ],
}

const manyItems = () => Array.from({ length: 8 }, (_, i) => errorItem(`/path/${i}`, 0.1, 'warning'))

describe('InsightCard', () => {
  it('renders title, summary, and severity badge', () => {
    render(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('3 URLs with elevated server error rates')).toBeTruthy()
    // Severity badge surfaces as uppercase text
    expect(screen.getByText(/^critical$/i)).toBeTruthy()
  })

  it('renders one row per item up to 5', () => {
    render(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(3)
  })

  it('shows "Show N more" button when items > 5', () => {
    const many: ErrorSpikesCard = { ..._BASE_INSIGHT, items: manyItems() }
    render(<InsightCard insight={many} windowHours="24" baselineHours="168" />)
    // First 5 visible
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(5)
    // "Show 3 more" button
    expect(screen.getByText(/Show 3 more/i)).toBeTruthy()
  })

  it('does not show "Show more" when items <= 5', () => {
    render(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.queryByText(/Show \d+ more/i)).toBeNull()
  })

  it('renders the help modal when the help button is clicked', async () => {
    const user = userEvent.setup()
    render(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    expect(screen.queryByTestId('help-modal-open')).toBeNull()
    await user.click(screen.getByTitle('How this works'))
    expect(screen.getByTestId('help-modal-open')).toBeTruthy()
  })

  it('renders the data modal when "Show N more" is clicked', async () => {
    const user = userEvent.setup()
    const many: ErrorSpikesCard = { ..._BASE_INSIGHT, items: manyItems() }
    render(<InsightCard insight={many} windowHours="24" baselineHours="168" />)
    expect(screen.queryByTestId('data-modal-open')).toBeNull()
    // The "Show 3 more" button is a real <button>; role lookup is
    // sturdier than text matching against the localized label.
    await user.click(screen.getByRole('button', { name: /show 3 more/i }))
    expect(screen.getByTestId('data-modal-open')).toBeTruthy()
  })

  it('falls back gracefully on unknown severity (no crash, default icon)', () => {
    // Defensive: if backend ever introduces a new severity the FE doesn't
    // know about, the card must still render rather than blank.
    const weird = { ..._BASE_INSIGHT, severity: 'urgent', items: [] }
    render(<InsightCard insight={weird} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
  })

  it('renders empty-items state without crashing', () => {
    // The "clean" / "info" severity case — summary text, no item rows.
    const clean = {
      ..._BASE_INSIGHT,
      severity: 'clean',
      summary: 'No error spikes detected',
      items: [],
    }
    render(<InsightCard insight={clean} windowHours="24" baselineHours="168" />)
    expect(screen.getByText('No error spikes detected')).toBeTruthy()
    expect(screen.queryAllByTestId('insight-item-row')).toHaveLength(0)
  })

  // TESTING_PLAN_3 item 19. Catch a11y regressions that contrast-checks
  // and component-library upgrades can introduce silently. Limited to
  // rule sets that work under jsdom (color-contrast needs a real layout
  // engine, so disable it; we still cover label/role/aria coverage).
  it('has no axe-detectable a11y violations', async () => {
    const { container } = render(<InsightCard insight={_BASE_INSIGHT} windowHours="24" baselineHours="168" />)
    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    })
    expect(results).toHaveNoViolations()
  })

  it('multiple InsightCards with distinct ids render distinct elements', () => {
    // Regression guard for the duplicate-React-key bug (backend used to
    // emit ``id="insight"`` for every failed insight). On the FE side,
    // distinct ids should produce distinct rendered cards.
    const a: InsightCardData = { ..._BASE_INSIGHT, title: 'Error Spikes' }
    const b: InsightCardData = { ..._BASE_INSIGHT, id: 'botnet_grouping', title: 'Botnet Grouping', items: [] }
    render(
      <>
        <InsightCard insight={a} windowHours="24" baselineHours="168" />
        <InsightCard insight={b} windowHours="24" baselineHours="168" />
      </>,
    )
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('Botnet Grouping')).toBeTruthy()
  })
})

const PERFORMANCE_IDS = [
  'city_latency_regressions',
  'asn_metro_performance',
  'latency_regression',
  'network_asn_health',
  'pop_latency_regression',
  'metro_delivery_degradation',
  'connection_type_mix',
  'http3_fallback',
  'payload_compression_regression',
  'timeout_split',
] as const

const SECURITY_IDS = [
  'impossible_distance',
  'ua_monoculture',
  'new_probe_urls',
  'waf_signal_spikes',
  'proxy_surge',
  'botnet_grouping',
  'low_and_slow',
  'credential_enumeration',
  'content_discovery',
  'session_harvesting',
  'repeated_patterns',
] as const

const ARMS_WITH_DIAGRAM = new Set<string>(['impossible_distance', 'repeated_patterns'])

const TRAFFIC_IDS = [
  'city_surges',
  'new_city_traffic',
  'new_country_traffic',
  'asn_concentration',
  'referer_monoculture',
  'method_drift',
  'new_asn_traffic',
  'asn_hosting_shift',
] as const

describe('getPerformanceContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getPerformanceContent('nope_not_a_real_insight')).toBeNull()
    expect(getPerformanceContent('')).toBeNull()
  })

  test.each(PERFORMANCE_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getPerformanceContent(id)
    expect(content).not.toBeNull()
    if (!content) return

    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)
    expect(isValidElement(content.icon)).toBe(true)
    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')
    expect(new Set(content.fields).size).toBe(content.fields.length)

    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('performance arms do not require a diagram and expose distinct titles', () => {
    for (const id of PERFORMANCE_IDS) {
      expect(getPerformanceContent(id)?.diagram).toBeUndefined()
    }
    const titles = PERFORMANCE_IDS.map((id) => getPerformanceContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })

  test('asn_metro_performance lists ASN/metro fields and network_asn_health surfaces kernel metrics', () => {
    const content = getPerformanceContent('asn_metro_performance')!
    expect(content.fields).toContain('asn')
    expect(content.fields).toContain('metro')
    expect(content.fields).toContain('tcp_rtt')

    const health = getPerformanceContent('network_asn_health')!
    const { container, unmount } = render(<>{health.description}</>)
    expect(container.textContent).toMatch(/Packet Loss/i)
    expect(container.textContent).toMatch(/Jitter/i)
    unmount()
  })
})

describe('getSecurityContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getSecurityContent('nope_not_a_real_insight')).toBeNull()
    expect(getSecurityContent('')).toBeNull()
  })

  test.each(SECURITY_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getSecurityContent(id)
    expect(content).not.toBeNull()
    if (!content) return

    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)
    expect(isValidElement(content.icon)).toBe(true)
    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')
    expect(new Set(content.fields).size).toBe(content.fields.length)

    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('impossible_distance and repeated_patterns include diagrams; only documented arms ship one', () => {
    const idist = getSecurityContent('impossible_distance')
    expect(idist?.diagram).toBeTruthy()
    const { container: c1, unmount: u1 } = render(<>{idist!.diagram}</>)
    expect(c1.textContent).toMatch(/Speed of Light/i)
    u1()

    const rpat = getSecurityContent('repeated_patterns')
    expect(rpat?.diagram).toBeTruthy()
    expect(rpat?.fields).toEqual(['ip', 'timestamp'])
    const { container: c2, unmount: u2 } = render(<>{rpat!.diagram}</>)
    expect(c2.textContent).toMatch(/Automated script/i)
    expect(c2.textContent).toMatch(/Human browsing/i)
    u2()

    for (const id of SECURITY_IDS) {
      const diagram = getSecurityContent(id)?.diagram
      if (ARMS_WITH_DIAGRAM.has(id)) {
        expect(diagram).toBeTruthy()
      } else {
        expect(diagram).toBeUndefined()
      }
    }
    const titles = SECURITY_IDS.map((id) => getSecurityContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })
})

describe('getTrafficContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getTrafficContent('nope_not_a_real_insight')).toBeNull()
    expect(getTrafficContent('')).toBeNull()
  })

  test.each(TRAFFIC_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getTrafficContent(id)
    expect(content).not.toBeNull()
    if (!content) return

    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)
    expect(isValidElement(content.icon)).toBe(true)
    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')

    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('traffic arms do not require a diagram and branch new_city_traffic vs new_country_traffic', () => {
    for (const id of TRAFFIC_IDS) {
      expect(getTrafficContent(id)?.diagram).toBeUndefined()
    }
    const city = getTrafficContent('new_city_traffic')!
    const country = getTrafficContent('new_country_traffic')!
    expect(city.title).toMatch(/City/i)
    expect(country.title).toMatch(/Country/i)
    expect(city.title).not.toBe(country.title)
    expect(city.fields).toContain('city')
    expect(country.fields).toContain('country')
    expect(getTrafficContent('city_surges')!.fields).toEqual(['city'])
    expect(getTrafficContent('asn_concentration')!.fields).toEqual(['asn'])
  })
})

const REPEATED_CARD: Extract<InsightCardData, { id: 'repeated_patterns' }> = {
  id: 'repeated_patterns',
  title: 'Scripted Traffic Patterns',
  description: 'IPs sending requests on a highly regular cadence',
  severity: 'critical',
  summary: '1 anomalies detected',
  items: [
    {
      label: '203.0.113.x',
      current_val: 60,
      baseline_val: 1.2,
      baseline_label: 'jitter (σ)',
      unit: 's interval',
      severity: 'critical',
      meta: {
        score: 92,
        cv: 0.04,
        modal_frac: 0.94,
        mean_interval_s: 60,
        stddev_s: 1.2,
        mode_gap_s: 60,
        n_gaps: 1439,
        n_events: 1440,
        span_s: 86400,
        rps: 0.0167,
        distinct_ua: 3,
        filters: { ip: '203.0.113.x' },
      },
    },
  ],
}

describe('InsightItemRow — Scripted Traffic trigger', () => {
  beforeEach(() => {
    realInsightSubcomponentsRef.value = true
    useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
  })

  it('shows the evidence trigger for repeated_patterns and maps meta on click', async () => {
    const user = userEvent.setup()
    const onScriptedTrafficClick = vi.fn()
    render(
      <InsightItemRow
        insight={REPEATED_CARD}
        index={0}
        onScriptedTrafficClick={onScriptedTrafficClick}
      />,
    )
    const btn = screen.getByRole('button', { name: /why 203\.0\.113\.x was flagged/i })
    await user.click(btn)
    expect(onScriptedTrafficClick).toHaveBeenCalledTimes(1)
    const payload = onScriptedTrafficClick.mock.calls[0][0] as ScriptedTrafficData
    expect(payload.label).toBe('203.0.113.x')
    expect(payload.score).toBe(92)
    expect(payload.modal_frac).toBe(0.94)
    expect(payload.mode_gap_s).toBe(60)
  })

  it('does NOT show the trigger for a different insight id', () => {
    render(
      <InsightItemRow
        insight={{
          id: 'error_spikes',
          title: 'Error Spikes',
          description: '',
          severity: 'critical',
          summary: '1 anomalies detected',
          items: [
            {
              label: '203.0.113.x',
              baseline_label: 'baseline',
              severity: 'critical',
              meta: { requests: 1, errors: 1, filters: {} },
            },
          ],
        }}
        index={0}
        onScriptedTrafficClick={vi.fn()}
      />,
    )
    expect(screen.queryByRole('button', { name: /was flagged/i })).toBeNull()
  })
})

describe('ScriptedTrafficModal', () => {
  beforeEach(() => {
    realInsightSubcomponentsRef.value = true
  })

  const DATA: ScriptedTrafficData = {
    label: '203.0.113.x',
    score: 92,
    cv: 0.04,
    modal_frac: 0.94,
    mean_interval_s: 60,
    stddev_s: 1.2,
    mode_gap_s: 60,
    n_gaps: 1439,
    n_events: 1440,
    span_s: 86400,
    rps: 0.0167,
    distinct_ua: 3,
  }

  it('renders nothing when data is null', () => {
    const { container } = render(
      <ScriptedTrafficModal isOpen onOpenChange={() => {}} data={null} />,
    )
    expect(container.querySelector('[role="dialog"]')).toBeNull()
  })

  it('surfaces the evidence when open', () => {
    render(<ScriptedTrafficModal isOpen onOpenChange={() => {}} data={DATA} />)
    expect(screen.getByText(/Why we flagged this/i)).toBeTruthy()
    expect(screen.getAllByText(/92\/100/).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/94%/).length).toBeGreaterThan(0)
    expect(screen.getByText(/CV 0\.04/)).toBeTruthy()
    expect(screen.getAllByText(/\b1d\b/).length).toBeGreaterThan(0)
  })
})

describe('CacheCollapseModal recent misses', () => {
  beforeEach(() => {
    realInsightSubcomponentsRef.value = true
    useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
  })

  it('lists recent misses with cache and status badges', async () => {
    server.use(
      http.post(`${getApiBase()}/api/insights/cache-collapse-detail`, () =>
        HttpResponse.json({
          timeline: [],
          recent_misses: [
            { timestamp: '2026-10-01T10:00:00Z', cache: 'MISS', pop: 'SJC', ip: '203.0.113.9', status: 200 },
            { timestamp: '2026-10-01T10:01:00Z', cache: 'PASS', pop: null, ip: null, status: 503 },
          ],
          baseline_hit_rate: 0.9,
          window_hit_rate: 0.1,
          baseline_pass_rate: 0,
          window_pass_rate: 0,
        }),
      ),
    )
    renderWithQuery(
      <CacheCollapseModal isOpen onOpenChange={() => {}} url="/hot" windowHours="1" baselineHours="24" />,
    )
    expect(await screen.findByText('SJC')).toBeTruthy()
    expect(screen.getByRole('columnheader', { name: /^Timestamp \(/ })).toBeTruthy()
    for (const name of ['Cache Status', 'POP', 'Client IP', 'HTTP Status']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('MISS')).toBeTruthy()
    expect(screen.getByText('PASS')).toBeTruthy()
    expect(screen.getByText('503')).toBeTruthy()
    expect(screen.getByText('203.0.113.9')).toBeTruthy()
  })
})
