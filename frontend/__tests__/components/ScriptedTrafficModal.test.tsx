/**
 * @vitest-environment jsdom
 *
 * Covers:
 *   1. "Why we flagged this" evidence affordance for the Scripted Traffic Patterns
 *      (repeated_patterns) insight (InsightItemRow + ScriptedTrafficModal).
 *   2. CacheCollapseModal "Recent Cache Misses" list through DataTableReadonly.
 */
import { stubResizeObserver, stubPointerCaptureAndScroll } from '@/__tests__/helpers/browser-stubs'
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeAll, beforeEach, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import React from 'react'
import { InsightItemRow } from '@/components/Insights/InsightItemRow'
import { ScriptedTrafficModal } from '@/components/Insights/ScriptedTrafficModal'
import { CacheCollapseModal } from '@/components/Insights/CacheCollapseModal'
import type { ScriptedTrafficData } from '@/components/Insights/shared'
import type { InsightCardData } from '@/types/api'
import { renderWithQuery } from '../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries" /> }))

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
})

afterEach(() => cleanup())

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
