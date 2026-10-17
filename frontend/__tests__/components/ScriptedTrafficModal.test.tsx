/**
 * @vitest-environment jsdom
 *
 * Covers the "Why we flagged this" evidence affordance for the
 * Scripted Traffic Patterns (repeated_patterns) insight:
 *   - InsightItemRow renders the trigger ONLY for repeated_patterns and
 *     hands the callback a ScriptedTrafficData object mapped from item.meta.
 *   - ScriptedTrafficModal surfaces the regularity evidence.
 */
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import React from 'react'
import { InsightItemRow } from '@/components/Insights/InsightItemRow'
import { ScriptedTrafficModal } from '@/components/Insights/ScriptedTrafficModal'
import type { ScriptedTrafficData } from '@/components/Insights/types'
import type { InsightCardData } from '@/types/api'

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
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
    // IP in the title
    expect(screen.getByText(/Why we flagged this/i)).toBeTruthy()
    // Regularity score (appears in verdict + stat card)
    expect(screen.getAllByText(/92\/100/).length).toBeGreaterThan(0)
    // Modal dominance rendered as a percentage
    expect(screen.getAllByText(/94%/).length).toBeGreaterThan(0)
    // Coefficient of variation in the breakdown
    expect(screen.getByText(/CV 0\.04/)).toBeTruthy()
    // Humanized observation span (86400s → 1d)
    expect(screen.getAllByText(/\b1d\b/).length).toBeGreaterThan(0)
  })
})
