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

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

afterEach(() => cleanup())

const REPEATED_ITEM = {
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
  },
}

describe('InsightItemRow — Scripted Traffic trigger', () => {
  it('shows the evidence trigger for repeated_patterns and maps meta on click', async () => {
    const user = userEvent.setup()
    const onScriptedTrafficClick = vi.fn()
    render(
      <InsightItemRow
        item={REPEATED_ITEM as never}
        insightId="repeated_patterns"
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
        item={REPEATED_ITEM as never}
        insightId="error_spikes"
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
