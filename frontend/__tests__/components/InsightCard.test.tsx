/**
 * @vitest-environment jsdom
 *
 * Tests for InsightCard — the FE's per-insight card on the
 * /insights page. Bug history in this component:
 *   - Duplicate React keys when multiple insights returned ``id="insight"``
 *     (fixed in backend; this test pins the FE side: distinct ids render
 *     distinct cards).
 *   - "Nothing loads on insights page" — info-severity placeholder
 *     rendering depends on correct severity → icon → badge mapping.
 *
 * Mocks: the modal children (Help, Data, ImpossibleDistance) are stubbed
 * because their internals (maplibre, fetch hooks) aren't relevant here.
 */
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { InsightCard } from '@/components/Insights/InsightCard'

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

// Project's vitest setup doesn't auto-cleanup between tests, so do it manually.
afterEach(() => cleanup())

// Stub the modal children — their internals (maplibre, query hooks) aren't
// relevant to InsightCard rendering tests.
vi.mock('@/components/Insights/InsightHelpModal', () => ({
  InsightHelpModal: ({ isOpen }: { isOpen: boolean }) =>
    isOpen ? <div data-testid="help-modal-open" /> : null,
}))
vi.mock('@/components/Insights/InsightDataModal', () => ({
  InsightDataModal: ({ isOpen }: { isOpen: boolean }) =>
    isOpen ? <div data-testid="data-modal-open" /> : null,
}))
vi.mock('@/components/Insights/ImpossibleDistanceModal', () => ({
  ImpossibleDistanceModal: ({ isOpen }: { isOpen: boolean }) =>
    isOpen ? <div data-testid="distance-modal-open" /> : null,
}))
vi.mock('@/components/Insights/InsightItemRow', () => ({
  InsightItemRow: ({ item }: { item: { label?: string } }) => (
    <div data-testid="insight-item-row">{item.label ?? 'no-label'}</div>
  ),
}))

const _BASE_INSIGHT = {
  id: 'error_spikes',
  title: 'Error Spikes',
  description: 'URLs with abnormally elevated 5xx error rates',
  severity: 'critical' as const,
  summary: '3 URLs with elevated server error rates',
  items: [
    { label: '/api/users', current_val: 0.45, severity: 'critical' },
    { label: '/api/orders', current_val: 0.35, severity: 'warning' },
    { label: '/api/auth', current_val: 0.25, severity: 'warning' },
  ],
}

describe('InsightCard', () => {
  it('renders title, summary, and severity badge', () => {
    render(<InsightCard insight={_BASE_INSIGHT as never} />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('3 URLs with elevated server error rates')).toBeTruthy()
    // Severity badge surfaces as uppercase text
    expect(screen.getByText(/^critical$/i)).toBeTruthy()
  })

  it('renders one row per item up to 5', () => {
    render(<InsightCard insight={_BASE_INSIGHT as never} />)
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(3)
  })

  it('shows "Show N more" button when items > 5', () => {
    const many = {
      ..._BASE_INSIGHT,
      items: Array.from({ length: 8 }, (_, i) => ({
        label: `/path/${i}`,
        current_val: 0.1,
        severity: 'warning',
      })),
    }
    render(<InsightCard insight={many as never} />)
    // First 5 visible
    expect(screen.getAllByTestId('insight-item-row')).toHaveLength(5)
    // "Show 3 more" button
    expect(screen.getByText(/Show 3 more/i)).toBeTruthy()
  })

  it('does not show "Show more" when items <= 5', () => {
    render(<InsightCard insight={_BASE_INSIGHT as never} />)
    expect(screen.queryByText(/Show \d+ more/i)).toBeNull()
  })

  it('renders the help modal when the help button is clicked', async () => {
    const user = userEvent.setup()
    render(<InsightCard insight={_BASE_INSIGHT as never} />)
    expect(screen.queryByTestId('help-modal-open')).toBeNull()
    await user.click(screen.getByTitle('How this works'))
    expect(screen.getByTestId('help-modal-open')).toBeTruthy()
  })

  it('renders the data modal when "Show N more" is clicked', async () => {
    const user = userEvent.setup()
    const many = {
      ..._BASE_INSIGHT,
      items: Array.from({ length: 8 }, (_, i) => ({
        label: `/path/${i}`,
        current_val: 0.1,
        severity: 'warning',
      })),
    }
    render(<InsightCard insight={many as never} />)
    expect(screen.queryByTestId('data-modal-open')).toBeNull()
    // The "Show 3 more" button is a real <button>; role lookup is
    // sturdier than text matching against the localized label.
    await user.click(screen.getByRole('button', { name: /show 3 more/i }))
    expect(screen.getByTestId('data-modal-open')).toBeTruthy()
  })

  it('falls back gracefully on unknown severity (no crash, default icon)', () => {
    // Defensive: if backend ever introduces a new severity the FE doesn't
    // know about, the card must still render rather than blank.
    const weird = { ..._BASE_INSIGHT, severity: 'urgent' as never, items: [] }
    render(<InsightCard insight={weird} />)
    expect(screen.getByText('Error Spikes')).toBeTruthy()
  })

  it('renders empty-items state without crashing', () => {
    // The "clean" / "info" severity case — summary text, no item rows.
    const clean = {
      ..._BASE_INSIGHT,
      severity: 'clean' as const,
      summary: 'No error spikes detected',
      items: [],
    }
    render(<InsightCard insight={clean} />)
    expect(screen.getByText('No error spikes detected')).toBeTruthy()
    expect(screen.queryAllByTestId('insight-item-row')).toHaveLength(0)
  })

  // TESTING_PLAN_3 item 19. Catch a11y regressions that contrast-checks
  // and component-library upgrades can introduce silently. Limited to
  // rule sets that work under jsdom (color-contrast needs a real layout
  // engine, so disable it; we still cover label/role/aria coverage).
  it('has no axe-detectable a11y violations', async () => {
    const { container } = render(<InsightCard insight={_BASE_INSIGHT as never} />)
    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    })
    expect(results).toHaveNoViolations()
  })

  it('multiple InsightCards with distinct ids render distinct elements', () => {
    // Regression guard for the duplicate-React-key bug (backend used to
    // emit ``id="insight"`` for every failed insight). On the FE side,
    // distinct ids should produce distinct rendered cards.
    const a = { ..._BASE_INSIGHT, id: 'error_spikes', title: 'Error Spikes' }
    const b = { ..._BASE_INSIGHT, id: 'botnet_grouping', title: 'Botnet Grouping' }
    render(
      <>
        <InsightCard insight={a as never} />
        <InsightCard insight={b as never} />
      </>,
    )
    expect(screen.getByText('Error Spikes')).toBeTruthy()
    expect(screen.getByText('Botnet Grouping')).toBeTruthy()
  })
})
