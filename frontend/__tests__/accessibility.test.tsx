/**
 * Accessibility smoke tests using axe-core.
 *
 * Scope: lightweight, focused components rendered in isolation. Heavier
 * tests against full page renders (admin/dashboard/etc.) would need
 * mocks for Plotly, MapLibre, CodeMirror, etc. — the resulting axe
 * report would mostly reflect the test harness, not real UX. We keep
 * those out and instead exercise the building blocks the pages compose.
 *
 * Adding a new component to the smoke set:
 *   1. Render it inside a `<div>` so axe has a document root.
 *   2. Add an `await axe(container)` assertion.
 *   3. If a real violation is found, FIX it (don't suppress) — these
 *      should stay zero-violation as the suite grows.
 */
import { render } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { AnalyticsCard } from '@/components/AnalyticsCard'

describe('a11y: AnalyticsCard', () => {
  it('has no detectable WCAG violations in its happy-path render', async () => {
    const { container } = render(
      <AnalyticsCard title="Total requests" helpContent={<p>How we count.</p>}>
        <div>
          <p>1,234,567</p>
          <p>over the last hour</p>
        </div>
      </AnalyticsCard>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  it('has no violations while showing the loading overlay', async () => {
    const { container } = render(
      <AnalyticsCard title="Slow Card" isLoading isFetching>
        <p>placeholder</p>
      </AnalyticsCard>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })
})

describe('a11y: basic form patterns', () => {
  it('a labelled input has no violations', async () => {
    const { container } = render(
      <div>
        <label htmlFor="search">Search</label>
        <input id="search" type="search" placeholder="Type to filter…" />
      </div>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

})
