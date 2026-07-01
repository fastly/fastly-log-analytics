/**
 * R-3a: smoke test — admin dashboard loads end-to-end.
 *
 * The spawned backend (e2e/global-setup.ts) seeds one mock service in
 * its sandbox configs dir, so /api/bootstrap surfaces an active service
 * and the dashboard renders without a "no service selected" prompt.
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

test('admin dashboard loads end-to-end', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
})

test('admin dashboard has no detectable WCAG 2.1 AA violations on first paint', async ({ page }) => {
  // a11y baseline for the most-trafficked admin route. Runs against
  // the real DOM rendered by Next + the live API surface, catching
  // regressions that the per-component vitest-axe suite can't see
  // (route-level focus management, contrast under the real theme,
  // landmark structure, etc.).
  //
  // Scoped to WCAG 2.1 AA — the same target other shipped pages
  // (admin/share, analytics) hold themselves to. Tightening to AAA
  // would surface aesthetic-vs-accessible trade-offs we'd want to
  // discuss before failing CI on.
  //
  // Excludes:
  //   - .recharts-* SVG (third-party — patches go upstream, not here)
  //   - [data-testid='plotly-host'] (Plotly canvas is opaque to axe)
  //   - [data-empty-placeholder='true'] (chart-card loading / no-data
  //     skeletons + the v2.0.0 sidebar footer). These are decorative,
  //     intentionally low-emphasis placeholders that get replaced by
  //     real content within seconds — WCAG SC 1.4.3 exempts incidental
  //     text. Search for `data-empty-placeholder` to see the marked
  //     components (TrafficChart, GeoMap, CardGrid, TopTenTable,
  //     AppLayout footer + analyst watermark).
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // Let hydration complete before axe samples the DOM. Without a settle,
  // axe races React 19's client takeover and sees the SSR'd attributes
  // (Base UI's popover-trigger without its hydrated aria-label; cascaded
  // text-foreground colors not yet applied) — false positives the
  // production app never shows users.
  //
  // NOT networkidle: AppLayout holds open the multiplexed admin SSE stream
  // (/api/admin/events/stream — sync-status + cron-runs + system-metrics on
  // every admin page), so the connection is never idle and
  // waitForLoadState('networkidle') just burns its full 30s timeout and
  // fails. Wait for `main` + a fixed window instead — the same proven
  // pattern as a11y-admin-routes.spec.ts.
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .exclude('.recharts-wrapper')
    .exclude('[data-testid="plotly-host"]')
    .exclude('[data-empty-placeholder="true"]')
    .analyze()

  expect(
    results.violations,
    `axe found ${results.violations.length} WCAG 2.1 AA violation(s):\n` +
      results.violations
        .map(
          (v) =>
            `  - [${v.id}] ${v.help} (${v.impact ?? 'unknown'})\n    ${v.helpUrl}\n` +
            `    nodes: ${v.nodes.length} (first: ${v.nodes[0]?.target?.join(' > ')})`,
        )
        .join('\n'),
  ).toEqual([])
})
