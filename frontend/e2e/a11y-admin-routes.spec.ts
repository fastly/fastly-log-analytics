/**
 * Net-new a11y gate (UX pre-release finding UX-11): extend the axe coverage from
 * the analytics routes (a11y-routes.spec.ts) + /dashboard (admin-login.spec.ts)
 * to the ELEVEN routes that ship real UI but have ZERO WCAG regression coverage:
 * the whole admin console + the general /alerts, /logs, /usage routes.
 *
 * The Playwright harness classifies 127.0.0.1 as LOOPBACK = ADMIN (see
 * playwright.config.ts + e2e/global-setup.ts), so every route below is reachable
 * in CI today — only the assertion was missing. hydration-smoke.spec.ts already
 * navigates to several of them, but runs no AxeBuilder.
 *
 * Conventions copied verbatim from a11y-routes.spec.ts:
 *   - gotoSettled() retries one transient nav-abort class (cold `next dev`
 *     compile widens the abort window).
 *   - wait for `main` visible + a fixed 3s settle (networkidle is unusable —
 *     AppLayout holds open SSE streams), then sample axe.
 *   - exclude the same third-party / canvas / decorative-placeholder regions.
 *
 * TRIAGED 2026-06-19, then REMEDIATED 2026-06-20 (chromium, live harness). All
 * 9 loopback-reachable ungated routes are now axe-clean and gated below. The
 * four that previously failed (/admin, /admin/session-scoring, /admin/usage-log,
 * /usage) had real WCAG 2.1 AA color-contrast + label violations, fixed by:
 *   - deepening the light-mode `--destructive` token (red-600 → red-700) so
 *     `text-destructive` clears 4.5:1 on its own light tint;
 *   - darkening status colors in light mode only (red/amber/green/yellow/blue
 *     -500/-600 → -600/-700; dark mode keeps the brighter shade);
 *   - dropping the `opacity-70/60` dimming on small muted-foreground labels;
 *   - giving the CostCalculator number inputs an accessible name via Row.
 * Keep them clean — do not silently weaken the shared exclude list to pass.
 *
 * NOT covered here (need a remote-classified fixture, separate follow-up): the
 * analyst entry flow /share-login + /share-login/acknowledge — under loopback
 * they redirect to /dashboard, so they cannot be scanned from this admin harness.
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

// Verified axe-clean — active gates. (/admin/queries additionally has no
// loading.tsx, UX-16 — unrelated to axe but flagged in the same finding.)
const CLEAN_ROUTES = [
  '/admin',
  '/admin/queries',
  '/admin/session-scoring',
  '/admin/share',
  '/admin/trends',
  '/admin/usage-log',
  '/alerts',
  '/logs',
  '/usage',
]

async function gotoSettled(page: import('@playwright/test').Page, route: string) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await page.goto(route)
      return
    } catch (e) {
      const msg = String((e as Error)?.message ?? e)
      const transient = /ERR_ABORTED|frame was detached|frame got detached|navigation .*interrupted/i.test(msg)
      if (attempt === 0 && transient) continue
      throw e
    }
  }
}

async function assertNoAxeViolations(page: import('@playwright/test').Page, route: string) {
  await gotoSettled(page, route)
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .exclude('.recharts-wrapper')
    .exclude('[data-testid="plotly-host"]')
    .exclude('[data-empty-placeholder="true"]')
    .exclude('.maplibregl-map')
    .analyze()

  expect(
    results.violations,
    `axe found ${results.violations.length} WCAG 2.1 AA violation(s) on ${route}:\n` +
      results.violations
        .map(
          (v) =>
            `  - [${v.id}] ${v.help} (${v.impact ?? 'unknown'})\n    ${v.helpUrl}\n` +
            `    nodes: ${v.nodes.length} (first: ${v.nodes[0]?.target?.join(' > ')})`,
        )
        .join('\n'),
  ).toEqual([])
}

for (const route of CLEAN_ROUTES) {
  test(`no WCAG 2.1 AA violations on ${route}`, async ({ page }) => {
    await assertNoAxeViolations(page, route)
  })
}
