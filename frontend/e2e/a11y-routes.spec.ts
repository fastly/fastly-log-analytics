/**
 * Route-level a11y coverage. Extends the single-route baseline in
 * admin-login.spec.ts (dashboard) to the rest of the analytics surface
 * so a WCAG 2.1 AA regression on these pages fails CI too.
 *
 * Conventions match the dashboard baseline:
 *   - wait for `main` + a fixed settle so React 19 hydration finishes
 *     before axe samples the DOM (avoids SSR-vs-hydrated false
 *     positives). NB: `waitForLoadState('networkidle')` is unusable
 *     here — the AppLayout holds open SSE streams (log-extents /
 *     sync-status / cron-runs) plus a 3s log-extents poll, so the
 *     network never goes idle and the wait would always time out.
 *   - exclude third-party / canvas / decorative-placeholder regions
 *     axe can't meaningfully evaluate.
 *
 * The mocked backend (e2e/global-setup.ts) seeds one service so each
 * route renders real content.
 *
 * CLEAN_ROUTES below were verified to pass on 2026-06-18 — every analytics
 * route is now gated; there are no remaining test.fixme a11y routes.
 *
 * /network is gated. It previously looked "non-deterministically flaky" —
 * intermittently rendering the segment error boundary whose own markup (an
 * <h2> contrast, a scrollable <pre>) tripped axe. The root cause was NOT the
 * harness: <ShieldingMap> violated the Rules of Hooks (three a11y useMemo
 * hooks sat below its early returns). On the cold-mount loading→empty
 * transition it rendered fewer hooks on the second pass, so React threw
 * "Rendered fewer hooks than expected" and crashed the page body to the
 * error boundary. That is a real production crash for any service with no
 * shielding data (the common case); hoisting the hooks above the returns
 * fixed it, and /network now renders deterministically.
 * Map a11y fixes that also shipped: (1) interactive MapLibre maps are an
 * accepted exclusion (`.maplibregl-map`) since their canvas+controls can't
 * satisfy aria-hidden-focus while staying mouse-interactive (data is in the
 * adjacent sr-only tables); (2) the map wrapper changed role="img" →
 * role="group" so it no longer claims to be an image while holding the
 * interactive controls (was: nested-interactive).
 *
 * /charts is gated now too. It had been parked as test.fixme for an "e2e
 * navigation flake (net::ERR_ABORTED / frame detached)". Re-investigated
 * 2026-06-18: the page renders correctly and axe-clean (0 violations) on
 * chromium, firefox AND webkit, across cold (.next-e2e wiped) and warm
 * loads. The abort was a transient harness-only race, NOT an a11y bug: this
 * is the heaviest route (it pulls the ~1.4MB plotly.js-cartesian chunk for
 * its pie-chart grid), so its cold compile widens the window in which a
 * same-document URL stamp or a next-dev reload can land mid-navigation. (The
 * ?service= stamp itself is benign — useUrlServiceSync writes it via nuqs
 * `shallow: true` → history.replaceState, having replaced the old
 * router.replace dance that *would* have aborted the in-flight goto.)
 * gotoSettled() below retries that one transient class once so the gate is
 * deterministic; a genuinely broken route still fails both attempts.
 *
 * Previously-failing routes now FIXED and gated: /insights (dropped an
 * opacity-50 on a muted label), /sessions (UpdatingBadge text moved from
 * text-primary to text-foreground for AA contrast), and /network (the
 * ShieldingMap Rules-of-Hooks crash described above).
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

const CLEAN_ROUTES = ['/origin', '/performance', '/security', '/query', '/insights', '/sessions', '/network', '/charts', '/control-room']

// `next dev` compiles each route on first request. On the heaviest route
// (/charts fetches the ~1.4MB plotly.js-cartesian chunk) that cold compile
// is long enough that a transient navigation abort — net::ERR_ABORTED or a
// "frame was detached" — can surface when a same-document URL stamp or a
// dev-server reload lands mid-goto. The route renders fine (verified
// axe-clean on chromium/firefox/webkit, cold and warm), so retrying that
// one transient class once makes the gate deterministic without masking a
// real failure: a genuinely broken route throws on BOTH attempts and still
// fails. Kept route-agnostic so every gated route gets the same cold-start
// resilience in CI, not just /charts.
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
  // Fixed settle for hydration + first data paint (see header note on
  // why networkidle is unusable on these SSE-bearing routes).
  await page.waitForTimeout(3_000)

  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .exclude('.recharts-wrapper')
    .exclude('[data-testid="plotly-host"]')
    .exclude('[data-empty-placeholder="true"]')
    // Interactive MapLibre maps are a documented known-limitation: their
    // container is aria-hidden (data is exposed via the adjacent accessible
    // tables), but MapLibre injects a focusable canvas + zoom controls that
    // axe flags as aria-hidden-focus. tabindex=-1 doesn't satisfy the rule,
    // and the map must stay mouse-interactive so `inert` isn't viable — so
    // the map region is excluded, same as the Plotly/recharts canvases above.
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
