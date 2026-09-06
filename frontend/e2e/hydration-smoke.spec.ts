/**
 * SSR hydration smoke test — the guard that would have caught the /admin
 * React #418 storm.
 *
 * The root layout is `export const dynamic = 'force-dynamic'`, so every
 * route is server-rendered and then hydrated on the client. A hydration
 * mismatch (server HTML != client's first render) throws React #418, blows
 * away the SSR tree, and re-renders everything client-side. Two classes hit
 * /admin at once:
 *   1. persisted Zustand stores rehydrating from localStorage during the
 *      first client render (server has no localStorage → divergent nav
 *      hrefs + "Active" badge).
 *   2. a <button> nested inside the DataTable's own sort <button> (the
 *      browser restructures invalid nesting on parse → DOM != React tree).
 *
 * Unit tests (jsdom, client-only render) structurally cannot see these —
 * they never do the server-render-then-hydrate cycle. This spec does: it
 * loads each SSR route in a real browser, with a service ALREADY persisted
 * in localStorage (the exact precondition that exposed #418), and fails if
 * React logs any hydration error.
 *
 * Scope note: we assert only on *hydration* console errors, not all console
 * errors — the mock backend legitimately 4xx/5xx's some endpoints, and
 * failing on those would make this flaky. The patterns below are React's
 * hydration-mismatch signatures (dev prints the full text; a prod build
 * would emit "Minified React error #418/#423/#425").
 */
import { expect, test } from '@playwright/test'

// Every route that renders under the SSR'd admin shell. The e2e session is
// classified admin (loopback Host, no Caddy marker — see lib/ssr/_transport),
// so all of these are reachable.
const ROUTES = [
  '/admin',
  '/admin/share',
  '/admin/session-scoring',
  '/admin/trends',
  '/admin/usage-log',
  '/admin/queries',
  '/dashboard',
  '/sessions',
  '/security',
  '/network',
  '/origin',
  '/performance',
  '/usage',
  '/alerts',
  '/logs',
  '/query',
  '/insights',
  '/control-room',
]

// Seeded by e2e/global-setup.ts (_seedDefaultServiceConfig).
const SERVICE_ID = 'svc-playwright-e2e'

// React hydration-mismatch signatures.
const HYDRATION_PATTERNS = [
  /hydrat/i, // "Hydration failed", "...hydrated but some attributes...", Next's react-hydration-error doc link
  /did not match/i,
  /cannot be a descendant/i,
  /text content does not match/i,
  /minified react error #(418|423|425)/i,
]

function isHydrationError(text: string): boolean {
  return HYDRATION_PATTERNS.some((re) => re.test(text))
}

test.describe('SSR hydration smoke', () => {
  for (const route of ROUTES) {
    test(`${route} hydrates without a React hydration error`, async ({ page }) => {
      // Seed the persisted Zustand stores BEFORE any page script runs, so
      // the page hydrates against a NON-default client state — the precise
      // condition that diverged from the server's empty-localStorage render
      // and threw #418. addInitScript runs pre-hydration on every navigation.
      await page.addInitScript((sid) => {
        localStorage.setItem(
          'service-storage',
          JSON.stringify({
            state: {
              activeServiceId: sid,
              services: [{ id: sid, name: 'Playwright E2E Service', accessLevel: 'read_write' }],
            },
            version: 0,
          }),
        )
        // Non-UTC zone exercises the timezoneStore SSR fix (server default
        // is 'UTC'; this would diverge on any SSR-rendered absolute time).
        localStorage.setItem(
          'timezone-storage',
          JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }),
        )
        // debug=true exercises the debugStore SSR fix.
        localStorage.setItem(
          'fastly-debug-settings',
          JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }),
        )
        // Hand-rolled localStorage-backed UI state (NOT zustand-persist):
        // a non-default chart-visibility set (/charts) and a collapsed
        // dashboard section. Reading these in a useState initializer diverged
        // the first client render from the server's default render → #418.
        localStorage.setItem('fastly_charts_card_visibility', JSON.stringify(['__e2e_only__']))
        localStorage.setItem('dashboard_collapsed_sections', JSON.stringify(['request', 'cache']))
      }, SERVICE_ID)

      const hydrationErrors: string[] = []
      page.on('console', (msg) => {
        if (msg.type() === 'error' && isHydrationError(msg.text())) {
          hydrationErrors.push(msg.text())
        }
      })
      page.on('pageerror', (err) => {
        const text = `${err.message}\n${err.stack ?? ''}`
        if (isHydrationError(text)) hydrationErrors.push(text)
      })

      await page.goto(route, { waitUntil: 'load', timeout: 30_000 })
      // Hydration runs right after the JS executes; the original #418 also
      // surfaced when the first react-query setData re-rendered the still-
      // hydrating tree. networkidle never settles here (SSE + polling), so
      // wait a fixed window long enough to cover hydration + first refetch.
      const settleTimeout = process.env.CI ? 6_000 : 2_500
      await page.waitForTimeout(settleTimeout)

      expect(
        hydrationErrors,
        `React hydration error(s) on ${route}:\n${hydrationErrors.join('\n---\n')}`,
      ).toEqual([])
    })
  }
})
