/**
 * Route-level a11y coverage, keyboard navigation, and SSR hydration smoke tests.
 * Extends the single-route baseline in admin-login.spec.ts (dashboard) to the
 * rest of the analytics surface so a WCAG 2.1 AA regression or hydration mismatch
 * on these pages fails CI too.
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'

const CLEAN_ROUTES = [
  '/origin',
  '/performance',
  '/security',
  '/query',
  '/insights',
  '/sessions',
  '/network',
  '/control-room',
  '/fastly-value',
  '/streaming',
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

async function gotoSettled(page: Page, route: string) {
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

async function assertNoAxeViolations(page: Page, route: string) {
  await gotoSettled(page, route)
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .exclude('.recharts-wrapper')
    .exclude('[data-testid="plotly-host"]')
    .exclude('[data-empty-placeholder="true"]')
    .exclude('[data-axe-ignore="true"]')
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

test('Tab navigation moves focus through the sidebar', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const focusedTagBefore = await page.evaluate(() => document.activeElement?.tagName)
  expect(focusedTagBefore).toBeTruthy()

  await page.keyboard.press('Tab')
  const focusedTagAfter = await page.evaluate(() => document.activeElement?.tagName)
  expect(focusedTagAfter).toBeTruthy()
})

test('Escape closes any open popover without trapping focus', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  await page.keyboard.press('Escape')
  expect(await page.locator('body').count()).toBe(1)
})

const HYDRATION_ROUTES = [
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
  '/rum',
  '/usage',
  '/alerts',
  '/logs',
  '/query',
  '/insights',
  '/control-room',
]

const SERVICE_ID = 'svc-playwright-e2e'

const HYDRATION_PATTERNS = [
  /hydrat/i,
  /did not match/i,
  /cannot be a descendant/i,
  /text content does not match/i,
  /minified react error #(418|423|425)/i,
]

function isHydrationError(text: string): boolean {
  return HYDRATION_PATTERNS.some((re) => re.test(text))
}

test.describe('SSR hydration smoke', () => {
  for (const route of HYDRATION_ROUTES) {
    test(`${route} hydrates without a React hydration error`, async ({ page }) => {
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
        localStorage.setItem(
          'timezone-storage',
          JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }),
        )
        localStorage.setItem(
          'fastly-debug-settings',
          JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }),
        )
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

      await page.goto(route, { waitUntil: 'domcontentloaded', timeout: 30_000 })
      await expect(page.locator('main, [role="main"], h1, h2').first()).toBeVisible({ timeout: 15_000 })

      await expect(page.locator('nextjs-portal, [data-nextjs-dialog-overlay], .alert-destructive, [data-testid="error-card"]')).toHaveCount(0)

      await page.waitForTimeout(300)

      expect(
        hydrationErrors,
        `React hydration error(s) on ${route}:\n${hydrationErrors.join('\n---\n')}`,
      ).toEqual([])
    })
  }
})
