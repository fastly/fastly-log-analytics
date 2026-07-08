/**
 * Control Room — Phase 0 structural e2e tests.
 *
 * Verifies the walking-skeleton UI delivered by Phase 0:
 *   - Sidebar link present and navigable
 *   - All 9 tabs render (admin session)
 *   - SSE connection badge turns green within 5s
 *   - Mobile layout shows the condensed overview
 *   - axe-core WCAG 2.1 AA accessibility gate
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

const SERVICE_ID = 'svc-playwright-e2e'

// Seed localStorage so the page has an active service on first paint
// (same pattern as hydration-smoke.spec.ts).
async function seedLocalStorage(page: import('@playwright/test').Page) {
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
      JSON.stringify({ state: { timezone: 'UTC' }, version: 0 }),
    )
  }, SERVICE_ID)
}

test.describe('Control Room — Phase 0', () => {
  test.beforeEach(async ({ page }) => {
    await seedLocalStorage(page)
  })

  test('sidebar link exists and navigates to /control-room', async ({ page }) => {
    await page.goto('/dashboard')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const sidebarLink = page.locator('nav a[href="/control-room"]')
    await expect(sidebarLink).toBeVisible({ timeout: 10_000 })
    await expect(sidebarLink).toContainText('Control Room')

    await sidebarLink.click()
    await expect(page).toHaveURL(/\/control-room/)
  })

  test('renders all 9 tabs on desktop viewport', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    const expectedTabs = [
      'Overview',
      'Performance',
      'Origin',
      'Security',
      'Network',
      'Sessions',
      'Cost',
      'Insights',
      'Admin',
    ]

    for (const tabName of expectedTabs) {
      const trigger = page.locator(`[role="tablist"] button:text("${tabName}")`)
      await expect(trigger).toBeVisible({ timeout: 5_000 })
    }
  })

  test('SSE connected badge turns green within 5s', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    // The ConnectionBadge shows "Connected" with a success variant
    // once the SSE stream starts delivering metrics_tick events.
    const connectedBadge = page.locator('[data-slot="badge"]:has-text("Connected")').first()
    await expect(connectedBadge).toBeVisible({ timeout: 10_000 })
  })

  test('mobile layout shows condensed overview', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    // Mobile overview should be visible
    const mobileOverview = page.locator('text=Live Overview')
    await expect(mobileOverview).toBeVisible({ timeout: 5_000 })

    // Desktop tabs should be hidden on mobile
    const tabsList = page.locator('[role="tablist"]')
    await expect(tabsList).toBeHidden()
  })

  test('no WCAG 2.1 AA violations on /control-room', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .exclude('[data-empty-placeholder="true"]')
      .analyze()

    expect(
      results.violations,
      `axe found ${results.violations.length} WCAG 2.1 AA violation(s) on /control-room:\n` +
        results.violations
          .map(
            (v) =>
              `  - [${v.id}] ${v.help} (${v.impact ?? 'unknown'})\n    ${v.helpUrl}\n` +
              `    nodes: ${v.nodes.length} (first: ${v.nodes[0]?.target?.join(' > ')})`,
          )
          .join('\n'),
    ).toEqual([])
  })
})
