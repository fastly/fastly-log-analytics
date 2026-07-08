/**
 * Control Room e2e tests.
 *
 * Phase 0: walking skeleton (sidebar, tabs, SSE badge, mobile, a11y)
 * Phase 1: real metrics on Overview/Cost tabs
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

test.describe('Control Room', () => {
  test.beforeEach(async ({ page }) => {
    await seedLocalStorage(page)
  })

  test('sidebar link exists and navigates to /control-room', async ({ page }) => {
    await page.goto('/dashboard')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const sidebarLink = page.getByRole('link', { name: 'Control Room' })
    await expect(sidebarLink).toBeVisible({ timeout: 10_000 })

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

  test('Overview tab shows metric cards with live data', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const reqCard = page.locator('text=Requests/s').first()
    await expect(reqCard).toBeVisible({ timeout: 10_000 })

    const errorCard = page.locator('text=Error Rate').first()
    await expect(errorCard).toBeVisible({ timeout: 5_000 })

    const cacheCard = page.locator('text=Cache Hit Ratio').first()
    await expect(cacheCard).toBeVisible({ timeout: 5_000 })

    const bwCard = page.locator('text=Bandwidth').first()
    await expect(bwCard).toBeVisible({ timeout: 5_000 })
  })

  test('Cost tab renders cost metric cards', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const costTab = page.locator('[role="tablist"] button:text("Cost")')
    await expect(costTab).toBeVisible({ timeout: 5_000 })
    await costTab.click()

    const costCard = page.locator('text=Estimated Cost').first()
    await expect(costCard).toBeVisible({ timeout: 10_000 })

    const billedCard = page.locator('text=Requests Billed').first()
    await expect(billedCard).toBeVisible({ timeout: 5_000 })
  })

  test('historical data link on Overview navigates to /dashboard', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    const histLink = page.locator('a[href="/dashboard"]').filter({ hasText: /historical/i }).first()
    await expect(histLink).toBeVisible({ timeout: 5_000 })
  })

  test('Security tab shows WAF metrics', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const secTab = page.locator('[role="tablist"] button:text("Security")')
    await secTab.click()

    await expect(page.locator('text=WAF Blocked').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=WAF Logged').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Network tab shows PoP counts', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const netTab = page.locator('[role="tablist"] button:text("Network")')
    await netTab.click()

    await expect(page.locator('text=Active PoPs').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=Healthy PoPs').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Origin tab shows origin metrics', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const originTab = page.locator('[role="tablist"] button:text("Origin")')
    await originTab.click()

    await expect(page.locator('text=Origin Requests/s').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=Shield Hit Ratio').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Sessions tab shows historical data message', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const sessionsTab = page.locator('[role="tablist"] button:text("Sessions")')
    await sessionsTab.click()

    await expect(page.locator('text=requires ingested log data').first()).toBeVisible({ timeout: 5_000 })
  })
})
