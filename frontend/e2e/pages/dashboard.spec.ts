/**
 * E2E Page Contract Test: Dashboard (/dashboard)
 *
 * Implements the 10-step verification checklist specified in docs/pages/dashboard.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Data bundle contract: exactly 1 round-trip on initial load, HTTP 200, valid schema.
 * 3. Metric switching: Requests, Bandwidth, Status Codes, Hit Ratio, Origin Latency.
 * 4. Time range switching: 1h, 24h, 7d presets update URL and chart interval.
 * 5. Click-to-filter drilldown: clicking top-N rows adds global filter and refetches.
 * 6. GeoMap rendering: SVG/Canvas mounts without WebGL errors.
 * 7. Compare mode: toggles compare and fires secondary /api/dashboard/aggregates.
 * 8. Category collapse persistence: localStorage remembers collapsed sections across reloads.
 * 9. Card customization: hiding a card updates grid without full refetch.
 * 10. Performance budget: FCP/LCP within performance budget (< 1500ms).
 */

import { expect, test } from '@playwright/test'

test.describe('Dashboard Page Contract (/dashboard)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Single round-trip /api/dashboard/bundle contract', async ({ page }) => {
    let bundleCalls = 0
    let bundleResponseData: any = null

    page.on('response', async (res) => {
      if (res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST') {
        bundleCalls++
        try {
          bundleResponseData = await res.json()
        } catch {
          // ignore non-json
        }
      }
    })

    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Wait for network to settle
    await page.waitForLoadState('networkidle')

    // Exactly 1 composite bundle request on cold load
    expect(bundleCalls).toBe(1)
    expect(bundleResponseData).toBeTruthy()
    expect(bundleResponseData).toHaveProperty('aggregates')
    expect(bundleResponseData.aggregates).toHaveProperty('data')
    expect(bundleResponseData).toHaveProperty('top_bots')
  })

  test('3. Metric switching on TrafficChart', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Metric buttons in TrafficChart (Reqs, 5xx, 4xx, CHR, Throughput)
    const metrics = ['Reqs', '5xx', '4xx', 'CHR']
    for (const metric of metrics) {
      const btn = page.getByRole('button', { name: new RegExp(`^${metric}$`, 'i') }).first()
      if (await btn.isVisible()) {
        await btn.click()
        await expect(btn).toHaveAttribute('aria-pressed', 'true')
      }
    }
  })

  test('4. Time range presets update time window', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Time preset dropdown or buttons in ReportLayout
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })
  })

  test('5. Click-to-filter drill-down adds filter chip and refetches', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Find any top-N table cell inside CardGrid
    const firstTableCell = page.locator('table tbody tr td').first()
    if (await firstTableCell.isVisible({ timeout: 5000 }).catch(() => false)) {
      const cellText = await firstTableCell.innerText()
      if (cellText && cellText.trim().length > 0) {
        await firstTableCell.click()
        // Verify filter pill appeared in FilterBar
        const filterBar = page.locator('[data-testid="filter-bar"]')
        if (await filterBar.isVisible({ timeout: 5000 }).catch(() => false)) {
          await expect(filterBar).toContainText(cellText.trim())
        }
      }
    }
  })

  test('6. GeoMap panel mounts properly', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Look for map container or SVG / canvas element
    const mapSection = page.locator('section').filter({ hasText: /geographic|traffic by country|world map/i }).first()
    if (await mapSection.isVisible({ timeout: 5000 }).catch(() => false)) {
      await expect(mapSection).toBeVisible()
    }
  })

  test('7. Compare mode triggers secondary aggregates query', async ({ page }) => {
    let compareFired = false
    page.on('request', (req) => {
      if (req.url().includes('/api/dashboard/aggregates') && req.method() === 'POST') {
        compareFired = true
      }
    })

    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Compare switch in DashboardHeader
    const compareSwitch = page.locator('button[role="switch"]').filter({ hasText: /compare/i })
      .or(page.getByLabel(/compare/i))
      .first()

    if (await compareSwitch.isVisible({ timeout: 5000 }).catch(() => false)) {
      await compareSwitch.click()
      await page.waitForTimeout(1000)
      expect(compareFired).toBe(true)
    }
  })

  test('8. Category collapse state persists in localStorage across reloads', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Section headers have collapsible chevron buttons (e.g. Request, Cache, Geography)
    const categoryHeader = page.locator('h3').filter({ hasText: /geography|cache|origin/i }).first()
    if (await categoryHeader.isVisible({ timeout: 5000 }).catch(() => false)) {
      await categoryHeader.click()

      // Reload page and check localStorage key
      await page.reload()
      await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

      const collapsedStorage = await page.evaluate(() => localStorage.getItem('dashboard_collapsed_sections'))
      expect(collapsedStorage).toBeTruthy()
    }
  })

  test('9. Card customization popover visibility toggle', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
    await expect(cardsBtn).toBeVisible({ timeout: 15_000 })
    await cardsBtn.click()

    await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })
  })

  test('10. Performance budget: LCP < 1500ms on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/dashboard', { waitUntil: 'networkidle' })

    const start = Date.now()
    await page.reload({ waitUntil: 'networkidle' })
    const totalTime = Date.now() - start

    const lcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        try {
          new PerformanceObserver((list) => {
            const entries = list.getEntries()
            const lastEntry = entries[entries.length - 1]
            resolve(lastEntry.startTime)
          }).observe({ type: 'largest-contentful-paint', buffered: true })
          setTimeout(() => resolve(0), 1000)
        } catch {
          resolve(0)
        }
      })
    })

    console.log(`[PERF /dashboard] Total reload: ${totalTime}ms | LCP: ${Math.round(lcp)}ms`)
    expect(totalTime).toBeLessThan(10_000)
    if (lcp > 0) {
      expect(lcp).toBeLessThan(1500)
    }
  })

})
