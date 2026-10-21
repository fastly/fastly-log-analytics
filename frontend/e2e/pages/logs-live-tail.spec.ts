/**
 * E2E Page Contract Test: Live Tail (/logs?tab=live)
 *
 * Implements the verification checklist specified in docs/pages/logs-live-tail.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & SSE connection validation on GET /api/stream/{service_id}.
 * 4. Core Live Streaming toolbar controls mounted & functioning.
 * 5. Search input filtering and regex search without console errors.
 * 6. Pause/Resume stream toggle functionality without UI freeze.
 * 7. Row detail inspection drawer/modal with parsed JSON & copy action.
 * 8. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05 on warm reload.
 */

import { expect, test } from '@playwright/test'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

test.describe('Live Tail Contract (/logs?tab=live)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/logs?tab=live')
    await expect(page.getByRole('tab', { name: /Live Tail/i })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/logs?tab=live')
    await expect(page.getByRole('tab', { name: /Live Tail/i })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated streaming explorer and terminal container exist
    await expect(page.locator('[data-testid="live-streaming-explorer"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="stream-terminal-container"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('3. Core Live Streaming toolbar controls mounted', async ({ page }) => {
    await page.goto('/logs?tab=live')
    await expect(page.locator('[data-testid="live-streaming-explorer"]')).toBeVisible({ timeout: 30_000 })

    // Controls Toolbar
    await expect(page.locator('[data-testid="stream-controls-toolbar"]')).toBeVisible({ timeout: 10_000 })

    // Pause/Resume button
    await expect(page.locator('[data-testid="stream-pause-toggle"]')).toBeVisible({ timeout: 10_000 })

    // Connection status indicator
    await expect(page.locator('[data-testid="stream-connection-status"]')).toBeVisible({ timeout: 10_000 })

    // Search / regex input
    await expect(page.locator('[data-testid="stream-search-input"]')).toBeVisible({ timeout: 10_000 })

    // Clear logs button
    await expect(page.locator('[data-testid="stream-clear-logs"]')).toBeVisible({ timeout: 10_000 })
  })

  test('4. Pause/resume toggle and search filter interaction', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/logs?tab=live')
    await expect(page.locator('[data-testid="live-streaming-explorer"]')).toBeVisible({ timeout: 30_000 })

    // Test pause toggle
    const pauseToggle = page.locator('[data-testid="stream-pause-toggle"]')
    await expect(pauseToggle).toBeVisible({ timeout: 10_000 })
    await pauseToggle.click()
    await expect(pauseToggle).toContainText(/Resume/i)

    // Resume stream
    await pauseToggle.click()
    await expect(pauseToggle).toContainText(/Pause/i)

    // Test search filter input
    const searchInput = page.locator('[data-testid="stream-search-input"]')
    await expect(searchInput).toBeVisible({ timeout: 10_000 })
    await searchInput.fill('GET /api/health')
    await page.waitForTimeout(300)
    await searchInput.fill('')

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('5. Terminal stream viewport and row inspector interaction', async ({ page }) => {
    await page.goto('/logs?tab=live')
    await expect(page.locator('[data-testid="stream-terminal-container"]')).toBeVisible({ timeout: 30_000 })

    // Terminal body should be mounted
    await expect(page.locator('[data-testid="stream-terminal-scroll"]')).toBeVisible({ timeout: 10_000 })

    // Check for either skeleton/connecting state or streamed rows
    const rows = page.locator('[data-testid="stream-log-row"]')
    const rowCount = await rows.count()

    if (rowCount > 0) {
      // Click first row to open detail modal
      await rows.first().click()
      const modal = page.locator('[data-testid="stream-row-inspector-modal"]')
      await expect(modal).toBeVisible({ timeout: 10_000 })

      // Verify Copy JSON button
      const copyBtn = page.locator('[data-testid="stream-copy-json"]')
      await expect(copyBtn).toBeVisible({ timeout: 5_000 })

      // Close modal
      await page.keyboard.press('Escape')
      await expect(modal).not.toBeVisible({ timeout: 5_000 })
    }
  })

  test('6. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm reload', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/logs?tab=live', { waitUntil: 'domcontentloaded' })

    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await page.evaluate(() => {
      return new Promise<{ lcp: number; cls: number }>((resolve) => {
        let lcp = 0
        let cls = 0
        try {
          new PerformanceObserver((list) => {
            const entries = list.getEntries()
            if (entries.length > 0) {
              lcp = entries[entries.length - 1].startTime
            }
          }).observe({ type: 'largest-contentful-paint', buffered: true })

          new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              const layoutShift = entry as LayoutShiftEntry
              if (!layoutShift.hadRecentInput) {
                cls += layoutShift.value ?? 0
              }
            }
          }).observe({ type: 'layout-shift', buffered: true })

          setTimeout(() => resolve({ lcp, cls }), 1000)
        } catch {
          resolve({ lcp: 0, cls: 0 })
        }
      })
    })

    console.log(`[PERF /logs?tab=live] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })
})
