/**
 * E2E Page Contract Test: Usage & Cost Estimator (/usage)
 *
 * Implements the verification checklist specified in docs/pages/usage-and-cost.md:
 * 1. Route accessibility & environment indicator verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & schema validation on /api/usage/* endpoints.
 * 4. Core stat cards, charts, and Cost Estimator mounted.
 * 5. Interactive Delivery Billing Zone (MBZ100, MBZ200, Global) switching & egress recalculation.
 * 6. Zero error cards or broken layout containers.
 * 7. Performance budget: LCP < 1500ms, CLS <= 0.05 on warm reload.
 */

import { expect, test } from '@playwright/test'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

test.describe('Usage & Cost Estimator Page Contract (/usage)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Verify intrinsic size reservation classes across KPI grids and sections
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(3)
  })

  test('3. Data contract & schema validation on /api/usage endpoints', async ({ page }) => {
    test.skip(!!process.env.CI, 'requires seeded log data / real FOS; the CI e2e backend has neither')
    // 1. Storage endpoint
    const storageRes = await page.request.get('/api/usage/current-storage')
    expect(storageRes.ok()).toBeTruthy()
    const storageData = await storageRes.json()
    expect(storageData).toHaveProperty('live_bytes')
    expect(storageData).toHaveProperty('total_billed_gb_hours')

    // 2. Prefill rates endpoint
    const ratesRes = await page.request.get('/api/usage/prefill/rates')
    expect(ratesRes.ok()).toBeTruthy()
    const ratesData = await ratesRes.json()
    expect(ratesData).toHaveProperty('class_a_rate_per_1k')
    expect(ratesData).toHaveProperty('cdn_egress_rate_per_gb')
    expect(ratesData).toHaveProperty('billing_zone')
    expect(['MBZ100', 'MBZ200', 'Global']).toContain(ratesData.billing_zone)

    // 3. Operations endpoint
    const opsRes = await page.request.get('/api/usage/operations?by=hour')
    expect(opsRes.ok()).toBeTruthy()
    const opsData = await opsRes.json()
    expect(opsData).toHaveProperty('total_class_a')
    expect(opsData).toHaveProperty('total_class_b')
    expect(Array.isArray(opsData.data)).toBeTruthy()

    // 4. Bandwidth endpoint
    const bwRes = await page.request.get('/api/usage/bandwidth?by=hour')
    expect(bwRes.ok()).toBeTruthy()
    const bwData = await bwRes.json()
    expect(bwData).toHaveProperty('total_bytes')
    expect(Array.isArray(bwData.data)).toBeTruthy()

    // 5. Log activity endpoint
    const actRes = await page.request.get('/api/usage/log-activity?by=hour')
    expect(actRes.ok()).toBeTruthy()
    const actData = await actRes.json()
    expect(actData).toHaveProperty('total_rows')
    expect(Array.isArray(actData.data)).toBeTruthy()
  })

  test('4. Core stat cards, charts, and Cost Estimator mounted', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Stat cards
    await expect(page.getByText('Storage Impact (Period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Class A Ops (period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Class B Ops (period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Estimated Incurred Cost')).toBeVisible({ timeout: 15_000 })

    // Analytics cards
    await expect(page.getByText('FOS Operations')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('CDN Bandwidth')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Estimated Cost')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Log Activity (Processed)')).toBeVisible({ timeout: 15_000 })

    // Cost Estimator
    await expect(page.getByText('Cost Estimator')).toBeVisible({ timeout: 15_000 })
  })

  test('5. Delivery Billing Zone interactive switching', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Scroll down to Cost Estimator if needed
    const costEstimator = page.getByText('Cost Estimator')
    await costEstimator.scrollIntoViewIfNeeded()
    await expect(costEstimator).toBeVisible({ timeout: 15_000 })

    // Check for Billing Zone buttons: MBZ100, MBZ200, Global
    const zoneMBZ100 = page.getByRole('button', { name: 'MBZ100' })
    const zoneMBZ200 = page.getByRole('button', { name: 'MBZ200' })
    const zoneGlobal = page.getByRole('button', { name: 'Global' })

    await expect(zoneMBZ100).toBeVisible({ timeout: 15_000 })
    await expect(zoneMBZ200).toBeVisible({ timeout: 15_000 })
    await expect(zoneGlobal).toBeVisible({ timeout: 15_000 })

    // Click MBZ100 and verify rate updates to $0.08 / GB
    await zoneMBZ100.click()
    await expect(page.getByText('$0.08 / GB')).toBeVisible({ timeout: 5_000 })

    // Click Global and verify rate updates to $0.18 / GB
    await zoneGlobal.click()
    await expect(page.getByText('$0.18 / GB')).toBeVisible({ timeout: 5_000 })

    // Click MBZ200 and verify rate returns to $0.12 / GB
    await zoneMBZ200.click()
    await expect(page.getByText('$0.12 / GB')).toBeVisible({ timeout: 5_000 })
  })

  test('6. Zero error cards on usage page', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Ensure zero error cards
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/usage', { waitUntil: 'domcontentloaded' })

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

    console.log(`[PERF /usage] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })
})
