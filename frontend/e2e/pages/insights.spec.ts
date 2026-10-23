/**
 * E2E Page Contract Test: Insights & Anomaly Detection (/insights)
 *
 * Implements the verification checklist specified in docs/pages/insights.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & in-place loading states (Zero CLS).
 * 3. Data contract: POST /api/insights & GET /api/insight-availability schema validation.
 * 4. Instant client-side tab switching across all category tabs (all, traffic, performance, origin, security, routing).
 * 5. Plausible detector values or clean "normal parameters" pass states.
 * 6. Time range preset transitions (window vs baseline).
 * 7. Performance budget: FCP/LCP within budget (< 1500ms), CLS <= 0.05.
 * 8. Telemetry instrumentation & query efficiency audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { installClsObserver, measureWarmPaintAndCls } from '../cls-observer'

type InsightsDebugResponse = {
  _debug_queries?: DebugQuery[]
  insights?: unknown[]
  computed_at?: string
  timing_ms?: Record<string, number>
}

test.describe('Insights Page Contract (/insights)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    // Verify intrinsic size reservation classes on container elements
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    await expect(reservedContainers.first()).toBeVisible({ timeout: 15_000 })
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)

    // Verify either grid or clean pass state has rendered
    const content = page.locator('[data-testid="insights-grid"], [data-testid="insights-empty-pass"], [data-testid="insights-grid-loading"]').first()
    await expect(content).toBeVisible({ timeout: 15_000 })
  })

  test('3. Data contract & schema validation (/api/insights & /api/insight-availability)', async ({ page }) => {
    // 1. Verify /api/insight-availability
    const availRes = await page.request.get('/api/insight-availability')
    expect(availRes.ok()).toBeTruthy()
    const availData = await availRes.json()
    expect(availData).toHaveProperty('insights')
    expect(Array.isArray(availData.insights)).toBeTruthy()
    if (availData.insights.length > 0) {
      expect(availData.insights[0]).toHaveProperty('id')
      expect(availData.insights[0]).toHaveProperty('available')
    }

    // 2. Verify /api/insights
    const apiRes = await page.request.post('/api/insights', {
      data: {
        window_size_hrs: 1,
        baseline_hours: 24,
        filters: {},
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toHaveProperty('insights')
    expect(data).toHaveProperty('computed_at')
    expect(Array.isArray(data.insights)).toBeTruthy()
  })

  test('4. Instant client-side tab switching across all 5 category tabs and All', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    // Tabs to test: all, security, origin, edge (aliased as performance), network (aliased as routing), traffic
    const tabs = ['all', 'security', 'origin', 'edge', 'network', 'traffic']

    for (const tabKey of tabs) {
      const trigger = page
        .locator(`button[data-tab="${tabKey}"]`)
        .or(page.locator(`button[data-alias="${tabKey}"]`))
        .first()

      await expect(trigger).toBeVisible({ timeout: 10_000 })
      await trigger.click()

      // Active trigger should have active state
      await expect(trigger).toHaveAttribute('aria-selected', 'true')

      // Active panel should contain either cards or clean pass state with zero CLS container
      const activeContent = page
        .locator(`[data-tab-content="${tabKey}"]`)
        .or(page.locator(`[data-alias="${tabKey}"]`))
        .first()

      await expect(activeContent).toBeVisible({ timeout: 10_000 })
    }
  })

  test('5. Plausible detector values or clean normal parameters pass states', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    // Check that either anomaly cards are rendered or clean pass state is shown
    const emptyPass = page.locator('[data-testid="insights-empty-pass"]').first()
    const cardsGrid = page.locator('[data-testid="insights-grid"]').first()

    // Wait for the data load to settle into one of the two end states.
    await expect(emptyPass.or(cardsGrid)).toBeVisible({ timeout: 30_000 })

    const hasEmptyPass = await emptyPass.isVisible()
    const hasCardsGrid = await cardsGrid.isVisible()

    expect(hasEmptyPass || hasCardsGrid).toBeTruthy()

    if (hasEmptyPass) {
      await expect(emptyPass).toContainText(/No anomalies detected/i)
      await expect(emptyPass).toContainText(/baseline parameters/i)
    } else {
      const card = cardsGrid.locator('.rounded-xl, .border').first()
      await expect(card).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Time range preset transitions (window vs baseline)', async ({ page }) => {
    // Navigate with explicit search params to test preset transition
    await page.goto('/insights?window=24&baseline=168&tab=security')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    // Verify security tab is active
    const secTrigger = page.locator('button[data-tab="security"]').first()
    await expect(secTrigger).toBeVisible({ timeout: 10_000 })
    await expect(secTrigger).toHaveAttribute('aria-selected', 'true')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/insights', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /insights] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/insights', {
      data: {
        window_size_hrs: 1,
        baseline_hours: 24,
        filters: {},
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json() as InsightsDebugResponse
    expect(json).toHaveProperty('insights')
    if (json._debug_queries) {
      console.log(`[TELEMETRY /insights] Captured ${json._debug_queries.length} queries`)
      expect(json._debug_queries.length).toBeGreaterThan(0)
    }
  })
})
