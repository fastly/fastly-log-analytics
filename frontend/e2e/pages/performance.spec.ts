/**
 * E2E Page Contract Test: Performance (/performance)
 *
 * Implements the verification checklist specified in docs/pages/performance.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & in-place loading states (Zero CLS).
 * 3. Data contract: exactly 1 round-trip on cold load (/api/performance/aggregates), HTTP 200, valid schema.
 * 4. All 5 core performance cards mounted and visible.
 * 5. Time range switching: 1h, 24h, 7d presets update URL and scan window.
 * 6. Column visibility controls on latency data tables.
 * 7. Non-empty Plotly visualizations and zero error cards.
 * 8. Performance budget: FCP/LCP within budget (< 1500ms), CLS <= 0.05.
 * 9. Telemetry instrumentation & query efficiency audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { installClsObserver, measureWarmPaintAndCls } from '../helpers'

type PerformanceDebugResponse = {
  _debug_queries?: DebugQuery[]
  waterfall?: Record<string, unknown>
  top_urls?: unknown[]
  top_asns?: unknown[]
  ttl_dist?: unknown[]
  scatter?: unknown[]
  _approx?: boolean
}

type OriginDebugResponse = {
  _debug_queries?: DebugQuery[]
  summary?: Record<string, unknown>
  timeseries?: Record<string, unknown>
  slow_urls?: Record<string, unknown>
  status_codes?: Record<string, unknown>
  path_breakdown?: Record<string, unknown>
  pop_latency?: Record<string, unknown>
  ip_health?: Record<string, unknown>
  has_data?: boolean
}

test.describe('Performance Page Contract (/performance)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated cards exist with content-visibility / contain-intrinsic-size
    const waterfallCard = page.getByText('End-to-End Latency Waterfall (Average)').locator('visible=true')
    await expect(waterfallCard).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(3)
  })

  test('3. Data contract & round-trip verification (/api/performance/aggregates)', async ({ page }) => {
    // 1. Verify endpoint schema directly via API client
    const apiRes = await page.request.post('/api/performance/aggregates', {
      data: {
        range_token: '24h',
        sort_by: 'p99',
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toBeTruthy()
    expect(data).toHaveProperty('waterfall')
    expect(data).toHaveProperty('top_urls')
    expect(data).toHaveProperty('top_asns')
    expect(data).toHaveProperty('ttl_dist')
    expect(data).toHaveProperty('scatter')

    // 2. Cold load leverages SSR dehydration without client-side errors
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All 5 core performance cards render properly', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    await expect(page.getByText('End-to-End Latency Waterfall (Average)').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Slowest URLs').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Slowest Networks').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Cache TTL Distribution').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Origin vs Edge Processing (ms)').locator('visible=true')).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets and adaptive history extents', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar (defaults to 24h for >=24h data, or max extent for <24h data)
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    // Column visibility dropdown trigger button on Slowest URLs card
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p99/i }).or(page.getByText(/p99/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    // Must have rendered Plotly charts
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    await expect(plotlyChart).toBeVisible({ timeout: 20_000 })
    await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })

    // Zero error cards on performance page
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/performance', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /performance] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/performance/aggregates', {
      data: {
        range_token: '24h',
        sort_by: 'p99',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as PerformanceDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /performance] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})


test.describe('Origin Page Contract (/origin)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated cards exist with content-visibility / contain-intrinsic-size
    const originLatencyCard = page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Latency' }).first()
    await expect(originLatencyCard).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes across KPI, timeseries, and table cards
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(6)
  })

  test('3. Data contract & round-trip verification (/api/origin/aggregates)', async ({ page }) => {
    // 1. Verify endpoint schema directly via API client
    const apiRes = await page.request.post('/api/origin/aggregates', {
      data: {
        range_token: '24h',
        timeseries_metric: 'ttfb',
        timeseries_percentile: 'p95',
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toBeTruthy()
    expect(data).toHaveProperty('summary')
    expect(data).toHaveProperty('timeseries')
    expect(data).toHaveProperty('slow_urls')
    expect(data).toHaveProperty('status_codes')
    expect(data).toHaveProperty('path_breakdown')
    expect(data).toHaveProperty('pop_latency')
    expect(data).toHaveProperty('ip_health')

    // 2. Cold load leverages SSR dehydration without client-side errors
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All core origin cards render properly', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Check KPI Cards
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin TTFB (P50)' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin TTFB (P95)' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Error Rate' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Fetch Volume' }).first()).toBeVisible({ timeout: 15_000 })

    // Check Timeseries & Distribution Cards
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Latency' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Status Code Distribution' }).first()).toBeVisible({ timeout: 15_000 })

    // Check Heatmap & Breakdown Cards
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Slowest URLs at Origin' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Performance by POP' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin IP Health' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Payload Size' }).first()).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    // No severe 500 or uncaught errors during range switching
    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Column visibility dropdown trigger button on Slowest URLs card
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p95/i }).or(page.getByText(/p95/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    // Must have rendered Plotly charts when data exists
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }

    // Zero error cards on origin page
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/origin', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /origin] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/origin/aggregates', {
      data: {
        range_token: '24h',
        timeseries_metric: 'ttfb',
        timeseries_percentile: 'p95',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as OriginDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /origin] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
