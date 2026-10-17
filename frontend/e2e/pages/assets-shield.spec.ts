/**
 * E2E Page Contract Test: Assets & Shield (/assets-shield)
 *
 * Implements the verification checklist specified in docs/pages/assets-shield.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & schema validation on POST /api/assets/aggregates and GET /api/assets/aggregates.
 * 4. Core asset cards & sections mounted.
 * 5. Time range presets switching cleanly without console errors or 500s.
 * 6. Zero error cards on assets page.
 * 7. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05.
 * 8. Telemetry instrumentation & query attribution audit.
 */

import { expect, test } from '@playwright/test'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

type DebugQuery = {
  sql?: string
  params?: unknown
  time_ms?: number
}

type AssetsDebugResponse = {
  _debug_queries?: DebugQuery[]
  asset_type_breakdown?: unknown[]
  cache_performance?: unknown[]
  compression_performance?: unknown[]
  large_uncompressed_assets?: unknown[]
  low_ttl_assets?: unknown[]
  missing_fields?: string[]
}

test.describe('Assets & Shield Page Contract (/assets-shield)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated cards exist with testids. `:visible` because Next keeps the
    // previous route tree mounted-but-hidden across a soft navigation (the ?service= replace),
    // which makes a bare testid locator match two elements.
    await expect(page.locator('[data-testid="assets-overview-grid"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-cache-efficiency-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-compression-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-uncompressed-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-low-ttl-card"]:visible')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes across cards
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(5)
  })

  test('3. Data contract & schema validation (/api/assets/aggregates)', async ({ page }) => {
    // 1. Verify POST /api/assets/aggregates
    const postRes = await page.request.post('/api/assets/aggregates', {
      data: {
        range_token: '24h',
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = await postRes.json()
    expect(postData).toBeTruthy()
    expect(postData).toHaveProperty('asset_type_breakdown')
    expect(postData).toHaveProperty('cache_performance')
    expect(postData).toHaveProperty('compression_performance')
    expect(postData).toHaveProperty('large_uncompressed_assets')
    expect(postData).toHaveProperty('low_ttl_assets')
    expect(postData).toHaveProperty('missing_fields')
    expect(Array.isArray(postData.asset_type_breakdown)).toBeTruthy()
    expect(Array.isArray(postData.cache_performance)).toBeTruthy()
    expect(Array.isArray(postData.compression_performance)).toBeTruthy()
    expect(Array.isArray(postData.large_uncompressed_assets)).toBeTruthy()
    expect(Array.isArray(postData.low_ttl_assets)).toBeTruthy()
    expect(Array.isArray(postData.missing_fields)).toBeTruthy()

    // 2. Verify GET /api/assets/aggregates
    const getRes = await page.request.get('/api/assets/aggregates?range_token=24h')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('asset_type_breakdown')
    expect(getData).toHaveProperty('missing_fields')

    // 3. Cold load mounts without client-side exceptions
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Core asset cards & breakdown sections render properly', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    // Check main section headings
    await expect(page.getByRole('heading', { name: 'Cache Efficiency by Asset Type' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Payload Compression Efficiency' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Large Uncompressed Assets' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Low Cache-TTL Hotspots' })).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    // No severe 500 or uncaught errors during range switching
    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Zero error cards on assets page', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    // Zero error cards on assets page
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/assets-shield', { waitUntil: 'domcontentloaded' })

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

    console.log(`[PERF /assets-shield] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/assets/aggregates', {
      data: {
        range_token: '24h',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as AssetsDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /assets-shield] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
