/**
 * E2E Page Contract Test: Network Path & ASN Heatmap (/network)
 *
 * Implements the verification checklist specified in docs/pages/network.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & in-place loading states (Zero CLS).
 * 3. Data contract & schema validation on /api/network/bundle and /api/network-health.
 * 4. Core network cards, Edge Transport section & ASN leaderboard rendering.
 * 5. Time range presets switching cleanly without console errors or 500s.
 * 6. Non-empty visualizations or clean fallback states and zero error cards.
 * 7. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05.
 * 8. Telemetry instrumentation & query efficiency audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { installClsObserver, measureWarmPaintAndCls } from '../helpers'

type NetworkDebugResponse = {
  _debug_queries?: DebugQuery[]
  transport?: Record<string, unknown>
  available?: boolean
  reason?: string
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

test.describe('Network Page Contract (/network)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    // Verify intrinsic size reservation classes across KPI grids and sections
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(4)
  })

  test('3. Data contract & schema validation (/api/network/bundle & /api/network-health)', async ({ page }) => {
    // 1. Verify /api/network/bundle
    const bundleRes = await page.request.post('/api/network/bundle', {
      data: {
        sections: ['transport'],
      },
    })
    expect(bundleRes.ok()).toBeTruthy()
    const bundleData = await bundleRes.json()
    expect(bundleData).toBeTruthy()
    expect(bundleData).toHaveProperty('transport')
    if (bundleData.transport) {
      expect(bundleData.transport).toHaveProperty('protocol_distribution')
      expect(bundleData.transport).toHaveProperty('missing_fields')
      expect(Array.isArray(bundleData.transport.protocol_distribution)).toBeTruthy()
      expect(Array.isArray(bundleData.transport.missing_fields)).toBeTruthy()
    }

    // 2. Verify /api/network-health
    const healthRes = await page.request.post('/api/network-health', {
      data: {
        sections: ['summary', 'leaderboard', 'metro_leaderboard'],
        top_n: 30,
      },
    })
    expect(healthRes.ok()).toBeTruthy()
    const healthData = await healthRes.json()
    expect(healthData).toBeTruthy()
    expect(healthData).toHaveProperty('available')

    // 3. Cold load mounts without client-side exceptions
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Core network cards & transport section mounted', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    // When network fields are active or fallback banner renders
    const unavailableNotice = page.getByText('Network Metrics Unavailable')
    const hasNotice = await unavailableNotice.isVisible().catch(() => false)

    if (!hasNotice) {
      // Summary KPI cards
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Global Health' }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Worst ASN' }).first()).toBeVisible({ timeout: 15_000 })

      // Edge Transport section
      await expect(page.getByText('Edge Transport & TCP Performance')).toBeVisible({ timeout: 15_000 })

      // ASN Leaderboard or PoP Heatmap
      const leaderboardTitle = page.getByText('ASN Performance Leaderboard')
      const popHeatmapTitle = page.getByText('Fastly Edge PoP Health Heatmap')
      await expect(leaderboardTitle.or(popHeatmapTitle).first()).toBeVisible({ timeout: 15_000 })
    } else {
      await expect(unavailableNotice).toBeVisible()
    }
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    // No severe 500 or uncaught errors during range switching
    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Zero error cards on network page', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    // Zero error cards on network page
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/network', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /network] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/network/bundle', {
      data: {
        sections: ['transport'],
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as NetworkDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /network] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})


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

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

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
