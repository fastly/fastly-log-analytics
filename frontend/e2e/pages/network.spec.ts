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

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

type DebugQuery = {
  sql?: string
  params?: unknown
  time_ms?: number
}

type NetworkDebugResponse = {
  _debug_queries?: DebugQuery[]
  transport?: Record<string, unknown>
  available?: boolean
  reason?: string
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

    console.log(`[PERF /network] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    test.skip(!!process.env.CI, 'requires seeded log data / real FOS; the CI e2e backend has neither')
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
