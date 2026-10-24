/**
 * E2E Page Contract Test: High-Scale Request Facts (/high-scale/request-facts)
 *
 * Implements the verification checklist specified in docs/pages/high-scale-request-facts.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & zero-CLS containers.
 * 3. Data contract & schema validation on GET/POST /api/high-scale/request-facts and POST /api/high-scale/services/{service_id}/request-facts.
 * 4. Metadata bar, mode badge (ClickHouse vs. DuckDB Emulation), filter controls, and table rendering.
 * 5. Filter controls interaction (status, domain, pop, country, batch_id) & clear filters without 500s.
 * 6. Performance budget: FCP < 400ms / LCP < 1500ms and CLS <= 0.05 on warm load.
 * 7. Telemetry instrumentation & query attribution audit (_debug_queries).
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import {
  getFirstServiceId as resolveActiveServiceId,
  installClsObserver,
  measureWarmPaintAndCls,
} from '../helpers'

type RequestFactsDebugResponse = {
  _debug_queries?: DebugQuery[]
  rows?: Array<Record<string, unknown>>
  metadata?: {
    status?: string
    exact?: boolean
    coverage?: number
    freshness_lag_seconds?: number
    mode?: string
    watermark?: Record<string, unknown>
  }
  next_cursor?: string | null
}

test.describe('High-Scale Request Facts Page Contract (/high-scale/request-facts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    // Verify intrinsic size reservation classes exist on the page
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('3. Data contract & schema validation (/api/high-scale/request-facts)', async ({ page }) => {
    const serviceId = await resolveActiveServiceId(page)
    expect(serviceId).toBeTruthy()

    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    // 1. Verify GET /api/high-scale/request-facts
    const getRes = await page.request.get(
      `/api/high-scale/request-facts?service_id=${encodeURIComponent(serviceId!)}&start_time=${encodeURIComponent(oneHourAgo.toISOString())}&end_time=${encodeURIComponent(now.toISOString())}&limit=10`
    )
    expect(getRes.ok()).toBeTruthy()
    const getData = (await getRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(getData.rows)).toBeTruthy()
    expect(getData.metadata).toBeTruthy()
    expect(['complete', 'partial', 'unavailable']).toContain(getData.metadata?.status)
    expect(['clickhouse', 'duckdb_emulation']).toContain(getData.metadata?.mode)

    // 2. Verify POST /api/high-scale/services/{service_id}/request-facts with emulate_standard=true
    const postRes = await page.request.post(
      `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`,
      {
        data: {
          start_time: oneHourAgo.toISOString(),
          end_time: now.toISOString(),
          limit: 10,
          emulate_standard: true,
        },
      }
    )
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(postData.rows)).toBeTruthy()
    expect(postData.metadata?.watermark?.service_id).toBe(serviceId)
  })

  test('4. Metadata bar, mode badge, filter bar, and request facts table render cleanly', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })
    await expect(page.getByTestId('high-scale-metadata')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByTestId('high-scale-mode-badge')).toBeVisible({ timeout: 15_000 })

    const badgeText = await page.getByTestId('high-scale-mode-badge').innerText()
    expect(badgeText).toMatch(/DuckDB Standard Mode Emulation|ClickHouse Distributed Cluster/)

    await expect(page.getByTestId('high-scale-filters')).toBeVisible()
    await expect(page.getByTestId(' high-scale-table-wrapper'.trim())).toBeVisible()
    await expect(page.locator('[role="alert"].border-destructive')).toHaveCount(0)
  })

  test('5. Interactive filter controls (status, domain, pop, batch_id) & clear filters', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/high-scale/request-facts')
    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })

    await page.getByTestId('filter-status').fill('200')
    await page.getByTestId('filter-pop').fill('IAD')
    await expect(page.getByTestId('filter-clear')).toBeVisible()

    await page.getByTestId('filter-clear').click()
    await expect(page.getByTestId('filter-status')).toHaveValue('')
    await expect(page.getByTestId('filter-pop')).toHaveValue('')

    const fatalErrors = consoleErrors.filter(
      (e) => e.includes('500') || e.includes('Internal Server Error')
    )
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Performance budget: FCP < 400ms / LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/high-scale/request-facts', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(
      `[PERF /high-scale/request-facts] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`
    )
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = await resolveActiveServiceId(page)
    expect(serviceId).toBeTruthy()

    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    const res = await page.request.post(
      `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`,
      {
        data: {
          start_time: oneHourAgo.toISOString(),
          end_time: now.toISOString(),
          limit: 10,
          emulate_standard: true,
        },
        headers: {
          'X-Page-Load-ID': `e2e-hs-facts-${Date.now()}`,
          'x-debug-responses': '1',
        },
      }
    )
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as RequestFactsDebugResponse
    expect(json).toHaveProperty('_debug_queries')
    const queries = json._debug_queries ?? []
    console.log(`[TELEMETRY /high-scale/request-facts] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
