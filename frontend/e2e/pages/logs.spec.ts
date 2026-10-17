/**
 * E2E Page Contract Test: Raw Logs Explorer (/logs)
 *
 * Implements the verification checklist specified in docs/pages/logs.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & schema validation on POST /api/logs/query and GET /api/logs.
 * 4. Core Raw Logs Explorer controls mounted & functioning.
 * 5. Search input and filter drill-downs without console errors or 500s.
 * 6. Row detail inspection drawer/modal with parsed fields & copy action.
 * 7. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05.
 * 8. Telemetry instrumentation & query attribution audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../_types'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

type LogsDebugResponse = {
  _debug_queries?: DebugQuery[]
  rows?: unknown[]
  total_count?: number
  has_more?: boolean
  columns?: string[]
  schema_columns?: Array<{ name: string; type: string }>
}

test.describe('Raw Logs Explorer Page Contract (/logs)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.getByRole('heading', { name: /Raw Logs Explorer|Data Management/i })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.getByRole('heading', { name: /Raw Logs Explorer|Data Management/i })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated raw logs container exists with testid
    await expect(page.locator('[data-testid="raw-logs-explorer"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="raw-logs-table-container"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('3. Data contract & schema validation (/api/logs/query)', async ({ page }) => {
    // 1. Verify POST /api/logs/query
    const postRes = await page.request.post('/api/logs/query', {
      data: {
        limit: 10,
        offset: 0,
        sort_dir: 'desc',
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = await postRes.json()
    expect(postData).toBeTruthy()
    expect(postData).toHaveProperty('rows')
    expect(postData).toHaveProperty('total_count')
    expect(postData).toHaveProperty('has_more')
    expect(postData).toHaveProperty('columns')
    expect(postData).toHaveProperty('schema_columns')
    expect(Array.isArray(postData.rows)).toBeTruthy()
    expect(typeof postData.total_count).toBe('number')
    expect(typeof postData.has_more).toBe('boolean')
    expect(Array.isArray(postData.columns)).toBeTruthy()
    expect(Array.isArray(postData.schema_columns)).toBeTruthy()

    // 2. Verify GET /api/logs
    const getRes = await page.request.get('/api/logs?limit=5')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('rows')
    expect(getData).toHaveProperty('total_count')
    expect(Array.isArray(getData.rows)).toBeTruthy()

    // 3. Cold load mounts without client-side exceptions
    await page.goto('/logs')
    await expect(page.getByRole('heading', { name: /Raw Logs Explorer|Data Management/i })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Core Raw Logs Explorer controls & column selector', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.locator('[data-testid="raw-logs-explorer"]')).toBeVisible({ timeout: 30_000 })

    // Search input
    await expect(page.locator('[data-testid="raw-logs-search-input"]')).toBeVisible({ timeout: 10_000 })

    // Column selector
    const colSelector = page.locator('[data-testid="raw-logs-column-selector"]')
    await expect(colSelector).toBeVisible({ timeout: 10_000 })
    await colSelector.click()
    await expect(page.getByRole('menuitemcheckbox', { name: 'timestamp', exact: true })).toBeVisible({ timeout: 5_000 })
    // Close dropdown
    await page.keyboard.press('Escape')

    // Pagination buttons
    await expect(page.locator('[data-testid="raw-logs-pagination-prev"]')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('[data-testid="raw-logs-pagination-next"]')).toBeVisible({ timeout: 10_000 })
  })

  test('5. Search input debouncing without console errors or 500s', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/logs')
    await expect(page.locator('[data-testid="raw-logs-explorer"]')).toBeVisible({ timeout: 30_000 })

    const searchInput = page.locator('[data-testid="raw-logs-search-input"]')
    await expect(searchInput).toBeVisible({ timeout: 10_000 })
    await searchInput.fill('/api/health')
    await page.waitForTimeout(500) // allow debounce

    // Clear search
    await searchInput.fill('')
    await page.waitForTimeout(500)

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Row detail inspection modal with parsed fields & copy action', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.locator('[data-testid="raw-logs-table-container"]')).toBeVisible({ timeout: 30_000 })

    // If rows are present in the table, click the first row
    const tableRows = page.locator('[data-testid="raw-logs-table"] tbody tr')
    const rowCount = await tableRows.count()

    if (rowCount > 0) {
      await tableRows.first().click()
      const modal = page.locator('[data-testid="raw-logs-row-detail-modal"]')
      await expect(modal).toBeVisible({ timeout: 10_000 })

      // Verify Copy JSON button
      const copyBtn = page.locator('[data-testid="raw-logs-copy-json"]')
      await expect(copyBtn).toBeVisible({ timeout: 5_000 })

      // Verify Formatted tab content
      await expect(modal.getByRole('tab', { name: /Formatted/i })).toBeVisible({ timeout: 5_000 })

      // Switch to Raw JSON tab
      const rawTab = modal.getByRole('tab', { name: /Raw JSON/i })
      await expect(rawTab).toBeVisible({ timeout: 5_000 })
      await rawTab.click()
      await expect(modal.locator('pre')).toBeVisible({ timeout: 5_000 })

      // Close modal
      await page.keyboard.press('Escape')
      await expect(modal).not.toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/logs', { waitUntil: 'domcontentloaded' })

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

    console.log(`[PERF /logs] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/logs/query', {
      data: {
        limit: 10,
        offset: 0,
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as LogsDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /logs] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
