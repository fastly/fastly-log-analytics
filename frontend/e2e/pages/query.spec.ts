/**
 * E2E Page Contract Test: SQL Query Pad (/query)
 *
 * Implements the verification checklist specified in docs/pages/query.md:
 * 1. Route accessibility, footer environment indicator, and layout mounting.
 * 2. Pre-allocated zero-CLS containers and editor skeletons.
 * 3. Schema explorer expansion and table column display (GET /api/query/schema/{service_id}).
 * 4. Execution of valid analytical queries (SELECT count(*) FROM logs).
 * 5. Safety enforcement rejecting mutating queries (DROP TABLE logs) with user-friendly errors.
 * 6. Client-side CSV export trigger.
 * 7. Performance budget compliance (FCP < 400ms, CLS <= 0.05).
 * 8. Telemetry and query recording (_debug_queries / debug_queries).
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

type QueryDebugResponse = {
  _debug_queries?: DebugQuery[]
  debug_queries?: DebugQuery[]
  columns?: string[]
  data?: Array<Record<string, unknown>>
  total_rows?: number
  elapsed_ms?: number
}

test.describe('SQL Query Pad Page Contract (/query)', () => {

  test('1. Route accessibility, environment indicator & layout mounting', async ({ page }) => {
    await page.goto('/query')
    await expect(page.getByRole('heading', { name: /Query Explorer/i })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await page.goto('/query')
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="query-editor-container"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="query-run-btn"]')).toBeVisible({ timeout: 15_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('3. Schema explorer & schema reflection contract (/api/query/schema/{service_id})', async ({ page }) => {
    const servicesRes = await page.request.get('/api/services')
    expect(servicesRes.ok()).toBeTruthy()
    const servicesJson = await servicesRes.json()
    const services = Array.isArray(servicesJson) ? servicesJson : (servicesJson?.services ?? [])
    const serviceId = services[0]?.id ?? services[0]?.service_id ?? 'default'

    const schemaRes = await page.request.get(`/api/query/schema/${encodeURIComponent(serviceId)}`)
    expect(schemaRes.ok()).toBeTruthy()
    const schemaData = await schemaRes.json()
    expect(schemaData).toHaveProperty('columns')
    expect(Array.isArray(schemaData.columns)).toBeTruthy()
    expect(schemaData.columns.length).toBeGreaterThan(0)

    await page.goto('/query')
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })

    const rawSqlTab = page.getByRole('tab', { name: /Edit Raw SQL/i })
    await expect(rawSqlTab).toBeVisible({ timeout: 10_000 })
    await rawSqlTab.click()

    const sidebar = page.locator('[data-testid="query-schema-sidebar"]')
    await expect(sidebar).toBeVisible({ timeout: 10_000 })
    await expect(sidebar).toContainText('timestamp')
    await expect(sidebar).toContainText('status')
  })

  test('4. Valid analytical SQL execution contract (SELECT count(*) FROM logs)', async ({ page }) => {
    const postRes = await page.request.post('/api/query', {
      data: {
        sql: 'SELECT count(*) AS total_count FROM logs',
        dataset: 'logs',
        max_rows: 100,
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as QueryDebugResponse
    expect(postData).toHaveProperty('columns')
    expect(postData).toHaveProperty('data')
    expect(postData).toHaveProperty('total_rows')
    expect(postData).toHaveProperty('elapsed_ms')
    expect(Array.isArray(postData.columns)).toBeTruthy()
    expect(Array.isArray(postData.data)).toBeTruthy()
    expect(postData.data?.length).toBe(1)

    // Execute query via UI using ?q= URL parameter hydration
    await page.goto('/query?q=' + encodeURIComponent('SELECT count(*) AS total_count FROM logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })

    const runBtn = page.locator('[data-testid="query-run-btn"]')
    await expect(runBtn).toBeVisible({ timeout: 10_000 })
    await runBtn.click()

    await expect(page.locator('[data-testid="query-results-table"]')).toBeVisible({ timeout: 20_000 })
  })

  test('5. AST safety enforcement rejects mutating queries (DROP TABLE logs)', async ({ page }) => {
    // 1. Direct API contract verification
    for (const mutatingSql of [
      'DROP TABLE logs',
      'DELETE FROM logs',
      'INSERT INTO logs VALUES (1)',
      'ALTER TABLE logs DROP COLUMN url',
    ]) {
      const res = await page.request.post('/api/query', {
        data: { sql: mutatingSql, dataset: 'logs' },
      })
      expect(res.status()).toBe(403)
      const body = await res.json()
      expect(JSON.stringify(body)).toMatch(/sql_not_permitted|only SELECT/i)
    }

    // 2. UI error alert verification
    await page.goto('/query?q=' + encodeURIComponent('DROP TABLE logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })

    await page.locator('[data-testid="query-run-btn"]').click()

    const errorAlert = page.locator('[data-testid="query-error-alert"]')
    await expect(errorAlert).toBeVisible({ timeout: 15_000 })
    await expect(errorAlert).toContainText(/only SELECT|sql_not_permitted|Query Execution Failed/i)
  })

  test('6. Client-side CSV export trigger', async ({ page }) => {
    await page.goto('/query?q=' + encodeURIComponent('SELECT count(*) AS total_count FROM logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })

    await page.locator('[data-testid="query-run-btn"]').click()
    await expect(page.locator('[data-testid="query-results-table"]')).toBeVisible({ timeout: 20_000 })

    const exportBtn = page.locator('[data-testid="query-csv-export-btn"]')
    await expect(exportBtn).toBeVisible({ timeout: 10_000 })

    const downloadPromise = page.waitForEvent('download')
    await exportBtn.click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe('query_results.csv')
  })

  test('7. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/query', { waitUntil: 'domcontentloaded' })

    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await page.evaluate(() => {
      return new Promise<{ fcp: number; cls: number }>((resolve) => {
        let fcp = 0
        let cls = 0
        try {
          const paintEntries = performance.getEntriesByName('first-contentful-paint')
          if (paintEntries.length > 0) {
            fcp = paintEntries[0].startTime
          }

          new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              const layoutShift = entry as LayoutShiftEntry
              if (!layoutShift.hadRecentInput) {
                cls += layoutShift.value ?? 0
              }
            }
          }).observe({ type: 'layout-shift', buffered: true })

          setTimeout(() => resolve({ fcp, cls }), 1000)
        } catch {
          resolve({ fcp: 0, cls: 0 })
        }
      })
    })

    console.log(`[PERF /query] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.fcp > 0) {
      expect(metrics.fcp).toBeLessThan(400)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/query', {
      data: {
        sql: 'SELECT count(*) AS total_count FROM logs',
        dataset: 'logs',
        max_rows: 10,
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as QueryDebugResponse
    const queries = json._debug_queries ?? json.debug_queries ?? []
    console.log(`[TELEMETRY /query] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
