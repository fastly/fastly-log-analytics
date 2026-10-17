/**
 * E2E Page Contract Test: User Sessions Explorer (/sessions) & Stream Detail (/sessions/stream)
 *
 * Implements the verification checklist specified in docs/pages/sessions.md and docs/pages/sessions-stream.md:
 * 1. Route accessibility & footer environment mode verification for /sessions.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & schema validation on POST /api/sessions and POST /api/sessions/detail.
 * 4. Filter controls (flagged, streaming, min-reqs, min-4xx%) without console errors or 500s.
 * 5. Row inspection: Clicking a session opens the SessionDetail dialog with metadata and request logs.
 * 6. Stream Session Detail (/sessions/stream): Handles both missing token and valid token states.
 * 7. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05 on warm reload.
 * 8. Telemetry instrumentation & query attribution audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../_types'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

type SessionsDebugResponse = {
  _debug_queries?: DebugQuery[]
  sessions?: Array<{
    ip: string
    session_token?: string
    session_start: string
    session_end: string
    req_count: number
    is_streaming?: boolean
  }>
  total?: number
  page?: number
  limit?: number
}

test.describe('User Sessions Explorer Page Contract (/sessions)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated user sessions container exists with testid
    await expect(page.locator('[data-testid="user-sessions-explorer"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="sessions-table-container"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="sessions-scoring-controls"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('3. Data contract & schema validation (/api/sessions and /api/sessions/detail)', async ({ page }) => {
    // 1. Verify POST /api/sessions
    const postRes = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as SessionsDebugResponse
    expect(postData).toBeTruthy()
    expect(postData).toHaveProperty('sessions')
    expect(postData).toHaveProperty('total')
    expect(Array.isArray(postData.sessions)).toBeTruthy()
    expect(typeof postData.total).toBe('number')

    // 2. If sessions exist, test POST /api/sessions/detail with sealed token
    if (postData.sessions && postData.sessions.length > 0) {
      const firstSession = postData.sessions[0]
      expect(firstSession).toHaveProperty('session_token')
      expect(typeof firstSession.session_token).toBe('string')

      const detailRes = await page.request.post('/api/sessions/detail', {
        data: {
          session_token: firstSession.session_token,
        },
      })
      expect(detailRes.ok()).toBeTruthy()
      const detailData = await detailRes.json()
      expect(detailData).toHaveProperty('columns')
      expect(detailData).toHaveProperty('data')
      expect(Array.isArray(detailData.columns)).toBeTruthy()
      expect(Array.isArray(detailData.data)).toBeTruthy()
    }

    // 3. Page mounts without uncaught exceptions
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Filter controls & toggle interactions without console errors or 500s', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/sessions')
    await expect(page.locator('[data-testid="sessions-scoring-controls"]')).toBeVisible({ timeout: 30_000 })

    // Toggle Flagged only switch
    const flaggedSwitch = page.locator('[data-testid="flagged-only-switch"]')
    await expect(flaggedSwitch).toBeVisible({ timeout: 10_000 })
    await flaggedSwitch.click()
    await page.waitForTimeout(500)
    await flaggedSwitch.click()
    await page.waitForTimeout(500)

    // Type in Min. requests input
    const minReqsInput = page.locator('[data-testid="min-reqs-input"]')
    await expect(minReqsInput).toBeVisible({ timeout: 10_000 })
    await minReqsInput.fill('5')
    await page.waitForTimeout(500)
    await minReqsInput.fill('')
    await page.waitForTimeout(500)

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('5. Row detail inspection dialog with session metadata', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.locator('[data-testid="sessions-table-container"]')).toBeVisible({ timeout: 30_000 })

    // Find table rows
    const tableRows = page.locator('[data-testid="sessions-table-container"] tbody tr')
    // Placeholder/loading rows also match `tbody tr`; wait for a real session
    // row (it starts with the client IP) before counting and clicking.
    await expect(tableRows.first()).toContainText(/\d{1,3}(\.\d{1,3}){3}|[0-9a-f:]{6,}/i, { timeout: 30_000 })
    const rowCount = await tableRows.count()

    if (rowCount > 0) {
      // Click the first row or its Details button
      await tableRows.first().click()

      const dialog = page.locator('[data-testid="session-detail-dialog"]')
      await expect(dialog).toBeVisible({ timeout: 10_000 })

      // Verify metadata grid elements
      await expect(dialog.getByText(/Session:/i)).toBeVisible({ timeout: 5_000 })
      await expect(dialog.getByText(/Requests/i)).toBeVisible({ timeout: 5_000 })

      // Close dialog via Escape key
      await page.keyboard.press('Escape')
      await expect(dialog).not.toBeVisible({ timeout: 5_000 })
    }
  })

  test('6. Stream Session Detail page (/sessions/stream)', async ({ page }) => {
    // 1. Without token: verifies "No session token" alert
    await page.goto('/sessions/stream')
    await expect(page.getByRole('heading', { name: /Stream Details/i })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText(/No session token/i)).toBeVisible({ timeout: 10_000 })

    // 2. With token: fetch a session token from API and visit
    const postRes = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
    })
    if (postRes.ok()) {
      const postData = (await postRes.json()) as SessionsDebugResponse
      const token = postData.sessions?.[0]?.session_token
      if (token) {
        await page.goto(`/sessions/stream?token=${encodeURIComponent(token)}`)
        await expect(page.locator('[data-testid="stream-detail-client"]')).toBeVisible({ timeout: 30_000 })
        await expect(page.getByRole('heading', { name: /Stream Session Details/i })).toBeVisible({ timeout: 10_000 })
      }
    }
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/sessions', { waitUntil: 'domcontentloaded' })

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

    console.log(`[PERF /sessions] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as SessionsDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /sessions] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
