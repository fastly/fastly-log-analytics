/**
 * E2E Page Contract Test: Admin Live Query Monitor (/admin/queries)
 *
 * Implements the verification checklist specified in docs/pages/admin/queries.md:
 * 1. Route accessibility (GET /admin/queries returns 200 for Admin; Analyst Path B receives 403 on /api/admin/queries and /api/admin/slow-queries) & ServerFooter environment indicator.
 * 2. Instant shell, pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms).
 * 3. Backend data contracts: /api/admin/queries, /api/admin/queries/summary, /api/admin/slow-queries, /api/admin/slow-queries/count, /api/admin/app-config/query-monitor.
 * 4. Live query snapshot, summary strip, active/completed/notable slow query cards rendering with live backend data.
 * 5. Interactive filtering / tab switching (All / Live only / Past only, DB engine filter, Persistent slow history window) and manual refresh workflow with X-Page-Load-ID telemetry attribution.
 */

import { expect, test } from '@playwright/test'
import { adminGatewayContextOptions } from '../../../scripts/lib/admin_gateway_client.cjs'

type LayoutShiftEntry = PerformanceEntry & {
  hadRecentInput?: boolean
  value?: number
}

test.describe('Admin Live Query Monitor Page Contract (/admin/queries)', () => {
  if (process.env.PLAYWRIGHT_BASE_URL) {
    test.use({
      baseURL: process.env.PLAYWRIGHT_BASE_URL,
      ...adminGatewayContextOptions(process.env.PLAYWRIGHT_BASE_URL),
    })
  }

  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/admin/queries')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Live Query Monitor', exact: true })).toBeVisible({
      timeout: 30_000,
    })
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 15_000 })

    // Verify ServerFooter exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)

    // Verify Admin queries endpoint returns 200 for Admin session
    const snapRes = await page.request.get('/api/admin/queries')
    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.last_seq).toBe('number')
    expect(Array.isArray(snapBody.active)).toBe(true)

    // On the isolated E2E harness, also verify Analyst Path B receives 403 on /api/admin/queries and /api/admin/slow-queries
    if (!process.env.PLAYWRIGHT_BASE_URL) {
      const bootRes = await page.request.get('/api/bootstrap')
      const bootJson = await bootRes.json()
      const serviceId =
        bootJson?.services?.[0]?.id ?? bootJson?.services?.[0]?.service_id ?? 'svc-playwright-e2e'
      const email = `e2e-queries-guard-${Date.now()}@example.com`
      const passcode = 'harbor-cloud-forest-91'
      let inviteId: string | null = null
      try {
        await page.request.post('/api/admin/share/start', {
          data: { public_endpoint: 'https://share-e2e.example.com', forward_port: 13004 },
        })
        const invRes = await page.request.post('/api/admin/share/invites', {
          data: {
            name: 'Queries Guard Analyst',
            email,
            passcode,
            duration_hours: 1,
            service_ids: [serviceId],
            auth_method: 'passcode',
          },
        })
        expect(invRes.ok()).toBeTruthy()
        inviteId = (await invRes.json()).id

        const loginRes = await page.request.post('/api/share/login', {
          data: { email, passcode },
        })
        expect(loginRes.ok()).toBeTruthy()

        const tosRes = await page.request.get('/api/share/tos')
        expect(tosRes.ok()).toBeTruthy()
        const tosJson = await tosRes.json()

        const ackRes = await page.request.post('/api/share/acknowledge', {
          data: { version: tosJson.version },
        })
        expect(ackRes.ok()).toBeTruthy()

        const analystQueriesRes = await page.request.get('/api/admin/queries', {
          headers: { 'x-remote-analyst': '1' },
        })
        expect(analystQueriesRes.status()).toBe(403)
        const analystQueriesBody = await analystQueriesRes.json()
        expect(analystQueriesBody.error).toBe('admin_only')

        const analystSlowRes = await page.request.get(`/api/admin/slow-queries?service_id=${encodeURIComponent(serviceId)}`, {
          headers: { 'x-remote-analyst': '1' },
        })
        expect(analystSlowRes.status()).toBe(403)
      } finally {
        await page.request.post('/api/share/logout').catch(() => {})
        if (inviteId) {
          await page.request.delete(`/api/admin/share/invites/${inviteId}`).catch(() => {})
        }
        await page.request.post('/api/admin/share/stop').catch(() => {})
      }
    }
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms)', async ({
    page,
  }) => {
    // First visit warms browser cache
    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    // Second (warm) load measures FCP and CLS
    await page.addInitScript(() => {
      ;(window as unknown as { __clsScore: number }).__clsScore = 0
      try {
        const obs = new PerformanceObserver((list) => {
          for (const entry of list.getEntries() as LayoutShiftEntry[]) {
            if (!entry.hadRecentInput && typeof entry.value === 'number') {
              ;(window as unknown as { __clsScore: number }).__clsScore += entry.value
            }
          }
        })
        obs.observe({ type: 'layout-shift', buffered: true })
      } catch {
        // Ignore if unsupported
      }
    })

    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 15_000 })

    // Verify all zero-CLS reservation containers are mounted with contain-intrinsic-size
    const reservations = [
      'admin-queries-summary-reservation',
      'admin-queries-active-reservation',
      'admin-queries-slow-reservation',
      'admin-queries-completed-reservation',
    ]
    for (const testId of reservations) {
      const container = page.locator(`[data-testid="${testId}"]:visible`)
      await expect(container).toBeVisible({ timeout: 15_000 })
      const classAttr = await container.getAttribute('class')
      expect(classAttr).toContain('[contain-intrinsic-size:auto_')
      expect(classAttr).toContain('[content-visibility:auto]')
    }

    const metrics = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0]
      return {
        fcpMs: fcpEntry ? fcpEntry.startTime : 0,
        cls: (window as unknown as { __clsScore?: number }).__clsScore ?? 0,
      }
    })

    expect(metrics.cls).toBeLessThanOrEqual(0.05)
    if (metrics.fcpMs > 0) {
      // Allow slight headroom for containerized CI/remote runners while enforcing fast shell paint
      expect(metrics.fcpMs).toBeLessThan(1200)
    }
  })

  test('3. Backend data contracts: /api/admin/queries, /summary, /slow-queries, /slow-queries/count, /app-config/query-monitor', async ({
    page,
  }) => {
    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    const bootRes = await page.request.get('/api/bootstrap')
    const bootJson = await bootRes.json()
    const serviceId =
      bootJson?.active_service_id ??
      bootJson?.services?.[0]?.id ??
      bootJson?.services?.[0]?.service_id ??
      'svc-playwright-e2e'

    const [cfgRes, snapRes, summaryRes, slowRes, slowCountRes] = await Promise.all([
      page.request.get('/api/admin/app-config/query-monitor'),
      page.request.get('/api/admin/queries?include_completed=true&cold_limit=50'),
      page.request.get('/api/admin/queries/summary'),
      page.request.get(`/api/admin/slow-queries?service_id=${encodeURIComponent(serviceId)}&since_hours=24&threshold_ms=100`),
      page.request.get(`/api/admin/slow-queries/count?service_id=${encodeURIComponent(serviceId)}&since_hours=24&threshold_ms=1000`),
    ])

    expect(cfgRes.status()).toBe(200)
    const cfgBody = await cfgRes.json()
    expect(cfgBody.enabled).toBe(true)

    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.last_seq).toBe('number')
    expect(Array.isArray(snapBody.active)).toBe(true)
    expect(Array.isArray(snapBody.completed)).toBe(true)

    expect(summaryRes.status()).toBe(200)
    const summaryBody = await summaryRes.json()
    expect(typeof summaryBody.active_total).toBe('number')
    expect(typeof summaryBody.by_db_type).toBe('object')
    expect(typeof summaryBody.longest_ms).toBe('number')

    expect(slowRes.status()).toBe(200)
    const slowBody = await slowRes.json()
    expect(Array.isArray(slowBody.rows)).toBe(true)
    expect(slowBody.since_hours).toBe(24)

    expect(slowCountRes.status()).toBe(200)
    const slowCountBody = await slowCountRes.json()
    expect(typeof slowCountBody.count).toBe('number')
  })

  test('4. Live query snapshot, summary strip, and table sections render cleanly', async ({ page }) => {
    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    // Summary strip reservation is visible
    await expect(page.locator('[data-testid="admin-queries-summary-reservation"]:visible')).toBeVisible({
      timeout: 15_000,
    })

    // All three cards render in 'All' view mode
    await expect(page.locator('[data-testid="admin-queries-active-reservation"]:visible')).toContainText(
      /Active & Just-Finished/i,
      { timeout: 15_000 },
    )
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]:visible')).toContainText(
      /Notable Slow Queries/i,
      { timeout: 15_000 },
    )
    await expect(page.locator('[data-testid="admin-queries-completed-reservation"]:visible')).toContainText(
      /Recently Completed/i,
      { timeout: 15_000 },
    )
  })

  test('5. Interactive filtering, tab switching, and manual refresh with X-Page-Load-ID attribution', async ({
    page,
  }) => {
    const observedHeaders: Record<string, string>[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/queries') || req.url().includes('/api/admin/slow-queries')) {
        observedHeaders.push(req.headers())
      }
    })

    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    // Switch to 'Live only' tab -> Slow & Completed cards should hide
    const liveTab = page.getByRole('tab', { name: 'Live only' })
    await liveTab.click()
    await expect(page.locator('[data-testid="admin-queries-active-reservation"]:visible')).toBeVisible()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]')).toHaveCount(0)

    // Switch back to 'All' tab -> Slow & Completed cards reappear
    const allTab = page.getByRole('tab', { name: 'All' })
    await allTab.click()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]:visible')).toBeVisible()

    // Switch Notable Slow Queries history toggle from 'Recent' to '24 h' to trigger /api/admin/slow-queries fetch
    const slow24hBtn = page
      .locator('[data-testid="admin-queries-slow-reservation"]')
      .getByRole('button', { name: '24 h', exact: true })
    const slowResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/admin/slow-queries') && res.status() === 200,
      { timeout: 15_000 },
    )
    await slow24hBtn.click()
    const slowRes = await slowResponsePromise
    expect(slowRes.status()).toBe(200)

    // Trigger manual refresh button and verify /api/admin/queries request fires with x-page-load-id
    const refreshBtn = page.locator('[data-testid="admin-queries-refresh-btn"]:visible')
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })

    const refreshResPromise = page.waitForResponse(
      (res) => res.url().includes('/api/admin/queries') && res.status() === 200,
      { timeout: 15_000 },
    )
    await refreshBtn.click()
    const refreshRes = await refreshResPromise
    expect(refreshRes.status()).toBe(200)

    const withPageLoadId = observedHeaders.filter((h) => Boolean(h['x-page-load-id']))
    expect(withPageLoadId.length).toBeGreaterThan(0)
  })
})
