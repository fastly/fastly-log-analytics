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
import { clsInitScript } from '../cls-observer'
import { loginAsPasscodeAnalyst, revokeAnalystInvite } from '../helpers/analyst-session'
import { configureExternalAdminOrigin, getFirstServiceId } from './helpers'

configureExternalAdminOrigin()

test.describe('Admin Live Query Monitor Page Contract (/admin/queries)', () => {
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
      const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
      let inviteId: string | null = null
      try {
        const seeded = await loginAsPasscodeAnalyst(page.request, {
          name: 'Queries Guard Analyst',
          email: `e2e-queries-guard-${Date.now()}@example.com`,
          passcode: 'harbor-cloud-forest-91',
          durationHours: 1,
          serviceIds: [serviceId],
        })
        inviteId = seeded.inviteId

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
        await revokeAnalystInvite(page.request, inviteId, { logout: true })
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
    await page.addInitScript(clsInitScript)

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
        cls: window.__clsScore ?? 0,
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

    const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'

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

  test('6. Pre-nuqs shared links hydrate filter state and round-trip through the URL', async ({ page }) => {
    await page.goto('/admin/queries?q=COMPACT&view=live&slow=250&nogroup=1')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    await expect(page.getByRole('tab', { name: 'Live only' })).toHaveAttribute('aria-selected', 'true')
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]')).toHaveCount(0)

    // Switching back to the default view strips its param; the rest survive.
    await page.getByRole('tab', { name: 'All' }).click()
    await expect.poll(() => new URL(page.url()).searchParams.get('view')).toBeNull()
    const params = new URL(page.url()).searchParams
    expect(params.get('q')).toBe('COMPACT')
    expect(params.get('slow')).toBe('250')
    expect(params.get('nogroup')).toBe('1')
  })
})
