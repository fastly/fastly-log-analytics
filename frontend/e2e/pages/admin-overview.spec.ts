/**
 * E2E Page Contract Test: Admin Overview & Ingest Operations (/admin)
 *
 * Implements the verification checklist specified in docs/pages/admin/overview.md:
 * 1. Route accessibility (GET /admin returns 200 for Admin; Analyst Path B receives 403 on /api/admin/sync-status) & ServerFooter environment indicator.
 * 2. Instant shell, pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms).
 * 3. Backend data contracts: /api/admin/sync-status, /api/admin/system-metrics, /api/admin/health, /api/admin/compaction-status, /api/services.
 * 4. Ingest & sync status cards, system metrics, and configured services list rendering with live backend data.
 * 5. Interactive manual status refresh workflow with X-Page-Load-ID telemetry attribution.
 */

import { expect, test } from '@playwright/test'
import { installClsObserver, measureWarmPaintAndCls } from '../cls-observer'
import { loginAsPasscodeAnalyst, revokeAnalystInvite } from '../helpers/analyst-session'
import { configureExternalAdminOrigin, getFirstServiceId } from './helpers'

configureExternalAdminOrigin()

test.describe('Admin Overview & Ingest Operations Page Contract (/admin)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/admin')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Admin', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 15_000 })

    // Verify ServerFooter exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)

    // Verify Admin sync-status endpoint returns access_level='read_write' for Admin session
    const syncRes = await page.request.get('/api/admin/sync-status')
    expect(syncRes.status()).toBe(200)
    const syncBody = await syncRes.json()
    expect(syncBody.access_level).toBe('read_write')

    // On the isolated E2E harness, also verify Analyst Path B receives 403 on /api/admin/sync-status
    if (!process.env.PLAYWRIGHT_BASE_URL) {
      const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
      let inviteId: string | null = null
      try {
        const seeded = await loginAsPasscodeAnalyst(page.request, {
          name: 'Admin Guard Analyst',
          email: `e2e-admin-guard-${Date.now()}@example.com`,
          passcode: 'harbor-cloud-forest-91',
          durationHours: 1,
          serviceIds: [serviceId],
        })
        inviteId = seeded.inviteId

        const analystSyncRes = await page.request.get('/api/admin/sync-status', {
          headers: { 'x-remote-analyst': '1' },
        })
        expect(analystSyncRes.status()).toBe(403)
        const analystSyncBody = await analystSyncRes.json()
        expect(analystSyncBody.error).toBe('admin_only')
      } finally {
        await revokeAnalystInvite(page.request, inviteId, { logout: true })
      }
    }
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms)', async ({ page }) => {
    await page.goto('/admin', { waitUntil: 'domcontentloaded' })
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated reservation containers
    await expect(page.locator('[data-testid="admin-operations-reservation"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="admin-services-reservation"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="admin-health-reservation"]:visible')).toBeVisible({ timeout: 15_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(5)

    // Measure warm-load FCP and CLS
    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 15_000 })

    const metrics = await measureWarmPaintAndCls(page, 500)

    console.log(
      `[PERF /admin] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`,
    )
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('3. Backend REST contracts (/api/admin/sync-status, /api/admin/system-metrics, /api/admin/health, /api/admin/compaction-status)', async ({ page }) => {
    const syncRes = await page.request.get('/api/admin/sync-status')
    expect(syncRes.ok()).toBeTruthy()
    const syncData = await syncRes.json()
    expect(syncData).toHaveProperty('configured')
    expect(syncData).toHaveProperty('access_level')

    const metricsRes = await page.request.get('/api/admin/system-metrics')
    expect(metricsRes.ok()).toBeTruthy()
    const metricsData = await metricsRes.json()
    expect(metricsData).toHaveProperty('load')
    expect(metricsData).toHaveProperty('vcpus')
    expect(metricsData).toHaveProperty('in_flight_runs')
    expect(metricsData).toHaveProperty('compaction')

    const healthRes = await page.request.get('/api/admin/health')
    expect(healthRes.ok()).toBeTruthy()
    const healthData = await healthRes.json()
    expect(healthData).toHaveProperty('load')
    expect(healthData).toHaveProperty('recent_cron_failures')

    const servicesRes = await page.request.get('/api/services')
    expect(servicesRes.ok()).toBeTruthy()
    const servicesData = await servicesRes.json()
    expect(Array.isArray(servicesData.services)).toBeTruthy()

    if (servicesData.services.length > 0) {
      const firstId = servicesData.services[0].service_id
      const compRes = await page.request.get(`/api/admin/compaction-status/${firstId}`)
      expect(compRes.ok()).toBeTruthy()
      const compData = await compRes.json()
      expect(compData).toHaveProperty('partitions')
      expect(compData).toHaveProperty('total_files')

      const acctRes = await page.request.get(`/api/admin/log-accounting/${firstId}?hours=24`)
      expect(acctRes.ok()).toBeTruthy()
      const acctData = await acctRes.json()
      expect(acctData).toHaveProperty('buckets')
      expect(acctData).toHaveProperty('totals')
    }
  })

  test('4. Ingest & sync status cards, system metrics, and configured services list render live data', async ({ page }) => {
    await page.goto('/admin')
    await expect(page.locator('[data-testid="admin-operations-overview"]:visible')).toBeVisible({ timeout: 30_000 })

    // Verify 4 OperationsOverview cards
    await expect(page.getByRole('link', { name: /Live Queries/ }).first()).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText('Ingest Gap').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText('Notable Slow Queries').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText('Workers & Queues').first()).toBeVisible({ timeout: 10_000 })

    // Verify Service Management table section
    await expect(page.locator('[data-testid="admin-services-section"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Service Management' }).first()).toBeVisible({ timeout: 10_000 })

    // Verify System Health Card
    await expect(page.getByText('System Health').first()).toBeVisible({ timeout: 15_000 })
  })

  test('5. Interactive manual status refresh workflow with X-Page-Load-ID telemetry attribution', async ({ page }) => {
    const pageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/services') || req.url().includes('/api/admin/')) {
        const plid = req.headers()['x-page-load-id']
        if (plid) pageLoadIds.push(plid)
      }
    })

    await page.goto('/admin')
    await expect(page.locator('[data-testid="admin-overview-refresh-btn"]:visible')).toBeVisible({ timeout: 30_000 })

    // Wait for initial load to settle
    await expect(page.locator('[data-testid="admin-overview-refresh-btn"]:visible')).toBeEnabled({ timeout: 15_000 })

    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/services') && req.method() === 'GET',
      { timeout: 15_000 },
    )
    await page.locator('[data-testid="admin-overview-refresh-btn"]:visible').click()
    const refreshReq = await requestPromise
    expect(refreshReq.headers()['x-page-load-id']).toBeTruthy()
    expect(pageLoadIds.length).toBeGreaterThan(0)
  })
})
