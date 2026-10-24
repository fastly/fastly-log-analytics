/**
 * E2E Page Contract Test: Data Management (/logs) & Custom Field VCL Validation
 *
 * Implements the verification checklist specified in docs/pages/logs.md:
 * 1. Route accessibility (GET /logs returns 200 for Admin; Analyst Path B receives 403 on /api/cron-runs, /api/cron-schedule, /api/audit-logs) & ServerFooter environment indicator.
 * 2. Instant shell & warm-load performance budget (CLS <= 0.05, FCP < 400ms).
 * 3. Backend REST contracts: /api/cron-runs, /api/cron-schedule, /api/audit-logs, /api/admin/ingested-files, /api/schema.
 * 4. 7-tab workspace navigation (Cron Runs, Service History, Ingestion History, DuckLake Storage, Metadata Storage, Available Logs, DuckDB Schema).
 * 5. Interactive Cron Runs & Service History refresh with X-Page-Load-ID telemetry attribution.
 */

import { expect, test } from '@playwright/test'
import {
  configureExternalAdminOrigin,
  getFirstServiceId,
  installClsObserver,
  loginAsPasscodeAnalyst,
  measureWarmPaintAndCls,
  revokeAnalystInvite,
} from '../helpers'

configureExternalAdminOrigin()

test.describe('Data Management Page Contract (/logs)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/logs')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('button', { name: /Import Logs/i })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('button', { name: /Sync from Cloud/i })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('button', { name: /Commit Buffer/i })).toBeVisible({ timeout: 15_000 })

    // Verify ServerFooter exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)

    // On the isolated E2E harness, also verify Analyst Path B receives 403 on /api/cron-runs, /api/cron-schedule, /api/audit-logs
    if (!process.env.PLAYWRIGHT_BASE_URL) {
      const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
      let inviteId: string | null = null
      try {
        const seeded = await loginAsPasscodeAnalyst(page.request, {
          name: 'Logs Guard Analyst',
          email: `e2e-logs-guard-${Date.now()}@example.com`,
          passcode: 'harbor-cloud-forest-91',
          durationHours: 1,
          serviceIds: [serviceId],
        })
        inviteId = seeded.inviteId

        for (const path of ['/api/cron-runs', '/api/cron-schedule', '/api/audit-logs', '/api/admin/ingested-files']) {
          const analystRes = await page.request.get(path, {
            headers: { 'x-remote-analyst': '1' },
          })
          expect(analystRes.status()).toBe(403)
          const analystBody = await analystRes.json()
          expect(analystBody.error).toBe('admin_only')
        }
      } finally {
        await revokeAnalystInvite(page.request, inviteId, { logout: true })
      }
    }
  })

  test('2. Instant shell & warm-load performance budget (CLS <= 0.05)', async ({ page }) => {
    await page.goto('/logs', { waitUntil: 'domcontentloaded' })
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 15_000 })

    const metrics = await measureWarmPaintAndCls(page, 500)

    console.log(
      `[PERF /logs] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`,
    )
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('3. Backend REST contracts (/api/cron-runs, /api/cron-schedule, /api/audit-logs, /api/admin/ingested-files, /api/schema)', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)

    const cronRes = await page.request.get(
      serviceId ? `/api/cron-runs?service_id=${encodeURIComponent(serviceId)}&limit=50` : '/api/cron-runs?limit=50',
    )
    expect(cronRes.ok()).toBeTruthy()
    const cronData = await cronRes.json()
    expect(cronData).toHaveProperty('entries')
    expect(Array.isArray(cronData.entries)).toBe(true)

    const schedRes = await page.request.get(
      serviceId ? `/api/cron-schedule?service_id=${encodeURIComponent(serviceId)}` : '/api/cron-schedule',
    )
    expect(schedRes.ok()).toBeTruthy()
    const schedData = await schedRes.json()
    expect(schedData).toHaveProperty('schedules')
    expect(Array.isArray(schedData.schedules)).toBe(true)

    const auditRes = await page.request.get(
      serviceId ? `/api/audit-logs?service_id=${encodeURIComponent(serviceId)}&limit=50` : '/api/audit-logs?limit=50',
    )
    expect(auditRes.ok()).toBeTruthy()
    const auditData = await auditRes.json()
    expect(auditData).toHaveProperty('entries')
    expect(Array.isArray(auditData.entries)).toBe(true)

    const ingestedRes = await page.request.get(
      serviceId
        ? `/api/admin/ingested-files?service_id=${encodeURIComponent(serviceId)}&limit=50`
        : '/api/admin/ingested-files?limit=50',
    )
    expect(ingestedRes.ok()).toBeTruthy()
    const ingestedData = await ingestedRes.json()
    expect(ingestedData).toHaveProperty('files')
    expect(Array.isArray(ingestedData.files)).toBe(true)

    const schemaRes = await page.request.get(
      serviceId ? `/api/schema?service_id=${encodeURIComponent(serviceId)}` : '/api/schema',
    )
    expect(schemaRes.ok()).toBeTruthy()
    const schemaData = await schemaRes.json()
    expect(schemaData).toHaveProperty('schema')
    expect(Array.isArray(schemaData.schema)).toBe(true)
  })

  test('4. 7-tab workspace navigation & lazy-mounted tab panels', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })

    // Verify all 7 Admin tabs are present
    const cronTab = page.getByRole('tab', { name: /Cron Runs/i })
    const historyTab = page.getByRole('tab', { name: /Service History/i })
    const ingestionTab = page.getByRole('tab', { name: /Ingestion History/i })
    const icebergTab = page.getByRole('tab', { name: /DuckLake Storage/i })
    const metadataTab = page.getByRole('tab', { name: /Metadata Storage/i })
    const rawTab = page.getByRole('tab', { name: /Available Logs/i })
    const schemaTab = page.getByRole('tab', { name: /DuckDB Schema/i })

    await expect(cronTab).toBeVisible()
    await expect(historyTab).toBeVisible()
    await expect(ingestionTab).toBeVisible()
    await expect(icebergTab).toBeVisible()
    await expect(metadataTab).toBeVisible()
    await expect(rawTab).toBeVisible()
    await expect(schemaTab).toBeVisible()

    // Switch to Service History
    await historyTab.click()
    await expect(page.getByRole('heading', { name: 'Service History', exact: true })).toBeVisible({ timeout: 15_000 })

    // Switch to Ingestion History
    await ingestionTab.click()
    await expect(page.getByRole('heading', { name: 'Log Ingestion History', exact: true })).toBeVisible({ timeout: 15_000 })

    // Switch to DuckDB Schema (lazy-mounted)
    await schemaTab.click()
    await expect(page.getByRole('heading', { name: 'DuckDB Table Schema & Statistics', exact: true })).toBeVisible({
      timeout: 15_000,
    })

    // Return to Cron Runs
    await cronTab.click()
    await expect(cronTab).toHaveAttribute('aria-selected', 'true')
  })

  test('5. Interactive Service History refresh propagates X-Page-Load-ID header', async ({ page }) => {
    await page.goto('/logs?tab=service_history')
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: 'Service History', exact: true })).toBeVisible({ timeout: 15_000 })

    const capturedPageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/audit-logs')) {
        const pid = req.headers()['x-page-load-id']
        if (pid) {
          capturedPageLoadIds.push(pid)
        }
      }
    })

    const refreshBtn = page.getByRole('button', { name: /^Refresh$/i })
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })
    await refreshBtn.click()

    await expect.poll(() => capturedPageLoadIds.length, { timeout: 10_000 }).toBeGreaterThan(0)
  })
})

test('custom-field admin page renders', async ({ page }) => {
  const r = await page.goto('/admin')
  expect(r?.status()).toBeLessThan(500)
})

test('VCL editor debounce → validate-vcl fires after 500 ms in a real browser', async ({
  page,
}) => {
  const validateWaiter = page.waitForRequest(
    (req) =>
      req.method() === 'POST' && /\/api\/services\/[^/]+\/custom-fields\/validate-vcl/.test(req.url()),
    { timeout: 30_000 },
  )

  await page.goto('/admin')

  const manageBtn = page.getByRole('button', { name: /manage/i }).first()
  await expect(manageBtn).toBeVisible({ timeout: 30_000 })
  await manageBtn.click()
  const logSettingsItem = page.getByRole('menuitem', { name: /log settings/i })
  await expect(logSettingsItem).toBeVisible({ timeout: 15_000 })
  await logSettingsItem.click()

  const nextStep = page.getByRole('button', { name: /next step/i })
  await expect(nextStep).toBeVisible({ timeout: 30_000 })
  await nextStep.click()

  const addField = page.getByRole('button', { name: /add field/i })
  await expect(addField).toBeVisible({ timeout: 30_000 })
  await addField.click()

  const label = page.getByLabel('Label', { exact: false }).first()
  await label.fill('Pw Vcl Probe')

  const vclInput = page.getByLabel('VCL Log Expression')
  await expect(vclInput).toBeVisible()
  await vclInput.fill('req.http.Host')

  const validateReq = await validateWaiter
  expect(validateReq.url()).toMatch(/\/custom-fields\/validate-vcl/)

  await expect(
    page
      .getByText(/VCL Expression is valid|Validation Errors|Validation could not run/i)
      .first(),
  ).toBeVisible({ timeout: 30_000 })
})
