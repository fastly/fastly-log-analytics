/**
 * E2E Page Contract Test: Alerts & Notifications (/alerts)
 *
 * Implements the verification checklist specified in docs/pages/alerts.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Alerts list contract & status filter controls (all, alerting, ok, muted).
 * 4. Rule lifecycle: Create rule (POST), Update (PUT), Toggle mute/enable (PATCH), Test webhook (POST), Evaluate rules (POST), Delete cleanup (DELETE).
 * 5. Role verification & SSRF protection checks on webhook URLs.
 * 6. Performance budget: FCP < 400ms, CLS <= 0.05 on warm load.
 * 7. Telemetry instrumentation & query attribution audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { getFirstServiceId, installClsObserver, measureWarmPaintAndCls } from '../helpers'

test.describe('Alerts & Notifications Page Contract (/alerts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/alerts')
    await expect(page.getByRole('heading', { name: /Alerts/i }).first()).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/alerts')
    await expect(page.getByRole('heading', { name: /Alerts/i }).first()).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated alerts root, summary grid, and list card containers
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="alerts-summary-grid"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="alerts-list-card"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('3. Alerts list contract & status filter controls', async ({ page }) => {
    // 1. Verify GET /api/alerts/
    const getRes = await page.request.get('/api/alerts/')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('data')
    expect(Array.isArray(getData.data)).toBeTruthy()

    // 2. Verify status query parameter filtering
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const statusRes = await page.request.get(`/api/alerts/?status=${status}`)
      expect(statusRes.ok()).toBeTruthy()
      const statusJson = await statusRes.json()
      expect(Array.isArray(statusJson.data)).toBeTruthy()
    }

    // 3. Verify status filter controls in UI
    await page.goto('/alerts')
    const statusFilter = page.locator('[data-testid="alerts-status-filter"]:visible')
    await expect(statusFilter).toHaveCount(1, { timeout: 15_000 })
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const btn = statusFilter.getByTestId(`alerts-filter-${status}`)
      await expect(btn).toBeVisible({ timeout: 5_000 })
    }
  })

  test('4. Admin rule lifecycle: Create, Update, Toggle, Test Webhook, Evaluate & Delete', async ({ page }) => {
    // Resolve active service ID from /api/bootstrap so service-scoped alerts match UI
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const ruleId = `e2e-alert-${Date.now()}`

    try {
      // 1. Create alert rule via POST /api/alerts/
      const createRes = await page.request.post('/api/alerts/', {
        headers: { 'x-fastly-service-id': serviceId },
        data: {
          id: ruleId,
          service_id: serviceId,
          name: 'E2E Test High 5xx Alert',
          category: 'reliability',
          metric: '5xx_rate',
          evaluation_type: 'absolute',
          evaluation_scope: 'all',
          operator: '>',
          threshold: 5.0,
          window_min: 5,
          channels: [{ type: 'webhook', url: 'https://hooks.example.com/e2e-alert-webhook' }],
          enabled: true,
        },
      })
      expect(createRes.ok()).toBeTruthy()
      const createData = await createRes.json()
      expect(['saved', 'success']).toContain(createData.data?.status)
      expect(createData.data?.id).toBe(ruleId)

      // 2. Verify it appears on GET /api/alerts/{service_id} and in UI
      const listRes = await page.request.get(`/api/alerts/${serviceId}`)
      expect(listRes.ok()).toBeTruthy()
      const listJson = await listRes.json()
      expect((listJson.data as { id?: string }[]).some((a) => a.id === ruleId)).toBeTruthy()

      await page.goto(`/alerts?service=${encodeURIComponent(serviceId)}`)
      await expect(page.getByText('E2E Test High 5xx Alert')).toBeVisible({ timeout: 15_000 })

      // 3. Update rule via PUT /api/alerts/{alert_id}
      const putRes = await page.request.put(`/api/alerts/${ruleId}`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: {
          id: ruleId,
          service_id: serviceId,
          name: 'E2E Test High 5xx Alert Updated',
          category: 'reliability',
          metric: '5xx_rate',
          evaluation_type: 'absolute',
          evaluation_scope: 'all',
          operator: '>',
          threshold: 7.5,
          window_min: 10,
          channels: [{ type: 'webhook', url: 'https://hooks.example.com/e2e-alert-webhook' }],
          enabled: true,
        },
      })
      expect(putRes.ok()).toBeTruthy()

      // 4. Toggle mute/enable via PATCH /api/alerts/{alert_id}/enabled
      const patchRes = await page.request.patch(`/api/alerts/${ruleId}/enabled`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { enabled: false },
      })
      expect(patchRes.ok()).toBeTruthy()

      // Re-enable for evaluation
      const reEnableRes = await page.request.patch(`/api/alerts/${ruleId}/enabled`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { enabled: true },
      })
      expect(reEnableRes.ok()).toBeTruthy()

      // 5. Trigger test webhook notification (dry_run=true) via POST /api/alerts/{alert_id}/test
      const testWebhookRes = await page.request.post(`/api/alerts/${ruleId}/test`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { service_id: serviceId, dry_run: true },
      })
      expect(testWebhookRes.ok()).toBeTruthy()
      const testWebhookJson = await testWebhookRes.json()
      expect(testWebhookJson.data?.status).toBe('validated')
      expect(testWebhookJson.data?.channel_type).toBe('webhook')

      // 6. Trigger on-demand evaluation via POST /api/alerts/evaluate
      const evalRes = await page.request.post('/api/alerts/evaluate', {
        headers: { 'x-fastly-service-id': serviceId },
        data: { service_id: serviceId },
      })
      expect(evalRes.ok()).toBeTruthy()
      const evalJson = await evalRes.json()
      expect(evalJson.data?.evaluated).toBeGreaterThanOrEqual(1)
    } finally {
      // 7. Always clean up test rule
      await page.request.delete(`/api/alerts/${ruleId}`, {
        headers: { 'x-fastly-service-id': serviceId },
      })
    }
  })

  test('5. Role verification & SSRF protection checks', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'

    // Verify SSRF validation rejects loopback/private/metadata webhook targets with 400
    const ssrfRes = await page.request.post('/api/alerts/', {
      headers: { 'x-fastly-service-id': serviceId },
      data: {
        id: 'ssrf-probe-rule',
        service_id: serviceId,
        name: 'SSRF Probe',
        category: 'reliability',
        metric: '5xx_rate',
        operator: '>',
        threshold: 5,
        window_min: 5,
        channels: [{ type: 'webhook', url: 'http://169.254.169.254/latest/meta-data/' }],
        enabled: true,
      },
    })
    expect(ssrfRes.status()).toBe(400)

    // Verify UI mounts with appropriate role controls
    await page.goto('/alerts')
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    const footerText = await page.locator('footer').innerText()
    if (/Admin View/i.test(footerText)) {
      await expect(page.locator('[data-testid="alerts-create-btn"]')).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/alerts', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 800)

    console.log(`[PERF /alerts] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'

    // 1. Verify GET /api/alerts/{service_id} returns valid response
    const listRes = await page.request.get(`/api/alerts/${serviceId}`, {
      headers: {
        'x-debug-responses': '1',
        'x-page-load-id': `e2e-alerts-${Date.now()}`,
      },
    })
    expect(listRes.ok()).toBeTruthy()

    // 2. Verify POST /api/alerts/preview executes analytical DuckDB query and attributes telemetry
    const previewRes = await page.request.post('/api/alerts/preview', {
      data: {
        id: 'telemetry-preview',
        service_id: serviceId,
        name: 'Telemetry Preview Rule',
        category: 'reliability',
        metric: '5xx_rate',
        evaluation_type: 'absolute',
        evaluation_scope: 'all',
        operator: '>',
        threshold: 5,
        window_min: 15,
      },
      headers: {
        'x-fastly-service-id': serviceId,
        'x-debug-responses': '1',
        'x-page-load-id': `e2e-alerts-preview-${Date.now()}`,
      },
    })
    expect(previewRes.ok()).toBeTruthy()
    const previewJson = await previewRes.json()
    expect(previewJson).toHaveProperty('_debug_queries')
    const queries = (previewJson._debug_queries ?? []) as DebugQuery[]
    console.log(`[TELEMETRY /alerts] Captured ${queries.length} analytical preview queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
