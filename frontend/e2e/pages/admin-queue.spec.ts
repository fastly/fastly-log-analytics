/**
 * E2E Page Contract Test: Admin Task Queue & Ingest Ledger (/admin/queue)
 *
 * Implements the verification checklist specified in docs/pages/admin/queue.md:
 * 1. Route accessibility (GET /admin/queue returns 200 for Admin; Analyst Path B receives 403 on
 *    /api/admin/celery/status and /api/admin/metric-history/batch), ServerFooter rendering, and
 *    Queue & Workers page layout mounting.
 * 2. Pre-allocated zero-CLS reservation containers ([contain-intrinsic-size:auto_*] and
 *    [content-visibility:auto]) and warm-load performance budget (CLS <= 0.05, FCP < 1200ms).
 * 3. Backend data contracts: /api/admin/celery/status and /api/admin/metric-history/batch?since=1h.
 * 4. Queue depth sparklines, queue depths table, active workers, ingest ledger status cards, and
 *    In-Process schedules sections rendering cleanly across Standard and High-Scale modes.
 * 5. Interactive manual refresh workflow (data-testid="admin-queue-refresh-btn") with
 *    X-Page-Load-ID telemetry attribution.
 */

import { expect, test } from '@playwright/test'
import { adminGatewayContextOptions } from '../../../scripts/lib/admin_gateway_client.cjs'
import { clsInitScript } from '../cls-observer'

test.describe('Admin Task Queue & Ingest Ledger Page Contract (/admin/queue)', () => {
  if (process.env.PLAYWRIGHT_BASE_URL) {
    test.use({
      baseURL: process.env.PLAYWRIGHT_BASE_URL,
      ...adminGatewayContextOptions(process.env.PLAYWRIGHT_BASE_URL),
    })
  }

  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({
    page,
  }) => {
    const res = await page.goto('/admin/queue')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Queue & Workers', exact: true })).toBeVisible({
      timeout: 30_000,
    })
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 15_000,
    })
    await expect(page.locator('[data-testid="admin-queue-client"]:visible')).toBeVisible({
      timeout: 15_000,
    })

    // Verify ServerFooter exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)

    // Verify Admin celery status endpoint returns 200 for Admin session
    const snapRes = await page.request.get('/api/admin/celery/status')
    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.broker_reachable).toBe('boolean')
    expect(typeof snapBody.queues).toBe('object')
    expect(typeof snapBody.ledger).toBe('object')

    // On the isolated E2E harness, also verify Analyst Path B receives 403 on /api/admin/celery/status and /api/admin/metric-history/batch
    if (!process.env.PLAYWRIGHT_BASE_URL) {
      const bootRes = await page.request.get('/api/bootstrap')
      const bootJson = await bootRes.json()
      const serviceId =
        bootJson?.services?.[0]?.id ?? bootJson?.services?.[0]?.service_id ?? 'svc-playwright-e2e'
      const email = `e2e-queue-guard-${Date.now()}@example.com`
      const passcode = 'harbor-cloud-forest-91'
      let inviteId: string | null = null
      try {
        await page.request.post('/api/admin/share/start', {
          data: { public_endpoint: 'https://share-e2e.example.com', forward_port: 13004 },
        })
        const invRes = await page.request.post('/api/admin/share/invites', {
          data: {
            name: 'Queue Guard Analyst',
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

        const analystStatusRes = await page.request.get('/api/admin/celery/status', {
          headers: { 'x-remote-analyst': '1' },
        })
        expect(analystStatusRes.status()).toBe(403)
        const analystStatusBody = await analystStatusRes.json()
        expect(analystStatusBody.error).toBe('admin_only')

        const analystHistoryRes = await page.request.get('/api/admin/metric-history/batch?since=1h', {
          headers: { 'x-remote-analyst': '1' },
        })
        expect(analystHistoryRes.status()).toBe(403)
        const analystHistoryBody = await analystHistoryRes.json()
        expect(analystHistoryBody.error).toBe('admin_only')
      } finally {
        await page.request.post('/api/share/logout').catch(() => {})
        if (inviteId) {
          await page.request.delete(`/api/admin/share/invites/${inviteId}`).catch(() => {})
        }
        await page.request.post('/api/admin/share/stop').catch(() => {})
      }
    }
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 1200ms)', async ({
    page,
  }) => {
    // First visit warms browser cache
    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 30_000,
    })

    // Second (warm) load measures FCP and CLS
    await page.addInitScript(clsInitScript)

    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 15_000,
    })

    // Verify all zero-CLS reservation containers are mounted with contain-intrinsic-size and content-visibility:auto
    const reservations = [
      'admin-queue-trends-reservation',
      'admin-queue-depths-reservation',
      'admin-queue-workers-reservation',
      'admin-queue-ledger-reservation',
      'admin-queue-schedules-reservation',
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
      expect(metrics.fcpMs).toBeLessThan(1200)
    }
  })

  test('3. Backend data contracts: /api/admin/celery/status and /api/admin/metric-history/batch?since=1h', async ({
    page,
  }) => {
    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 30_000,
    })

    const [statusRes, batchRes] = await Promise.all([
      page.request.get('/api/admin/celery/status'),
      page.request.get('/api/admin/metric-history/batch?since=1h'),
    ])

    expect(statusRes.status()).toBe(200)
    const statusBody = await statusRes.json()
    expect(typeof statusBody.broker_reachable).toBe('boolean')
    expect(typeof statusBody.workers).toBe('object')
    expect(typeof statusBody.workers.count).toBe('number')
    expect(typeof statusBody.queues).toBe('object')
    expect(Array.isArray(statusBody.inprocess_schedules)).toBe(true)
    expect(typeof statusBody.ledger).toBe('object')
    for (const [, count] of Object.entries(statusBody.ledger as Record<string, unknown>)) {
      expect(typeof count).toBe('number')
    }

    expect(batchRes.status()).toBe(200)
    const batchBody = await batchRes.json()
    expect(typeof batchBody.series).toBe('object')
    for (const [, points] of Object.entries(batchBody.series as Record<string, unknown>)) {
      expect(Array.isArray(points)).toBe(true)
    }
  })

  test('4. Mode-aware sections (Standard In-Process vs High-Scale Celery) and Ingest Ledger render cleanly', async ({
    page,
  }) => {
    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 30_000,
    })

    const statusRes = await page.request.get('/api/admin/celery/status')
    expect(statusRes.status()).toBe(200)
    const statusBody = await statusRes.json()
    const isStandardMode =
      statusBody.deployment_mode === 'standard' && !statusBody.broker_reachable

    // Broker / Mode connectivity badge is visible
    const brokerBadge = page.locator('[data-testid="admin-queue-broker-badge"]:visible')
    await expect(brokerBadge).toBeVisible({ timeout: 15_000 })

    if (isStandardMode) {
      await expect(brokerBadge).toContainText(/Standard Mode · In-Process Scheduler/i)

      // Standard summary cards
      await expect(
        page.locator('[data-testid="admin-queue-trends-reservation"]:visible'),
      ).toContainText(/Standard Ingest Summary/i, { timeout: 15_000 })
      await expect(
        page.locator('[data-testid="admin-queue-trends-reservation"]:visible'),
      ).toContainText(/In-Process Scheduler Overview/i, { timeout: 15_000 })

      // Quarantined Files & Corrupt Lines card replaces Celery Queue Depths in Standard Mode
      const depthsCard = page.locator('[data-testid="admin-queue-depths-reservation"]:visible')
      await expect(depthsCard).toContainText(/Quarantined Files & Corrupt Lines \(\d+\)/i, {
        timeout: 15_000,
      })
      await expect(depthsCard).not.toContainText(/Queue Depths/i)

      // Active In-Process Jobs card
      await expect(
        page.locator('[data-testid="admin-queue-workers-reservation"]:visible'),
      ).toContainText(/Active In-Process Jobs \(\d+\)/i, { timeout: 15_000 })
    } else {
      await expect(brokerBadge).toContainText(
        /Broker Reachable|Broker Unreachable \/ Standard Mode/i,
      )

      // Sparkline cards
      await expect(
        page.locator('[data-testid="admin-queue-trends-reservation"]:visible'),
      ).toContainText(/Total Queue Depth \(1h\)/i, { timeout: 15_000 })
      await expect(
        page.locator('[data-testid="admin-queue-trends-reservation"]:visible'),
      ).toContainText(/Active Tasks \(1h\)/i, { timeout: 15_000 })

      // Queue Depths card (either table when queues exist or empty fallback message)
      const depthsCard = page.locator('[data-testid="admin-queue-depths-reservation"]:visible')
      await expect(depthsCard).toContainText(/Queue Depths/i, { timeout: 15_000 })
      await expect(depthsCard).toContainText(
        /Queue Name|No queues found or Celery mode is inactive/i,
        { timeout: 15_000 },
      )

      // Active Workers card
      await expect(
        page.locator('[data-testid="admin-queue-workers-reservation"]:visible'),
      ).toContainText(/Active Workers \(\d+\)/i, { timeout: 15_000 })
    }

    // Unified In-Process Schedules card is visible in both modes
    await expect(
      page.locator('[data-testid="admin-queue-schedules-reservation"]:visible'),
    ).toContainText(/In-Process Schedules \(\d+\)/i, { timeout: 15_000 })

    // Ingest Ledger Status card is visible in both modes
    const ledgerSection = page.locator('[data-testid="admin-queue-ledger-reservation"]:visible')
    await expect(ledgerSection).toContainText(/Ingest Ledger Status/i, { timeout: 15_000 })
    await expect(ledgerSection).toContainText(
      /Ledger is empty|discovered|claimed|committed|quarantined|dead_letter/i,
      { timeout: 15_000 },
    )
  })

  test('5. Interactive manual refresh workflow with X-Page-Load-ID telemetry attribution', async ({
    page,
  }) => {
    const observedHeaders: Record<string, string>[] = []
    page.on('request', (req) => {
      if (
        req.url().includes('/api/admin/celery/status') ||
        req.url().includes('/api/admin/metric-history/batch')
      ) {
        observedHeaders.push(req.headers())
      }
    })

    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({
      timeout: 30_000,
    })

    const refreshBtn = page.locator('[data-testid="admin-queue-refresh-btn"]:visible')
    await expect(refreshBtn).toBeVisible({ timeout: 15_000 })

    const refreshResPromise = page.waitForResponse(
      (res) => res.url().includes('/api/admin/celery/status') && res.status() === 200,
      { timeout: 15_000 },
    )
    await refreshBtn.click()
    const refreshRes = await refreshResPromise
    expect(refreshRes.status()).toBe(200)

    const withPageLoadId = observedHeaders.filter((h) => Boolean(h['x-page-load-id']))
    expect(withPageLoadId.length).toBeGreaterThan(0)
  })
})
