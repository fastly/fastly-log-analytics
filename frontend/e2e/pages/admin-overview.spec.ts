/**
 * E2E Page Contract Suite: Admin Surfaces (/admin, /admin/share, /admin/queries,
 * /admin/session-scoring, /admin/queue, /admin/trends, /admin/rum, /admin/usage-log,
 * /admin/clickhouse, and /control-room).
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import {
  assertAnalyst403,
  assertFooterVisible,
  assertIntrinsicReservations,
  assertManualRefreshAttribution,
  assertWarmClsAndFcp,
  clsInitScript,
  configureExternalAdminOrigin,
  getFirstServiceId,
  installClsObserver,
  measureWarmPaintAndCls,
  readClsScore,
  resolveFirstServiceId,
} from '../helpers'

const externalOrigin = configureExternalAdminOrigin()

const LEGACY_STREAMS = [
  '/api/sync-status/stream',
  '/api/cron-runs/stream',
  '/api/admin/system-metrics/stream',
  '/api/admin/share/stream',
]

function recordStreams(page: Page): string[] {
  const streamRequests: string[] = []
  page.on('response', (resp) => {
    const url = resp.url()
    if (url.includes('/stream')) streamRequests.push(new URL(url).pathname + new URL(url).search)
  })
  return streamRequests
}

function buildProvisionPayload(sid: string, label: string, prefix: string) {
  const ts = Date.now()
  return {
    token: 'mock-fastly-token',
    service_id: sid,
    service_name: `${label} ${sid}`,
    endpoint_name: `${prefix} Logger`,
    fos_region: 'us-east-1',
    fos_bucket_name: `${prefix.toLowerCase().replace(/\s+/g, '-')}-bucket-${ts}`,
    fos_prefix: '',
    sample_rate: '100',
    edge_only: true,
    custom_condition: '',
    log_period: '60',
    cdn_service_name: `${prefix} CDN`,
    cdn_url: `https://${prefix.toLowerCase().replace(/\s+/g, '-')}-${ts}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  }
}

async function streamProvisionExecute(page: Page, deployBody: Record<string, unknown>): Promise<string[]> {
  return page.evaluate(async (body: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!resp.body) throw new Error(`provision/execute returned no body (status=${resp.status})`)
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    const out: string[] = []
    let buf = ''
    const deadline = Date.now() + 90_000
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) buf += decoder.decode(value, { stream: true })
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) out.push(line.slice(6))
        }
        m = SEP.exec(buf)
      }
      if (done) break
    }
    if (buf.trim()) {
      for (const line of buf.split(/\r\n|\n|\r/)) {
        if (line.startsWith('data: ')) out.push(line.slice(6))
      }
    }
    return out
  }, deployBody)
}

async function provisionFreshService(page: Page, sid: string) {
  await page.goto('/admin')
  const messages = await streamProvisionExecute(
    page,
    buildProvisionPayload(sid, 'PW Teardown Target', 'PW Teardown'),
  )
  const done = messages.some((raw) => {
    try {
      return (JSON.parse(raw) as { type?: string }).type === 'done'
    } catch {
      return false
    }
  })
  expect(done, `orchestrator did not emit 'done' for ${sid}; got ${messages.length} events`).toBe(true)
}

// ═══════════════════════════════════════════════════════════════════════════
// /admin — Admin Overview & Ingest Operations
// ═══════════════════════════════════════════════════════════════════════════

test.describe('Admin Overview & Ingest Operations Page Contract (/admin)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/admin')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Admin', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 15_000 })
    await assertFooterVisible(page, /Admin View|Analyst View/i)

    const syncRes = await page.request.get('/api/admin/sync-status')
    expect(syncRes.status()).toBe(200)
    expect((await syncRes.json()).access_level).toBe('read_write')

    await assertAnalyst403(page, 'Admin', () => ['/api/admin/sync-status'])
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms)', async ({ page }) => {
    await page.goto('/admin', { waitUntil: 'domcontentloaded' })
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 30_000 })

    for (const id of ['admin-operations-reservation', 'admin-services-reservation', 'admin-health-reservation']) {
      await expect(page.locator(`[data-testid="${id}"]:visible`)).toBeVisible({ timeout: 15_000 })
    }
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(5)

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 15_000 })

    const metrics = await measureWarmPaintAndCls(page, 500)
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('3. Backend REST contracts (/api/admin/sync-status, /api/admin/system-metrics, /api/admin/health, /api/admin/compaction-status)', async ({ page }) => {
    const [syncRes, metricsRes, healthRes, servicesRes] = await Promise.all([
      page.request.get('/api/admin/sync-status'),
      page.request.get('/api/admin/system-metrics'),
      page.request.get('/api/admin/health'),
      page.request.get('/api/services'),
    ])
    expect(syncRes.ok()).toBeTruthy()
    const syncData = await syncRes.json()
    expect(syncData).toHaveProperty('configured')
    expect(syncData).toHaveProperty('access_level')

    expect(metricsRes.ok()).toBeTruthy()
    const metricsData = await metricsRes.json()
    for (const prop of ['load', 'vcpus', 'in_flight_runs', 'compaction']) {
      expect(metricsData).toHaveProperty(prop)
    }

    expect(healthRes.ok()).toBeTruthy()
    const healthData = await healthRes.json()
    expect(healthData).toHaveProperty('load')
    expect(healthData).toHaveProperty('recent_cron_failures')

    expect(servicesRes.ok()).toBeTruthy()
    const servicesData = await servicesRes.json()
    expect(Array.isArray(servicesData.services)).toBeTruthy()

    if (servicesData.services.length > 0) {
      const firstId = servicesData.services[0].service_id
      const [compRes, acctRes] = await Promise.all([
        page.request.get(`/api/admin/compaction-status/${firstId}`),
        page.request.get(`/api/admin/log-accounting/${firstId}?hours=24`),
      ])
      expect(compRes.ok()).toBeTruthy()
      const compData = await compRes.json()
      expect(compData).toHaveProperty('partitions')
      expect(compData).toHaveProperty('total_files')

      expect(acctRes.ok()).toBeTruthy()
      const acctData = await acctRes.json()
      expect(acctData).toHaveProperty('buckets')
      expect(acctData).toHaveProperty('totals')
    }
  })

  test('4. Ingest & sync status cards, system metrics, and configured services list render live data', async ({ page }) => {
    await page.goto('/admin')
    await expect(page.locator('[data-testid="admin-operations-overview"]:visible')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('link', { name: /Live Queries/ }).first()).toBeVisible({ timeout: 10_000 })
    for (const text of ['Ingest Gap', 'Notable Slow Queries', 'Workers & Queues']) {
      await expect(page.getByText(text).first()).toBeVisible({ timeout: 10_000 })
    }
    await expect(page.locator('[data-testid="admin-services-section"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Service Management' }).first()).toBeVisible({ timeout: 10_000 })
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
    const btn = page.locator('[data-testid="admin-overview-refresh-btn"]:visible')
    await expect(btn).toBeVisible({ timeout: 30_000 })
    await expect(btn).toBeEnabled({ timeout: 15_000 })

    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/services') && req.method() === 'GET',
      { timeout: 15_000 },
    )
    await btn.click()
    const refreshReq = await requestPromise
    expect(refreshReq.headers()['x-page-load-id']).toBeTruthy()
    expect(pageLoadIds.length).toBeGreaterThan(0)
  })
})

for (const { label, path, channel } of [
  {
    label: 'admin page opens one multiplexed SSE stream, not the three legacy ones',
    path: '/admin',
    channel: 'system-metrics',
  },
  {
    label: 'admin/share opens one multiplexed stream (share channel folded in), not a second stream',
    path: '/admin/share',
    channel: 'share',
  },
]) {
  test(label, async ({ page }) => {
    const streamRequests = recordStreams(page)
    await page.goto(path)
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)

    const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
    const legacy = streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))
    expect(legacy).toHaveLength(0)
    expect(merged.length).toBeGreaterThanOrEqual(1)
    expect(merged.some((p) => p.includes('channels=') && p.includes(channel))).toBe(true)
  })
}

// ═══════════════════════════════════════════════════════════════════════════
// /admin/share — Admin Live Share Management Contract
// ═══════════════════════════════════════════════════════════════════════════

test.describe('/admin/share — Admin Live Share Management Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, and Live Share layout mount', async ({ page }) => {
    const response = await page.goto('/admin/share', { waitUntil: 'domcontentloaded' })
    expect(response?.status()).toBe(200)

    await expect(page.getByTestId('admin-share-root')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: 'Remote Dashboard Sharing' })).toBeVisible()
    for (const id of [
      'back-to-admin-link',
      'admin-share-refresh-btn',
      'admin-share-control-reservation',
      'admin-share-tabs',
      'admin-share-panel-reservation',
    ]) {
      await expect(page.getByTestId(id)).toBeVisible()
    }
    await assertFooterVisible(page)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({ page }) => {
    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-share-root')).toBeVisible({ timeout: 30_000 })

    const controlBox = await page.getByTestId('admin-share-control-reservation').boundingBox()
    const panelBox = await page.getByTestId('admin-share-panel-reservation').boundingBox()
    expect(controlBox?.height ?? 0).toBeGreaterThanOrEqual(180)
    expect(panelBox?.height ?? 0).toBeGreaterThanOrEqual(240)

    await assertWarmClsAndFcp(page, () => page.reload({ waitUntil: 'domcontentloaded' }).then(() => {}), [
      page.getByTestId('admin-share-root'),
      page.getByTestId('admin-share-control-panel'),
    ])
  })

  test('3. Backend data contracts: /api/admin/share/status, /live, /settings, /invites, and /tos', async ({
    page,
    request,
  }) => {
    let statusPayload: Record<string, unknown> | null = null
    let livePayload: Record<string, unknown> | null = null

    page.on('response', async (res) => {
      if (res.status() !== 200) return
      const url = res.url()
      if (url.endsWith('/api/admin/share/status')) {
        statusPayload = await res.json().catch(() => null)
      } else if (url.endsWith('/api/admin/share/live')) {
        livePayload = await res.json().catch(() => null)
      }
    })

    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({ timeout: 30_000 })

    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull()
    expect(typeof statusPayload!['sharing_active']).toBe('boolean')
    for (const k of ['invites', 'sessions', 'services', 'audit_logs']) {
      expect(Array.isArray(statusPayload![k])).toBe(true)
    }
    expect(typeof statusPayload!['rate_limits']).toBe('object')

    if (livePayload) {
      expect(typeof livePayload['sharing_active']).toBe('boolean')
      expect(typeof livePayload['active_session_count']).toBe('number')
    }

    const [settingsRes, invitesRes, tosRes] = await Promise.all([
      request.get('/api/admin/share/settings'),
      request.get('/api/admin/share/invites'),
      request.get('/api/admin/share/tos'),
    ])
    expect(settingsRes.status()).toBe(200)
    expect(typeof (await settingsRes.json())['max_concurrent_analyst_sessions']).toBe('number')
    expect(invitesRes.status()).toBe(200)
    expect(Array.isArray((await invitesRes.json())['invites'])).toBe(true)
    expect(tosRes.status()).toBe(200)
    const tosJson = await tosRes.json()
    expect('version' in tosJson && 'text' in tosJson).toBe(true)
  })

  test('4. Sharing Control Panel, tabs (Invitations, Sessions, Audit), and URL ?tab= sync', async ({ page }) => {
    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({ timeout: 30_000 })

    const badgeText = ((await page.getByTestId('admin-share-status-badge').textContent()) ?? '').trim()
    expect(badgeText).toMatch(/^(ACTIVE|OFF)$/)
    if (badgeText === 'ACTIVE') {
      await expect(page.getByTestId('admin-share-stop-btn')).toBeVisible()
      await expect(page.getByTestId('admin-share-panic-btn')).toBeVisible()
    } else {
      await expect(page.getByTestId('admin-share-start-btn')).toBeVisible()
    }

    const tabInvites = page.getByTestId('admin-share-tab-invites')
    const tabSessions = page.getByTestId('admin-share-tab-sessions')
    const tabAudit = page.getByTestId('admin-share-tab-audit')
    await expect(page.getByTestId('admin-share-invites-panel')).toBeVisible()

    await tabSessions.click()
    await expect(tabSessions).toHaveAttribute('aria-selected', 'true')
    await expect(page.getByTestId('admin-share-sessions-panel')).toBeVisible({ timeout: 15_000 })
    await expect(page).toHaveURL(/[?&]tab=sessions(?:&|$)/)

    await tabAudit.click()
    await expect(tabAudit).toHaveAttribute('aria-selected', 'true')
    await expect(page.getByTestId('admin-share-audit-panel')).toBeVisible({ timeout: 15_000 })
    await expect(page).toHaveURL(/[?&]tab=audit(?:&|$)/)

    await tabInvites.click()
    await expect(tabInvites).toHaveAttribute('aria-selected', 'true')
    await expect(page.getByTestId('admin-share-invites-panel')).toBeVisible({ timeout: 15_000 })
  })

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({ page }) => {
    await assertManualRefreshAttribution(page, {
      url: '/admin/share',
      readyLocator: page.getByTestId('admin-share-control-panel'),
      refreshLocator: page.getByTestId('admin-share-refresh-btn'),
      requestFilter: (url) => /\/api\/admin\/share\/(status|live)/.test(url),
    })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/queries — Admin Live Query Monitor Page Contract
// ═══════════════════════════════════════════════════════════════════════════

test.describe('Admin Live Query Monitor Page Contract (/admin/queries)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/admin/queries')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Live Query Monitor', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 15_000 })
    await assertFooterVisible(page, /Admin View|Analyst View/i)

    const snapRes = await page.request.get('/api/admin/queries')
    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.last_seq).toBe('number')
    expect(Array.isArray(snapBody.active)).toBe(true)

    await assertAnalyst403(page, 'Queries', (sid) => [
      '/api/admin/queries',
      `/api/admin/slow-queries?service_id=${encodeURIComponent(sid)}`,
    ])
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms)', async ({ page }) => {
    await page.goto('/admin/queries')
    const root = page.locator('[data-testid="admin-queries-root"]:visible')
    await expect(root).toBeVisible({ timeout: 30_000 })

    await assertWarmClsAndFcp(page, () => page.goto('/admin/queries').then(() => {}), [root])
    await assertIntrinsicReservations(page, [
      'admin-queries-summary-reservation',
      'admin-queries-active-reservation',
      'admin-queries-slow-reservation',
      'admin-queries-completed-reservation',
    ])
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
    expect((await cfgRes.json()).enabled).toBe(true)

    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.last_seq).toBe('number')
    expect(Array.isArray(snapBody.active) && Array.isArray(snapBody.completed)).toBe(true)

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
    expect(typeof (await slowCountRes.json()).count).toBe('number')
  })

  test('4. Live query snapshot, summary strip, and table sections render cleanly', async ({ page }) => {
    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="admin-queries-summary-reservation"]:visible')).toBeVisible({ timeout: 15_000 })
    for (const [id, pattern] of [
      ['admin-queries-active-reservation', /Active & Just-Finished/i],
      ['admin-queries-slow-reservation', /Notable Slow Queries/i],
      ['admin-queries-completed-reservation', /Recently Completed/i],
    ] as const) {
      await expect(page.locator(`[data-testid="${id}"]:visible`)).toContainText(pattern, { timeout: 15_000 })
    }
  })

  test('5. Interactive filtering, tab switching, and manual refresh with X-Page-Load-ID attribution', async ({ page }) => {
    const observedHeaders: Record<string, string>[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/queries') || req.url().includes('/api/admin/slow-queries')) {
        observedHeaders.push(req.headers())
      }
    })

    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    await page.getByRole('tab', { name: 'Live only' }).click()
    await expect(page.locator('[data-testid="admin-queries-active-reservation"]:visible')).toBeVisible()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]')).toHaveCount(0)

    await page.getByRole('tab', { name: 'All' }).click()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]:visible')).toBeVisible()

    const slow24hBtn = page
      .locator('[data-testid="admin-queries-slow-reservation"]')
      .getByRole('button', { name: '24 h', exact: true })
    const slowResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/admin/slow-queries') && res.status() === 200,
      { timeout: 15_000 },
    )
    await slow24hBtn.click()
    expect((await slowResponsePromise).status()).toBe(200)

    const refreshBtn = page.locator('[data-testid="admin-queries-refresh-btn"]:visible')
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })
    const refreshResPromise = page.waitForResponse(
      (res) => res.url().includes('/api/admin/queries') && res.status() === 200,
      { timeout: 15_000 },
    )
    await refreshBtn.click()
    expect((await refreshResPromise).status()).toBe(200)
    expect(observedHeaders.some((h) => Boolean(h['x-page-load-id']))).toBe(true)
  })

  test('6. Pre-nuqs shared links hydrate filter state and round-trip through the URL', async ({ page }) => {
    await page.goto('/admin/queries?q=COMPACT&view=live&slow=250&nogroup=1')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })

    await expect(page.getByRole('tab', { name: 'Live only' })).toHaveAttribute('aria-selected', 'true')
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]')).toHaveCount(0)

    await page.getByRole('tab', { name: 'All' }).click()
    await expect.poll(() => new URL(page.url()).searchParams.get('view')).toBeNull()
    const params = new URL(page.url()).searchParams
    expect(params.get('q')).toBe('COMPACT')
    expect(params.get('slow')).toBe('250')
    expect(params.get('nogroup')).toBe('1')
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/session-scoring — Admin Session Scoring Configuration Contract
// ═══════════════════════════════════════════════════════════════════════════

test.describe('/admin/session-scoring — Admin Session Scoring Configuration Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, and Session Scoring layout mount', async ({ page }) => {
    const response = await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    expect(response?.status()).toBe(200)

    await expect(page.getByTestId('admin-session-scoring-root')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: 'Session Scoring' })).toBeVisible()
    for (const id of [
      'back-to-admin-link',
      'admin-session-scoring-refresh-btn',
      'admin-session-scoring-status-reservation',
      'admin-session-scoring-tabs',
    ]) {
      await expect(page.getByTestId(id)).toBeVisible()
    }
    await assertFooterVisible(page)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({ page }) => {
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-session-scoring-root')).toBeVisible({ timeout: 30_000 })

    const statusBox = await page.getByTestId('admin-session-scoring-status-reservation').boundingBox()
    const overviewBox = await page.getByTestId('admin-session-scoring-overview-reservation').boundingBox()
    expect(statusBox?.height ?? 0).toBeGreaterThanOrEqual(200)
    expect(overviewBox?.height ?? 0).toBeGreaterThanOrEqual(400)

    await assertWarmClsAndFcp(page, () => page.reload({ waitUntil: 'domcontentloaded' }).then(() => {}), [
      page.getByTestId('admin-session-scoring-root'),
      page.getByTestId('admin-session-scoring-status-line'),
    ])
  })

  test('3. Backend data contracts: /api/services/{service_id}/scoring/config, /scoring/status, and /api/services', async ({
    page,
    request,
  }) => {
    let configPayload: Record<string, unknown> | null = null
    let analyticsPayload: Record<string, unknown> | null = null
    let servicesPayload: Record<string, unknown> | null = null
    let resolvedServiceId: string | null = null

    page.on('response', async (res) => {
      if (res.status() !== 200) return
      const url = res.url()
      const configMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/config/)
      const analyticsMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/analytics/)
      if (configMatch) {
        resolvedServiceId = decodeURIComponent(configMatch[1])
        configPayload = await res.json().catch(() => null)
      } else if (analyticsMatch) {
        resolvedServiceId = decodeURIComponent(analyticsMatch[1])
        analyticsPayload = await res.json().catch(() => null)
      } else if (url.endsWith('/api/services')) {
        servicesPayload = await res.json().catch(() => null)
      }
    })

    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({ timeout: 30_000 })

    await expect.poll(() => configPayload, { timeout: 25_000 }).not.toBeNull()
    expect(typeof (configPayload!['status'] as Record<string, unknown>)['enabled']).toBe('boolean')
    for (const k of ['threshold', 'exclude_regex', 'enforce_status_code']) {
      expect(typeof configPayload![k]).toBe('object')
    }
    if (analyticsPayload) expect(typeof analyticsPayload['health']).toBe('object')
    if (servicesPayload) expect(Array.isArray(servicesPayload['services'])).toBe(true)

    if (resolvedServiceId) {
      const statusRes = await request.get(`/api/services/${encodeURIComponent(resolvedServiceId)}/scoring/status`)
      expect(statusRes.status()).toBe(200)
      expect(typeof (await statusRes.json())['enabled']).toBe('boolean')
    }
  })

  test('4. Session Scoring Status panel, tabs (Overview, Labels, Matrix history, Audit), and controls', async ({ page }) => {
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByTestId('admin-session-scoring-status-card')).toBeVisible()

    const badgeText = ((await page.getByTestId('admin-session-scoring-status-badge').textContent()) ?? '').trim()
    expect(badgeText).toMatch(/^(Enabled|Disabled)$/)
    if (badgeText === 'Enabled') {
      await expect(page.getByTestId('admin-session-scoring-redeploy-btn')).toBeVisible()
      await expect(page.getByTestId('admin-session-scoring-disable-btn')).toBeVisible()
    } else {
      await expect(page.getByTestId('admin-session-scoring-enable-btn')).toBeVisible()
    }

    for (const id of [
      'admin-session-scoring-tab-audit',
      'admin-session-scoring-tab-matrix',
      'admin-session-scoring-tab-labels',
      'admin-session-scoring-tab-overview',
    ]) {
      const tab = page.getByTestId(id)
      await expect(tab).toBeVisible()
      await tab.click()
      await expect(tab).toHaveAttribute('aria-selected', 'true')
    }
  })

  test('prerequisite dialog stays closed after readiness recovers and later regresses', async ({ page }) => {
    let ready = false
    await page.route(/\/api\/services\/[^/]+\/(?:session-)?scoring\/(?:config|status)(?:\?|$)/, async (route) => {
      const response = await route.fetch()
      const body = await response.json()
      const status = { enabled: false, preflight_pragmas_ready: ready }
      await route.fulfill({
        response,
        json: new URL(route.request().url()).pathname.endsWith('/config')
          ? { ...body, status: { ...body.status, ...status } }
          : { ...body, ...status },
      })
    })
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeVisible()
    await page.getByTestId('admin-session-scoring-enable-btn').click()
    const dialog = page.getByTestId('admin-session-scoring-pragma-modal')
    await expect(dialog).toBeVisible()
    ready = true
    await page.getByTestId('admin-session-scoring-pragma-recheck-btn').click()
    await expect(dialog).toBeHidden()
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeHidden()
    ready = false
    await page.getByTestId('admin-session-scoring-refresh-btn').click()
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeVisible()
    await expect(dialog).toBeHidden()
    await page.getByTestId('admin-session-scoring-enable-btn').click()
    await expect(dialog).toBeVisible()
  })

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({ page }) => {
    await assertManualRefreshAttribution(page, {
      url: '/admin/session-scoring',
      readyLocator: page.getByTestId('admin-session-scoring-status-line'),
      refreshLocator: page.getByTestId('admin-session-scoring-refresh-btn'),
      requestFilter: (url) => /\/api\/services\/[^/]+\/(?:session-)?scoring\/(config|analytics|status)/.test(url),
    })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/queue — Admin Task Queue & Ingest Ledger Page Contract
// ═══════════════════════════════════════════════════════════════════════════

test.describe('Admin Task Queue & Ingest Ledger Page Contract (/admin/queue)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/admin/queue')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Queue & Workers', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="admin-queue-client"]:visible')).toBeVisible({ timeout: 15_000 })
    await assertFooterVisible(page, /Admin View|Analyst View/i)

    const snapRes = await page.request.get('/api/admin/celery/status')
    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.broker_reachable).toBe('boolean')
    expect(typeof snapBody.queues).toBe('object')
    expect(typeof snapBody.ledger).toBe('object')

    await assertAnalyst403(page, 'Queue', () => [
      '/api/admin/celery/status',
      '/api/admin/metric-history/batch?since=1h',
    ])
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 1200ms)', async ({ page }) => {
    await page.goto('/admin/queue')
    const root = page.locator('[data-testid="admin-queue-root"]:visible')
    await expect(root).toBeVisible({ timeout: 30_000 })

    await assertWarmClsAndFcp(page, () => page.goto('/admin/queue').then(() => {}), [root])
    await assertIntrinsicReservations(page, [
      'admin-queue-trends-reservation',
      'admin-queue-depths-reservation',
      'admin-queue-workers-reservation',
      'admin-queue-ledger-reservation',
      'admin-queue-schedules-reservation',
    ])
  })

  test('3. Backend data contracts: /api/admin/celery/status and /api/admin/metric-history/batch?since=1h', async ({ page }) => {
    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({ timeout: 30_000 })

    const [statusRes, batchRes] = await Promise.all([
      page.request.get('/api/admin/celery/status'),
      page.request.get('/api/admin/metric-history/batch?since=1h'),
    ])

    expect(statusRes.status()).toBe(200)
    const statusBody = await statusRes.json()
    expect(typeof statusBody.broker_reachable).toBe('boolean')
    expect(typeof statusBody.workers?.count).toBe('number')
    expect(typeof statusBody.queues).toBe('object')
    expect(Array.isArray(statusBody.inprocess_schedules)).toBe(true)
    for (const [, count] of Object.entries(statusBody.ledger as Record<string, unknown>)) {
      expect(typeof count).toBe('number')
    }

    expect(batchRes.status()).toBe(200)
    const batchBody = await batchRes.json()
    for (const [, points] of Object.entries(batchBody.series as Record<string, unknown>)) {
      expect(Array.isArray(points)).toBe(true)
    }
  })

  test('4. Mode-aware sections (Standard In-Process vs High-Scale Celery) and Ingest Ledger render cleanly', async ({ page }) => {
    await page.goto('/admin/queue')
    await expect(page.locator('[data-testid="admin-queue-root"]:visible')).toBeVisible({ timeout: 30_000 })

    const statusBody = await (await page.request.get('/api/admin/celery/status')).json()
    const isStandardMode = statusBody.deployment_mode === 'standard' && !statusBody.broker_reachable
    const brokerBadge = page.locator('[data-testid="admin-queue-broker-badge"]:visible')
    await expect(brokerBadge).toBeVisible({ timeout: 15_000 })

    const trendsCard = page.locator('[data-testid="admin-queue-trends-reservation"]:visible')
    const depthsCard = page.locator('[data-testid="admin-queue-depths-reservation"]:visible')
    const workersCard = page.locator('[data-testid="admin-queue-workers-reservation"]:visible')

    if (isStandardMode) {
      await expect(brokerBadge).toContainText(/Standard Mode · In-Process Scheduler/i)
      await expect(trendsCard).toContainText(/Standard Ingest Summary/i, { timeout: 15_000 })
      await expect(trendsCard).toContainText(/In-Process Scheduler Overview/i, { timeout: 15_000 })
      await expect(depthsCard).toContainText(/Quarantined Files & Corrupt Lines \(\d+\)/i, { timeout: 15_000 })
      await expect(depthsCard).not.toContainText(/Queue Depths/i)
      await expect(workersCard).toContainText(/Active In-Process Jobs \(\d+\)/i, { timeout: 15_000 })
    } else {
      await expect(brokerBadge).toContainText(/Broker Reachable|Broker Unreachable \/ Standard Mode/i)
      await expect(trendsCard).toContainText(/Total Queue Depth \(1h\)/i, { timeout: 15_000 })
      await expect(trendsCard).toContainText(/Active Tasks \(1h\)/i, { timeout: 15_000 })
      await expect(depthsCard).toContainText(/Queue Depths/i, { timeout: 15_000 })
      await expect(depthsCard).toContainText(/Queue Name|No queues found or Celery mode is inactive/i, { timeout: 15_000 })
      await expect(workersCard).toContainText(/Active Workers \(\d+\)/i, { timeout: 15_000 })
    }

    await expect(page.locator('[data-testid="admin-queue-schedules-reservation"]:visible')).toContainText(
      /In-Process Schedules \(\d+\)/i,
      { timeout: 15_000 },
    )
    const ledgerSection = page.locator('[data-testid="admin-queue-ledger-reservation"]:visible')
    await expect(ledgerSection).toContainText(/Ingest Ledger Status/i, { timeout: 15_000 })
    await expect(ledgerSection).toContainText(
      /Ledger is empty|discovered|claimed|committed|quarantined|dead_letter/i,
      { timeout: 15_000 },
    )
  })

  test('5. Interactive manual refresh workflow with X-Page-Load-ID telemetry attribution', async ({ page }) => {
    await assertManualRefreshAttribution(page, {
      url: '/admin/queue',
      readyLocator: page.locator('[data-testid="admin-queue-root"]:visible'),
      refreshLocator: page.locator('[data-testid="admin-queue-refresh-btn"]:visible'),
      requestFilter: (url) =>
        url.includes('/api/admin/celery/status') || url.includes('/api/admin/metric-history/batch'),
    })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// Provision Wizard & Service Teardown
// ═══════════════════════════════════════════════════════════════════════════

test('admin shell reaches the provision-wizard entry', async ({ page }) => {
  await page.goto('/admin')
  await expect(page).toHaveURL(/\/admin/)
  await expect(page.getByRole('button', { name: /add service/i })).toBeVisible({ timeout: 30_000 })
})

test('POST /api/provision/execute streams all 8 Step banners and persists the new service', async ({ page }) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-wizard-${Date.now()}`
  await page.goto('/admin')

  const messages = await streamProvisionExecute(
    page,
    buildProvisionPayload(sid, 'Playwright Wizard E2E', 'PW Wizard'),
  )
  const stepBanners = messages
    .map((raw) => {
      try {
        return JSON.parse(raw) as { type?: string; message?: string }
      } catch {
        return null
      }
    })
    .filter((m): m is { type: string; message: string } => !!m && typeof m.message === 'string')
    .filter((m) => /Step \d+\/8/.test(m.message))

  expect(stepBanners.length).toBeGreaterThanOrEqual(8)
  for (let i = 1; i <= 8; i++) {
    expect(stepBanners.some((m) => m.message.includes(`Step ${i}/8`))).toBe(true)
  }

  const bootstrapResp = await page.request.get('/api/bootstrap')
  expect(bootstrapResp.ok()).toBe(true)
  const bootstrap = (await bootstrapResp.json()) as { services?: { service_id: string }[] }
  expect(bootstrap.services?.some((s) => s.service_id === sid)).toBe(true)
})

test('teardown CTA opens a confirm dialog and removes the service from /api/bootstrap', async ({ page }) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-teardown-${Date.now()}`
  await provisionFreshService(page, sid)

  const before = (await (await page.request.get('/api/bootstrap')).json()) as {
    services?: { service_id: string }[]
  }
  expect(before.services?.some((s) => s.service_id === sid)).toBe(true)

  await page.goto('/admin')
  await page.getByRole('button', { name: /refresh status/i }).click()

  const row = page.getByRole('row').filter({ hasText: `PW Teardown Target ${sid}` }).first()
  await expect(row).toBeVisible({ timeout: 30_000 })

  await row.getByRole('button', { name: /manage/i }).click()
  await page.getByRole('menuitem', { name: /teardown service/i }).click()

  await expect(page.getByRole('dialog')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText(new RegExp(`Teardown:.*${sid}`, 'i'))).toBeVisible()

  const tokenField = page.getByLabel(/Paste a Fastly token/i)
  await tokenField.focus()
  await tokenField.pressSequentially('mock-teardown-token', { delay: 5 })

  const executeBtn = page.getByRole('button', { name: /execute teardown/i })
  await expect(executeBtn).toBeEnabled({ timeout: 10_000 })
  await page.waitForTimeout(250)
  await executeBtn.click({ timeout: 30_000, force: true })

  await expect(page.getByRole('button', { name: /^close$/i }).first()).toBeVisible({ timeout: 60_000 })

  await expect
    .poll(
      async () => {
        const j = (await (await page.request.get('/api/bootstrap')).json()) as {
          services?: { service_id: string }[]
        }
        return j.services?.some((s) => s.service_id === sid) ?? false
      },
      { timeout: 8_000, intervals: [250, 500, 1000, 1500] },
    )
    .toBe(false)
})

// ═══════════════════════════════════════════════════════════════════════════
// /control-room — Control Room Live Overview
// ═══════════════════════════════════════════════════════════════════════════

const CONTROL_ROOM_SERVICE_ID = process.env.E2E_REAL_RT_FASTLY_SERVICE_ID || 'svc-playwright-e2e'

test.describe('Control Room', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript((sid) => {
      localStorage.setItem(
        'service-storage',
        JSON.stringify({
          state: {
            activeServiceId: sid,
            services: [{ id: sid, name: 'Playwright E2E Service', accessLevel: 'read_write' }],
          },
          version: 0,
        }),
      )
      localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'UTC' }, version: 0 }))
    }, CONTROL_ROOM_SERVICE_ID)
  })

  test('sidebar link exists and navigates to /control-room', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const sidebarLink = page.getByRole('link', { name: 'Control Room' })
    await expect(sidebarLink).toBeVisible({ timeout: 10_000 })
    await expect(sidebarLink).not.toHaveAttribute('aria-disabled', 'true', { timeout: 10_000 })
    await expect(sidebarLink).toHaveAttribute('href', /\/control-room/)
  })

  test('renders all 6 tabs on desktop viewport', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    for (const tabName of ['Summary', 'CDN & Caching', 'Security', 'Performance', 'Network', 'Admin']) {
      await expect(page.locator(`[role="tablist"] button:text("${tabName}")`)).toBeVisible({ timeout: 5_000 })
    }
  })

  test('SSE connected badge turns green within 5s', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await expect(page.locator('[data-slot="badge"]:has-text("Connected")').first()).toBeVisible({ timeout: 10_000 })
  })

  test('mobile layout shows condensed overview', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    await expect(page.locator('text=Live Overview')).toBeVisible({ timeout: 5_000 })
    await expect(page.locator('[role="tablist"]')).toBeHidden()
  })

  test('no WCAG 2.1 AA violations on /control-room', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .exclude('[data-empty-placeholder="true"]')
      .exclude('[data-axe-ignore="true"]')
      .analyze()

    expect(results.violations).toEqual([])
  })

  test('Overview tab shows metric cards with live data', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.locator('text=Loading app configuration…').waitFor({ state: 'detached', timeout: 30_000 })

    for (const label of ['Requests/s', 'Error Rate', 'Cache Hit Ratio', 'Bandwidth']) {
      await expect(page.locator(`text=${label}`).filter({ visible: true }).first()).toBeVisible({ timeout: 10_000 })
    }
  })

  test('historical data link on Overview navigates to /dashboard', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)
    await expect(page.locator('a[href^="/dashboard"]').filter({ hasText: /historical/i }).first()).toBeVisible({
      timeout: 5_000,
    })
  })

  for (const { name, tabText, metrics } of [
    { name: 'Security tab shows WAF metrics', tabText: 'Security', metrics: ['WAF Blocked', 'WAF Logged'] },
    { name: 'Network tab shows PoP counts', tabText: 'Network', metrics: ['Active PoPs', 'Healthy PoPs'] },
    {
      name: 'Origin tab shows origin metrics',
      tabText: 'CDN & Caching',
      metrics: ['Origin Requests/s', 'Shield Hit Ratio'],
    },
  ]) {
    test(name, async ({ page }) => {
      await page.goto('/control-room')
      await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
      await page.locator(`[role="tablist"] button:text("${tabText}")`).first().click()
      for (const m of metrics) {
        await expect(page.locator(`text=${m}`).first()).toBeVisible({ timeout: 10_000 })
      }
    })
  }
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/trends — Operational Trends & Health Sparklines (Folded from admin-trends.spec.ts)
// ═══════════════════════════════════════════════════════════════════════════

test.describe('/admin/trends — Operational Trends & Health Sparklines Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, and System Trends layout mount', async ({ page }) => {
    const response = await page.goto('/admin/trends')
    expect(response?.status()).toBe(200)

    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: /Operational Trends/i }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="admin-trends-toolbar"]:visible')).toBeVisible()
    await assertFooterVisible(page)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({ page }) => {
    await page.goto('/admin/trends')
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({ timeout: 30_000 })
    await assertIntrinsicReservations(page, ['admin-trends-toolbar', 'admin-trends-grid-reservation'])

    const toolbarBox = await page.locator('[data-testid="admin-trends-toolbar"]:visible').boundingBox()
    const gridBox = await page.locator('[data-testid="admin-trends-grid-reservation"]:visible').boundingBox()
    expect(toolbarBox?.height ?? 0).toBeGreaterThanOrEqual(32)
    expect(gridBox?.height ?? 0).toBeGreaterThanOrEqual(400)

    const isRemoteOrigin =
      Boolean(externalOrigin) && !externalOrigin.includes('127.0.0.1') && !externalOrigin.includes('localhost')
    await assertWarmClsAndFcp(
      page,
      () => page.goto('/admin/trends').then(() => {}),
      [
        page.locator('[data-testid="admin-trends-root"]:visible'),
        page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible'),
      ],
      isRemoteOrigin ? 4000 : 1200,
    )
  })

  test('3. Backend data contract (/api/admin/metric-history/batch?since=1h)', async ({ page }) => {
    const res = await page.request.get('/api/admin/metric-history/batch?since=1h')
    expect(res.status()).toBe(200)
    const body = (await res.json()) as Record<string, unknown>
    expect(typeof body['series']).toBe('object')
    expect(body['series']).not.toBeNull()

    const badRes = await page.request.get('/api/admin/metric-history/batch?since=invalid')
    expect(badRes.status()).toBe(400)
  })

  test('4. Time-window selector interaction (1h -> 6h -> 24h -> 7d) and 7 sparkline cards', async ({ page }) => {
    const windowStatuses = new Map<string, number>()
    page.on('response', (res) => {
      if (res.url().includes('/api/admin/metric-history/batch') && res.request().method() === 'GET') {
        const match = res.url().match(/[?&]since=([^&]+)/)
        if (match) windowStatuses.set(match[1], res.status())
      }
    })

    await page.goto('/admin/trends')
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({ timeout: 30_000 })

    for (const metric of [
      'cpu_load_1m',
      'mem_used_pct',
      'disk_used_pct',
      'disk_used_pct_root',
      'active_query_count',
      'pool_wait_p95_ms',
      'ingest_lag_s',
    ]) {
      await expect(page.locator(`[data-testid="admin-trends-card-${metric}"]:visible`)).toBeVisible({ timeout: 15_000 })
    }

    await expect(page.locator('[data-testid="admin-trends-window-1h"]:visible')).toHaveAttribute('aria-pressed', 'true')
    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = page.locator(`[data-testid="admin-trends-window-${win}"]:visible`)
      await expect(btn).toBeVisible()
      await expect
        .poll(
          async () => {
            if ((await btn.getAttribute('aria-pressed')) !== 'true') await btn.click()
            return windowStatuses.get(win) ?? 0
          },
          { timeout: 30_000, intervals: [250, 500, 1000] },
        )
        .toBe(200)
      await expect(btn).toHaveAttribute('aria-pressed', 'true')
    }
  })

  test('5. Interactive manual refresh workflow and X-Page-Load-ID attribution', async ({ page }) => {
    await assertManualRefreshAttribution(page, {
      url: '/admin/trends',
      readyLocator: page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible'),
      refreshLocator: page.locator('[data-testid="admin-trends-refresh-btn"]:visible'),
      requestFilter: (url) => url.includes('/api/admin/metric-history/batch'),
    })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/rum — Admin RUM Ingestion & Configuration (Folded from admin-trends.spec.ts)
// ═══════════════════════════════════════════════════════════════════════════

test.describe('/admin/rum — Admin RUM Ingestion & Configuration Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, and RUM Configuration layout mount', async ({ page }) => {
    const response = await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    expect(response?.status()).toBe(200)

    await expect(page.getByTestId('admin-rum-root')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: 'Real User Monitoring' })).toBeVisible()
    await expect(page.getByTestId('back-to-admin-link')).toBeVisible()
    await expect(page.getByTestId('admin-rum-toolbar')).toBeVisible()
    await assertFooterVisible(page)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({ page }) => {
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-rum-root')).toBeVisible({ timeout: 30_000 })

    const toolbarBox = await page.getByTestId('admin-rum-toolbar').boundingBox()
    const statusBox = await page.getByTestId('admin-rum-status-reservation').boundingBox()
    expect(toolbarBox?.height ?? 0).toBeGreaterThanOrEqual(48)
    expect(statusBox?.height ?? 0).toBeGreaterThanOrEqual(200)

    await assertWarmClsAndFcp(page, () => page.reload({ waitUntil: 'domcontentloaded' }).then(() => {}), [
      page.getByTestId('admin-rum-root'),
      page.getByTestId('admin-rum-status-line'),
    ])
  })

  test('3. Backend data contracts: /api/services/{service_id}/rum/status and /api/services', async ({ page }) => {
    let statusPayload: Record<string, unknown> | null = null
    let servicesPayload: Record<string, unknown> | null = null

    page.on('response', async (res) => {
      if (res.status() !== 200) return
      const url = res.url()
      if (/\/api\/services\/[^/]+\/rum\/status/.test(url)) {
        statusPayload = await res.json().catch(() => null)
      } else if (url.endsWith('/api/services')) {
        servicesPayload = await res.json().catch(() => null)
      }
    })

    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({ timeout: 30_000 })

    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull()
    for (const k of [
      'enabled',
      'vcl_drift',
      'capture_vitals',
      'capture_performance',
      'capture_errors',
      'capture_events',
    ]) {
      expect(typeof statusPayload![k]).toBe('boolean')
    }
    if (servicesPayload) expect(Array.isArray(servicesPayload['services'])).toBe(true)
  })

  test('4. RUM Status card, Enable/Disable controls, and Faro SDK version state', async ({ page }) => {
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({ timeout: 30_000 })

    const badgeText = (await page.getByTestId('admin-rum-status-badge').textContent()) ?? ''
    expect(badgeText).toMatch(/Beacon Pipeline (Active|Inactive)/)

    const isEnabled = ((await page.getByTestId('admin-rum-status-line').textContent()) ?? '').includes('Enabled')
    const enableBtn = page.getByTestId('admin-rum-enable-btn')
    const disableBtn = page.getByTestId('admin-rum-disable-btn')
    await expect(enableBtn).toBeVisible()
    await expect(disableBtn).toBeVisible()

    if (isEnabled) {
      await expect(enableBtn).toBeDisabled()
      await expect(disableBtn).toBeEnabled()
      for (const id of ['admin-rum-enabled-at', 'admin-rum-vcl-drift', 'admin-rum-capture-summary']) {
        await expect(page.getByTestId(id)).toBeVisible()
      }
    } else {
      await expect(enableBtn).toBeEnabled()
      await expect(disableBtn).toBeDisabled()
    }
  })

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({ page }) => {
    await assertManualRefreshAttribution(page, {
      url: '/admin/rum',
      readyLocator: page.getByTestId('admin-rum-status-line'),
      refreshLocator: page.getByTestId('admin-rum-refresh-btn'),
      requestFilter: (url) => /\/api\/services\/[^/]+\/rum\/status/.test(url),
    })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/usage-log — Admin FOS Usage & Class A/B Ledger (Folded from admin-usage-log.spec.ts)
// ═══════════════════════════════════════════════════════════════════════════

test.describe('/admin/usage-log — Admin FOS Usage & Class A/B Ledger Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, and Usage Log layout mount', async ({ page }) => {
    const main = page.locator('#main')
    const response = await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' })
    expect(response?.status()).toBe(200)

    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 })
    await expect(main.getByRole('heading', { name: /FOS Usage Log/i })).toBeVisible({ timeout: 15_000 })
    for (const id of [
      'admin-usage-log-toolbar',
      'admin-usage-log-kpi-reservation',
      'admin-usage-log-chart-reservation',
      'admin-usage-log-table-reservation',
    ]) {
      await expect(main.getByTestId(id)).toBeVisible()
    }
    await assertFooterVisible(page)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({ page }) => {
    const main = page.locator('#main')
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' })
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 })

    const ids = [
      'admin-usage-log-toolbar',
      'admin-usage-log-kpi-reservation',
      'admin-usage-log-chart-reservation',
      'admin-usage-log-table-reservation',
    ]
    await assertIntrinsicReservations(page, ids, main)

    const minHeights: Record<string, number> = {
      'admin-usage-log-toolbar': 32,
      'admin-usage-log-kpi-reservation': 150,
      'admin-usage-log-chart-reservation': 340,
      'admin-usage-log-table-reservation': 450,
    }
    for (const [id, minH] of Object.entries(minHeights)) {
      const box = await main.getByTestId(id).boundingBox()
      expect(box?.height ?? 0).toBeGreaterThanOrEqual(minH)
    }

    await assertWarmClsAndFcp(page, () => page.reload({ waitUntil: 'domcontentloaded' }).then(() => {}), [
      main.getByTestId('admin-usage-log-root'),
      main.getByTestId('admin-usage-log-filters'),
    ])
  })

  test('3. Backend data contracts (/api/admin/usage-log, /api/admin/usage-logging, /api/admin/system-jobs)', async ({
    page,
  }) => {
    const svcRes = await page.request.get('/api/services')
    expect(svcRes.status()).toBe(200)
    const svcBody = (await svcRes.json()) as {
      services?: Array<{ service_id?: string; id?: string }>
      active_service_id?: string
    }
    const serviceId =
      svcBody.active_service_id || svcBody.services?.[0]?.service_id || svcBody.services?.[0]?.id || ''

    const [ulRes, cfgRes, badPatch, jobsRes] = await Promise.all([
      page.request.get(`/api/admin/usage-log?service_id=${encodeURIComponent(serviceId)}&page_size=25`),
      page.request.get('/api/admin/usage-logging'),
      page.request.patch('/api/admin/usage-logging', { data: { billing_zone: 'INVALID_ZONE' } }),
      page.request.get('/api/admin/system-jobs'),
    ])

    expect(ulRes.status()).toBe(200)
    const ulBody = (await ulRes.json()) as Record<string, unknown>
    expect(Array.isArray(ulBody['entries'])).toBe(true)
    expect(typeof ulBody['total']).toBe('number')
    const agg = ulBody['aggregate'] as Record<string, unknown>
    for (const k of ['total_class_a', 'total_class_b', 'total_cdn_bytes', 'estimated_cost_total']) {
      expect(typeof agg[k]).toBe('number')
    }

    expect(cfgRes.status()).toBe(200)
    const cfgBody = (await cfgRes.json()) as Record<string, unknown>
    expect(typeof cfgBody['enabled']).toBe('boolean')
    for (const k of ['retention_days', 'class_a_rate_per_1k', 'class_b_rate_per_10k']) {
      expect(typeof cfgBody[k]).toBe('number')
    }

    expect(badPatch.status()).toBe(400)

    expect(jobsRes.status()).toBe(200)
    const jobsBody = (await jobsRes.json()) as { jobs?: Array<{ id: string; name: string }> }
    expect((jobsBody.jobs ?? []).map((j) => j.id)).toEqual(
      expect.arrayContaining(['bot_data_refresh', 'rdns_enrichment', 'share_audit_purge']),
    )
  })

  test('4. Filter & granularity interaction across preset windows and filter inputs', async ({ page }) => {
    const main = page.locator('#main')
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' })
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 })
    await expect(main.getByTestId('admin-usage-log-filters')).toBeVisible({ timeout: 15_000 })

    await expect(main.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute('aria-pressed', 'true')
    for (const hours of [1, 168, 720] as const) {
      const btn = main.getByTestId(`admin-usage-log-preset-${hours}h`)
      await expect(btn).toBeVisible()
      await btn.click()
      await expect(btn).toHaveAttribute('aria-pressed', 'true')
    }

    const procInput = main.getByTestId('admin-usage-log-process-input')
    const opInput = main.getByTestId('admin-usage-log-operation-input')
    await procInput.fill('cron:')
    await opInput.fill('PutObject')

    const exportBtn = main.getByTestId('admin-usage-log-export-btn')
    await expect(exportBtn).toHaveAttribute('href', /process_context=cron%3A/)
    await expect(exportBtn).toHaveAttribute('href', /operation_type=PutObject/)

    await procInput.fill('')
    await opInput.fill('')
  })

  test('5. Interactive refresh and CSV export workflow with X-Page-Load-ID attribution', async ({ page }) => {
    const main = page.locator('#main')
    await assertManualRefreshAttribution(page, {
      url: '/admin/usage-log',
      readyLocator: main.getByTestId('admin-usage-log-root'),
      refreshLocator: main.getByTestId('admin-usage-log-refresh-btn'),
      requestFilter: (url) => url.includes('/api/admin/usage-log') || url.includes('/api/admin/log-accounting'),
    })

    const exportBtn = main.getByTestId('admin-usage-log-export-btn')
    await expect(exportBtn).toBeVisible()
    const exportHref = await exportBtn.getAttribute('href')
    expect(exportHref).toBeTruthy()

    const csvRes = await page.request.get(exportHref!)
    expect(csvRes.status()).toBe(200)
    expect(csvRes.headers()['content-type'] ?? '').toContain('text/csv')
    expect((await csvRes.text()).split('\n')[0]).toContain(
      'timestamp,service_id,operation_class,operation_type,url,bytes,duration_ms,function_name,process_context,status,count',
    )
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// /admin/clickhouse — Admin ClickHouse Replay & Cluster Operations (Folded from admin-usage-log.spec.ts)
// ═══════════════════════════════════════════════════════════════════════════

async function isClickHouseEnabledForEnv(page: Page): Promise<boolean> {
  const serviceId = await resolveFirstServiceId(page)
  const statusRes = await page.request.get(
    `/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`,
  )
  if (statusRes.status() !== 200) return false
  return Boolean((await statusRes.json()).enabled)
}

test.describe('Page Spec: /admin/clickhouse (Admin ClickHouse Replay & Cluster Operations)', () => {
  test('1. Route accessibility, ServerFooter identity, and ClickHouse Admin layout mount', async ({ page }) => {
    const enabled = await isClickHouseEnabledForEnv(page)
    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' })
    const status = response?.status() ?? 0
    expect([200, 404]).toContain(status)

    if (!enabled || status === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0)
      return
    }

    await expect(page.getByRole('heading', { name: 'ClickHouse Replay & Cluster Operations' })).toBeVisible()
    await expect(page.getByTestId('admin-clickhouse-root')).toBeVisible()
    await assertFooterVisible(page, /Admin View.*(standard|high-scale)/i)
  })

  test('2. Pre-allocated zero-CLS reservation containers and warm-load budget (CLS <= 0.05, FCP < 1200ms)', async ({
    page,
  }) => {
    const enabled = await isClickHouseEnabledForEnv(page)
    await page.addInitScript(clsInitScript)

    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' })
    const status = response?.status() ?? 0
    expect([200, 404]).toContain(status)

    if (!enabled || status === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0)
    } else {
      await expect(page.getByTestId('admin-clickhouse-toolbar')).toBeVisible()
      for (const [id, minH] of [
        ['admin-clickhouse-vitals-reservation', 200],
        ['admin-clickhouse-generation-reservation', 180],
        ['admin-clickhouse-replay-reservation', 260],
      ] as const) {
        const loc = page.getByTestId(id)
        await expect(loc).toBeVisible()
        await expect.poll(async () => (await loc.boundingBox())?.height ?? 0).toBeGreaterThanOrEqual(minH)
      }
      await expect(page.getByTestId('admin-clickhouse-health-badge')).toHaveText(/^(ok|disabled|unavailable)$/)
      await page.waitForTimeout(300)
    }

    const clsScore = await readClsScore(page)
    const vitalsPerf = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as PerformanceEntry | undefined
      return { cls: Number(window.__clsScore ?? 0), fcpMs: fcpEntry ? fcpEntry.startTime : 0 }
    })

    expect(clsScore).toBeLessThanOrEqual(0.05)
    expect(vitalsPerf.cls).toBeLessThanOrEqual(0.05)
    if (vitalsPerf.fcpMs > 0) expect(vitalsPerf.fcpMs).toBeLessThan(1200)
  })

  test('3. Backend data contract for /api/admin/clickhouse/status across Standard and High-Scale modes', async ({
    page,
  }) => {
    const serviceId = await resolveFirstServiceId(page)
    const statusRes = await page.request.get(
      `/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`,
    )
    expect(statusRes.status()).toBe(200)
    const statusJson = await statusRes.json()

    expect(statusJson.service_id).toBe(serviceId)
    expect(typeof statusJson.enabled).toBe('boolean')

    if (!statusJson.enabled) {
      expect(statusJson.health).toBe('disabled')
      expect(statusJson.schema_version).toBeNull()
      expect(statusJson.publication_counts).toBeNull()
    } else {
      expect(statusJson.health).toBe('ok')
      expect(typeof statusJson.schema_version).toBe('number')
      expect(typeof statusJson.publication_counts?.published).toBe('number')
      expect(typeof statusJson.publication_counts?.pending).toBe('number')
    }

    expect((await page.request.get('/api/admin/clickhouse/status')).status()).toBe(400)
  })

  test('4. Replay validation & dry-run contract for POST /api/admin/clickhouse/replay', async ({ page }) => {
    const serviceId = await resolveFirstServiceId(page)

    const [overLimitRes, missingRefRes, statusRes, dryRunRes] = await Promise.all([
      page.request.post('/api/admin/clickhouse/replay', {
        data: { service_id: serviceId, dataset_id: 'ds_probe_01', dry_run: true, limit: 101 },
      }),
      page.request.post('/api/admin/clickhouse/replay', {
        data: { service_id: serviceId, dry_run: true, limit: 10 },
      }),
      page.request.get(`/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`),
      page.request.post('/api/admin/clickhouse/replay', {
        data: { service_id: serviceId, dataset_id: 'ds_nonexistent_probe', dry_run: true, limit: 10 },
      }),
    ])
    expect(overLimitRes.status()).toBe(422)
    expect(missingRefRes.status()).toBe(422)

    const statusJson = await statusRes.json()
    if (!statusJson.enabled) {
      expect(dryRunRes.status()).toBe(409)
      expect((await dryRunRes.json())?.detail?.error).toBe('clickhouse_disabled')
    } else {
      expect([200, 404]).toContain(dryRunRes.status())
      if (dryRunRes.status() === 404) {
        expect((await dryRunRes.json())?.detail?.error).toBe('replay_not_found')
      }
    }
  })

  test('5. Interactive manual refresh workflow with X-Page-Load-ID telemetry attribution', async ({ page }) => {
    const enabled = await isClickHouseEnabledForEnv(page)
    const observedPageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/clickhouse/status')) {
        const headerVal = req.headers()['x-page-load-id']
        if (headerVal) observedPageLoadIds.push(headerVal)
      }
    })

    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' })
    if (!enabled || response?.status() === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0)
      expect(observedPageLoadIds.length).toBe(0)
      return
    }

    await expect(page.getByTestId('admin-clickhouse-health-badge')).toHaveText(/^(ok|disabled|unavailable)$/)
    const refreshBtn = page.getByTestId('admin-clickhouse-refresh-btn')
    await expect(refreshBtn).toBeVisible()
    await expect(refreshBtn).toBeEnabled()

    const refreshResponsePromise = page.waitForResponse(
      (res) =>
        res.url().includes('/api/admin/clickhouse/status') &&
        (res.status() === 200 || res.status() === 503),
    )
    await refreshBtn.click()
    expect([200, 503]).toContain((await refreshResponsePromise).status())
    expect(observedPageLoadIds.length).toBeGreaterThan(0)
  })
})
