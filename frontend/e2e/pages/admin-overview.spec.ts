/**
 * E2E Page Contract Suite: Admin Surfaces (/admin, /admin/share, /admin/queries,
 * /admin/session-scoring, /admin/queue, /admin/trends, /admin/rum, /admin/usage-log,
 * /admin/clickhouse, and /control-room).
 */
import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Locator, type Page } from '@playwright/test'
import {
  assertAnalyst403, assertFooterVisible, assertIntrinsicReservations, assertManualRefreshAttribution,
  assertWarmClsAndFcp, clsInitScript, configureExternalAdminOrigin, getFirstServiceId,
  installClsObserver, measureWarmPaintAndCls, readClsScore, resolveFirstServiceId,
} from '../helpers'

const externalOrigin = configureExternalAdminOrigin()
const LEGACY_STREAMS = ['/api/sync-status/stream', '/api/cron-runs/stream', '/api/admin/system-metrics/stream', '/api/admin/share/stream']

function recordStreams(page: Page): string[] {
  const streamRequests: string[] = []
  page.on('response', (resp) => {
    const url = resp.url()
    if (url.includes('/stream')) streamRequests.push(new URL(url).pathname + new URL(url).search)
  })
  return streamRequests
}

async function assertMinHeights(scope: Page | Locator, pairs: ReadonlyArray<readonly [string, number]>) {
  for (const [id, minH] of pairs) expect((await scope.getByTestId(id).boundingBox())?.height ?? 0).toBeGreaterThanOrEqual(minH)
}

type AdminMountOpts = {
  rootTestId: string; heading: string | RegExp; exactHeading?: boolean; testIds?: readonly string[]; visibleTestIds?: readonly string[]
  intrinsicIds?: readonly string[]; minHeights?: ReadonlyArray<readonly [string, number]>; warmLocators?: readonly Locator[]
  warmMaxFcpMs?: number; warmReload?: boolean; footerPattern?: RegExp; scope?: Locator; domContentLoaded?: boolean
}

async function assertAdminMount(page: Page, url: string, opts: AdminMountOpts) {
  const res = await page.goto(url, opts.domContentLoaded ? { waitUntil: 'domcontentloaded' } : undefined)
  expect(res?.status()).toBe(200)
  const scope = opts.scope ?? page
  if (opts.visibleTestIds) {
    await expect(page.locator(`[data-testid="${opts.rootTestId}"]:visible`)).toBeVisible({ timeout: 30_000 })
    for (const id of opts.visibleTestIds) await expect(page.locator(`[data-testid="${id}"]:visible`)).toBeVisible({ timeout: 15_000 })
  } else {
    await expect(scope.getByTestId(opts.rootTestId)).toBeVisible({ timeout: 30_000 })
    for (const id of opts.testIds ?? []) await expect(scope.getByTestId(id)).toBeVisible()
  }
  await expect(scope.getByRole('heading', { name: opts.heading, exact: opts.exactHeading }).first()).toBeVisible({ timeout: 30_000 })
  await assertFooterVisible(page, opts.footerPattern)
  if (opts.intrinsicIds) await assertIntrinsicReservations(page, [...opts.intrinsicIds], opts.scope)
  if (opts.minHeights) await assertMinHeights(scope, opts.minHeights)
  if (opts.warmLocators) {
    const trigger = opts.warmReload ? () => page.reload({ waitUntil: 'domcontentloaded' }).then(() => {}) : () => page.goto(url).then(() => {})
    await assertWarmClsAndFcp(page, trigger, [...opts.warmLocators], opts.warmMaxFcpMs)
  }
}

function buildProvisionPayload(sid: string, label: string, prefix: string) {
  const ts = Date.now(), slug = prefix.toLowerCase().replace(/\s+/g, '-')
  return {
    token: 'mock-fastly-token', service_id: sid, service_name: `${label} ${sid}`, endpoint_name: `${prefix} Logger`,
    fos_region: 'us-east-1', fos_bucket_name: `${slug}-bucket-${ts}`, fos_prefix: '', sample_rate: '100',
    edge_only: true, custom_condition: '', log_period: '60', cdn_service_name: `${prefix} CDN`,
    cdn_url: `https://${slug}-${ts}.example`, cdn_shield: 'none', enable_cron_sync: false, delete_after: true,
    commit_interval_mins: 5, enable_cron_compact: false, log_retention_days: 30, log_fields: null,
  }
}

async function streamProvisionExecute(page: Page, deployBody: Record<string, unknown>): Promise<string[]> {
  return page.evaluate(async (body: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    if (!resp.body) throw new Error(`provision/execute returned no body (status=${resp.status})`)
    const reader = resp.body.getReader(), decoder = new TextDecoder(), out: string[] = [], deadline = Date.now() + 90_000, SEP = /\r\n\r\n|\n\n|\r\r/
    let buf = ''
    const pushDataLines = (chunk: string) => { for (const line of chunk.split(/\r\n|\n|\r/)) if (line.startsWith('data: ')) out.push(line.slice(6)) }
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) buf += decoder.decode(value, { stream: true })
      for (let m = SEP.exec(buf); m; m = SEP.exec(buf)) { pushDataLines(buf.slice(0, m.index)); buf = buf.slice(m.index + m[0].length) }
      if (done) break
    }
    if (buf.trim()) pushDataLines(buf)
    return out
  }, deployBody)
}

async function provisionFreshService(page: Page, sid: string) {
  await page.goto('/admin')
  const messages = await streamProvisionExecute(page, buildProvisionPayload(sid, 'PW Teardown Target', 'PW Teardown'))
  const done = messages.some((raw) => { try { return (JSON.parse(raw) as { type?: string }).type === 'done' } catch { return false } })
  expect(done, `orchestrator did not emit 'done' for ${sid}; got ${messages.length} events`).toBe(true)
}

test.describe('Admin Overview & Ingest Operations Page Contract (/admin)', () => {
  test('1. Route accessibility, Admin RBAC contract, zero-CLS containers & warm-load performance budget', async ({ page }) => {
    await assertAdminMount(page, '/admin', {
      rootTestId: 'admin-overview-root', heading: 'Admin', exactHeading: true,
      visibleTestIds: ['admin-operations-reservation', 'admin-services-reservation', 'admin-health-reservation'],
      footerPattern: /Admin View|Analyst View/i, domContentLoaded: true,
    })
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(5)
    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    expect(Date.now() - start).toBeLessThan(10_000)
    await expect(page.locator('[data-testid="admin-overview-root"]:visible')).toBeVisible({ timeout: 15_000 })
    expect((await measureWarmPaintAndCls(page, 500)).cls).toBeLessThanOrEqual(0.05)
    const syncRes = await page.request.get('/api/admin/sync-status')
    expect(syncRes.status()).toBe(200)
    expect(await syncRes.json()).toHaveProperty('configured')
    await assertAnalyst403(page, 'Admin', () => ['/api/admin/sync-status'])
  })

  test('3. Backend REST contracts (/api/admin/sync-status, /api/admin/system-metrics, /api/admin/health, /api/admin/compaction-status)', async ({ page }) => {
    const [syncRes, metricsRes, healthRes, servicesRes] = await Promise.all([
      page.request.get('/api/admin/sync-status'), page.request.get('/api/admin/system-metrics'),
      page.request.get('/api/admin/health'), page.request.get('/api/services'),
    ])
    expect(syncRes.ok() && metricsRes.ok() && healthRes.ok() && servicesRes.ok()).toBeTruthy()
    const [syncData, metricsData, healthData, servicesData] = await Promise.all([
      syncRes.json(), metricsRes.json(), healthRes.json(), servicesRes.json(),
    ])
    for (const p of ['configured']) expect(syncData).toHaveProperty(p)
    for (const p of ['load', 'vcpus', 'in_flight_runs', 'compaction']) expect(metricsData).toHaveProperty(p)
    for (const p of ['load', 'recent_cron_failures']) expect(healthData).toHaveProperty(p)
    expect(Array.isArray(servicesData.services)).toBeTruthy()
    if (servicesData.services.length > 0) {
      const firstId = servicesData.services[0].service_id
      const [compRes, acctRes] = await Promise.all([
        page.request.get(`/api/admin/compaction-status/${firstId}`),
        page.request.get(`/api/admin/log-accounting/${firstId}?hours=24`),
      ])
      expect(compRes.ok() && acctRes.ok()).toBeTruthy()
      const [compData, acctData] = await Promise.all([compRes.json(), acctRes.json()])
      for (const p of ['partitions', 'total_files']) expect(compData).toHaveProperty(p)
      for (const p of ['buckets', 'totals']) expect(acctData).toHaveProperty(p)
    }
  })

  test('4. Ingest & sync status cards, system metrics, configured services list, and manual refresh X-Page-Load-ID attribution', async ({ page }) => {
    const pageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/services') || req.url().includes('/api/admin/')) {
        const plid = req.headers()['x-page-load-id']
        if (plid) pageLoadIds.push(plid)
      }
    })
    await page.goto('/admin')
    await expect(page.locator('[data-testid="admin-operations-overview"]:visible')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('link', { name: /Live Queries/ }).first()).toBeVisible({ timeout: 10_000 })
    for (const text of ['Ingest Gap', 'Notable Slow Queries', 'Workers & Queues', 'System Health']) {
      await expect(page.getByText(text).first()).toBeVisible({ timeout: 15_000 })
    }
    await expect(page.locator('[data-testid="admin-services-section"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Service Management' }).first()).toBeVisible({ timeout: 10_000 })
    const btn = page.locator('[data-testid="admin-overview-refresh-btn"]:visible')
    await expect(btn).toBeVisible({ timeout: 30_000 })
    await expect(btn).toBeEnabled({ timeout: 15_000 })
    const requestPromise = page.waitForRequest((req) => req.url().includes('/api/services') && req.method() === 'GET', { timeout: 15_000 })
    await btn.click()
    expect((await requestPromise).headers()['x-page-load-id']).toBeTruthy()
    expect(pageLoadIds.length).toBeGreaterThan(0)
  })
})

for (const { label, path, channel } of [
  { label: 'admin page opens one multiplexed SSE stream, not the three legacy ones', path: '/admin', channel: 'system-metrics' },
  { label: 'admin/share opens one multiplexed stream (share channel folded in), not a second stream', path: '/admin/share', channel: 'share' },
]) {
  test(label, async ({ page }) => {
    const streamRequests = recordStreams(page)
    await page.goto(path)
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)
    const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
    expect(streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))).toHaveLength(0)
    expect(merged.length).toBeGreaterThanOrEqual(1)
    expect(merged.some((p) => p.includes('channels=') && p.includes(channel))).toBe(true)
  })
}

test.describe('/admin/share — Admin Live Share Management Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, Live Share layout mount, zero-CLS reservations & warm-load budget', async ({ page }) => {
    await assertAdminMount(page, '/admin/share', {
      rootTestId: 'admin-share-root', heading: 'Remote Dashboard Sharing', domContentLoaded: true, warmReload: true,
      testIds: ['back-to-admin-link', 'admin-share-refresh-btn', 'admin-share-control-reservation', 'admin-share-tabs', 'admin-share-panel-reservation'],
      minHeights: [['admin-share-control-reservation', 180], ['admin-share-panel-reservation', 240]],
      warmLocators: [page.getByTestId('admin-share-root'), page.getByTestId('admin-share-control-panel')],
    })
  })

  test('3. Backend data contracts: /api/admin/share/status, /live, /settings, /invites, and /tos', async ({ page, request }) => {
    let statusPayload: Record<string, unknown> | null = null, livePayload: Record<string, unknown> | null = null
    page.on('response', async (res) => {
      if (res.status() !== 200) return
      if (res.url().endsWith('/api/admin/share/status')) statusPayload = await res.json().catch(() => null)
      else if (res.url().endsWith('/api/admin/share/live')) livePayload = await res.json().catch(() => null)
    })
    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({ timeout: 30_000 })
    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull()
    expect(typeof statusPayload!['sharing_active']).toBe('boolean')
    for (const k of ['invites', 'sessions', 'services', 'audit_logs']) expect(Array.isArray(statusPayload![k])).toBe(true)
    expect(typeof statusPayload!['rate_limits']).toBe('object')
    if (livePayload) {
      expect(typeof livePayload['sharing_active']).toBe('boolean')
      expect(typeof livePayload['active_session_count']).toBe('number')
    }
    const [settingsRes, invitesRes, tosRes] = await Promise.all([
      request.get('/api/admin/share/settings'), request.get('/api/admin/share/invites'), request.get('/api/admin/share/tos'),
    ])
    expect(settingsRes.status() === 200 && invitesRes.status() === 200 && tosRes.status() === 200).toBe(true)
    expect(typeof (await settingsRes.json())['max_concurrent_analyst_sessions']).toBe('number')
    expect(Array.isArray((await invitesRes.json())['invites'])).toBe(true)
    const tosJson = await tosRes.json()
    expect('version' in tosJson && 'text' in tosJson).toBe(true)
  })

  test('4. Sharing Control Panel, tabs (Invitations, Sessions, Audit), URL ?tab= sync, and manual refresh attribution', async ({ page }) => {
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
    await expect(page.getByTestId('admin-share-invites-panel')).toBeVisible()
    for (const [tabId, panelId, urlRe] of [
      ['admin-share-tab-sessions', 'admin-share-sessions-panel', /[?&]tab=sessions(?:&|$)/],
      ['admin-share-tab-audit', 'admin-share-audit-panel', /[?&]tab=audit(?:&|$)/],
      ['admin-share-tab-invites', 'admin-share-invites-panel', null],
    ] as const) {
      const tab = page.getByTestId(tabId)
      await tab.click()
      await expect(tab).toHaveAttribute('aria-selected', 'true')
      await expect(page.getByTestId(panelId)).toBeVisible({ timeout: 15_000 })
      if (urlRe) await expect(page).toHaveURL(urlRe)
    }
    await assertManualRefreshAttribution(page, {
      url: '/admin/share', readyLocator: page.getByTestId('admin-share-control-panel'),
      refreshLocator: page.getByTestId('admin-share-refresh-btn'),
      requestFilter: (url) => /\/api\/admin\/share\/(status|live)/.test(url),
    })
  })
})

test.describe('Admin Live Query Monitor Page Contract (/admin/queries)', () => {
  test('1. Route accessibility, Admin RBAC contract, zero-CLS containers & warm-load budget', async ({ page }) => {
    await assertAdminMount(page, '/admin/queries', {
      rootTestId: 'admin-queries-root', heading: 'Live Query Monitor', exactHeading: true, visibleTestIds: [],
      footerPattern: /Admin View|Analyst View/i, warmLocators: [page.locator('[data-testid="admin-queries-root"]:visible')],
      intrinsicIds: ['admin-queries-summary-reservation', 'admin-queries-active-reservation', 'admin-queries-slow-reservation', 'admin-queries-completed-reservation'],
    })
    const snapRes = await page.request.get('/api/admin/queries')
    expect(snapRes.status()).toBe(200)
    const snapBody = await snapRes.json()
    expect(typeof snapBody.last_seq === 'number' && Array.isArray(snapBody.active)).toBe(true)
    await assertAnalyst403(page, 'Queries', (sid) => ['/api/admin/queries', `/api/admin/slow-queries?service_id=${encodeURIComponent(sid)}`])
  })

  test('3. Backend data contracts, live query snapshot, summary strip, and table sections', async ({ page }) => {
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
    const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
    const [cfgRes, snapRes, summaryRes, slowRes, slowCountRes] = await Promise.all([
      page.request.get('/api/admin/app-config/query-monitor'),
      page.request.get('/api/admin/queries?include_completed=true&cold_limit=50'),
      page.request.get('/api/admin/queries/summary'),
      page.request.get(`/api/admin/slow-queries?service_id=${encodeURIComponent(serviceId)}&since_hours=24&threshold_ms=100`),
      page.request.get(`/api/admin/slow-queries/count?service_id=${encodeURIComponent(serviceId)}&since_hours=24&threshold_ms=1000`),
    ])
    for (const r of [cfgRes, snapRes, summaryRes, slowRes, slowCountRes]) expect(r.status()).toBe(200)
    expect((await cfgRes.json()).enabled).toBe(true)
    const [snapBody, summaryBody, slowBody, slowCountBody] = await Promise.all([snapRes.json(), summaryRes.json(), slowRes.json(), slowCountRes.json()])
    expect(typeof snapBody.last_seq === 'number' && Array.isArray(snapBody.active) && Array.isArray(snapBody.completed)).toBe(true)
    expect(typeof summaryBody.active_total === 'number' && typeof summaryBody.by_db_type === 'object' && typeof summaryBody.longest_ms === 'number').toBe(true)
    expect(Array.isArray(slowBody.rows) && slowBody.since_hours === 24 && typeof slowCountBody.count === 'number').toBe(true)
  })

  test('5. Interactive filtering, tab switching, and manual refresh with X-Page-Load-ID attribution', async ({ page }) => {
    const observedHeaders: Record<string, string>[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/queries') || req.url().includes('/api/admin/slow-queries')) observedHeaders.push(req.headers())
    })
    await page.goto('/admin/queries')
    await expect(page.locator('[data-testid="admin-queries-root"]:visible')).toBeVisible({ timeout: 30_000 })
    await page.getByRole('tab', { name: 'Live only' }).click()
    await expect(page.locator('[data-testid="admin-queries-active-reservation"]:visible')).toBeVisible()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]')).toHaveCount(0)
    await page.getByRole('tab', { name: 'All' }).click()
    await expect(page.locator('[data-testid="admin-queries-slow-reservation"]:visible')).toBeVisible()

    const slowResponsePromise = page.waitForResponse((res) => res.url().includes('/api/admin/slow-queries') && res.status() === 200, { timeout: 15_000 })
    await page.locator('[data-testid="admin-queries-slow-reservation"]').getByRole('button', { name: '24 h', exact: true }).click()
    expect((await slowResponsePromise).status()).toBe(200)

    const refreshBtn = page.locator('[data-testid="admin-queries-refresh-btn"]:visible')
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })
    const refreshResPromise = page.waitForResponse((res) => res.url().includes('/api/admin/queries') && res.status() === 200, { timeout: 15_000 })
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
    expect([params.get('q'), params.get('slow'), params.get('nogroup')]).toEqual(['COMPACT', '250', '1'])
  })
})

test.describe('/admin/session-scoring — Admin Session Scoring Configuration Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, Session Scoring layout mount, zero-CLS reservations & warm-load budget', async ({ page }) => {
    await assertAdminMount(page, '/admin/session-scoring', {
      rootTestId: 'admin-session-scoring-root', heading: 'Session Scoring', domContentLoaded: true, warmReload: true,
      testIds: ['back-to-admin-link', 'admin-session-scoring-refresh-btn', 'admin-session-scoring-status-reservation', 'admin-session-scoring-tabs'],
      minHeights: [['admin-session-scoring-status-reservation', 200], ['admin-session-scoring-overview-reservation', 400]],
      warmLocators: [page.getByTestId('admin-session-scoring-root'), page.getByTestId('admin-session-scoring-status-line')],
    })
  })

  test('3. Backend data contracts: /api/services/{service_id}/scoring/config, /scoring/status, and /api/services', async ({ page, request }) => {
    let configPayload: Record<string, unknown> | null = null, analyticsPayload: Record<string, unknown> | null = null
    let servicesPayload: Record<string, unknown> | null = null, resolvedServiceId: string | null = null
    page.on('response', async (res) => {
      if (res.status() !== 200) return
      const url = res.url()
      const configMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/config/)
      const analyticsMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/analytics/)
      if (configMatch) { resolvedServiceId = decodeURIComponent(configMatch[1]); configPayload = await res.json().catch(() => null) }
      else if (analyticsMatch) { resolvedServiceId = decodeURIComponent(analyticsMatch[1]); analyticsPayload = await res.json().catch(() => null) }
      else if (url.endsWith('/api/services')) servicesPayload = await res.json().catch(() => null)
    })
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({ timeout: 30_000 })
    await expect.poll(() => configPayload, { timeout: 25_000 }).not.toBeNull()
    expect(typeof (configPayload!['status'] as Record<string, unknown>)['enabled']).toBe('boolean')
    for (const k of ['threshold', 'exclude_regex', 'enforce_status_code']) expect(typeof configPayload![k]).toBe('object')
    if (analyticsPayload) expect(typeof analyticsPayload['health']).toBe('object')
    if (servicesPayload) expect(Array.isArray(servicesPayload['services'])).toBe(true)
    if (resolvedServiceId) {
      const statusRes = await request.get(`/api/services/${encodeURIComponent(resolvedServiceId)}/scoring/status`)
      expect(statusRes.status()).toBe(200)
      expect(typeof (await statusRes.json())['enabled']).toBe('boolean')
    }
  })

  test('4. Session Scoring Status panel, tabs, controls, and manual refresh attribution', async ({ page }) => {
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
    for (const id of ['admin-session-scoring-tab-audit', 'admin-session-scoring-tab-matrix', 'admin-session-scoring-tab-labels', 'admin-session-scoring-tab-overview']) {
      const tab = page.getByTestId(id)
      await expect(tab).toBeVisible()
      await tab.click()
      await expect(tab).toHaveAttribute('aria-selected', 'true')
    }
    await assertManualRefreshAttribution(page, {
      url: '/admin/session-scoring', readyLocator: page.getByTestId('admin-session-scoring-status-line'),
      refreshLocator: page.getByTestId('admin-session-scoring-refresh-btn'),
      requestFilter: (url) => /\/api\/services\/[^/]+\/(?:session-)?scoring\/(config|analytics|status)/.test(url),
    })
  })

  test('prerequisite dialog stays closed after readiness recovers and later regresses', async ({ page }) => {
    let ready = false
    await page.route(/\/api\/services\/[^/]+\/(?:session-)?scoring\/(?:config|status)(?:\?|$)/, async (route) => {
      const response = await route.fetch()
      const body = await response.json()
      const status = { enabled: false, preflight_pragmas_ready: ready }
      await route.fulfill({
        response,
        json: new URL(route.request().url()).pathname.endsWith('/config') ? { ...body, status: { ...body.status, ...status } } : { ...body, ...status },
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
})

test.describe('Admin Task Queue & Ingest Ledger Page Contract (/admin/queue)', () => {
  test('1. Route accessibility, Admin RBAC, zero-CLS containers, warm-load budget & backend contracts', async ({ page }) => {
    await assertAdminMount(page, '/admin/queue', {
      rootTestId: 'admin-queue-root', heading: 'Queue & Workers', exactHeading: true, visibleTestIds: ['admin-queue-client'],
      footerPattern: /Admin View|Analyst View/i, warmLocators: [page.locator('[data-testid="admin-queue-root"]:visible')],
      intrinsicIds: ['admin-queue-trends-reservation', 'admin-queue-depths-reservation', 'admin-queue-workers-reservation', 'admin-queue-ledger-reservation', 'admin-queue-schedules-reservation'],
    })
    const [statusRes, batchRes] = await Promise.all([page.request.get('/api/admin/celery/status'), page.request.get('/api/admin/metric-history/batch?since=1h')])
    expect(statusRes.status() === 200 && batchRes.status() === 200).toBe(true)
    const [statusBody, batchBody] = await Promise.all([statusRes.json(), batchRes.json()])
    expect(typeof statusBody.broker_reachable === 'boolean' && typeof statusBody.workers?.count === 'number' && typeof statusBody.queues === 'object').toBe(true)
    expect(Array.isArray(statusBody.inprocess_schedules)).toBe(true)
    for (const [, count] of Object.entries(statusBody.ledger as Record<string, unknown>)) expect(typeof count).toBe('number')
    for (const [, points] of Object.entries(batchBody.series as Record<string, unknown>)) expect(Array.isArray(points)).toBe(true)
    await assertAnalyst403(page, 'Queue', () => ['/api/admin/celery/status', '/api/admin/metric-history/batch?since=1h'])
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
    await expect(page.locator('[data-testid="admin-queue-schedules-reservation"]:visible')).toContainText(/In-Process Schedules \(\d+\)/i, { timeout: 15_000 })
    const ledgerSection = page.locator('[data-testid="admin-queue-ledger-reservation"]:visible')
    await expect(ledgerSection).toContainText(/Ingest Ledger Status/i, { timeout: 15_000 })
    await expect(ledgerSection).toContainText(/Ledger is empty|discovered|claimed|committed|quarantined|dead_letter/i, { timeout: 15_000 })
    await assertManualRefreshAttribution(page, {
      url: '/admin/queue', readyLocator: page.locator('[data-testid="admin-queue-root"]:visible'),
      refreshLocator: page.locator('[data-testid="admin-queue-refresh-btn"]:visible'),
      requestFilter: (url) => url.includes('/api/admin/celery/status') || url.includes('/api/admin/metric-history/batch'),
    })
  })
})

test('admin shell reaches the provision-wizard entry', async ({ page }) => {
  await page.goto('/admin')
  await expect(page).toHaveURL(/\/admin/)
  await expect(page.getByRole('button', { name: /add service/i })).toBeVisible({ timeout: 30_000 })
})

test('POST /api/provision/execute streams all 8 Step banners and persists the new service', async ({ page }) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-wizard-${Date.now()}`
  await page.goto('/admin')
  const messages = await streamProvisionExecute(page, buildProvisionPayload(sid, 'Playwright Wizard E2E', 'PW Wizard'))
  const stepBanners = messages
    .map((raw) => { try { return JSON.parse(raw) as { type?: string; message?: string } } catch { return null } })
    .filter((m): m is { type: string; message: string } => !!m && typeof m.message === 'string' && /Step \d+\/8/.test(m.message))
  expect(stepBanners.length).toBeGreaterThanOrEqual(8)
  for (let i = 1; i <= 8; i++) expect(stepBanners.some((m) => m.message.includes(`Step ${i}/8`))).toBe(true)
  const bootstrapResp = await page.request.get('/api/bootstrap')
  expect(bootstrapResp.ok()).toBe(true)
  expect(((await bootstrapResp.json()) as { services?: { service_id: string }[] }).services?.some((s) => s.service_id === sid)).toBe(true)
})

test('teardown CTA opens a confirm dialog and removes the service from /api/bootstrap', async ({ page }) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-teardown-${Date.now()}`
  await provisionFreshService(page, sid)
  expect(((await (await page.request.get('/api/bootstrap')).json()) as { services?: { service_id: string }[] }).services?.some((s) => s.service_id === sid)).toBe(true)
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
  await expect.poll(async () => {
    const j = (await (await page.request.get('/api/bootstrap')).json()) as { services?: { service_id: string }[] }
    return j.services?.some((s) => s.service_id === sid) ?? false
  }, { timeout: 8_000, intervals: [250, 500, 1000, 1500] }).toBe(false)
})

const CONTROL_ROOM_SERVICE_ID = process.env.E2E_REAL_RT_FASTLY_SERVICE_ID || 'svc-playwright-e2e'
const gotoControlRoom = async (page: Page) => { await page.goto('/control-room'); await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 }) }

test.describe('Control Room', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript((sid) => {
      localStorage.setItem('service-storage', JSON.stringify({
        state: { activeServiceId: sid, services: [{ id: sid, name: 'Playwright E2E Service' }] }, version: 0,
      }))
      localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'UTC' }, version: 0 }))
    }, CONTROL_ROOM_SERVICE_ID)
  })

  test('sidebar link, 6 desktop tabs, SSE badge, Overview metrics, and historical dashboard link', async ({ page }) => {
    await gotoControlRoom(page)
    const sidebarLink = page.getByRole('link', { name: 'Control Room' })
    await expect(sidebarLink).toBeVisible({ timeout: 10_000 })
    await expect(sidebarLink).not.toHaveAttribute('aria-disabled', 'true', { timeout: 10_000 })
    await expect(sidebarLink).toHaveAttribute('href', /\/control-room/)
    await expect(page.locator('[data-slot="badge"]:has-text("Connected")').first()).toBeVisible({ timeout: 10_000 })
    await page.locator('text=Loading app configuration…').waitFor({ state: 'detached', timeout: 30_000 })
    for (const tabName of ['Summary', 'CDN & Caching', 'Security', 'Performance', 'Network', 'Admin']) {
      await expect(page.locator(`[role="tablist"] button:text("${tabName}")`)).toBeVisible({ timeout: 5_000 })
    }
    for (const label of ['Requests/s', 'Error Rate', 'Cache Hit Ratio', 'Bandwidth']) {
      await expect(page.locator(`text=${label}`).filter({ visible: true }).first()).toBeVisible({ timeout: 10_000 })
    }
    await expect(page.locator('a[href^="/dashboard"]').filter({ hasText: /historical/i }).first()).toBeVisible({ timeout: 5_000 })
  })

  test('mobile layout shows condensed overview', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await gotoControlRoom(page)
    await page.waitForTimeout(2_000)
    await expect(page.locator('text=Live Overview')).toBeVisible({ timeout: 5_000 })
    await expect(page.locator('[role="tablist"]')).toBeHidden()
  })

  test('no WCAG 2.1 AA violations on /control-room', async ({ page }) => {
    await gotoControlRoom(page)
    await page.waitForTimeout(3_000)
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .exclude('[data-empty-placeholder="true"]').exclude('[data-axe-ignore="true"]').analyze()
    expect(results.violations).toEqual([])
  })

  for (const { name, tabText, metrics } of [
    { name: 'Security tab shows WAF metrics', tabText: 'Security', metrics: ['WAF Blocked', 'WAF Logged'] },
    { name: 'Network tab shows PoP counts', tabText: 'Network', metrics: ['Active PoPs', 'Healthy PoPs'] },
    { name: 'Origin tab shows origin metrics', tabText: 'CDN & Caching', metrics: ['Origin Requests/s', 'Shield Hit Ratio'] },
  ]) {
    test(name, async ({ page }) => {
      await gotoControlRoom(page)
      await page.locator(`[role="tablist"] button:text("${tabText}")`).first().click()
      for (const m of metrics) await expect(page.locator(`text=${m}`).first()).toBeVisible({ timeout: 10_000 })
    })
  }
})

test.describe('/admin/trends — Operational Trends & Health Sparklines Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter, zero-CLS reservations, warm-load budget & backend contract', async ({ page }) => {
    const isRemoteOrigin = Boolean(externalOrigin) && !externalOrigin.includes('127.0.0.1') && !externalOrigin.includes('localhost')
    await assertAdminMount(page, '/admin/trends', {
      rootTestId: 'admin-trends-root', heading: /Operational Trends/i, visibleTestIds: ['admin-trends-toolbar'],
      intrinsicIds: ['admin-trends-toolbar', 'admin-trends-grid-reservation'],
      warmLocators: [page.locator('[data-testid="admin-trends-root"]:visible'), page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible')],
      warmMaxFcpMs: isRemoteOrigin ? 4000 : 1200,
    })
    expect((await page.locator('[data-testid="admin-trends-toolbar"]:visible').boundingBox())?.height ?? 0).toBeGreaterThanOrEqual(32)
    expect((await page.locator('[data-testid="admin-trends-grid-reservation"]:visible').boundingBox())?.height ?? 0).toBeGreaterThanOrEqual(400)
    const res = await page.request.get('/api/admin/metric-history/batch?since=1h')
    expect(res.status()).toBe(200)
    const body = (await res.json()) as Record<string, unknown>
    expect(typeof body['series'] === 'object' && body['series'] !== null).toBe(true)
    expect((await page.request.get('/api/admin/metric-history/batch?since=invalid')).status()).toBe(400)
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
    for (const metric of ['cpu_load_1m', 'mem_used_pct', 'disk_used_pct', 'disk_used_pct_root', 'active_query_count', 'pool_wait_p95_ms', 'ingest_lag_s']) {
      await expect(page.locator(`[data-testid="admin-trends-card-${metric}"]:visible`)).toBeVisible({ timeout: 15_000 })
    }
    await expect(page.locator('[data-testid="admin-trends-window-1h"]:visible')).toHaveAttribute('aria-pressed', 'true')
    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = page.locator(`[data-testid="admin-trends-window-${win}"]:visible`)
      await expect(btn).toBeVisible()
      await expect.poll(async () => {
        if ((await btn.getAttribute('aria-pressed')) !== 'true') await btn.click()
        return windowStatuses.get(win) ?? 0
      }, { timeout: 30_000, intervals: [250, 500, 1000] }).toBe(200)
      await expect(btn).toHaveAttribute('aria-pressed', 'true')
    }
    await assertManualRefreshAttribution(page, {
      url: '/admin/trends', readyLocator: page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible'),
      ssrQueryKey: 'metric-history-batch',
      refreshLocator: page.locator('[data-testid="admin-trends-refresh-btn"]:visible'),
      requestFilter: (url) => url.includes('/api/admin/metric-history/batch'),
    })
  })
})

test.describe('/admin/rum — Admin RUM Ingestion & Configuration Contract', () => {
  test.setTimeout(90_000)

  test('1. Route accessibility, ServerFooter identity, RUM layout mount, zero-CLS reservations & warm-load budget', async ({ page }) => {
    await assertAdminMount(page, '/admin/rum', {
      rootTestId: 'admin-rum-root', heading: 'Real User Monitoring', domContentLoaded: true, warmReload: true,
      testIds: ['back-to-admin-link', 'admin-rum-toolbar'],
      minHeights: [['admin-rum-toolbar', 48], ['admin-rum-status-reservation', 200]],
      warmLocators: [page.getByTestId('admin-rum-root'), page.getByTestId('admin-rum-status-line')],
    })
  })

  test('3. Backend data contracts: /api/services/{service_id}/rum/status and /api/services', async ({ page }) => {
    let statusPayload: Record<string, unknown> | null = null, servicesPayload: Record<string, unknown> | null = null
    page.on('response', async (res) => {
      if (res.status() !== 200) return
      if (/\/api\/services\/[^/]+\/rum\/status/.test(res.url())) statusPayload = await res.json().catch(() => null)
      else if (res.url().endsWith('/api/services')) servicesPayload = await res.json().catch(() => null)
    })
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({ timeout: 30_000 })
    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull()
    for (const k of ['enabled', 'vcl_drift', 'capture_vitals', 'capture_performance', 'capture_errors', 'capture_events']) {
      expect(typeof statusPayload![k]).toBe('boolean')
    }
    if (servicesPayload) expect(Array.isArray(servicesPayload['services'])).toBe(true)
  })

  test('4. RUM Status card, Enable/Disable controls, and Faro SDK version state', async ({ page }) => {
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' })
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({ timeout: 30_000 })
    expect((await page.getByTestId('admin-rum-status-badge').textContent()) ?? '').toMatch(/Beacon Pipeline (Active|Inactive)/)
    const isEnabled = ((await page.getByTestId('admin-rum-status-line').textContent()) ?? '').includes('Enabled')
    const enableBtn = page.getByTestId('admin-rum-enable-btn'), disableBtn = page.getByTestId('admin-rum-disable-btn')
    await expect(enableBtn).toBeVisible()
    await expect(disableBtn).toBeVisible()
    if (isEnabled) {
      await expect(enableBtn).toBeDisabled()
      await expect(disableBtn).toBeEnabled()
      for (const id of ['admin-rum-enabled-at', 'admin-rum-vcl-drift', 'admin-rum-capture-summary']) await expect(page.getByTestId(id)).toBeVisible()
    } else {
      await expect(enableBtn).toBeEnabled()
      await expect(disableBtn).toBeDisabled()
    }
    await assertManualRefreshAttribution(page, {
      url: '/admin/rum', readyLocator: page.getByTestId('admin-rum-status-line'),
      refreshLocator: page.getByTestId('admin-rum-refresh-btn'),
      requestFilter: (url) => /\/api\/services\/[^/]+\/rum\/status/.test(url),
    })
  })
})

test.describe('/admin/usage-log — Admin FOS Usage & Class A/B Ledger Contract', () => {
  test.setTimeout(90_000)
  const reservationIds = ['admin-usage-log-toolbar', 'admin-usage-log-kpi-reservation', 'admin-usage-log-chart-reservation', 'admin-usage-log-table-reservation']

  test('1. Route accessibility, ServerFooter identity, Usage Log layout mount, zero-CLS reservations & warm-load budget', async ({ page }) => {
    const main = page.locator('#main')
    await assertAdminMount(page, '/admin/usage-log', {
      rootTestId: 'admin-usage-log-root', heading: /FOS Usage Log/i, domContentLoaded: true, warmReload: true,
      testIds: reservationIds, intrinsicIds: reservationIds, scope: main,
      minHeights: [['admin-usage-log-toolbar', 32], ['admin-usage-log-kpi-reservation', 150], ['admin-usage-log-chart-reservation', 340], ['admin-usage-log-table-reservation', 450]],
      warmLocators: [main.getByTestId('admin-usage-log-root'), main.getByTestId('admin-usage-log-filters')],
    })
  })

  test('3. Backend data contracts (/api/admin/usage-log, /api/admin/usage-logging, /api/admin/system-jobs)', async ({ page }) => {
    const svcRes = await page.request.get('/api/services')
    expect(svcRes.status()).toBe(200)
    const svcBody = (await svcRes.json()) as { services?: Array<{ service_id?: string; id?: string }>; active_service_id?: string }
    const serviceId = svcBody.active_service_id || svcBody.services?.[0]?.service_id || svcBody.services?.[0]?.id || ''
    const [ulRes, cfgRes, badPatch, jobsRes] = await Promise.all([
      page.request.get(`/api/admin/usage-log?service_id=${encodeURIComponent(serviceId)}&page_size=25`),
      page.request.get('/api/admin/usage-logging'),
      page.request.patch('/api/admin/usage-logging', { data: { billing_zone: 'INVALID_ZONE' } }),
      page.request.get('/api/admin/system-jobs'),
    ])
    expect(ulRes.status() === 200 && cfgRes.status() === 200 && jobsRes.status() === 200 && badPatch.status() === 400).toBe(true)
    const [ulBody, cfgBody, jobsBody] = await Promise.all([
      ulRes.json() as Promise<Record<string, unknown>>, cfgRes.json() as Promise<Record<string, unknown>>,
      jobsRes.json() as Promise<{ jobs?: Array<{ id: string; name: string }> }>,
    ])
    expect(Array.isArray(ulBody['entries']) && typeof ulBody['total'] === 'number' && typeof cfgBody['enabled'] === 'boolean').toBe(true)
    const agg = ulBody['aggregate'] as Record<string, unknown>
    for (const k of ['total_class_a', 'total_class_b', 'total_cdn_bytes', 'estimated_cost_total']) expect(typeof agg[k]).toBe('number')
    for (const k of ['retention_days', 'class_a_rate_per_1k', 'class_b_rate_per_10k']) expect(typeof cfgBody[k]).toBe('number')
    expect((jobsBody.jobs ?? []).map((j) => j.id)).toEqual(expect.arrayContaining(['bot_data_refresh', 'rdns_enrichment', 'share_audit_purge']))
  })

  test('4. Filter & granularity interaction across preset windows, filter inputs, refresh & CSV export', async ({ page }) => {
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
    const procInput = main.getByTestId('admin-usage-log-process-input'), opInput = main.getByTestId('admin-usage-log-operation-input')
    await procInput.fill('cron:')
    await opInput.fill('PutObject')
    const exportBtn = main.getByTestId('admin-usage-log-export-btn')
    await expect(exportBtn).toHaveAttribute('href', /process_context=cron%3A/)
    await expect(exportBtn).toHaveAttribute('href', /operation_type=PutObject/)
    await procInput.fill('')
    await opInput.fill('')

    await assertManualRefreshAttribution(page, {
      url: '/admin/usage-log', readyLocator: main.getByTestId('admin-usage-log-root'),
      refreshLocator: main.getByTestId('admin-usage-log-refresh-btn'),
      requestFilter: (url) => url.includes('/api/admin/usage-log') || url.includes('/api/admin/log-accounting'),
    })
    await expect(exportBtn).toBeVisible()
    const exportHref = await exportBtn.getAttribute('href')
    expect(exportHref).toBeTruthy()
    const csvRes = await page.request.get(exportHref!)
    expect(csvRes.status()).toBe(200)
    expect(csvRes.headers()['content-type'] ?? '').toContain('text/csv')
    expect((await csvRes.text()).split('\n')[0]).toContain('timestamp,service_id,operation_class,operation_type,url,bytes,duration_ms,function_name,process_context,status,count')
  })
})

async function isClickHouseEnabledForEnv(page: Page): Promise<boolean> {
  const serviceId = await resolveFirstServiceId(page)
  const statusRes = await page.request.get(`/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`)
  return statusRes.status() === 200 ? Boolean((await statusRes.json()).enabled) : false
}

test.describe('Page Spec: /admin/clickhouse (Admin ClickHouse Replay & Cluster Operations)', () => {
  test('1. Route accessibility, ServerFooter, ClickHouse layout mount, zero-CLS reservations & warm-load budget', async ({ page }) => {
    const enabled = await isClickHouseEnabledForEnv(page)
    expect([200, 404]).toContain((await page.goto('/admin/clickhouse'))?.status())
    await page.addInitScript(clsInitScript)
    const status = (await page.reload({ waitUntil: 'domcontentloaded' }))?.status() ?? 0
    expect([200, 404]).toContain(status)
    if (!enabled || status === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0)
    } else {
      await expect(page.getByRole('heading', { name: 'ClickHouse Replay & Cluster Operations' })).toBeVisible()
      await expect(page.getByTestId('admin-clickhouse-root')).toBeVisible()
      await assertFooterVisible(page, /Admin View.*(standard|high-scale)/i)
      await expect(page.getByTestId('admin-clickhouse-toolbar')).toBeVisible()
      for (const [id, minH] of [['admin-clickhouse-vitals-reservation', 200], ['admin-clickhouse-generation-reservation', 180], ['admin-clickhouse-replay-reservation', 260]] as const) {
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

  test('3. Backend data contract for /api/admin/clickhouse/status and replay validation for POST /api/admin/clickhouse/replay', async ({ page }) => {
    const serviceId = await resolveFirstServiceId(page)
    const [statusRes, noSvcRes, overLimitRes, missingRefRes, dryRunRes] = await Promise.all([
      page.request.get(`/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`),
      page.request.get('/api/admin/clickhouse/status'),
      page.request.post('/api/admin/clickhouse/replay', { data: { service_id: serviceId, dataset_id: 'ds_probe_01', dry_run: true, limit: 101 } }),
      page.request.post('/api/admin/clickhouse/replay', { data: { service_id: serviceId, dry_run: true, limit: 10 } }),
      page.request.post('/api/admin/clickhouse/replay', { data: { service_id: serviceId, dataset_id: 'ds_nonexistent_probe', dry_run: true, limit: 10 } }),
    ])
    expect([statusRes.status(), noSvcRes.status(), overLimitRes.status(), missingRefRes.status()]).toEqual([200, 400, 422, 422])
    const statusJson = await statusRes.json()
    expect(statusJson.service_id === serviceId && typeof statusJson.enabled === 'boolean').toBe(true)
    if (!statusJson.enabled) {
      expect([statusJson.health, statusJson.schema_version, statusJson.publication_counts, dryRunRes.status()]).toEqual(['disabled', null, null, 409])
      expect((await dryRunRes.json())?.detail?.error).toBe('clickhouse_disabled')
    } else {
      expect(statusJson.health).toBe('ok')
      expect(typeof statusJson.schema_version === 'number' && typeof statusJson.publication_counts?.published === 'number' && typeof statusJson.publication_counts?.pending === 'number').toBe(true)
      expect([200, 404]).toContain(dryRunRes.status())
      if (dryRunRes.status() === 404) expect((await dryRunRes.json())?.detail?.error).toBe('replay_not_found')
    }
  })

  test('5. Interactive manual refresh workflow with X-Page-Load-ID telemetry attribution', async ({ page }) => {
    const enabled = await isClickHouseEnabledForEnv(page), observedPageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/clickhouse/status') && req.headers()['x-page-load-id']) observedPageLoadIds.push(req.headers()['x-page-load-id'])
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
    const refreshResponsePromise = page.waitForResponse((res) => res.url().includes('/api/admin/clickhouse/status') && (res.status() === 200 || res.status() === 503))
    await refreshBtn.click()
    expect([200, 503]).toContain((await refreshResponsePromise).status())
    expect(observedPageLoadIds.length).toBeGreaterThan(0)
  })
})
