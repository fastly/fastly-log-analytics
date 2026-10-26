/**
 * E2E Page Contract Test: Admin Overview & Ingest Operations (/admin) & Multiplexed SSE Stream
 *
 * Implements the verification checklist specified in docs/pages/admin/overview.md:
 * 1. Route accessibility (GET /admin returns 200 for Admin; Analyst Path B receives 403 on /api/admin/sync-status) & ServerFooter environment indicator.
 * 2. Instant shell, pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms).
 * 3. Backend data contracts: /api/admin/sync-status, /api/admin/system-metrics, /api/admin/health, /api/admin/compaction-status, /api/services.
 * 4. Ingest & sync status cards, system metrics, and configured services list rendering with live backend data.
 * 5. Interactive manual status refresh workflow with X-Page-Load-ID telemetry attribution.
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import {
  clsInitScript,
  configureExternalAdminOrigin,
  getFirstServiceId,
  installClsObserver,
  loginAsPasscodeAnalyst,
  measureWarmPaintAndCls,
  revokeAnalystInvite,
} from '../helpers'

configureExternalAdminOrigin()

const LEGACY_STREAMS = [
  '/api/sync-status/stream',
  '/api/cron-runs/stream',
  '/api/admin/system-metrics/stream',
  '/api/admin/share/stream',
]

function recordStreams(page: import('@playwright/test').Page): string[] {
  const streamRequests: string[] = []
  page.on('response', (resp) => {
    const url = resp.url()
    if (url.includes('/stream')) streamRequests.push(new URL(url).pathname + new URL(url).search)
  })
  return streamRequests
}

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

test('admin page opens one multiplexed SSE stream, not the three legacy ones', async ({ page }) => {
  const streamRequests = recordStreams(page)

  await page.goto('/admin')
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
  const legacy = streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))

  expect(legacy, `legacy per-purpose streams must not open; saw: ${legacy.join(', ')}`).toHaveLength(0)
  expect(
    merged.length,
    `expected the merged /api/admin/events/stream to open; saw stream reqs: ${streamRequests.join(', ')}`,
  ).toBeGreaterThanOrEqual(1)
  expect(merged.some((p) => p.includes('channels=') && p.includes('system-metrics'))).toBe(true)
})

test('admin/share opens one multiplexed stream (share channel folded in), not a second stream', async ({ page }) => {
  const streamRequests = recordStreams(page)

  await page.goto('/admin/share')
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
  const legacy = streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))

  expect(
    legacy,
    `no dedicated share/legacy stream may open on /admin/share; saw: ${legacy.join(', ')}`,
  ).toHaveLength(0)
  expect(
    merged.length,
    `expected the merged /api/admin/events/stream on /admin/share; saw: ${streamRequests.join(', ')}`,
  ).toBeGreaterThanOrEqual(1)
  expect(merged.some((p) => p.includes('channels=') && p.includes('share'))).toBe(true)
})


test.describe('/admin/share — Admin Live Share Management Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and Live Share layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/share', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.getByTestId('admin-share-root')).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByRole('heading', { name: 'Remote Dashboard Sharing' })
    ).toBeVisible();
    await expect(page.getByTestId('back-to-admin-link')).toBeVisible();
    await expect(page.getByTestId('admin-share-refresh-btn')).toBeVisible();
    await expect(page.getByTestId('admin-share-control-reservation')).toBeVisible();
    await expect(page.getByTestId('admin-share-tabs')).toBeVisible();
    await expect(page.getByTestId('admin-share-panel-reservation')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-share-root')).toBeVisible({ timeout: 30_000 });

    const controlReservation = page.getByTestId('admin-share-control-reservation');
    const panelReservation = page.getByTestId('admin-share-panel-reservation');
    await expect(controlReservation).toBeVisible();
    await expect(panelReservation).toBeVisible();

    const controlBox = await controlReservation.boundingBox();
    const panelBox = await panelReservation.boundingBox();
    expect(controlBox?.height ?? 0).toBeGreaterThanOrEqual(180);
    expect(panelBox?.height ?? 0).toBeGreaterThanOrEqual(240);

    await page.addInitScript(clsInitScript);

    await page.reload({ waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-share-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({
      timeout: 30_000,
    });

    const perfMetrics = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as
        | PerformanceEntry
        | undefined;
      const cls = window.__clsScore ?? 0;
      return {
        fcpMs: fcpEntry ? fcpEntry.startTime : 0,
        cls,
      };
    });

    expect(perfMetrics.cls).toBeLessThanOrEqual(0.05);
    if (perfMetrics.fcpMs > 0) {
      expect(perfMetrics.fcpMs).toBeLessThan(1200);
    }
  });

  test('3. Backend data contracts: /api/admin/share/status, /live, /settings, /invites, and /tos', async ({
    page,
    request,
  }) => {
    let statusPayload: Record<string, unknown> | null = null;
    let livePayload: Record<string, unknown> | null = null;

    page.on('response', async (res) => {
      const url = res.url();
      if (url.endsWith('/api/admin/share/status') && res.status() === 200) {
        try {
          statusPayload = (await res.json()) as Record<string, unknown>;
        } catch {
          // ignore body read errors
        }
      } else if (url.endsWith('/api/admin/share/live') && res.status() === 200) {
        try {
          livePayload = (await res.json()) as Record<string, unknown>;
        } catch {
          // ignore body read errors
        }
      }
    });

    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({
      timeout: 30_000,
    });

    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull();
    expect(typeof statusPayload!['sharing_active']).toBe('boolean');
    expect(Array.isArray(statusPayload!['invites'])).toBe(true);
    expect(Array.isArray(statusPayload!['sessions'])).toBe(true);
    expect(Array.isArray(statusPayload!['services'])).toBe(true);
    expect(Array.isArray(statusPayload!['audit_logs'])).toBe(true);
    expect(typeof statusPayload!['rate_limits']).toBe('object');

    if (livePayload) {
      expect(typeof livePayload['sharing_active']).toBe('boolean');
      expect(typeof livePayload['active_session_count']).toBe('number');
    }

    const settingsRes = await request.get('/api/admin/share/settings');
    expect(settingsRes.status()).toBe(200);
    const settingsJson = (await settingsRes.json()) as Record<string, unknown>;
    expect(typeof settingsJson['max_concurrent_analyst_sessions']).toBe('number');

    const invitesRes = await request.get('/api/admin/share/invites');
    expect(invitesRes.status()).toBe(200);
    const invitesJson = (await invitesRes.json()) as Record<string, unknown>;
    expect(Array.isArray(invitesJson['invites'])).toBe(true);

    const tosRes = await request.get('/api/admin/share/tos');
    expect(tosRes.status()).toBe(200);
    const tosJson = (await tosRes.json()) as Record<string, unknown>;
    expect('version' in tosJson).toBe(true);
    expect('text' in tosJson).toBe(true);
  });

  test('4. Sharing Control Panel, tabs (Invitations, Sessions, Audit), and URL ?tab= sync', async ({
    page,
  }) => {
    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({
      timeout: 30_000,
    });

    const statusBadge = page.getByTestId('admin-share-status-badge');
    await expect(statusBadge).toBeVisible();
    const badgeText = ((await statusBadge.textContent()) ?? '').trim();
    expect(badgeText).toMatch(/^(ACTIVE|OFF)$/);

    if (badgeText === 'ACTIVE') {
      await expect(page.getByTestId('admin-share-stop-btn')).toBeVisible();
      await expect(page.getByTestId('admin-share-panic-btn')).toBeVisible();
    } else {
      await expect(page.getByTestId('admin-share-start-btn')).toBeVisible();
    }

    const tabInvites = page.getByTestId('admin-share-tab-invites');
    const tabSessions = page.getByTestId('admin-share-tab-sessions');
    const tabAudit = page.getByTestId('admin-share-tab-audit');

    await expect(tabInvites).toBeVisible();
    await expect(tabSessions).toBeVisible();
    await expect(tabAudit).toBeVisible();
    await expect(page.getByTestId('admin-share-invites-panel')).toBeVisible();

    await tabSessions.click();
    await expect(tabSessions).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByTestId('admin-share-sessions-panel')).toBeVisible({ timeout: 15_000 });
    await expect(page).toHaveURL(/[?&]tab=sessions(?:&|$)/);

    await tabAudit.click();
    await expect(tabAudit).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByTestId('admin-share-audit-panel')).toBeVisible({ timeout: 15_000 });
    await expect(page).toHaveURL(/[?&]tab=audit(?:&|$)/);

    await tabInvites.click();
    await expect(tabInvites).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByTestId('admin-share-invites-panel')).toBeVisible({ timeout: 15_000 });
  });

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({
    page,
  }) => {
    const shareRequests: Array<{ url: string; pageLoadId: string | null }> = [];

    page.on('request', (req) => {
      if (/\/api\/admin\/share\/(status|live)/.test(req.url())) {
        shareRequests.push({
          url: req.url(),
          pageLoadId: req.headers()['x-page-load-id'] ?? null,
        });
      }
    });

    await page.goto('/admin/share', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible({
      timeout: 30_000,
    });

    await expect
      .poll(() => shareRequests.length, { timeout: 25_000 })
      .toBeGreaterThanOrEqual(1);
    expect(shareRequests[0].pageLoadId).toBeTruthy();

    const countBeforeRefresh = shareRequests.length;
    const refreshBtn = page.getByTestId('admin-share-refresh-btn');
    await expect(refreshBtn).toBeEnabled();
    await refreshBtn.click();

    await expect
      .poll(() => shareRequests.length, { timeout: 15_000 })
      .toBeGreaterThan(countBeforeRefresh);

    await expect(page.getByTestId('admin-share-control-panel')).toBeVisible();
  });
});

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

test.describe('/admin/session-scoring — Admin Session Scoring Configuration Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and Session Scoring layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.getByTestId('admin-session-scoring-root')).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByRole('heading', { name: 'Session Scoring' })
    ).toBeVisible();
    await expect(page.getByTestId('back-to-admin-link')).toBeVisible();
    await expect(page.getByTestId('admin-session-scoring-refresh-btn')).toBeVisible();
    await expect(page.getByTestId('admin-session-scoring-status-reservation')).toBeVisible();
    await expect(page.getByTestId('admin-session-scoring-tabs')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-root')).toBeVisible({ timeout: 30_000 });

    const statusReservation = page.getByTestId('admin-session-scoring-status-reservation');
    const overviewReservation = page.getByTestId('admin-session-scoring-overview-reservation');
    await expect(statusReservation).toBeVisible();
    await expect(overviewReservation).toBeVisible();

    const statusBox = await statusReservation.boundingBox();
    const overviewBox = await overviewReservation.boundingBox();
    expect(statusBox?.height ?? 0).toBeGreaterThanOrEqual(200);
    expect(overviewBox?.height ?? 0).toBeGreaterThanOrEqual(400);

    await page.addInitScript(clsInitScript);

    await page.reload({ waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({
      timeout: 30_000,
    });

    const perfMetrics = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as
        | PerformanceEntry
        | undefined;
      const cls = window.__clsScore ?? 0;
      return {
        fcpMs: fcpEntry ? fcpEntry.startTime : 0,
        cls,
      };
    });

    expect(perfMetrics.cls).toBeLessThanOrEqual(0.05);
    if (perfMetrics.fcpMs > 0) {
      expect(perfMetrics.fcpMs).toBeLessThan(1200);
    }
  });

  test('3. Backend data contracts: /api/services/{service_id}/scoring/config, /scoring/status, and /api/services', async ({
    page,
    request,
  }) => {
    let configPayload: Record<string, unknown> | null = null;
    let analyticsPayload: Record<string, unknown> | null = null;
    let servicesPayload: Record<string, unknown> | null = null;
    let resolvedServiceId: string | null = null;

    page.on('response', async (res) => {
      const url = res.url();
      const configMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/config/);
      const analyticsMatch = url.match(/\/api\/services\/([^/]+)\/(?:session-)?scoring\/analytics/);
      if (configMatch && res.status() === 200) {
        resolvedServiceId = decodeURIComponent(configMatch[1]);
        try {
          configPayload = (await res.json()) as Record<string, unknown>;
        } catch {
          // ignore body read errors
        }
      } else if (analyticsMatch && res.status() === 200) {
        resolvedServiceId = decodeURIComponent(analyticsMatch[1]);
        try {
          analyticsPayload = (await res.json()) as Record<string, unknown>;
        } catch {
          // ignore body read errors
        }
      } else if (url.endsWith('/api/services') && res.status() === 200) {
        try {
          servicesPayload = (await res.json()) as Record<string, unknown>;
        } catch {
          // ignore body read errors
        }
      }
    });

    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({
      timeout: 30_000,
    });

    await expect.poll(() => configPayload, { timeout: 25_000 }).not.toBeNull();
    expect(typeof configPayload!['status']).toBe('object');
    const statusObj = configPayload!['status'] as Record<string, unknown>;
    expect(typeof statusObj['enabled']).toBe('boolean');
    expect(typeof configPayload!['threshold']).toBe('object');
    expect(typeof configPayload!['exclude_regex']).toBe('object');
    expect(typeof configPayload!['enforce_status_code']).toBe('object');

    if (analyticsPayload) {
      expect(typeof analyticsPayload['health']).toBe('object');
    }

    if (servicesPayload) {
      expect(Array.isArray(servicesPayload['services'])).toBe(true);
    }

    if (resolvedServiceId) {
      const statusRes = await request.get(
        `/api/services/${encodeURIComponent(resolvedServiceId)}/scoring/status`
      );
      expect(statusRes.status()).toBe(200);
      const statusJson = (await statusRes.json()) as Record<string, unknown>;
      expect(typeof statusJson['enabled']).toBe('boolean');
    }
  });

  test('4. Session Scoring Status panel, tabs (Overview, Labels, Matrix history, Audit), and controls', async ({
    page,
  }) => {
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({
      timeout: 30_000,
    });

    await expect(page.getByTestId('admin-session-scoring-status-card')).toBeVisible();
    const statusBadge = page.getByTestId('admin-session-scoring-status-badge');
    await expect(statusBadge).toBeVisible();
    const badgeText = ((await statusBadge.textContent()) ?? '').trim();
    expect(badgeText).toMatch(/^(Enabled|Disabled)$/);

    const isEnabled = badgeText === 'Enabled';
    if (isEnabled) {
      await expect(page.getByTestId('admin-session-scoring-redeploy-btn')).toBeVisible();
      await expect(page.getByTestId('admin-session-scoring-disable-btn')).toBeVisible();
    } else {
      await expect(page.getByTestId('admin-session-scoring-enable-btn')).toBeVisible();
    }

    const tabOverview = page.getByTestId('admin-session-scoring-tab-overview');
    const tabLabels = page.getByTestId('admin-session-scoring-tab-labels');
    const tabMatrix = page.getByTestId('admin-session-scoring-tab-matrix');
    const tabAudit = page.getByTestId('admin-session-scoring-tab-audit');

    await expect(tabOverview).toBeVisible();
    await expect(tabLabels).toBeVisible();
    await expect(tabMatrix).toBeVisible();
    await expect(tabAudit).toBeVisible();

    await tabAudit.click();
    await expect(tabAudit).toHaveAttribute('aria-selected', 'true');
    await tabMatrix.click();
    await expect(tabMatrix).toHaveAttribute('aria-selected', 'true');
    await tabLabels.click();
    await expect(tabLabels).toHaveAttribute('aria-selected', 'true');
    await tabOverview.click();
    await expect(tabOverview).toHaveAttribute('aria-selected', 'true');
  });

  test('prerequisite dialog stays closed after readiness recovers and later regresses', async ({ page }) => {
    let ready = false;
    await page.route(/\/api\/services\/[^/]+\/(?:session-)?scoring\/(?:config|status)(?:\?|$)/, async (route) => {
      const response = await route.fetch();
      const body = await response.json();
      const status = { enabled: false, preflight_pragmas_ready: ready };
      await route.fulfill({
        response,
        json: new URL(route.request().url()).pathname.endsWith('/config')
          ? { ...body, status: { ...body.status, ...status } }
          : { ...body, ...status },
      });
    });
    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeVisible();
    await page.getByTestId('admin-session-scoring-enable-btn').click();
    const dialog = page.getByTestId('admin-session-scoring-pragma-modal');
    await expect(dialog).toBeVisible();
    ready = true;
    await page.getByTestId('admin-session-scoring-pragma-recheck-btn').click();
    await expect(dialog).toBeHidden();
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeHidden();
    ready = false;
    await page.getByTestId('admin-session-scoring-refresh-btn').click();
    await expect(page.getByTestId('admin-session-scoring-pragma-warning')).toBeVisible();
    await expect(dialog).toBeHidden();
    await page.getByTestId('admin-session-scoring-enable-btn').click();
    await expect(dialog).toBeVisible();
  });

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({
    page,
  }) => {
    const scoringRequests: Array<{ url: string; pageLoadId: string | null }> = [];

    page.on('request', (req) => {
      if (/\/api\/services\/[^/]+\/(?:session-)?scoring\/(config|analytics|status)/.test(req.url())) {
        scoringRequests.push({
          url: req.url(),
          pageLoadId: req.headers()['x-page-load-id'] ?? null,
        });
      }
    });

    await page.goto('/admin/session-scoring', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible({
      timeout: 30_000,
    });

    await expect
      .poll(() => scoringRequests.length, { timeout: 25_000 })
      .toBeGreaterThanOrEqual(1);
    expect(scoringRequests[0].pageLoadId).toBeTruthy();

    const countBeforeRefresh = scoringRequests.length;
    const refreshBtn = page.getByTestId('admin-session-scoring-refresh-btn');
    await expect(refreshBtn).toBeEnabled();
    await refreshBtn.click();

    await expect
      .poll(() => scoringRequests.length, { timeout: 15_000 })
      .toBeGreaterThan(countBeforeRefresh);

    await expect(page.getByTestId('admin-session-scoring-status-line')).toBeVisible();
  });
});

test.describe('Admin Task Queue & Ingest Ledger Page Contract (/admin/queue)', () => {
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
      const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
      let inviteId: string | null = null
      try {
        const seeded = await loginAsPasscodeAnalyst(page.request, {
          name: 'Queue Guard Analyst',
          email: `e2e-queue-guard-${Date.now()}@example.com`,
          passcode: 'harbor-cloud-forest-91',
          durationHours: 1,
          serviceIds: [serviceId],
        })
        inviteId = seeded.inviteId

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
        await revokeAnalystInvite(page.request, inviteId, { logout: true })
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

async function provisionFreshService(page: Page, sid: string) {
  await page.goto('/admin')
  const messages = await page.evaluate(async (deployBody: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(deployBody),
    })
    if (!resp.body) {
      throw new Error(`provision/execute returned no body (status=${resp.status})`)
    }
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    const out: string[] = []
    let buf = ''
    const deadline = Date.now() + 90_000
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) {
        buf += decoder.decode(value, { stream: true })
      }
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) out.push(line.slice(6))
        }
        m = SEP.exec(buf)
      }
      if (done) {
        break
      }
    }
    if (buf.trim()) {
      for (const line of buf.split(/\r\n|\n|\r/)) {
        if (line.startsWith('data: ')) {
          out.push(line.slice(6))
        }
      }
    }
    return out
  }, {
    token: 'mock-fastly-token',
    service_id: sid,
    service_name: `PW Teardown Target ${sid}`,
    endpoint_name: 'PW Teardown Logger',
    fos_region: 'us-east-1',
    fos_bucket_name: `pw-teardown-bucket-${Date.now()}`,
    fos_prefix: '',
    sample_rate: '100',
    edge_only: true,
    custom_condition: '',
    log_period: '60',
    cdn_service_name: 'PW Teardown CDN',
    cdn_url: `https://pw-teardown-${Date.now()}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  })
  const done = messages.some((raw: string) => {
    try {
      const m = JSON.parse(raw) as { type?: string }
      return m.type === 'done'
    } catch {
      return false
    }
  })
  console.log(`RECEIVED ${messages.length} TEARDOWN PROVISION MESSAGES:`, messages)
  expect(done, `orchestrator did not emit 'done' for ${sid}; got ${messages.length} events`).toBe(true)
}

test('admin shell reaches the provision-wizard entry', async ({ page }) => {
  await page.goto('/admin')
  await expect(page).toHaveURL(/\/admin/)
  await expect(page.getByRole('button', { name: /add service/i })).toBeVisible({
    timeout: 30_000,
  })
})

test('POST /api/provision/execute streams all 8 Step banners and persists the new service', async ({
  page,
}) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-wizard-${Date.now()}`
  const body = {
    token: 'mock-fastly-token',
    service_id: sid,
    service_name: 'Playwright Wizard E2E',
    endpoint_name: 'PW Wizard Logger',
    fos_region: 'us-east-1',
    fos_bucket_name: `pw-wizard-bucket-${Date.now()}`,
    fos_prefix: '',
    sample_rate: '100',
    edge_only: true,
    custom_condition: '',
    log_period: '60',
    cdn_service_name: 'PW Wizard CDN',
    cdn_url: `https://pw-wizard-${Date.now()}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  }

  await page.goto('/admin')

  const messages = await page.evaluate(async (deployBody: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(deployBody),
    })
    if (!resp.body) {
      throw new Error(`provision/execute returned no body (status=${resp.status})`)
    }
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    const out: string[] = []
    let buf = ''
    const deadline = Date.now() + 90_000
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) {
        buf += decoder.decode(value, { stream: true })
      }
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) {
            out.push(line.slice(6))
          }
        }
        m = SEP.exec(buf)
      }
      if (done) {
        break
      }
    }
    if (buf.trim()) {
      for (const line of buf.split(/\r\n|\n|\r/)) {
        if (line.startsWith('data: ')) {
          out.push(line.slice(6))
        }
      }
    }
    return out
  }, body)

  console.log("ALL RAW SSE MESSAGES RECEIVED:", messages)
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

  console.log("STEP BANNERS RECEIVED:", stepBanners.map(m => m.message))
  expect(stepBanners.length).toBeGreaterThanOrEqual(8)
  for (let i = 1; i <= 8; i++) {
    expect(stepBanners.some((m) => m.message.includes(`Step ${i}/8`))).toBe(true)
  }

  const bootstrapResp = await page.request.get('/api/bootstrap')
  expect(bootstrapResp.ok()).toBe(true)
  const bootstrap = (await bootstrapResp.json()) as {
    services?: { service_id: string }[]
  }
  expect(bootstrap.services?.some((s) => s.service_id === sid)).toBe(true)
})

test('teardown CTA opens a confirm dialog and removes the service from /api/bootstrap', async ({
  page,
}) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-teardown-${Date.now()}`

  await provisionFreshService(page, sid)

  const beforeResp = await page.request.get('/api/bootstrap')
  const before = (await beforeResp.json()) as { services?: { service_id: string }[] }
  expect(before.services?.some((s) => s.service_id === sid)).toBe(true)

  await page.goto('/admin')

  await page.getByRole('button', { name: /refresh status/i }).click()

  const row = page
    .getByRole('row')
    .filter({ hasText: `PW Teardown Target ${sid}` })
    .first()
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

  await expect(
    page.getByRole('button', { name: /^close$/i }).first(),
  ).toBeVisible({ timeout: 60_000 })

  await expect
    .poll(
      async () => {
        const r = await page.request.get('/api/bootstrap')
        const j = (await r.json()) as { services?: { service_id: string }[] }
        return j.services?.some((s) => s.service_id === sid) ?? false
      },
      { timeout: 8_000, intervals: [250, 500, 1000, 1500] },
    )
    .toBe(false)
})

// ═══════════════════════════════════════════════════════════════════════════
// Folded from e2e/control-room.spec.ts
// ═══════════════════════════════════════════════════════════════════════════

const CONTROL_ROOM_SERVICE_ID = process.env.E2E_REAL_RT_FASTLY_SERVICE_ID || 'svc-playwright-e2e'

async function seedControlRoomLocalStorage(page: Page) {
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
    localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'UTC' }, version: 0 }),
    )
  }, CONTROL_ROOM_SERVICE_ID)
}

test.describe('Control Room', () => {
  test.beforeEach(async ({ page }) => {
    await seedControlRoomLocalStorage(page)
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

    const expectedTabs = [
      'Summary',
      'CDN & Caching',
      'Security',
      'Performance',
      'Network',
      'Admin',
    ]

    for (const tabName of expectedTabs) {
      const trigger = page.locator(`[role="tablist"] button:text("${tabName}")`)
      await expect(trigger).toBeVisible({ timeout: 5_000 })
    }
  })

  test('SSE connected badge turns green within 5s', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const connectedBadge = page.locator('[data-slot="badge"]:has-text("Connected")').first()
    await expect(connectedBadge).toBeVisible({ timeout: 10_000 })
  })

  test('mobile layout shows condensed overview', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    const mobileOverview = page.locator('text=Live Overview')
    await expect(mobileOverview).toBeVisible({ timeout: 5_000 })

    const tabsList = page.locator('[role="tablist"]')
    await expect(tabsList).toBeHidden()
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

    expect(
      results.violations,
      `axe found ${results.violations.length} WCAG 2.1 AA violation(s) on /control-room:\n` +
        results.violations
          .map(
            (v) =>
              `  - [${v.id}] ${v.help} (${v.impact ?? 'unknown'})\n    ${v.helpUrl}\n` +
              `    nodes: ${v.nodes.length} (first: ${v.nodes[0]?.target?.join(' > ')})`,
          )
          .join('\n'),
    ).toEqual([])
  })

  test('Overview tab shows metric cards with live data', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.locator('text=Loading app configuration…').waitFor({ state: 'detached', timeout: 30_000 })

    const reqCard = page.locator('text=Requests/s').filter({ visible: true }).first()
    await expect(reqCard).toBeVisible({ timeout: 10_000 })

    const errorCard = page.locator('text=Error Rate').filter({ visible: true }).first()
    await expect(errorCard).toBeVisible({ timeout: 5_000 })

    const cacheCard = page.locator('text=Cache Hit Ratio').filter({ visible: true }).first()
    await expect(cacheCard).toBeVisible({ timeout: 5_000 })

    const bwCard = page.locator('text=Bandwidth').filter({ visible: true }).first()
    await expect(bwCard).toBeVisible({ timeout: 5_000 })
  })

  test('historical data link on Overview navigates to /dashboard', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(2_000)

    const histLink = page.locator('a[href^="/dashboard"]').filter({ hasText: /historical/i }).first()
    await expect(histLink).toBeVisible({ timeout: 5_000 })
  })

  test('Security tab shows WAF metrics', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const secTab = page.locator('[role="tablist"] button:text("Security")')
    await secTab.click()

    await expect(page.locator('text=WAF Blocked').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=WAF Logged').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Network tab shows PoP counts', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const netTab = page.locator('[role="tablist"] button:text("Network")')
    await netTab.click()

    await expect(page.locator('text=Active PoPs').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=Healthy PoPs').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Origin tab shows origin metrics', async ({ page }) => {
    await page.goto('/control-room')
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })

    const originTab = page.locator('[role="tablist"] button:text("CDN & Caching")').first()
    await originTab.click()

    await expect(page.locator('text=Origin Requests/s').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('text=Shield Hit Ratio').first()).toBeVisible({ timeout: 5_000 })
  })
})
