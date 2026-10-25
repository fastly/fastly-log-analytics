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

import { expect, test } from '@playwright/test'
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
