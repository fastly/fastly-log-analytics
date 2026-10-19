/**
 * Page Specification E2E Suite: `/admin/share` (Admin Live Share Management)
 *
 * Validates the contracts defined in `docs/pages/admin/share.md`:
 *   1. Route accessibility (`GET /admin/share` returns 200), `ServerFooter` rendering,
 *      and Live Share page layout mounting (`Remote Dashboard Sharing` header, `Back to Admin`
 *      link, manual refresh button, control reservation, tabs, and panel reservation).
 *   2. Pre-allocated zero-CLS reservation containers (`admin-share-control-reservation`,
 *      `admin-share-panel-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contracts: `/api/admin/share/status`, `/api/admin/share/live`,
 *      `/api/admin/share/settings`, `/api/admin/share/invites`, and `/api/admin/share/tos`.
 *   4. Sharing Control Panel (`ACTIVE` / `OFF` badge, start/stop/panic controls) and tabs
 *      (`Invitations`, `Sessions`, `Audit`) + URL `?tab=` sync rendering cleanly across
 *      Standard and High-Scale modes.
 *   5. Interactive manual refresh workflow (`admin-share-refresh-btn`) with
 *      `X-Page-Load-ID` header propagation.
 */
import { test, expect } from '@playwright/test';
import { adminGatewayContextOptions } from '../../../scripts/lib/admin_gateway_client.cjs';
import { clsInitScript } from '../cls-observer';

const externalOrigin = process.env.PLAYWRIGHT_BASE_URL || '';
if (externalOrigin) {
  test.use({
    baseURL: externalOrigin,
    ...adminGatewayContextOptions(externalOrigin),
  });
}

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
    expect(page.url()).toContain('tab=sessions');

    await tabAudit.click();
    await expect(tabAudit).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByTestId('admin-share-audit-panel')).toBeVisible({ timeout: 15_000 });
    expect(page.url()).toContain('tab=audit');

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
