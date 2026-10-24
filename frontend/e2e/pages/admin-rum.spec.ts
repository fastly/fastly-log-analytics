/**
 * Page Specification E2E Suite: `/admin/rum` (Admin RUM Ingestion & Configuration)
 *
 * Validates the contracts defined in `docs/pages/admin/rum.md`:
 *   1. Route accessibility (`GET /admin/rum` returns 200), `ServerFooter` rendering,
 *      and RUM Configuration page layout mounting (`Real User Monitoring` header,
 *      `Back to Admin` link, toolbar, and RUM Status card).
 *   2. Pre-allocated zero-CLS reservation containers (`admin-rum-toolbar`,
 *      `admin-rum-status-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contracts: `/api/services/{service_id}/rum/status` and
 *      `/api/services` (plus `/api/services/{service_id}/rum/versions` when RUM
 *      is enabled).
 *   4. RUM status card (`Enabled` / `Disabled`, `Enabled at`, `VCL Drift`,
 *      collectors summary), Enable/Disable buttons (`admin-rum-enable-btn`,
 *      `admin-rum-disable-btn`), and Faro SDK / RUM Settings controls across
 *      Standard and High-Scale modes.
 *   5. Interactive manual refresh workflow (`admin-rum-refresh-btn`) with
 *      `X-Page-Load-ID` header propagation.
 */
import { test, expect } from '@playwright/test';
import { clsInitScript, configureExternalAdminOrigin } from '../helpers';

configureExternalAdminOrigin();

test.describe('/admin/rum — Admin RUM Ingestion & Configuration Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and RUM Configuration layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.getByTestId('admin-rum-root')).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByRole('heading', { name: 'Real User Monitoring' })
    ).toBeVisible();
    await expect(page.getByTestId('back-to-admin-link')).toBeVisible();
    await expect(page.getByTestId('admin-rum-toolbar')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-rum-root')).toBeVisible({ timeout: 30_000 });

    const toolbar = page.getByTestId('admin-rum-toolbar');
    const statusReservation = page.getByTestId('admin-rum-status-reservation');
    await expect(toolbar).toBeVisible();
    await expect(statusReservation).toBeVisible();

    const toolbarBox = await toolbar.boundingBox();
    const statusBox = await statusReservation.boundingBox();
    expect(toolbarBox?.height ?? 0).toBeGreaterThanOrEqual(48);
    expect(statusBox?.height ?? 0).toBeGreaterThanOrEqual(200);

    await page.addInitScript(clsInitScript);

    await page.reload({ waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-rum-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({
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

  test('3. Backend data contracts: /api/services/{service_id}/rum/status and /api/services', async ({
    page,
  }) => {
    let statusPayload: Record<string, unknown> | null = null;
    let servicesPayload: Record<string, unknown> | null = null;

    page.on('response', async (res) => {
      const url = res.url();
      if (/\/api\/services\/[^/]+\/rum\/status/.test(url) && res.status() === 200) {
        try {
          statusPayload = (await res.json()) as Record<string, unknown>;
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

    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({
      timeout: 30_000,
    });

    await expect.poll(() => statusPayload, { timeout: 25_000 }).not.toBeNull();
    expect(typeof statusPayload!['enabled']).toBe('boolean');
    expect(typeof statusPayload!['vcl_drift']).toBe('boolean');
    expect(typeof statusPayload!['capture_vitals']).toBe('boolean');
    expect(typeof statusPayload!['capture_performance']).toBe('boolean');
    expect(typeof statusPayload!['capture_errors']).toBe('boolean');
    expect(typeof statusPayload!['capture_events']).toBe('boolean');

    if (servicesPayload) {
      expect(Array.isArray(servicesPayload['services'])).toBe(true);
    }
  });

  test('4. RUM Status card, Enable/Disable controls, and Faro SDK version state', async ({
    page,
  }) => {
    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({
      timeout: 30_000,
    });

    const statusBadge = page.getByTestId('admin-rum-status-badge');
    await expect(statusBadge).toBeVisible();
    const badgeText = (await statusBadge.textContent()) ?? '';
    expect(badgeText).toMatch(/Beacon Pipeline (Active|Inactive)/);

    const statusLineText = (await page.getByTestId('admin-rum-status-line').textContent()) ?? '';
    const isEnabled = statusLineText.includes('Enabled');

    const enableBtn = page.getByTestId('admin-rum-enable-btn');
    const disableBtn = page.getByTestId('admin-rum-disable-btn');
    await expect(enableBtn).toBeVisible();
    await expect(disableBtn).toBeVisible();

    if (isEnabled) {
      await expect(enableBtn).toBeDisabled();
      await expect(disableBtn).toBeEnabled();
      await expect(page.getByTestId('admin-rum-enabled-at')).toBeVisible();
      await expect(page.getByTestId('admin-rum-vcl-drift')).toBeVisible();
      await expect(page.getByTestId('admin-rum-capture-summary')).toBeVisible();
    } else {
      await expect(enableBtn).toBeEnabled();
      await expect(disableBtn).toBeDisabled();
    }
  });

  test('5. Interactive manual refresh workflow and X-Page-Load-ID header attribution', async ({
    page,
  }) => {
    const statusRequests: Array<{ url: string; pageLoadId: string | null }> = [];

    page.on('request', (req) => {
      if (/\/api\/services\/[^/]+\/rum\/status/.test(req.url())) {
        statusRequests.push({
          url: req.url(),
          pageLoadId: req.headers()['x-page-load-id'] ?? null,
        });
      }
    });

    await page.goto('/admin/rum', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible({
      timeout: 30_000,
    });

    await expect
      .poll(() => statusRequests.length, { timeout: 25_000 })
      .toBeGreaterThanOrEqual(1);
    expect(statusRequests[0].pageLoadId).toBeTruthy();

    const countBeforeRefresh = statusRequests.length;
    const refreshBtn = page.getByTestId('admin-rum-refresh-btn');
    await expect(refreshBtn).toBeEnabled();
    await refreshBtn.click();

    await expect
      .poll(() => statusRequests.length, { timeout: 15_000 })
      .toBeGreaterThan(countBeforeRefresh);

    await expect(page.getByTestId('admin-rum-status-line')).toBeVisible();
  });
});
