/**
 * Page Specification E2E Suite: `/admin/session-scoring` (Admin Session Scoring Configuration)
 *
 * Validates the contracts defined in `docs/pages/admin/session-scoring.md`:
 *   1. Route accessibility (`GET /admin/session-scoring` returns 200), `ServerFooter` rendering,
 *      and Session Scoring page layout mounting (`Session Scoring` header, `Back to Admin` link,
 *      manual refresh button, status reservation, and tabs).
 *   2. Pre-allocated zero-CLS reservation containers (`admin-session-scoring-status-reservation`,
 *      `admin-session-scoring-overview-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contracts: `/api/services/{service_id}/scoring/config`,
 *      `/api/services/{service_id}/scoring/analytics`, `/api/services/{service_id}/session-scoring/status`,
 *      and `/api/services`.
 *   4. Session Scoring status panel (`Enabled` / `Disabled` badge, Enable or Redeploy/Disable
 *      buttons) and tabs (`Overview`, `Labels`, `Matrix history`, `Audit`) rendering cleanly
 *      across Standard and High-Scale modes.
 *   5. Interactive manual refresh workflow (`admin-session-scoring-refresh-btn`) with
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

  test('3. Backend data contracts: /api/services/{service_id}/scoring/config, /session-scoring/status, and /api/services', async ({
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
      const aliasRes = await request.get(
        `/api/services/${encodeURIComponent(resolvedServiceId)}/session-scoring/status`
      );
      expect(aliasRes.status()).toBe(200);
      const aliasJson = (await aliasRes.json()) as Record<string, unknown>;
      expect(typeof aliasJson['enabled']).toBe('boolean');
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
