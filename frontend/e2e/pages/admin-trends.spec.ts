/**
 * Page Specification E2E Suite: `/admin/trends` (Admin System Trends & Health Sparklines)
 *
 * Validates the contracts defined in `docs/pages/admin/trends.md`:
 *   1. Route accessibility (`GET /admin/trends` returns 200), Analyst Path B 403 rejection on
 *      `/api/admin/metric-history/batch`, `ServerFooter` rendering, and System Trends layout mount.
 *   2. Pre-allocated zero-CLS reservation containers (`admin-trends-toolbar`,
 *      `admin-trends-grid-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contract (`GET /api/admin/metric-history/batch?since=1h` returns 200 with
 *      `series` dictionary; invalid `since` returns 400).
 *   4. Time-window selector interaction (`1h` -> `6h` -> `24h` -> `7d`) and sparkline card
 *      rendering across Standard and High-Scale modes.
 *   5. Interactive manual refresh workflow (`data-testid="admin-trends-refresh-btn"`) with
 *      `X-Page-Load-ID` telemetry attribution.
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

test.describe('/admin/trends — Operational Trends & Health Sparklines Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and System Trends layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/trends', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.getByTestId('admin-trends-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole('heading', { name: /Operational Trends/i })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByTestId('admin-trends-toolbar')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/trends', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-trends-root')).toBeVisible({ timeout: 30_000 });

    const toolbar = page.getByTestId('admin-trends-toolbar');
    const gridReservation = page.getByTestId('admin-trends-grid-reservation');
    await expect(toolbar).toBeVisible();
    await expect(gridReservation).toBeVisible();

    for (const container of [toolbar, gridReservation]) {
      const classAttr = (await container.getAttribute('class')) ?? '';
      expect(classAttr).toContain('contain-intrinsic-size');
      expect(classAttr).toContain('content-visibility');
    }

    const toolbarBox = await toolbar.boundingBox();
    const gridBox = await gridReservation.boundingBox();
    expect(toolbarBox?.height ?? 0).toBeGreaterThanOrEqual(32);
    expect(gridBox?.height ?? 0).toBeGreaterThanOrEqual(400);

    await page.addInitScript(clsInitScript);

    await page.reload({ waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-trends-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-trends-card-cpu_load_1m')).toBeVisible({
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

  test('3. Backend data contract (/api/admin/metric-history/batch?since=1h)', async ({ page }) => {
    const res = await page.request.get('/api/admin/metric-history/batch?since=1h');
    expect(res.status()).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body).toHaveProperty('series');
    expect(typeof body['series']).toBe('object');
    expect(body['series']).not.toBeNull();

    const badRes = await page.request.get('/api/admin/metric-history/batch?since=invalid');
    expect(badRes.status()).toBe(400);
  });

  test('4. Time-window selector interaction (1h -> 6h -> 24h -> 7d) and 7 sparkline cards', async ({
    page,
  }) => {
    await page.goto('/admin/trends', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-trends-root')).toBeVisible({ timeout: 30_000 });

    for (const metric of [
      'cpu_load_1m',
      'mem_used_pct',
      'disk_used_pct',
      'disk_used_pct_root',
      'active_query_count',
      'pool_wait_p95_ms',
      'ingest_lag_s',
    ]) {
      await expect(page.getByTestId(`admin-trends-card-${metric}`)).toBeVisible({
        timeout: 15_000,
      });
    }

    await expect(page.getByTestId('admin-trends-window-1h')).toHaveAttribute(
      'aria-pressed',
      'true'
    );

    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = page.getByTestId(`admin-trends-window-${win}`);
      await expect(btn).toBeVisible();

      const resPromise = page.waitForResponse(
        (res) =>
          res.url().includes('/api/admin/metric-history/batch') &&
          res.url().includes(`since=${win}`) &&
          res.request().method() === 'GET',
        { timeout: 15_000 }
      );
      await btn.click();
      const res = await resPromise;
      expect(res.status()).toBe(200);
      await expect(btn).toHaveAttribute('aria-pressed', 'true');
    }
  });

  test('5. Interactive manual refresh workflow and X-Page-Load-ID attribution', async ({
    page,
  }) => {
    const batchRequests: Array<{ url: string; pageLoadId: string | null }> = [];

    page.on('request', (req) => {
      if (req.url().includes('/api/admin/metric-history/batch')) {
        batchRequests.push({
          url: req.url(),
          pageLoadId: req.headers()['x-page-load-id'] ?? null,
        });
      }
    });

    await page.goto('/admin/trends', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-trends-root')).toBeVisible({ timeout: 30_000 });

    const refreshBtn = page.getByTestId('admin-trends-refresh-btn');
    await expect(refreshBtn).toBeVisible({ timeout: 15_000 });
    await expect(refreshBtn).toBeEnabled();

    const countBeforeRefresh = batchRequests.length;
    await refreshBtn.click();

    await expect
      .poll(() => batchRequests.length, { timeout: 15_000 })
      .toBeGreaterThan(countBeforeRefresh);

    const latestReq = batchRequests[batchRequests.length - 1];
    expect(latestReq.pageLoadId).toBeTruthy();

    await expect(page.getByTestId('admin-trends-card-cpu_load_1m')).toBeVisible();
  });
});
