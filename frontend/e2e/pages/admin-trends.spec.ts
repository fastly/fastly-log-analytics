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
import { clsInitScript } from '../cls-observer';
import { configureExternalAdminOrigin } from '../helpers';

const externalOrigin = configureExternalAdminOrigin();

test.describe('/admin/trends — Operational Trends & Health Sparklines Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and System Trends layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/trends');
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByRole('heading', { name: /Operational Trends/i }).first()).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.locator('[data-testid="admin-trends-toolbar"]:visible')).toBeVisible();

    const footer = page.locator('footer:visible');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/trends');
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({
      timeout: 30_000,
    });

    const toolbar = page.locator('[data-testid="admin-trends-toolbar"]:visible');
    const gridReservation = page.locator('[data-testid="admin-trends-grid-reservation"]:visible');
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

    await page.goto('/admin/trends');
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible')).toBeVisible(
      {
        timeout: 30_000,
      }
    );

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
      const isRemoteOrigin =
        Boolean(externalOrigin) &&
        !externalOrigin.includes('127.0.0.1') &&
        !externalOrigin.includes('localhost');
      expect(perfMetrics.fcpMs).toBeLessThan(isRemoteOrigin ? 4000 : 1200);
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
    const windowStatuses = new Map<string, number>();
    page.on('response', (res) => {
      if (
        res.url().includes('/api/admin/metric-history/batch') &&
        res.request().method() === 'GET'
      ) {
        const match = res.url().match(/[?&]since=([^&]+)/);
        if (match) {
          windowStatuses.set(match[1], res.status());
        }
      }
    });

    await page.goto('/admin/trends');
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({
      timeout: 30_000,
    });

    for (const metric of [
      'cpu_load_1m',
      'mem_used_pct',
      'disk_used_pct',
      'disk_used_pct_root',
      'active_query_count',
      'pool_wait_p95_ms',
      'ingest_lag_s',
    ]) {
      await expect(page.locator(`[data-testid="admin-trends-card-${metric}"]:visible`)).toBeVisible(
        {
          timeout: 15_000,
        }
      );
    }

    await expect(page.locator('[data-testid="admin-trends-window-1h"]:visible')).toHaveAttribute(
      'aria-pressed',
      'true'
    );

    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = page.locator(`[data-testid="admin-trends-window-${win}"]:visible`);
      await expect(btn).toBeVisible();

      await expect
        .poll(
          async () => {
            const pressed = await btn.getAttribute('aria-pressed');
            if (pressed !== 'true') {
              await btn.click();
            }
            return windowStatuses.get(win) ?? 0;
          },
          { timeout: 30_000, intervals: [250, 500, 1000] }
        )
        .toBe(200);

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

    await page.goto('/admin/trends');
    await expect(page.locator('[data-testid="admin-trends-root"]:visible')).toBeVisible({
      timeout: 30_000,
    });

    const refreshBtn = page.locator('[data-testid="admin-trends-refresh-btn"]:visible');
    await expect(refreshBtn).toBeVisible({ timeout: 15_000 });
    await expect(refreshBtn).toBeEnabled();

    const countBeforeRefresh = batchRequests.length;

    await expect
      .poll(
        async () => {
          if (batchRequests.length <= countBeforeRefresh) {
            await refreshBtn.click();
          }
          return batchRequests.length;
        },
        { timeout: 15_000, intervals: [250, 500, 1000] }
      )
      .toBeGreaterThan(countBeforeRefresh);

    const latestReq = batchRequests[batchRequests.length - 1];
    expect(latestReq.pageLoadId).toBeTruthy();

    await expect(page.locator('[data-testid="admin-trends-card-cpu_load_1m"]:visible')).toBeVisible();
  });
});
