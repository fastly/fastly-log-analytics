/**
 * Page Specification E2E Suite: `/admin/clickhouse` (Admin ClickHouse Replay & Cluster Operations)
 *
 * Validates the contracts defined in `docs/pages/admin/clickhouse.md`:
 *   1. Route accessibility (`GET /admin/clickhouse` returns 200 for Admin), `ServerFooter`
 *      rendering, and ClickHouse Admin layout mounting.
 *   2. Pre-allocated zero-CLS reservation containers (`admin-clickhouse-toolbar`,
 *      `admin-clickhouse-vitals-reservation`, `admin-clickhouse-generation-reservation`,
 *      `admin-clickhouse-replay-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contract (`GET /api/admin/clickhouse/status?service_id=...` returns 200
 *      with `enabled: false, health: "disabled"` in Standard mode or `enabled: true, health: "ok"`
 *      in High-Scale mode).
 *   4. Replay validation & dry-run contract (`POST /api/admin/clickhouse/replay` enforces
 *      `limit <= 100` with 422 on `limit: 101`, rejects missing reference with 422, and returns
 *      409 `clickhouse_disabled` in Standard mode or valid dry-run/404 response in High-Scale mode).
 *   5. Interactive manual refresh workflow (`admin-clickhouse-refresh-btn`) with `X-Page-Load-ID`
 *      telemetry attribution.
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

async function resolveFirstServiceId(page: import('@playwright/test').Page): Promise<string> {
  const res = await page.request.get('/api/bootstrap');
  expect(res.status()).toBe(200);
  const body = await res.json();
  const services = Array.isArray(body?.services) ? body.services : [];
  expect(services.length).toBeGreaterThan(0);
  return services[0].service_id as string;
}

test.describe('Page Spec: /admin/clickhouse (Admin ClickHouse Replay & Cluster Operations)', () => {
  test('1. Route accessibility, ServerFooter identity, and ClickHouse Admin layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    expect(response?.status()).toBe(200);

    await expect(
      page.getByRole('heading', { name: 'ClickHouse Replay & Cluster Operations' }),
    ).toBeVisible();
    await expect(page.getByTestId('admin-clickhouse-root')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText).toMatch(/Admin View.*(standard|high-scale)/i);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load budget (CLS <= 0.05, FCP < 1200ms)', async ({
    page,
  }) => {
    await page.addInitScript(clsInitScript);

    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    expect(response?.status()).toBe(200);

    const toolbar = page.getByTestId('admin-clickhouse-toolbar');
    const vitals = page.getByTestId('admin-clickhouse-vitals-reservation');
    const generation = page.getByTestId('admin-clickhouse-generation-reservation');
    const replay = page.getByTestId('admin-clickhouse-replay-reservation');

    await expect(toolbar).toBeVisible();
    await expect(vitals).toBeVisible();
    await expect(generation).toBeVisible();
    await expect(replay).toBeVisible();

    const vitalsBox = await vitals.boundingBox();
    const generationBox = await generation.boundingBox();
    const replayBox = await replay.boundingBox();
    expect(vitalsBox?.height ?? 0).toBeGreaterThanOrEqual(200);
    expect(generationBox?.height ?? 0).toBeGreaterThanOrEqual(180);
    expect(replayBox?.height ?? 0).toBeGreaterThanOrEqual(260);

    // Wait for status badge to settle before sampling CLS & FCP
    const healthBadge = page.getByTestId('admin-clickhouse-health-badge');
    await expect(healthBadge).toHaveText(/^(ok|disabled|unavailable)$/);
    await page.waitForTimeout(300);

    const vitalsPerf = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as
        | PerformanceEntry
        | undefined;
      return {
        cls: Number((window as unknown as { __cls?: number }).__cls ?? 0),
        fcpMs: fcpEntry ? fcpEntry.startTime : 0,
      };
    });

    expect(vitalsPerf.cls).toBeLessThanOrEqual(0.05);
    if (vitalsPerf.fcpMs > 0) {
      expect(vitalsPerf.fcpMs).toBeLessThan(1200);
    }
  });

  test('3. Backend data contract for /api/admin/clickhouse/status across Standard and High-Scale modes', async ({
    page,
  }) => {
    const serviceId = await resolveFirstServiceId(page);

    const statusRes = await page.request.get(
      `/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`,
    );
    expect(statusRes.status()).toBe(200);
    const statusJson = await statusRes.json();

    expect(statusJson.service_id).toBe(serviceId);
    expect(typeof statusJson.enabled).toBe('boolean');

    if (!statusJson.enabled) {
      expect(statusJson.health).toBe('disabled');
      expect(statusJson.schema_version).toBeNull();
      expect(statusJson.publication_counts).toBeNull();
    } else {
      expect(statusJson.health).toBe('ok');
      expect(typeof statusJson.schema_version).toBe('number');
      expect(statusJson.publication_counts).toBeTruthy();
      expect(typeof statusJson.publication_counts.published).toBe('number');
      expect(typeof statusJson.publication_counts.pending).toBe('number');
    }

    // Missing service_id returns 400
    const missingSidRes = await page.request.get('/api/admin/clickhouse/status');
    expect(missingSidRes.status()).toBe(400);
  });

  test('4. Replay validation & dry-run contract for POST /api/admin/clickhouse/replay', async ({
    page,
  }) => {
    const serviceId = await resolveFirstServiceId(page);

    // 1. Rejects limit > 100 with 422
    const overLimitRes = await page.request.post('/api/admin/clickhouse/replay', {
      data: {
        service_id: serviceId,
        dataset_id: 'ds_probe_01',
        dry_run: true,
        limit: 101,
      },
    });
    expect(overLimitRes.status()).toBe(422);

    // 2. Rejects missing both dataset_id and generation with 422
    const missingRefRes = await page.request.post('/api/admin/clickhouse/replay', {
      data: {
        service_id: serviceId,
        dry_run: true,
        limit: 10,
      },
    });
    expect(missingRefRes.status()).toBe(422);

    // 3. Dry-run probe matches deployment mode (409 clickhouse_disabled in Standard, 200/404 in High-Scale)
    const statusRes = await page.request.get(
      `/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`,
    );
    const statusJson = await statusRes.json();

    const dryRunRes = await page.request.post('/api/admin/clickhouse/replay', {
      data: {
        service_id: serviceId,
        dataset_id: 'ds_nonexistent_probe',
        dry_run: true,
        limit: 10,
      },
    });

    if (!statusJson.enabled) {
      expect(dryRunRes.status()).toBe(409);
      const errBody = await dryRunRes.json();
      expect(errBody?.detail?.error).toBe('clickhouse_disabled');
    } else {
      expect([200, 404]).toContain(dryRunRes.status());
      if (dryRunRes.status() === 404) {
        const errBody = await dryRunRes.json();
        expect(errBody?.detail?.error).toBe('replay_not_found');
      }
    }
  });

  test('5. Interactive manual refresh workflow with X-Page-Load-ID telemetry attribution', async ({
    page,
  }) => {
    const observedPageLoadIds: string[] = [];
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/clickhouse/status')) {
        const headerVal = req.headers()['x-page-load-id'];
        if (headerVal) {
          observedPageLoadIds.push(headerVal);
        }
      }
    });

    await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-clickhouse-health-badge')).toHaveText(
      /^(ok|disabled|unavailable)$/,
    );

    const refreshBtn = page.getByTestId('admin-clickhouse-refresh-btn');
    await expect(refreshBtn).toBeVisible();
    await expect(refreshBtn).toBeEnabled();

    const refreshResponsePromise = page.waitForResponse(
      (res) =>
        res.url().includes('/api/admin/clickhouse/status') &&
        (res.status() === 200 || res.status() === 503),
    );
    await refreshBtn.click();
    const refreshRes = await refreshResponsePromise;
    expect([200, 503]).toContain(refreshRes.status());

    expect(observedPageLoadIds.length).toBeGreaterThan(0);
  });
});
