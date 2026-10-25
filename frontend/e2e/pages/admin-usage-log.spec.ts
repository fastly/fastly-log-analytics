/**
 * Page Specification E2E Suite: `/admin/usage-log` (Admin FOS Usage & Class A/B Ledger)
 *
 * Validates the contracts defined in `docs/pages/admin/usage-log.md`:
 *   1. Route accessibility (`GET /admin/usage-log` returns 200 for Admin), `ServerFooter`
 *      rendering, and Usage Log layout mounting.
 *   2. Pre-allocated zero-CLS reservation containers (`admin-usage-log-toolbar`,
 *      `admin-usage-log-kpi-reservation`, `admin-usage-log-chart-reservation`,
 *      `admin-usage-log-table-reservation`) and warm-load performance budget
 *      (`CLS <= 0.05`, `FCP < 1200ms`).
 *   3. Backend data contracts (`GET /api/admin/usage-log`, `GET /api/admin/usage-logging`,
 *      and `GET /api/admin/system-jobs` return 200 with expected schemas; invalid
 *      `PATCH /api/admin/usage-logging` returns 400).
 *   4. Filter & granularity interaction (switching preset time windows, filtering by
 *      process_context/operation_type, and Ingest Accounting preset selector) across
 *      Standard and High-Scale modes.
 *   5. Interactive manual refresh and CSV export workflow with `X-Page-Load-ID`
 *      telemetry attribution.
 */
import { test, expect, type Page } from '@playwright/test';
import { clsInitScript, configureExternalAdminOrigin, readClsScore, resolveFirstServiceId } from '../helpers';

configureExternalAdminOrigin();

test.describe('/admin/usage-log — Admin FOS Usage & Class A/B Ledger Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and Usage Log layout mount', async ({
    page,
  }) => {
    const main = page.locator('#main');
    const response = await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(main.getByRole('heading', { name: /FOS Usage Log/i })).toBeVisible({
      timeout: 15_000,
    });
    await expect(main.getByTestId('admin-usage-log-toolbar')).toBeVisible();
    await expect(main.getByTestId('admin-usage-log-kpi-reservation')).toBeVisible();
    await expect(main.getByTestId('admin-usage-log-chart-reservation')).toBeVisible();
    await expect(main.getByTestId('admin-usage-log-table-reservation')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    const main = page.locator('#main');
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });

    const toolbar = main.getByTestId('admin-usage-log-toolbar');
    const kpiReservation = main.getByTestId('admin-usage-log-kpi-reservation');
    const chartReservation = main.getByTestId('admin-usage-log-chart-reservation');
    const tableReservation = main.getByTestId('admin-usage-log-table-reservation');

    for (const container of [toolbar, kpiReservation, chartReservation, tableReservation]) {
      await expect(container).toBeVisible();
      const classAttr = (await container.getAttribute('class')) ?? '';
      expect(classAttr).toContain('contain-intrinsic-size');
      expect(classAttr).toContain('content-visibility');
    }

    const toolbarBox = await toolbar.boundingBox();
    const kpiBox = await kpiReservation.boundingBox();
    const chartBox = await chartReservation.boundingBox();
    const tableBox = await tableReservation.boundingBox();
    expect(toolbarBox?.height ?? 0).toBeGreaterThanOrEqual(32);
    expect(kpiBox?.height ?? 0).toBeGreaterThanOrEqual(150);
    expect(chartBox?.height ?? 0).toBeGreaterThanOrEqual(340);
    expect(tableBox?.height ?? 0).toBeGreaterThanOrEqual(450);

    await page.addInitScript(clsInitScript);

    await page.reload({ waitUntil: 'domcontentloaded' });
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(main.getByTestId('admin-usage-log-filters')).toBeVisible({ timeout: 30_000 });

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

  test('3. Backend data contracts (/api/admin/usage-log, /api/admin/usage-logging, /api/admin/system-jobs)', async ({
    page,
  }) => {
    // Discover active service_id from /api/services so /api/admin/usage-log can resolve get_source
    const svcRes = await page.request.get('/api/services');
    expect(svcRes.status()).toBe(200);
    const svcBody = (await svcRes.json()) as {
      services?: Array<{ service_id?: string; id?: string }>;
      active_service_id?: string;
    };
    const serviceId =
      svcBody.active_service_id ||
      svcBody.services?.[0]?.service_id ||
      svcBody.services?.[0]?.id ||
      '';

    const ulRes = await page.request.get(
      `/api/admin/usage-log?service_id=${encodeURIComponent(serviceId)}&page_size=25`
    );
    expect(ulRes.status()).toBe(200);
    const ulBody = (await ulRes.json()) as Record<string, unknown>;
    expect(Array.isArray(ulBody['entries'])).toBe(true);
    expect(typeof ulBody['total']).toBe('number');
    expect(typeof ulBody['aggregate']).toBe('object');
    const agg = ulBody['aggregate'] as Record<string, unknown>;
    expect(typeof agg['total_class_a']).toBe('number');
    expect(typeof agg['total_class_b']).toBe('number');
    expect(typeof agg['total_cdn_bytes']).toBe('number');
    expect(typeof agg['estimated_cost_total']).toBe('number');

    const cfgRes = await page.request.get('/api/admin/usage-logging');
    expect(cfgRes.status()).toBe(200);
    const cfgBody = (await cfgRes.json()) as Record<string, unknown>;
    expect(typeof cfgBody['enabled']).toBe('boolean');
    expect(typeof cfgBody['retention_days']).toBe('number');
    expect(typeof cfgBody['class_a_rate_per_1k']).toBe('number');
    expect(typeof cfgBody['class_b_rate_per_10k']).toBe('number');

    // Validation check: invalid billing_zone must return 400
    const badPatch = await page.request.patch('/api/admin/usage-logging', {
      data: { billing_zone: 'INVALID_ZONE' },
    });
    expect(badPatch.status()).toBe(400);

    const jobsRes = await page.request.get('/api/admin/system-jobs');
    expect(jobsRes.status()).toBe(200);
    const jobsBody = (await jobsRes.json()) as { jobs?: Array<{ id: string; name: string }> };
    expect(Array.isArray(jobsBody.jobs)).toBe(true);
    const jobIds = (jobsBody.jobs ?? []).map((j) => j.id);
    expect(jobIds).toEqual(
      expect.arrayContaining(['bot_data_refresh', 'rdns_enrichment', 'share_audit_purge'])
    );
  });

  test('4. Filter & granularity interaction across preset windows and filter inputs', async ({
    page,
  }) => {
    const main = page.locator('#main');
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(main.getByTestId('admin-usage-log-filters')).toBeVisible({ timeout: 15_000 });

    // Default preset is 24h
    await expect(main.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute(
      'aria-pressed',
      'true'
    );

    for (const hours of [1, 168, 720] as const) {
      const btn = main.getByTestId(`admin-usage-log-preset-${hours}h`);
      await expect(btn).toBeVisible();
      await btn.click();
      await expect(btn).toHaveAttribute('aria-pressed', 'true');
    }

    // Filter by process_context and operation_type
    const procInput = main.getByTestId('admin-usage-log-process-input');
    const opInput = main.getByTestId('admin-usage-log-operation-input');
    await expect(procInput).toBeVisible();
    await expect(opInput).toBeVisible();

    await procInput.fill('cron:');
    await opInput.fill('PutObject');

    const exportBtn = main.getByTestId('admin-usage-log-export-btn');
    await expect(exportBtn).toHaveAttribute('href', /process_context=cron%3A/);
    await expect(exportBtn).toHaveAttribute('href', /operation_type=PutObject/);

    // Clear filters back to default
    await procInput.fill('');
    await opInput.fill('');
  });

  test('5. Interactive refresh and CSV export workflow with X-Page-Load-ID attribution', async ({
    page,
  }) => {
    const main = page.locator('#main');
    const usageLogRequests: Array<{ url: string; pageLoadId: string | null }> = [];

    page.on('request', (req) => {
      if (
        req.url().includes('/api/admin/usage-log') ||
        req.url().includes('/api/admin/log-accounting')
      ) {
        usageLogRequests.push({
          url: req.url(),
          pageLoadId: req.headers()['x-page-load-id'] ?? null,
        });
      }
    });

    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    await expect(main.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });

    const refreshBtn = main.getByTestId('admin-usage-log-refresh-btn');
    await expect(refreshBtn).toBeVisible({ timeout: 15_000 });
    await expect(refreshBtn).toBeEnabled({ timeout: 30_000 });

    const countBeforeRefresh = usageLogRequests.length;
    await refreshBtn.click();

    await expect
      .poll(() => usageLogRequests.length, { timeout: 15_000 })
      .toBeGreaterThan(countBeforeRefresh);

    const latestWithHeader = usageLogRequests.filter((r) => Boolean(r.pageLoadId));
    expect(latestWithHeader.length).toBeGreaterThan(0);

    // Verify CSV export endpoint contract directly
    const exportBtn = main.getByTestId('admin-usage-log-export-btn');
    await expect(exportBtn).toBeVisible();
    const exportHref = await exportBtn.getAttribute('href');
    expect(exportHref).toBeTruthy();

    const csvRes = await page.request.get(exportHref!);
    expect(csvRes.status()).toBe(200);
    expect(csvRes.headers()['content-type'] ?? '').toContain('text/csv');
    const csvText = await csvRes.text();
    expect(csvText.split('\n')[0]).toContain(
      'timestamp,service_id,operation_class,operation_type,url,bytes,duration_ms,function_name,process_context,status,count'
    );
  });
});


async function isClickHouseEnabledForEnv(page: Page): Promise<boolean> {
  const serviceId = await resolveFirstServiceId(page);
  const statusRes = await page.request.get(
    `/api/admin/clickhouse/status?service_id=${encodeURIComponent(serviceId)}`,
  );
  if (statusRes.status() !== 200) return false;
  const statusJson = await statusRes.json();
  return Boolean(statusJson.enabled);
}

test.describe('Page Spec: /admin/clickhouse (Admin ClickHouse Replay & Cluster Operations)', () => {
  test('1. Route accessibility, ServerFooter identity, and ClickHouse Admin layout mount', async ({
    page,
  }) => {
    const enabled = await isClickHouseEnabledForEnv(page);
    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    const status = response?.status() ?? 0;
    expect([200, 404]).toContain(status);

    if (!enabled || status === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0);
      return;
    }

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
    const enabled = await isClickHouseEnabledForEnv(page);
    await page.addInitScript(clsInitScript);

    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    const status = response?.status() ?? 0;
    expect([200, 404]).toContain(status);

    if (!enabled || status === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0);
    } else {
      const toolbar = page.getByTestId('admin-clickhouse-toolbar');
      const vitals = page.getByTestId('admin-clickhouse-vitals-reservation');
      const generation = page.getByTestId('admin-clickhouse-generation-reservation');
      const replay = page.getByTestId('admin-clickhouse-replay-reservation');

      await expect(toolbar).toBeVisible();
      await expect(vitals).toBeVisible();
      await expect(generation).toBeVisible();
      await expect(replay).toBeVisible();

      const heightOf = (locator: typeof vitals) => async () => (await locator.boundingBox())?.height ?? 0;
      await expect.poll(heightOf(vitals)).toBeGreaterThanOrEqual(200);
      await expect.poll(heightOf(generation)).toBeGreaterThanOrEqual(180);
      await expect.poll(heightOf(replay)).toBeGreaterThanOrEqual(260);

      const healthBadge = page.getByTestId('admin-clickhouse-health-badge');
      await expect(healthBadge).toHaveText(/^(ok|disabled|unavailable)$/);
      await page.waitForTimeout(300);
    }

    const clsScore = await readClsScore(page);
    const vitalsPerf = await page.evaluate(() => {
      const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as
        | PerformanceEntry
        | undefined;
      return {
        cls: Number(window.__clsScore ?? 0),
        fcpMs: fcpEntry ? fcpEntry.startTime : 0,
      };
    });

    expect(clsScore).toBeLessThanOrEqual(0.05);
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

    const missingSidRes = await page.request.get('/api/admin/clickhouse/status');
    expect(missingSidRes.status()).toBe(400);
  });

  test('4. Replay validation & dry-run contract for POST /api/admin/clickhouse/replay', async ({
    page,
  }) => {
    const serviceId = await resolveFirstServiceId(page);

    const overLimitRes = await page.request.post('/api/admin/clickhouse/replay', {
      data: {
        service_id: serviceId,
        dataset_id: 'ds_probe_01',
        dry_run: true,
        limit: 101,
      },
    });
    expect(overLimitRes.status()).toBe(422);

    const missingRefRes = await page.request.post('/api/admin/clickhouse/replay', {
      data: {
        service_id: serviceId,
        dry_run: true,
        limit: 10,
      },
    });
    expect(missingRefRes.status()).toBe(422);

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
    const enabled = await isClickHouseEnabledForEnv(page);
    const observedPageLoadIds: string[] = [];
    page.on('request', (req) => {
      if (req.url().includes('/api/admin/clickhouse/status')) {
        const headerVal = req.headers()['x-page-load-id'];
        if (headerVal) {
          observedPageLoadIds.push(headerVal);
        }
      }
    });

    const response = await page.goto('/admin/clickhouse', { waitUntil: 'domcontentloaded' });
    if (!enabled || response?.status() === 404) {
      await expect(page.getByTestId('admin-clickhouse-root')).toHaveCount(0);
      expect(observedPageLoadIds.length).toBe(0);
      return;
    }

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
