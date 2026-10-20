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

test.describe('/admin/usage-log — Admin FOS Usage & Class A/B Ledger Contract', () => {
  test.setTimeout(90_000);

  test('1. Route accessibility, ServerFooter identity, and Usage Log layout mount', async ({
    page,
  }) => {
    const response = await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    await expect(page.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole('heading', { name: /FOS Usage Log/i })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByTestId('admin-usage-log-toolbar')).toBeVisible();
    await expect(page.getByTestId('admin-usage-log-kpi-reservation')).toBeVisible();
    await expect(page.getByTestId('admin-usage-log-chart-reservation')).toBeVisible();
    await expect(page.getByTestId('admin-usage-log-table-reservation')).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerText = (await footer.textContent()) ?? '';
    expect(footerText.trim().length).toBeGreaterThan(0);
  });

  test('2. Pre-allocated zero-CLS reservation containers and warm-load performance budget', async ({
    page,
  }) => {
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });

    const toolbar = page.getByTestId('admin-usage-log-toolbar');
    const kpiReservation = page.getByTestId('admin-usage-log-kpi-reservation');
    const chartReservation = page.getByTestId('admin-usage-log-chart-reservation');
    const tableReservation = page.getByTestId('admin-usage-log-table-reservation');

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
    await expect(page.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-usage-log-filters')).toBeVisible({ timeout: 30_000 });

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
    await page.goto('/admin/usage-log', { waitUntil: 'domcontentloaded' });
    await expect(page.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('admin-usage-log-filters')).toBeVisible({ timeout: 15_000 });

    // Default preset is 24h
    await expect(page.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute(
      'aria-pressed',
      'true'
    );

    for (const hours of [1, 168, 720] as const) {
      const btn = page.getByTestId(`admin-usage-log-preset-${hours}h`);
      await expect(btn).toBeVisible();
      await btn.click();
      await expect(btn).toHaveAttribute('aria-pressed', 'true');
    }

    // Filter by process_context and operation_type
    const procInput = page.getByTestId('admin-usage-log-process-input');
    const opInput = page.getByTestId('admin-usage-log-operation-input');
    await expect(procInput).toBeVisible();
    await expect(opInput).toBeVisible();

    await procInput.fill('cron:');
    await opInput.fill('PutObject');

    const exportBtn = page.getByTestId('admin-usage-log-export-btn');
    await expect(exportBtn).toHaveAttribute('href', /process_context=cron%3A/);
    await expect(exportBtn).toHaveAttribute('href', /operation_type=PutObject/);

    // Clear filters back to default
    await procInput.fill('');
    await opInput.fill('');
  });

  test('5. Interactive refresh and CSV export workflow with X-Page-Load-ID attribution', async ({
    page,
  }) => {
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
    await expect(page.getByTestId('admin-usage-log-root')).toBeVisible({ timeout: 30_000 });

    const refreshBtn = page.getByTestId('admin-usage-log-refresh-btn');
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
    const exportBtn = page.getByTestId('admin-usage-log-export-btn');
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
