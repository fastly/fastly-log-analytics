/**
 * E2E Page Contract Test: Usage & Cost Estimator (/usage)
 *
 * Implements the verification checklist specified in docs/pages/usage-and-cost.md:
 * 1. Route accessibility & environment indicator verification.
 * 2. Instant shell, pre-allocated layout & zero CLS containers.
 * 3. Data contract & schema validation on /api/usage/* endpoints.
 * 4. Core stat cards, charts, and Cost Estimator mounted.
 * 5. Interactive Delivery Billing Zone (MBZ100, MBZ200, Global) switching & egress recalculation.
 * 6. Zero error cards or broken layout containers.
 * 7. Performance budget: LCP < 1500ms, CLS <= 0.05 on warm reload.
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import { installClsObserver, measureWarmPaintAndCls, readClsScore } from '../helpers'

test.describe('Usage & Cost Estimator Page Contract (/usage)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Verify intrinsic size reservation classes across KPI grids and sections
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(3)
  })

  test('3. Data contract & schema validation on /api/usage endpoints', async ({ page }) => {
    // 1. Storage endpoint
    const storageRes = await page.request.get('/api/usage/current-storage')
    expect(storageRes.ok()).toBeTruthy()
    const storageData = await storageRes.json()
    expect(storageData).toHaveProperty('live_bytes')
    expect(storageData).toHaveProperty('total_billed_gb_hours')

    // 2. Prefill rates endpoint
    const ratesRes = await page.request.get('/api/usage/prefill/rates')
    expect(ratesRes.ok()).toBeTruthy()
    const ratesData = await ratesRes.json()
    expect(ratesData).toHaveProperty('class_a_rate_per_1k')
    expect(ratesData).toHaveProperty('cdn_egress_rate_per_gb')
    expect(ratesData).toHaveProperty('billing_zone')
    expect(['MBZ100', 'MBZ200', 'Global']).toContain(ratesData.billing_zone)

    // 3. Operations endpoint
    const opsRes = await page.request.get('/api/usage/operations?by=hour')
    expect(opsRes.ok()).toBeTruthy()
    const opsData = await opsRes.json()
    expect(opsData).toHaveProperty('total_class_a')
    expect(opsData).toHaveProperty('total_class_b')
    expect(Array.isArray(opsData.data)).toBeTruthy()

    // 4. Bandwidth endpoint
    const bwRes = await page.request.get('/api/usage/bandwidth?by=hour')
    expect(bwRes.ok()).toBeTruthy()
    const bwData = await bwRes.json()
    expect(bwData).toHaveProperty('total_bytes')
    expect(Array.isArray(bwData.data)).toBeTruthy()

    // 5. Log activity endpoint
    const actRes = await page.request.get('/api/usage/log-activity?by=hour')
    expect(actRes.ok()).toBeTruthy()
    const actData = await actRes.json()
    expect(actData).toHaveProperty('total_rows')
    expect(Array.isArray(actData.data)).toBeTruthy()
  })

  test('4. Core stat cards, charts, and Cost Estimator mounted', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Stat cards
    await expect(page.getByText('Storage Impact (Period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Class A Ops (period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Class B Ops (period)')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Estimated Incurred Cost')).toBeVisible({ timeout: 15_000 })

    // Analytics cards
    await expect(page.getByText('FOS Operations')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('CDN Bandwidth')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Estimated Cost')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Log Activity (Processed)')).toBeVisible({ timeout: 15_000 })

    // Cost Estimator
    await expect(page.getByText('Cost Estimator')).toBeVisible({ timeout: 15_000 })
  })

  test('5. Delivery Billing Zone interactive switching', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Scroll down to Cost Estimator if needed
    const costEstimator = page.getByText('Cost Estimator')
    await costEstimator.scrollIntoViewIfNeeded()
    await expect(costEstimator).toBeVisible({ timeout: 15_000 })

    // Check for Billing Zone buttons: MBZ100, MBZ200, Global
    const zoneMBZ100 = page.getByRole('button', { name: 'MBZ100' })
    const zoneMBZ200 = page.getByRole('button', { name: 'MBZ200' })
    const zoneGlobal = page.getByRole('button', { name: 'Global' })

    await expect(zoneMBZ100).toBeVisible({ timeout: 15_000 })
    await expect(zoneMBZ200).toBeVisible({ timeout: 15_000 })
    await expect(zoneGlobal).toBeVisible({ timeout: 15_000 })

    // Click MBZ100 and verify rate updates to $0.08 / GB
    await zoneMBZ100.click()
    await expect(page.getByText('$0.08 / GB')).toBeVisible({ timeout: 5_000 })

    // Click Global and verify rate updates to $0.18 / GB
    await zoneGlobal.click()
    await expect(page.getByText('$0.18 / GB')).toBeVisible({ timeout: 5_000 })

    // Click MBZ200 and verify rate returns to $0.12 / GB
    await zoneMBZ200.click()
    await expect(page.getByText('$0.12 / GB')).toBeVisible({ timeout: 5_000 })
  })

  test('6. Zero error cards on usage page', async ({ page }) => {
    await page.goto('/usage')
    await expect(page.getByRole('heading', { name: 'System Usage' })).toBeVisible({ timeout: 30_000 })

    // Ensure zero error cards
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/usage', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /usage] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })
})


function bucket(minutesAgo: number): string {
  return new Date(Date.now() - minutesAgo * 60_000).toISOString()
}

function populatedPayload() {
  const buckets = [30, 25, 20, 15, 10, 5].map(bucket)
  return {
    available: true,
    has_data: true,
    overview: {
      active_sessions: 1234,
      peak_viewers: 321,
      avg_session_duration: 95,
      rebuffer_rate: 4.2,
      avg_bitrate: 3200,
      avg_buffer_length: 14500,
      median_throughput: 28000,
    },
    sessions_ts: buckets.map((b, i) => ({ bucket: b, concurrent_sessions: 30 + i * 4, rebuffer_session_pct: 3 + i * 0.1, new_sessions: 8 + i })),
    buffer_health_ts: buckets.map((b, i) => ({ bucket: b, p50_buffer: 12000 + i * 100, p95_buffer: 26000 + i * 100, starvation_rate: 2 + i * 0.1 })),
    bitrate_ts: buckets.map((b, i) => ({ bucket: b, avg_bitrate: 3000 + i * 50, utilization_ratio: 0.5 })),
    throughput_ts: buckets.map((b, i) => ({ bucket: b, p50: 28000 + i * 200, p95: 45000, p99: 49000 })),
    startup_ts: buckets.map((b, i) => ({ bucket: b, startup_ratio: 8 + i * 0.2 })),
    top_content: [{ content_id: 'movie-001', session_count: 77, rebuffer_rate: 5, avg_bitrate: 3100, avg_buffer_length: 13000 }],
    rebuffer_by_country: [{ country: 'US', rebuffer_rate: 4, session_count: 600, median_throughput: 30000 }],
    rebuffer_by_asn: [{ asn: 7922, label: 'AS7922 Comcast', rebuffer_rate: 6, session_count: 200, avg_bitrate: 2900, avg_buffer_length: 12000 }],
    object_type_dist: [{ object_type: 'v', request_count: 900 }],
    streaming_format_dist: [{ streaming_format: 'h', session_count: 700 }],
    session_duration_dist: [{ duration_bucket: '1-5 min', session_count: 300 }],
  }
}

const fixturePath = '/api/__e2e__/streaming-fixture'

async function stubAggregates(page: Page, payload: unknown) {
  const response = await page.request.post(fixturePath, { data: payload })
  expect(response.ok()).toBe(true)
  return {
    bodies: async (): Promise<Array<Record<string, unknown>>> => {
      const response = await page.request.get(fixturePath)
      expect(response.ok()).toBe(true)
      return (await response.json()).bodies
    },
  }
}

test.describe('Streaming (CMCD) Page Contract (/streaming)', () => {
  test.afterEach(async ({ page }) => {
    const response = await page.request.delete(fixturePath)
    expect(response.ok()).toBe(true)
  })

  test('1. Route accessibility, environment indicator and request contract', async ({ page }) => {
    const seen = await stubAggregates(page, populatedPayload())
    const response = await page.goto('/streaming')
    expect(await response?.text()).toContain('1,234')
    await expect(page.getByRole('heading', { name: 'Streaming' })).toBeVisible({ timeout: 30_000 })

    const footerText = await page.locator('footer').innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)

    await expect.poll(async () => (await seen.bodies()).length, { timeout: 15_000 }).toBeGreaterThan(0)
    expect((await seen.bodies())[0]).toMatchObject({ bucket_seconds: 300, top_n: 30 })
  })

  test('2. Headline cards and charts render from a populated response', async ({ page }) => {
    await stubAggregates(page, populatedPayload())
    await page.goto('/streaming')

    for (const title of ['Total Sessions', 'Peak Viewers', 'Avg Duration', 'Rebuffer Rate', 'Avg Bitrate', 'Avg Buffer', 'Median Throughput']) {
      await expect(page.getByText(title, { exact: true }).first()).toBeVisible({ timeout: 30_000 })
    }
    await expect(page.getByText('1,234').first()).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('4.2%').first()).toBeVisible()
    await expect(page.locator('.js-plotly-plot, .plotly').first()).toBeVisible({ timeout: 45_000 })
  })

  test('3. Shows the not-available state when CMCD is off for the service', async ({ page }) => {
    await stubAggregates(page, { available: false, has_data: false })
    await page.goto('/streaming')
    await expect(page.getByText('CMCD data not available')).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText('Total Sessions', { exact: true })).toHaveCount(0)
  })

  test('4. No WCAG 2.1 AA violations on the populated page', async ({ page }) => {
    await stubAggregates(page, populatedPayload())
    await page.goto('/streaming')
    await expect(page.getByText('Total Sessions', { exact: true }).first()).toBeVisible({ timeout: 30_000 })
    await page.waitForTimeout(2_000)

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .exclude('[data-testid="plotly-host"]')
      .exclude('[data-empty-placeholder="true"]')
      .exclude('[data-axe-ignore="true"]')
      .exclude('.maplibregl-map')
      .analyze()
    expect(
      results.violations,
      results.violations.map((v) => `[${v.id}] ${v.help} (nodes: ${v.nodes.length}, first: ${v.nodes[0]?.target?.join(' > ')})`).join('\n'),
    ).toEqual([])
  })

  test('5. Layout stability: CLS <= 0.05 on a warm reload', async ({ page }) => {
    await stubAggregates(page, populatedPayload())
    await page.goto('/streaming')
    await expect(page.getByText('Total Sessions', { exact: true }).first()).toBeVisible({ timeout: 30_000 })

    await installClsObserver(page)
    await page.reload()
    await expect(page.getByText('Total Sessions', { exact: true }).first()).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText('1,234').first()).toBeVisible({ timeout: 15_000 })
    await page.waitForTimeout(1_500)

    const cls = await readClsScore(page)
    expect(cls).toBeLessThanOrEqual(0.05)
  })
})
