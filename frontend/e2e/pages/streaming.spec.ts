/**
 * E2E Page Contract Test: CMCD Streaming Analytics (/streaming)
 *
 * Implements the verification checklist in docs/pages/streaming.md. The e2e
 * backend has no CMCD columns, so a cookie-scoped backend fixture serves the
 * same response to SSR and browser fetches. page.route cannot intercept SSR.
 * Real CMCD data is verified live by scripts/verify_dashboard.js.
 *
 * 1. Route accessibility, environment footer, and the request contract.
 * 2. Headline cards and charts render from a populated response.
 * 3. "CMCD data not available" state when the service has no CMCD data.
 * 4. No WCAG 2.1 AA violations on the populated page.
 * 5. Layout stability: CLS <= 0.05 on a warm reload.
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import { installClsObserver, readClsScore } from '../cls-observer'

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
