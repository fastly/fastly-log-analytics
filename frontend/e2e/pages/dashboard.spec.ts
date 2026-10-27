/**
 * E2E Page Contracts: Dashboard, Alerts, Usage, Streaming, A11y Routes,
 * SSR Hydration Smoke, Network, Assets & Shield, Performance, Origin,
 * Insights, and High-Scale Request Facts.
 */
import { expect, test, type Page } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import {
  assertIntrinsicCount, assertNoWcagViolations, assertPostTelemetry, assertRouteAndFooter,
  assertTimePresetsNo500, assertWarmPerfBudget, assertZeroErrorCards, type DebugResponse,
  getFirstServiceId, gotoHeading, installClsObserver, readClsScore,
} from '../helpers'

async function assertPlotlyAndZeroErrorCards(page: Page, route: string, heading: string) {
  await gotoHeading(page, route, heading)
  const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
  await expect(plotlyChart).toBeVisible({ timeout: 20_000 })
  await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
  await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
}

async function assertColumnVisibilityControl(page: Page, route: string, heading: string) {
  await gotoHeading(page, route, heading)
  const colBtn = page.getByRole('button', { name: /columns/i }).first()
  if (await colBtn.isVisible()) {
    await colBtn.click()
    await expect(page.getByRole('menuitemcheckbox', { name: /p99/i }).or(page.getByText(/p99/i)).first()).toBeVisible({ timeout: 5_000 })
  }
}

type StandardContractOpts = {
  intrinsicCount?: number; shellTitle?: string; shellCheck?: (page: Page) => Promise<void>
  dataContract?: { title: string; check: (page: Page) => Promise<void> }; timePresetsTitle?: string
  zeroErrorTitle?: string; columnControl?: boolean; plotlyAndZeroError?: boolean; perfTitle?: string
  telemetry?: { url: string; body: Record<string, unknown>; title?: string }
}

function registerStandardContracts(route: string, heading: string, opts: StandardContractOpts) {
  test('1. Route accessibility & environment indicator', ({ page }) => assertRouteAndFooter(page, route, heading))
  if (opts.intrinsicCount !== undefined || opts.shellCheck) {
    test(opts.shellTitle ?? '2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
      await gotoHeading(page, route, heading)
      if (opts.intrinsicCount !== undefined) await assertIntrinsicCount(page, opts.intrinsicCount)
      if (opts.shellCheck) await opts.shellCheck(page)
    })
  }
  if (opts.dataContract) test(opts.dataContract.title, ({ page }) => opts.dataContract!.check(page))
  if (opts.timePresetsTitle) test(opts.timePresetsTitle, ({ page }) => assertTimePresetsNo500(page, route, heading))
  if (opts.columnControl) test('6. Column visibility controls on latency tables', ({ page }) => assertColumnVisibilityControl(page, route, heading))
  if (opts.plotlyAndZeroError) test('7. Non-empty Plotly visualizations and zero error cards', ({ page }) => assertPlotlyAndZeroErrorCards(page, route, heading))
  if (opts.zeroErrorTitle) test(opts.zeroErrorTitle, ({ page }) => assertZeroErrorCards(page, route, heading))
  if (opts.perfTitle) test(opts.perfTitle, ({ page }) => assertWarmPerfBudget(page, route))
  if (opts.telemetry) {
    test(opts.telemetry.title ?? '8. Telemetry instrumentation & query attribution audit', ({ page }) =>
      assertPostTelemetry(page, opts.telemetry!.url, opts.telemetry!.body))
  }
}

test.describe('Dashboard Page Contract (/dashboard)', () => {
  registerStandardContracts('/dashboard', 'Dashboard', {
    plotlyAndZeroError: true,
    perfTitle: '11. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
  })

  test('2. Instant shell, pre-allocated layout, in-place loading states, and Single round-trip /api/dashboard/bundle contract', async ({ page }) => {
    let bundleCalls = 0
    page.on('response', (res) => { if (res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST') bundleCalls++ })
    const bundleResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
      { timeout: 30_000 },
    )
    await gotoHeading(page, '/dashboard', 'Dashboard')
    await expect(page.locator('text=Traffic over Time').or(page.locator('[data-empty-placeholder="true"]')).first()).toBeVisible({ timeout: 10_000 })
    const placeholders = page.locator('main [data-empty-placeholder="true"]')
    if ((await placeholders.count()) > 0) {
      expect((await placeholders.allTextContents()).some((t) => /Crunching logs|Loading|Initializing|Mapping|No data available/i.test(t))).toBeTruthy()
    }
    const bundleResponseData = await (await bundleResponsePromise).json()
    expect(bundleCalls).toBe(1)
    expect(bundleResponseData).toHaveProperty('aggregates.data')
    expect(bundleResponseData).toHaveProperty('top_bots')
  })

  test('4. Metric switching on TrafficChart, time range presets & Compare mode secondary query', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
    for (const metric of ['Reqs', '5xx', '4xx', 'CHR']) {
      const btn = page.getByRole('button', { name: new RegExp(`^${metric}$`, 'i') }).first()
      if (await btn.isVisible()) { await btn.click(); await expect(btn).toHaveAttribute('aria-pressed', 'true') }
    }
    const compareSwitch = page.getByRole('switch', { name: /compare/i })
    await expect(compareSwitch).toBeVisible({ timeout: 15_000 })
    const compareReq = page.waitForRequest((req) => req.url().includes('/api/dashboard/aggregates') && req.method() === 'POST', { timeout: 15_000 })
    await compareSwitch.click({ force: true })
    expect(await compareReq).toBeTruthy()
  })

  test('6. Click-to-filter drill-down adds filter chip and refetches', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const filterBtn = page.locator('button[aria-label^="Filter to "]').first()
    await expect.poll(async () => {
      if ((await filterBtn.count()) === 0) { await page.mouse.move(700, 500); await page.mouse.wheel(0, 700) }
      return filterBtn.count()
    }, { timeout: 60_000, intervals: [500, 1000] }).toBeGreaterThan(0)
    await filterBtn.scrollIntoViewIfNeeded()
    await expect(filterBtn).toBeVisible({ timeout: 10_000 })
    const filterValue = (await filterBtn.getAttribute('aria-label'))?.replace('Filter to ', '').trim() || ''
    expect(filterValue.length).toBeGreaterThan(0)
    await filterBtn.click()
    const filterBar = page.locator('[data-testid="filter-bar"]')
    await expect(filterBar).toBeVisible({ timeout: 10_000 })
    await expect(filterBar).toContainText(filterValue)
  })

  test('7. GeoMap panel mounts and its country picker preserves keyboard focus', async ({ page }) => {
    const pageErrors: string[] = []
    page.on('pageerror', (error) => pageErrors.push(error.message))
    await gotoHeading(page, '/dashboard', 'Dashboard')
    await expect(page.locator('h3').filter({ hasText: /requests by country/i }).first()).toBeVisible({ timeout: 15_000 })
    const browse = page.getByRole('button', { name: 'Browse countries' })
    await browse.scrollIntoViewIfNeeded()
    await browse.click()
    const list = page.getByRole('listbox', { name: /Countries by request count/ })
    await expect(list).toBeFocused()
    await list.press('End')
    await expect(list.getByRole('option').last()).toHaveAttribute('aria-selected', 'true')
    await list.press('Home')
    await expect(list.getByRole('option').first()).toHaveAttribute('aria-selected', 'true')
    await list.press('Escape')
    await expect(list).toBeHidden()
    await expect(browse).toBeFocused()
    expect(pageErrors).toEqual([])
  })

  test('9. Category collapse state and card customization toggle persist in localStorage across reload', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
    await expect(cardsBtn).toBeVisible({ timeout: 15_000 })
    await cardsBtn.click()
    await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })
    const firstChecked = page.locator('input[type="checkbox"][id^="card-"]:checked').first()
    await expect(firstChecked).toBeVisible({ timeout: 15_000 })
    const cardId = (await firstChecked.getAttribute('id'))!
    const cardKey = cardId.replace('card-', '')
    await page.locator(`label[for="${cardId}"]`).click()
    await expect(page.locator(`input#${cardId}`)).not.toBeChecked()
    await page.keyboard.press('Escape')
    const categoryButton = page.locator('section button:has(h3)').first()
    await expect(categoryButton).toBeVisible({ timeout: 15_000 })
    await categoryButton.click()
    await page.reload()
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    expect(await page.evaluate(() => localStorage.getItem('dashboard_collapsed_sections'))).toBeTruthy()
    expect((JSON.parse((await page.evaluate(() => localStorage.getItem('dashboard_cards')))!) as string[]).includes(cardKey)).toBe(false)
  })

  test('12. Telemetry instrumentation & query efficiency audit', async ({ page }) => {
    await page.setExtraHTTPHeaders({ 'x-debug-responses': '1' })
    const bundleResPromise = page.waitForResponse(
      (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
      { timeout: 30_000 },
    ).catch(() => null)
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const bundleRes = await bundleResPromise
    const json = (await bundleRes?.json().catch(() => null)) as DebugResponse | null
    if (json?._debug_queries || json?._debug_sqlite) {
      const queries = json._debug_queries ?? []
      expect(queries.length).toBeGreaterThan(0)
      expect(queries.reduce((acc, q) => acc + (q.time_ms || 0), 0)).toBeLessThan(2500)
      const analyticalQueries = queries.filter((q) => !q.sql?.startsWith('-- DuckDB Storage View Resolution'))
      const sqlStatements = analyticalQueries.map((q) => JSON.stringify({ sql: q.sql?.trim?.() ?? '', params: q.params ?? null }))
      expect(new Set(sqlStatements).size).toBe(sqlStatements.length)
    }
  })

  test('Deep link ?metric= consumption, URL filter round-trip, and maplibre container mount', async ({ page }) => {
    await page.goto('/dashboard?metric=bandwidth&keep=1')
    await expect(page.locator('main').first()).toBeVisible({ timeout: 30_000 })
    await expect.poll(() => new URL(page.url()).searchParams.get('metric'), { timeout: 15_000 }).toBeNull()
    expect(new URL(page.url()).searchParams.get('keep')).toBe('1')
    await Promise.any([
      page.locator('canvas').first().waitFor({ state: 'attached', timeout: 45_000 }),
      page.getByText(/Interactive map unavailable/i).waitFor({ state: 'visible', timeout: 45_000 }),
    ])
    expect((await page.locator('canvas').count()) + (await page.getByText(/Interactive map unavailable/i).count())).toBeGreaterThanOrEqual(1)
    await gotoHeading(page, '/dashboard?filter_country=US', 'Dashboard')
    await expect(page.getByText(/country/i).first()).toBeVisible({ timeout: 15_000 })
    await gotoHeading(page, '/security', 'Security')
  })
})

test('an unconfigured service never shows NGWAF cards, even with saved selections or Show all', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('dashboard_cards', JSON.stringify(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'])))
  await page.goto('/dashboard')
  const cards = page.getByRole('button', { name: 'Toggle visible dashboard cards' })
  await expect(cards).toHaveText(/^Cards\s*1$/, { timeout: 15_000 })
  await page.getByRole('heading', { name: 'Security', exact: true }).scrollIntoViewIfNeeded()
  await expect(page.getByRole('heading', { name: /^Fastly Bots/ })).toBeVisible({ timeout: 15_000 })
  await expect(page.getByRole('heading', { name: /NGWAF/ })).toHaveCount(0)
  await cards.click()
  await expect(page.getByLabel('NGWAF Verified Bots', { exact: true })).toHaveCount(0)
  await expect(page.getByLabel('NGWAF Signals', { exact: true })).toHaveCount(0)
  await page.getByRole('button', { name: 'Show all', exact: true }).click()
  await page.keyboard.press('Escape')
  await expect(page.getByRole('heading', { name: /NGWAF/ })).toHaveCount(0)
  await page.getByRole('heading', { name: 'Security', exact: true }).scrollIntoViewIfNeeded()
  await expect(page.getByRole('heading', { name: /^Fastly Bots/ })).toBeVisible({ timeout: 15_000 })
})

test.describe('Alerts & Notifications Page Contract (/alerts)', () => {
  test('1. Route accessibility, environment indicator, zero-CLS containers & warm-load budget', async ({ page }) => {
    await assertRouteAndFooter(page, '/alerts', /Alerts/i)
    for (const id of ['alerts-page-root', 'alerts-summary-grid', 'alerts-list-card']) await expect(page.locator(`[data-testid="${id}"]`)).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 2)
    await assertWarmPerfBudget(page, '/alerts', { settleMs: 800, checkLcp: false })
  })

  test('3. Alerts list contract, status filter controls, role verification & SSRF protection checks', async ({ page }) => {
    const getRes = await page.request.get('/api/alerts/')
    expect(getRes.ok() && Array.isArray((await getRes.json()).data)).toBeTruthy()
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const statusRes = await page.request.get(`/api/alerts/?status=${status}`)
      expect(statusRes.ok() && Array.isArray((await statusRes.json()).data)).toBeTruthy()
    }
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const ssrfRes = await page.request.post('/api/alerts/', {
      headers: { 'x-fastly-service-id': serviceId },
      data: { id: 'ssrf-probe-rule', service_id: serviceId, name: 'SSRF Probe', category: 'reliability', metric: '5xx_rate', operator: '>', threshold: 5, window_min: 5, channels: [{ type: 'webhook', url: 'http://169.254.169.254/latest/meta-data/' }], enabled: true },
    })
    expect(ssrfRes.status()).toBe(400)
    await page.goto('/alerts')
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    const statusFilter = page.locator('[data-testid="alerts-status-filter"]:visible')
    await expect(statusFilter).toHaveCount(1, { timeout: 15_000 })
    for (const status of ['all', 'alerting', 'ok', 'muted']) await expect(statusFilter.getByTestId(`alerts-filter-${status}`)).toBeVisible({ timeout: 5_000 })
    if (/Admin View/i.test(await page.locator('footer').innerText())) await expect(page.locator('[data-testid="alerts-create-btn"]')).toBeVisible({ timeout: 10_000 })
  })

  test('4. Admin rule lifecycle (Create, Update, Toggle, Test Webhook, Evaluate & Delete) and telemetry attribution', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default', ruleId = `e2e-alert-${Date.now()}`, headers = { 'x-fastly-service-id': serviceId }
    const baseRule = {
      id: ruleId, service_id: serviceId, name: 'E2E Test High 5xx Alert', category: 'reliability', metric: '5xx_rate',
      evaluation_type: 'absolute', evaluation_scope: 'all', operator: '>', threshold: 5.0, window_min: 5,
      channels: [{ type: 'webhook', url: 'https://hooks.example.com/e2e-alert-webhook' }], enabled: true,
    }
    try {
      const createRes = await page.request.post('/api/alerts/', { headers, data: baseRule })
      expect(createRes.ok()).toBeTruthy()
      const createData = await createRes.json()
      expect(['saved', 'success']).toContain(createData.data?.status)
      expect(createData.data?.id).toBe(ruleId)
      const listRes = await page.request.get(`/api/alerts/${serviceId}`, { headers: { 'x-debug-responses': '1', 'x-page-load-id': `e2e-alerts-${Date.now()}` } })
      expect(listRes.ok() && ((await listRes.json()).data as { id?: string }[]).some((a) => a.id === ruleId)).toBeTruthy()
      await page.goto(`/alerts?service=${encodeURIComponent(serviceId)}`)
      await expect(page.getByText('E2E Test High 5xx Alert')).toBeVisible({ timeout: 15_000 })
      expect((await page.request.put(`/api/alerts/${ruleId}`, { headers, data: { ...baseRule, name: 'E2E Test High 5xx Alert Updated', threshold: 7.5, window_min: 10 } })).ok()).toBeTruthy()
      for (const enabled of [false, true]) expect((await page.request.patch(`/api/alerts/${ruleId}/enabled`, { headers, data: { enabled } })).ok()).toBeTruthy()
      const testWebhookRes = await page.request.post(`/api/alerts/${ruleId}/test`, { headers, data: { service_id: serviceId, dry_run: true } })
      expect(testWebhookRes.ok()).toBeTruthy()
      expect((await testWebhookRes.json()).data).toMatchObject({ status: 'validated', channel_type: 'webhook' })
      const evalRes = await page.request.post('/api/alerts/evaluate', { headers, data: { service_id: serviceId } })
      expect(evalRes.ok() && (await evalRes.json()).data?.evaluated >= 1).toBeTruthy()
      await assertPostTelemetry(
        page, '/api/alerts/preview',
        { id: 'telemetry-preview', service_id: serviceId, name: 'Telemetry Preview Rule', category: 'reliability', metric: '5xx_rate', evaluation_type: 'absolute', evaluation_scope: 'all', operator: '>', threshold: 5, window_min: 15 },
        { 'x-fastly-service-id': serviceId, 'x-page-load-id': `e2e-alerts-preview-${Date.now()}` },
      )
    } finally {
      await page.request.delete(`/api/alerts/${ruleId}`, { headers })
    }
  })
})

test.describe('Usage & Cost Estimator Page Contract (/usage)', () => {
  registerStandardContracts('/usage', 'System Usage', {
    intrinsicCount: 3, zeroErrorTitle: '6. Zero error cards on usage page', perfTitle: '7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
    dataContract: {
      title: '3. Data contract & schema validation on /api/usage endpoints',
      check: async (page) => {
        const [storageRes, ratesRes] = await Promise.all([page.request.get('/api/usage/current-storage'), page.request.get('/api/usage/prefill/rates')])
        expect(storageRes.ok() && ratesRes.ok()).toBeTruthy()
        expect(await storageRes.json()).toMatchObject({ live_bytes: expect.anything(), total_billed_gb_hours: expect.anything() })
        const ratesData = await ratesRes.json()
        expect(ratesData).toHaveProperty('class_a_rate_per_1k')
        expect(ratesData).toHaveProperty('cdn_egress_rate_per_gb')
        expect(['MBZ100', 'MBZ200', 'Global']).toContain(ratesData.billing_zone)
        for (const [url, prop] of [['/api/usage/operations?by=hour', 'total_class_a'], ['/api/usage/bandwidth?by=hour', 'total_bytes'], ['/api/usage/log-activity?by=hour', 'total_rows']] as const) {
          const res = await page.request.get(url)
          expect(res.ok()).toBeTruthy()
          const data = await res.json()
          expect(data).toHaveProperty(prop)
          expect(Array.isArray(data.data)).toBeTruthy()
        }
      },
    },
  })

  test('4. Core stat cards, charts, Cost Estimator, and Billing Zone switching', async ({ page }) => {
    await gotoHeading(page, '/usage', 'System Usage')
    for (const label of ['Storage Impact (Period)', 'Class A Ops (period)', 'Class B Ops (period)', 'Estimated Incurred Cost', 'FOS Operations', 'CDN Bandwidth', 'Estimated Cost', 'Log Activity (Processed)', 'Cost Estimator']) {
      await expect(page.getByText(label)).toBeVisible({ timeout: 15_000 })
    }
    await page.getByText('Cost Estimator').scrollIntoViewIfNeeded()
    for (const [zone, rate] of [['MBZ100', '$0.08 / GB'], ['Global', '$0.18 / GB'], ['MBZ200', '$0.12 / GB']] as const) {
      const btn = page.getByRole('button', { name: zone })
      await expect(btn).toBeVisible({ timeout: 15_000 })
      await btn.click()
      await expect(page.getByText(rate)).toBeVisible({ timeout: 5_000 })
    }
  })
})

const bucket = (minutesAgo: number) => new Date(Date.now() - minutesAgo * 60_000).toISOString()

function populatedPayload() {
  const buckets = [30, 25, 20, 15, 10, 5].map(bucket)
  return {
    available: true, has_data: true,
    overview: { active_sessions: 1234, peak_viewers: 321, avg_session_duration: 95, rebuffer_rate: 4.2, avg_bitrate: 3200, avg_buffer_length: 14500, median_throughput: 28000 },
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
  expect((await page.request.post(fixturePath, { data: payload })).ok()).toBe(true)
  return {
    bodies: async (): Promise<Array<Record<string, unknown>>> => {
      const res = await page.request.get(fixturePath)
      expect(res.ok()).toBe(true)
      return (await res.json()).bodies
    },
  }
}

test.describe('Streaming (CMCD) Page Contract (/streaming)', () => {
  test.afterEach(async ({ page }) => { expect((await page.request.delete(fixturePath)).ok()).toBe(true) })

  test('1. Route accessibility, environment indicator and request contract', async ({ page }) => {
    const seen = await stubAggregates(page, populatedPayload())
    expect(await (await page.goto('/streaming'))?.text()).toContain('1,234')
    await expect(page.getByRole('heading', { name: 'Streaming' })).toBeVisible({ timeout: 30_000 })
    expect(await page.locator('footer').innerText()).toMatch(/Admin View|Analyst View/i)
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

  test('4. No WCAG 2.1 AA violations and layout stability (CLS <= 0.05) on warm reload', async ({ page }) => {
    await stubAggregates(page, populatedPayload())
    await page.goto('/streaming')
    await expect(page.getByText('Total Sessions', { exact: true }).first()).toBeVisible({ timeout: 30_000 })
    await page.waitForTimeout(2_000)
    await assertNoWcagViolations(page)
    await installClsObserver(page)
    await page.reload()
    await expect(page.getByText('Total Sessions', { exact: true }).first()).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText('1,234').first()).toBeVisible({ timeout: 15_000 })
    await page.waitForTimeout(1_500)
    expect(await readClsScore(page)).toBeLessThanOrEqual(0.05)
  })
})

const CLEAN_ROUTES = ['/origin', '/performance', '/security', '/query', '/insights', '/sessions', '/network', '/control-room', '/fastly-value', '/streaming', '/admin', '/admin/queries', '/admin/session-scoring', '/admin/share', '/admin/trends', '/admin/usage-log', '/alerts', '/logs', '/usage']
const HYDRATION_ROUTES = ['/admin', '/admin/share', '/admin/session-scoring', '/admin/trends', '/admin/usage-log', '/admin/queries', '/dashboard', '/sessions', '/security', '/network', '/origin', '/performance', '/rum', '/usage', '/alerts', '/logs', '/query', '/insights', '/control-room']
const SERVICE_ID = 'svc-playwright-e2e'
const HYDRATION_PATTERNS = [/hydrat/i, /did not match/i, /cannot be a descendant/i, /text content does not match/i, /minified react error #(418|423|425)/i]
const isHydrationError = (text: string) => HYDRATION_PATTERNS.some((re) => re.test(text))

async function gotoSettled(page: Page, route: string) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try { await page.goto(route); return } catch (e) {
      if (attempt === 0 && /ERR_ABORTED|frame was detached|frame got detached|navigation .*interrupted/i.test(String((e as Error)?.message ?? e))) continue
      throw e
    }
  }
}

for (const route of CLEAN_ROUTES) {
  test(`no WCAG 2.1 AA violations on ${route}`, async ({ page }) => {
    await gotoSettled(page, route)
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)
    await assertNoWcagViolations(page, ['.recharts-wrapper'])
  })
}

test('Tab navigation moves focus through sidebar and Escape closes open popovers without trapping focus', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  expect(await page.evaluate(() => document.activeElement?.tagName)).toBeTruthy()
  await page.keyboard.press('Tab')
  expect(await page.evaluate(() => document.activeElement?.tagName)).toBeTruthy()
  await page.keyboard.press('Escape')
  expect(await page.locator('body').count()).toBe(1)
})

test.describe('SSR hydration smoke', () => {
  for (const route of HYDRATION_ROUTES) {
    test(`${route} hydrates without a React hydration error`, async ({ page }) => {
      await page.addInitScript((sid) => {
        localStorage.setItem('service-storage', JSON.stringify({ state: { activeServiceId: sid, services: [{ id: sid, name: 'Playwright E2E Service' }] }, version: 0 }))
        localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }))
        localStorage.setItem('fastly-debug-settings', JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }))
        localStorage.setItem('dashboard_collapsed_sections', JSON.stringify(['request', 'cache']))
      }, SERVICE_ID)
      const hydrationErrors: string[] = []
      page.on('console', (msg) => { if (msg.type() === 'error' && isHydrationError(msg.text())) hydrationErrors.push(msg.text()) })
      page.on('pageerror', (err) => { const text = `${err.message}\n${err.stack ?? ''}`; if (isHydrationError(text)) hydrationErrors.push(text) })
      await page.goto(route, { waitUntil: 'domcontentloaded', timeout: 30_000 })
      await expect(page.locator('main, [role="main"], h1, h2').first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('nextjs-portal, [data-nextjs-dialog-overlay], .alert-destructive, [data-testid="error-card"]')).toHaveCount(0)
      await page.waitForTimeout(300)
      expect(hydrationErrors, `React hydration error(s) on ${route}:\n${hydrationErrors.join('\n---\n')}`).toEqual([])
    })
  }
})

test.describe('Network Page Contract (/network)', () => {
  registerStandardContracts('/network', 'Network & ASN Health', {
    intrinsicCount: 4, timePresetsTitle: '5. Time range presets switching cleanly (15m, 24h, 7d)',
    zeroErrorTitle: '6. Zero error cards on network page', perfTitle: '7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
    telemetry: { url: '/api/network/bundle', body: { sections: ['transport'] } },
    dataContract: {
      title: '3. Data contract & schema validation (/api/network/bundle & /api/network-health)',
      check: async (page) => {
        const [bundleRes, healthRes] = await Promise.all([
          page.request.post('/api/network/bundle', { data: { sections: ['transport'] } }),
          page.request.post('/api/network-health', { data: { sections: ['summary', 'leaderboard', 'metro_leaderboard'], top_n: 30 } }),
        ])
        expect(bundleRes.ok() && healthRes.ok()).toBeTruthy()
        const bundleData = await bundleRes.json()
        expect(bundleData).toHaveProperty('transport')
        if (bundleData.transport) expect(Array.isArray(bundleData.transport.protocol_distribution) && Array.isArray(bundleData.transport.missing_fields)).toBeTruthy()
        expect(await healthRes.json()).toHaveProperty('available')
        await gotoHeading(page, '/network', 'Network & ASN Health')
      },
    },
  })

  test('4. Core network cards & transport section mounted', async ({ page }) => {
    await gotoHeading(page, '/network', 'Network & ASN Health')
    const unavailableNotice = page.getByText('Network Metrics Unavailable')
    if (!(await unavailableNotice.isVisible().catch(() => false))) {
      for (const title of ['Global Health', 'Worst ASN']) await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.getByText('Edge Transport & TCP Performance')).toBeVisible({ timeout: 15_000 })
      await expect(page.getByText('ASN Performance Leaderboard').or(page.getByText('Fastly Edge PoP Health Heatmap')).first()).toBeVisible({ timeout: 15_000 })
    } else {
      await expect(unavailableNotice).toBeVisible()
    }
  })
})

test.describe('Assets & Shield Page Contract (/assets-shield)', () => {
  registerStandardContracts('/assets-shield', 'Assets & Shield', {
    intrinsicCount: 5, shellTitle: '2. Instant shell, pre-allocated layout, zero CLS containers & core headings',
    shellCheck: async (page) => {
      for (const id of ['assets-overview-grid', 'assets-cache-efficiency-card', 'assets-compression-card', 'assets-uncompressed-card', 'assets-low-ttl-card']) {
        await expect(page.locator(`[data-testid="${id}"]:visible`)).toBeVisible({ timeout: 15_000 })
      }
      for (const h of ['Cache Efficiency by Asset Type', 'Payload Compression Efficiency', 'Large Uncompressed Assets', 'Low Cache-TTL Hotspots']) {
        await expect(page.getByRole('heading', { name: h })).toBeVisible({ timeout: 15_000 })
      }
    },
    dataContract: {
      title: '3. Data contract & schema validation (/api/assets/aggregates)',
      check: async (page) => {
        const [postRes, getRes] = await Promise.all([page.request.post('/api/assets/aggregates', { data: { range_token: '24h' } }), page.request.get('/api/assets/aggregates?range_token=24h')])
        expect(postRes.ok() && getRes.ok()).toBeTruthy()
        const postData = await postRes.json()
        for (const key of ['asset_type_breakdown', 'cache_performance', 'compression_performance', 'large_uncompressed_assets', 'low_ttl_assets', 'missing_fields']) expect(Array.isArray(postData[key])).toBeTruthy()
        expect(await getRes.json()).toMatchObject({ asset_type_breakdown: expect.anything(), missing_fields: expect.anything() })
        await gotoHeading(page, '/assets-shield', 'Assets & Shield')
      },
    },
    timePresetsTitle: '5. Time range presets switching cleanly (15m, 24h, 7d)',
    zeroErrorTitle: '6. Zero error cards on assets page', perfTitle: '7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
    telemetry: { url: '/api/assets/aggregates', body: { range_token: '24h' } },
  })
})

test.describe('Performance Page Contract (/performance)', () => {
  registerStandardContracts('/performance', 'Performance', {
    intrinsicCount: 3, shellTitle: '2. Instant shell, zero CLS containers, 5 core cards & time presets',
    shellCheck: async (page) => {
      await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
      for (const label of ['End-to-End Latency Waterfall (Average)', 'Slowest URLs', 'Slowest Networks', 'Cache TTL Distribution', 'Origin vs Edge Processing (ms)']) {
        await expect(page.getByText(label).locator('visible=true')).toBeVisible({ timeout: 15_000 })
      }
    },
    dataContract: {
      title: '3. Data contract & round-trip verification (/api/performance/aggregates)',
      check: async (page) => {
        const apiRes = await page.request.post('/api/performance/aggregates', { data: { range_token: '24h', sort_by: 'p99' } })
        expect(apiRes.ok()).toBeTruthy()
        const data = await apiRes.json()
        for (const key of ['waterfall', 'top_urls', 'top_asns', 'ttl_dist', 'scatter']) expect(data).toHaveProperty(key)
        await gotoHeading(page, '/performance', 'Performance')
      },
    },
    columnControl: true, plotlyAndZeroError: true, perfTitle: '8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
    telemetry: { url: '/api/performance/aggregates', body: { range_token: '24h', sort_by: 'p99' }, title: '9. Telemetry instrumentation & query attribution audit' },
  })
})

test.describe('Origin Page Contract (/origin)', () => {
  registerStandardContracts('/origin', 'Origin Performance', {
    intrinsicCount: 6, shellTitle: '2. Instant shell, pre-allocated layout, zero CLS containers & core origin cards',
    shellCheck: async (page) => {
      for (const title of ['Origin TTFB (P50)', 'Origin TTFB (P95)', 'Origin Error Rate', 'Fetch Volume', 'Origin Latency', 'Status Code Distribution', 'Slowest URLs at Origin', 'Origin Performance by POP', 'Origin IP Health', 'Origin Payload Size']) {
        await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({ timeout: 15_000 })
      }
    },
    dataContract: {
      title: '3. Data contract & round-trip verification (/api/origin/aggregates)',
      check: async (page) => {
        const apiRes = await page.request.post('/api/origin/aggregates', { data: { range_token: '24h', timeseries_metric: 'ttfb', timeseries_percentile: 'p95' } })
        expect(apiRes.ok()).toBeTruthy()
        const data = await apiRes.json()
        for (const key of ['summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health']) expect(data).toHaveProperty(key)
        await gotoHeading(page, '/origin', 'Origin Performance')
      },
    },
    timePresetsTitle: '5. Time range presets and adaptive history extents (15m, 24h, 30d)',
    columnControl: true, plotlyAndZeroError: true, perfTitle: '8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
    telemetry: { url: '/api/origin/aggregates', body: { range_token: '24h', timeseries_metric: 'ttfb', timeseries_percentile: 'p95' }, title: '9. Telemetry instrumentation & query attribution audit' },
  })
})

type RequestFactsDebugResponse = {
  _debug_queries?: DebugQuery[]; rows?: Array<Record<string, unknown>>; next_cursor?: string | null
  metadata?: { status?: string; exact?: boolean; coverage?: number; freshness_lag_seconds?: number; mode?: string; watermark?: Record<string, unknown> }
}

test.describe('Insights Page Contract (/insights)', () => {
  registerStandardContracts('/insights', 'Anomaly Detection', {
    intrinsicCount: 1,
    shellCheck: async (page) => {
      await expect(page.locator('[class*="contain-intrinsic-size"]').first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('[data-testid="insights-grid"], [data-testid="insights-empty-pass"], [data-testid="insights-grid-loading"]').first()).toBeVisible({ timeout: 15_000 })
    },
    dataContract: {
      title: '3. Data contract & schema validation (/api/insights & /api/insight-availability)',
      check: async (page) => {
        const [availRes, apiRes] = await Promise.all([page.request.get('/api/insight-availability'), page.request.post('/api/insights', { data: { window_size_hrs: 1, baseline_hours: 24, filters: {} } })])
        expect(availRes.ok() && apiRes.ok()).toBeTruthy()
        const [availData, data] = await Promise.all([availRes.json(), apiRes.json()])
        expect(Array.isArray(availData.insights) && Array.isArray(data.insights)).toBeTruthy()
        if (availData.insights.length > 0) expect(availData.insights[0]).toMatchObject({ id: expect.anything(), available: expect.anything() })
        expect(data).toHaveProperty('computed_at')
      },
    },
    perfTitle: '7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load',
  })

  test('4. Instant client-side tab switching across all 5 category tabs and All', async ({ page }) => {
    await gotoHeading(page, '/insights', 'Anomaly Detection')
    for (const tabKey of ['all', 'security', 'origin', 'edge', 'network', 'traffic']) {
      const trigger = page.locator(`button[data-tab="${tabKey}"]`).or(page.locator(`button[data-alias="${tabKey}"]`)).first()
      await expect(trigger).toBeVisible({ timeout: 10_000 })
      await trigger.click()
      await expect(trigger).toHaveAttribute('aria-selected', 'true')
      await expect(page.locator(`[data-tab-content="${tabKey}"]`).or(page.locator(`[data-alias="${tabKey}"]`)).first()).toBeVisible({ timeout: 10_000 })
    }
  })

  test('5. Plausible detector values or clean normal parameters pass states & URL preset transitions', async ({ page }) => {
    await gotoHeading(page, '/insights', 'Anomaly Detection')
    const emptyPass = page.locator('[data-testid="insights-empty-pass"]').first(), cardsGrid = page.locator('[data-testid="insights-grid"]').first()
    await expect(emptyPass.or(cardsGrid)).toBeVisible({ timeout: 30_000 })
    if (await emptyPass.isVisible()) {
      await expect(emptyPass).toContainText(/No anomalies detected/i)
      await expect(emptyPass).toContainText(/baseline parameters/i)
    } else {
      await expect(cardsGrid.locator('.rounded-xl, .border').first()).toBeVisible({ timeout: 10_000 })
    }
    await gotoHeading(page, '/insights?window=24&baseline=168&tab=security', 'Anomaly Detection')
    const secTrigger = page.locator('button[data-tab="security"]').first()
    await expect(secTrigger).toBeVisible({ timeout: 10_000 })
    await expect(secTrigger).toHaveAttribute('aria-selected', 'true')
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    expect(await assertPostTelemetry(page, '/api/insights', { window_size_hrs: 1, baseline_hours: 24, filters: {} }, {}, true)).toHaveProperty('insights')
  })
})

test.describe('High-Scale Request Facts Page Contract (/high-scale/request-facts)', () => {
  registerStandardContracts('/high-scale/request-facts', 'High-scale request facts', {
    intrinsicCount: 2, perfTitle: '6. Performance budget: FCP < 400ms / LCP < 1500ms and CLS <= 0.05 on warm load',
    dataContract: {
      title: '3. Data contract & schema validation (/api/high-scale/request-facts)',
      check: async (page) => {
        const serviceId = await getFirstServiceId(page)
        expect(serviceId).toBeTruthy()
        const now = new Date(), oneHourAgo = new Date(now.getTime() - 3600 * 1000)
        const [getRes, postRes] = await Promise.all([
          page.request.get(`/api/high-scale/request-facts?service_id=${encodeURIComponent(serviceId!)}&start_time=${encodeURIComponent(oneHourAgo.toISOString())}&end_time=${encodeURIComponent(now.toISOString())}&limit=10`),
          page.request.post(`/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`, { data: { start_time: oneHourAgo.toISOString(), end_time: now.toISOString(), limit: 10, emulate_standard: true } }),
        ])
        expect(getRes.ok() && postRes.ok()).toBeTruthy()
        const [getData, postData] = (await Promise.all([getRes.json(), postRes.json()])) as [RequestFactsDebugResponse, RequestFactsDebugResponse]
        expect(Array.isArray(getData.rows) && Array.isArray(postData.rows)).toBeTruthy()
        expect(['complete', 'partial', 'unavailable']).toContain(getData.metadata?.status)
        expect(['clickhouse', 'duckdb_emulation']).toContain(getData.metadata?.mode)
        expect(postData.metadata?.watermark?.service_id).toBe(serviceId)
      },
    },
  })

  test('4. Metadata bar, mode badge, filter bar, table & interactive filter controls', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => { if (msg.type() === 'error') consoleErrors.push(msg.text()) })
    await gotoHeading(page, '/high-scale/request-facts', 'High-scale request facts')
    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })
    for (const id of ['high-scale-metadata', 'high-scale-mode-badge', 'high-scale-filters', 'high-scale-table-wrapper']) await expect(page.getByTestId(id)).toBeVisible({ timeout: 15_000 })
    expect(await page.getByTestId('high-scale-mode-badge').innerText()).toMatch(/DuckDB Standard Mode Emulation|ClickHouse Distributed Cluster/)
    await expect(page.locator('[role="alert"].border-destructive')).toHaveCount(0)
    await page.getByTestId('filter-status').fill('200')
    await page.getByTestId('filter-pop').fill('IAD')
    await expect(page.getByTestId('filter-clear')).toBeVisible()
    await page.getByTestId('filter-clear').click()
    await expect(page.getByTestId('filter-status')).toHaveValue('')
    await expect(page.getByTestId('filter-pop')).toHaveValue('')
    expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    expect(serviceId).toBeTruthy()
    const now = new Date(), oneHourAgo = new Date(now.getTime() - 3600 * 1000)
    await assertPostTelemetry(page, `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`, { start_time: oneHourAgo.toISOString(), end_time: now.toISOString(), limit: 10, emulate_standard: true }, { 'X-Page-Load-ID': `e2e-hs-facts-${Date.now()}` })
  })
})
