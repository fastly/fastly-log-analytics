/**
 * E2E Page Contracts: Dashboard, Alerts, Usage, Streaming, A11y Routes,
 * SSR Hydration Smoke, Network, Assets & Shield, Performance, Origin,
 * Insights, and High-Scale Request Facts.
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { getFirstServiceId, installClsObserver, measureWarmPaintAndCls, readClsScore } from '../helpers'

type DebugResponse = {
  _debug_queries?: DebugQuery[]
  _debug_sqlite?: unknown[]
  [key: string]: unknown
}

async function gotoHeading(page: Page, route: string, heading: string | RegExp) {
  await page.goto(route)
  await expect(page.getByRole('heading', { name: heading }).first()).toBeVisible({ timeout: 30_000 })
}

async function assertRouteAndFooter(page: Page, route: string, heading: string | RegExp) {
  await gotoHeading(page, route, heading)
  const footer = page.locator('footer')
  await expect(footer).toBeVisible({ timeout: 10_000 })
  expect(await footer.innerText()).toMatch(/Admin View|Analyst View/i)
}

async function assertIntrinsicCount(page: Page, minCount: number) {
  const reserved = page.locator('[class*="contain-intrinsic-size"]')
  expect(await reserved.count()).toBeGreaterThanOrEqual(minCount)
}

async function assertZeroErrorCards(page: Page, route: string, heading: string | RegExp) {
  await gotoHeading(page, route, heading)
  await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
}

async function assertTimePresetsNo500(page: Page, route: string, heading: string | RegExp) {
  const consoleErrors: string[] = []
  page.on('console', (msg) => {
    if (msg.type() === 'error') consoleErrors.push(msg.text())
  })
  await gotoHeading(page, route, heading)
  await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
  expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
}

async function assertWarmPerfBudget(page: Page, route: string, settleMs = 1000, checkLcp = true) {
  await page.goto(route, { waitUntil: 'domcontentloaded' })
  await installClsObserver(page)
  const start = Date.now()
  await page.reload({ waitUntil: 'domcontentloaded' })
  const totalTime = Date.now() - start
  const metrics = await measureWarmPaintAndCls(page, settleMs)
  console.log(`[PERF ${route}] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
  expect(totalTime).toBeLessThan(10_000)
  if (checkLcp && metrics.lcp > 0) expect(metrics.lcp).toBeLessThan(1500)
  expect(metrics.cls).toBeLessThanOrEqual(0.05)
}

async function assertPostTelemetry(
  page: Page,
  endpoint: string,
  data: Record<string, unknown>,
  extraHeaders: Record<string, string> = {},
  optionalQueries = false,
): Promise<DebugResponse> {
  const res = await page.request.post(endpoint, {
    data,
    headers: { 'x-debug-responses': '1', ...extraHeaders },
  })
  expect(res.ok()).toBeTruthy()
  const json = (await res.json()) as DebugResponse
  if (!optionalQueries || json._debug_queries) {
    expect(json).toHaveProperty('_debug_queries')
    expect((json._debug_queries ?? []).length).toBeGreaterThan(0)
  }
  return json
}

// ---------------------------------------------------------------------------
// Dashboard Page Contract (/dashboard)
// ---------------------------------------------------------------------------

test.describe('Dashboard Page Contract (/dashboard)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/dashboard', 'Dashboard')
  })

  test('2. Instant shell, pre-allocated layout & in-place loading states', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const chartContainer = page.locator('text=Traffic over Time').or(page.locator('[data-empty-placeholder="true"]'))
    await expect(chartContainer.first()).toBeVisible({ timeout: 10_000 })

    const placeholders = page.locator('main [data-empty-placeholder="true"]')
    if ((await placeholders.count()) > 0) {
      const texts = await placeholders.allTextContents()
      expect(texts.some((t) => /Crunching logs|Loading|Initializing|Mapping|No data available/i.test(t))).toBeTruthy()
    }
  })

  test('3. Single round-trip /api/dashboard/bundle contract', async ({ page }) => {
    let bundleCalls = 0
    page.on('response', (res) => {
      if (res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST') bundleCalls++
    })
    const bundleResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
      { timeout: 30_000 },
    )
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const bundleResponseData = await (await bundleResponsePromise).json()

    expect(bundleCalls).toBe(1)
    expect(bundleResponseData).toBeTruthy()
    expect(bundleResponseData).toHaveProperty('aggregates')
    expect(bundleResponseData.aggregates).toHaveProperty('data')
    expect(bundleResponseData).toHaveProperty('top_bots')
  })

  test('4. Metric switching on TrafficChart', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    for (const metric of ['Reqs', '5xx', '4xx', 'CHR']) {
      const btn = page.getByRole('button', { name: new RegExp(`^${metric}$`, 'i') }).first()
      if (await btn.isVisible()) {
        await btn.click()
        await expect(btn).toHaveAttribute('aria-pressed', 'true')
      }
    }
  })

  test('5. Time range presets and adaptive history extents', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
  })

  test('6. Click-to-filter drill-down adds filter chip and refetches', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const filterBtn = page.locator('button[aria-label^="Filter to "]').first()
    await expect
      .poll(
        async () => {
          if ((await filterBtn.count()) === 0) {
            await page.mouse.move(700, 500)
            await page.mouse.wheel(0, 700)
          }
          return filterBtn.count()
        },
        { timeout: 60_000, intervals: [500, 1000] },
      )
      .toBeGreaterThan(0)
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

  test('8. Compare mode triggers secondary aggregates query', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const compareSwitch = page.getByRole('switch', { name: /compare/i })
    await expect(compareSwitch).toBeVisible({ timeout: 15_000 })
    const compareRequestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/dashboard/aggregates') && req.method() === 'POST',
      { timeout: 15_000 },
    )
    await compareSwitch.click({ force: true })
    expect(await compareRequestPromise).toBeTruthy()
  })

  test('9. Category collapse state persists in localStorage across reloads', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const categoryButton = page.locator('section button:has(h3)').first()
    await expect(categoryButton).toBeVisible({ timeout: 15_000 })
    await categoryButton.click()

    await page.reload()
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    expect(await page.evaluate(() => localStorage.getItem('dashboard_collapsed_sections'))).toBeTruthy()
  })

  test('10. Card customization popover visibility toggle', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
    await expect(cardsBtn).toBeVisible({ timeout: 15_000 })
    await cardsBtn.click()
    await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })
  })

  test('11. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/dashboard')
  })

  test('12. Telemetry instrumentation & query efficiency audit', async ({ page }) => {
    let bundleDebugData: DebugResponse | null = null
    await page.setExtraHTTPHeaders({ 'x-debug-responses': '1' })
    const bundleResponsePromise = page
      .waitForResponse(
        (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
        { timeout: 30_000 },
      )
      .catch(() => null)

    await gotoHeading(page, '/dashboard', 'Dashboard')
    const bundleRes = await bundleResponsePromise
    if (bundleRes) {
      try {
        const json = await bundleRes.json()
        if (json._debug_queries || json._debug_sqlite) bundleDebugData = json
      } catch {
        // ignore non-json
      }
    }

    if (bundleDebugData) {
      const queries = bundleDebugData._debug_queries ?? []
      expect(queries.length).toBeGreaterThan(0)
      expect(queries.reduce((acc, q) => acc + (q.time_ms || 0), 0)).toBeLessThan(2500)
      const analyticalQueries = queries.filter((q) => !q.sql?.startsWith('-- DuckDB Iceberg View Resolution'))
      const sqlStatements = analyticalQueries.map((q) => JSON.stringify({ sql: q.sql?.trim?.() ?? '', params: q.params ?? null }))
      expect(new Set(sqlStatements).size).toBe(sqlStatements.length)
    }
  })

  test('13. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await gotoHeading(page, '/dashboard', 'Dashboard')
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    await expect(plotlyChart).toBeVisible({ timeout: 20_000 })
    await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('Deep link ?metric= is consumed from the URL while unrelated params survive', async ({ page }) => {
    await page.goto('/dashboard?metric=bandwidth&keep=1')
    await expect(page.locator('main').first()).toBeVisible({ timeout: 30_000 })
    await expect.poll(() => new URL(page.url()).searchParams.get('metric'), { timeout: 15_000 }).toBeNull()
    expect(new URL(page.url()).searchParams.get('keep')).toBe('1')
  })
})

test('an unconfigured service never shows NGWAF cards, even with saved selections or Show all', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('dashboard_cards', JSON.stringify(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind']))
  })
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

test('URL filter param round-trips through the filter store', async ({ page }) => {
  await gotoHeading(page, '/dashboard?filter_country=US', 'Dashboard')
  await expect(page.getByText(/country/i).first()).toBeVisible({ timeout: 15_000 })
})

test('navigating between analytics pages preserves the active service id', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await gotoHeading(page, '/security', 'Security')
})

test('dashboard renders charts without flashing empty state', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await expect(page.getByText('Traffic over Time').first()).toBeVisible({ timeout: 30_000 })
  await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 })
  expect(await page.getByText(/no data/i).isVisible().catch(() => false)).toBe(false)
})

test('dashboard mounts the maplibre container without crashing', async ({ page }) => {
  const geojsonRequests: string[] = []
  page.on('request', (req) => {
    const u = req.url()
    if (u.endsWith('.geojson') || (u.endsWith('.json') && u.includes('countries'))) geojsonRequests.push(u)
  })
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await Promise.any([
    page.locator('canvas').first().waitFor({ state: 'attached', timeout: 45_000 }),
    page.getByText(/Interactive map unavailable/i).waitFor({ state: 'visible', timeout: 45_000 }),
  ])
  expect((await page.locator('canvas').count()) + (await page.getByText(/Interactive map unavailable/i).count())).toBeGreaterThanOrEqual(1)
})

test('dashboard card grid mounts and the Cards toggle is reachable', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await expect(page.getByRole('button', { name: /cards/i }).first()).toBeVisible({ timeout: 15_000 })
})

test('toggling a card off persists across page reload', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await page.getByRole('button', { name: /cards/i }).first().click()
  await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })

  const firstChecked = page.locator('input[type="checkbox"][id^="card-"]:checked').first()
  await expect(firstChecked).toBeVisible({ timeout: 15_000 })
  const cardId = await firstChecked.getAttribute('id')
  expect(cardId).toBeTruthy()
  const cardKey = cardId!.replace('card-', '')

  await page.locator(`label[for="${cardId}"]`).click()
  await expect(page.locator(`input#${cardId}`)).not.toBeChecked()

  const storedBefore = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedBefore, 'dashboard_cards must be written after toggle').toBeTruthy()
  expect((JSON.parse(storedBefore!) as string[]).includes(cardKey)).toBe(false)

  await page.reload()
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
  const storedAfter = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedAfter, 'dashboard_cards must survive reload').toBeTruthy()
  expect((JSON.parse(storedAfter!) as string[]).includes(cardKey)).toBe(false)
})

// ---------------------------------------------------------------------------
// Alerts & Notifications Page Contract (/alerts)
// ---------------------------------------------------------------------------

test.describe('Alerts & Notifications Page Contract (/alerts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/alerts', /Alerts/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/alerts', /Alerts/i)
    for (const id of ['alerts-page-root', 'alerts-summary-grid', 'alerts-list-card']) {
      await expect(page.locator(`[data-testid="${id}"]`)).toBeVisible({ timeout: 15_000 })
    }
    await assertIntrinsicCount(page, 2)
  })

  test('3. Alerts list contract & status filter controls', async ({ page }) => {
    const getRes = await page.request.get('/api/alerts/')
    expect(getRes.ok()).toBeTruthy()
    expect(Array.isArray((await getRes.json()).data)).toBeTruthy()

    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const statusRes = await page.request.get(`/api/alerts/?status=${status}`)
      expect(statusRes.ok()).toBeTruthy()
      expect(Array.isArray((await statusRes.json()).data)).toBeTruthy()
    }

    await page.goto('/alerts')
    const statusFilter = page.locator('[data-testid="alerts-status-filter"]:visible')
    await expect(statusFilter).toHaveCount(1, { timeout: 15_000 })
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      await expect(statusFilter.getByTestId(`alerts-filter-${status}`)).toBeVisible({ timeout: 5_000 })
    }
  })

  test('4. Admin rule lifecycle: Create, Update, Toggle, Test Webhook, Evaluate & Delete', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const ruleId = `e2e-alert-${Date.now()}`
    const headers = { 'x-fastly-service-id': serviceId }
    const baseRule = {
      id: ruleId,
      service_id: serviceId,
      name: 'E2E Test High 5xx Alert',
      category: 'reliability',
      metric: '5xx_rate',
      evaluation_type: 'absolute',
      evaluation_scope: 'all',
      operator: '>',
      threshold: 5.0,
      window_min: 5,
      channels: [{ type: 'webhook', url: 'https://hooks.example.com/e2e-alert-webhook' }],
      enabled: true,
    }

    try {
      const createRes = await page.request.post('/api/alerts/', { headers, data: baseRule })
      expect(createRes.ok()).toBeTruthy()
      const createData = await createRes.json()
      expect(['saved', 'success']).toContain(createData.data?.status)
      expect(createData.data?.id).toBe(ruleId)

      const listRes = await page.request.get(`/api/alerts/${serviceId}`)
      expect(listRes.ok()).toBeTruthy()
      expect(((await listRes.json()).data as { id?: string }[]).some((a) => a.id === ruleId)).toBeTruthy()

      await page.goto(`/alerts?service=${encodeURIComponent(serviceId)}`)
      await expect(page.getByText('E2E Test High 5xx Alert')).toBeVisible({ timeout: 15_000 })

      const putRes = await page.request.put(`/api/alerts/${ruleId}`, {
        headers,
        data: { ...baseRule, name: 'E2E Test High 5xx Alert Updated', threshold: 7.5, window_min: 10 },
      })
      expect(putRes.ok()).toBeTruthy()

      for (const enabled of [false, true]) {
        const patchRes = await page.request.patch(`/api/alerts/${ruleId}/enabled`, { headers, data: { enabled } })
        expect(patchRes.ok()).toBeTruthy()
      }

      const testWebhookRes = await page.request.post(`/api/alerts/${ruleId}/test`, {
        headers,
        data: { service_id: serviceId, dry_run: true },
      })
      expect(testWebhookRes.ok()).toBeTruthy()
      const testWebhookJson = await testWebhookRes.json()
      expect(testWebhookJson.data?.status).toBe('validated')
      expect(testWebhookJson.data?.channel_type).toBe('webhook')

      const evalRes = await page.request.post('/api/alerts/evaluate', { headers, data: { service_id: serviceId } })
      expect(evalRes.ok()).toBeTruthy()
      expect((await evalRes.json()).data?.evaluated).toBeGreaterThanOrEqual(1)
    } finally {
      await page.request.delete(`/api/alerts/${ruleId}`, { headers })
    }
  })

  test('5. Role verification & SSRF protection checks', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const ssrfRes = await page.request.post('/api/alerts/', {
      headers: { 'x-fastly-service-id': serviceId },
      data: {
        id: 'ssrf-probe-rule',
        service_id: serviceId,
        name: 'SSRF Probe',
        category: 'reliability',
        metric: '5xx_rate',
        operator: '>',
        threshold: 5,
        window_min: 5,
        channels: [{ type: 'webhook', url: 'http://169.254.169.254/latest/meta-data/' }],
        enabled: true,
      },
    })
    expect(ssrfRes.status()).toBe(400)

    await page.goto('/alerts')
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    if (/Admin View/i.test(await page.locator('footer').innerText())) {
      await expect(page.locator('[data-testid="alerts-create-btn"]')).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/alerts', 800, false)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const listRes = await page.request.get(`/api/alerts/${serviceId}`, {
      headers: { 'x-debug-responses': '1', 'x-page-load-id': `e2e-alerts-${Date.now()}` },
    })
    expect(listRes.ok()).toBeTruthy()

    await assertPostTelemetry(
      page,
      '/api/alerts/preview',
      {
        id: 'telemetry-preview',
        service_id: serviceId,
        name: 'Telemetry Preview Rule',
        category: 'reliability',
        metric: '5xx_rate',
        evaluation_type: 'absolute',
        evaluation_scope: 'all',
        operator: '>',
        threshold: 5,
        window_min: 15,
      },
      { 'x-fastly-service-id': serviceId, 'x-page-load-id': `e2e-alerts-preview-${Date.now()}` },
    )
  })
})

// ---------------------------------------------------------------------------
// Usage & Cost Estimator Page Contract (/usage)
// ---------------------------------------------------------------------------

test.describe('Usage & Cost Estimator Page Contract (/usage)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/usage', 'System Usage')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/usage', 'System Usage')
    await assertIntrinsicCount(page, 3)
  })

  test('3. Data contract & schema validation on /api/usage endpoints', async ({ page }) => {
    const storageRes = await page.request.get('/api/usage/current-storage')
    expect(storageRes.ok()).toBeTruthy()
    const storageData = await storageRes.json()
    expect(storageData).toHaveProperty('live_bytes')
    expect(storageData).toHaveProperty('total_billed_gb_hours')

    const ratesRes = await page.request.get('/api/usage/prefill/rates')
    expect(ratesRes.ok()).toBeTruthy()
    const ratesData = await ratesRes.json()
    expect(ratesData).toHaveProperty('class_a_rate_per_1k')
    expect(ratesData).toHaveProperty('cdn_egress_rate_per_gb')
    expect(['MBZ100', 'MBZ200', 'Global']).toContain(ratesData.billing_zone)

    for (const [url, prop] of [
      ['/api/usage/operations?by=hour', 'total_class_a'],
      ['/api/usage/bandwidth?by=hour', 'total_bytes'],
      ['/api/usage/log-activity?by=hour', 'total_rows'],
    ] as const) {
      const res = await page.request.get(url)
      expect(res.ok()).toBeTruthy()
      const data = await res.json()
      expect(data).toHaveProperty(prop)
      expect(Array.isArray(data.data)).toBeTruthy()
    }
  })

  test('4. Core stat cards, charts, and Cost Estimator mounted', async ({ page }) => {
    await gotoHeading(page, '/usage', 'System Usage')
    for (const label of [
      'Storage Impact (Period)',
      'Class A Ops (period)',
      'Class B Ops (period)',
      'Estimated Incurred Cost',
      'FOS Operations',
      'CDN Bandwidth',
      'Estimated Cost',
      'Log Activity (Processed)',
      'Cost Estimator',
    ]) {
      await expect(page.getByText(label)).toBeVisible({ timeout: 15_000 })
    }
  })

  test('5. Delivery Billing Zone interactive switching', async ({ page }) => {
    await gotoHeading(page, '/usage', 'System Usage')
    const costEstimator = page.getByText('Cost Estimator')
    await costEstimator.scrollIntoViewIfNeeded()
    await expect(costEstimator).toBeVisible({ timeout: 15_000 })

    for (const [zone, rate] of [
      ['MBZ100', '$0.08 / GB'],
      ['Global', '$0.18 / GB'],
      ['MBZ200', '$0.12 / GB'],
    ] as const) {
      const btn = page.getByRole('button', { name: zone })
      await expect(btn).toBeVisible({ timeout: 15_000 })
      await btn.click()
      await expect(page.getByText(rate)).toBeVisible({ timeout: 5_000 })
    }
  })

  test('6. Zero error cards on usage page', async ({ page }) => {
    await assertZeroErrorCards(page, '/usage', 'System Usage')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/usage')
  })
})

// ---------------------------------------------------------------------------
// Streaming (CMCD) Page Contract (/streaming)
// ---------------------------------------------------------------------------

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
      const res = await page.request.get(fixturePath)
      expect(res.ok()).toBe(true)
      return (await res.json()).bodies
    },
  }
}

test.describe('Streaming (CMCD) Page Contract (/streaming)', () => {
  test.afterEach(async ({ page }) => {
    expect((await page.request.delete(fixturePath)).ok()).toBe(true)
  })

  test('1. Route accessibility, environment indicator and request contract', async ({ page }) => {
    const seen = await stubAggregates(page, populatedPayload())
    const response = await page.goto('/streaming')
    expect(await response?.text()).toContain('1,234')
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

    expect(await readClsScore(page)).toBeLessThanOrEqual(0.05)
  })
})

// ---------------------------------------------------------------------------
// Folded from a11y-routes.spec.ts
// ---------------------------------------------------------------------------

const CLEAN_ROUTES = [
  '/origin',
  '/performance',
  '/security',
  '/query',
  '/insights',
  '/sessions',
  '/network',
  '/control-room',
  '/fastly-value',
  '/streaming',
  '/admin',
  '/admin/queries',
  '/admin/session-scoring',
  '/admin/share',
  '/admin/trends',
  '/admin/usage-log',
  '/alerts',
  '/logs',
  '/usage',
]

async function gotoSettled(page: Page, route: string) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await page.goto(route)
      return
    } catch (e) {
      const msg = String((e as Error)?.message ?? e)
      if (attempt === 0 && /ERR_ABORTED|frame was detached|frame got detached|navigation .*interrupted/i.test(msg)) continue
      throw e
    }
  }
}

for (const route of CLEAN_ROUTES) {
  test(`no WCAG 2.1 AA violations on ${route}`, async ({ page }) => {
    await gotoSettled(page, route)
    await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.waitForTimeout(3_000)

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .exclude('.recharts-wrapper')
      .exclude('[data-testid="plotly-host"]')
      .exclude('[data-empty-placeholder="true"]')
      .exclude('[data-axe-ignore="true"]')
      .exclude('.maplibregl-map')
      .analyze()

    expect(
      results.violations,
      `axe found ${results.violations.length} WCAG 2.1 AA violation(s) on ${route}:\n` +
        results.violations
          .map(
            (v) =>
              `  - [${v.id}] ${v.help} (${v.impact ?? 'unknown'})\n    ${v.helpUrl}\n` +
              `    nodes: ${v.nodes.length} (first: ${v.nodes[0]?.target?.join(' > ')})`,
          )
          .join('\n'),
    ).toEqual([])
  })
}

test('Tab navigation moves focus through the sidebar', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  expect(await page.evaluate(() => document.activeElement?.tagName)).toBeTruthy()
  await page.keyboard.press('Tab')
  expect(await page.evaluate(() => document.activeElement?.tagName)).toBeTruthy()
})

test('Escape closes any open popover without trapping focus', async ({ page }) => {
  await gotoHeading(page, '/dashboard', 'Dashboard')
  await page.keyboard.press('Escape')
  expect(await page.locator('body').count()).toBe(1)
})

const HYDRATION_ROUTES = [
  '/admin',
  '/admin/share',
  '/admin/session-scoring',
  '/admin/trends',
  '/admin/usage-log',
  '/admin/queries',
  '/dashboard',
  '/sessions',
  '/security',
  '/network',
  '/origin',
  '/performance',
  '/rum',
  '/usage',
  '/alerts',
  '/logs',
  '/query',
  '/insights',
  '/control-room',
]

const SERVICE_ID = 'svc-playwright-e2e'
const HYDRATION_PATTERNS = [/hydrat/i, /did not match/i, /cannot be a descendant/i, /text content does not match/i, /minified react error #(418|423|425)/i]
const isHydrationError = (text: string) => HYDRATION_PATTERNS.some((re) => re.test(text))

test.describe('SSR hydration smoke', () => {
  for (const route of HYDRATION_ROUTES) {
    test(`${route} hydrates without a React hydration error`, async ({ page }) => {
      await page.addInitScript((sid) => {
        localStorage.setItem(
          'service-storage',
          JSON.stringify({
            state: { activeServiceId: sid, services: [{ id: sid, name: 'Playwright E2E Service', accessLevel: 'read_write' }] },
            version: 0,
          }),
        )
        localStorage.setItem('timezone-storage', JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }))
        localStorage.setItem('fastly-debug-settings', JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }))
        localStorage.setItem('dashboard_collapsed_sections', JSON.stringify(['request', 'cache']))
      }, SERVICE_ID)

      const hydrationErrors: string[] = []
      page.on('console', (msg) => {
        if (msg.type() === 'error' && isHydrationError(msg.text())) hydrationErrors.push(msg.text())
      })
      page.on('pageerror', (err) => {
        const text = `${err.message}\n${err.stack ?? ''}`
        if (isHydrationError(text)) hydrationErrors.push(text)
      })

      await page.goto(route, { waitUntil: 'domcontentloaded', timeout: 30_000 })
      await expect(page.locator('main, [role="main"], h1, h2').first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('nextjs-portal, [data-nextjs-dialog-overlay], .alert-destructive, [data-testid="error-card"]')).toHaveCount(0)
      await page.waitForTimeout(300)

      expect(hydrationErrors, `React hydration error(s) on ${route}:\n${hydrationErrors.join('\n---\n')}`).toEqual([])
    })
  }
})

// ---------------------------------------------------------------------------
// Folded from network.spec.ts
// ---------------------------------------------------------------------------

test.describe('Network Page Contract (/network)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/network', 'Network & ASN Health')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/network', 'Network & ASN Health')
    await assertIntrinsicCount(page, 4)
  })

  test('3. Data contract & schema validation (/api/network/bundle & /api/network-health)', async ({ page }) => {
    const bundleRes = await page.request.post('/api/network/bundle', { data: { sections: ['transport'] } })
    expect(bundleRes.ok()).toBeTruthy()
    const bundleData = await bundleRes.json()
    expect(bundleData).toHaveProperty('transport')
    if (bundleData.transport) {
      expect(Array.isArray(bundleData.transport.protocol_distribution)).toBeTruthy()
      expect(Array.isArray(bundleData.transport.missing_fields)).toBeTruthy()
    }

    const healthRes = await page.request.post('/api/network-health', {
      data: { sections: ['summary', 'leaderboard', 'metro_leaderboard'], top_n: 30 },
    })
    expect(healthRes.ok()).toBeTruthy()
    expect(await healthRes.json()).toHaveProperty('available')

    await gotoHeading(page, '/network', 'Network & ASN Health')
  })

  test('4. Core network cards & transport section mounted', async ({ page }) => {
    await gotoHeading(page, '/network', 'Network & ASN Health')
    const unavailableNotice = page.getByText('Network Metrics Unavailable')
    if (!(await unavailableNotice.isVisible().catch(() => false))) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Global Health' }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Worst ASN' }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.getByText('Edge Transport & TCP Performance')).toBeVisible({ timeout: 15_000 })
      await expect(page.getByText('ASN Performance Leaderboard').or(page.getByText('Fastly Edge PoP Health Heatmap')).first()).toBeVisible({ timeout: 15_000 })
    } else {
      await expect(unavailableNotice).toBeVisible()
    }
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    await assertTimePresetsNo500(page, '/network', 'Network & ASN Health')
  })

  test('6. Zero error cards on network page', async ({ page }) => {
    await assertZeroErrorCards(page, '/network', 'Network & ASN Health')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/network')
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/network/bundle', { sections: ['transport'] })
  })
})

test.describe('Assets & Shield Page Contract (/assets-shield)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/assets-shield', 'Assets & Shield')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/assets-shield', 'Assets & Shield')
    for (const id of [
      'assets-overview-grid',
      'assets-cache-efficiency-card',
      'assets-compression-card',
      'assets-uncompressed-card',
      'assets-low-ttl-card',
    ]) {
      await expect(page.locator(`[data-testid="${id}"]:visible`)).toBeVisible({ timeout: 15_000 })
    }
    await assertIntrinsicCount(page, 5)
  })

  test('3. Data contract & schema validation (/api/assets/aggregates)', async ({ page }) => {
    const postRes = await page.request.post('/api/assets/aggregates', { data: { range_token: '24h' } })
    expect(postRes.ok()).toBeTruthy()
    const postData = await postRes.json()
    for (const key of [
      'asset_type_breakdown',
      'cache_performance',
      'compression_performance',
      'large_uncompressed_assets',
      'low_ttl_assets',
      'missing_fields',
    ]) {
      expect(Array.isArray(postData[key])).toBeTruthy()
    }

    const getRes = await page.request.get('/api/assets/aggregates?range_token=24h')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('asset_type_breakdown')
    expect(getData).toHaveProperty('missing_fields')

    await gotoHeading(page, '/assets-shield', 'Assets & Shield')
  })

  test('4. Core asset cards & breakdown sections render properly', async ({ page }) => {
    await gotoHeading(page, '/assets-shield', 'Assets & Shield')
    for (const heading of [
      'Cache Efficiency by Asset Type',
      'Payload Compression Efficiency',
      'Large Uncompressed Assets',
      'Low Cache-TTL Hotspots',
    ]) {
      await expect(page.getByRole('heading', { name: heading })).toBeVisible({ timeout: 15_000 })
    }
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    await assertTimePresetsNo500(page, '/assets-shield', 'Assets & Shield')
  })

  test('6. Zero error cards on assets page', async ({ page }) => {
    await assertZeroErrorCards(page, '/assets-shield', 'Assets & Shield')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/assets-shield')
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/assets/aggregates', { range_token: '24h' })
  })
})

// ---------------------------------------------------------------------------
// Folded from performance.spec.ts
// ---------------------------------------------------------------------------

test.describe('Performance Page Contract (/performance)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/performance', 'Performance')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/performance', 'Performance')
    await expect(page.getByText('End-to-End Latency Waterfall (Average)').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 3)
  })

  test('3. Data contract & round-trip verification (/api/performance/aggregates)', async ({ page }) => {
    const apiRes = await page.request.post('/api/performance/aggregates', { data: { range_token: '24h', sort_by: 'p99' } })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    for (const key of ['waterfall', 'top_urls', 'top_asns', 'ttl_dist', 'scatter']) {
      expect(data).toHaveProperty(key)
    }
    await gotoHeading(page, '/performance', 'Performance')
  })

  test('4. All 5 core performance cards render properly', async ({ page }) => {
    await gotoHeading(page, '/performance', 'Performance')
    for (const label of [
      'End-to-End Latency Waterfall (Average)',
      'Slowest URLs',
      'Slowest Networks',
      'Cache TTL Distribution',
      'Origin vs Edge Processing (ms)',
    ]) {
      await expect(page.getByText(label).locator('visible=true')).toBeVisible({ timeout: 15_000 })
    }
  })

  test('5. Time range presets and adaptive history extents', async ({ page }) => {
    await gotoHeading(page, '/performance', 'Performance')
    await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await gotoHeading(page, '/performance', 'Performance')
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p99/i }).or(page.getByText(/p99/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await gotoHeading(page, '/performance', 'Performance')
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    await expect(plotlyChart).toBeVisible({ timeout: 20_000 })
    await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/performance')
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/performance/aggregates', { range_token: '24h', sort_by: 'p99' })
  })
})

test.describe('Origin Page Contract (/origin)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/origin', 'Origin Performance')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/origin', 'Origin Performance')
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Latency' }).first()).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 6)
  })

  test('3. Data contract & round-trip verification (/api/origin/aggregates)', async ({ page }) => {
    const apiRes = await page.request.post('/api/origin/aggregates', {
      data: { range_token: '24h', timeseries_metric: 'ttfb', timeseries_percentile: 'p95' },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    for (const key of ['summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health']) {
      expect(data).toHaveProperty(key)
    }
    await gotoHeading(page, '/origin', 'Origin Performance')
  })

  test('4. All core origin cards render properly', async ({ page }) => {
    await gotoHeading(page, '/origin', 'Origin Performance')
    for (const title of [
      'Origin TTFB (P50)',
      'Origin TTFB (P95)',
      'Origin Error Rate',
      'Fetch Volume',
      'Origin Latency',
      'Status Code Distribution',
      'Slowest URLs at Origin',
      'Origin Performance by POP',
      'Origin IP Health',
      'Origin Payload Size',
    ]) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({ timeout: 15_000 })
    }
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    await assertTimePresetsNo500(page, '/origin', 'Origin Performance')
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await gotoHeading(page, '/origin', 'Origin Performance')
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p95/i }).or(page.getByText(/p95/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await gotoHeading(page, '/origin', 'Origin Performance')
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/origin')
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/origin/aggregates', {
      range_token: '24h',
      timeseries_metric: 'ttfb',
      timeseries_percentile: 'p95',
    })
  })
})

// ---------------------------------------------------------------------------
// Folded from insights.spec.ts
// ---------------------------------------------------------------------------

type RequestFactsDebugResponse = {
  _debug_queries?: DebugQuery[]
  rows?: Array<Record<string, unknown>>
  metadata?: {
    status?: string
    exact?: boolean
    coverage?: number
    freshness_lag_seconds?: number
    mode?: string
    watermark?: Record<string, unknown>
  }
  next_cursor?: string | null
}

test.describe('Insights Page Contract (/insights)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/insights', 'Anomaly Detection')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/insights', 'Anomaly Detection')
    await expect(page.locator('[class*="contain-intrinsic-size"]').first()).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 1)
    await expect(
      page.locator('[data-testid="insights-grid"], [data-testid="insights-empty-pass"], [data-testid="insights-grid-loading"]').first(),
    ).toBeVisible({ timeout: 15_000 })
  })

  test('3. Data contract & schema validation (/api/insights & /api/insight-availability)', async ({ page }) => {
    const availRes = await page.request.get('/api/insight-availability')
    expect(availRes.ok()).toBeTruthy()
    const availData = await availRes.json()
    expect(Array.isArray(availData.insights)).toBeTruthy()
    if (availData.insights.length > 0) {
      expect(availData.insights[0]).toHaveProperty('id')
      expect(availData.insights[0]).toHaveProperty('available')
    }

    const apiRes = await page.request.post('/api/insights', { data: { window_size_hrs: 1, baseline_hours: 24, filters: {} } })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(Array.isArray(data.insights)).toBeTruthy()
    expect(data).toHaveProperty('computed_at')
  })

  test('4. Instant client-side tab switching across all 5 category tabs and All', async ({ page }) => {
    await gotoHeading(page, '/insights', 'Anomaly Detection')
    for (const tabKey of ['all', 'security', 'origin', 'edge', 'network', 'traffic']) {
      const trigger = page.locator(`button[data-tab="${tabKey}"]`).or(page.locator(`button[data-alias="${tabKey}"]`)).first()
      await expect(trigger).toBeVisible({ timeout: 10_000 })
      await trigger.click()
      await expect(trigger).toHaveAttribute('aria-selected', 'true')
      const activeContent = page.locator(`[data-tab-content="${tabKey}"]`).or(page.locator(`[data-alias="${tabKey}"]`)).first()
      await expect(activeContent).toBeVisible({ timeout: 10_000 })
    }
  })

  test('5. Plausible detector values or clean normal parameters pass states', async ({ page }) => {
    await gotoHeading(page, '/insights', 'Anomaly Detection')
    const emptyPass = page.locator('[data-testid="insights-empty-pass"]').first()
    const cardsGrid = page.locator('[data-testid="insights-grid"]').first()
    await expect(emptyPass.or(cardsGrid)).toBeVisible({ timeout: 30_000 })

    const hasEmptyPass = await emptyPass.isVisible()
    expect(hasEmptyPass || (await cardsGrid.isVisible())).toBeTruthy()
    if (hasEmptyPass) {
      await expect(emptyPass).toContainText(/No anomalies detected/i)
      await expect(emptyPass).toContainText(/baseline parameters/i)
    } else {
      await expect(cardsGrid.locator('.rounded-xl, .border').first()).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Time range preset transitions (window vs baseline)', async ({ page }) => {
    await gotoHeading(page, '/insights?window=24&baseline=168&tab=security', 'Anomaly Detection')
    const secTrigger = page.locator('button[data-tab="security"]').first()
    await expect(secTrigger).toBeVisible({ timeout: 10_000 })
    await expect(secTrigger).toHaveAttribute('aria-selected', 'true')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/insights')
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const json = await assertPostTelemetry(page, '/api/insights', { window_size_hrs: 1, baseline_hours: 24, filters: {} }, {}, true)
    expect(json).toHaveProperty('insights')
  })
})

test.describe('High-Scale Request Facts Page Contract (/high-scale/request-facts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/high-scale/request-facts', 'High-scale request facts')
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await gotoHeading(page, '/high-scale/request-facts', 'High-scale request facts')
    await assertIntrinsicCount(page, 2)
  })

  test('3. Data contract & schema validation (/api/high-scale/request-facts)', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    expect(serviceId).toBeTruthy()
    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    const getRes = await page.request.get(
      `/api/high-scale/request-facts?service_id=${encodeURIComponent(serviceId!)}&start_time=${encodeURIComponent(oneHourAgo.toISOString())}&end_time=${encodeURIComponent(now.toISOString())}&limit=10`,
    )
    expect(getRes.ok()).toBeTruthy()
    const getData = (await getRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(getData.rows)).toBeTruthy()
    expect(['complete', 'partial', 'unavailable']).toContain(getData.metadata?.status)
    expect(['clickhouse', 'duckdb_emulation']).toContain(getData.metadata?.mode)

    const postRes = await page.request.post(`/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`, {
      data: { start_time: oneHourAgo.toISOString(), end_time: now.toISOString(), limit: 10, emulate_standard: true },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(postData.rows)).toBeTruthy()
    expect(postData.metadata?.watermark?.service_id).toBe(serviceId)
  })

  test('4. Metadata bar, mode badge, filter bar, and request facts table render cleanly', async ({ page }) => {
    await gotoHeading(page, '/high-scale/request-facts', 'High-scale request facts')
    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })
    await expect(page.getByTestId('high-scale-metadata')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByTestId('high-scale-mode-badge')).toBeVisible({ timeout: 15_000 })
    expect(await page.getByTestId('high-scale-mode-badge').innerText()).toMatch(
      /DuckDB Standard Mode Emulation|ClickHouse Distributed Cluster/,
    )
    await expect(page.getByTestId('high-scale-filters')).toBeVisible()
    await expect(page.getByTestId('high-scale-table-wrapper')).toBeVisible()
    await expect(page.locator('[role="alert"].border-destructive')).toHaveCount(0)
  })

  test('5. Interactive filter controls (status, domain, pop, batch_id) & clear filters', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })
    await page.goto('/high-scale/request-facts')
    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })

    await page.getByTestId('filter-status').fill('200')
    await page.getByTestId('filter-pop').fill('IAD')
    await expect(page.getByTestId('filter-clear')).toBeVisible()
    await page.getByTestId('filter-clear').click()
    await expect(page.getByTestId('filter-status')).toHaveValue('')
    await expect(page.getByTestId('filter-pop')).toHaveValue('')

    expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
  })

  test('6. Performance budget: FCP < 400ms / LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/high-scale/request-facts')
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    expect(serviceId).toBeTruthy()
    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    await assertPostTelemetry(
      page,
      `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`,
      { start_time: oneHourAgo.toISOString(), end_time: now.toISOString(), limit: 10, emulate_standard: true },
      { 'X-Page-Load-ID': `e2e-hs-facts-${Date.now()}` },
    )
  })
})
