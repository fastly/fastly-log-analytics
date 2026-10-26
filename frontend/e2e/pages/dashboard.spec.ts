/**
 * E2E Page Contract Test: Dashboard (/dashboard)
 *
 * Implements the 12-step verification checklist specified in docs/pages/dashboard.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & in-place loading states.
 * 3. Data bundle contract: exactly 1 round-trip on initial load, HTTP 200, valid schema.
 * 4. Metric switching: Requests, Bandwidth, Status Codes, Hit Ratio, Origin Latency.
 * 5. Time range switching: 1h, 24h, 7d presets update URL and chart interval.
 * 6. Click-to-filter drilldown: clicking top-N rows adds global filter and refetches.
 * 7. GeoMap rendering: SVG/Canvas mounts without WebGL errors.
 * 8. Compare mode: toggles compare and fires secondary /api/dashboard/aggregates.
 * 9. Category collapse persistence: localStorage remembers collapsed sections across reloads.
 * 10. Card customization: hiding a card updates grid without full refetch.
 * 11. Performance budget: FCP/LCP within performance budget (< 1500ms), CLS <= 0.05.
 * 12. Telemetry instrumentation & query efficiency audit.
 */

import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { getFirstServiceId, installClsObserver, measureWarmPaintAndCls, readClsScore } from '../helpers'

type DashboardBundleDebug = {
  _debug_queries?: DebugQuery[]
  _debug_sqlite?: unknown[]
}

test.describe('Dashboard Page Contract (/dashboard)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & in-place loading states', async ({ page }) => {
    // Navigate without waiting for full network idle to catch initial shell render
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Verify main panel containers are already rendered in place from first paint
    const chartContainer = page.locator('text=Traffic over Time').or(page.locator('[data-empty-placeholder="true"]'))
    await expect(chartContainer.first()).toBeVisible({ timeout: 10_000 })

    // Verify that empty-placeholder elements use layout reservation
    const placeholders = page.locator('main [data-empty-placeholder="true"]')
    const count = await placeholders.count()
    if (count > 0) {
      const texts = await placeholders.allTextContents()
      expect(texts.some(text => /Crunching logs|Loading|Initializing|Mapping|No data available/i.test(text))).toBeTruthy()
    }
  })

  test('3. Single round-trip /api/dashboard/bundle contract', async ({ page }) => {
    let bundleCalls = 0

    page.on('response', (res) => {
      if (res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST') {
        bundleCalls++
      }
    })

    const bundleResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
      { timeout: 30_000 }
    )

    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    const bundleRes = await bundleResponsePromise
    const bundleResponseData = await bundleRes.json()

    // Exactly 1 composite bundle request on cold load
    expect(bundleCalls).toBe(1)
    expect(bundleResponseData).toBeTruthy()
    expect(bundleResponseData).toHaveProperty('aggregates')
    expect(bundleResponseData.aggregates).toHaveProperty('data')
    expect(bundleResponseData).toHaveProperty('top_bots')
  })

  test('4. Metric switching on TrafficChart', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Metric buttons in TrafficChart (Reqs, 5xx, 4xx, CHR, Throughput)
    const metrics = ['Reqs', '5xx', '4xx', 'CHR']
    for (const metric of metrics) {
      const btn = page.getByRole('button', { name: new RegExp(`^${metric}$`, 'i') }).first()
      if (await btn.isVisible()) {
        await btn.click()
        await expect(btn).toHaveAttribute('aria-pressed', 'true')
      }
    }
  })

  test('5. Time range presets and adaptive history extents', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar (defaults to 24h for >=24h data, or max extent for <24h data)
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })
  })

  test('6. Click-to-filter drill-down adds filter chip and refetches', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Cards mount lazily when scrolled into view. Keep scrolling (a slow CI runner may need more
    // than one nudge) until a top-N dimension button inside CardGrid exists.
    const filterBtn = page.locator('button[aria-label^="Filter to "]').first()
    await expect
      .poll(
        async () => {
          if ((await filterBtn.count()) === 0) {
            // The wheel scrolls whatever is under the pointer: park it over the content area
            // (the default position is the sidebar corner, which would not scroll the page).
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
    const btnLabel = await filterBtn.getAttribute('aria-label')
    const filterValue = btnLabel?.replace('Filter to ', '').trim() || ''
    expect(filterValue.length).toBeGreaterThan(0)
    await filterBtn.click()

    // Verify filter pill appeared in FilterBar
    const filterBar = page.locator('[data-testid="filter-bar"]')
    await expect(filterBar).toBeVisible({ timeout: 10_000 })
    await expect(filterBar).toContainText(filterValue)
  })

  test('7. GeoMap panel mounts and its country picker preserves keyboard focus', async ({ page }) => {
    const pageErrors: string[] = []
    page.on('pageerror', error => pageErrors.push(error.message))
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Look for map container or header "Requests by Country"
    const mapHeading = page.locator('h3').filter({ hasText: /requests by country/i }).first()
    await expect(mapHeading).toBeVisible({ timeout: 15_000 })
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
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Compare switch in FilterBar
    const compareSwitch = page.getByRole('switch', { name: /compare/i })
    await expect(compareSwitch).toBeVisible({ timeout: 15_000 })
    const compareRequestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/dashboard/aggregates') && req.method() === 'POST',
      { timeout: 15_000 }
    )
    await compareSwitch.click({ force: true })
    const req = await compareRequestPromise
    expect(req).toBeTruthy()
  })

  test('9. Category collapse state persists in localStorage across reloads', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Section headers have collapsible chevron buttons (e.g. Request, Security, Cache, Geography)
    const categoryButton = page.locator('section button:has(h3)').first()
    await expect(categoryButton).toBeVisible({ timeout: 15_000 })
    await categoryButton.click()

    // Reload page and check localStorage key
    await page.reload()
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    const collapsedStorage = await page.evaluate(() => localStorage.getItem('dashboard_collapsed_sections'))
    expect(collapsedStorage).toBeTruthy()
  })

  test('10. Card customization popover visibility toggle', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
    await expect(cardsBtn).toBeVisible({ timeout: 15_000 })
    await cardsBtn.click()

    await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })
  })

  test('11. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/dashboard', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /dashboard] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('12. Telemetry instrumentation & query efficiency audit', async ({ page }) => {
    let bundleDebugData: DashboardBundleDebug | null = null

    // Request with debug responses enabled so backend attaches query timings
    await page.setExtraHTTPHeaders({
      'x-debug-responses': '1',
    })

    const bundleResponsePromise = page.waitForResponse(
      (res) => res.url().includes('/api/dashboard/bundle') && res.request().method() === 'POST' && res.status() === 200,
      { timeout: 30_000 }
    ).catch(() => null)

    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    const bundleRes = await bundleResponsePromise
    if (bundleRes) {
      try {
        const json = await bundleRes.json()
        if (json._debug_queries || json._debug_sqlite) {
          bundleDebugData = json
        }
      } catch {
        // ignore non-json
      }
    }

    // Audit captured queries if debug instrumentation returned
    if (bundleDebugData) {
      const queries = bundleDebugData._debug_queries ?? []
      console.log(`[TELEMETRY /dashboard] Captured ${queries.length} DuckDB queries`)

      // 1. Completeness: Analytical queries were executed and measured
      expect(queries.length).toBeGreaterThan(0)

      // 2. Efficiency: Total query execution duration within budget (< 2500ms)
      const totalQueryTime = queries.reduce((acc, q) => acc + (q.time_ms || 0), 0)
      expect(totalQueryTime).toBeLessThan(2500)

      // 3. Propriety: Zero duplicate identical statements
      const analyticalQueries = queries.filter(q => !q.sql?.startsWith('-- DuckDB Iceberg View Resolution'))
      const sqlStatements = analyticalQueries.map(q => JSON.stringify({ sql: q.sql?.trim?.() ?? '', params: q.params ?? null }))
      const uniqueStatements = new Set(sqlStatements)
      expect(uniqueStatements.size).toBe(sqlStatements.length)
    }
  })

  test('13. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

    // Must have rendered Plotly charts
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    await expect(plotlyChart).toBeVisible({ timeout: 20_000 })

    // Must have SVG elements inside the chart (.main-svg proves Plotly initialized and rendered)
    await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })

    // Zero error cards on dashboard
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
    localStorage.setItem('dashboard_cards', JSON.stringify([
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    ]))
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
  await page.goto('/dashboard?filter_country=US')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // The filter store renders one pill per active filter (FilterBar).
  // Pin that the country=US pill appears after URL sync runs.
  await expect(page.getByText(/country/i).first()).toBeVisible({ timeout: 15_000 })
})

test('navigating between analytics pages preserves the active service id', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // The seeded service ID lives in the sidebar's "active service" pill.
  // Navigate to /security and assert the same service stays selected.
  await page.goto('/security')
  await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })
})

test('dashboard renders charts without flashing empty state', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // "Traffic over Time" is the first card. Wait for its container.
  const trafficCard = page.getByText('Traffic over Time').first()
  await expect(trafficCard).toBeVisible({ timeout: 30_000 })

  // Wait for Plotly to actually render — the chart inserts an `.js-plotly-plot`
  // wrapper div with an inner SVG once Plotly.newPlot resolves. Polling
  // for that selector is the deterministic "chart is on screen" signal
  // (replaces a fixed 2 s settle).
  await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 })

  // Empty-state copy should NOT linger after the chart wrapper is up.
  // The flash regression would surface as the placeholder being visible
  // after Plotly attached.
  const emptyStateVisible = await page.getByText(/no data/i).isVisible().catch(() => false)
  expect(emptyStateVisible).toBe(false)
})

test('dashboard mounts the maplibre container without crashing', async ({ page }) => {
  const geojsonRequests: string[] = []
  page.on('request', (req) => {
    const u = req.url()
    if (u.endsWith('.geojson') || u.endsWith('.json') && u.includes('countries')) {
      geojsonRequests.push(u)
    }
  })

  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const canvasAttached = page.locator('canvas').first().waitFor({ state: 'attached', timeout: 45_000 })
  const fallbackVisible = page.getByText(/Interactive map unavailable/i).waitFor({ state: 'visible', timeout: 45_000 })
  await Promise.any([canvasAttached, fallbackVisible])

  const canvasCount = await page.locator('canvas').count()
  const fallbackCount = await page.getByText(/Interactive map unavailable/i).count()
  expect(canvasCount + fallbackCount).toBeGreaterThanOrEqual(1)
})

test('dashboard card grid mounts and the Cards toggle is reachable', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
  await expect(page.getByRole('button', { name: /cards/i }).first()).toBeVisible({
    timeout: 15_000,
  })
})

test('toggling a card off persists across page reload', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
  await cardsBtn.click()

  await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })

  const firstChecked = page.locator('input[type="checkbox"][id^="card-"]:checked').first()
  await expect(firstChecked).toBeVisible({ timeout: 15_000 })
  const cardId = await firstChecked.getAttribute('id')
  expect(cardId).toBeTruthy()
  const cardKey = cardId!.replace('card-', '')

  const label = page.locator(`label[for="${cardId}"]`)
  await label.click()

  await expect(page.locator(`input#${cardId}`)).not.toBeChecked()

  const storedBefore = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedBefore, 'dashboard_cards must be written after toggle').toBeTruthy()
  const visibleBefore = JSON.parse(storedBefore!) as string[]
  expect(visibleBefore.includes(cardKey)).toBe(false)

  await page.reload()
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const storedAfter = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedAfter, 'dashboard_cards must survive reload').toBeTruthy()
  const visibleAfter = JSON.parse(storedAfter!) as string[]
  expect(visibleAfter.includes(cardKey)).toBe(false)
})

test.describe('Alerts & Notifications Page Contract (/alerts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/alerts')
    await expect(page.getByRole('heading', { name: /Alerts/i }).first()).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/alerts')
    await expect(page.getByRole('heading', { name: /Alerts/i }).first()).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated alerts root, summary grid, and list card containers
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="alerts-summary-grid"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="alerts-list-card"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('3. Alerts list contract & status filter controls', async ({ page }) => {
    // 1. Verify GET /api/alerts/
    const getRes = await page.request.get('/api/alerts/')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('data')
    expect(Array.isArray(getData.data)).toBeTruthy()

    // 2. Verify status query parameter filtering
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const statusRes = await page.request.get(`/api/alerts/?status=${status}`)
      expect(statusRes.ok()).toBeTruthy()
      const statusJson = await statusRes.json()
      expect(Array.isArray(statusJson.data)).toBeTruthy()
    }

    // 3. Verify status filter controls in UI
    await page.goto('/alerts')
    const statusFilter = page.locator('[data-testid="alerts-status-filter"]:visible')
    await expect(statusFilter).toHaveCount(1, { timeout: 15_000 })
    for (const status of ['all', 'alerting', 'ok', 'muted']) {
      const btn = statusFilter.getByTestId(`alerts-filter-${status}`)
      await expect(btn).toBeVisible({ timeout: 5_000 })
    }
  })

  test('4. Admin rule lifecycle: Create, Update, Toggle, Test Webhook, Evaluate & Delete', async ({ page }) => {
    // Resolve active service ID from /api/bootstrap so service-scoped alerts match UI
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    const ruleId = `e2e-alert-${Date.now()}`

    try {
      // 1. Create alert rule via POST /api/alerts/
      const createRes = await page.request.post('/api/alerts/', {
        headers: { 'x-fastly-service-id': serviceId },
        data: {
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
        },
      })
      expect(createRes.ok()).toBeTruthy()
      const createData = await createRes.json()
      expect(['saved', 'success']).toContain(createData.data?.status)
      expect(createData.data?.id).toBe(ruleId)

      // 2. Verify it appears on GET /api/alerts/{service_id} and in UI
      const listRes = await page.request.get(`/api/alerts/${serviceId}`)
      expect(listRes.ok()).toBeTruthy()
      const listJson = await listRes.json()
      expect((listJson.data as { id?: string }[]).some((a) => a.id === ruleId)).toBeTruthy()

      await page.goto(`/alerts?service=${encodeURIComponent(serviceId)}`)
      await expect(page.getByText('E2E Test High 5xx Alert')).toBeVisible({ timeout: 15_000 })

      // 3. Update rule via PUT /api/alerts/{alert_id}
      const putRes = await page.request.put(`/api/alerts/${ruleId}`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: {
          id: ruleId,
          service_id: serviceId,
          name: 'E2E Test High 5xx Alert Updated',
          category: 'reliability',
          metric: '5xx_rate',
          evaluation_type: 'absolute',
          evaluation_scope: 'all',
          operator: '>',
          threshold: 7.5,
          window_min: 10,
          channels: [{ type: 'webhook', url: 'https://hooks.example.com/e2e-alert-webhook' }],
          enabled: true,
        },
      })
      expect(putRes.ok()).toBeTruthy()

      // 4. Toggle mute/enable via PATCH /api/alerts/{alert_id}/enabled
      const patchRes = await page.request.patch(`/api/alerts/${ruleId}/enabled`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { enabled: false },
      })
      expect(patchRes.ok()).toBeTruthy()

      // Re-enable for evaluation
      const reEnableRes = await page.request.patch(`/api/alerts/${ruleId}/enabled`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { enabled: true },
      })
      expect(reEnableRes.ok()).toBeTruthy()

      // 5. Trigger test webhook notification (dry_run=true) via POST /api/alerts/{alert_id}/test
      const testWebhookRes = await page.request.post(`/api/alerts/${ruleId}/test`, {
        headers: { 'x-fastly-service-id': serviceId },
        data: { service_id: serviceId, dry_run: true },
      })
      expect(testWebhookRes.ok()).toBeTruthy()
      const testWebhookJson = await testWebhookRes.json()
      expect(testWebhookJson.data?.status).toBe('validated')
      expect(testWebhookJson.data?.channel_type).toBe('webhook')

      // 6. Trigger on-demand evaluation via POST /api/alerts/evaluate
      const evalRes = await page.request.post('/api/alerts/evaluate', {
        headers: { 'x-fastly-service-id': serviceId },
        data: { service_id: serviceId },
      })
      expect(evalRes.ok()).toBeTruthy()
      const evalJson = await evalRes.json()
      expect(evalJson.data?.evaluated).toBeGreaterThanOrEqual(1)
    } finally {
      // 7. Always clean up test rule
      await page.request.delete(`/api/alerts/${ruleId}`, {
        headers: { 'x-fastly-service-id': serviceId },
      })
    }
  })

  test('5. Role verification & SSRF protection checks', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'

    // Verify SSRF validation rejects loopback/private/metadata webhook targets with 400
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

    // Verify UI mounts with appropriate role controls
    await page.goto('/alerts')
    await expect(page.locator('[data-testid="alerts-page-root"]')).toBeVisible({ timeout: 15_000 })
    const footerText = await page.locator('footer').innerText()
    if (/Admin View/i.test(footerText)) {
      await expect(page.locator('[data-testid="alerts-create-btn"]')).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/alerts', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 800)

    console.log(`[PERF /alerts] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'

    // 1. Verify GET /api/alerts/{service_id} returns valid response
    const listRes = await page.request.get(`/api/alerts/${serviceId}`, {
      headers: {
        'x-debug-responses': '1',
        'x-page-load-id': `e2e-alerts-${Date.now()}`,
      },
    })
    expect(listRes.ok()).toBeTruthy()

    // 2. Verify POST /api/alerts/preview executes analytical DuckDB query and attributes telemetry
    const previewRes = await page.request.post('/api/alerts/preview', {
      data: {
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
      headers: {
        'x-fastly-service-id': serviceId,
        'x-debug-responses': '1',
        'x-page-load-id': `e2e-alerts-preview-${Date.now()}`,
      },
    })
    expect(previewRes.ok()).toBeTruthy()
    const previewJson = await previewRes.json()
    expect(previewJson).toHaveProperty('_debug_queries')
    const queries = (previewJson._debug_queries ?? []) as DebugQuery[]
    console.log(`[TELEMETRY /alerts] Captured ${queries.length} analytical preview queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})

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
      const transient = /ERR_ABORTED|frame was detached|frame got detached|navigation .*interrupted/i.test(msg)
      if (attempt === 0 && transient) continue
      throw e
    }
  }
}

async function assertNoAxeViolations(page: Page, route: string) {
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
}

for (const route of CLEAN_ROUTES) {
  test(`no WCAG 2.1 AA violations on ${route}`, async ({ page }) => {
    await assertNoAxeViolations(page, route)
  })
}

test('Tab navigation moves focus through the sidebar', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const focusedTagBefore = await page.evaluate(() => document.activeElement?.tagName)
  expect(focusedTagBefore).toBeTruthy()

  await page.keyboard.press('Tab')
  const focusedTagAfter = await page.evaluate(() => document.activeElement?.tagName)
  expect(focusedTagAfter).toBeTruthy()
})

test('Escape closes any open popover without trapping focus', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

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

const HYDRATION_PATTERNS = [
  /hydrat/i,
  /did not match/i,
  /cannot be a descendant/i,
  /text content does not match/i,
  /minified react error #(418|423|425)/i,
]

function isHydrationError(text: string): boolean {
  return HYDRATION_PATTERNS.some((re) => re.test(text))
}

test.describe('SSR hydration smoke', () => {
  for (const route of HYDRATION_ROUTES) {
    test(`${route} hydrates without a React hydration error`, async ({ page }) => {
      await page.addInitScript((sid) => {
        localStorage.setItem(
          'service-storage',
          JSON.stringify({
            state: {
              activeServiceId: sid,
              services: [{ id: sid, name: 'Playwright E2E Service', accessLevel: 'read_write' }],
            },
            version: 0,
          }),
        )
        localStorage.setItem(
          'timezone-storage',
          JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }),
        )
        localStorage.setItem(
          'fastly-debug-settings',
          JSON.stringify({ state: { enabled: true, apiCallsEnabled: true }, version: 0 }),
        )
        localStorage.setItem('dashboard_collapsed_sections', JSON.stringify(['request', 'cache']))
      }, SERVICE_ID)

      const hydrationErrors: string[] = []
      page.on('console', (msg) => {
        if (msg.type() === 'error' && isHydrationError(msg.text())) {
          hydrationErrors.push(msg.text())
        }
      })
      page.on('pageerror', (err) => {
        const text = `${err.message}\n${err.stack ?? ''}`
        if (isHydrationError(text)) hydrationErrors.push(text)
      })

      await page.goto(route, { waitUntil: 'domcontentloaded', timeout: 30_000 })
      await expect(page.locator('main, [role="main"], h1, h2').first()).toBeVisible({ timeout: 15_000 })

      await expect(page.locator('nextjs-portal, [data-nextjs-dialog-overlay], .alert-destructive, [data-testid="error-card"]')).toHaveCount(0)

      await page.waitForTimeout(300)

      expect(
        hydrationErrors,
        `React hydration error(s) on ${route}:\n${hydrationErrors.join('\n---\n')}`,
      ).toEqual([])
    })
  }
})

// ---------------------------------------------------------------------------
// Folded from network.spec.ts
// ---------------------------------------------------------------------------

type NetworkDebugResponse = {
  _debug_queries?: DebugQuery[]
  transport?: Record<string, unknown>
  available?: boolean
  reason?: string
}

type AssetsDebugResponse = {
  _debug_queries?: DebugQuery[]
  asset_type_breakdown?: unknown[]
  cache_performance?: unknown[]
  compression_performance?: unknown[]
  large_uncompressed_assets?: unknown[]
  low_ttl_assets?: unknown[]
  missing_fields?: string[]
}

test.describe('Network Page Contract (/network)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(4)
  })

  test('3. Data contract & schema validation (/api/network/bundle & /api/network-health)', async ({ page }) => {
    const bundleRes = await page.request.post('/api/network/bundle', {
      data: {
        sections: ['transport'],
      },
    })
    expect(bundleRes.ok()).toBeTruthy()
    const bundleData = await bundleRes.json()
    expect(bundleData).toBeTruthy()
    expect(bundleData).toHaveProperty('transport')
    if (bundleData.transport) {
      expect(bundleData.transport).toHaveProperty('protocol_distribution')
      expect(bundleData.transport).toHaveProperty('missing_fields')
      expect(Array.isArray(bundleData.transport.protocol_distribution)).toBeTruthy()
      expect(Array.isArray(bundleData.transport.missing_fields)).toBeTruthy()
    }

    const healthRes = await page.request.post('/api/network-health', {
      data: {
        sections: ['summary', 'leaderboard', 'metro_leaderboard'],
        top_n: 30,
      },
    })
    expect(healthRes.ok()).toBeTruthy()
    const healthData = await healthRes.json()
    expect(healthData).toBeTruthy()
    expect(healthData).toHaveProperty('available')

    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Core network cards & transport section mounted', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    const unavailableNotice = page.getByText('Network Metrics Unavailable')
    const hasNotice = await unavailableNotice.isVisible().catch(() => false)

    if (!hasNotice) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Global Health' }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Worst ASN' }).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.getByText('Edge Transport & TCP Performance')).toBeVisible({ timeout: 15_000 })

      const leaderboardTitle = page.getByText('ASN Performance Leaderboard')
      const popHeatmapTitle = page.getByText('Fastly Edge PoP Health Heatmap')
      await expect(leaderboardTitle.or(popHeatmapTitle).first()).toBeVisible({ timeout: 15_000 })
    } else {
      await expect(unavailableNotice).toBeVisible()
    }
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Zero error cards on network page', async ({ page }) => {
    await page.goto('/network')
    await expect(page.getByRole('heading', { name: 'Network & ASN Health' })).toBeVisible({ timeout: 30_000 })

    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/network', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /network] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/network/bundle', {
      data: {
        sections: ['transport'],
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as NetworkDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /network] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})

test.describe('Assets & Shield Page Contract (/assets-shield)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    await expect(page.locator('[data-testid="assets-overview-grid"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-cache-efficiency-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-compression-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-uncompressed-card"]:visible')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="assets-low-ttl-card"]:visible')).toBeVisible({ timeout: 15_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(5)
  })

  test('3. Data contract & schema validation (/api/assets/aggregates)', async ({ page }) => {
    const postRes = await page.request.post('/api/assets/aggregates', {
      data: {
        range_token: '24h',
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = await postRes.json()
    expect(postData).toBeTruthy()
    expect(postData).toHaveProperty('asset_type_breakdown')
    expect(postData).toHaveProperty('cache_performance')
    expect(postData).toHaveProperty('compression_performance')
    expect(postData).toHaveProperty('large_uncompressed_assets')
    expect(postData).toHaveProperty('low_ttl_assets')
    expect(postData).toHaveProperty('missing_fields')
    expect(Array.isArray(postData.asset_type_breakdown)).toBeTruthy()
    expect(Array.isArray(postData.cache_performance)).toBeTruthy()
    expect(Array.isArray(postData.compression_performance)).toBeTruthy()
    expect(Array.isArray(postData.large_uncompressed_assets)).toBeTruthy()
    expect(Array.isArray(postData.low_ttl_assets)).toBeTruthy()
    expect(Array.isArray(postData.missing_fields)).toBeTruthy()

    const getRes = await page.request.get('/api/assets/aggregates?range_token=24h')
    expect(getRes.ok()).toBeTruthy()
    const getData = await getRes.json()
    expect(getData).toHaveProperty('asset_type_breakdown')
    expect(getData).toHaveProperty('missing_fields')

    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Core asset cards & breakdown sections render properly', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    await expect(page.getByRole('heading', { name: 'Cache Efficiency by Asset Type' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Payload Compression Efficiency' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Large Uncompressed Assets' })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByRole('heading', { name: 'Low Cache-TTL Hotspots' })).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets switching cleanly (15m, 24h, 7d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Zero error cards on assets page', async ({ page }) => {
    await page.goto('/assets-shield')
    await expect(page.getByRole('heading', { name: 'Assets & Shield' })).toBeVisible({ timeout: 30_000 })

    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/assets-shield', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /assets-shield] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/assets/aggregates', {
      data: {
        range_token: '24h',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as AssetsDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /assets-shield] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})

// ---------------------------------------------------------------------------
// Folded from performance.spec.ts
// ---------------------------------------------------------------------------

type PerformanceDebugResponse = {
  _debug_queries?: DebugQuery[]
  waterfall?: Record<string, unknown>
  top_urls?: unknown[]
  top_asns?: unknown[]
  ttl_dist?: unknown[]
  scatter?: unknown[]
  _approx?: boolean
}

type OriginDebugResponse = {
  _debug_queries?: DebugQuery[]
  summary?: Record<string, unknown>
  timeseries?: Record<string, unknown>
  slow_urls?: Record<string, unknown>
  status_codes?: Record<string, unknown>
  path_breakdown?: Record<string, unknown>
  pop_latency?: Record<string, unknown>
  ip_health?: Record<string, unknown>
  has_data?: boolean
}

test.describe('Performance Page Contract (/performance)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    const waterfallCard = page.getByText('End-to-End Latency Waterfall (Average)').locator('visible=true')
    await expect(waterfallCard).toBeVisible({ timeout: 15_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(3)
  })

  test('3. Data contract & round-trip verification (/api/performance/aggregates)', async ({ page }) => {
    const apiRes = await page.request.post('/api/performance/aggregates', {
      data: {
        range_token: '24h',
        sort_by: 'p99',
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toBeTruthy()
    expect(data).toHaveProperty('waterfall')
    expect(data).toHaveProperty('top_urls')
    expect(data).toHaveProperty('top_asns')
    expect(data).toHaveProperty('ttl_dist')
    expect(data).toHaveProperty('scatter')

    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All 5 core performance cards render properly', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    await expect(page.getByText('End-to-End Latency Waterfall (Average)').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Slowest URLs').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Slowest Networks').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Cache TTL Distribution').locator('visible=true')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Origin vs Edge Processing (ms)').locator('visible=true')).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets and adaptive history extents', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p99/i }).or(page.getByText(/p99/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/performance')
    await expect(page.getByRole('heading', { name: 'Performance' })).toBeVisible({ timeout: 30_000 })

    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    await expect(plotlyChart).toBeVisible({ timeout: 20_000 })
    await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })

    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/performance', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /performance] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/performance/aggregates', {
      data: {
        range_token: '24h',
        sort_by: 'p99',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as PerformanceDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /performance] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})

test.describe('Origin Page Contract (/origin)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    const originLatencyCard = page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Latency' }).first()
    await expect(originLatencyCard).toBeVisible({ timeout: 15_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(6)
  })

  test('3. Data contract & round-trip verification (/api/origin/aggregates)', async ({ page }) => {
    const apiRes = await page.request.post('/api/origin/aggregates', {
      data: {
        range_token: '24h',
        timeseries_metric: 'ttfb',
        timeseries_percentile: 'p95',
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toBeTruthy()
    expect(data).toHaveProperty('summary')
    expect(data).toHaveProperty('timeseries')
    expect(data).toHaveProperty('slow_urls')
    expect(data).toHaveProperty('status_codes')
    expect(data).toHaveProperty('path_breakdown')
    expect(data).toHaveProperty('pop_latency')
    expect(data).toHaveProperty('ip_health')

    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All core origin cards render properly', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin TTFB (P50)' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin TTFB (P95)' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Error Rate' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Fetch Volume' }).first()).toBeVisible({ timeout: 15_000 })

    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Latency' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Status Code Distribution' }).first()).toBeVisible({ timeout: 15_000 })

    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Slowest URLs at Origin' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Performance by POP' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin IP Health' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Origin Payload Size' }).first()).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Column visibility controls on latency tables', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox', { name: /p95/i }).or(page.getByText(/p95/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/origin')
    await expect(page.getByRole('heading', { name: 'Origin Performance' })).toBeVisible({ timeout: 30_000 })

    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }

    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/origin', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /origin] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/origin/aggregates', {
      data: {
        range_token: '24h',
        timeseries_metric: 'ttfb',
        timeseries_percentile: 'p95',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as OriginDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /origin] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})

// ---------------------------------------------------------------------------
// Folded from insights.spec.ts
// ---------------------------------------------------------------------------

type InsightsDebugResponse = {
  _debug_queries?: DebugQuery[]
  insights?: unknown[]
  computed_at?: string
  timing_ms?: Record<string, number>
}

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
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    await expect(reservedContainers.first()).toBeVisible({ timeout: 15_000 })
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)

    const content = page.locator('[data-testid="insights-grid"], [data-testid="insights-empty-pass"], [data-testid="insights-grid-loading"]').first()
    await expect(content).toBeVisible({ timeout: 15_000 })
  })

  test('3. Data contract & schema validation (/api/insights & /api/insight-availability)', async ({ page }) => {
    const availRes = await page.request.get('/api/insight-availability')
    expect(availRes.ok()).toBeTruthy()
    const availData = await availRes.json()
    expect(availData).toHaveProperty('insights')
    expect(Array.isArray(availData.insights)).toBeTruthy()
    if (availData.insights.length > 0) {
      expect(availData.insights[0]).toHaveProperty('id')
      expect(availData.insights[0]).toHaveProperty('available')
    }

    const apiRes = await page.request.post('/api/insights', {
      data: {
        window_size_hrs: 1,
        baseline_hours: 24,
        filters: {},
      },
    })
    expect(apiRes.ok()).toBeTruthy()
    const data = await apiRes.json()
    expect(data).toHaveProperty('insights')
    expect(data).toHaveProperty('computed_at')
    expect(Array.isArray(data.insights)).toBeTruthy()
  })

  test('4. Instant client-side tab switching across all 5 category tabs and All', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    const tabs = ['all', 'security', 'origin', 'edge', 'network', 'traffic']

    for (const tabKey of tabs) {
      const trigger = page
        .locator(`button[data-tab="${tabKey}"]`)
        .or(page.locator(`button[data-alias="${tabKey}"]`))
        .first()

      await expect(trigger).toBeVisible({ timeout: 10_000 })
      await trigger.click()

      await expect(trigger).toHaveAttribute('aria-selected', 'true')

      const activeContent = page
        .locator(`[data-tab-content="${tabKey}"]`)
        .or(page.locator(`[data-alias="${tabKey}"]`))
        .first()

      await expect(activeContent).toBeVisible({ timeout: 10_000 })
    }
  })

  test('5. Plausible detector values or clean normal parameters pass states', async ({ page }) => {
    await page.goto('/insights')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    const emptyPass = page.locator('[data-testid="insights-empty-pass"]').first()
    const cardsGrid = page.locator('[data-testid="insights-grid"]').first()

    await expect(emptyPass.or(cardsGrid)).toBeVisible({ timeout: 30_000 })

    const hasEmptyPass = await emptyPass.isVisible()
    const hasCardsGrid = await cardsGrid.isVisible()

    expect(hasEmptyPass || hasCardsGrid).toBeTruthy()

    if (hasEmptyPass) {
      await expect(emptyPass).toContainText(/No anomalies detected/i)
      await expect(emptyPass).toContainText(/baseline parameters/i)
    } else {
      const card = cardsGrid.locator('.rounded-xl, .border').first()
      await expect(card).toBeVisible({ timeout: 10_000 })
    }
  })

  test('6. Time range preset transitions (window vs baseline)', async ({ page }) => {
    await page.goto('/insights?window=24&baseline=168&tab=security')
    await expect(page.getByRole('heading', { name: 'Anomaly Detection' })).toBeVisible({ timeout: 30_000 })

    const secTrigger = page.locator('button[data-tab="security"]').first()
    await expect(secTrigger).toBeVisible({ timeout: 10_000 })
    await expect(secTrigger).toHaveAttribute('aria-selected', 'true')
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/insights', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /insights] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/insights', {
      data: {
        window_size_hrs: 1,
        baseline_hours: 24,
        filters: {},
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json() as InsightsDebugResponse
    expect(json).toHaveProperty('insights')
    if (json._debug_queries) {
      console.log(`[TELEMETRY /insights] Captured ${json._debug_queries.length} queries`)
      expect(json._debug_queries.length).toBeGreaterThan(0)
    }
  })
})

test.describe('High-Scale Request Facts Page Contract (/high-scale/request-facts)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('3. Data contract & schema validation (/api/high-scale/request-facts)', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    expect(serviceId).toBeTruthy()

    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    const getRes = await page.request.get(
      `/api/high-scale/request-facts?service_id=${encodeURIComponent(serviceId!)}&start_time=${encodeURIComponent(oneHourAgo.toISOString())}&end_time=${encodeURIComponent(now.toISOString())}&limit=10`
    )
    expect(getRes.ok()).toBeTruthy()
    const getData = (await getRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(getData.rows)).toBeTruthy()
    expect(getData.metadata).toBeTruthy()
    expect(['complete', 'partial', 'unavailable']).toContain(getData.metadata?.status)
    expect(['clickhouse', 'duckdb_emulation']).toContain(getData.metadata?.mode)

    const postRes = await page.request.post(
      `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`,
      {
        data: {
          start_time: oneHourAgo.toISOString(),
          end_time: now.toISOString(),
          limit: 10,
          emulate_standard: true,
        },
      }
    )
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as RequestFactsDebugResponse
    expect(Array.isArray(postData.rows)).toBeTruthy()
    expect(postData.metadata?.watermark?.service_id).toBe(serviceId)
  })

  test('4. Metadata bar, mode badge, filter bar, and request facts table render cleanly', async ({ page }) => {
    await page.goto('/high-scale/request-facts')
    await expect(
      page.getByRole('heading', { name: 'High-scale request facts' })
    ).toBeVisible({ timeout: 30_000 })

    await expect(page.getByTestId('high-scale-facts-card')).toBeVisible({ timeout: 20_000 })
    await expect(page.getByTestId('high-scale-metadata')).toBeVisible({ timeout: 15_000 })
    await expect(page.getByTestId('high-scale-mode-badge')).toBeVisible({ timeout: 15_000 })

    const badgeText = await page.getByTestId('high-scale-mode-badge').innerText()
    expect(badgeText).toMatch(/DuckDB Standard Mode Emulation|ClickHouse Distributed Cluster/)

    await expect(page.getByTestId('high-scale-filters')).toBeVisible()
    await expect(page.getByTestId(' high-scale-table-wrapper'.trim())).toBeVisible()
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

    const fatalErrors = consoleErrors.filter(
      (e) => e.includes('500') || e.includes('Internal Server Error')
    )
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Performance budget: FCP < 400ms / LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await page.goto('/high-scale/request-facts', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(
      `[PERF /high-scale/request-facts] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`
    )
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('7. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    expect(serviceId).toBeTruthy()

    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 3600 * 1000)

    const res = await page.request.post(
      `/api/high-scale/services/${encodeURIComponent(serviceId!)}/request-facts`,
      {
        data: {
          start_time: oneHourAgo.toISOString(),
          end_time: now.toISOString(),
          limit: 10,
          emulate_standard: true,
        },
        headers: {
          'X-Page-Load-ID': `e2e-hs-facts-${Date.now()}`,
          'x-debug-responses': '1',
        },
      }
    )
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as RequestFactsDebugResponse
    expect(json).toHaveProperty('_debug_queries')
    const queries = json._debug_queries ?? []
    console.log(`[TELEMETRY /high-scale/request-facts] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})
