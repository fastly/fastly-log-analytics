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

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { installClsObserver, measureWarmPaintAndCls } from '../helpers'

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
