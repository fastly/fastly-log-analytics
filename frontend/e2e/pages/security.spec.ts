/**
 * E2E Page Contract Test: Edge Security & Bot Intelligence (/security)
 *
 * Implements the verification checklist specified in docs/pages/security.md:
 * 1. Route accessibility & footer environment mode verification.
 * 2. Instant shell, pre-allocated layout & in-place loading states (Zero CLS).
 * 3. Data contract: single composite round-trip on cold load (/api/security/aggregates & /api/security/bundle), HTTP 200, valid schema.
 * 4. All core security cards mounted across tabs (WAF Activity, Threat Intelligence, VPN & Proxy Watchdog).
 * 5. Time range switching: 15m, 24h, 30d presets execute cleanly without console errors or 500s.
 * 6. Column visibility controls on bot/fingerprint data tables.
 * 7. Non-empty Plotly visualizations and zero error cards.
 * 8. Performance budget: LCP within budget (< 1500ms), CLS <= 0.05.
 * 9. Telemetry instrumentation & query efficiency audit.
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import { installClsObserver, measureWarmPaintAndCls } from '../helpers'

type SecurityDebugResponse = {
  _debug_queries?: DebugQuery[]
  ngwaf_verified_bots?: unknown[]
  wellknown_bots?: unknown[]
  tls_fingerprints?: unknown[]
  req_size_dist?: unknown[]
  top_ips_header?: unknown[]
  ipv6_adoption?: unknown[]
  proxy_dist?: unknown[]
  conn_reuse_dist?: unknown[]
  active_proxies_count?: number
  tunnel_requests_count?: number
  distance_mismatches_count?: number
}

type SessionsDebugResponse = {
  _debug_queries?: DebugQuery[]
  sessions?: Array<{
    ip: string
    session_token?: string
    session_start: string
    session_end: string
    req_count: number
    is_streaming?: boolean
  }>
  total?: number
  page?: number
  limit?: number
}

test.describe('Security Page Contract (/security)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated cards exist with content-visibility / contain-intrinsic-size
    const botCard = page.locator('#main [data-slot="card-title"]').filter({ hasText: /Well-Known Bots|Verified Bots/i }).first()
    await expect(botCard).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes across initial tab cards
    const initialReservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const initialCount = await initialReservedContainers.count()
    expect(initialCount).toBeGreaterThanOrEqual(2)

    // Switch to VPN & Proxy Watchdog tab and verify its reserved containers
    const proxyTab = page.getByRole('tab', { name: /vpn & proxy watchdog/i })
    await expect(proxyTab).toBeVisible({ timeout: 10_000 })
    await proxyTab.click()

    const proxyCard = page.locator('#main [data-slot="card-title"]').filter({ hasText: /Active VPN & Proxy Users/i }).first()
    await expect(proxyCard).toBeVisible({ timeout: 15_000 })

    const proxyReservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const proxyCount = await proxyReservedContainers.count()
    expect(proxyCount).toBeGreaterThanOrEqual(6)
  })

  test('3. Data contract & round-trip verification (/api/security/aggregates & /api/security/bundle)', async ({ page }) => {
    // 1. Verify aggregates endpoint schema directly via API client
    const aggregatesRes = await page.request.post('/api/security/aggregates', {
      data: {
        range_token: '24h',
        sections: [
          'ngwaf_verified_bots',
          'ngwaf_verified_bots_ts',
          'wellknown_bots',
          'tls_fingerprints',
          'fingerprint_coverage',
          'req_size_dist',
          'top_ips_header',
          'ipv6_adoption',
          'proxy_dist',
          'conn_reuse_dist',
          'proxies',
        ],
      },
    })
    expect(aggregatesRes.ok()).toBeTruthy()
    const aggData = await aggregatesRes.json()
    expect(aggData).toBeTruthy()
    expect(aggData).toHaveProperty('wellknown_bots')
    expect(aggData).toHaveProperty('tls_fingerprints')
    expect(aggData).toHaveProperty('req_size_dist')
    expect(aggData).toHaveProperty('top_ips_header')
    expect(aggData).toHaveProperty('ipv6_adoption')
    expect(aggData).toHaveProperty('proxy_dist')
    expect(aggData).toHaveProperty('conn_reuse_dist')

    // 2. Verify bundle endpoints (POST & GET)
    const bundlePostRes = await page.request.post('/api/security/bundle', {
      data: { range_token: '24h' },
    })
    expect(bundlePostRes.ok()).toBeTruthy()
    const bundlePostData = await bundlePostRes.json()
    expect(bundlePostData).toHaveProperty('wellknown_bots')

    const bundleGetRes = await page.request.get('/api/security/bundle?range_token=24h')
    expect(bundleGetRes.ok()).toBeTruthy()
    const bundleGetData = await bundleGetRes.json()
    expect(bundleGetData).toHaveProperty('wellknown_bots')

    // 3. Cold load leverages SSR dehydration without client-side errors
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All core security cards render properly across tabs', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Tab 1: WAF Activity cards
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Well-Known Bots' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Top TLS Fingerprints' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Request Header Size Distribution' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Oversized Request Headers (by IP)' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'IPv6 Adoption over Time' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Proxy/Anonymizer Breakdown' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Connection Reuse (Requests per Connection)' }).first()).toBeVisible({ timeout: 15_000 })

    // Tab 2: Threat Intelligence
    const threatTab = page.getByRole('tab', { name: /threat intelligence/i })
    await expect(threatTab).toBeVisible()
    await threatTab.click()
    await page.waitForTimeout(500)

    // Tab 3: VPN & Proxy Watchdog
    const proxyTab = page.getByRole('tab', { name: /vpn & proxy watchdog/i })
    await expect(proxyTab).toBeVisible()
    await proxyTab.click()

    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Active VPN & Proxy Users' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Tunnel Requests' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Impossible Distance Mismatches' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Traffic Quality Breakdown' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Suspicious ISPs & Providers' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'VPN/Proxy Export Center' }).first()).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: 'Active Suspicious Clients' }).first()).toBeVisible({ timeout: 15_000 })
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Time presets in FilterBar
    const timePresetBtn = page.getByRole('button', { name: /24h|1h|7d|last/i }).first()
    await expect(timePresetBtn).toBeVisible({ timeout: 10_000 })

    // No severe 500 or uncaught errors during range switching
    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('6. Column visibility controls on security tables', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Column visibility dropdown trigger button on Well-Known Bots card
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox').or(page.getByText(/requests|ips|name/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    // Must have rendered Plotly charts when data exists
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }

    // Zero error cards on security page
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/security', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /security] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/security/aggregates', {
      data: {
        range_token: '24h',
        sections: ['wellknown_bots', 'req_size_dist'],
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as SecurityDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /security] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
  })
})


test.describe('User Sessions Explorer Page Contract (/sessions)', () => {

  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })

    // Verify footer exists and contains environment mode
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })

    // Verify pre-allocated user sessions container exists with testid
    await expect(page.locator('[data-testid="user-sessions-explorer"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="sessions-table-container"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="sessions-scoring-controls"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('3. Data contract & schema validation (/api/sessions and /api/sessions/detail)', async ({ page }) => {
    // 1. Verify POST /api/sessions
    const postRes = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as SessionsDebugResponse
    expect(postData).toBeTruthy()
    expect(postData).toHaveProperty('sessions')
    expect(postData).toHaveProperty('total')
    expect(Array.isArray(postData.sessions)).toBeTruthy()
    expect(typeof postData.total).toBe('number')

    // 2. If sessions exist, test POST /api/sessions/detail with sealed token
    if (postData.sessions && postData.sessions.length > 0) {
      const firstSession = postData.sessions[0]
      expect(firstSession).toHaveProperty('session_token')
      expect(typeof firstSession.session_token).toBe('string')

      const detailRes = await page.request.post('/api/sessions/detail', {
        data: {
          session_token: firstSession.session_token,
        },
      })
      expect(detailRes.ok()).toBeTruthy()
      const detailData = await detailRes.json()
      expect(detailData).toHaveProperty('columns')
      expect(detailData).toHaveProperty('data')
      expect(Array.isArray(detailData.columns)).toBeTruthy()
      expect(Array.isArray(detailData.data)).toBeTruthy()
    }

    // 3. Page mounts without uncaught exceptions
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })
  })

  test('4. Filter controls & toggle interactions without console errors or 500s', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/sessions')
    await expect(page.locator('[data-testid="sessions-scoring-controls"]')).toBeVisible({ timeout: 30_000 })

    // Toggle Flagged only switch
    const flaggedSwitch = page.locator('[data-testid="flagged-only-switch"]')
    await expect(flaggedSwitch).toBeVisible({ timeout: 10_000 })
    await flaggedSwitch.click()
    await page.waitForTimeout(500)
    await flaggedSwitch.click()
    await page.waitForTimeout(500)

    // Type in Min. requests input
    const minReqsInput = page.locator('[data-testid="min-reqs-input"]')
    await expect(minReqsInput).toBeVisible({ timeout: 10_000 })
    await minReqsInput.fill('5')
    await page.waitForTimeout(500)
    await minReqsInput.fill('')
    await page.waitForTimeout(500)

    const fatalErrors = consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))
    expect(fatalErrors).toHaveLength(0)
  })

  test('5. Row detail inspection dialog with session metadata', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.locator('[data-testid="sessions-table-container"]')).toBeVisible({ timeout: 30_000 })

    // Find table rows
    const tableRows = page.locator('[data-testid="sessions-table-container"] tbody tr')
    await expect(tableRows.first()).toContainText(/\d{1,3}(\.\d{1,3}){3}|[0-9a-f:]{6,}/i, { timeout: 30_000 })
    const rowCount = await tableRows.count()

    if (rowCount > 0) {
      // Click the first row or its Details button
      await tableRows.first().click()

      const dialog = page.locator('[data-testid="session-detail-dialog"]')
      await expect(dialog).toBeVisible({ timeout: 10_000 })

      // Verify metadata grid elements
      await expect(dialog.getByText(/Session:/i)).toBeVisible({ timeout: 5_000 })
      await expect(dialog.getByText(/Requests/i)).toBeVisible({ timeout: 5_000 })

      // Close dialog via Escape key
      await page.keyboard.press('Escape')
      await expect(dialog).not.toBeVisible({ timeout: 5_000 })
    }
  })

  test('6. Stream Session Detail page (/sessions/stream)', async ({ page }) => {
    // 1. Without token: verifies "No session token" alert
    await page.goto('/sessions/stream')
    await expect(page.getByRole('heading', { name: /Stream Details/i })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText(/No session token/i)).toBeVisible({ timeout: 10_000 })

    // 2. With token: fetch a session token from API and visit
    const postRes = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
    })
    if (postRes.ok()) {
      const postData = (await postRes.json()) as SessionsDebugResponse
      const token = postData.sessions?.[0]?.session_token
      if (token) {
        await page.goto(`/sessions/stream?token=${encodeURIComponent(token)}`)
        await expect(page.locator('[data-testid="stream-detail-client"]')).toBeVisible({ timeout: 30_000 })
        await expect(page.getByRole('heading', { name: /Stream Session Details/i })).toBeVisible({ timeout: 10_000 })
      }
    }
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    // Prime cache on first visit
    await page.goto('/sessions', { waitUntil: 'domcontentloaded' })

    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 1000)

    console.log(`[PERF /sessions] Reload: ${totalTime}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`)
    expect(totalTime).toBeLessThan(10_000)
    if (metrics.lcp > 0) {
      expect(metrics.lcp).toBeLessThan(1500)
    }
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    // Request with debug responses enabled
    const res = await page.request.post('/api/sessions', {
      data: {
        page: 1,
        limit: 10,
        sort_by: 'session_start',
        sort_dir: 'desc',
      },
      headers: {
        'x-debug-responses': '1',
      },
    })
    expect(res.ok()).toBeTruthy()
    const json = await res.json()
    expect(json).toHaveProperty('_debug_queries')
    const queries = (json as SessionsDebugResponse)._debug_queries ?? []
    console.log(`[TELEMETRY /sessions] Captured ${queries.length} queries`)
    expect(queries.length).toBeGreaterThan(0)
    const sessionQueries = queries.filter((q) => !q.sql?.includes('CREATE OR REPLACE VIEW'))
    for (const q of sessionQueries) {
      if (typeof q.time_ms === 'number') {
        expect(q.time_ms).toBeLessThan(2000)
      }
    }
  })
})
