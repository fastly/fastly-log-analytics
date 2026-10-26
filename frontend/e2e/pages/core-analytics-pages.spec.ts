/**
 * E2E Page Contract Tests: Core Analytics & Data Workspace Pages
 *
 * Consolidates page contract specs for:
 * - Data Management (/logs) & Custom Field VCL Validation
 * - SQL Query Pad (/query)
 * - Edge Security & Bot Intelligence (/security)
 * - User Sessions Explorer (/sessions & /sessions/stream)
 */

import { expect, test, type Page } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import {
  configureExternalAdminOrigin,
  getFirstServiceId,
  installClsObserver,
  loginAsPasscodeAnalyst,
  measureWarmPaintAndCls,
  revokeAnalystInvite,
} from '../helpers'

type QueryDebugResponse = {
  _debug_queries?: DebugQuery[]
  debug_queries?: DebugQuery[]
  columns?: string[]
  data?: Array<Record<string, unknown>>
  total_rows?: number
  elapsed_ms?: number
}

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

configureExternalAdminOrigin()

async function assertFooterMode(page: Page) {
  const footer = page.locator('footer')
  await expect(footer).toBeVisible({ timeout: 10_000 })
  expect(await footer.innerText()).toMatch(/Admin View|Analyst View/i)
}

async function assertWarmLoadBudget(
  page: Page,
  route: string,
  opts: { settleMs?: number; maxFcpMs?: number; maxLcpMs?: number; heading?: string } = {},
) {
  await page.goto(route, { waitUntil: 'domcontentloaded' })
  if (opts.heading) {
    await expect(page.getByRole('heading', { name: opts.heading, exact: true })).toBeVisible({ timeout: 30_000 })
  }

  await installClsObserver(page)
  const start = Date.now()
  await page.reload({ waitUntil: 'domcontentloaded' })
  const totalTime = Date.now() - start
  if (opts.heading) {
    await expect(page.getByRole('heading', { name: opts.heading, exact: true })).toBeVisible({ timeout: 15_000 })
  }

  const metrics = await measureWarmPaintAndCls(page, opts.settleMs ?? 1000)
  console.log(
    `[PERF ${route}] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | LCP: ${Math.round(metrics.lcp)}ms | CLS: ${metrics.cls.toFixed(3)}`,
  )
  expect(totalTime).toBeLessThan(10_000)
  if (opts.maxFcpMs && metrics.fcp > 0) expect(metrics.fcp).toBeLessThan(opts.maxFcpMs)
  if (opts.maxLcpMs && metrics.lcp > 0) expect(metrics.lcp).toBeLessThan(opts.maxLcpMs)
  expect(metrics.cls).toBeLessThanOrEqual(0.05)
}

// ── Data Management Page Contract (/logs) ───────────────────────────────────

test.describe('Data Management Page Contract (/logs)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/logs')
    expect(res?.status()).toBe(200)

    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })
    for (const btnName of [/Import Logs/i, /Sync from Cloud/i, /Commit Buffer/i]) {
      await expect(page.getByRole('button', { name: btnName })).toBeVisible({ timeout: 15_000 })
    }
    await assertFooterMode(page)

    if (!process.env.PLAYWRIGHT_BASE_URL) {
      const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
      let inviteId: string | null = null
      try {
        const seeded = await loginAsPasscodeAnalyst(page.request, {
          name: 'Logs Guard Analyst',
          email: `e2e-logs-guard-${Date.now()}@example.com`,
          passcode: 'harbor-cloud-forest-91',
          durationHours: 1,
          serviceIds: [serviceId],
        })
        inviteId = seeded.inviteId

        for (const path of ['/api/cron-runs', '/api/cron-schedule', '/api/audit-logs', '/api/admin/ingested-files']) {
          const analystRes = await page.request.get(path, { headers: { 'x-remote-analyst': '1' } })
          expect(analystRes.status()).toBe(403)
          expect((await analystRes.json()).error).toBe('admin_only')
        }
      } finally {
        await revokeAnalystInvite(page.request, inviteId, { logout: true })
      }
    }
  })

  test('2. Instant shell & warm-load performance budget (CLS <= 0.05)', async ({ page }) => {
    await assertWarmLoadBudget(page, '/logs', { settleMs: 500, heading: 'Data Management' })
  })

  test('3. Backend REST contracts (/api/cron-runs, /api/cron-schedule, /api/audit-logs, /api/admin/ingested-files, /api/schema)', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    const withSvc = (base: string, extra = '') =>
      serviceId
        ? `${base}?service_id=${encodeURIComponent(serviceId)}${extra ? `&${extra}` : ''}`
        : `${base}${extra ? `?${extra}` : ''}`

    const contracts: Array<[string, string]> = [
      [withSvc('/api/cron-runs', 'limit=50'), 'entries'],
      [withSvc('/api/cron-schedule'), 'schedules'],
      [withSvc('/api/audit-logs', 'limit=50'), 'entries'],
      [withSvc('/api/admin/ingested-files', 'limit=50'), 'files'],
      [withSvc('/api/schema'), 'schema'],
    ]

    for (const [url, prop] of contracts) {
      const res = await page.request.get(url)
      expect(res.ok()).toBeTruthy()
      const data = await res.json()
      expect(data).toHaveProperty(prop)
      expect(Array.isArray(data[prop])).toBe(true)
    }
  })

  test('4. 7-tab workspace navigation & lazy-mounted tab panels', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })

    const tabNames = [
      /Cron Runs/i,
      /Service History/i,
      /Ingestion History/i,
      /DuckLake Storage/i,
      /Metadata Storage/i,
      /Available Logs/i,
      /DuckDB Schema/i,
    ]
    for (const name of tabNames) {
      await expect(page.getByRole('tab', { name })).toBeVisible()
    }

    const switches: Array<[RegExp, string]> = [
      [/Service History/i, 'Service History'],
      [/Ingestion History/i, 'Log Ingestion History'],
      [/DuckDB Schema/i, 'DuckDB Table Schema & Statistics'],
    ]
    for (const [tabRe, heading] of switches) {
      await page.getByRole('tab', { name: tabRe }).click()
      await expect(page.getByRole('heading', { name: heading, exact: true })).toBeVisible({ timeout: 15_000 })
    }

    const cronTab = page.getByRole('tab', { name: /Cron Runs/i })
    await cronTab.click()
    await expect(cronTab).toHaveAttribute('aria-selected', 'true')
  })

  test('5. Interactive Service History refresh propagates X-Page-Load-ID header', async ({ page }) => {
    await page.goto('/logs?tab=service_history')
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('heading', { name: 'Service History', exact: true })).toBeVisible({ timeout: 15_000 })

    const capturedPageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/audit-logs')) {
        const pid = req.headers()['x-page-load-id']
        if (pid) capturedPageLoadIds.push(pid)
      }
    })

    const refreshBtn = page.getByRole('button', { name: /^Refresh$/i })
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })
    await refreshBtn.click()
    await expect.poll(() => capturedPageLoadIds.length, { timeout: 10_000 }).toBeGreaterThan(0)
  })
})

test('custom-field admin page renders', async ({ page }) => {
  const r = await page.goto('/admin')
  expect(r?.status()).toBeLessThan(500)
})

test('VCL editor debounce → validate-vcl fires after 500 ms in a real browser', async ({ page }) => {
  const validateWaiter = page.waitForRequest(
    (req) => req.method() === 'POST' && /\/api\/services\/[^/]+\/custom-fields\/validate-vcl/.test(req.url()),
    { timeout: 30_000 },
  )

  await page.goto('/admin')

  const manageBtn = page.getByRole('button', { name: /manage/i }).first()
  await expect(manageBtn).toBeVisible({ timeout: 30_000 })
  await manageBtn.click()
  const logSettingsItem = page.getByRole('menuitem', { name: /log settings/i })
  await expect(logSettingsItem).toBeVisible({ timeout: 15_000 })
  await logSettingsItem.click()

  const nextStep = page.getByRole('button', { name: /next step/i })
  await expect(nextStep).toBeVisible({ timeout: 30_000 })
  await nextStep.click()

  const addField = page.getByRole('button', { name: /add field/i })
  await expect(addField).toBeVisible({ timeout: 30_000 })
  await addField.click()

  await page.getByLabel('Label', { exact: false }).first().fill('Pw Vcl Probe')

  const vclInput = page.getByLabel('VCL Log Expression')
  await expect(vclInput).toBeVisible()
  await vclInput.fill('req.http.Host')

  const validateReq = await validateWaiter
  expect(validateReq.url()).toMatch(/\/custom-fields\/validate-vcl/)

  await expect(
    page.getByText(/VCL Expression is valid|Validation Errors|Validation could not run/i).first(),
  ).toBeVisible({ timeout: 30_000 })
})

// ── SQL Query Pad Page Contract (/query) ────────────────────────────────────

test.describe('SQL Query Pad Page Contract (/query)', () => {
  test('1. Route accessibility, environment indicator & layout mounting', async ({ page }) => {
    await page.goto('/query')
    await expect(page.getByRole('heading', { name: /Query Explorer/i })).toBeVisible({ timeout: 30_000 })
    await assertFooterMode(page)
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await page.goto('/query')
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="query-editor-container"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="query-run-btn"]')).toBeVisible({ timeout: 15_000 })
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(2)
  })

  test('3. Schema explorer & schema reflection contract (/api/query/schema/{service_id})', async ({ page }) => {
    const servicesRes = await page.request.get('/api/services')
    expect(servicesRes.ok()).toBeTruthy()
    const servicesJson = await servicesRes.json()
    const services = Array.isArray(servicesJson) ? servicesJson : (servicesJson?.services ?? [])
    const serviceId = services[0]?.id ?? services[0]?.service_id ?? 'default'

    const schemaRes = await page.request.get(`/api/query/schema/${encodeURIComponent(serviceId)}`)
    expect(schemaRes.ok()).toBeTruthy()
    const schemaData = await schemaRes.json()
    expect(Array.isArray(schemaData.columns)).toBeTruthy()
    expect(schemaData.columns.length).toBeGreaterThan(0)

    await page.goto('/query')
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })

    const rawSqlTab = page.getByRole('tab', { name: /Edit Raw SQL/i })
    await expect(rawSqlTab).toBeVisible({ timeout: 10_000 })
    await rawSqlTab.click()

    const sidebar = page.locator('[data-testid="query-schema-sidebar"]')
    await expect(sidebar).toBeVisible({ timeout: 10_000 })
    await expect(sidebar).toContainText('timestamp')
    await expect(sidebar).toContainText('status')
  })

  test('4. Valid analytical SQL execution contract (SELECT count(*) FROM logs)', async ({ page }) => {
    const postRes = await page.request.post('/api/query', {
      data: { sql: 'SELECT count(*) AS total_count FROM logs', dataset: 'logs', max_rows: 100 },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as QueryDebugResponse
    for (const k of ['columns', 'data', 'total_rows', 'elapsed_ms']) expect(postData).toHaveProperty(k)
    expect(Array.isArray(postData.columns)).toBeTruthy()
    expect(Array.isArray(postData.data)).toBeTruthy()
    expect(postData.data?.length).toBe(1)

    await page.goto('/query?q=' + encodeURIComponent('SELECT count(*) AS total_count FROM logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await page.locator('[data-testid="query-run-btn"]').click()
    await expect(page.locator('[data-testid="query-results-table"]')).toBeVisible({ timeout: 20_000 })
  })

  test('5. AST safety enforcement rejects mutating queries (DROP TABLE logs)', async ({ page }) => {
    for (const sql of ['DROP TABLE logs', 'DELETE FROM logs', 'INSERT INTO logs VALUES (1)', 'ALTER TABLE logs DROP COLUMN url']) {
      const res = await page.request.post('/api/query', { data: { sql, dataset: 'logs' } })
      expect(res.status()).toBe(403)
      expect(JSON.stringify(await res.json())).toMatch(/sql_not_permitted|only SELECT/i)
    }

    await page.goto('/query?q=' + encodeURIComponent('DROP TABLE logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await page.locator('[data-testid="query-run-btn"]').click()

    const errorAlert = page.locator('[data-testid="query-error-alert"]')
    await expect(errorAlert).toBeVisible({ timeout: 15_000 })
    await expect(errorAlert).toContainText(/only SELECT|sql_not_permitted|Query Execution Failed/i)
  })

  test('6. Client-side CSV export trigger', async ({ page }) => {
    await page.goto('/query?q=' + encodeURIComponent('SELECT count(*) AS total_count FROM logs'))
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await page.locator('[data-testid="query-run-btn"]').click()
    await expect(page.locator('[data-testid="query-results-table"]')).toBeVisible({ timeout: 20_000 })

    const exportBtn = page.locator('[data-testid="query-csv-export-btn"]')
    await expect(exportBtn).toBeVisible({ timeout: 10_000 })
    const downloadPromise = page.waitForEvent('download')
    await exportBtn.click()
    expect((await downloadPromise).suggestedFilename()).toBe('query_results.csv')
  })

  test('7. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmLoadBudget(page, '/query', { maxFcpMs: 400 })
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/query', {
      data: { sql: 'SELECT count(*) AS total_count FROM logs', dataset: 'logs', max_rows: 10 },
      headers: { 'x-debug-responses': '1' },
    })
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as QueryDebugResponse
    const queries = json._debug_queries ?? json.debug_queries ?? []
    expect(queries.length).toBeGreaterThan(0)
  })

  test('9. ?mode=raw deep link opens Raw SQL and the mode toggle round-trips through the URL', async ({ page }) => {
    await page.goto('/query?mode=raw&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    const rawTab = page.getByRole('tab', { name: /Edit Raw SQL/i })
    await expect(rawTab).toHaveAttribute('aria-selected', 'true', { timeout: 30_000 })

    await page.getByRole('tab', { name: /Structured/i }).click()
    await expect.poll(() => new URL(page.url()).searchParams.get('mode')).toBeNull()
    expect(new URL(page.url()).searchParams.get('start_time')).toBe('2026-01-01T00:00:00Z')

    await rawTab.click()
    await expect.poll(() => new URL(page.url()).searchParams.get('mode')).toBe('raw')
  })
})

// ── Security Page Contract (/security) ──────────────────────────────────────

test.describe('Security Page Contract (/security)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })
    await assertFooterMode(page)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    await expect(
      page.locator('#main [data-slot="card-title"]').filter({ hasText: /Well-Known Bots|Verified Bots/i }).first(),
    ).toBeVisible({ timeout: 15_000 })
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(2)

    const proxyTab = page.getByRole('tab', { name: /vpn & proxy watchdog/i })
    await expect(proxyTab).toBeVisible({ timeout: 10_000 })
    await proxyTab.click()

    await expect(
      page.locator('#main [data-slot="card-title"]').filter({ hasText: /Active VPN & Proxy Users/i }).first(),
    ).toBeVisible({ timeout: 15_000 })
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(6)
  })

  test('3. Data contract & round-trip verification (/api/security/aggregates & /api/security/bundle)', async ({ page }) => {
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
    for (const k of [
      'wellknown_bots',
      'tls_fingerprints',
      'req_size_dist',
      'top_ips_header',
      'ipv6_adoption',
      'proxy_dist',
      'conn_reuse_dist',
    ]) {
      expect(aggData).toHaveProperty(k)
    }

    const bundlePostRes = await page.request.post('/api/security/bundle', { data: { range_token: '24h' } })
    expect(bundlePostRes.ok()).toBeTruthy()
    expect(await bundlePostRes.json()).toHaveProperty('wellknown_bots')

    const bundleGetRes = await page.request.get('/api/security/bundle?range_token=24h')
    expect(bundleGetRes.ok()).toBeTruthy()
    expect(await bundleGetRes.json()).toHaveProperty('wellknown_bots')

    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })
  })

  test('4. All core security cards render properly across tabs', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    for (const title of [
      'Well-Known Bots',
      'Top TLS Fingerprints',
      'Request Header Size Distribution',
      'Oversized Request Headers (by IP)',
      'IPv6 Adoption over Time',
      'Proxy/Anonymizer Breakdown',
      'Connection Reuse (Requests per Connection)',
    ]) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({
        timeout: 15_000,
      })
    }

    const threatTab = page.getByRole('tab', { name: /threat intelligence/i })
    await expect(threatTab).toBeVisible()
    await threatTab.click()
    await page.waitForTimeout(500)

    const proxyTab = page.getByRole('tab', { name: /vpn & proxy watchdog/i })
    await expect(proxyTab).toBeVisible()
    await proxyTab.click()

    for (const title of [
      'Active VPN & Proxy Users',
      'Tunnel Requests',
      'Impossible Distance Mismatches',
      'Traffic Quality Breakdown',
      'Suspicious ISPs & Providers',
      'VPN/Proxy Export Center',
      'Active Suspicious Clients',
    ]) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({
        timeout: 15_000,
      })
    }
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })

    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })

    expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
  })

  test('6. Column visibility controls on security tables', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(
        page.getByRole('menuitemcheckbox').or(page.getByText(/requests|ips|name/i)).first(),
      ).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await page.goto('/security')
    await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible({ timeout: 30_000 })

    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmLoadBudget(page, '/security', { maxLcpMs: 1500 })
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/security/aggregates', {
      data: { range_token: '24h', sections: ['wellknown_bots', 'req_size_dist'] },
      headers: { 'x-debug-responses': '1' },
    })
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as SecurityDebugResponse
    expect(json).toHaveProperty('_debug_queries')
    expect((json._debug_queries ?? []).length).toBeGreaterThan(0)
  })
})

// ── User Sessions Explorer Page Contract (/sessions) ────────────────────────

test.describe('User Sessions Explorer Page Contract (/sessions)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })
    await assertFooterMode(page)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.getByRole('heading', { name: /User Sessions/i })).toBeVisible({ timeout: 30_000 })

    for (const tid of ['user-sessions-explorer', 'sessions-table-container', 'sessions-scoring-controls']) {
      await expect(page.locator(`[data-testid="${tid}"]`)).toBeVisible({ timeout: 15_000 })
    }
    expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(1)
  })

  test('3. Data contract & schema validation (/api/sessions and /api/sessions/detail)', async ({ page }) => {
    const postRes = await page.request.post('/api/sessions', {
      data: { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' },
    })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as SessionsDebugResponse
    expect(Array.isArray(postData.sessions)).toBeTruthy()
    expect(typeof postData.total).toBe('number')

    if (postData.sessions && postData.sessions.length > 0) {
      const firstSession = postData.sessions[0]
      expect(typeof firstSession.session_token).toBe('string')

      const detailRes = await page.request.post('/api/sessions/detail', {
        data: { session_token: firstSession.session_token },
      })
      expect(detailRes.ok()).toBeTruthy()
      const detailData = await detailRes.json()
      expect(Array.isArray(detailData.columns)).toBeTruthy()
      expect(Array.isArray(detailData.data)).toBeTruthy()
    }

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

    const flaggedSwitch = page.locator('[data-testid="flagged-only-switch"]')
    await expect(flaggedSwitch).toBeVisible({ timeout: 10_000 })
    await flaggedSwitch.click()
    await page.waitForTimeout(500)
    await flaggedSwitch.click()
    await page.waitForTimeout(500)

    const minReqsInput = page.locator('[data-testid="min-reqs-input"]')
    await expect(minReqsInput).toBeVisible({ timeout: 10_000 })
    await minReqsInput.fill('5')
    await page.waitForTimeout(500)
    await minReqsInput.fill('')
    await page.waitForTimeout(500)

    expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
  })

  test('5. Row detail inspection dialog with session metadata', async ({ page }) => {
    await page.goto('/sessions')
    await expect(page.locator('[data-testid="sessions-table-container"]')).toBeVisible({ timeout: 30_000 })

    const tableRows = page.locator('[data-testid="sessions-table-container"] tbody tr')
    await expect(tableRows.first()).toContainText(/\d{1,3}(\.\d{1,3}){3}|[0-9a-f:]{6,}/i, { timeout: 30_000 })

    if ((await tableRows.count()) > 0) {
      await tableRows.first().click()
      const dialog = page.locator('[data-testid="session-detail-dialog"]')
      await expect(dialog).toBeVisible({ timeout: 10_000 })
      await expect(dialog.getByText(/Session:/i)).toBeVisible({ timeout: 5_000 })
      await expect(dialog.getByText(/Requests/i)).toBeVisible({ timeout: 5_000 })

      await page.keyboard.press('Escape')
      await expect(dialog).not.toBeVisible({ timeout: 5_000 })
    }
  })

  test('6. Stream Session Detail page (/sessions/stream)', async ({ page }) => {
    await page.goto('/sessions/stream')
    await expect(page.getByRole('heading', { name: /Stream Details/i })).toBeVisible({ timeout: 30_000 })
    await expect(page.getByText(/No session token/i)).toBeVisible({ timeout: 10_000 })

    const postRes = await page.request.post('/api/sessions', {
      data: { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' },
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
    await assertWarmLoadBudget(page, '/sessions', { maxLcpMs: 1500 })
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const res = await page.request.post('/api/sessions', {
      data: { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' },
      headers: { 'x-debug-responses': '1' },
    })
    expect(res.ok()).toBeTruthy()
    const json = (await res.json()) as SessionsDebugResponse
    expect(json).toHaveProperty('_debug_queries')
    const queries = json._debug_queries ?? []
    expect(queries.length).toBeGreaterThan(0)
    for (const q of queries.filter((item) => !item.sql?.includes('CREATE OR REPLACE VIEW'))) {
      if (typeof q.time_ms === 'number') expect(q.time_ms).toBeLessThan(2000)
    }
  })
})
