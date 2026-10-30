/**
 * E2E Page Contract Tests: Core Analytics & Data Workspace Pages
 * (- /logs, /query, /security, /sessions, /sessions/stream)
 */

import { expect, test } from '@playwright/test'
import type { DebugQuery } from '../../types/api'
import {
  assertAnalyst403,
  assertFooterVisible,
  assertIntrinsicCount,
  assertPostTelemetry,
  assertRouteAndFooter,
  assertTimePresetsNo500,
  assertWarmPerfBudget,
  configureExternalAdminOrigin,
  getFirstServiceId,
  gotoHeading,
} from '../helpers'

type SessionsDebugResponse = {
  _debug_queries?: DebugQuery[]
  sessions?: Array<{ ip: string; session_token?: string; session_start: string; session_end: string; req_count: number; is_streaming?: boolean }>
  total?: number
}

configureExternalAdminOrigin()

test.describe('Data Management Page Contract (/logs)', () => {
  test('1. Route accessibility, Admin RBAC contract & ServerFooter environment indicator', async ({ page }) => {
    const res = await page.goto('/logs')
    expect(res?.status()).toBe(200)
    await expect(page.getByRole('heading', { name: 'Data Management', exact: true })).toBeVisible({ timeout: 30_000 })
    for (const btnName of [/Import Logs/i, /Backfill Logs/i, /Commit Buffer/i]) {
      await expect(page.getByRole('button', { name: btnName })).toBeVisible({ timeout: 15_000 })
    }
    await assertFooterVisible(page)
    await assertAnalyst403(page, 'Logs', () => ['/api/cron-runs', '/api/cron-schedule', '/api/audit-logs', '/api/admin/ingested-files'])
  })

  test('2. Instant shell & warm-load performance budget (CLS <= 0.05)', async ({ page }) => {
    await assertWarmPerfBudget(page, '/logs', { settleMs: 500, checkLcp: false, heading: 'Data Management' })
  })

  test('3. Backend REST contracts (/api/cron-runs, /api/cron-schedule, /api/audit-logs, /api/admin/ingested-files, /api/schema)', async ({ page }) => {
    const serviceId = await getFirstServiceId(page)
    const withSvc = (base: string, extra = '') =>
      serviceId ? `${base}?service_id=${encodeURIComponent(serviceId)}${extra ? `&${extra}` : ''}` : `${base}${extra ? `?${extra}` : ''}`

    for (const [url, prop] of [
      [withSvc('/api/cron-runs', 'limit=50'), 'entries'],
      [withSvc('/api/cron-schedule'), 'schedules'],
      [withSvc('/api/audit-logs', 'limit=50'), 'entries'],
      [withSvc('/api/admin/ingested-files', 'limit=50'), 'files'],
      [withSvc('/api/schema'), 'schema'],
    ] as const) {
      const res = await page.request.get(url)
      expect(res.ok()).toBeTruthy()
      expect(Array.isArray((await res.json())[prop])).toBe(true)
    }
  })

  test('4. 7-tab workspace navigation & lazy-mounted tab panels', async ({ page }) => {
    await gotoHeading(page, '/logs', 'Data Management')
    for (const name of [/Cron Runs/i, /Service History/i, /Ingestion History/i, /DuckLake Storage/i, /Metadata Storage/i, /Available Logs/i, /DuckDB Schema/i]) {
      await expect(page.getByRole('tab', { name })).toBeVisible()
    }
    for (const [tabRe, heading] of [
      [/Service History/i, 'Service History'],
      [/Ingestion History/i, 'Log Ingestion History'],
      [/DuckDB Schema/i, 'DuckDB Table Schema & Statistics'],
    ] as const) {
      await page.getByRole('tab', { name: tabRe }).click()
      await expect(page.getByRole('heading', { name: heading, exact: true })).toBeVisible({ timeout: 15_000 })
    }
    const cronTab = page.getByRole('tab', { name: /Cron Runs/i })
    await cronTab.click()
    await expect(cronTab).toHaveAttribute('aria-selected', 'true')
  })

  test('5. Interactive Service History refresh propagates X-Page-Load-ID header', async ({ page }) => {
    await gotoHeading(page, '/logs?tab=service_history', 'Data Management')
    await expect(page.getByRole('heading', { name: 'Service History', exact: true })).toBeVisible({ timeout: 15_000 })
    const capturedPageLoadIds: string[] = []
    page.on('request', (req) => {
      if (req.url().includes('/api/audit-logs') && req.headers()['x-page-load-id']) {
        capturedPageLoadIds.push(req.headers()['x-page-load-id'])
      }
    })
    const refreshBtn = page.getByRole('button', { name: /^Refresh$/i })
    await expect(refreshBtn).toBeVisible({ timeout: 10_000 })
    await refreshBtn.click()
    await expect.poll(() => capturedPageLoadIds.length, { timeout: 10_000 }).toBeGreaterThan(0)
  })
})

test('custom-field admin page renders', async ({ page }) => {
  expect((await page.goto('/admin'))?.status()).toBeLessThan(500)
})

test('VCL editor debounce → validate-vcl fires after 500 ms in a real browser', async ({ page }) => {
  const validateWaiter = page.waitForRequest(
    (req) => req.method() === 'POST' && /\/api\/services\/[^/]+\/custom-fields\/validate-vcl/.test(req.url()),
    { timeout: 30_000 },
  )
  await page.goto('/admin')
  for (const step of [
    page.getByRole('button', { name: /manage/i }).first(),
    page.getByRole('menuitem', { name: /log settings/i }),
    page.getByRole('button', { name: /next step/i }),
    page.getByRole('button', { name: /add field/i }),
  ]) {
    await expect(step).toBeVisible({ timeout: 30_000 })
    await step.click()
  }
  await page.getByLabel('Label', { exact: false }).first().fill('Pw Vcl Probe')
  const vclInput = page.getByLabel('VCL Log Expression')
  await expect(vclInput).toBeVisible()
  await vclInput.fill('req.http.Host')
  expect((await validateWaiter).url()).toMatch(/\/custom-fields\/validate-vcl/)
  await expect(page.getByText(/VCL Expression is valid|Validation Errors|Validation could not run/i).first()).toBeVisible({ timeout: 30_000 })
})

test.describe('SQL Query Pad Page Contract (/query)', () => {
  test('1. Route accessibility, environment indicator & layout mounting', async ({ page }) => {
    await assertRouteAndFooter(page, '/query', /Query Explorer/i)
  })

  test('2. Instant shell, pre-allocated layout & zero-CLS containers', async ({ page }) => {
    await page.goto('/query')
    for (const id of ['query-page-root', 'query-editor-container', 'query-run-btn']) {
      await expect(page.locator(`[data-testid="${id}"]`)).toBeVisible({ timeout: 30_000 })
    }
    await assertIntrinsicCount(page, 2)
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
    expect(Array.isArray(schemaData.columns) && schemaData.columns.length > 0).toBeTruthy()

    await page.goto('/query')
    await expect(page.locator('[data-testid="query-page-root"]')).toBeVisible({ timeout: 30_000 })
    await page.getByRole('tab', { name: /Edit Raw SQL/i }).click()
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
    const postData = await postRes.json()
    for (const k of ['columns', 'data', 'total_rows', 'elapsed_ms']) expect(postData).toHaveProperty(k)
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
    await expect(page.locator('[data-testid="query-error-alert"]')).toContainText(/only SELECT|sql_not_permitted|Query Execution Failed/i, { timeout: 15_000 })
  })

  test('6. Client-side CSV export trigger', async ({ page }) => {
    await page.goto('/query?q=' + encodeURIComponent('SELECT count(*) AS total_count FROM logs'))
    const main = page.getByRole('main')
    await expect(main.getByTestId('query-page-root')).toBeVisible({ timeout: 30_000 })
    await main.getByTestId('query-run-btn').click()
    await expect(main.getByTestId('query-results-table')).toBeVisible({ timeout: 20_000 })

    const exportBtn = main.getByTestId('query-csv-export-btn')
    await expect(exportBtn).toBeVisible({ timeout: 10_000 })
    const downloadPromise = page.waitForEvent('download')
    await exportBtn.click()
    expect((await downloadPromise).suggestedFilename()).toBe('query_results.csv')
  })

  test('7. Performance budget: FCP < 400ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/query', { maxFcpMs: 400, checkLcp: false })
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/query', { sql: 'SELECT count(*) AS total_count FROM logs', dataset: 'logs', max_rows: 10 })
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

test.describe('Security Page Contract (/security)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/security', 'Security')
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/security', 'Security')
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: /Well-Known Bots|Verified Bots/i }).first()).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 2)
    await page.getByRole('tab', { name: /vpn & proxy watchdog/i }).click()
    await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: /Active VPN & Proxy Users/i }).first()).toBeVisible({ timeout: 15_000 })
    await assertIntrinsicCount(page, 6)
  })

  test('3. Data contract & round-trip verification (/api/security/aggregates & /api/security/bundle)', async ({ page }) => {
    const aggregatesRes = await page.request.post('/api/security/aggregates', {
      data: {
        range_token: '24h',
        sections: ['ngwaf_verified_bots', 'ngwaf_verified_bots_ts', 'wellknown_bots', 'tls_fingerprints', 'fingerprint_coverage', 'req_size_dist', 'top_ips_header', 'ipv6_adoption', 'proxy_dist', 'conn_reuse_dist', 'proxies'],
      },
    })
    expect(aggregatesRes.ok()).toBeTruthy()
    const aggData = await aggregatesRes.json()
    for (const k of ['wellknown_bots', 'tls_fingerprints', 'req_size_dist', 'top_ips_header', 'ipv6_adoption', 'proxy_dist', 'conn_reuse_dist']) {
      expect(aggData).toHaveProperty(k)
    }
    expect(await (await page.request.post('/api/security/bundle', { data: { range_token: '24h' } })).json()).toHaveProperty('wellknown_bots')
    expect(await (await page.request.get('/api/security/bundle?range_token=24h')).json()).toHaveProperty('wellknown_bots')
    await gotoHeading(page, '/security', 'Security')
  })

  test('4. All core security cards render properly across tabs', async ({ page }) => {
    await gotoHeading(page, '/security', 'Security')
    for (const title of ['Well-Known Bots', 'Top TLS Fingerprints', 'Request Header Size Distribution', 'Oversized Request Headers (by IP)', 'IPv6 Adoption over Time', 'Proxy/Anonymizer Breakdown', 'Connection Reuse (Requests per Connection)']) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({ timeout: 15_000 })
    }
    await page.getByRole('tab', { name: /threat intelligence/i }).click()
    await page.waitForTimeout(500)
    await page.getByRole('tab', { name: /vpn & proxy watchdog/i }).click()
    for (const title of ['Active VPN & Proxy Users', 'Tunnel Requests', 'Impossible Distance Mismatches', 'Traffic Quality Breakdown', 'Suspicious ISPs & Providers', 'VPN/Proxy Export Center', 'Active Suspicious Clients']) {
      await expect(page.locator('#main [data-slot="card-title"]').filter({ hasText: title }).first()).toBeVisible({ timeout: 15_000 })
    }
  })

  test('5. Time range presets and adaptive history extents (15m, 24h, 30d)', async ({ page }) => {
    await assertTimePresetsNo500(page, '/security', 'Security')
  })

  test('6. Column visibility controls on security tables', async ({ page }) => {
    await gotoHeading(page, '/security', 'Security')
    const colBtn = page.getByRole('button', { name: /columns/i }).first()
    if (await colBtn.isVisible()) {
      await colBtn.click()
      await expect(page.getByRole('menuitemcheckbox').or(page.getByText(/requests|ips|name/i)).first()).toBeVisible({ timeout: 5_000 })
    }
  })

  test('7. Non-empty Plotly visualizations and zero error cards', async ({ page }) => {
    await gotoHeading(page, '/security', 'Security')
    const plotlyChart = page.locator('.js-plotly-plot, .plotly').first()
    if (await plotlyChart.isVisible()) {
      await expect(plotlyChart.locator('.main-svg').first()).toBeVisible({ timeout: 10_000 })
    }
    await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
  })

  test('8. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/security')
  })

  test('9. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    await assertPostTelemetry(page, '/api/security/aggregates', { range_token: '24h', sections: ['wellknown_bots', 'req_size_dist'] })
  })
})

test.describe('User Sessions Explorer Page Contract (/sessions)', () => {
  test('1. Route accessibility & environment indicator', async ({ page }) => {
    await assertRouteAndFooter(page, '/sessions', /User Sessions/i)
  })

  test('2. Instant shell, pre-allocated layout & zero CLS containers', async ({ page }) => {
    await gotoHeading(page, '/sessions', /User Sessions/i)
    for (const tid of ['user-sessions-explorer', 'sessions-table-container', 'sessions-scoring-controls']) {
      await expect(page.locator(`[data-testid="${tid}"]`)).toBeVisible({ timeout: 15_000 })
    }
    await assertIntrinsicCount(page, 1)
  })

  test('3. Data contract & schema validation (/api/sessions and /api/sessions/detail)', async ({ page }) => {
    const postRes = await page.request.post('/api/sessions', { data: { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' } })
    expect(postRes.ok()).toBeTruthy()
    const postData = (await postRes.json()) as SessionsDebugResponse
    expect(Array.isArray(postData.sessions) && typeof postData.total === 'number').toBeTruthy()

    if (postData.sessions && postData.sessions.length > 0) {
      const detailRes = await page.request.post('/api/sessions/detail', { data: { session_token: postData.sessions[0].session_token } })
      expect(detailRes.ok()).toBeTruthy()
      const detailData = await detailRes.json()
      expect(Array.isArray(detailData.columns) && Array.isArray(detailData.data)).toBeTruthy()
    }
    await gotoHeading(page, '/sessions', /User Sessions/i)
  })

  test('4. Filter controls & toggle interactions without console errors or 500s', async ({ page }) => {
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    })
    await page.goto('/sessions')
    const main = page.getByRole('main')
    await expect(main.getByTestId('sessions-scoring-controls')).toBeVisible({ timeout: 30_000 })

    const flaggedSwitch = main.getByTestId('flagged-only-switch')
    await flaggedSwitch.click()
    await page.waitForTimeout(500)
    await flaggedSwitch.click()
    await page.waitForTimeout(500)

    const minReqsInput = main.getByTestId('min-reqs-input')
    await minReqsInput.fill('5')
    await page.waitForTimeout(500)
    await minReqsInput.fill('')
    await page.waitForTimeout(500)
    expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
  })

  test('5. Row detail inspection dialog with session metadata', async ({ page }) => {
    await page.goto('/sessions')
    const table = page.getByRole('main').getByTestId('sessions-table-container')
    await expect(table).toBeVisible({ timeout: 30_000 })
    const tableRows = table.locator('tbody tr')
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
    await gotoHeading(page, '/sessions/stream', /Stream Details/i)
    await expect(page.getByText(/No session token/i)).toBeVisible({ timeout: 10_000 })

    const postRes = await page.request.post('/api/sessions', { data: { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' } })
    if (postRes.ok()) {
      const token = ((await postRes.json()) as SessionsDebugResponse).sessions?.[0]?.session_token
      if (token) {
        await page.goto(`/sessions/stream?token=${encodeURIComponent(token)}`)
        await expect(page.locator('[data-testid="stream-detail-client"]')).toBeVisible({ timeout: 30_000 })
        await expect(page.getByRole('heading', { name: /Stream Session Details/i })).toBeVisible({ timeout: 10_000 })
      }
    }
  })

  test('7. Performance budget: LCP < 1500ms and CLS <= 0.05 on warm load', async ({ page }) => {
    await assertWarmPerfBudget(page, '/sessions')
  })

  test('8. Telemetry instrumentation & query attribution audit', async ({ page }) => {
    const json = await assertPostTelemetry(page, '/api/sessions', { page: 1, limit: 10, sort_by: 'session_start', sort_dir: 'desc' })
    for (const q of (json._debug_queries ?? []).filter((item) => !item.sql?.includes('CREATE OR REPLACE VIEW'))) {
      if (typeof q.time_ms === 'number') expect(q.time_ms).toBeLessThan(2000)
    }
  })
})
