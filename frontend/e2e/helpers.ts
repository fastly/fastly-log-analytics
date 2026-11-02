import AxeBuilder from '@axe-core/playwright'
import { expect, test, type APIRequestContext, type Locator, type Page } from '@playwright/test'
import { adminGatewayContextOptions } from '../../scripts/lib/admin_gateway_client.cjs'
import type { DebugQuery } from '../types/api'

declare global {
  interface Window {
    __clsScore?: number
  }
}

interface LayoutShiftEntry extends PerformanceEntry {
  hadRecentInput?: boolean
  value?: number
}

export type DebugResponse = {
  _debug_queries?: DebugQuery[]
  debug_queries?: DebugQuery[]
  _debug_sqlite?: unknown[]
  [key: string]: unknown
}

/**
 * Configure external origin and admin gateway client TLS options when
 * `PLAYWRIGHT_BASE_URL` is set in the environment.
 */
export function configureExternalAdminOrigin(): string {
  const externalOrigin = process.env.PLAYWRIGHT_BASE_URL || ''
  if (externalOrigin) {
    test.use({
      baseURL: externalOrigin,
      ...adminGatewayContextOptions(externalOrigin),
    })
  }
  return externalOrigin
}

/**
 * Resolve the first configured service ID from `/api/bootstrap`, asserting that
 * bootstrap succeeds and at least one service is registered.
 */
export async function resolveFirstServiceId(page: Page): Promise<string> {
  const res = await page.request.get('/api/bootstrap')
  expect(res.status()).toBe(200)
  const body = (await res.json()) as {
    active_service_id?: string | null
    services?: Array<{ id?: string; service_id?: string }>
  }
  const services = Array.isArray(body?.services) ? body.services : []
  expect(services.length).toBeGreaterThan(0)
  return (body.active_service_id || services[0]?.id || services[0]?.service_id) as string
}

/**
 * Resolve the active or first service ID from `/api/bootstrap`, returning
 * `null` if bootstrap fails or no services are configured.
 */
export async function getFirstServiceId(page: Page): Promise<string | null> {
  const res = await page.request.get('/api/bootstrap')
  if (!res.ok()) return null
  const body = (await res.json()) as {
    active_service_id?: string | null
    services?: Array<{ id?: string; service_id?: string }>
  }
  return body.active_service_id ?? body.services?.[0]?.id ?? body.services?.[0]?.service_id ?? null
}

/**
 * Pass to `page.addInitScript`. Playwright serialises the function body, so it
 * must stay self-contained (no references to module-level values).
 */
export function clsInitScript(): void {
  window.__clsScore = 0
  try {
    const obs = new PerformanceObserver((list) => {
      for (const entry of list.getEntries() as LayoutShiftEntry[]) {
        if (!entry.hadRecentInput && typeof entry.value === 'number') {
          window.__clsScore = (window.__clsScore ?? 0) + entry.value
        }
      }
    })
    obs.observe({ type: 'layout-shift', buffered: true })
  } catch {
    // layout-shift observer unsupported in this browser
  }
}

export async function installClsObserver(page: Page): Promise<void> {
  await page.addInitScript(clsInitScript)
}

export async function readClsScore(page: Page): Promise<number> {
  return page.evaluate(() => Number(window.__clsScore ?? 0))
}

export async function measureWarmPaintAndCls(
  page: Page,
  settleMs = 500,
): Promise<{ fcp: number; lcp: number; cls: number }> {
  return page.evaluate((waitMs) => {
    return new Promise<{ fcp: number; lcp: number; cls: number }>((resolve) => {
      let fcp = 0
      let lcp = 0
      try {
        const paintEntries = performance.getEntriesByType('paint')
        const fcpEntry = paintEntries.find((e) => e.name === 'first-contentful-paint')
        if (fcpEntry) fcp = fcpEntry.startTime
        new PerformanceObserver((list) => {
          const entries = list.getEntries()
          if (entries.length > 0) lcp = entries[entries.length - 1].startTime
        }).observe({ type: 'largest-contentful-paint', buffered: true })
      } catch {
        // ignore if unsupported
      }
      setTimeout(() => resolve({ fcp, lcp, cls: Number(window.__clsScore ?? 0) }), waitMs)
    })
  }, settleMs)
}

export function setCookieValues(
  headers: { name: string; value: string }[],
  cookieName: string,
): string | null {
  for (const h of headers) {
    if (h.name.toLowerCase() === 'set-cookie' && h.value.startsWith(`${cookieName}=`)) {
      const val = h.value.split(';', 1)[0].slice(cookieName.length + 1)
      if (val) return val
    }
  }
  return null
}

export interface PasscodeInviteSpec {
  name?: string
  email?: string
  passcode?: string
  durationHours?: number
  serviceIds: string[]
}

export interface SeededPasscodeInvite {
  inviteId: string
  email: string
  passcode: string
}

export async function seedPasscodeAnalystInvite(
  request: APIRequestContext,
  spec: PasscodeInviteSpec,
): Promise<SeededPasscodeInvite> {
  const email = spec.email ?? `e2e-analyst-${Date.now()}@example.com`
  const passcode = spec.passcode ?? 'ocean-breeze-cabin-42'
  const createRes = await request.post('/api/admin/share/invites', {
    data: {
      name: spec.name ?? 'E2E Share Analyst',
      email,
      passcode,
      duration_hours: spec.durationHours ?? 2,
      service_ids: spec.serviceIds,
      auth_method: 'passcode',
    },
  })
  expect(createRes.ok(), await createRes.text()).toBeTruthy()
  const inviteData = (await createRes.json()) as { id: string }
  expect(inviteData.id).toBeTruthy()
  return { inviteId: inviteData.id, email, passcode }
}

export async function revokeAnalystInvite(
  request: APIRequestContext,
  inviteId: string | null | undefined,
  options?: { logout?: boolean; stopShare?: boolean },
): Promise<void> {
  if (options?.logout) await request.post('/api/share/logout').catch(() => {})
  if (inviteId) await request.delete(`/api/admin/share/invites/${inviteId}`).catch(() => {})
  if (options?.stopShare ?? options?.logout) await request.post('/api/admin/share/stop').catch(() => {})
}

export async function loginAsPasscodeAnalyst(
  request: APIRequestContext,
  spec: PasscodeInviteSpec,
): Promise<SeededPasscodeInvite> {
  await request.post('/api/admin/share/start', {
    data: { public_endpoint: 'https://share-e2e.example.com', forward_port: 13004 },
  })
  const seeded = await seedPasscodeAnalystInvite(request, spec)
  expect((await request.post('/api/share/login', { data: { email: seeded.email, passcode: seeded.passcode } })).ok()).toBeTruthy()
  const tosRes = await request.get('/api/share/tos')
  expect(tosRes.ok()).toBeTruthy()
  const tosJson = (await tosRes.json()) as { version: string }
  expect((await request.post('/api/share/acknowledge', { data: { version: tosJson.version } })).ok()).toBeTruthy()
  return seeded
}

export async function gotoHeading(page: Page, route: string, heading: string | RegExp) {
  await page.goto(route)
  await expect(page.getByRole('heading', { name: heading }).first()).toBeVisible({ timeout: 30_000 })
}

export async function assertFooterVisible(page: Page, pattern: RegExp = /Admin View|Analyst View/i) {
  const footer = page.locator('footer').first()
  await expect(footer).toBeVisible({ timeout: 10_000 })
  const text = ((await footer.textContent()) ?? '').trim()
  expect(text.length).toBeGreaterThan(0)
  expect(text).toMatch(pattern)
}

export async function assertRouteAndFooter(page: Page, route: string, heading: string | RegExp) {
  await gotoHeading(page, route, heading)
  await assertFooterVisible(page)
}

export async function assertIntrinsicCount(page: Page, minCount: number) {
  expect(await page.locator('[class*="contain-intrinsic-size"]').count()).toBeGreaterThanOrEqual(minCount)
}

export async function assertZeroErrorCards(page: Page, route: string, heading: string | RegExp) {
  await gotoHeading(page, route, heading)
  await expect(page.locator('[data-testid="error-card"], .alert-destructive')).toHaveCount(0)
}

export async function assertTimePresetsNo500(page: Page, route: string, heading: string | RegExp) {
  const consoleErrors: string[] = []
  page.on('console', (msg) => {
    if (msg.type() === 'error') consoleErrors.push(msg.text())
  })
  await gotoHeading(page, route, heading)
  await expect(page.getByRole('button', { name: /24h|1h|7d|last/i }).first()).toBeVisible({ timeout: 10_000 })
  expect(consoleErrors.filter((e) => e.includes('500') || e.includes('Internal Server Error'))).toHaveLength(0)
}

export async function assertWarmPerfBudget(
  page: Page,
  route: string,
  opts: { settleMs?: number; maxFcpMs?: number; maxLcpMs?: number; checkLcp?: boolean; heading?: string } = {},
) {
  await page.goto(route, { waitUntil: 'domcontentloaded' })
  if (opts.heading) await expect(page.getByRole('heading', { name: opts.heading, exact: true })).toBeVisible({ timeout: 30_000 })
  await installClsObserver(page)
  const start = Date.now()
  await page.reload({ waitUntil: 'domcontentloaded' })
  const totalTime = Date.now() - start
  if (opts.heading) await expect(page.getByRole('heading', { name: opts.heading, exact: true })).toBeVisible({ timeout: 15_000 })
  const metrics = await measureWarmPaintAndCls(page, opts.settleMs ?? 1000)
  expect(totalTime).toBeLessThan(10_000)
  if (opts.maxFcpMs && metrics.fcp > 0) expect(metrics.fcp).toBeLessThan(opts.maxFcpMs)
  if ((opts.checkLcp ?? true) && metrics.lcp > 0) expect(metrics.lcp).toBeLessThan(opts.maxLcpMs ?? 1500)
  expect(metrics.cls).toBeLessThanOrEqual(0.05)
}

export async function assertPostTelemetry(
  page: Page,
  endpoint: string,
  data: Record<string, unknown>,
  extraHeaders: Record<string, string> = {},
  optionalQueries = false,
): Promise<DebugResponse> {
  const res = await page.request.post(endpoint, { data, headers: { 'x-debug-responses': '1', ...extraHeaders } })
  expect(res.ok()).toBeTruthy()
  const json = (await res.json()) as DebugResponse
  const queries = json._debug_queries ?? json.debug_queries
  if (!optionalQueries || queries) {
    expect(Array.isArray(queries) && queries.length > 0).toBeTruthy()
  }
  return json
}

export async function assertAnalyst403(
  page: Page,
  label: string,
  urlBuilder: (serviceId: string) => string[],
) {
  if (process.env.PLAYWRIGHT_BASE_URL) return
  const serviceId = (await getFirstServiceId(page)) ?? 'svc-playwright-e2e'
  let inviteId: string | null = null
  try {
    const seeded = await loginAsPasscodeAnalyst(page.request, {
      name: `${label} Guard Analyst`,
      email: `e2e-${label.toLowerCase()}-guard-${Date.now()}@example.com`,
      passcode: 'harbor-cloud-forest-91',
      durationHours: 1,
      serviceIds: [serviceId],
    })
    inviteId = seeded.inviteId
    for (const url of urlBuilder(serviceId)) {
      const res = await page.request.get(url, { headers: { 'x-remote-analyst': '1' } })
      expect(res.status()).toBe(403)
      const body = await res.json().catch(() => ({}))
      if (body.error) expect(body.error).toBe('admin_only')
    }
  } finally {
    await revokeAnalystInvite(page.request, inviteId, { logout: true })
  }
}

export async function assertIntrinsicReservations(page: Page, testIds: string[], root?: Locator) {
  for (const testId of testIds) {
    const container = root ? root.getByTestId(testId) : page.locator(`[data-testid="${testId}"]:visible`)
    await expect(container).toBeVisible({ timeout: 15_000 })
    const classAttr = (await container.getAttribute('class')) ?? ''
    expect(classAttr).toContain('contain-intrinsic-size')
    expect(classAttr).toContain('content-visibility')
  }
}

export async function assertWarmClsAndFcp(
  page: Page,
  navigateOrReload: () => Promise<void>,
  readyLocators: Locator[],
  maxFcpMs = 1200,
) {
  await page.addInitScript(clsInitScript)
  await navigateOrReload()
  for (const loc of readyLocators) await expect(loc).toBeVisible({ timeout: 30_000 })
  const metrics = await page.evaluate(() => {
    const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0] as PerformanceEntry | undefined
    return { fcpMs: fcpEntry ? fcpEntry.startTime : 0, cls: window.__clsScore ?? 0 }
  })
  expect(metrics.cls).toBeLessThanOrEqual(0.05)
  if (metrics.fcpMs > 0) expect(metrics.fcpMs).toBeLessThan(maxFcpMs)
}

export async function assertManualRefreshAttribution(
  page: Page,
  opts: { url: string; readyLocator: Locator; refreshLocator: Locator; requestFilter: (url: string) => boolean; ssrQueryKey?: string },
) {
  const tracked: Array<{ url: string; pageLoadId: string | null }> = []
  page.on('request', (req) => {
    if (opts.requestFilter(req.url())) tracked.push({ url: req.url(), pageLoadId: req.headers()['x-page-load-id'] ?? null })
  })
  const response = await page.goto(opts.url, { waitUntil: 'domcontentloaded' })
  await expect(opts.readyLocator).toBeVisible({ timeout: 30_000 })
  if (opts.ssrQueryKey) {
    expect(await response?.text()).toContain(opts.ssrQueryKey)
  } else {
    await expect.poll(() => tracked.length, { timeout: 25_000 }).toBeGreaterThanOrEqual(1)
    expect(tracked[0].pageLoadId).toBeTruthy()
  }

  const before = tracked.length
  await expect(opts.refreshLocator).toBeVisible({ timeout: 15_000 })
  await expect(opts.refreshLocator).toBeEnabled({ timeout: 15_000 })
  await expect
    .poll(
      async () => {
        if (tracked.length <= before) await opts.refreshLocator.click()
        return tracked.length
      },
      { timeout: 15_000, intervals: [250, 500, 1000] },
    )
    .toBeGreaterThan(before)
  expect(tracked.slice(before).every((r) => Boolean(r.pageLoadId))).toBe(true)
  await expect(opts.readyLocator).toBeVisible()
}

export async function assertNoWcagViolations(page: Page, extraExcludes: string[] = []) {
  let builder = new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .exclude('[data-testid="plotly-host"]')
    .exclude('[data-empty-placeholder="true"]')
    .exclude('[data-axe-ignore="true"]')
    .exclude('.maplibregl-map')
  for (const sel of extraExcludes) builder = builder.exclude(sel)
  const results = await builder.analyze()
  expect(
    results.violations,
    results.violations.map((v) => `[${v.id}] ${v.help} (nodes: ${v.nodes.length}, first: ${v.nodes[0]?.target?.join(' > ')})`).join('\n'),
  ).toEqual([])
}
