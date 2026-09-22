import { test, expect, chromium, type APIRequestContext, type BrowserContext } from '@playwright/test'
import * as fs from 'fs'
import * as path from 'path'
import { E2E_BACKEND_PORT } from '../playwright.config'

const SERVICE_ID = 'svc-playwright-e2e'
const IS_EXHAUSTIVE = process.env.PERF_EXHAUSTIVE === '1'
const NUM_ITERATIONS = IS_EXHAUSTIVE ? 5 : 1

interface MetricResult {
  lcp: number
  fcp: number
  fp: number
  duration: number
  domInteractive: number
  domContentLoaded: number
  slowestApi: number
}

interface TestResult {
  name: string
  path: string
  role: 'Admin' | 'Analyst'
  blocked: boolean
  metrics: MetricResult
}

const ALL_PAGES = [
  { name: 'Dashboard', path: '/dashboard', adminOnly: false },
  { name: 'Control Room', path: '/control-room', adminOnly: false },
  { name: 'Service Summary / Value', path: '/fastly-value', adminOnly: false },
  { name: 'Performance', path: '/performance', adminOnly: false },
  { name: 'Origin Health', path: '/origin', adminOnly: false },
  { name: 'Security', path: '/security', adminOnly: false },
  { name: 'Insights', path: '/insights', adminOnly: false },
  { name: 'Network Path', path: '/network', adminOnly: false },
  { name: 'Streaming', path: '/streaming', adminOnly: false },
  { name: 'RUM', path: '/rum', adminOnly: false },
  { name: 'Sessions', path: '/sessions', adminOnly: false },
  { name: 'Sessions Stream', path: '/sessions/stream', adminOnly: false },
  { name: 'Usage & Cost', path: '/usage', adminOnly: true },
  { name: 'SQL Query Pad', path: '/query', adminOnly: false },
  { name: 'Alerts', path: '/alerts', adminOnly: true },
  { name: 'Raw Logs', path: '/logs', adminOnly: true },
  { name: 'Assets & Shield', path: '/assets-shield', adminOnly: false },
  { name: 'High-Scale Facts', path: '/high-scale/request-facts', adminOnly: true },
  { name: 'Analyst Share Login', path: '/share-login', adminOnly: false },
  { name: 'Admin Overview', path: '/admin', adminOnly: true },
  { name: 'Live Query Monitor', path: '/admin/queries', adminOnly: true },
  { name: 'Task Queue', path: '/admin/queue', adminOnly: true },
  { name: 'RUM Ingestion', path: '/admin/rum', adminOnly: true },
  { name: 'Session Scoring Config', path: '/admin/session-scoring', adminOnly: true },
  { name: 'Live Share Admin', path: '/admin/share', adminOnly: true },
  { name: 'System Trends', path: '/admin/trends', adminOnly: true },
  { name: 'FOS Usage Ledger', path: '/admin/usage-log', adminOnly: true }
]

const CORE_PAGES = [
  { name: 'Dashboard', path: '/dashboard', adminOnly: false },
  { name: 'Control Room', path: '/control-room', adminOnly: false },
  { name: 'Performance', path: '/performance', adminOnly: false },
  { name: 'Security', path: '/security', adminOnly: false },
  { name: 'SQL Query Pad', path: '/query', adminOnly: false },
]

const PAGES = IS_EXHAUSTIVE ? ALL_PAGES : CORE_PAGES

const allResults: TestResult[] = []

function getPercentile(values: number[], pct: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const index = Math.ceil((pct / 100) * sorted.length) - 1
  return sorted[index]
}

function getCookieValue(headers: { name: string; value: string }[], cookieName: string): string | null {
  for (const h of headers) {
    if (h.name.toLowerCase() === 'set-cookie') {
      const match = h.value.match(new RegExp(`^${cookieName}=([^;]+)`))
      if (match) {
        const val = match[1].replace(/^"|"$/g, '').trim()
        if (val && !h.value.includes('Max-Age=0')) return val
      }
    }
  }
  return null
}

async function loginAsAnalyst(request: APIRequestContext, context: BrowserContext, serviceId: string): Promise<string> {
  const email = `e2e-analyst-${Date.now()}@example.com`

  // 1. Seed an OAuth invite
  const created = await request.post(`http://127.0.0.1:${E2E_BACKEND_PORT}/api/admin/share/invites`, {
    headers: { 'Content-Type': 'application/json' },
    data: JSON.stringify({
      name: 'E2E Perf Analyst',
      email,
      auth_method: 'oauth',
      oauth_provider: 'google',
      service_ids: [serviceId],
      duration_hours: 24,
    }),
  })
  if (!created.ok()) {
    throw new Error(`Failed to seed analyst invite: ${await created.text()}`)
  }

  // 2. /authorize
  const auth = await request.get(`http://127.0.0.1:${E2E_BACKEND_PORT}/api/share/oauth/authorize?provider=google`, { maxRedirects: 0 })
  const flowState = getCookieValue(auth.headersArray(), 'oauth_flow_state')
  const idpUrl = auth.headers()['location']
  if (!flowState || !idpUrl) {
    throw new Error('Failed to start OAuth flow')
  }

  // 3. Mock IdP
  const idp = await request.get(idpUrl, {
    maxRedirects: 0,
    headers: { cookie: `mock_idp_email=${email}` },
  })
  const callbackUrl = idp.headers()['location']
  if (!callbackUrl) {
    throw new Error('Failed to get callback URL from IdP')
  }

  // 4. Callback
  const cb = await request.get(callbackUrl, {
    maxRedirects: 0,
    headers: { cookie: `oauth_flow_state=${flowState}` },
  })

  // If TOS was already accepted or callback granted session directly:
  const directSessionId = getCookieValue(cb.headersArray(), 'analyst_session_id')
  if (directSessionId) {
    return directSessionId
  }

  const pendingSessionId = getCookieValue(cb.headersArray(), 'analyst_pending_session_id')
  if (!pendingSessionId) {
    throw new Error('Failed to get analyst_pending_session_id cookie')
  }

  // 5. Acknowledge TOS directly against backend
  const ack = await request.post(`http://127.0.0.1:${E2E_BACKEND_PORT}/api/share/acknowledge`, {
    data: { version: 'v1' },
    headers: {
      cookie: `analyst_pending_session_id=${pendingSessionId}`,
    },
  })
  const sessionId = getCookieValue(ack.headersArray(), 'analyst_session_id')
  if (!sessionId) {
    throw new Error(`Failed to get analyst_session_id cookie after TOS acknowledgement: status=${ack.status()} body=${await ack.text()}`)
  }

  return sessionId
}

test.describe('E2E Performance & Posture Harness', () => {
  let analystSessionId: string | null = null

  test.beforeAll(async ({ request, browser }) => {
    // Perform standard analyst login once at startup to reuse across the Analyst role run
    const context = await browser.newContext()
    try {
      analystSessionId = await loginAsAnalyst(request, context, SERVICE_ID)
      console.log(`[PERF] Seeded Analyst Session: ${analystSessionId}`)
    } catch (e) {
      console.warn(`[WARN] Analyst login setup failed: ${e}`)
      throw e
    } finally {
      await context.close()
    }
  })

  test.afterAll(() => {
    const rootDir = process.cwd().endsWith('frontend') ? path.join(process.cwd(), '..') : process.cwd()
    const reportPath = path.join(rootDir, 'performance-report')

    // Generate UTC timestamp directory name, e.g., "2026-09-20_17-10-32"
    const now = new Date()
    const timestamp = now.toISOString()
      .replace(/T/, '_')
      .replace(/\..+/, '')
      .replace(/:/g, '-')
    const archivePath = path.join(reportPath, timestamp)

    if (!fs.existsSync(reportPath)) {
      fs.mkdirSync(reportPath, { recursive: true })
    }
    if (!fs.existsSync(archivePath)) {
      fs.mkdirSync(archivePath, { recursive: true })
    }

    const jsonContent = JSON.stringify(allResults, null, 2)

    // 1. Output unified JSON reports (both root and archive)
    fs.writeFileSync(path.join(reportPath, 'e2e-performance.json'), jsonContent, 'utf-8')
    fs.writeFileSync(path.join(archivePath, 'e2e-performance.json'), jsonContent, 'utf-8')

    // 2. Output beautifully formatted Markdown report
    let markdown = '# E2E Performance & Security Posture Audit Report\n\n'
    markdown += `*Generated on: ${now.toISOString()}*\n\n`
    markdown += 'This automated report records p95 performance timings and security blockades across all 27 pages/endpoints for both Admin and Analyst roles.\n\n'
    markdown += '## Unified Results Summary Table\n\n'
    markdown += '| Route | Page Name | Role | Posture / Status | LCP (p95) | FCP (p95) | Load (p95) | DomInt (p95) | Slowest API (p95) |\n'
    markdown += '|---|---|---|---|---|---|---|---|---|\n'

    for (const res of allResults) {
      const postureStr = res.blocked ? '🔴 Blocked (Security OK)' : '🟢 Permitted'
      const lcpStr = res.blocked ? 'N/A' : `${res.metrics.lcp}ms`
      const fcpStr = res.blocked ? 'N/A' : `${res.metrics.fcp}ms`
      const loadStr = res.blocked ? 'N/A' : `${res.metrics.duration}ms`
      const domIntStr = res.blocked ? 'N/A' : `${res.metrics.domInteractive}ms`
      const slowestApiStr = res.blocked ? 'N/A' : (res.metrics.slowestApi > 0 ? `${res.metrics.slowestApi}ms` : 'None')

      markdown += `| \`${res.path}\` | ${res.name} | **${res.role}** | ${postureStr} | ${lcpStr} | ${fcpStr} | ${loadStr} | ${domIntStr} | ${slowestApiStr} |\n`
    }

    fs.writeFileSync(path.join(reportPath, 'e2e-performance.md'), markdown, 'utf-8')
    fs.writeFileSync(path.join(archivePath, 'e2e-performance.md'), markdown, 'utf-8')
    console.log(`[PERF] Performance reports saved successfully to:`)
    console.log(`  - Root: performance-report/e2e-performance.*`)
    console.log(`  - Archive: performance-report/${timestamp}/e2e-performance.*`)
  })

  // ── Role 1: Admin (read_write direct) ──────────────────────────────────────
  test.describe('Admin Role', () => {
    for (const pageInfo of PAGES) {
      test(`Admin | ${pageInfo.name} (${pageInfo.path})`, async ({ page }) => {
        const url = `${pageInfo.path}?service=${SERVICE_ID}`
        const lcps: number[] = []
        const fcps: number[] = []
        const fps: number[] = []
        const durations: number[] = []
        const domInteractives: number[] = []
        const domContentLoadeds: number[] = []
        const slowestApis: number[] = []

        for (let iter = 1; iter <= NUM_ITERATIONS; iter++) {
          const requestStartTimes = new Map<string, number>()
          const apiCalls: { url: string; duration: number }[] = []

          page.on('request', req => {
            if (req.url().includes('/api/')) {
              requestStartTimes.set(req.url(), Date.now())
            }
          })

          page.on('requestfinished', req => {
            if (req.url().includes('/api/')) {
              const start = requestStartTimes.get(req.url())
              if (start) {
                apiCalls.push({
                  url: req.url(),
                  duration: Date.now() - start
                })
              }
            }
          })

          const start = Date.now()
          const response = await page.goto(url, { waitUntil: 'load' })
          expect(response?.status()).toBeLessThan(400) // Asserts Admin can load the page

          // Evaluate Web Performance APIs in-browser
          const metrics = await page.evaluate(() => {
            return new Promise<{
              lcp: number
              fcp: number
              fp: number
              duration: number
              domInteractive: number
              domContentLoaded: number
            }>((resolve) => {
              let lcp = 0
              try {
                new PerformanceObserver((list) => {
                  const entries = list.getEntries()
                  const lastEntry = entries[entries.length - 1]
                  lcp = lastEntry.startTime
                }).observe({ type: 'largest-contentful-paint', buffered: true })
              } catch (e) {}

              setTimeout(() => {
                const perf = window.performance
                const nav = perf.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
                const paint = perf.getEntriesByType('paint')

                let fcp = 0
                let fp = 0
                if (paint) {
                  const fcpEntry = paint.find(e => e.name === 'first-contentful-paint')
                  const fpEntry = paint.find(e => e.name === 'first-paint')
                  if (fcpEntry) fcp = fcpEntry.startTime
                  if (fpEntry) fp = fpEntry.startTime
                }

                const duration = nav ? nav.duration : 0
                const domInteractive = nav ? nav.domInteractive : 0
                const domContentLoaded = nav ? nav.domContentLoadedEventEnd : 0

                resolve({
                  lcp: Math.round(lcp),
                  fcp: Math.round(fcp),
                  fp: Math.round(fp),
                  duration: Math.round(duration),
                  domInteractive: Math.round(domInteractive),
                  domContentLoaded: Math.round(domContentLoaded)
                })
              }, 50)
            })
          })

          lcps.push(metrics.lcp)
          fcps.push(metrics.fcp)
          fps.push(metrics.fp)
          durations.push(Date.now() - start - 50)
          domInteractives.push(metrics.domInteractive)
          domContentLoadeds.push(metrics.domContentLoaded)

          const pageSlowestApi = apiCalls.length > 0 ? Math.max(...apiCalls.map(c => c.duration)) : 0
          slowestApis.push(pageSlowestApi)

          // Clean event listeners for next iteration
          page.removeAllListeners('request')
          page.removeAllListeners('requestfinished')
        }

        const stats: MetricResult = {
          lcp: getPercentile(lcps, 95),
          fcp: getPercentile(fcps, 95),
          fp: getPercentile(fps, 95),
          duration: getPercentile(durations, 95),
          domInteractive: getPercentile(domInteractives, 95),
          domContentLoaded: getPercentile(domContentLoadeds, 95),
          slowestApi: getPercentile(slowestApis, 95)
        }

        console.log(`[PERF] Admin | ${pageInfo.name} | LCP: ${stats.lcp}ms | FCP: ${stats.fcp}ms | Load: ${stats.duration}ms | DomInt: ${stats.domInteractive}ms | Slowest API: ${stats.slowestApi}ms`)
        expect(stats.duration).toBeLessThan(12_000)
        expect(stats.slowestApi).toBeLessThan(6_000)
        if (stats.lcp > 0) {
          expect(stats.lcp).toBeLessThan(5_000)
        }

        allResults.push({
          name: pageInfo.name,
          path: pageInfo.path,
          role: 'Admin',
          blocked: false,
          metrics: stats
        })
      })
    }
  })

  // ── Role 2: Analyst (read_only masked remote share) ────────────────────────
  test.describe('Analyst Role', () => {
    test.beforeEach(async ({ context }) => {
      // Thread the pre-generated analyst session cookie
      if (analystSessionId) {
        await context.addCookies([
          {
            name: 'analyst_session_id',
            value: analystSessionId,
            domain: 'localhost',
            path: '/',
            httpOnly: true,
            secure: false,
            sameSite: 'Strict'
          },
          {
            name: 'analyst_session_id',
            value: analystSessionId,
            domain: '127.0.0.1',
            path: '/',
            httpOnly: true,
            secure: false,
            sameSite: 'Strict'
          }
        ])
      }
    })

    for (const pageInfo of PAGES) {
      test(`Analyst | ${pageInfo.name} (${pageInfo.path})`, async ({ page }) => {
        const url = `${pageInfo.path}?service=${SERVICE_ID}`

        if (pageInfo.adminOnly) {
          // POSTURE CHECK: Analyst attempting to request Admin-only pages must be redirected or blocked
          const response = await page.goto(url)

          // Confirms security blockade: either the HTTP request is blocked with 401/403 OR
          // the app redirects the unauthorized user to the permitted /dashboard (standard client-side RBAC)
          await expect.poll(() => {
            const currentUrl = page.url()
            return (response?.status() && response.status() >= 400) || currentUrl.includes('/dashboard') || currentUrl.includes('/share-login')
          }, { timeout: 10_000 }).toBeTruthy()

          allResults.push({
            name: pageInfo.name,
            path: pageInfo.path,
            role: 'Analyst',
            blocked: true,
            metrics: { lcp: 0, fcp: 0, fp: 0, duration: 0, domInteractive: 0, domContentLoaded: 0, slowestApi: 0 }
          })
          console.log(`[POSTURE] Analyst | ${pageInfo.name} | BLOCKED (Security Posture OK)`)
          return
        }

        const lcps: number[] = []
        const fcps: number[] = []
        const fps: number[] = []
        const durations: number[] = []
        const domInteractives: number[] = []
        const domContentLoadeds: number[] = []
        const slowestApis: number[] = []

        for (let iter = 1; iter <= NUM_ITERATIONS; iter++) {
          const requestStartTimes = new Map<string, number>()
          const apiCalls: { url: string; duration: number }[] = []

          page.on('request', req => {
            if (req.url().includes('/api/')) {
              requestStartTimes.set(req.url(), Date.now())
            }
          })

          page.on('requestfinished', req => {
            if (req.url().includes('/api/')) {
              const start = requestStartTimes.get(req.url())
              if (start) {
                apiCalls.push({
                  url: req.url(),
                  duration: Date.now() - start
                })
              }
            }
          })

          const start = Date.now()
          const response = await page.goto(url, { waitUntil: 'load' })
          expect(response?.status()).toBeLessThan(400) // Permitted Analyst pages load successfully

          // Evaluate Web Performance APIs in-browser
          const metrics = await page.evaluate(() => {
            return new Promise<{
              lcp: number
              fcp: number
              fp: number
              duration: number
              domInteractive: number
              domContentLoaded: number
            }>((resolve) => {
              let lcp = 0
              try {
                new PerformanceObserver((list) => {
                  const entries = list.getEntries()
                  const lastEntry = entries[entries.length - 1]
                  lcp = lastEntry.startTime
                }).observe({ type: 'largest-contentful-paint', buffered: true })
              } catch (e) {}

              setTimeout(() => {
                const perf = window.performance
                const nav = perf.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
                const paint = perf.getEntriesByType('paint')

                let fcp = 0
                let fp = 0
                if (paint) {
                  const fcpEntry = paint.find(e => e.name === 'first-contentful-paint')
                  const fpEntry = paint.find(e => e.name === 'first-paint')
                  if (fcpEntry) fcp = fcpEntry.startTime
                  if (fpEntry) fp = fpEntry.startTime
                }

                const duration = nav ? nav.duration : 0
                const domInteractive = nav ? nav.domInteractive : 0
                const domContentLoaded = nav ? nav.domContentLoadedEventEnd : 0

                resolve({
                  lcp: Math.round(lcp),
                  fcp: Math.round(fcp),
                  fp: Math.round(fp),
                  duration: Math.round(duration),
                  domInteractive: Math.round(domInteractive),
                  domContentLoaded: Math.round(domContentLoaded)
                })
              }, 50)
            })
          })

          lcps.push(metrics.lcp)
          fcps.push(metrics.fcp)
          fps.push(metrics.fp)
          durations.push(Date.now() - start - 50)
          domInteractives.push(metrics.domInteractive)
          domContentLoadeds.push(metrics.domContentLoaded)

          const pageSlowestApi = apiCalls.length > 0 ? Math.max(...apiCalls.map(c => c.duration)) : 0
          slowestApis.push(pageSlowestApi)

          // Clean event listeners for next iteration
          page.removeAllListeners('request')
          page.removeAllListeners('requestfinished')
        }

        const stats: MetricResult = {
          lcp: getPercentile(lcps, 95),
          fcp: getPercentile(fcps, 95),
          fp: getPercentile(fps, 95),
          duration: getPercentile(durations, 95),
          domInteractive: getPercentile(domInteractives, 95),
          domContentLoaded: getPercentile(domContentLoadeds, 95),
          slowestApi: getPercentile(slowestApis, 95)
        }

        console.log(`[PERF] Analyst | ${pageInfo.name} | LCP: ${stats.lcp}ms | FCP: ${stats.fcp}ms | Load: ${stats.duration}ms | DomInt: ${stats.domInteractive}ms | Slowest API: ${stats.slowestApi}ms`)
        expect(stats.duration).toBeLessThan(12_000)
        expect(stats.slowestApi).toBeLessThan(6_000)
        if (stats.lcp > 0) {
          expect(stats.lcp).toBeLessThan(5_000)
        }

        allResults.push({
          name: pageInfo.name,
          path: pageInfo.path,
          role: 'Analyst',
          blocked: false,
          metrics: stats
        })
      })
    }
  })
})
