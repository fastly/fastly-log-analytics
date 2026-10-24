import { expect, test, type APIRequestContext, type Page } from '@playwright/test'
import { adminGatewayContextOptions } from '../../scripts/lib/admin_gateway_client.cjs'

declare global {
  interface Window {
    __clsScore?: number
  }
}

interface LayoutShiftEntry extends PerformanceEntry {
  hadRecentInput?: boolean
  value?: number
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

/**
 * Install the cumulative layout-shift observer onto `page` via `addInitScript`.
 */
export async function installClsObserver(page: Page): Promise<void> {
  await page.addInitScript(clsInitScript)
}

/**
 * Read the accumulated unexpected layout-shift score from `window.__clsScore`.
 */
export async function readClsScore(page: Page): Promise<number> {
  return page.evaluate(() => Number(window.__clsScore ?? 0))
}

/**
 * Wait `settleMs` after a warm reload and return `{ fcp, lcp, cls }` using the
 * shared `clsInitScript` accumulator (`window.__clsScore`).
 */
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
        if (fcpEntry) {
          fcp = fcpEntry.startTime
        }
        new PerformanceObserver((list) => {
          const entries = list.getEntries()
          if (entries.length > 0) {
            lcp = entries[entries.length - 1].startTime
          }
        }).observe({ type: 'largest-contentful-paint', buffered: true })
      } catch {
        // ignore if unsupported
      }
      setTimeout(() => {
        resolve({
          fcp,
          lcp,
          cls: Number(window.__clsScore ?? 0),
        })
      }, waitMs)
    })
  }, settleMs)
}

/**
 * Extract the value of a named cookie from an HTTP response's `headersArray()`.
 */
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

/**
 * Seed a passcode-authenticated Analyst Path B invite via the admin API.
 */
export async function seedPasscodeAnalystInvite(
  request: APIRequestContext,
  spec: PasscodeInviteSpec,
): Promise<SeededPasscodeInvite> {
  const uniqueSuffix = Date.now()
  const email = spec.email ?? `e2e-analyst-${uniqueSuffix}@example.com`
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
  return {
    inviteId: inviteData.id,
    email,
    passcode,
  }
}

/**
 * Best-effort cleanup of an analyst invite and any active session.
 */
export async function revokeAnalystInvite(
  request: APIRequestContext,
  inviteId: string | null | undefined,
  options?: { logout?: boolean; stopShare?: boolean },
): Promise<void> {
  if (options?.logout) {
    await request.post('/api/share/logout').catch(() => {})
  }
  if (inviteId) {
    await request.delete(`/api/admin/share/invites/${inviteId}`).catch(() => {})
  }
  if (options?.stopShare ?? options?.logout) {
    await request.post('/api/admin/share/stop').catch(() => {})
  }
}

/**
 * Ensure sharing is started, seed a passcode analyst invite, log in, and
 * acknowledge TOS on `request`. Caller should clean up with
 * `revokeAnalystInvite(request, inviteId, { logout: true })`.
 */
export async function loginAsPasscodeAnalyst(
  request: APIRequestContext,
  spec: PasscodeInviteSpec,
): Promise<SeededPasscodeInvite> {
  await request.post('/api/admin/share/start', {
    data: { public_endpoint: 'https://share-e2e.example.com', forward_port: 13004 },
  })
  const seeded = await seedPasscodeAnalystInvite(request, spec)
  const loginRes = await request.post('/api/share/login', {
    data: { email: seeded.email, passcode: seeded.passcode },
  })
  expect(loginRes.ok()).toBeTruthy()

  const tosRes = await request.get('/api/share/tos')
  expect(tosRes.ok()).toBeTruthy()
  const tosJson = (await tosRes.json()) as { version: string }

  const ackRes = await request.post('/api/share/acknowledge', {
    data: { version: tosJson.version },
  })
  expect(ackRes.ok()).toBeTruthy()

  return seeded
}
