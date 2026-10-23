import { expect, test, type Page } from '@playwright/test'
import { adminGatewayContextOptions } from '../../scripts/lib/admin_gateway_client.cjs'

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
