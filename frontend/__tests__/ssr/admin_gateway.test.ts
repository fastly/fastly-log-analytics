import { createServer, type IncomingHttpHeaders, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mockHeaders = vi.fn()
const mockCookies = vi.fn()
vi.mock('next/headers', () => ({
  headers: () => mockHeaders(),
  cookies: () => mockCookies(),
}))

const secret = 'test-gateway-secret-with-at-least-32-characters'
let server: Server
let captured: IncomingHttpHeaders | undefined

beforeEach(async () => {
  captured = undefined
  mockCookies.mockReturnValue({ toString: () => '', get: () => undefined })
  process.env.ADMIN_GATEWAY_SECRET = secret
  process.env.ADMIN_GATEWAY_REQUIRED = '1'
  server = createServer((request, response) => {
    captured = request.headers
    response.end('{}')
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  process.env.API_PROXY_URL = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
  vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
  delete process.env.ADMIN_GATEWAY_SECRET
  delete process.env.ADMIN_GATEWAY_REQUIRED
  delete process.env.API_PROXY_URL
  vi.restoreAllMocks()
})

function inbound(headers: Record<string, string>) {
  mockHeaders.mockReturnValue(new Headers(headers))
}

describe('certificate gateway SSR trust', () => {
  it('forwards authenticated admin traffic on a non-loopback hostname', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    const response = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
    expect(response?.statusCode).toBe(200)
    expect(captured?.['x-admin-gateway-token']).toBe(secret)
    expect(captured?.host).toBe('admin.example.com')
    expect(captured?.['x-remote-analyst']).toBeUndefined()
  })

  it('rejects a forged credential even with a localhost Host', async () => {
    inbound({ host: 'localhost:3000', 'x-admin-gateway-token': 'forged' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
    expect(captured).toBeUndefined()
  })

  it('does not prefetch admin data for a cross-site browser request', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'sec-fetch-site': 'cross-site' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
    expect(captured).toBeUndefined()
  })

  it('disables the old localhost admin shortcut in required mode', async () => {
    inbound({ host: 'localhost:3000' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
    expect(captured).toBeUndefined()
  })

  it('keeps public ingress traffic on the analyst path', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    expect((await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' }))?.statusCode).toBe(200)
    expect(captured?.['x-remote-analyst']).toBe('1')
    expect(captured?.['x-admin-gateway-token']).toBeUndefined()
    expect(captured?.host).toBe('analyst.example.com')
  })

  it('never promotes a public marked request even if it carries a gateway token', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
    expect(captured).toBeUndefined()
  })

  it('does not let caller-supplied extraHeaders forge a gateway credential', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await ssrUpstreamGet({
      path: '/api/bootstrap',
      logPrefix: 'test',
      extraHeaders: { 'x-admin-gateway-token': secret },
    })
    expect(captured?.['x-admin-gateway-token']).toBeUndefined()
    expect(captured?.['x-remote-analyst']).toBe('1')
  })

  it('never returns a shared admin bootstrap to an uncredentialed localhost request', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'gateway-cache-test' })
    expect(await fetchBootstrapServerSide()).toEqual({})
    captured = undefined
    inbound({ host: 'localhost:3000', 'x-service-id': 'gateway-cache-test' })
    expect(await fetchBootstrapServerSide()).toBeNull()
    expect(captured).toBeUndefined()
  })

  it('does not share public analyst bootstrap responses between requests', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-service-id': 'gateway-cache-test' })
    expect(await fetchBootstrapServerSide()).toEqual({})
    captured = undefined
    expect(await fetchBootstrapServerSide()).toEqual({})
    expect(captured?.['x-remote-analyst']).toBe('1')
  })

  it('bypasses the shared bootstrap cache for an analyst session cookie', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-cookie-cache-test' })
    expect(await fetchBootstrapServerSide()).toEqual({})
    captured = undefined
    mockCookies.mockReturnValue({
      toString: () => 'analyst_session_id=test-session',
      get: (name: string) => name === 'analyst_session_id' ? { value: 'test-session' } : undefined,
    })
    expect(await fetchBootstrapServerSide()).toEqual({})
    expect(captured).toEqual(expect.objectContaining({ cookie: 'analyst_session_id=test-session' }))
    captured = undefined
    mockCookies.mockReturnValue({ toString: () => '', get: () => undefined })
    expect(await fetchBootstrapServerSide()).toEqual({})
    expect(captured).toBeUndefined()
  })
})
