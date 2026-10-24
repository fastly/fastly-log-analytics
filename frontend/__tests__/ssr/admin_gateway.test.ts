import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

const { setHeaders: inbound, setCookies } = setupNextHeadersMock()

const secret = 'test-gateway-secret-with-at-least-32-characters'
let upstreamBody: object

beforeEach(() => {
  upstreamBody = {}
  process.env.ADMIN_GATEWAY_SECRET = secret
  process.env.ADMIN_GATEWAY_REQUIRED = '1'
  vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(() => {
  delete process.env.ADMIN_GATEWAY_SECRET
  delete process.env.ADMIN_GATEWAY_REQUIRED
  vi.restoreAllMocks()
})

describe('certificate gateway SSR trust', () => {
  it('forwards authenticated admin traffic on a non-loopback hostname', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        const response = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(response?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBe(secret)
        expect(srv.lastRequest?.headers.host).toBe('admin.example.com')
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it('rejects a forged credential even with a localhost Host', async () => {
    inbound({ host: 'localhost:3000', 'x-admin-gateway-token': 'forged' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not prefetch admin data for a cross-site browser request', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'sec-fetch-site': 'cross-site' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('disables the old localhost admin shortcut in required mode', async () => {
    inbound({ host: 'localhost:3000' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('keeps public ingress traffic on the analyst path', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect((await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' }))?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers.host).toBe('analyst.example.com')
      },
    )
  })

  it('never promotes a public marked request even if it carries a gateway token', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not let caller-supplied extraHeaders forge a gateway credential', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        await ssrUpstreamGet({
          path: '/api/bootstrap',
          logPrefix: 'test',
          extraHeaders: { 'x-admin-gateway-token': secret },
        })
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      },
    )
  })

  it('never returns a shared admin bootstrap to an uncredentialed localhost request', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        inbound({ host: 'localhost:3000', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not share public analyst bootstrap responses between requests', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      },
    )
  })

  it('bypasses the shared bootstrap cache for an analyst session cookie', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-cookie-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        setCookies(
          'analyst_session_id=test-session',
          (name: string) => (name === 'analyst_session_id' ? { value: 'test-session' } : undefined),
        )
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers).toEqual(
          expect.objectContaining({ cookie: 'analyst_session_id=test-session' }),
        )
        srv.clearRequests()
        setCookies('')
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('never caches a bootstrap response marked as analyst-scoped', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-response-cache-test' })
        upstreamBody = { settings: { is_remote_analyst: true } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        srv.clearRequests()
        upstreamBody = { settings: { is_remote_analyst: false } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        expect(srv.lastRequest?.headers).toBeDefined()
      },
    )
  })
})
